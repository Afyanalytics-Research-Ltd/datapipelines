#!/usr/bin/env python3
"""
gdrive_landingai_extract_dag.py

Airflow DAG that pulls medical-record PDFs from Google Drive, parses them page
by page with LandingAI ADE, splits each file into individual patient records,
extracts structured JSON per record, and appends every result to results.csv.
Requires Airflow >= 2.3 (dynamic task mapping).

WHY THIS IS FASTER THAN A STANDALONE SCRIPT
  A standalone script's only parallelism is threads inside ONE Python process,
  capped by one machine's CPU/network and by the GIL for anything that isn't
  pure I/O wait.

  This DAG makes EACH FILE its own Airflow task, created dynamically with
  `.expand()` over the file list. The scheduler can then run as many of those
  file-tasks in parallel as your executor allows -- potentially dozens at once
  across multiple worker machines (Celery/Kubernetes executor). Inside each
  file-task the page-parse, boundary-classify and record-extract thread pools
  still run, so you get two multiplied layers of parallelism:

      total concurrent LandingAI API calls
        ~= (files running at once, via Airflow)
           x (PAGE_PARSE_WORKERS / BOUNDARY_WORKERS / RECORD_WORKERS threads)

  THIS MEANS YOU CAN ACCIDENTALLY OVERWHELM THE API. See "CONCURRENCY KNOBS".

RECORD-BOUNDARY DETECTION (the hard part)
  One PDF holds many patient records back to back. A record is sometimes 2
  pages, sometimes 4, sometimes 6; a single file can run to 116 pages. An
  earlier version used two heuristics -- form-header keywords and a patient
  name change -- and mis-grouped in four situations:

    1. Requiring two header keywords on the SAME page meant a first page
       carrying only one keyword scored too low and silently merged into the
       previous record.
    2. Two consecutive visits by the SAME patient produced no name change, so
       the two records fused.
    3. If a record's first page failed to parse, the placeholder markdown
       matched nothing and the record fused with its predecessor.
    4. Exact string comparison of names: "JOHN M. OTIENO" != "John Otieno",
       so OCR variance created phantom boundaries.

  The detector in this file layers four signals, cheapest and most reliable
  first:

    1. "Page 1 of 4" / "1/4" / "Pg 2 of 6" printed on the form. The strongest
       signal available -- it says both THAT a record starts and exactly HOW
       LONG it is, which is the whole 2-vs-4-vs-6 question. Free (regex) and
       it overrides everything else. It also BACK-FILLS: seeing "Page 3 of 4"
       on page i implies the record began at page i-2, recovering records
       whose first page failed to parse (fixes failure 3).
    2. A cached per-page LandingAI ade/extract call using BOUNDARY_SCHEMA,
       which asks the model directly "is this the first page of a record?"
       plus patient name / patient id / page-of-total. Cached in
       state["page_meta"], so a retried task never pays twice. By default it
       runs ONLY on pages the deterministic signals could not settle
       (BOUNDARY_ONLY_WHEN_AMBIGUOUS=1), so a 116-page file does not cost 116
       extra API calls.
    3. A weighted score over: model verdict, header keyword hits, patient-id
       change, and FUZZY patient-name change (token overlap, so dropped middle
       initials and OCR noise don't split -- fixes failure 4).
    4. Thresholded at BOUNDARY_SCORE_THRESHOLD (default 2).

  FALLBACKS, so a bad boundary never loses data:
    - Any single detected record longer than MAX_PAGES_PER_RECORD is exploded
      into one-record-per-page locally, leaving correctly detected records
      around it intact.
    - If detection yields <= 1 boundary for a long document, the whole file
      falls back to one record per page.
    - In every fallback path the last seen patient name and patient id are
      CARRIED FORWARD, so a continuation page that names nobody still reports
      the right patient, until a new name is encountered. Carried values are
      written into the JSON with patient_name_source="carried_forward" so you
      can always tell a read value from an inherited one.

  Audit it with the record_length_histogram line logged per file, and with the
  split_method / name_is_carried / boundary_score columns in results.csv.

OFFLINE TUNING (no API calls, no Airflow)
      python gdrive_landingai_extract_dag.py --inspect state/<file_id>.json
  prints how the deterministic signals alone would group a document. Use your
  existing state files to tune BOUNDARY_SCORE_THRESHOLD, RECORD_START_MARKERS
  and MAX_PAGES_PER_RECORD before spending a single API call.

REQUIREMENT -- SHARED STORAGE ACROSS WORKERS
  With CeleryExecutor/KubernetesExecutor (multiple worker machines) STATE_DIR,
  JSON_OUTPUT_DIR and OUTPUT_DIR must all resolve to the SAME shared storage
  (NFS/EFS mount, GCS/S3 FUSE mount, shared PVC) visible at the same path from
  every worker. If each worker only sees local disk, resumability breaks (a
  retried task on a different worker won't see previously cached pages,
  page_meta or records) and results.csv fragments across workers. On
  LocalExecutor this is automatic.

CROSS-PROCESS-SAFE CSV WRITES
  Many separate OS processes, possibly on different machines, append to one
  results.csv concurrently, so a threading.Lock alone would let two processes
  interleave writes and corrupt a row. append_csv_row() adds an OS-level file
  lock (fcntl.flock) around the write, which is the actual cross-process
  guard, falling back to the in-process lock only on non-POSIX systems.

CONCURRENCY KNOBS
  1. Airflow Pool "landingai_api_pool" -- caps concurrent file-tasks across
     ALL workers regardless of executor capacity. Create it once:
         airflow pools set landingai_api_pool 8 "Concurrent LandingAI file tasks"
     Raise gradually (8 -> 12 -> 16) while watching logs for 429 warnings from
     call_with_retry. Retries make things SLOWER, not faster.
  2. PAGE_PARSE_WORKERS / BOUNDARY_WORKERS / RECORD_WORKERS -- concurrency PER
     FILE TASK (default 3 each). Bumping file-level and thread-level
     concurrency at once is how you accidentally DDoS your own API key.
  3. DAG-level max_active_tasks -- hard ceiling on concurrent task instances;
     keep it >= the pool size.

NOTE ON AIRFLOW VARIABLES
  Variable.get() is called INSIDE tasks, not at module top level. Top-level
  Variable.get() hits the Airflow metadata DB on every DAG-file parse (every
  ~30s by default) and makes the file unimportable outside Airflow, which
  would break the --inspect CLI above.

Install dependencies:
    pip install "apache-airflow>=2.3" requests pypdf google-api-python-client \
                google-auth pendulum

One-time setup:
    airflow pools set landingai_api_pool 8 "Concurrent LandingAI file tasks"

Trigger a run:
    airflow dags trigger gdrive_landingai_extract
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from pypdf import PdfReader, PdfWriter

try:  # keep the --inspect CLI usable without Airflow installed
    import pendulum
    from airflow.decorators import dag, task
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:  # pragma: no cover
    AIRFLOW_AVAILABLE = False

try:
    import fcntl  # POSIX only -- true cross-process file locking
except ImportError:
    fcntl = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)
log = logging.getLogger("landingai_extract")

# ==========================================================================
# CONFIG
# ==========================================================================
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]

PARSE_MODEL = os.getenv("LANDINGAI_PARSE_MODEL", "dpt-2-latest")
EXTRACT_MODEL = os.getenv("LANDINGAI_EXTRACT_MODEL", "extract-latest")

OUTPUT_DIR = os.getenv("LANDING_OUTPUT_DIR", "./output")
JSON_OUTPUT_DIR = os.getenv("LANDING_JSON_OUTPUT_DIR", "./output/json")
STATE_DIR = os.getenv("LANDING_STATE_DIR", "./state")
SCHEMA_PATH = os.getenv("SCHEMA_PATH", "./dags/jsons/schema.json")

PAGE_PARSE_WORKERS = int(os.getenv("PAGE_PARSE_WORKERS", "3"))
BOUNDARY_WORKERS = int(os.getenv("BOUNDARY_WORKERS", "3"))
RECORD_WORKERS = int(os.getenv("RECORD_WORKERS", "3"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
RETRY_BACKOFF_SECONDS = 5
REQUEST_TIMEOUT = 180

FILE_CONCURRENCY = int(os.getenv("FILE_CONCURRENCY", "8"))

# ---- record-boundary detection -------------------------------------------
DEFAULT_RECORD_START_MARKERS = [
    "outpatient clinical consultation form",
    "patient demographics",
    "siaya medical camp",
    "chronic diseases society",
]
RECORD_START_MARKERS = [
    m.strip().lower()
    for m in os.getenv(
        "RECORD_START_MARKERS", ",".join(DEFAULT_RECORD_START_MARKERS)
    ).split(",")
    if m.strip()
]
RECORD_START_MIN_KEYWORD_MATCHES = max(
    1, min(int(os.getenv("RECORD_START_MIN_MATCHES", "2")), len(RECORD_START_MARKERS))
)
# Longest plausible record before the localised per-page fallback kicks in.
# Your records run 2-6 pages; 8 leaves headroom. If legitimate records are
# longer, raise this -- otherwise valid records get needlessly exploded, which
# shows up immediately as a spike in fallback_per_page in the histogram log.
MAX_PAGES_PER_RECORD = int(os.getenv("MAX_PAGES_PER_RECORD", "8"))
MAX_PLAUSIBLE_RUN = int(os.getenv("MAX_PLAUSIBLE_RUN", "30"))
BOUNDARY_ONLY_WHEN_AMBIGUOUS = os.getenv("BOUNDARY_ONLY_WHEN_AMBIGUOUS", "1") == "1"
BOUNDARY_SCORE_THRESHOLD = int(os.getenv("BOUNDARY_SCORE_THRESHOLD", "2"))

FAILED_PAGE_PREFIX = "<!-- page"

STATE_LOCK = threading.Lock()
CSV_LOCK = threading.Lock()

CSV_FIELDS = [
    "filename",
    "record_index",
    "page_range",
    "patient_label",
    "status",
    "split_method",
    "name_is_carried",
    "boundary_score",
    "missing_pages",
    "json_path",
    "extracted_json",
]


# ==========================================================================
# LAZY SETTINGS (see "NOTE ON AIRFLOW VARIABLES" in the module docstring)
# ==========================================================================
def get_api_key() -> str:
    key = os.getenv("VA_API_KEY")
    if not key:
        raise RuntimeError("VA_API_KEY environment variable is required.")
    return key


def landing_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {get_api_key()}"}


def _airflow_var(name: str) -> str:
    if AIRFLOW_AVAILABLE:
        try:
            return Variable.get(name)
        except Exception:
            pass
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"{name} not found -- set it as an Airflow Variable "
            f"(Admin > Variables) or as an environment variable."
        )
    return value


def get_source_folder_id() -> str:
    return _airflow_var("GDRIVE_SOURCE_FOLDER_ID")


def get_dest_folder_id() -> str:
    return _airflow_var("GDRIVE_DEST_FOLDER_ID")


# ==========================================================================
# GOOGLE DRIVE HELPERS
# ==========================================================================
def _get_service_account_info() -> Dict[str, Any]:
    """Load the service-account JSON from the Airflow Variable GOOGLE_SA_JSON
    (Admin -> Variables), which holds the full contents of gdrive-sa.json as a
    single JSON string. Falls back to an OS environment variable of the same
    name."""
    sa_json_str = None
    if AIRFLOW_AVAILABLE:
        try:
            sa_json_str = Variable.get("GOOGLE_SA_JSON")
        except Exception:
            pass
    if not sa_json_str:
        sa_json_str = os.getenv("GOOGLE_SA_JSON")
    if not sa_json_str:
        raise RuntimeError(
            "GOOGLE_SA_JSON not found -- set it as an Airflow Variable "
            "(Admin > Variables, value = the full contents of gdrive-sa.json) "
            "or as an environment variable on your workers."
        )
    try:
        return json.loads(sa_json_str)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"GOOGLE_SA_JSON is not valid JSON: {e}")


def get_drive_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    sa_info = _get_service_account_info()
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


def list_source_files(service, source_folder_id: str) -> List[Dict[str, Any]]:
    query = (
        f"'{source_folder_id}' in parents "
        f"and trashed = false "
        f"and mimeType = 'application/pdf'"
    )
    files: List[Dict[str, Any]] = []
    page_token = None
    while True:
        results = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, parents, mimeType)",
            pageSize=1000,
            pageToken=page_token,
        ).execute()
        files.extend(results.get("files", []))
        page_token = results.get("nextPageToken")
        if not page_token:
            break
    log.info("Found %s PDF file(s)", len(files))
    return files


def download_file(service, file_id: str, dest_path: Path) -> None:
    from googleapiclient.http import MediaIoBaseDownload

    request = service.files().get_media(fileId=file_id)
    with open(dest_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def move_file(service, file_id: str, add_parent_id: str):
    from googleapiclient.errors import HttpError

    try:
        file = service.files().get(fileId=file_id, fields="parents").execute()
        previous_parents = ",".join(file.get("parents", []))
        return service.files().update(
            fileId=file_id,
            addParents=add_parent_id,
            removeParents=previous_parents,
            fields="id, parents",
        ).execute()
    except HttpError as error:
        raise RuntimeError(f"Failed to move file {file_id}: {error}")


def save_json_locally(filename: str, json_data: dict, output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    local_path = os.path.join(output_dir, filename)
    tmp_path = local_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(json_data, f, indent=2)
    os.replace(tmp_path, local_path)
    return local_path


# ==========================================================================
# RETRY HELPER
# ==========================================================================
def call_with_retry(fn, *args, max_retries: int = MAX_RETRIES, **kwargs):
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except (requests.exceptions.RequestException, RuntimeError) as e:
            last_exc = e
            wait = RETRY_BACKOFF_SECONDS * attempt
            log.warning(
                "Attempt %s/%s failed (%s) -- retrying in %ss",
                attempt, max_retries, e, wait,
            )
            time.sleep(wait)
    raise RuntimeError(f"All {max_retries} attempts failed: {last_exc}")


# ==========================================================================
# CHECKPOINT STATE (per Drive file, keyed by file_id)
# ==========================================================================
def _state_path(file_id: str) -> Path:
    return Path(STATE_DIR) / f"{file_id}.json"


def load_state(file_id: str, filename: str) -> Dict[str, Any]:
    path = _state_path(file_id)
    if path.exists():
        with open(path, "r") as f:
            state = json.load(f)
        state.setdefault("pages", {})
        state.setdefault("page_errors", {})
        state.setdefault("page_meta", {})   # cached boundary classifications
        state.setdefault("records", {})
        return state
    return {
        "file_id": file_id,
        "filename": filename,
        "total_pages": None,
        "file_done": False,
        "pages": {},
        "page_errors": {},
        "page_meta": {},
        "records": {},
    }


def save_state(state: Dict[str, Any]) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    path = _state_path(state["file_id"])
    tmp_path = path.with_name(path.name + ".tmp")
    with open(tmp_path, "w") as f:
        json.dump(state, f)
    os.replace(tmp_path, path)


# ==========================================================================
# CSV -- appended to immediately, one row per completed record.
# ==========================================================================
def get_csv_path(output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    return os.path.join(output_dir, "results.csv")


def append_csv_row(csv_path: str, row: Dict[str, Any]) -> None:
    with CSV_LOCK:  # cheap in-process fast path; flock below is the real guard
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            locked = False
            if fcntl is not None:
                try:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                    locked = True
                except OSError:
                    pass  # filesystem doesn't support flock -- best effort
            try:
                has_rows = os.fstat(f.fileno()).st_size > 0
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                if not has_rows:
                    writer.writeheader()
                writer.writerow({k: row.get(k) for k in CSV_FIELDS})
                f.flush()
                os.fsync(f.fileno())
            finally:
                if locked:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def rebuild_csv_from_state(output_dir: str, state_dir: str) -> str:
    csv_path = get_csv_path(output_dir)
    if os.path.exists(csv_path):
        os.remove(csv_path)
    state_files = sorted(Path(state_dir).glob("*.json"))
    rows_written = 0
    for state_file in state_files:
        with open(state_file, "r") as f:
            state = json.load(f)
        for idx_str, record in sorted(
            state.get("records", {}).items(), key=lambda kv: int(kv[0])
        ):
            if record.get("status") not in ("done", "done_with_missing_pages"):
                continue
            append_csv_row(csv_path, {
                "filename": state.get("filename"),
                "record_index": idx_str,
                "page_range": record.get("page_range"),
                "patient_label": record.get("patient_label"),
                "status": record.get("status"),
                "split_method": record.get("split_method"),
                "name_is_carried": record.get("name_is_carried"),
                "boundary_score": record.get("boundary_score"),
                "missing_pages": ",".join(str(p) for p in record.get("missing_pages", [])) or None,
                "json_path": record.get("json_path"),
                "extracted_json": json.dumps(record.get("extracted_json")),
            })
            rows_written += 1
    log.info(
        "Rebuilt %s from state: %d row(s) from %d state file(s).",
        csv_path, rows_written, len(state_files),
    )
    return csv_path


# ==========================================================================
# PDF SPLITTING + PER-PAGE PARSE (resumable)
# ==========================================================================
def split_pdf_into_pages(pdf_path: Path, out_dir: Path) -> List[Path]:
    reader = PdfReader(str(pdf_path))
    page_paths = []
    for i, page in enumerate(reader.pages, start=1):
        writer = PdfWriter()
        writer.add_page(page)
        page_path = out_dir / f"page_{i:04d}.pdf"
        with open(page_path, "wb") as f:
            writer.write(f)
        page_paths.append(page_path)
    return page_paths


def parse_single_page(page_path: Path, page_num: int) -> Dict[str, Any]:
    def _do_parse():
        with open(page_path, "rb") as fh:
            resp = requests.post(
                "https://api.va.landing.ai/v1/ade/parse",
                headers=landing_headers(),
                files={"document": (page_path.name, fh, "application/pdf")},
                data={"model": PARSE_MODEL},
                timeout=REQUEST_TIMEOUT,
            )
        if resp.status_code == 206:
            raise RuntimeError(
                f"Page {page_num} parsed with failures: "
                f"{resp.json().get('metadata', {}).get('failed_pages')}"
            )
        resp.raise_for_status()
        return resp.json()

    try:
        data = call_with_retry(_do_parse)
    except Exception as e:
        log.error("Page %d could not be parsed after %d attempt(s): %s", page_num, MAX_RETRIES, e)
        return {"page": page_num, "markdown": None, "error": str(e)}

    markdown = data.get("markdown", "")
    if not markdown.strip():
        log.info("Page %d parsed successfully but has no text (likely a blank page).", page_num)
    return {"page": page_num, "markdown": markdown, "error": None}


def parse_pdf_per_page_resumable(
    pdf_local_path: Path, work_dir: Path, state: Dict[str, Any]
) -> List[Dict[str, Any]]:
    reader = PdfReader(str(pdf_local_path))
    total_pages = len(reader.pages)

    with STATE_LOCK:
        state["total_pages"] = total_pages
        state.setdefault("page_errors", {})
        save_state(state)

    cached_pages = state.get("pages", {})
    missing_page_nums = [i for i in range(1, total_pages + 1) if str(i) not in cached_pages]

    if not missing_page_nums:
        log.info(
            "%s: all %d page(s) already parsed in a previous run -- skipping parse.",
            state["filename"], total_pages,
        )
    else:
        log.info(
            "%s: %d of %d page(s) still need parsing.",
            state["filename"], len(missing_page_nums), total_pages,
        )
        page_paths = split_pdf_into_pages(pdf_local_path, work_dir)
        targets = {i: page_paths[i - 1] for i in missing_page_nums}
        with ThreadPoolExecutor(max_workers=PAGE_PARSE_WORKERS) as executor:
            futures = {executor.submit(parse_single_page, p, i): i for i, p in targets.items()}
            for future in as_completed(futures):
                page_num = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    log.error("Unexpected error parsing page %d: %s", page_num, e)
                    result = {"page": page_num, "markdown": None, "error": str(e)}

                with STATE_LOCK:
                    if result.get("error") is not None:
                        state["page_errors"][str(page_num)] = result["error"]
                    else:
                        state["pages"][str(page_num)] = result["markdown"]
                        state["page_errors"].pop(str(page_num), None)
                        # a re-parsed page invalidates its cached classification
                        state.get("page_meta", {}).pop(str(page_num), None)
                    save_state(state)

    page_results = []
    for i in range(1, total_pages + 1):
        key = str(i)
        if key in state["pages"]:
            page_results.append({"page": i, "markdown": state["pages"][key]})
        else:
            err = state.get("page_errors", {}).get(key, "unknown error")
            page_results.append({
                "page": i,
                "markdown": f"<!-- page {i} FAILED TO PARSE, will retry on a future run: {err} -->",
            })
    return page_results


# ==========================================================================
# RECORD BOUNDARY DETECTION
# ==========================================================================
BOUNDARY_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "is_first_page_of_record": {
            "type": "boolean",
            "description": (
                "True ONLY if this page begins a new patient record - it shows a "
                "form header or title block together with patient demographics. "
                "False for continuation pages (lab results, progress notes, "
                "medication lists, signature pages) that belong to a record "
                "started on an earlier page."
            ),
        },
        "patient_name": {
            "type": ["string", "null"],
            "description": "Full name of the patient as printed on this page, or null.",
        },
        "patient_id": {
            "type": ["string", "null"],
            "description": (
                "MRN, OP number, file number, clinic number or any other printed "
                "patient identifier, or null."
            ),
        },
        "page_number_in_record": {
            "type": ["integer", "null"],
            "description": "If the page prints 'Page 2 of 4', return 2. Otherwise null.",
        },
        "total_pages_in_record": {
            "type": ["integer", "null"],
            "description": "If the page prints 'Page 2 of 4', return 4. Otherwise null.",
        },
        "visit_date": {
            "type": ["string", "null"],
            "description": "Date of the encounter printed on this page, or null.",
        },
    },
    "required": ["is_first_page_of_record"],
}

_PAGE_OF_TOTAL = re.compile(
    r"\bp(?:a?ge?)?\.?\s*(\d{1,2})\s*(?:of|/|\\|out\s+of)\s*(\d{1,2})\b", re.IGNORECASE
)

_PATIENT_NAME_PATTERNS = [
    re.compile(r"^\s*\|\s*patient\s*name\s*\|\s*([^|\n]{2,80}?)\s*\|",
               re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*patient\s*(?:'s)?\s*name\s*[:\-]\s*(.{2,80}?)\s*$",
               re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*name\s+of\s+patient\s*[:\-]\s*(.{2,80}?)\s*$",
               re.IGNORECASE | re.MULTILINE),
]

_NAME_STOPWORDS = {"details", "field", "name", "n a", "na", "nil", "none", "unknown"}


def _normalize(text: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def is_failed_page(markdown: Optional[str]) -> bool:
    md = (markdown or "").lstrip()
    return md.startswith(FAILED_PAGE_PREFIX) and "FAILED TO PARSE" in md


def marker_hits(markdown: Optional[str]) -> int:
    nz = _normalize(markdown)
    return sum(1 for m in RECORD_START_MARKERS if _normalize(m) in nz)


def is_record_start_page(markdown: Optional[str]) -> bool:
    return marker_hits(markdown) >= RECORD_START_MIN_KEYWORD_MATCHES


def extract_patient_name(markdown: Optional[str]) -> Optional[str]:
    for pattern in _PATIENT_NAME_PATTERNS:
        m = pattern.search(markdown or "")
        if not m:
            continue
        name = m.group(1).strip(" :|-\t")
        if not name or not 1 <= len(name.split()) <= 6:
            continue
        if _normalize(name) in _NAME_STOPWORDS:
            continue
        return name
    return None


def parse_page_of_total(markdown: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    """Return (current, total) from a printed 'Page 2 of 4', else (None, None)."""
    m = _PAGE_OF_TOTAL.search(markdown or "")
    if not m:
        return None, None
    cur, tot = int(m.group(1)), int(m.group(2))
    if tot < 1 or tot > MAX_PLAUSIBLE_RUN or cur < 1 or cur > tot:
        return None, None
    return cur, tot


def _name_tokens(name: Optional[str]) -> frozenset:
    return frozenset(t for t in _normalize(name).split() if len(t) > 1)


def same_person(a: Optional[str], b: Optional[str]) -> bool:
    """Fuzzy comparison. An unknown name never triggers a split; one shared
    given+family pair (or a full match for single-token names) counts as the
    same person, so OCR variance and dropped middle initials are tolerated."""
    ta, tb = _name_tokens(a), _name_tokens(b)
    if not ta or not tb:
        return True
    shared = len(ta & tb)
    return shared >= 2 or shared == min(len(ta), len(tb))


def classify_one_page(markdown: str) -> Dict[str, Any]:
    if not markdown or not markdown.strip() or is_failed_page(markdown):
        return {}
    try:
        raw = extract_structured_data(markdown, BOUNDARY_SCHEMA) or {}
    except Exception as e:  # never fatal -- deterministic signals still apply
        log.warning("Boundary classification failed: %s", e)
        return {}
    return raw.get("extraction") or {}


def needs_classification(markdown: Optional[str]) -> bool:
    """False when the free signals already settle the page."""
    if not BOUNDARY_ONLY_WHEN_AMBIGUOUS:
        return True
    if is_failed_page(markdown):
        return False
    cur, _ = parse_page_of_total(markdown)
    if cur is not None:
        return False
    if marker_hits(markdown) >= RECORD_START_MIN_KEYWORD_MATCHES:
        return False
    return True


def classify_pages_resumable(
    page_results: List[Dict[str, Any]], state: Dict[str, Any]
) -> Dict[str, Dict[str, Any]]:
    """One cached ade/extract call per ambiguous page. Safe to re-run: pages
    already present in state["page_meta"] are never re-sent."""
    meta: Dict[str, Dict[str, Any]] = state.setdefault("page_meta", {})

    todo = [
        p for p in page_results
        if str(p["page"]) not in meta and needs_classification(p.get("markdown"))
    ]
    if not todo:
        log.info(
            "%s: boundary classification -- nothing to do (%d page(s) already resolved).",
            state.get("filename"), len(page_results),
        )
        return meta

    log.info(
        "%s: classifying %d ambiguous page(s) of %d.",
        state.get("filename"), len(todo), len(page_results),
    )
    with ThreadPoolExecutor(max_workers=BOUNDARY_WORKERS) as ex:
        futures = {
            ex.submit(classify_one_page, p.get("markdown") or ""): p["page"] for p in todo
        }
        for future in as_completed(futures):
            page_num = futures[future]
            try:
                result = future.result()
            except Exception as e:
                log.warning("Boundary classification error on page %s: %s", page_num, e)
                result = {}
            with STATE_LOCK:
                meta[str(page_num)] = result
                save_state(state)
    return meta


def detect_boundaries(
    page_results: List[Dict[str, Any]], page_meta: Dict[str, Dict[str, Any]]
) -> Tuple[List[int], Dict[int, int]]:
    """Return (sorted start indices, {start_index: score}).

    A printed 'Page N of M' is authoritative and suppresses every other signal
    for the pages it covers."""
    n = len(page_results)
    starts: Dict[int, int] = {}
    covered_to = -1
    last_name: Optional[str] = None
    last_id: Optional[str] = None

    for i, p in enumerate(page_results):
        md = p.get("markdown") or ""
        meta = page_meta.get(str(p["page"])) or {}

        cur, tot = parse_page_of_total(md)
        if cur is None:
            mc, mt = meta.get("page_number_in_record"), meta.get("total_pages_in_record")
            if isinstance(mc, int) and isinstance(mt, int) and 1 <= mc <= mt <= MAX_PLAUSIBLE_RUN:
                cur, tot = mc, mt

        if cur is not None:
            start_idx = i - (cur - 1)
            if start_idx >= 0 and start_idx > covered_to:
                starts[start_idx] = max(starts.get(start_idx, 0), 10)
                covered_to = start_idx + tot - 1
                if cur > 1:
                    log.info(
                        "Page %s prints 'page %d of %d' -- back-filling record start at page %s.",
                        p["page"], cur, tot, page_results[start_idx]["page"],
                    )
            nm = meta.get("patient_name") or extract_patient_name(md)
            if nm:
                last_name = nm
            if meta.get("patient_id"):
                last_id = meta["patient_id"]
            continue

        if i <= covered_to:
            continue  # inside an authoritative run -- never split

        name = meta.get("patient_name") or extract_patient_name(md)
        pid = meta.get("patient_id")

        score = 0
        if meta.get("is_first_page_of_record") is True:
            score += 2
        hits = marker_hits(md)
        score += 2 if hits >= RECORD_START_MIN_KEYWORD_MATCHES else (1 if hits else 0)
        if pid and last_id and _normalize(pid) != _normalize(last_id):
            score += 3
        if name and last_name and not same_person(name, last_name):
            score += 2

        if i == 0:
            starts[i] = max(starts.get(i, 0), score)
        elif score >= BOUNDARY_SCORE_THRESHOLD:
            starts[i] = score

        if name:
            last_name = name
        if pid:
            last_id = pid

    if not starts:
        starts = {0: 0}
    if 0 not in starts:
        starts[0] = 0
    return sorted(starts), starts


def _page_identity(
    page: Dict[str, Any], page_meta: Dict[str, Dict[str, Any]]
) -> Tuple[Optional[str], Optional[str]]:
    meta = page_meta.get(str(page["page"])) or {}
    name = meta.get("patient_name") or extract_patient_name(page.get("markdown") or "")
    return name, meta.get("patient_id")


def _make_record(pages, page_meta, method, score, last_name, last_id):
    name = pid = None
    for p in pages:
        n, i = _page_identity(p, page_meta)
        name = name or n
        pid = pid or i

    carried = name is None and last_name is not None
    rec = {
        "pages": pages,
        "page_numbers": [p["page"] for p in pages],
        "patient_name_hint": name or last_name,
        "patient_id_hint": pid or last_id,
        "name_is_carried": carried,
        "split_method": method,
        "boundary_score": score,
    }
    return rec, (name or last_name), (pid or last_id)


def group_pages_into_records(
    page_results: List[Dict[str, Any]],
    page_meta: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Group pages into records.

    Returns a list of DICTS (not page-lists):
        {pages, page_numbers, patient_name_hint, patient_id_hint,
         name_is_carried, split_method, boundary_score}
    """
    page_meta = page_meta or {}
    n = len(page_results)
    if n == 0:
        return []

    starts, scores = detect_boundaries(page_results, page_meta)

    # Whole-file fallback: a long document that produced no usable boundary.
    if len(starts) <= 1 and n > MAX_PAGES_PER_RECORD:
        log.warning(
            "No usable record boundary found across %d page(s) -- falling back to "
            "one record per page with patient carry-forward.", n,
        )
        starts, scores = list(range(n)), {}

    records: List[Dict[str, Any]] = []
    last_name: Optional[str] = None
    last_id: Optional[str] = None

    for k, start in enumerate(starts):
        end = starts[k + 1] if k + 1 < len(starts) else n
        pages = page_results[start:end]
        score = scores.get(start, 0)

        # Localised fallback: this one record is implausibly long, but the
        # records detected around it are probably fine -- explode only this one.
        if len(pages) > MAX_PAGES_PER_RECORD:
            log.warning(
                "Record starting at page %s spans %d page(s) (max %d) -- splitting "
                "it per page with carry-forward.",
                pages[0]["page"], len(pages), MAX_PAGES_PER_RECORD,
            )
            for p in pages:
                rec, last_name, last_id = _make_record(
                    [p], page_meta, "fallback_per_page", 0, last_name, last_id
                )
                records.append(rec)
            continue

        method = "detected" if score else "assumed_start"
        rec, last_name, last_id = _make_record(
            pages, page_meta, method, score, last_name, last_id
        )
        records.append(rec)

    return records


def build_record_markdown(rec: Dict[str, Any]) -> str:
    """Concatenate a record's pages, prefixing patient context when the name
    had to be carried forward so the extraction model sees it."""
    body = "\n\n".join(
        f"<!-- page {p['page']} -->\n{p.get('markdown') or ''}" for p in rec["pages"]
    )
    if not rec.get("name_is_carried"):
        return body
    hints = []
    if rec.get("patient_name_hint"):
        hints.append(f"patient name: {rec['patient_name_hint']}")
    if rec.get("patient_id_hint"):
        hints.append(f"patient id: {rec['patient_id_hint']}")
    if not hints:
        return body
    return (
        "<!-- context: these pages continue the record of "
        + "; ".join(hints)
        + " -->\n\n"
        + body
    )


def apply_carry_forward(extracted: Dict[str, Any], rec: Dict[str, Any]) -> Dict[str, Any]:
    """Fill patient_name / patient_id from the carried-forward hints when the
    model found none on these pages. Mutates and returns `extracted`."""
    if not isinstance(extracted, dict):
        return extracted
    extraction = extracted.get("extraction")
    if not isinstance(extraction, dict):
        extraction = {}
        extracted["extraction"] = extraction

    if not extraction.get("patient_name") and rec.get("patient_name_hint"):
        extraction["patient_name"] = rec["patient_name_hint"]
        extraction["patient_name_source"] = "carried_forward"
    if not extraction.get("patient_id") and rec.get("patient_id_hint"):
        extraction["patient_id"] = rec["patient_id_hint"]
        extraction.setdefault("patient_id_source", "carried_forward")

    extraction.setdefault("_split_method", rec.get("split_method"))
    extraction.setdefault("_page_numbers", rec.get("page_numbers"))
    return extracted


def record_length_histogram(records: List[Dict[str, Any]]) -> str:
    """One-line audit string, e.g.
    '24 record(s); lengths 2x11 4x9 6x4; methods detected=22 fallback_per_page=2'."""
    if not records:
        return "0 record(s)"
    lengths: Dict[int, int] = {}
    methods: Dict[str, int] = {}
    carried = 0
    for r in records:
        lengths[len(r["pages"])] = lengths.get(len(r["pages"]), 0) + 1
        methods[r["split_method"]] = methods.get(r["split_method"], 0) + 1
        if r.get("name_is_carried"):
            carried += 1
    len_str = " ".join(f"{k}x{v}" for k, v in sorted(lengths.items()))
    meth_str = " ".join(f"{k}={v}" for k, v in sorted(methods.items()))
    return f"{len(records)} record(s); lengths {len_str}; methods {meth_str}; carried_names={carried}"


# ==========================================================================
# EXTRACT
# ==========================================================================
def extract_structured_data(markdown_content: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    def _do_extract():
        resp = requests.post(
            "https://api.va.landing.ai/v1/ade/extract",
            headers=landing_headers(),
            files={"markdown": ("document.md", markdown_content, "text/markdown")},
            data={"schema": json.dumps(schema), "model": EXTRACT_MODEL},
            timeout=REQUEST_TIMEOUT,
        )
        if not resp.ok:
            raise RuntimeError(f"extract failed: {resp.status_code} - {resp.text}")
        return resp.json()

    return call_with_retry(_do_extract)


# ==========================================================================
# PER-RECORD EXTRACTION (concurrent within a file)
# ==========================================================================
def _process_one_record(
    file_name: str, file_id: str, idx: int, rec: Dict[str, Any],
    schema: Dict[str, Any], json_output_dir: str, csv_path: str, save_json: bool,
    state: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    key = str(idx)
    record_pages = rec["pages"]
    page_numbers = rec["page_numbers"]
    page_range = f"{min(page_numbers)}-{max(page_numbers)}"

    existing = state["records"].get(key)
    if existing and existing.get("status") == "done":
        log.info("%s record %d (pages %s) already done -- skipping.", file_name, idx, page_range)
        return None

    page_errors = state.get("page_errors", {})
    missing_pages = [p for p in page_numbers if str(p) in page_errors]

    try:
        md = build_record_markdown(rec)
        extracted = extract_structured_data(md, schema)
        apply_carry_forward(extracted, rec)

        json_filename = f"{Path(file_name).stem}_record{idx:02d}_p{page_range}.json"
        json_path = None
        if save_json:
            json_path = save_json_locally(json_filename, extracted, json_output_dir)

        patient_label = (
            (extracted.get("extraction") or {}).get("patient_name")
            or rec.get("patient_name_hint")
        )
        status = "done_with_missing_pages" if missing_pages else "done"
        if missing_pages:
            log.warning(
                "%s record %d (pages %s) extracted with page(s) %s still unparseable -- "
                "will retry that content on a future run.",
                file_name, idx, page_range, missing_pages,
            )

        with STATE_LOCK:
            state["records"][key] = {
                "page_range": page_range,
                "status": status,
                "patient_label": patient_label,
                "split_method": rec.get("split_method"),
                "name_is_carried": rec.get("name_is_carried"),
                "boundary_score": rec.get("boundary_score"),
                "missing_pages": missing_pages,
                "json_path": json_path,
                "extracted_json": extracted,
            }
            save_state(state)

        # NOTE: the full extracted JSON is written to disk (CSV + local JSON)
        # but deliberately NOT returned -- Airflow persists task return values
        # to its XCom metadata database, and pushing ~1800 extraction blobs
        # through XCom would bloat it for no benefit.
        append_csv_row(csv_path, {
            "filename": file_name,
            "record_index": idx,
            "page_range": page_range,
            "patient_label": patient_label,
            "status": status,
            "split_method": rec.get("split_method"),
            "name_is_carried": rec.get("name_is_carried"),
            "boundary_score": rec.get("boundary_score"),
            "missing_pages": ",".join(str(p) for p in missing_pages) or None,
            "json_path": json_path,
            "extracted_json": json.dumps(extracted),
        })
        return {
            "ok": True, "filename": file_name, "record_index": idx,
            "page_range": page_range, "status": status,
            "split_method": rec.get("split_method"),
        }

    except Exception as e:
        log.exception("Failed to extract record %d (pages %s) of %s", idx, page_range, file_name)
        with STATE_LOCK:
            state["records"][key] = {
                "page_range": page_range, "status": "failed", "error": str(e),
                "split_method": rec.get("split_method"),
            }
            save_state(state)
        return {
            "ok": False, "filename": file_name, "file_id": file_id,
            "record_index": idx, "page_range": page_range, "error": str(e),
        }


# ==========================================================================
# PER-FILE PIPELINE -- one Airflow task instance per file via .expand()
# ==========================================================================
def process_one_file(
    service, file_meta: Dict[str, Any], schema: Dict[str, Any], dest_folder_id: str,
    json_output_dir: str, csv_path: str, save_json: bool = True,
) -> List[Dict[str, Any]]:
    file_id = file_meta["id"]
    file_name = file_meta["name"]
    mime_type = file_meta.get("mimeType", "")

    if mime_type != "application/pdf":
        return [{
            "ok": False, "filename": file_name, "file_id": file_id, "record_index": 0,
            "page_range": None, "error": f"Unsupported mimeType: {mime_type}",
        }]

    state = load_state(file_id, file_name)

    if state.get("file_done"):
        log.info(
            "%s already marked done in a previous run -- confirming it's out of the source folder.",
            file_name,
        )
        try:
            move_file(service, file_id, dest_folder_id)
        except Exception:
            pass
        return []

    tmp_dir = Path(tempfile.mkdtemp(prefix="landingai_"))
    local_path = tmp_dir / file_name
    results: List[Dict[str, Any]] = []

    try:
        total_pages_known = state.get("total_pages")
        pages_cached = state.get("pages", {})
        fully_cached = bool(total_pages_known) and all(
            str(i) in pages_cached for i in range(1, total_pages_known + 1)
        )

        if fully_cached:
            log.info(
                "%s: reusing %d cached page(s) from a previous run -- skipping download.",
                file_name, total_pages_known,
            )
            page_results = [
                {"page": i, "markdown": pages_cached[str(i)]}
                for i in range(1, total_pages_known + 1)
            ]
        else:
            log.info("Downloading %s (%s)", file_name, file_id)
            download_file(service, file_id, local_path)
            page_results = parse_pdf_per_page_resumable(local_path, tmp_dir, state)

        page_meta = classify_pages_resumable(page_results, state)
        records = group_pages_into_records(page_results, page_meta)
        log.info("%s: %d page(s) -> %s", file_name, len(page_results), record_length_histogram(records))

        with ThreadPoolExecutor(max_workers=RECORD_WORKERS) as record_executor:
            record_futures = {
                record_executor.submit(
                    _process_one_record, file_name, file_id, idx, rec,
                    schema, json_output_dir, csv_path, save_json, state,
                ): idx
                for idx, rec in enumerate(records, start=1)
            }
            for future in as_completed(record_futures):
                idx = record_futures[future]
                try:
                    r = future.result()
                except Exception as e:
                    log.exception("Unexpected error processing record %d of %s", idx, file_name)
                    r = {"ok": False, "filename": file_name, "file_id": file_id,
                         "record_index": idx, "page_range": None, "error": str(e)}
                if r is not None:
                    results.append(r)

        all_done = len(records) > 0 and all(
            state["records"].get(str(i), {}).get("status") == "done"
            for i in range(1, len(records) + 1)
        )
        if all_done:
            with STATE_LOCK:
                state["file_done"] = True
                save_state(state)
            move_file(service, file_id, dest_folder_id)
        else:
            log.warning("%s left in source folder: at least one record still needs a retry.", file_name)

        return results

    except Exception as e:
        log.exception("Failed to process %s", file_name)
        return [{
            "ok": False, "filename": file_name, "file_id": file_id,
            "record_index": 0, "page_range": None, "error": str(e),
        }]

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ==========================================================================
# AIRFLOW DAG
# ==========================================================================
if AIRFLOW_AVAILABLE:

    @dag(
        dag_id="gdrive_landingai_extract",
        schedule=None,  # triggered manually / on demand -- not a recurring job
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_tasks=max(FILE_CONCURRENCY, 8),
        tags=["landingai", "gdrive", "extraction"],
        doc_md=__doc__,
    )
    def gdrive_landingai_extract():

        @task
        def list_files() -> List[Dict[str, Any]]:
            service = get_drive_service()
            return list_source_files(service, get_source_folder_id())

        @task(
            pool="landingai_api_pool",  # airflow pools set landingai_api_pool 8 "..."
            max_active_tis_per_dag=FILE_CONCURRENCY,
            retries=2,
            retry_delay=timedelta(minutes=3),
            execution_timeout=timedelta(minutes=45),
        )
        def process_file(file_meta: Dict[str, Any]) -> List[Dict[str, Any]]:
            service = get_drive_service()
            with open(SCHEMA_PATH, "r") as f:
                schema = json.load(f)
            csv_path = get_csv_path(OUTPUT_DIR)
            return process_one_file(
                service, file_meta, schema, get_dest_folder_id(),
                JSON_OUTPUT_DIR, csv_path,
            )

        @task
        def summarize_run(results: List[List[Dict[str, Any]]]) -> None:
            flat = [r for sub in results for r in sub]
            total = len(flat)
            ok = sum(1 for r in flat if r.get("ok"))
            failed = [r for r in flat if not r.get("ok")]

            methods: Dict[str, int] = {}
            for r in flat:
                if r.get("ok") and r.get("split_method"):
                    methods[r["split_method"]] = methods.get(r["split_method"], 0) + 1

            log.info("Run complete. %s/%s records succeeded this run.", ok, total)
            if methods:
                log.info(
                    "Split methods: %s",
                    " ".join(f"{k}={v}" for k, v in sorted(methods.items())),
                )
                fallback = methods.get("fallback_per_page", 0)
                if total and fallback / max(ok, 1) > 0.25:
                    log.warning(
                        "%d/%d records came from the per-page fallback (>25%%). "
                        "Check RECORD_START_MARKERS and MAX_PAGES_PER_RECORD -- "
                        "boundary detection is probably missing a form header.",
                        fallback, ok,
                    )
            if failed:
                log.warning("Failures (will be retried automatically next run):")
                for f in failed:
                    log.warning(
                        "  - %s record %s (pages %s): %s",
                        f.get("filename"), f.get("record_index"),
                        f.get("page_range"), f.get("error"),
                    )

        files = list_files()
        per_file_results = process_file.expand(file_meta=files)
        summarize_run(per_file_results)

    gdrive_landingai_extract()


# ==========================================================================
# OFFLINE TUNING CLI -- no API calls, deterministic signals only
#     python gdrive_landingai_extract_dag.py --inspect state/<file_id>.json
# ==========================================================================
def _inspect(state_path: str) -> int:
    with open(state_path, "r", encoding="utf-8") as f:
        state = json.load(f)

    pages = state.get("pages", {})
    total = state.get("total_pages") or max((int(k) for k in pages), default=0)
    page_results = [{"page": i, "markdown": pages.get(str(i), "")} for i in range(1, total + 1)]
    page_meta = state.get("page_meta", {})

    records = group_pages_into_records(page_results, page_meta)

    print(f"\nfile      : {state.get('filename')}")
    print(f"pages     : {len(page_results)}")
    print(f"classified: {len(page_meta)} page(s) have cached boundary metadata")
    print(f"summary   : {record_length_histogram(records)}\n")
    for idx, r in enumerate(records, start=1):
        nums = r["page_numbers"]
        span = f"{nums[0]}-{nums[-1]}" if len(nums) > 1 else str(nums[0])
        flag = "  [name carried]" if r["name_is_carried"] else ""
        print(
            f"  record {idx:>3}  pages {span:<9} {r['split_method']:<18} "
            f"score={r['boundary_score']:<3} {r['patient_name_hint'] or '(unknown)'}{flag}"
        )
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--inspect":
        sys.exit(_inspect(sys.argv[2]))
    print(__doc__)
    print("usage: python gdrive_landingai_extract_dag.py --inspect state/<file_id>.json")
    sys.exit(1)