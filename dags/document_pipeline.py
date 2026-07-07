#!/usr/bin/env python3
"""
gdrive_landingai_extract_dag.py

Airflow DAG version of gdrive_landingai_extract.py, built for raw throughput
under time pressure. Requires Airflow >= 2.3 (dynamic task mapping).

WHY THIS IS FASTER THAN THE STANDALONE SCRIPT
  The standalone script's only parallelism was threads inside ONE Python
  process (FILE_WORKERS files at once, each with PAGE_PARSE_WORKERS /
  RECORD_WORKERS threads). That's capped by one machine's CPU/network and by
  the GIL for anything that isn't pure I/O wait.

  This DAG instead makes EACH FILE its own Airflow task, created dynamically
  with `.expand()` over the file list. Airflow's scheduler can then run as
  many of those file-tasks in parallel as your executor/workers allow --
  potentially dozens running at once across multiple worker machines (Celery/
  Kubernetes executor), not just multiple threads on one box. Inside each
  file-task, the existing page-parse and record-extract thread pools still
  run too, so you get two multiplied layers of parallelism:

      total concurrent LandingAI API calls
        ~= (files running at once, via Airflow)
           x (PAGE_PARSE_WORKERS or RECORD_WORKERS, via threads)

  THIS MEANS YOU CAN ACCIDENTALLY OVERWHELM THE API. See "CONCURRENCY KNOBS"
  below -- there's a dedicated Airflow Pool to cap total concurrent files
  regardless of how many workers you have, specifically to prevent this.

REQUIREMENT -- SHARED STORAGE ACROSS WORKERS:
  If you run this with CeleryExecutor/KubernetesExecutor (multiple worker
  machines), STATE_DIR, JSON_OUTPUT_DIR, OUTPUT_DIR (results.csv), and the
  service-account file path must all resolve to the SAME shared storage
  (NFS/EFS mount, GCS/S3 FUSE mount, shared PVC, etc.) visible at the same
  path from every worker. If each worker only sees its own local disk,
  resumability breaks (a retried task on a different worker won't see
  previously-cached pages/records) and results.csv fragments across workers
  instead of being one file. If you're on LocalExecutor (single machine),
  this is automatically satisfied and you can ignore this section.

CROSS-PROCESS-SAFE CSV WRITES:
  The standalone script guarded results.csv with a Python threading.Lock,
  which only protects against other THREADS in the same process. Under this
  DAG, many separate OS processes (possibly on different machines) append to
  the same results.csv concurrently, so threading.Lock alone would let two
  processes interleave writes and corrupt a row. append_csv_row() below adds
  an OS-level file lock (fcntl.flock) around the write, which is the actual
  cross-process guard (works for any workers that share the same
  filesystem/mount; falls back to the in-process lock only on non-POSIX
  systems, which Airflow workers never are in practice).

CONCURRENCY KNOBS (tune these to go faster without tripping API rate limits):
  1. Airflow Pool "landingai_api_pool" -- caps how many file-tasks run at
     once, across ALL workers, regardless of how much executor capacity you
     have. Create it once before running:
         airflow pools set landingai_api_pool 8 "Concurrent LandingAI file tasks"
     Raise the slot count gradually (8 -> 12 -> 16 ...) while watching the
     task logs for 429 / rate-limit warnings from call_with_retry. If you see
     those warnings increasing, lower it back down -- retries make things
     SLOWER, not faster.
  2. PAGE_PARSE_WORKERS / RECORD_WORKERS env vars -- concurrency PER FILE
     TASK (default lowered to 3 each here, vs. 4 in the standalone script,
     since file-level parallelism now does most of the scaling work; bumping
     both dimensions at once is how you accidentally DDoS your own API key).
  3. DAG-level max_active_tasks below -- a hard ceiling on total concurrently
     running task instances for this DAG; keep it >= the pool size above.

  Everything else (resumability, non-fatal page/record failures, continuous
  CSV output, two-signal multi-patient record-boundary detection, local JSON
  storage, dpt-2-latest model, Bearer auth) is unchanged from the standalone
  script -- see that file's docstring for the full history of why each of
  those exists. This file is a straight architectural port plus one bug fix
  it was missing: record extraction within a file now also runs concurrently
  (RECORD_WORKERS) instead of one record at a time, which was the single
  biggest bottleneck found during a live production run.

Install dependencies (on top of the standalone script's requirements.txt):
    pip install "apache-airflow>=2.3"

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
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pendulum
import requests
from pypdf import PdfReader, PdfWriter

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from airflow.decorators import dag, task

try:
    import fcntl  # POSIX only -- true cross-process file locking
except ImportError:
    fcntl = None  # Airflow workers are always POSIX in practice; guarded anyway

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)
log = logging.getLogger("landingai_extract")

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]

SOURCE_FOLDER_ID = os.getenv("GDRIVE_SOURCE_FOLDER_ID", "val")
DEST_FOLDER_ID = os.getenv("GDRIVE_DEST_FOLDER_ID", "val")

VA_API_KEY = os.getenv("VA_API_KEY")
if not VA_API_KEY:
    raise RuntimeError("VA_API_KEY environment variable is required.")

LANDING_HEADERS = {"Authorization": f"Bearer {VA_API_KEY}"}

PARSE_MODEL = os.getenv("LANDINGAI_PARSE_MODEL", "dpt-2-latest")
EXTRACT_MODEL = os.getenv("LANDINGAI_EXTRACT_MODEL", "extract-latest")

OUTPUT_DIR = os.getenv("LANDING_OUTPUT_DIR", "./output")
JSON_OUTPUT_DIR = os.getenv("LANDING_JSON_OUTPUT_DIR", "./output/json")
STATE_DIR = os.getenv("LANDING_STATE_DIR", "./state")
SCHEMA_PATH = os.getenv("SCHEMA_PATH", "./dags/jsons/schema.json")

SERVICE_ACCOUNT_FILE = os.getenv("GDRIVE_SERVICE_ACCOUNT_FILE", "./gdrive-sa.json")

# Lower per-file defaults than the standalone script (3 vs 4) -- file-level
# parallelism via Airflow now does a big share of the scaling, so per-file
# thread counts get multiplied by however many files run at once. Tune the
# Airflow Pool slot count first; only raise these if a single file itself is
# still slow with plenty of pool headroom to spare.
PAGE_PARSE_WORKERS = int(os.getenv("PAGE_PARSE_WORKERS", "3"))
RECORD_WORKERS = int(os.getenv("RECORD_WORKERS", "3"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
RETRY_BACKOFF_SECONDS = 5
REQUEST_TIMEOUT = 180

# Concurrent files, across ALL Airflow workers. This is the primary "go
# faster" knob -- see CONCURRENCY KNOBS in the module docstring. Also create
# a matching Airflow Pool (landingai_api_pool) so this cap holds even if the
# DAG-level max_active_tasks is higher.
FILE_CONCURRENCY = int(os.getenv("FILE_CONCURRENCY", "8"))

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

# In-process guards. Real cross-process safety for the CSV comes from
# fcntl.flock inside append_csv_row -- see module docstring.
STATE_LOCK = threading.Lock()
CSV_LOCK = threading.Lock()

CSV_FIELDS = ["filename", "record_index", "page_range", "patient_label", "status", "missing_pages", "json_path", "extracted_json"]


# --------------------------------------------------------------------------
# GOOGLE DRIVE HELPERS
# --------------------------------------------------------------------------
def write_service_account_file(path: str = SERVICE_ACCOUNT_FILE) -> None:
    sa_json = {
        "type": os.getenv("GCP_SA_TYPE", "service_account"),
        "project_id": os.getenv("GCP_SA_PROJECT_ID", "default"),
        "private_key_id": os.getenv("GCP_SA_PRIVATE_KEY_ID", "default"),
        "private_key": os.getenv("GCP_SA_PRIVATE_KEY", "default").replace("\\n", "\n"),
        "client_email": os.getenv("GCP_SA_CLIENT_EMAIL", "default"),
        "client_id": os.getenv("GCP_SA_CLIENT_ID", "default"),
        "token_uri": os.getenv("GCP_SA_TOKEN_URI", "https://oauth2.googleapis.com/token"),
    }
    with open(path, "w") as f:
        json.dump(sa_json, f)


def get_drive_service():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        write_service_account_file()
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)


def list_source_files(service, source_folder_id: str) -> List[Dict[str, Any]]:
    query = (
        f"'{source_folder_id}' in parents "
        f"and trashed = false "
        f"and mimeType = 'application/pdf'"
    )
    results = service.files().list(
        q=query, fields="files(id, name, parents, mimeType)"
    ).execute()
    files = results.get("files", [])
    log.info("Found %s PDF file(s)", len(files))
    return files


def download_file(service, file_id: str, dest_path: Path) -> None:
    request = service.files().get_media(fileId=file_id)
    with open(dest_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def move_file(service, file_id: str, add_parent_id: str):
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


# --------------------------------------------------------------------------
# RETRY HELPER
# --------------------------------------------------------------------------
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


# --------------------------------------------------------------------------
# CHECKPOINT STATE (per Drive file, keyed by file_id)
# --------------------------------------------------------------------------
def _state_path(file_id: str) -> Path:
    return Path(STATE_DIR) / f"{file_id}.json"


def load_state(file_id: str, filename: str) -> Dict[str, Any]:
    path = _state_path(file_id)
    if path.exists():
        with open(path, "r") as f:
            state = json.load(f)
        state.setdefault("pages", {})
        state.setdefault("page_errors", {})
        state.setdefault("records", {})
        return state
    return {
        "file_id": file_id,
        "filename": filename,
        "total_pages": None,
        "file_done": False,
        "pages": {},
        "page_errors": {},
        "records": {},
    }


def save_state(state: Dict[str, Any]) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    path = _state_path(state["file_id"])
    tmp_path = path.with_name(path.name + ".tmp")
    with open(tmp_path, "w") as f:
        json.dump(state, f)
    os.replace(tmp_path, path)


# --------------------------------------------------------------------------
# CSV -- appended to immediately, one row per completed record.
# Cross-process safe (fcntl.flock) since many separate Airflow task
# processes -- possibly on different worker machines sharing the same
# mounted output directory -- append to this one file concurrently.
# --------------------------------------------------------------------------
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
                file_exists = os.fstat(f.fileno()).st_size > 0
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                if not file_exists:
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
        for idx_str, record in sorted(state.get("records", {}).items(), key=lambda kv: int(kv[0])):
            if record.get("status") not in ("done", "done_with_missing_pages"):
                continue
            append_csv_row(csv_path, {
                "filename": state.get("filename"),
                "record_index": idx_str,
                "page_range": record.get("page_range"),
                "patient_label": record.get("patient_label"),
                "status": record.get("status"),
                "missing_pages": ",".join(str(p) for p in record.get("missing_pages", [])) or None,
                "json_path": record.get("json_path"),
                "extracted_json": json.dumps(record.get("extracted_json")),
            })
            rows_written += 1
    log.info("Rebuilt %s from state: %d row(s) from %d state file(s).", csv_path, rows_written, len(state_files))
    return csv_path


# --------------------------------------------------------------------------
# PDF SPLITTING + PER-PAGE PARSE (resumable)
# --------------------------------------------------------------------------
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
                headers=LANDING_HEADERS,
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
        log.info("%s: all %d page(s) already parsed in a previous run -- skipping parse.",
                  state["filename"], total_pages)
    else:
        log.info("%s: %d of %d page(s) still need parsing.",
                  state["filename"], len(missing_page_nums), total_pages)
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


# --------------------------------------------------------------------------
# RECORD SPLITTING (two independent signals -- see standalone script's
# module docstring for the full rationale; unchanged here)
# --------------------------------------------------------------------------
def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower())


def is_record_start_page(markdown: str) -> bool:
    normalized = _normalize(markdown)
    matches = sum(1 for marker in RECORD_START_MARKERS if _normalize(marker) in normalized)
    return matches >= RECORD_START_MIN_KEYWORD_MATCHES


_PATIENT_NAME_PATTERNS = [
    re.compile(r"^\s*\|\s*patient\s*name\s*\|\s*([^|\n]{2,80}?)\s*\|", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*patient\s*name\s*[:\-]\s*(.{2,80}?)\s*$", re.IGNORECASE | re.MULTILINE),
]


def extract_patient_name(markdown: str) -> Optional[str]:
    for pattern in _PATIENT_NAME_PATTERNS:
        m = pattern.search(markdown)
        if m:
            name = m.group(1).strip(" :|-")
            word_count = len(name.split())
            if name and 1 <= word_count <= 6 and _normalize(name) not in ("details", "field"):
                return name
    return None


def group_pages_into_records(page_results: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    if not page_results:
        return []

    start_indices = set(
        i for i, p in enumerate(page_results) if is_record_start_page(p["markdown"])
    )

    current_name = None
    for i, p in enumerate(page_results):
        name = extract_patient_name(p["markdown"])
        if name is None:
            continue
        if current_name is not None and i not in start_indices and _normalize(name) != _normalize(current_name):
            log.warning(
                "Page %d: patient name changed ('%s' -> '%s') without a recognized form "
                "header -- treating it as a new record boundary anyway.",
                p["page"], current_name, name,
            )
            start_indices.add(i)
        current_name = name

    start_indices = sorted(start_indices)

    if not start_indices:
        log.warning("No record-start signal found on any page -- treating the whole file as a single record.")
        return [page_results]

    if start_indices[0] != 0:
        start_indices = [0] + start_indices

    records = []
    for idx, start in enumerate(start_indices):
        end = start_indices[idx + 1] if idx + 1 < len(start_indices) else len(page_results)
        records.append(page_results[start:end])
    return records


def record_markdown(record_pages: List[Dict[str, Any]]) -> str:
    return "\n\n".join(
        f"<!-- page {r['page']} -->\n{r['markdown']}" for r in record_pages
    )


# --------------------------------------------------------------------------
# EXTRACT
# --------------------------------------------------------------------------
def extract_structured_data(markdown_content: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    def _do_extract():
        resp = requests.post(
            "https://api.va.landing.ai/v1/ade/extract",
            headers=LANDING_HEADERS,
            files={"markdown": ("document.md", markdown_content, "text/markdown")},
            data={"schema": json.dumps(schema), "model": EXTRACT_MODEL},
            timeout=REQUEST_TIMEOUT,
        )
        if not resp.ok:
            raise RuntimeError(f"extract failed: {resp.status_code} - {resp.text}")
        return resp.json()

    return call_with_retry(_do_extract)


# --------------------------------------------------------------------------
# PER-RECORD EXTRACTION (concurrent within a file -- the fix that was
# missing from the version of this script pasted back in; see module
# docstring)
# --------------------------------------------------------------------------
def _process_one_record(
    file_name: str, file_id: str, idx: int, record_pages: List[Dict[str, Any]],
    schema: Dict[str, Any], json_output_dir: str, csv_path: str, save_json: bool,
    state: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    key = str(idx)
    page_numbers = [p["page"] for p in record_pages]
    page_range = f"{min(page_numbers)}-{max(page_numbers)}"

    existing = state["records"].get(key)
    if existing and existing.get("status") == "done":
        log.info("%s record %d (pages %s) already done -- skipping.", file_name, idx, page_range)
        return None

    page_errors = state.get("page_errors", {})
    missing_pages = [p["page"] for p in record_pages if str(p["page"]) in page_errors]

    try:
        md = record_markdown(record_pages)
        extracted = extract_structured_data(md, schema)

        json_filename = f"{Path(file_name).stem}_record{idx:02d}_p{page_range}.json"
        json_path = None
        if save_json:
            json_path = save_json_locally(json_filename, extracted, json_output_dir)

        patient_label = (extracted.get("extraction") or {}).get("patient_name")
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
                "missing_pages": missing_pages,
                "json_path": json_path,
                "extracted_json": extracted,
            }
            save_state(state)

        # NOTE: the full extracted JSON is written to disk (CSV + local JSON
        # file) but deliberately NOT included in what gets returned here --
        # Airflow persists task return values to its XCom metadata database,
        # and pushing ~1800 full extraction blobs through XCom would bloat
        # it for no benefit (the actual data already lives in results.csv
        # and json_path on shared storage).
        append_csv_row(csv_path, {
            "filename": file_name, "record_index": idx, "page_range": page_range,
            "patient_label": patient_label, "status": status,
            "missing_pages": ",".join(str(p) for p in missing_pages) or None,
            "json_path": json_path, "extracted_json": json.dumps(extracted),
        })
        return {"ok": True, "filename": file_name, "record_index": idx, "page_range": page_range, "status": status}

    except Exception as e:
        log.exception("Failed to extract record %d (pages %s) of %s", idx, page_range, file_name)
        with STATE_LOCK:
            state["records"][key] = {
                "page_range": page_range, "status": "failed", "error": str(e),
            }
            save_state(state)
        return {
            "ok": False, "filename": file_name, "file_id": file_id,
            "record_index": idx, "page_range": page_range, "error": str(e),
        }


# --------------------------------------------------------------------------
# PER-FILE PIPELINE -- this whole function becomes ONE Airflow task instance
# per file via dynamic task mapping (see the DAG definition below).
# --------------------------------------------------------------------------
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
        log.info("%s already marked done in a previous run -- confirming it's out of the source folder.", file_name)
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
            log.info("%s: reusing %d cached page(s) from a previous run -- skipping download.",
                      file_name, total_pages_known)
            page_results = [{"page": i, "markdown": pages_cached[str(i)]} for i in range(1, total_pages_known + 1)]
        else:
            log.info("Downloading %s (%s)", file_name, file_id)
            download_file(service, file_id, local_path)
            page_results = parse_pdf_per_page_resumable(local_path, tmp_dir, state)

        records = group_pages_into_records(page_results)
        log.info("%s: %d page(s) grouped into %d record(s)", file_name, len(page_results), len(records))

        with ThreadPoolExecutor(max_workers=RECORD_WORKERS) as record_executor:
            record_futures = {
                record_executor.submit(
                    _process_one_record, file_name, file_id, idx, record_pages,
                    schema, json_output_dir, csv_path, save_json, state,
                ): idx
                for idx, record_pages in enumerate(records, start=1)
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
            state["records"].get(str(i), {}).get("status") == "done" for i in range(1, len(records) + 1)
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


# --------------------------------------------------------------------------
# AIRFLOW DAG
# --------------------------------------------------------------------------
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
        return list_source_files(service, SOURCE_FOLDER_ID)

    @task(
        pool="landingai_api_pool",  # create with: airflow pools set landingai_api_pool 8 "..."
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
        return process_one_file(service, file_meta, schema, DEST_FOLDER_ID, JSON_OUTPUT_DIR, csv_path)

    @task
    def summarize_run(results: List[List[Dict[str, Any]]]) -> None:
        flat = [r for sub in results for r in sub]
        total = len(flat)
        ok = sum(1 for r in flat if r.get("ok"))
        failed = [r for r in flat if not r.get("ok")]
        log.info("Run complete. %s/%s records succeeded this run.", ok, total)
        if failed:
            log.warning("Failures (will be retried automatically next run):")
            for f in failed:
                log.warning("  - %s record %s (pages %s): %s",
                            f.get("filename"), f.get("record_index"), f.get("page_range"), f.get("error"))

    files = list_files()
    per_file_results = process_file.expand(file_meta=files)
    summarize_run(per_file_results)


gdrive_landingai_extract()