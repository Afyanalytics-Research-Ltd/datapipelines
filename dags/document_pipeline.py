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

PARAMS (Trigger DAG w/ config, or `airflow dags trigger -c '<json>'`)
  Every param is optional; a blank one falls back to the environment
  variable / Airflow Variable the DAG used before, so a plain trigger behaves
  exactly as it always did.

  Schemas -- each one inline OR from a file; inline wins when both are set:
    schema                 extraction schema as a JSON object or JSON text
                           (a plain path typed here is read as a file too)
    schema_path            JSON file for the above (default $SCHEMA_PATH,
                           ./dags/jsons/schema.json)
    boundary_schema        the per-page record-boundary schema, same rules
    boundary_schema_path   JSON file for the above; both blank = the built-in
                           BOUNDARY_SCHEMA. Keep its field names
                           (is_first_page_of_record, patient_name, patient_id,
                           page_number_in_record, total_pages_in_record) --
                           the splitter reads those; extra fields are fine.
    Relative paths resolve against the working dir, the repo root and dags/.
    Changing a schema re-runs what it affects: a new boundary schema drops the
    cached page_meta, a new extraction schema re-extracts records (and files
    already marked done) instead of skipping them.

  Which files:  source_folder_id / dest_folder_id (blank = the
    GDRIVE_*_FOLDER_ID Variables), file_ids (just these Drive files),
    name_contains, max_files (0 = all), move_processed (false = leave files in
    the source folder -- handy for test runs).
  Splitting:    record_start_markers, record_start_min_matches,
    max_pages_per_record, boundary_score_threshold,
    boundary_only_when_ambiguous.
  Throughput:   page_parse_workers, boundary_workers, record_workers,
    max_retries. (The file-level cap, FILE_CONCURRENCY / the pool, is fixed
    when the DAG file is parsed and is not a param.)
  Output:       output_dir, json_output_dir, state_dir, save_json,
    parse_model, extract_model.
  Snowflake:    push_to_snowflake (default on; env PUSH_TO_SNOWFLAKE=0 turns
    the default off), snowflake_database (blank = $SNOWFLAKE_DATABASE),
    snowflake_schema (blank = the extraction schema's title, e.g.
    SIMPLE_NEONATAL_NOTE_EXTRACTION), snowflake_table (blank =
    EXTRACTED_RECORDS).

SNOWFLAKE LOAD (task load_snowflake, after every file task)
  The schema and table are created when missing and used as they are when
  present. A new table gets record metadata columns (RECORD_KEY, FILE_ID,
  FILENAME, PAGE_RANGE, ... EXTRACTION variant, LOADED_AT) plus one column per
  top-level extraction-schema property (string -> VARCHAR, integer -> NUMBER,
  number -> FLOAT, boolean -> BOOLEAN, object/array -> VARIANT). An existing
  table only gains the columns it lacks; values are cast to whatever type its
  columns already have, and unparseable values land as NULL (the raw value is
  always in EXTRACTION). Each load replaces every row of the run's files
  (DELETE by FILE_ID + INSERT, one transaction), so reruns never duplicate.
  It reads the records from the state files, so files finished in earlier
  runs are (re)loaded too.

Install dependencies:
    pip install "apache-airflow>=2.3" requests pypdf google-api-python-client \
                google-auth pendulum

One-time setup:
    airflow pools set landingai_api_pool 8 "Concurrent LandingAI file tasks"

Trigger a run:
    airflow dags trigger gdrive_landingai_extract
    airflow dags trigger gdrive_landingai_extract -c '{
        "schema_path": "dags/jsons/schema.json",
        "boundary_schema": {"type": "object", "properties": {...}},
        "name_contains": "siaya", "max_files": 2, "move_processed": false}'
"""

from __future__ import annotations

import csv
import hashlib
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
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from pypdf import PdfReader, PdfWriter

try:  # keep the --inspect CLI usable without Airflow installed
    import pendulum
    try:
        from airflow.sdk import Param, Variable, dag, get_current_context, task
    except ImportError:  # Airflow 2.x
        from airflow.decorators import dag, task
        from airflow.models import Variable
        from airflow.models.param import Param
        from airflow.operators.python import get_current_context
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

DAGS_DIR = Path(__file__).resolve().parent

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

# ---- Snowflake load ------------------------------------------------------
PUSH_TO_SNOWFLAKE = os.getenv("PUSH_TO_SNOWFLAKE", "1") == "1"
SNOWFLAKE_DEFAULT_SCHEMA = "LANDINGAI_EXTRACT"
SNOWFLAKE_DEFAULT_TABLE = "EXTRACTED_RECORDS"

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
def _state_path(file_id: str, state_dir: str = STATE_DIR) -> Path:
    return Path(state_dir) / f"{file_id}.json"


def load_state(file_id: str, filename: str, state_dir: str = STATE_DIR) -> Dict[str, Any]:
    path = _state_path(file_id, state_dir)
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


def save_state(state: Dict[str, Any], state_dir: str = STATE_DIR) -> None:
    os.makedirs(state_dir, exist_ok=True)
    path = _state_path(state["file_id"], state_dir)
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


def parse_single_page(
    page_path: Path, page_num: int, cfg: Optional[PipelineConfig] = None
) -> Dict[str, Any]:
    cfg = cfg or DEFAULT_CONFIG

    def _do_parse():
        with open(page_path, "rb") as fh:
            resp = requests.post(
                "https://api.va.landing.ai/v1/ade/parse",
                headers=landing_headers(),
                files={"document": (page_path.name, fh, "application/pdf")},
                data={"model": cfg.parse_model},
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
        data = call_with_retry(_do_parse, max_retries=cfg.max_retries)
    except Exception as e:
        log.error("Page %d could not be parsed after %d attempt(s): %s", page_num, cfg.max_retries, e)
        return {"page": page_num, "markdown": None, "error": str(e)}

    markdown = data.get("markdown", "")
    if not markdown.strip():
        log.info("Page %d parsed successfully but has no text (likely a blank page).", page_num)
    return {"page": page_num, "markdown": markdown, "error": None}


def parse_pdf_per_page_resumable(
    pdf_local_path: Path, work_dir: Path, state: Dict[str, Any],
    cfg: Optional[PipelineConfig] = None,
) -> List[Dict[str, Any]]:
    cfg = cfg or DEFAULT_CONFIG
    reader = PdfReader(str(pdf_local_path))
    total_pages = len(reader.pages)

    with STATE_LOCK:
        state["total_pages"] = total_pages
        state.setdefault("page_errors", {})
        save_state(state, cfg.state_dir)

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
        with ThreadPoolExecutor(max_workers=cfg.page_parse_workers) as executor:
            futures = {executor.submit(parse_single_page, p, i, cfg): i for i, p in targets.items()}
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
                    save_state(state, cfg.state_dir)

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

# The boundary-schema fields detect_boundaries() reads. A custom boundary
# schema may add fields, but one without these gives the splitter nothing.
BOUNDARY_FIELDS = (
    "is_first_page_of_record",
    "patient_name",
    "patient_id",
    "page_number_in_record",
    "total_pages_in_record",
)


# ==========================================================================
# RUN CONFIG -- defaults from the environment, overridden per run by params
# ==========================================================================
def json_hash(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode("utf-8")).hexdigest()[:16]


@dataclass
class PipelineConfig:
    """Everything one run can tune. The defaults are the environment-derived
    constants above; a DAG run overrides them with config_from_params()."""

    schema: Dict[str, Any] = field(default_factory=dict)
    boundary_schema: Dict[str, Any] = field(default_factory=lambda: BOUNDARY_SCHEMA)
    source_folder_id: Optional[str] = None
    dest_folder_id: Optional[str] = None
    file_ids: List[str] = field(default_factory=list)
    name_contains: str = ""
    max_files: int = 0
    move_processed: bool = True
    save_json: bool = True
    output_dir: str = OUTPUT_DIR
    json_output_dir: str = JSON_OUTPUT_DIR
    state_dir: str = STATE_DIR
    parse_model: str = PARSE_MODEL
    extract_model: str = EXTRACT_MODEL
    page_parse_workers: int = PAGE_PARSE_WORKERS
    boundary_workers: int = BOUNDARY_WORKERS
    record_workers: int = RECORD_WORKERS
    max_retries: int = MAX_RETRIES
    record_start_markers: List[str] = field(default_factory=lambda: list(RECORD_START_MARKERS))
    record_start_min_matches: int = RECORD_START_MIN_KEYWORD_MATCHES
    max_pages_per_record: int = MAX_PAGES_PER_RECORD
    max_plausible_run: int = MAX_PLAUSIBLE_RUN
    boundary_only_when_ambiguous: bool = BOUNDARY_ONLY_WHEN_AMBIGUOUS
    boundary_score_threshold: int = BOUNDARY_SCORE_THRESHOLD
    push_to_snowflake: bool = PUSH_TO_SNOWFLAKE
    snowflake_database: str = ""
    snowflake_schema: str = ""
    snowflake_table: str = ""

    def __post_init__(self) -> None:
        self.record_start_markers = [
            m.strip().lower() for m in self.record_start_markers if m and m.strip()
        ]
        self.record_start_min_matches = max(
            1, min(int(self.record_start_min_matches), len(self.record_start_markers) or 1)
        )

    @property
    def schema_hash(self) -> str:
        return json_hash(self.schema)

    @property
    def boundary_schema_hash(self) -> str:
        return json_hash(self.boundary_schema)

    @property
    def snowflake_target(self) -> Tuple[str, str, str]:
        """(database, schema, table). Blank params: $SNOWFLAKE_DATABASE, the
        extraction schema's title, EXTRACTED_RECORDS."""
        database = sf_identifier(self.snowflake_database or os.getenv("SNOWFLAKE_DATABASE", ""))
        if not database:
            raise ValueError("snowflake_database: not set and no SNOWFLAKE_DATABASE in the environment")
        schema = (
            sf_identifier(self.snowflake_schema)
            or sf_identifier(str(self.schema.get("title") or ""))
            or SNOWFLAKE_DEFAULT_SCHEMA
        )
        return database, schema, sf_identifier(self.snowflake_table) or SNOWFLAKE_DEFAULT_TABLE

    def describe(self) -> str:
        boundary = "built-in" if self.boundary_schema == BOUNDARY_SCHEMA else "custom"
        return (
            f"schema {len(self.schema.get('properties', {}))} field(s) #{self.schema_hash}; "
            f"boundary schema {boundary} #{self.boundary_schema_hash}; "
            f"files {self.file_ids or (self.name_contains and repr(self.name_contains)) or 'all'}"
            f"{f' (max {self.max_files})' if self.max_files else ''}; "
            f"move_processed={self.move_processed}; markers={self.record_start_markers} "
            f"(min {self.record_start_min_matches}); max_pages_per_record={self.max_pages_per_record}; "
            f"threshold={self.boundary_score_threshold}; workers "
            f"{self.page_parse_workers}/{self.boundary_workers}/{self.record_workers}; "
            f"state_dir={self.state_dir}; output_dir={self.output_dir}; "
            f"snowflake={self._snowflake_label()}"
        )

    def _snowflake_label(self) -> str:
        if not self.push_to_snowflake:
            return "off"
        try:
            return ".".join(self.snowflake_target)
        except ValueError as e:
            return f"not configured ({e})"


DEFAULT_CONFIG = PipelineConfig()
DEFAULT_BOUNDARY_SCHEMA_HASH = json_hash(BOUNDARY_SCHEMA)


def _resolve_path(path: str) -> Path:
    p = Path(path).expanduser()
    if p.is_absolute():
        return p
    for base in (Path.cwd(), DAGS_DIR.parent, DAGS_DIR):
        if (base / p).exists():
            return base / p
    return p


def load_json_param(
    blob: Any, path: Optional[str], label: str,
    default: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """A JSON schema given inline (`blob`: a dict, or JSON text) or read from
    a file (`path`). The blob wins when both are set; a blob that is plain
    text rather than JSON is taken as a file path. Falls back to `default`."""
    value: Any = None
    if isinstance(blob, str):
        text = blob.strip()
        if text[:1] in ("{", "[", '"'):
            try:
                value = json.loads(text)
                if isinstance(value, str):  # double-encoded JSON text
                    value = json.loads(value)
            except json.JSONDecodeError as e:
                raise ValueError(f"{label}: not valid JSON ({e})") from None
        elif text:
            path = text
    elif blob:
        value = blob

    if value is None and path and str(path).strip():
        resolved = _resolve_path(str(path).strip())
        if not resolved.is_file():
            raise ValueError(
                f"{label}: file not found: {path} "
                f"(looked in {Path.cwd()}, {DAGS_DIR.parent} and {DAGS_DIR})"
            )
        with open(resolved, "r", encoding="utf-8") as f:
            try:
                value = json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"{label}: {resolved} is not valid JSON ({e})") from None
        log.info("%s: loaded from %s", label, resolved)

    if value is None:
        if default is None:
            raise ValueError(f"{label}: give it inline as JSON, or as a JSON file path")
        return default
    if not isinstance(value, dict) or not isinstance(value.get("properties"), dict):
        raise ValueError(f"{label}: expected a JSON schema object with a 'properties' map")
    return value


def _as_list(value: Any) -> List[str]:
    """A list param, or the same typed as one comma/newline separated string."""
    if value is None:
        return []
    if isinstance(value, str):
        value = re.split(r"[,\n]", value)
    return [str(v).strip() for v in value if str(v).strip()]


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "y", "on")
    return bool(value)


def config_from_params(params: Optional[Dict[str, Any]]) -> PipelineConfig:
    """Build the run config from DAG params. Blank params (None / "" / [])
    keep the environment default, so an empty conf is the old behaviour."""
    p = {k: v for k, v in (params or {}).items() if v not in (None, "", [])}
    d = DEFAULT_CONFIG

    def _int(name: str, minimum: int = 0) -> int:
        return max(minimum, int(p.get(name, getattr(d, name))))

    _schema_path = p.get("schema_path", SCHEMA_PATH)
    if not p.get("schema") and _schema_path and not _resolve_path(str(_schema_path)).is_file():
        _fallback = "dags/jsons/siaya_outpatient_schema.json"
        log.warning("schema: file not found: %s — using default schema %s", _schema_path, _fallback)
        _schema_path = _fallback

    cfg = PipelineConfig(
        schema=load_json_param(p.get("schema"), _schema_path, "schema"),
        boundary_schema=load_json_param(
            p.get("boundary_schema"), p.get("boundary_schema_path"), "boundary_schema",
            default=BOUNDARY_SCHEMA,
        ),
        source_folder_id=p.get("source_folder_id"),
        dest_folder_id=p.get("dest_folder_id"),
        file_ids=_as_list(p.get("file_ids")),
        name_contains=str(p.get("name_contains", "")).strip(),
        max_files=_int("max_files"),
        move_processed=_as_bool(p.get("move_processed", d.move_processed)),
        save_json=_as_bool(p.get("save_json", d.save_json)),
        output_dir=p.get("output_dir", d.output_dir),
        json_output_dir=p.get("json_output_dir", d.json_output_dir),
        state_dir=p.get("state_dir", d.state_dir),
        parse_model=p.get("parse_model", d.parse_model),
        extract_model=p.get("extract_model", d.extract_model),
        page_parse_workers=_int("page_parse_workers", 1),
        boundary_workers=_int("boundary_workers", 1),
        record_workers=_int("record_workers", 1),
        max_retries=_int("max_retries", 1),
        record_start_markers=_as_list(p.get("record_start_markers")) or d.record_start_markers,
        record_start_min_matches=_int("record_start_min_matches", 1),
        max_pages_per_record=_int("max_pages_per_record", 1),
        boundary_only_when_ambiguous=_as_bool(
            p.get("boundary_only_when_ambiguous", d.boundary_only_when_ambiguous)
        ),
        boundary_score_threshold=_int("boundary_score_threshold", 1),
        push_to_snowflake=_as_bool(p.get("push_to_snowflake", d.push_to_snowflake)),
        snowflake_database=str(p.get("snowflake_database", "")).strip(),
        snowflake_schema=str(p.get("snowflake_schema", "")).strip(),
        snowflake_table=str(p.get("snowflake_table", "")).strip(),
    )

    missing = [f for f in BOUNDARY_FIELDS if f not in cfg.boundary_schema["properties"]]
    if missing:
        log.warning(
            "boundary_schema has no %s -- the record splitter reads those fields, so "
            "without them it falls back to the printed page numbers and header markers.",
            ", ".join(missing),
        )
    return cfg


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


def marker_hits(markdown: Optional[str], cfg: Optional[PipelineConfig] = None) -> int:
    cfg = cfg or DEFAULT_CONFIG
    nz = _normalize(markdown)
    return sum(1 for m in cfg.record_start_markers if _normalize(m) in nz)


def is_record_start_page(markdown: Optional[str], cfg: Optional[PipelineConfig] = None) -> bool:
    cfg = cfg or DEFAULT_CONFIG
    return marker_hits(markdown, cfg) >= cfg.record_start_min_matches


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


def parse_page_of_total(
    markdown: Optional[str], cfg: Optional[PipelineConfig] = None
) -> Tuple[Optional[int], Optional[int]]:
    """Return (current, total) from a printed 'Page 2 of 4', else (None, None)."""
    cfg = cfg or DEFAULT_CONFIG
    m = _PAGE_OF_TOTAL.search(markdown or "")
    if not m:
        return None, None
    cur, tot = int(m.group(1)), int(m.group(2))
    if tot < 1 or tot > cfg.max_plausible_run or cur < 1 or cur > tot:
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


def classify_one_page(markdown: str, cfg: Optional[PipelineConfig] = None) -> Dict[str, Any]:
    cfg = cfg or DEFAULT_CONFIG
    if not markdown or not markdown.strip() or is_failed_page(markdown):
        return {}
    try:
        raw = extract_structured_data(markdown, cfg.boundary_schema, cfg) or {}
    except Exception as e:  # never fatal -- deterministic signals still apply
        log.warning("Boundary classification failed: %s", e)
        return {}
    return raw.get("extraction") or {}


def needs_classification(markdown: Optional[str], cfg: Optional[PipelineConfig] = None) -> bool:
    """False when the free signals already settle the page."""
    cfg = cfg or DEFAULT_CONFIG
    if not cfg.boundary_only_when_ambiguous:
        return True
    if is_failed_page(markdown):
        return False
    cur, _ = parse_page_of_total(markdown, cfg)
    if cur is not None:
        return False
    if is_record_start_page(markdown, cfg):
        return False
    return True


def classify_pages_resumable(
    page_results: List[Dict[str, Any]], state: Dict[str, Any],
    cfg: Optional[PipelineConfig] = None,
) -> Dict[str, Dict[str, Any]]:
    """One cached ade/extract call per ambiguous page. Safe to re-run: pages
    already present in state["page_meta"] are never re-sent."""
    cfg = cfg or DEFAULT_CONFIG
    meta: Dict[str, Dict[str, Any]] = state.setdefault("page_meta", {})

    todo = [
        p for p in page_results
        if str(p["page"]) not in meta and needs_classification(p.get("markdown"), cfg)
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
    with ThreadPoolExecutor(max_workers=cfg.boundary_workers) as ex:
        futures = {
            ex.submit(classify_one_page, p.get("markdown") or "", cfg): p["page"] for p in todo
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
                save_state(state, cfg.state_dir)
    return meta


def detect_boundaries(
    page_results: List[Dict[str, Any]], page_meta: Dict[str, Dict[str, Any]],
    cfg: Optional[PipelineConfig] = None,
) -> Tuple[List[int], Dict[int, int]]:
    """Return (sorted start indices, {start_index: score}).

    A printed 'Page N of M' is authoritative and suppresses every other signal
    for the pages it covers."""
    cfg = cfg or DEFAULT_CONFIG
    starts: Dict[int, int] = {}
    covered_to = -1
    last_name: Optional[str] = None
    last_id: Optional[str] = None

    for i, p in enumerate(page_results):
        md = p.get("markdown") or ""
        meta = page_meta.get(str(p["page"])) or {}

        cur, tot = parse_page_of_total(md, cfg)
        if cur is None:
            mc, mt = meta.get("page_number_in_record"), meta.get("total_pages_in_record")
            if isinstance(mc, int) and isinstance(mt, int) and 1 <= mc <= mt <= cfg.max_plausible_run:
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
        hits = marker_hits(md, cfg)
        score += 2 if hits >= cfg.record_start_min_matches else (1 if hits else 0)
        if pid and last_id and _normalize(pid) != _normalize(last_id):
            score += 3
        if name and last_name and not same_person(name, last_name):
            score += 2

        if i == 0:
            starts[i] = max(starts.get(i, 0), score)
        elif score >= cfg.boundary_score_threshold:
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
    cfg: Optional[PipelineConfig] = None,
) -> List[Dict[str, Any]]:
    """Group pages into records.

    Returns a list of DICTS (not page-lists):
        {pages, page_numbers, patient_name_hint, patient_id_hint,
         name_is_carried, split_method, boundary_score}
    """
    cfg = cfg or DEFAULT_CONFIG
    page_meta = page_meta or {}
    n = len(page_results)
    if n == 0:
        return []

    starts, scores = detect_boundaries(page_results, page_meta, cfg)

    # Whole-file fallback: a long document that produced no usable boundary.
    if len(starts) <= 1 and n > cfg.max_pages_per_record:
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
        if len(pages) > cfg.max_pages_per_record:
            log.warning(
                "Record starting at page %s spans %d page(s) (max %d) -- splitting "
                "it per page with carry-forward.",
                pages[0]["page"], len(pages), cfg.max_pages_per_record,
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
def extract_structured_data(
    markdown_content: str, schema: Dict[str, Any], cfg: Optional[PipelineConfig] = None
) -> Dict[str, Any]:
    cfg = cfg or DEFAULT_CONFIG

    def _do_extract():
        resp = requests.post(
            "https://api.va.landing.ai/v1/ade/extract",
            headers=landing_headers(),
            files={"markdown": ("document.md", markdown_content, "text/markdown")},
            data={"schema": json.dumps(schema), "model": cfg.extract_model},
            timeout=REQUEST_TIMEOUT,
        )
        if not resp.ok:
            raise RuntimeError(f"extract failed: {resp.status_code} - {resp.text}")
        return resp.json()

    return call_with_retry(_do_extract, max_retries=cfg.max_retries)


# ==========================================================================
# PER-RECORD EXTRACTION (concurrent within a file)
# ==========================================================================
def _process_one_record(
    file_name: str, file_id: str, idx: int, rec: Dict[str, Any],
    cfg: PipelineConfig, csv_path: str, state: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    key = str(idx)
    page_numbers = rec["page_numbers"]
    page_range = f"{min(page_numbers)}-{max(page_numbers)}"

    # Done only counts for the same pages under the same schema; records
    # saved before schema hashes were kept count as the current schema.
    existing = state["records"].get(key)
    if (
        existing and existing.get("status") == "done"
        and existing.get("page_range") == page_range
        and existing.get("schema_hash", cfg.schema_hash) == cfg.schema_hash
    ):
        log.info("%s record %d (pages %s) already done -- skipping.", file_name, idx, page_range)
        return None

    page_errors = state.get("page_errors", {})
    missing_pages = [p for p in page_numbers if str(p) in page_errors]

    try:
        md = build_record_markdown(rec)
        extracted = extract_structured_data(md, cfg.schema, cfg)
        apply_carry_forward(extracted, rec)

        json_filename = f"{Path(file_name).stem}_record{idx:02d}_p{page_range}.json"
        json_path = None
        if cfg.save_json:
            json_path = save_json_locally(json_filename, extracted, cfg.json_output_dir)

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
                "schema_hash": cfg.schema_hash,
                "extracted_json": extracted,
            }
            save_state(state, cfg.state_dir)

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
            save_state(state, cfg.state_dir)
        return {
            "ok": False, "filename": file_name, "file_id": file_id,
            "record_index": idx, "page_range": page_range, "error": str(e),
        }


# ==========================================================================
# PER-FILE PIPELINE -- one Airflow task instance per file via .expand()
# ==========================================================================
def select_files(service, cfg: PipelineConfig) -> List[Dict[str, Any]]:
    """The run's files: the listed file_ids, or the source folder's PDFs,
    then filtered by name_contains and capped at max_files (in name order)."""
    if cfg.file_ids:
        files = [
            service.files().get(fileId=fid, fields="id, name, parents, mimeType").execute()
            for fid in cfg.file_ids
        ]
    else:
        files = list_source_files(service, cfg.source_folder_id or get_source_folder_id())
    if cfg.name_contains:
        needle = cfg.name_contains.lower()
        files = [f for f in files if needle in f["name"].lower()]
    files.sort(key=lambda f: f["name"])
    if cfg.max_files:
        files = files[: cfg.max_files]
    log.info("Selected %d file(s): %s", len(files), ", ".join(f["name"] for f in files))
    return files


def process_one_file(
    service, file_meta: Dict[str, Any], dest_folder_id: Optional[str],
    cfg: Optional[PipelineConfig] = None,
) -> List[Dict[str, Any]]:
    """dest_folder_id None leaves the file in the source folder."""
    cfg = cfg or DEFAULT_CONFIG
    file_id = file_meta["id"]
    file_name = file_meta["name"]
    mime_type = file_meta.get("mimeType", "")
    csv_path = get_csv_path(cfg.output_dir)

    if mime_type != "application/pdf":
        return [{
            "ok": False, "filename": file_name, "file_id": file_id, "record_index": 0,
            "page_range": None, "error": f"Unsupported mimeType: {mime_type}",
        }]

    state = load_state(file_id, file_name, cfg.state_dir)

    if state.get("file_done"):
        if state.get("schema_hash", cfg.schema_hash) == cfg.schema_hash:
            log.info(
                "%s already marked done in a previous run -- confirming it's out of the source folder.",
                file_name,
            )
            if dest_folder_id:
                try:
                    move_file(service, file_id, dest_folder_id)
                except Exception:
                    pass
            return []
        log.info("%s was done under another schema -- extracting it again.", file_name)
        state["file_done"] = False

    # Page classifications belong to the boundary schema that produced them.
    if state.get("boundary_schema_hash", DEFAULT_BOUNDARY_SCHEMA_HASH) != cfg.boundary_schema_hash:
        if state.get("page_meta"):
            log.info(
                "%s: boundary schema changed -- dropping %d cached page classification(s).",
                file_name, len(state["page_meta"]),
            )
        state["page_meta"] = {}
    state["boundary_schema_hash"] = cfg.boundary_schema_hash

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
            page_results = parse_pdf_per_page_resumable(local_path, tmp_dir, state, cfg)

        page_meta = classify_pages_resumable(page_results, state, cfg)
        records = group_pages_into_records(page_results, page_meta, cfg)
        log.info("%s: %d page(s) -> %s", file_name, len(page_results), record_length_histogram(records))
        with STATE_LOCK:
            # records above this index are left over from an older grouping
            state["record_count"] = len(records)
            save_state(state, cfg.state_dir)

        with ThreadPoolExecutor(max_workers=cfg.record_workers) as record_executor:
            record_futures = {
                record_executor.submit(
                    _process_one_record, file_name, file_id, idx, rec,
                    cfg, csv_path, state,
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
                state["schema_hash"] = cfg.schema_hash
                save_state(state, cfg.state_dir)
            if dest_folder_id:
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
# SNOWFLAKE LOAD -- one table per extraction schema, built from its properties
# ==========================================================================
# Record metadata columns, ahead of one column per extraction-schema property.
# FILE_ID is the reload key: each load replaces every row of the files it
# carries, so reruns and re-extractions never duplicate.
SF_META_COLUMNS: Dict[str, str] = {
    "RECORD_KEY": "VARCHAR",
    "FILE_ID": "VARCHAR",
    "FILENAME": "VARCHAR",
    "RECORD_INDEX": "NUMBER(38,0)",
    "PAGE_RANGE": "VARCHAR",
    "PATIENT_LABEL": "VARCHAR",
    "STATUS": "VARCHAR",
    "SPLIT_METHOD": "VARCHAR",
    "NAME_IS_CARRIED": "BOOLEAN",
    "BOUNDARY_SCORE": "NUMBER(38,0)",
    "MISSING_PAGES": "VARCHAR",
    "SCHEMA_HASH": "VARCHAR",
    "EXTRACTION": "VARIANT",  # the whole extraction, extra keys included
    "LOADED_AT": "TIMESTAMP_NTZ",
}
_JSON_TO_SF = {
    "string": "VARCHAR",
    "integer": "NUMBER(38,0)",
    "number": "FLOAT",
    "boolean": "BOOLEAN",
    "object": "VARIANT",
    "array": "VARIANT",
}
SF_INSERT_BATCH_CHARS = 400_000  # keep each multi-row INSERT well under the statement limit


def sf_identifier(name: str) -> str:
    """An unquoted Snowflake identifier: 'Birth weight (kg)' -> BIRTH_WEIGHT_KG."""
    ident = re.sub(r"[^A-Za-z0-9_]+", "_", name or "").strip("_").upper()
    return f"_{ident}" if ident[:1].isdigit() else ident


def sf_type_for(prop: Dict[str, Any]) -> str:
    types = prop.get("type")
    types = [t for t in (types if isinstance(types, list) else [types]) if t and t != "null"]
    if not types and "enum" in prop:
        return "VARCHAR"
    return _JSON_TO_SF.get(types[0], "VARIANT") if len(types) == 1 else "VARIANT"


def schema_columns(schema: Dict[str, Any]) -> List[Tuple[str, str, str]]:
    """(column, property name, Snowflake type) per top-level property. A
    property whose name collides with a metadata column, or with an earlier
    property after cleaning, gets an F_ prefix / numeric suffix."""
    columns: List[Tuple[str, str, str]] = []
    taken = set(SF_META_COLUMNS)
    for name, prop in schema.get("properties", {}).items():
        col = sf_identifier(name) or "FIELD"
        if col in SF_META_COLUMNS:
            col = f"F_{col}"
        base, n = col, 2
        while col in taken:
            col, n = f"{base}_{n}", n + 1
        taken.add(col)
        columns.append((col, name, sf_type_for(prop if isinstance(prop, dict) else {})))
    return columns


def _sql_str(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def sf_cast(v: str, data_type: str, scale: Optional[int] = None) -> str:
    """SQL turning VARIANT expression `v` into a column of `data_type`
    (INFORMATION_SCHEMA spelling). Unparseable values become NULL rather than
    failing the load; JSON nulls become SQL NULL."""
    t = data_type.upper()
    text = f"IFF(IS_NULL_VALUE({v}), NULL, IFF(IS_VARCHAR({v}), {v}::VARCHAR, TO_JSON({v})))"
    if t == "VARIANT":
        return f"IFF(IS_NULL_VALUE({v}), NULL, {v})"
    if t == "OBJECT":
        return f"IFF(IS_OBJECT({v}), {v}::OBJECT, NULL)"
    if t == "ARRAY":
        return f"IFF(IS_ARRAY({v}), {v}::ARRAY, NULL)"
    if t == "BOOLEAN":
        return f"IFF(IS_BOOLEAN({v}), {v}::BOOLEAN, TRY_TO_BOOLEAN({text}))"
    if t in ("NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT"):
        return f"TRY_TO_NUMBER({text}, 38, {int(scale or 0)})"
    if t in ("FLOAT", "DOUBLE", "REAL", "FLOAT4", "FLOAT8", "DOUBLE PRECISION"):
        return f"TRY_TO_DOUBLE({text})"
    if t == "DATE":
        return f"TRY_TO_DATE({text})"
    if t.startswith("TIMESTAMP"):
        return f"TRY_TO_TIMESTAMP({text})"
    return text


class SnowflakeClient:
    """Key-pair auth from the SNOWFLAKE_* environment, as the other DAGs here
    do (dags/flatten_jsons_schemas.py); password when there is no key file."""

    def __init__(self, database: str):
        import snowflake.connector

        auth: Dict[str, Any] = {}
        key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()
        if key_path:
            auth["private_key_file"] = key_path
        else:
            auth["password"] = os.getenv("SNOWFLAKE_PASSWORD", "")
        if os.getenv("SNOWFLAKE_ROLE", "").strip():
            auth["role"] = os.getenv("SNOWFLAKE_ROLE").strip()
        self._conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER", "").strip(),
            account=os.getenv("SNOWFLAKE_ACCOUNT", "").strip(),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "").strip(),
            database=database,
            **auth,
        )

    def execute(self, sql: str, params: Any = None) -> int:
        log.info("snowflake: %.200s", " ".join(sql.split()))
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount

    def executemany(self, sql: str, rows: List[Tuple[Any, ...]]) -> int:
        with self._conn.cursor() as cur:
            cur.executemany(sql, rows)
            return cur.rowcount

    def fetchall(self, sql: str, params: Any = None) -> List[Tuple[Any, ...]]:
        with self._conn.cursor() as cur:
            return cur.execute(sql, params).fetchall()

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()


def ensure_snowflake_table(
    sf, database: str, schema: str, table: str, columns: List[Tuple[str, str, str]]
) -> Dict[str, Tuple[str, Optional[int]]]:
    """Use the schema and table when they exist, create them when they don't,
    and add any column the table lacks (existing columns keep their types).
    Returns {column: (data_type, numeric_scale)} as Snowflake has it."""
    wanted = {**SF_META_COLUMNS, **{col: typ for col, _, typ in columns}}

    if sf.fetchall(
        f"SELECT 1 FROM {database}.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s", (schema,)
    ):
        log.info("Snowflake schema %s.%s exists -- using it.", database, schema)
    else:
        log.info("Snowflake schema %s.%s not found -- creating it.", database, schema)
        sf.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")

    def _existing() -> Dict[str, Tuple[str, Optional[int]]]:
        rows = sf.fetchall(
            f"SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_SCALE FROM {database}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
            (schema, table),
        )
        return {r[0]: (r[1], r[2]) for r in rows}

    fq = f"{database}.{schema}.{table}"
    existing = _existing()
    if not existing:
        log.info("Creating %s with %d column(s).", fq, len(wanted))
        sf.execute(
            f"CREATE TABLE IF NOT EXISTS {fq} ("
            + ", ".join(f"{col} {typ}" for col, typ in wanted.items())
            + ")"
        )
    else:
        missing = [col for col in wanted if col not in existing]
        log.info(
            "Table %s exists (%d column(s)) -- using it%s.", fq, len(existing),
            f", adding {', '.join(missing)}" if missing else "",
        )
        for col in missing:
            sf.execute(f"ALTER TABLE {fq} ADD COLUMN {col} {wanted[col]}")
        if not missing:
            return existing
    return _existing()


def snowflake_docs(cfg: PipelineConfig, file_ids: List[str]) -> List[Dict[str, Any]]:
    """One document per extracted record of these files, from their state:
    current grouping only, extracted under the current schema."""
    docs: List[Dict[str, Any]] = []
    for file_id in file_ids:
        path = _state_path(file_id, cfg.state_dir)
        if not path.exists():
            continue
        with open(path, "r", encoding="utf-8") as f:
            state = json.load(f)
        count = state.get("record_count")
        for idx_str, rec in sorted(state.get("records", {}).items(), key=lambda kv: int(kv[0])):
            if rec.get("status") not in ("done", "done_with_missing_pages"):
                continue
            if count is not None and int(idx_str) > count:
                continue
            if rec.get("schema_hash", cfg.schema_hash) != cfg.schema_hash:
                continue
            docs.append({
                "record_key": f"{file_id}:{idx_str}",
                "file_id": file_id,
                "filename": state.get("filename"),
                "record_index": int(idx_str),
                "page_range": rec.get("page_range"),
                "patient_label": rec.get("patient_label"),
                "status": rec.get("status"),
                "split_method": rec.get("split_method"),
                "name_is_carried": rec.get("name_is_carried"),
                "boundary_score": rec.get("boundary_score"),
                "missing_pages": ",".join(str(p) for p in rec.get("missing_pages") or []) or None,
                "schema_hash": cfg.schema_hash,
                "extraction": (rec.get("extracted_json") or {}).get("extraction") or {},
            })
    return docs


def _batches(rows: List[str], max_chars: int) -> List[List[str]]:
    out: List[List[str]] = [[]]
    size = 0
    for row in rows:
        if out[-1] and size + len(row) > max_chars:
            out.append([])
            size = 0
        out[-1].append(row)
        size += len(row)
    return [b for b in out if b]


def load_to_snowflake(cfg: PipelineConfig, file_ids: List[str], sf=None) -> Dict[str, Any]:
    """Replace these files' rows in the run's Snowflake table. Rows are staged
    as JSON text in a temporary table, then typed per the target's actual
    columns in one DELETE + INSERT transaction."""
    database, schema, table = cfg.snowflake_target
    fq = f"{database}.{schema}.{table}"
    docs = snowflake_docs(cfg, file_ids)
    if not docs:
        log.info("Snowflake: no extracted records for %d file(s) -- nothing to load.", len(file_ids))
        return {"table": fq, "rows": 0, "files": 0}

    columns = schema_columns(cfg.schema)
    own = sf is None
    sf = sf or SnowflakeClient(database)
    try:
        types = ensure_snowflake_table(sf, database, schema, table, columns)

        tmp = f"{database}.{schema}.LANDINGAI_LOAD_{os.getpid()}_{threading.get_ident()}"
        sf.execute(f"CREATE OR REPLACE TEMPORARY TABLE {tmp} (DOC VARCHAR)")
        for batch in _batches([json.dumps(d, default=str) for d in docs], SF_INSERT_BATCH_CHARS):
            sf.executemany(f"INSERT INTO {tmp} (DOC) VALUES (%s)", [(b,) for b in batch])

        targets: List[Tuple[str, str]] = []
        for col in SF_META_COLUMNS:
            if col == "LOADED_AT":
                targets.append((col, "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ"))
            elif col == "EXTRACTION":
                targets.append((col, sf_cast("d:extraction", *types[col])))
            else:
                targets.append((col, sf_cast(f"d:{col.lower()}", *types[col])))
        for col, name, _ in columns:
            targets.append((col, sf_cast(f"GET(d:extraction, {_sql_str(name)})", *types[col])))

        sf.execute("BEGIN")
        deleted = sf.execute(
            f"DELETE FROM {fq} WHERE FILE_ID IN "
            f"(SELECT DISTINCT PARSE_JSON(DOC):file_id::VARCHAR FROM {tmp})"
        )
        inserted = sf.execute(
            f"INSERT INTO {fq} ({', '.join(c for c, _ in targets)}) "
            f"SELECT {', '.join(e for _, e in targets)} "
            f"FROM (SELECT PARSE_JSON(DOC) AS d FROM {tmp})"
        )
        sf.commit()
        sf.execute(f"DROP TABLE IF EXISTS {tmp}")
    except Exception:
        try:
            sf.rollback()
        except Exception:
            pass
        raise
    finally:
        if own:
            sf.close()

    files = len({d["file_id"] for d in docs})
    log.info(
        "Snowflake: %s -- replaced %d old row(s) with %d row(s) from %d file(s).",
        fq, deleted, inserted, files,
    )
    return {"table": fq, "rows": inserted, "replaced": deleted, "files": files}


# ==========================================================================
# AIRFLOW DAG
# ==========================================================================
JSON_BLOB = ["null", "object", "string"]  # a pasted JSON object, JSON text, or a path


def dag_params() -> Dict[str, Any]:
    """The trigger form. Defaults mirror DEFAULT_CONFIG, so what you see is
    what runs; see PARAMS in the module docstring."""
    d = DEFAULT_CONFIG
    return {
        # -- schemas -------------------------------------------------------
        "schema": Param(
            None, type=JSON_BLOB, title="Extraction schema (JSON)", section="Schemas",
            description="Paste the extraction JSON schema here. Leave empty to read schema_path.",
        ),
        "schema_path": Param(
            SCHEMA_PATH, type="string", title="Extraction schema file", section="Schemas",
            description="JSON file used when the schema above is empty. Relative paths resolve "
                        "against the working dir, the repo root and dags/.",
        ),
        "boundary_schema": Param(
            None, type=JSON_BLOB, title="Boundary schema (JSON)", section="Schemas",
            description="Per-page record-boundary schema. Keep the fields is_first_page_of_record, "
                        "patient_name, patient_id, page_number_in_record, total_pages_in_record. "
                        "Empty = boundary_schema_path, else the built-in one.",
        ),
        "boundary_schema_path": Param(
            "", type=["null", "string"], title="Boundary schema file", section="Schemas",
            description="JSON file used when the boundary schema above is empty.",
        ),
        # -- which files ---------------------------------------------------
        "source_folder_id": Param(
            "", type=["null", "string"], title="Source Drive folder", section="Files",
            description="Empty = the GDRIVE_SOURCE_FOLDER_ID Variable.",
        ),
        "dest_folder_id": Param(
            "", type=["null", "string"], title="Done Drive folder", section="Files",
            description="Empty = the GDRIVE_DEST_FOLDER_ID Variable.",
        ),
        "file_ids": Param(
            [], type=["null", "array"], items={"type": "string"}, title="Only these Drive file ids",
            section="Files", description="One per line. Empty = every PDF in the source folder.",
        ),
        "name_contains": Param(
            "", type=["null", "string"], title="File name contains", section="Files",
            description="Case-insensitive filter on the file name.",
        ),
        "max_files": Param(
            0, type="integer", minimum=0, title="Max files", section="Files",
            description="0 = all. Files are taken in name order.",
        ),
        "move_processed": Param(
            d.move_processed, type="boolean", title="Move finished files", section="Files",
            description="Off = leave them in the source folder (test runs).",
        ),
        # -- record splitting ----------------------------------------------
        "record_start_markers": Param(
            d.record_start_markers, type="array", items={"type": "string"},
            title="Record start markers", section="Record splitting",
            description="Header phrases that open a record, one per line.",
        ),
        "record_start_min_matches": Param(
            d.record_start_min_matches, type="integer", minimum=1,
            title="Markers needed on a first page", section="Record splitting",
        ),
        "max_pages_per_record": Param(
            d.max_pages_per_record, type="integer", minimum=1,
            title="Max pages per record", section="Record splitting",
            description="Longer detected records are split one per page.",
        ),
        "boundary_score_threshold": Param(
            d.boundary_score_threshold, type="integer", minimum=1,
            title="Boundary score threshold", section="Record splitting",
        ),
        "boundary_only_when_ambiguous": Param(
            d.boundary_only_when_ambiguous, type="boolean",
            title="Classify only ambiguous pages", section="Record splitting",
            description="Off = one boundary call per page.",
        ),
        # -- throughput ----------------------------------------------------
        "page_parse_workers": Param(d.page_parse_workers, type="integer", minimum=1, maximum=32,
                                    title="Page parse threads per file", section="Throughput"),
        "boundary_workers": Param(d.boundary_workers, type="integer", minimum=1, maximum=32,
                                  title="Boundary threads per file", section="Throughput"),
        "record_workers": Param(d.record_workers, type="integer", minimum=1, maximum=32,
                                title="Record extract threads per file", section="Throughput"),
        "max_retries": Param(d.max_retries, type="integer", minimum=1, maximum=10,
                             title="API attempts per call", section="Throughput"),
        # -- output --------------------------------------------------------
        "parse_model": Param(d.parse_model, type="string", title="Parse model", section="Output"),
        "extract_model": Param(d.extract_model, type="string", title="Extract model", section="Output"),
        "output_dir": Param(d.output_dir, type="string", title="Output dir (results.csv)", section="Output"),
        "json_output_dir": Param(d.json_output_dir, type="string", title="Per-record JSON dir", section="Output"),
        "state_dir": Param(d.state_dir, type="string", title="State dir", section="Output",
                           description="Checkpoints; a fresh dir re-parses everything."),
        "save_json": Param(d.save_json, type="boolean", title="Write per-record JSON files", section="Output"),
        # -- snowflake -----------------------------------------------------
        "push_to_snowflake": Param(
            d.push_to_snowflake, type="boolean", title="Load records into Snowflake", section="Snowflake",
            description="After extraction, replace the run's files' rows in the table below.",
        ),
        "snowflake_database": Param(
            "", type=["null", "string"], title="Database", section="Snowflake",
            description="Empty = $SNOWFLAKE_DATABASE.",
        ),
        "snowflake_schema": Param(
            "", type=["null", "string"], title="Schema", section="Snowflake",
            description="Created when missing, used as-is when it exists. Empty = the extraction "
                        f"schema's title (e.g. SIMPLE_NEONATAL_NOTE_EXTRACTION), else {SNOWFLAKE_DEFAULT_SCHEMA}.",
        ),
        "snowflake_table": Param(
            "", type=["null", "string"], title="Table", section="Snowflake",
            description=f"Empty = {SNOWFLAKE_DEFAULT_TABLE}. Created from the extraction schema's "
                        "properties when missing; an existing table is kept and only gains missing columns.",
        ),
    }


if AIRFLOW_AVAILABLE:

    @dag(
        dag_id="gdrive_landingai_extract",
        schedule=None,  # triggered manually / on demand -- not a recurring job
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_tasks=max(FILE_CONCURRENCY, 8),
        tags=["landingai", "gdrive", "extraction"],
        doc_md=__doc__,
        params=dag_params(),
    )
    def gdrive_landingai_extract():

        @task
        def list_files() -> List[Dict[str, Any]]:
            # Resolving here fails a bad schema / path before any file task starts.
            cfg = config_from_params(get_current_context()["params"])
            log.info("Run config: %s", cfg.describe())
            return select_files(get_drive_service(), cfg)

        @task(
            pool="landingai_api_pool",  # airflow pools set landingai_api_pool 8 "..."
            max_active_tis_per_dag=FILE_CONCURRENCY,
            retries=2,
            retry_delay=timedelta(minutes=3),
            execution_timeout=timedelta(minutes=45),
        )
        def process_file(file_meta: Dict[str, Any]) -> List[Dict[str, Any]]:
            # Each file task rebuilds the config rather than pulling the
            # schemas through XCom once per file.
            cfg = config_from_params(get_current_context()["params"])
            dest = (cfg.dest_folder_id or get_dest_folder_id()) if cfg.move_processed else None
            return process_one_file(get_drive_service(), file_meta, dest, cfg)

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
                        "Check the record_start_markers and max_pages_per_record params -- "
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

        @task(trigger_rule="all_done", retries=2, retry_delay=timedelta(minutes=2))
        def load_snowflake() -> Dict[str, Any]:
            # all_done: one failed file task must not keep the others out of
            # Snowflake. The file list is pulled here rather than passed in, so
            # a failed list_files means "nothing to load", not a retry loop.
            context = get_current_context()
            files = context["ti"].xcom_pull(task_ids="list_files")
            if not files:
                log.info("No files listed this run -- nothing to load into Snowflake.")
                return {}
            cfg = config_from_params(context["params"])
            if not cfg.push_to_snowflake:
                log.info("push_to_snowflake is off -- skipping the Snowflake load.")
                return {}
            return load_to_snowflake(cfg, [f["id"] for f in files])

        files = list_files()
        per_file_results = process_file.expand(file_meta=files)
        summarize_run(per_file_results)
        per_file_results >> load_snowflake()

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