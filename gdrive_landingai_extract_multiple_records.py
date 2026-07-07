#!/usr/bin/env python3
"""
gdrive_landingai_extract.py

Standalone Python script (no Airflow) that:

  1. Lists PDF files in a Google Drive source folder.
  2. Downloads each PDF.
  3. Splits the PDF into individual ONE-PAGE PDFs and sends each page as its
     own request to the LandingAI ADE Parse API (instead of sending the whole
     multi-page PDF in one call). This is the "per page, not per PDF" change:
     each page gets the model's full attention and its own retry budget, so a
     messy/handwritten page late in the document can no longer cause content
     from other pages to be dropped or truncated.
  4. IMPORTANT -- based on inspecting a real sample (B6.pdf): these Siaya
     Medical Camp scans are NOT one patient per PDF. They are batches of many
     patients' 4-page outpatient consultation forms scanned back-to-back into
     a single file (in the sample, 80 pages = 20 different patients, 4 pages
     each). Running ADE Extract once on the whole file -- which is what the
     original DAG did -- can only ever fill in ONE record's worth of the
     schema, so 19 out of 20 patients' data was silently discarded. That is
     almost certainly the "missing a lot of information" you were seeing.
     To fix this, after per-page parsing the script detects each new form's
     start page and groups pages into per-patient records, then runs ADE
     Extract ONCE PER RECORD. A single-patient PDF just becomes a file with
     one record, so this also works for normal documents. Detecting a new
     patient's start page combines TWO independent signals so a single
     OCR-garbled header can't silently merge two patients into one record:
     a keyword score against the letterhead/header text (needs several
     keywords to match, not one exact phrase), plus a "Patient Name" field
     fingerprint -- if a page asserts a different patient name than the one
     currently tracked, it's treated as a new record even if the header
     itself wasn't recognized at all. See the RECORD SPLITTING section
     further down for details.
  5. Saves one JSON file per extracted record to a local folder, moves the
     source PDF to a "done" Drive folder once every record in it has
     succeeded, and appends one row per record to a results CSV as soon as
     that record is done.

NOTE ON JSON STORAGE: this used to upload each record's JSON to a Drive
  folder, but a service account has no Drive storage quota of its own -- it
  can only create new files inside a Shared Drive (or via OAuth domain-wide
  delegation impersonating a real user). Since neither was set up, every
  upload failed with `storageQuotaExceeded`. JSON output is now written to a
  local folder (LANDING_JSON_OUTPUT_DIR) instead. If you later move the
  source/dest folders to a Shared Drive, it's easy to add a Drive upload back
  in -- ask and I'll wire it up.

RESUMABILITY:
  Every file's progress is checkpointed to a small JSON state file under
  LANDING_STATE_DIR (default ./state/<drive_file_id>.json): which pages have
  already been parsed (with their markdown cached, so a rerun never re-parses
  a page it already has), and which records have already been extracted and
  uploaded. If the script is interrupted or a record/page fails, rerunning it
  picks up exactly where it left off -- already-done pages and records are
  skipped, only the missing/failed ones are retried. A file is only moved out
  of the source folder once every one of its records is done.

NON-FATAL PAGE/PAGE FAILURES:
  A page that parses successfully but comes back with no text (a blank scan,
  e.g. the blank verso side of a form) is a normal outcome, not an error --
  it's cached as-is and the pipeline moves on. A page that genuinely can't be
  parsed even after all retries is logged (not raised): the rest of the
  file's pages, and any records that don't depend on that page, keep being
  processed normally. A record that DOES depend on a still-broken page is
  extracted anyway with whatever content is available, saved to the CSV/JSON
  as usual, but flagged with status "done_with_missing_pages" instead of
  "done" -- which means it automatically gets re-extracted (with the
  now-complete content) on a future run once that page finally succeeds. The
  file itself is only moved to the "done" Drive folder once every record is
  fully "done" with no missing pages left.

CONTINUOUS CSV OUTPUT:
  Instead of collecting everything in memory and writing the CSV once at the
  end, each record's row is appended to LANDING_OUTPUT_DIR/results.csv (with
  an immediate flush + fsync) the moment that record finishes. Open the CSV
  in a spreadsheet app or `tail -f` it while the script is running to watch
  progress in real time. The CSV is cumulative across runs, matching the
  state directory: rows are never duplicated because already-done records are
  skipped on rerun. If you ever need to regenerate the CSV purely from what's
  already been extracted (no API calls), run with --rebuild-csv.

IMPORTANT NOTE ON THE MODEL:
  You asked to switch the parse model to "dpt-3". As of July 2026, LandingAI
  has not released a DPT-3 model -- their documented parse models are DPT-2
  (and DPT-2 mini), with `dpt-2-latest` always pointing at the newest, most
  accurate snapshot (currently dpt-2-20260410, which specifically improved
  cell-level text capture and column alignment in tables/forms -- relevant
  here since these forms are full of hand-filled table cells). This script
  uses `dpt-2-latest` for that reason. If Landing AI ships a dpt-3 model
  later, just change PARSE_MODEL below (or the LANDINGAI_PARSE_MODEL env var)
  -- nothing else needs to change.
  Source: https://docs.landing.ai/ade/ade-parse-models

ALSO FIXED: the original script sent `Authorization: Basic <key>`. LandingAI's
  API reference (https://docs.landing.ai/api-reference/tools/ade-parse) uses
  `Authorization: Bearer <key>`. That's corrected below.

ABOUT schema.json: the schema.json you shared was for neonatal discharge
  notes, but the sample document (B6.pdf) is a general adult "Siaya Medical
  Camp Outpatient Clinical Consultation Form" -- a completely different
  layout (demographics, vitals, chief complaints, exam, diagnosis, Rx,
  doctor's notes, lab results). The schema.json in this folder has been
  rewritten to match that real form. If some of your other PDFs really are
  neonatal notes, keep the two schemas separate and pass whichever one
  applies with --schema.

Install dependencies:
    pip install -r requirements.txt

Required environment variables:
    VA_API_KEY                     LandingAI API key (required)
    GDRIVE_SOURCE_FOLDER_ID        Drive folder to read PDFs from
    GDRIVE_DEST_FOLDER_ID          Drive folder to move processed PDFs to
    GCP_SA_TYPE, GCP_SA_PROJECT_ID, GCP_SA_PRIVATE_KEY_ID, GCP_SA_PRIVATE_KEY,
    GCP_SA_CLIENT_EMAIL, GCP_SA_CLIENT_ID, GCP_SA_TOKEN_URI
                                    Google service-account credentials

Optional environment variables:
    SCHEMA_PATH                    Path to schema.json (default ./schema.json)
    LANDING_OUTPUT_DIR             Where results.csv is written (default ./output)
    LANDING_JSON_OUTPUT_DIR        Where per-record JSON files are written (default ./output/json)
    LANDING_STATE_DIR              Where per-file checkpoint state is written (default ./state)
    LANDINGAI_PARSE_MODEL          Default: dpt-2-latest
    LANDINGAI_EXTRACT_MODEL        Default: extract-latest
    PAGE_PARSE_WORKERS             Parallel page-parse requests per file (default 4)
    FILE_WORKERS                   Parallel files processed at once (default 1)
    MAX_RETRIES                    Retries per API call before failing (default 4)
    RECORD_START_MARKERS           Comma-separated lowercase keywords/phrases checked
                                    against each page's header text (default: "outpatient
                                    clinical consultation form,patient demographics,
                                    siaya medical camp,chronic diseases society")
    RECORD_START_MIN_MATCHES       How many of the keywords above must appear on a page
                                    for the header signal to count it as a new record's
                                    start (default 2). A patient-name-change fingerprint
                                    is also checked independently -- see RECORD SPLITTING
                                    in the code for details.

Usage:
    python gdrive_landingai_extract.py
    python gdrive_landingai_extract.py --dry-run
    python gdrive_landingai_extract.py --source-folder-id XXXX --schema ./schema.json
    python gdrive_landingai_extract.py --rebuild-csv     # rebuild results.csv from state only, no API calls
    python gdrive_landingai_extract.py --reset-state      # wipe checkpoints and start every file over

Testing a single file:
    python gdrive_landingai_extract.py --dry-run                     # list every file's id + name first
    python gdrive_landingai_extract.py --file-name scan3_20260630134309.pdf
    python gdrive_landingai_extract.py --file-id 1AbCDeFGhijKLmnoPQRstuv
"""

from __future__ import annotations

import argparse
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
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from pypdf import PdfReader, PdfWriter

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

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

# LandingAI expects a Bearer token, not Basic auth (the original script had
# this wrong -- see module docstring).
LANDING_HEADERS = {"Authorization": f"Bearer {VA_API_KEY}"}

# See the module docstring: there is no dpt-3 model as of July 2026.
# dpt-2-latest is Landing AI's most accurate/most current parsing model.
PARSE_MODEL = os.getenv("LANDINGAI_PARSE_MODEL", "dpt-2-latest")
EXTRACT_MODEL = os.getenv("LANDINGAI_EXTRACT_MODEL", "extract-latest")

OUTPUT_DIR = os.getenv("LANDING_OUTPUT_DIR", "./output")
JSON_OUTPUT_DIR = os.getenv("LANDING_JSON_OUTPUT_DIR", "./output/json")
STATE_DIR = os.getenv("LANDING_STATE_DIR", "./state")
SCHEMA_PATH = os.getenv("SCHEMA_PATH", "./schema.json")

SERVICE_ACCOUNT_FILE = os.getenv("GDRIVE_SERVICE_ACCOUNT_FILE", "./gdrive-sa.json")

PAGE_PARSE_WORKERS = int(os.getenv("PAGE_PARSE_WORKERS", "4"))
FILE_WORKERS = int(os.getenv("FILE_WORKERS", "1"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
RETRY_BACKOFF_SECONDS = 5
REQUEST_TIMEOUT = 180

# Record-boundary detection uses TWO independent signals (see
# group_pages_into_records / is_record_start_page):
#   1. A keyword score against the printed header/letterhead text -- a page
#      is treated as a new record's start if enough of these keywords show
#      up, rather than requiring one exact phrase to survive OCR intact.
#   2. A patient-name fingerprint -- if a page's "Patient Name" field value
#      differs from the name tracked for the current record, that page is
#      treated as a new record's start even if the header text itself wasn't
#      recognized at all.
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
# How many of the keywords above must appear on a page for the header signal
# alone to count it as a new record's start. Lower = more sensitive to
# garbled OCR but more prone to false splits; higher = the opposite.
RECORD_START_MIN_KEYWORD_MATCHES = max(
    1, min(int(os.getenv("RECORD_START_MIN_MATCHES", "2")), len(RECORD_START_MARKERS))
)

# Guards concurrent writes from multiple threads (page-parse workers within a
# file, and/or multiple files in flight at once).
STATE_LOCK = threading.Lock()
CSV_LOCK = threading.Lock()

CSV_FIELDS = ["filename", "record_index", "page_range", "patient_label", "status", "missing_pages", "json_path", "extracted_json"]


# --------------------------------------------------------------------------
# GOOGLE DRIVE HELPERS
# --------------------------------------------------------------------------
def write_service_account_file(path: str = SERVICE_ACCOUNT_FILE) -> None:
    """Materialize the service-account JSON from individual env vars."""
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


def get_file_by_id(service, file_id: str) -> Dict[str, Any]:
    """Fetch a single file's metadata by its Drive file ID -- used by
    --file-id to test/reprocess one specific file without listing (or being
    limited to) the whole source folder."""
    return service.files().get(fileId=file_id, fields="id, name, parents, mimeType").execute()


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
    """Write a record's extracted JSON to a local folder.

    We used to upload this to Drive, but a service account has no storage
    quota of its own -- it can only create files inside a Shared Drive, or
    via OAuth domain-wide delegation impersonating a real user. Since this
    Drive setup uses neither, every create-file call failed with
    `storageQuotaExceeded`. Writing locally sidesteps that entirely; the
    source PDFs are still moved between your existing Drive folders (which
    only reassigns parents on files that already exist, so it doesn't touch
    the service account's quota).
    """
    os.makedirs(output_dir, exist_ok=True)
    local_path = os.path.join(output_dir, filename)
    tmp_path = local_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(json_data, f, indent=2)
    os.replace(tmp_path, local_path)  # atomic -- no half-written JSON on disk
    return local_path


# --------------------------------------------------------------------------
# RETRY HELPER
# --------------------------------------------------------------------------
def call_with_retry(fn, *args, max_retries: int = MAX_RETRIES, **kwargs):
    """Retry transient failures. Raises (does not swallow) after exhausting
    retries -- we never want to silently continue with missing content."""
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
#
# {
#   "file_id": ..., "filename": ..., "total_pages": 80, "file_done": false,
#   "pages": {"1": "<markdown>", "2": "<markdown>", ...},
#   "records": {
#       "1": {"page_range": "1-4", "status": "done", "patient_label": ...,
#             "json_path": "./output/json/....json", "extracted_json": {...}},
#       "2": {"page_range": "5-8", "status": "failed", "error": "..."}
#   }
# }
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
    os.replace(tmp_path, path)  # atomic on POSIX -- avoids a half-written state file


# --------------------------------------------------------------------------
# CSV -- appended to immediately, one row per completed record
# --------------------------------------------------------------------------
def get_csv_path(output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    return os.path.join(output_dir, "results.csv")


def append_csv_row(csv_path: str, row: Dict[str, Any]) -> None:
    with CSV_LOCK:
        file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerow({k: row.get(k) for k in CSV_FIELDS})
            f.flush()
            os.fsync(f.fileno())


def rebuild_csv_from_state(output_dir: str, state_dir: str) -> str:
    """Regenerate results.csv purely from checkpoint state, with no API
    calls. Useful if the CSV was deleted/corrupted but the extractions are
    already safely recorded in the state directory."""
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
    """Parse one page. Never raises: a page that is genuinely blank (a scan's
    blank verso, for example) is a normal, successful result with empty
    markdown -- it must not be treated as a failure. A page that truly can't
    be parsed after all retries is reported back as an error instead of
    raising, so one bad page can't take down the whole file."""

    def _do_parse():
        with open(page_path, "rb") as fh:
            resp = requests.post(
                "https://api.va.landing.ai/v1/ade/parse",
                headers=LANDING_HEADERS,
                files={"document": (page_path.name, fh, "application/pdf")},
                data={"model": PARSE_MODEL},
                timeout=REQUEST_TIMEOUT,
            )
        # 206 = "some pages failed to parse". For a single-page request this
        # means THE page failed -- treat it as an error so it gets retried
        # instead of silently producing a blank/partial result.
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
        # Exhausted all retries -- log it and hand back an error marker
        # instead of raising, so the rest of the file can keep going.
        log.error("Page %d could not be parsed after %d attempt(s): %s", page_num, MAX_RETRIES, e)
        return {"page": page_num, "markdown": None, "error": str(e)}

    markdown = data.get("markdown", "")
    if not markdown.strip():
        # A successful parse with no text is a normal outcome for a blank
        # page -- not an error, and not something to retry.
        log.info("Page %d parsed successfully but has no text (likely a blank page).", page_num)
    return {"page": page_num, "markdown": markdown, "error": None}


def parse_pdf_per_page_resumable(
    pdf_local_path: Path, work_dir: Path, state: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Parse each page independently, skipping any page whose markdown is
    already cached in `state` from a previous run. Newly parsed pages are
    written into `state` (and persisted to disk) as soon as each one
    completes. A page that fails after all retries is logged and recorded in
    `state["page_errors"]` (so it stays eligible for a retry on the next
    run) but does NOT stop the other pages in this file from being parsed --
    it's filled in with a placeholder for this run so grouping/extraction can
    still proceed on everything that IS available."""
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
                    # Defensive -- parse_single_page shouldn't raise anymore,
                    # but if something unexpected slips through, log it and
                    # keep going rather than aborting the whole file.
                    log.error("Unexpected error parsing page %d: %s", page_num, e)
                    result = {"page": page_num, "markdown": None, "error": str(e)}

                with STATE_LOCK:
                    if result.get("error") is not None:
                        state["page_errors"][str(page_num)] = result["error"]
                        # Deliberately NOT cached into state["pages"], so the
                        # next run retries this exact page.
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
# RECORD SPLITTING
#
# The sample document (B6.pdf) showed that these scans are batches of many
# patients' forms, ~4 pages each, concatenated into one PDF. Each new
# patient's form starts with a printed header/letterhead ("OUTPATIENT
# CLINICAL CONSULTATION FORM", "1. Patient Demographics", "Siaya Medical
# Camp", "Chronic Diseases Society"). Relying on ONE exact phrase to survive
# OCR/parsing intact is risky -- a single garbled word on the header of just
# one page silently merges two different patients into one record, and
# because the extraction schema is a flat single-patient object, one of them
# quietly loses data with no visible error. To make this harder to miss, two
# independent signals are combined:
#
#   1. Keyword score: count how many of RECORD_START_MARKERS show up
#      (independently) on a page. Needing RECORD_START_MIN_KEYWORD_MATCHES
#      of them, rather than one exact phrase, tolerates a keyword or two
#      being OCR-mangled while still requiring real topical evidence.
#   2. Patient-name fingerprint: if a page's "Patient Name" field has a
#      different value than the name currently tracked for the record in
#      progress, that page is treated as a new record's start even if the
#      header text was not recognized at all. Continuation pages (exam,
#      doctor's notes, labs) don't carry a "Patient Name" field in this form,
#      so this fires almost exclusively on genuine new-patient pages.
# --------------------------------------------------------------------------
def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower())


def is_record_start_page(markdown: str) -> bool:
    normalized = _normalize(markdown)
    matches = sum(1 for marker in RECORD_START_MARKERS if _normalize(marker) in normalized)
    return matches >= RECORD_START_MIN_KEYWORD_MATCHES


_PATIENT_NAME_PATTERNS = [
    # Markdown table row: | Patient Name | AKINYI IRENE ANYANGO OKOTH |
    re.compile(r"^\s*\|\s*patient\s*name\s*\|\s*([^|\n]{2,80}?)\s*\|", re.IGNORECASE | re.MULTILINE),
    # A dedicated "Patient Name: X" line. Anchored to the whole line (not
    # `.search()` anywhere in the text) so a sentence that merely mentions
    # "Patient Name" mid-paragraph in doctor's notes doesn't get mistaken
    # for a real field and grab a run of trailing prose as the "name".
    re.compile(r"^\s*patient\s*name\s*[:\-]\s*(.{2,80}?)\s*$", re.IGNORECASE | re.MULTILINE),
]


def extract_patient_name(markdown: str) -> Optional[str]:
    """Best-effort pull of the 'Patient Name' field value from a page's
    markdown, used only as a cross-check for record boundaries -- not fed
    into the schema extraction itself."""
    for pattern in _PATIENT_NAME_PATTERNS:
        m = pattern.search(markdown)
        if m:
            name = m.group(1).strip(" :|-")
            word_count = len(name.split())
            # Guard against matching the column header itself (a
            # "Field | Details" row), OCR noise, or a long run of prose that
            # isn't actually a name (real names are a handful of words).
            if name and 1 <= word_count <= 6 and _normalize(name) not in ("details", "field"):
                return name
    return None


def group_pages_into_records(page_results: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    if not page_results:
        return []

    start_indices = set(
        i for i, p in enumerate(page_results) if is_record_start_page(p["markdown"])
    )

    # Second pass: fingerprint-based fallback. Walk the pages in order and
    # track the patient name for the record currently in progress; if a page
    # asserts a different name and wasn't already caught by the header
    # signal, treat it as a new record boundary too.
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

    # Fallback: if nothing matched at all (e.g. OCR of the header AND the
    # patient name were both too garbled), treat the whole file as a single
    # record rather than dropping everything.
    if not start_indices:
        log.warning(
            "No record-start signal found on any page -- treating the whole "
            "file as a single record."
        )
        return [page_results]

    # Anything before the first detected start page is a leading fragment
    # (e.g. a scan that begins mid-form). Keep it attached to the first
    # record rather than discarding it.
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
# PER-FILE PIPELINE (resumable + incremental CSV)
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

        any_failed = False
        for idx, record_pages in enumerate(records, start=1):
            key = str(idx)
            page_numbers = [p["page"] for p in record_pages]
            page_range = f"{min(page_numbers)}-{max(page_numbers)}"

            existing = state["records"].get(key)
            if existing and existing.get("status") == "done":
                log.info("%s record %d (pages %s) already done -- skipping.", file_name, idx, page_range)
                continue  # already in the CSV from a previous run; nothing more to do

            # If any page in this record hasn't been successfully parsed yet
            # (permanent-for-now failure), we still extract whatever we DO
            # have -- so progress isn't blocked -- but mark the record as
            # "done_with_missing_pages" rather than "done" so it gets
            # automatically re-extracted (with the now-complete content) once
            # that page eventually succeeds on a later run.
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
                    any_failed = True
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

                row = {
                    "filename": file_name,
                    "record_index": idx,
                    "page_range": page_range,
                    "patient_label": patient_label,
                    "status": status,
                    "missing_pages": ",".join(str(p) for p in missing_pages) or None,
                    "json_path": json_path,
                    "extracted_json": json.dumps(extracted),
                }
                append_csv_row(csv_path, row)  # visible immediately, this record is done
                results.append({"ok": True, **row})

            except Exception as e:
                any_failed = True
                log.exception("Failed to extract record %d (pages %s) of %s", idx, page_range, file_name)
                with STATE_LOCK:
                    state["records"][key] = {
                        "page_range": page_range, "status": "failed", "error": str(e),
                    }
                    save_state(state)
                results.append({
                    "ok": False, "filename": file_name, "file_id": file_id,
                    "record_index": idx, "page_range": page_range, "error": str(e),
                })

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
# SUMMARY
# --------------------------------------------------------------------------
def summarize(results: List[Dict[str, Any]], csv_path: str) -> None:
    total = len(results)
    ok = sum(1 for r in results if r.get("ok"))
    failed = [r for r in results if not r.get("ok")]

    log.info("Done this run. Results CSV (cumulative): %s", csv_path)
    log.info("Records newly extracted this run: %s/%s succeeded", ok, total)
    if failed:
        log.warning("Failures (will be retried automatically next run):")
        for f in failed:
            log.warning(
                "  - %s record %s (pages %s): %s",
                f.get("filename"), f.get("record_index"), f.get("page_range"), f.get("error"),
            )


# --------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------
def main():
    global STATE_DIR
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-folder-id", default=SOURCE_FOLDER_ID)
    parser.add_argument("--dest-folder-id", default=DEST_FOLDER_ID)
    parser.add_argument("--schema", default=SCHEMA_PATH)
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    parser.add_argument("--json-output-dir", default=JSON_OUTPUT_DIR,
                         help="Local folder to write each record's JSON to (default ./output/json)")
    parser.add_argument("--state-dir", default=STATE_DIR)
    parser.add_argument("--no-json-output", action="store_true",
                         help="Skip writing the extracted JSON files locally")
    parser.add_argument("--dry-run", action="store_true",
                         help="List files that would be processed and exit")
    parser.add_argument("--file-id", default=None,
                         help="Only process this one Drive file ID (bypasses the source-folder listing entirely)")
    parser.add_argument("--file-name", default=None,
                         help="Only process file(s) matching this exact name within the source folder")
    parser.add_argument("--reset-state", action="store_true",
                         help="Delete all checkpoint state first, so every file is reprocessed from scratch")
    parser.add_argument("--rebuild-csv", action="store_true",
                         help="Regenerate results.csv from existing state only (no API calls) and exit")
    args = parser.parse_args()
    STATE_DIR = args.state_dir

    if args.reset_state:
        if os.path.exists(STATE_DIR):
            shutil.rmtree(STATE_DIR)
        log.info("Cleared state directory %s -- next run reprocesses everything.", STATE_DIR)
        if not args.rebuild_csv:
            return

    if args.rebuild_csv:
        rebuild_csv_from_state(args.output_dir, STATE_DIR)
        return

    with open(args.schema, "r") as f:
        schema = json.load(f)

    service = get_drive_service()

    if args.file_id:
        files = [get_file_by_id(service, args.file_id)]
        log.info("Targeting a single file by id: %s (%s)", args.file_id, files[0].get("name"))
    else:
        files = list_source_files(service, args.source_folder_id)
        if args.file_name:
            matched = [f for f in files if f["name"] == args.file_name]
            if not matched:
                log.warning("No file named %r found in the source folder -- nothing to do.", args.file_name)
            files = matched

    if args.dry_run:
        for f in files:
            print(f"{f['id']}  {f['name']}")
        return

    csv_path = get_csv_path(args.output_dir)
    log.info("Appending progress to %s as each record finishes.", csv_path)
    log.info("Writing per-record JSON files to %s", args.json_output_dir)

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=FILE_WORKERS) as executor:
        futures = {
            executor.submit(
                process_one_file, service, f, schema,
                args.dest_folder_id, args.json_output_dir, csv_path,
                not args.no_json_output,
            ): f
            for f in files
        }
        for future in as_completed(futures):
            results.extend(future.result())

    summarize(results, csv_path)


if __name__ == "__main__":
    main()