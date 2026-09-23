"""
dags/siaya_gdrive_landingai_extract.py

Airflow port of the standalone script `gdrive_landingai_extract_multiple_records.py`
(repo root). Pulls "Siaya Medical Camp" PDF scans from a Google Drive folder,
parses each PDF PAGE BY PAGE with LandingAI ADE Parse (never the whole
multi-page PDF in one call), groups pages into per-patient records (these
scans are batches of many patients' forms scanned back-to-back -- see
group_pages_into_records() below), runs LandingAI ADE Extract ONCE PER
RECORD against schema.json's Siaya outpatient-form schema, and files the
source PDF into a "done" Drive folder once every record in it is complete.

WHY A SEPARATE DAG FROM dags/document_pipeline.py (gdrive_landingai_extract)
  This repo already has an Airflow DAG (dags/document_pipeline.py, dag_id
  "gdrive_landingai_extract") built from a LATER, more elaborate revision of
  this same script -- it adds a 4-signal boundary detector ("Page N of M"
  parsing, a model-based boundary classifier, fuzzy name matching, per-record
  length fallback). THIS dag intentionally mirrors the simpler two-signal
  splitter (header keyword score + patient-name fingerprint) from
  gdrive_landingai_extract_multiple_records.py only, per the task that asked
  for it. Before enabling both, note they will race for the same Drive
  source/dest folders and would double-process files if run concurrently --
  most likely dags/document_pipeline.py's DAG (the more robust splitter)
  should stay the one that's scheduled, and this file kept as a reference /
  disabled. See this DAG's run report for the full assessment.

WHAT CHANGED FROM THE STANDALONE SCRIPT
  - Google Drive service-account credentials: read from the Airflow Variable
    GOOGLE_SA_JSON (a single JSON string -- the full contents of the SA key
    file), matching this repo's existing convention (see `_gsheet_client()`
    in dags/v3_api_to_snowflake_raw.py for the gspread equivalent), instead
    of a local `gdrive-sa.json` file written from individual GCP_SA_* env
    vars.
  - LandingAI API key: read from the Airflow Variable VA_API_KEY instead of
    an OS environment variable.
  - Per-file checkpoint state, per-record extracted JSON, and the results
    CSV all move from local disk to S3 (bucket `collabmedbucket`, prefix
    `raw/siaya_landingai`), so progress survives worker restarts/redeploys
    and is visible from any Airflow worker, not just whichever host happened
    to run a given task:
        state:        s3://collabmedbucket/raw/siaya_landingai/state/<file_id>.json
        record JSON:  s3://collabmedbucket/raw/siaya_landingai/json/<file>_record<NN>_p<range>.json
        per-file CSV: s3://collabmedbucket/raw/siaya_landingai/results/<file_id>.csv
        merged CSV:   s3://collabmedbucket/raw/siaya_landingai/results/results.csv
    Each mapped file-task rewrites its OWN small CSV shard (all of that
    file's records, freshly regenerated from its state) rather than trying
    to append to one shared CSV from multiple concurrent tasks/workers --
    S3 has no atomic append, so sharding per file sidesteps that entirely.
    A final task merges the shards into one results.csv for convenience.
  - schema.json's contents are embedded below as SCHEMA (a Python dict)
    instead of read from a local file path, so the DAG has no dependency on
    a file living at a particular path relative to wherever Airflow was
    started.
  - Dedup/resume: no separate "already processed" Variable is needed. Each
    run lists whatever PDFs are still in GDRIVE_SOURCE_FOLDER_ID; a file is
    only moved to GDRIVE_DEST_FOLDER_ID once every record in it is fully
    "done", so already-finished files simply stop appearing in the source
    listing on the next run (matching the standalone script's behaviour).
    Within a file, the S3 state blob makes reruns resume from the last
    successfully parsed page / extracted record, exactly like the local
    state/<file_id>.json file did.
  - Orchestration: classic PythonOperator + .partial().expand() (this repo's
    house style -- see dags/v3_api_to_snowflake_raw.py) instead of the
    standalone script's ThreadPoolExecutor-across-files and argparse CLI.
    Page-level parallelism *within* one file's task still uses a small
    ThreadPoolExecutor, same as the original script.

Airflow Variables required:
  GOOGLE_SA_JSON            Google service-account credentials, full JSON as one string
  GDRIVE_SOURCE_FOLDER_ID   Drive folder to read Siaya PDFs from
  GDRIVE_DEST_FOLDER_ID     Drive folder to move fully-processed PDFs to
  VA_API_KEY                LandingAI API key

Airflow Connections required:
  aws_default                AWS credentials for the S3Hook (state/JSON/CSV sink)

Env vars: none required.

Output sink: S3 (bucket collabmedbucket, prefix raw/siaya_landingai) -- see
  "WHAT CHANGED" above. No Snowflake load is wired up here; add a follow-on
  DAG/task (COPY INTO, mirroring v3_api_to_snowflake_raw.py) if these records
  need to land in Snowflake too.
"""
from __future__ import annotations

import csv
import json
import logging
import re
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from pypdf import PdfReader, PdfWriter

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

DAG_ID = "siaya_gdrive_landingai_extract"

# ── Google Drive ─────────────────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]

# ── LandingAI ADE ────────────────────────────────────────────────────────
PARSE_URL = "https://api.va.landing.ai/v1/ade/parse"
EXTRACT_URL = "https://api.va.landing.ai/v1/ade/extract"
# dpt-2-latest is LandingAI's most accurate/most current parse model as of
# the original script's writing (no dpt-3 model exists yet) -- see
# gdrive_landingai_extract_multiple_records.py's module docstring.
PARSE_MODEL = "dpt-2-latest"
EXTRACT_MODEL = "extract-latest"

MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 5
REQUEST_TIMEOUT = 180
PAGE_PARSE_WORKERS = 3

# ── S3 sink (replaces LANDING_STATE_DIR / LANDING_OUTPUT_DIR / LANDING_JSON_OUTPUT_DIR) ──
S3_CONN_ID = "aws_default"
S3_BUCKET = "collabmedbucket"
S3_PREFIX = "raw/siaya_landingai"

STATE_LOCK = threading.Lock()

CSV_FIELDS = [
    "filename", "file_id", "record_index", "page_range", "patient_label",
    "status", "missing_pages", "json_s3_uri", "extracted_json",
]

# ── Record-boundary detection (unchanged from gdrive_landingai_extract_multiple_records.py) ──
# These Siaya scans are batches of many patients' 4-page outpatient
# consultation forms scanned back-to-back into a single PDF, not one patient
# per file. Two independent signals decide where a new patient's record
# starts, so a single OCR-garbled header can't silently merge two patients
# into one record:
#   1. A keyword score against the printed header/letterhead text -- needs
#      RECORD_START_MIN_KEYWORD_MATCHES of these markers, not one exact
#      phrase, to tolerate OCR noise.
#   2. A "Patient Name" field fingerprint -- if a page asserts a different
#      patient name than the one currently tracked, it's treated as a new
#      record even if the header itself wasn't recognized at all.
DEFAULT_RECORD_START_MARKERS = [
    "outpatient clinical consultation form",
    "patient demographics",
    "siaya medical camp",
    "chronic diseases society",
]
RECORD_START_MARKERS = [m.strip().lower() for m in DEFAULT_RECORD_START_MARKERS]
RECORD_START_MIN_KEYWORD_MATCHES = 2

# ── Extraction schema (embedded from schema.json -- Siaya adult outpatient
#    consultation form; NOT the neonatal-note schema in dags/jsons/schema.json,
#    which is a different, older form layout) ──────────────────────────────
SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Siaya Medical Camp outpatient clinical consultation form extraction",
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "camp_file_number": {"type": ["string", "null"], "description": "The camp-assigned file/reference number handwritten or stamped near the top of the form (e.g. '0296', '056339'), distinct from Patient ID / File No in the demographics table."},
        "visit_date": {"type": ["string", "null"], "description": "Date of visit exactly as written next to 'Date:'."},
        "time_in": {"type": ["string", "null"]},
        "time_out": {"type": ["string", "null"]},
        "attending_clinician": {"type": ["string", "null"]},
        "patient_name": {"type": ["string", "null"]},
        "date_of_birth": {"type": ["string", "null"], "description": "Date of birth exactly as written; may sometimes only be a year."},
        "phone_number": {"type": ["string", "null"]},
        "address": {"type": ["string", "null"]},
        "patient_id_file_no": {"type": ["string", "null"], "description": "Value in the 'Patient ID / File No.' field."},
        "age": {"type": ["string", "null"]},
        "sex": {"type": ["string", "null"]},
        "emergency_contact": {"type": ["string", "null"]},
        "blood_pressure": {"type": ["string", "null"], "description": "BP reading exactly as written, including units, e.g. '149/83 mmHg'. If more than one reading (e.g. a repeat) is recorded, include all of them."},
        "heart_rate": {"type": ["string", "null"]},
        "oxygen_saturation": {"type": ["string", "null"]},
        "temperature": {"type": ["string", "null"]},
        "respiratory_rate": {"type": ["string", "null"]},
        "weight": {"type": ["string", "null"]},
        "height": {"type": ["string", "null"]},
        "body_mass_index": {"type": ["string", "null"]},
        "blood_sugar": {"type": ["string", "null"], "description": "RBS/FBS value exactly as written, including units."},
        "nurses_triage_notes": {"type": ["string", "null"]},
        "chief_complaints": {"type": "array", "items": {"type": "string"}, "description": "Each numbered chief complaint and its duration, as separate entries."},
        "history_of_present_illness": {"type": ["string", "null"]},
        "past_medical_surgical_history": {"type": ["string", "null"]},
        "known_allergies": {"type": ["string", "null"], "description": "Whether 'None Known' was checked, or the allergy detail written next to 'Yes'."},
        "current_medications": {"type": ["string", "null"]},
        "general_appearance": {"type": ["string", "null"]},
        "systemic_exam_cvs": {"type": ["string", "null"], "description": "Cardiovascular exam finding; note if the 'normal' checkbox was ticked with no further text."},
        "systemic_exam_resp": {"type": ["string", "null"]},
        "systemic_exam_git": {"type": ["string", "null"]},
        "systemic_exam_cns": {"type": ["string", "null"]},
        "systemic_exam_msk_skin": {"type": ["string", "null"]},
        "provisional_diagnosis": {"type": "array", "items": {"type": "string"}, "description": "Each numbered provisional/confirmed diagnosis line."},
        "lab_imaging_investigations_ordered": {"type": "array", "items": {"type": "string"}},
        "treatment_plan_prescriptions": {"type": "array", "items": {"type": "string"}, "description": "Each numbered prescription/treatment line, exactly as written (drug, dose, frequency, duration)."},
        "follow_up_referral_notes": {"type": ["string", "null"], "description": "Review-in period, referral destination, and whether admission was marked as required."},
        "doctors_notes": {"type": ["string", "null"], "description": "Free-text narrative from the 'Doctor's Notes' page(s), such as case summaries, impression, and plan."},
        "lab_results": {"type": "array", "items": {"type": "string"}, "description": "Each distinct lab/imaging result as its own entry, e.g. 'CXR AI Finding: Normal chest, TB Negative', 'Hb - 13.2 g/dL', 'RBS - 4.7 mmol/L (3.5-7.8)', 'MRDT - Negative', 'U/A: pH 6.6, SG 1.020, Leucocytes ++, Blood ++++, Protein ++'."},
        "raw_notes": {"type": ["string", "null"], "description": "Any other handwritten content that does not fit cleanly into the fields above."},
    },
    "required": [
        "camp_file_number", "visit_date", "time_in", "time_out", "attending_clinician",
        "patient_name", "date_of_birth", "phone_number", "address", "patient_id_file_no",
        "age", "sex", "emergency_contact", "blood_pressure", "heart_rate",
        "oxygen_saturation", "temperature", "respiratory_rate", "weight", "height",
        "body_mass_index", "blood_sugar", "nurses_triage_notes", "chief_complaints",
        "history_of_present_illness", "past_medical_surgical_history", "known_allergies",
        "current_medications", "general_appearance", "systemic_exam_cvs",
        "systemic_exam_resp", "systemic_exam_git", "systemic_exam_cns",
        "systemic_exam_msk_skin", "provisional_diagnosis",
        "lab_imaging_investigations_ordered", "treatment_plan_prescriptions",
        "follow_up_referral_notes", "doctors_notes", "lab_results", "raw_notes",
    ],
}


# ═══════════════════════════════════════════════════════════════════════
# Credentials / clients (Variable.get() called lazily, inside tasks --
# not at module top level, so parsing this DAG file never hits the
# metadata DB or requires Airflow Variables to already exist).
# ═══════════════════════════════════════════════════════════════════════
def get_drive_service():
    sa_info = json.loads(Variable.get("GOOGLE_SA_JSON"))
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


def landing_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {Variable.get('VA_API_KEY')}"}


def _s3() -> S3Hook:
    return S3Hook(aws_conn_id=S3_CONN_ID)


# ═══════════════════════════════════════════════════════════════════════
# Google Drive helpers
# ═══════════════════════════════════════════════════════════════════════
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
            q=query, fields="nextPageToken, files(id, name, parents, mimeType)",
            pageSize=1000, pageToken=page_token,
        ).execute()
        files.extend(results.get("files", []))
        page_token = results.get("nextPageToken")
        if not page_token:
            break
    log.info("Found %s PDF file(s) in source folder", len(files))
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
            fileId=file_id, addParents=add_parent_id, removeParents=previous_parents,
            fields="id, parents",
        ).execute()
    except HttpError as error:
        raise RuntimeError(f"Failed to move file {file_id}: {error}")


# ═══════════════════════════════════════════════════════════════════════
# S3-backed checkpoint state, per-record JSON, per-file CSV shard
# (replaces the standalone script's LANDING_STATE_DIR / *_OUTPUT_DIR)
# ═══════════════════════════════════════════════════════════════════════
def _state_key(file_id: str) -> str:
    return f"{S3_PREFIX}/state/{file_id}.json"


def load_state(s3: S3Hook, file_id: str, filename: str) -> Dict[str, Any]:
    key = _state_key(file_id)
    if s3.check_for_key(key, bucket_name=S3_BUCKET):
        state = json.loads(s3.read_key(key, bucket_name=S3_BUCKET))
    else:
        state = {
            "file_id": file_id, "filename": filename, "total_pages": None,
            "file_done": False, "pages": {}, "page_errors": {}, "records": {},
        }
    state.setdefault("pages", {})
    state.setdefault("page_errors", {})
    state.setdefault("records", {})
    return state


def save_state(s3: S3Hook, state: Dict[str, Any]) -> None:
    s3.load_string(
        json.dumps(state), _state_key(state["file_id"]), bucket_name=S3_BUCKET, replace=True,
    )


def save_record_json(s3: S3Hook, filename: str, json_data: dict) -> str:
    key = f"{S3_PREFIX}/json/{filename}"
    s3.load_string(json.dumps(json_data, indent=2), key, bucket_name=S3_BUCKET, replace=True)
    return f"s3://{S3_BUCKET}/{key}"


def write_csv_shard(s3: S3Hook, file_id: str, rows: List[Dict[str, Any]]) -> None:
    """Rewrite this file's CSV shard from scratch each run (all of its
    records, freshly derived from state). S3 has no atomic append, so each
    file owns its own shard instead of many tasks appending to one CSV."""
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_FIELDS)
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k) for k in CSV_FIELDS})
    s3.load_string(buf.getvalue(), f"{S3_PREFIX}/results/{file_id}.csv", bucket_name=S3_BUCKET, replace=True)


# ═══════════════════════════════════════════════════════════════════════
# Retry helper
# ═══════════════════════════════════════════════════════════════════════
def call_with_retry(fn, *args, max_retries: int = MAX_RETRIES, **kwargs):
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except (requests.exceptions.RequestException, RuntimeError) as e:
            last_exc = e
            wait = RETRY_BACKOFF_SECONDS * attempt
            log.warning("Attempt %s/%s failed (%s) -- retrying in %ss", attempt, max_retries, e, wait)
            time.sleep(wait)
    raise RuntimeError(f"All {max_retries} attempts failed: {last_exc}")


# ═══════════════════════════════════════════════════════════════════════
# PDF splitting + per-page parse (resumable via S3 state)
# ═══════════════════════════════════════════════════════════════════════
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


def parse_single_page(page_path: Path, page_num: int, headers: Dict[str, str]) -> Dict[str, Any]:
    """Parse one page. Never raises: a genuinely blank page (blank verso of a
    form) is a normal successful result with empty markdown. A page that
    can't be parsed after all retries is reported back as an error instead
    of raising, so one bad page can't take down the whole file's task."""

    def _do_parse():
        with open(page_path, "rb") as fh:
            resp = requests.post(
                PARSE_URL, headers=headers,
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
    pdf_local_path: Path, work_dir: Path, state: Dict[str, Any], s3: S3Hook, headers: Dict[str, str],
) -> List[Dict[str, Any]]:
    reader = PdfReader(str(pdf_local_path))
    total_pages = len(reader.pages)

    with STATE_LOCK:
        state["total_pages"] = total_pages
        save_state(s3, state)

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
            futures = {executor.submit(parse_single_page, p, i, headers): i for i, p in targets.items()}
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
                    save_state(s3, state)

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


# ═══════════════════════════════════════════════════════════════════════
# Record splitting (unchanged logic from gdrive_landingai_extract_multiple_records.py)
# ═══════════════════════════════════════════════════════════════════════
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
    return "\n\n".join(f"<!-- page {r['page']} -->\n{r['markdown']}" for r in record_pages)


# ═══════════════════════════════════════════════════════════════════════
# Extract
# ═══════════════════════════════════════════════════════════════════════
def extract_structured_data(markdown_content: str, schema: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
    def _do_extract():
        resp = requests.post(
            EXTRACT_URL, headers=headers,
            files={"markdown": ("document.md", markdown_content, "text/markdown")},
            data={"schema": json.dumps(schema), "model": EXTRACT_MODEL},
            timeout=REQUEST_TIMEOUT,
        )
        if not resp.ok:
            raise RuntimeError(f"extract failed: {resp.status_code} - {resp.text}")
        return resp.json()

    return call_with_retry(_do_extract)


# ═══════════════════════════════════════════════════════════════════════
# Per-file pipeline -- one Airflow mapped task instance per file
# ═══════════════════════════════════════════════════════════════════════
def process_one_file(service, file_meta: Dict[str, Any], dest_folder_id: str, s3: S3Hook) -> List[Dict[str, Any]]:
    file_id = file_meta["id"]
    file_name = file_meta["name"]
    mime_type = file_meta.get("mimeType", "")
    headers = landing_headers()

    if mime_type != "application/pdf":
        return [{"ok": False, "filename": file_name, "file_id": file_id, "record_index": 0,
                  "page_range": None, "error": f"Unsupported mimeType: {mime_type}"}]

    state = load_state(s3, file_id, file_name)

    if state.get("file_done"):
        log.info("%s already marked done in a previous run -- confirming it's out of the source folder.", file_name)
        try:
            move_file(service, file_id, dest_folder_id)
        except Exception:
            pass
        return []

    tmp_dir = Path(tempfile.mkdtemp(prefix="siaya_landingai_"))
    local_path = tmp_dir / file_name
    results: List[Dict[str, Any]] = []
    csv_rows: List[Dict[str, Any]] = []

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
            page_results = parse_pdf_per_page_resumable(local_path, tmp_dir, state, s3, headers)

        records = group_pages_into_records(page_results)
        log.info("%s: %d page(s) grouped into %d record(s)", file_name, len(page_results), len(records))

        for idx, record_pages in enumerate(records, start=1):
            key = str(idx)
            page_numbers = [p["page"] for p in record_pages]
            page_range = f"{min(page_numbers)}-{max(page_numbers)}"

            existing = state["records"].get(key)
            if existing and existing.get("status") == "done":
                log.info("%s record %d (pages %s) already done -- skipping.", file_name, idx, page_range)
                csv_rows.append({
                    "filename": file_name, "file_id": file_id, "record_index": idx,
                    "page_range": existing.get("page_range"), "patient_label": existing.get("patient_label"),
                    "status": existing.get("status"),
                    "missing_pages": ",".join(str(p) for p in existing.get("missing_pages", [])) or None,
                    "json_s3_uri": existing.get("json_s3_uri"),
                    "extracted_json": json.dumps(existing.get("extracted_json")),
                })
                continue

            page_errors = state.get("page_errors", {})
            missing_pages = [p["page"] for p in record_pages if str(p["page"]) in page_errors]

            try:
                md = record_markdown(record_pages)
                extracted = extract_structured_data(md, SCHEMA, headers)

                json_filename = f"{Path(file_name).stem}_record{idx:02d}_p{page_range}.json"
                json_s3_uri = save_record_json(s3, json_filename, extracted)

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
                        "page_range": page_range, "status": status, "patient_label": patient_label,
                        "missing_pages": missing_pages, "json_s3_uri": json_s3_uri,
                        "extracted_json": extracted,
                    }
                    save_state(s3, state)

                row = {
                    "filename": file_name, "file_id": file_id, "record_index": idx,
                    "page_range": page_range, "patient_label": patient_label, "status": status,
                    "missing_pages": ",".join(str(p) for p in missing_pages) or None,
                    "json_s3_uri": json_s3_uri, "extracted_json": json.dumps(extracted),
                }
                csv_rows.append(row)
                results.append({"ok": True, **{k: v for k, v in row.items() if k != "extracted_json"}})

            except Exception as e:
                log.exception("Failed to extract record %d (pages %s) of %s", idx, page_range, file_name)
                with STATE_LOCK:
                    state["records"][key] = {"page_range": page_range, "status": "failed", "error": str(e)}
                    save_state(s3, state)
                csv_rows.append({
                    "filename": file_name, "file_id": file_id, "record_index": idx,
                    "page_range": page_range, "patient_label": None, "status": "failed",
                    "missing_pages": None, "json_s3_uri": None, "extracted_json": None,
                })
                results.append({"ok": False, "filename": file_name, "file_id": file_id,
                                 "record_index": idx, "page_range": page_range, "error": str(e)})

        write_csv_shard(s3, file_id, csv_rows)

        all_done = len(records) > 0 and all(
            state["records"].get(str(i), {}).get("status") == "done" for i in range(1, len(records) + 1)
        )
        if all_done:
            with STATE_LOCK:
                state["file_done"] = True
                save_state(s3, state)
            move_file(service, file_id, dest_folder_id)
        else:
            log.warning("%s left in source folder: at least one record still needs a retry.", file_name)

        return results

    except Exception as e:
        log.exception("Failed to process %s", file_name)
        return [{"ok": False, "filename": file_name, "file_id": file_id,
                  "record_index": 0, "page_range": None, "error": str(e)}]

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════
# Airflow task callables
# ═══════════════════════════════════════════════════════════════════════
def list_files_task(**context) -> List[Dict[str, Any]]:
    """Returns a list of {"file_meta": {...}} dicts for .expand(op_kwargs=...)."""
    service = get_drive_service()
    source_folder_id = Variable.get("GDRIVE_SOURCE_FOLDER_ID")
    files = list_source_files(service, source_folder_id)
    return [{"file_meta": f} for f in files]


def process_file_task(file_meta: Dict[str, Any], **context) -> List[Dict[str, Any]]:
    service = get_drive_service()
    dest_folder_id = Variable.get("GDRIVE_DEST_FOLDER_ID")
    s3 = _s3()
    return process_one_file(service, file_meta, dest_folder_id, s3)


def merge_results_task(**context) -> None:
    """Merge every file's CSV shard under raw/siaya_landingai/results/ into
    one results.csv, and log a summary of this run's outcomes pulled from
    process_file's XCom returns."""
    ti = context["ti"]
    per_file_results = ti.xcom_pull(task_ids="process_file") or []
    flat = [r for sub in (per_file_results or []) for r in (sub or [])]
    total = len(flat)
    ok = sum(1 for r in flat if r.get("ok"))
    failed = [r for r in flat if not r.get("ok")]
    log.info("Run complete. %s/%s records succeeded this run.", ok, total)
    if failed:
        log.warning("Failures (will be retried automatically next run):")
        for f in failed:
            log.warning("  - %s record %s (pages %s): %s",
                        f.get("filename"), f.get("record_index"), f.get("page_range"), f.get("error"))

    s3 = _s3()
    shard_prefix = f"{S3_PREFIX}/results/"
    shard_keys = [
        k for k in (s3.list_keys(bucket_name=S3_BUCKET, prefix=shard_prefix) or [])
        if k.endswith(".csv") and not k.endswith("/results.csv")
    ]
    merged = StringIO()
    writer = csv.DictWriter(merged, fieldnames=CSV_FIELDS)
    writer.writeheader()
    row_count = 0
    for key in shard_keys:
        content = s3.read_key(key, bucket_name=S3_BUCKET)
        reader = csv.DictReader(StringIO(content))
        for row in reader:
            writer.writerow({k: row.get(k) for k in CSV_FIELDS})
            row_count += 1
    s3.load_string(merged.getvalue(), f"{shard_prefix}results.csv", bucket_name=S3_BUCKET, replace=True)
    log.info("Merged %d shard(s) into s3://%s/%sresults.csv (%d row(s) total).",
              len(shard_keys), S3_BUCKET, shard_prefix, row_count)


# ═══════════════════════════════════════════════════════════════════════
# DAG definition
# ═══════════════════════════════════════════════════════════════════════
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    max_active_tasks=4,
    tags=["siaya", "gdrive", "landingai", "extract"],
    doc_md=__doc__,
) as dag:

    t_list = PythonOperator(
        task_id="list_files",
        python_callable=list_files_task,
    )

    t_process = PythonOperator.partial(
        task_id="process_file",
        python_callable=process_file_task,
        execution_timeout=timedelta(minutes=45),
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(op_kwargs=t_list.output)

    t_merge = PythonOperator(
        task_id="merge_results",
        python_callable=merge_results_task,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_list >> t_process >> t_merge
