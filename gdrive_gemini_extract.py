#!/usr/bin/env python3
"""
gdrive_gemini_extract.py

Alternative extraction pipeline for the same Siaya Medical Camp batch PDFs,
using the Gemini API instead of LandingAI ADE -- built specifically to
collapse API round-trips under time pressure.

WHY THIS IS ARCHITECTURALLY FASTER
  The LandingAI pipeline does, per patient record: ~4 page-parse calls (one
  per page of that patient's form) + 1 extract call = ~5 API round-trips.
  For an 80-page/20-patient batch file, that's ~100 calls before the file is
  done.

  Gemini can read a whole PDF (up to 1000 pages) natively in ONE call and
  return structured JSON for every patient in it directly, via Structured
  Outputs (a JSON Schema you provide). So one 80-page/20-patient file becomes
  roughly 2 API calls total: one Files API upload, one generateContent call
  that returns all 20 patients' structured data at once. That's the speed
  win -- not more threads, fewer round-trips per unit of work.

NOTE ON "SUMMARIZE THEN STRUCTURE":
  The original ask was to get a summary and then structure it. That would be
  TWO Gemini calls per file (summarize, then re-parse the summary into
  fields) and, worse, the summary step is a lossy paraphrase -- exactly the
  kind of information loss the whole LandingAI pipeline was hardened against
  ("I do not want it to even miss even a comma"). Structured Outputs lets
  Gemini go straight from the PDF to schema-validated JSON in one call, so
  this script skips the summary step entirely and extracts directly. It's
  both faster (one call, not two) and more accurate (no lossy intermediate
  text) than doing it in two steps.

ACCURACY IS UNVERIFIED -- VALIDATE BEFORE TRUSTING THE WHOLE BACKLOG TO THIS:
  LandingAI's ADE models are purpose-built for form/table/handwriting
  parsing. Gemini is a general-purpose multimodal model -- very capable at
  document understanding, but untested by us on YOUR specific messy,
  handwritten camp forms. Before pointing this at your remaining backlog:

      python gdrive_gemini_extract.py --file-id <a file your LandingAI run already finished>

  and diff output/gemini/json/<file>_record*.json against the equivalent
  rows already in your LandingAI results.csv for that same file. If the
  fields match well, you can trust it on the rest. If Gemini is missing or
  misreading fields (especially handwritten ones), stick with the LandingAI
  pipeline and just keep tuning its concurrency instead.

WHERE OUTPUT GOES (deliberately separate from the LandingAI run):
  Writes to GEMINI_OUTPUT_DIR/results_gemini.csv and
  GEMINI_JSON_OUTPUT_DIR/*.json (defaults ./output_gemini/...), NOT the same
  files as gdrive_landingai_extract.py. This is deliberate: it keeps the two
  pipelines' output separable so you can compare/audit them, and it means
  you do NOT need to cross-reference the two pipelines' state -- a file
  already finished by the LandingAI run has already been moved out of
  GDRIVE_SOURCE_FOLDER_ID into GDRIVE_DEST_FOLDER_ID, so listing the source
  folder here naturally only returns what's still outstanding. Safe to run
  both pipelines pointed at the same source/dest folder IDs without
  double-processing anything (whichever one claims a file first moves it out
  of the other's way).

RESUMABILITY:
  Coarser-grained than the LandingAI pipeline by necessity: since a whole
  file is one Gemini call instead of many page-calls, the unit of retry is
  the whole file, not the page. A failed file just retries entirely next
  run -- which is fine, because retrying one call is cheap. State lives in
  GEMINI_STATE_DIR/<drive_file_id>.json.

Setup:
    pip install google-genai

    # Auth option A -- Gemini Developer API (simplest):
    export GEMINI_API_KEY="..."          # from https://aistudio.google.com/apikey

    # Auth option B -- Vertex AI (reuse your existing GCP project):
    export GOOGLE_GENAI_USE_VERTEXAI=true
    export GOOGLE_CLOUD_PROJECT="..."
    export GOOGLE_CLOUD_LOCATION="us-central1"
    export GOOGLE_APPLICATION_CREDENTIALS="./gdrive-sa.json"   # same service account works if it has Vertex AI User role

Usage:
    python gdrive_gemini_extract.py --dry-run
    python gdrive_gemini_extract.py --file-id 1AbCDeFGhijKLmnoPQRstuv   # validate accuracy on one file first
    python gdrive_gemini_extract.py                                     # process everything left in the source folder
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

from google import genai

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

try:
    import fcntl
except ImportError:
    fcntl = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)
log = logging.getLogger("gemini_extract")

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]

SOURCE_FOLDER_ID = os.getenv("GDRIVE_SOURCE_FOLDER_ID", "val")
DEST_FOLDER_ID = os.getenv("GDRIVE_DEST_FOLDER_ID", "val")
SERVICE_ACCOUNT_FILE = os.getenv("GDRIVE_SERVICE_ACCOUNT_FILE", "./gdrive-sa.json")

SCHEMA_PATH = os.getenv("SCHEMA_PATH", "./schema.json")

# gemini-3.5-flash is the current fast/GA model as of this writing and
# supports Structured Outputs + PDF document understanding. For higher
# accuracy at the cost of latency, try gemini-3.1-pro-preview instead.
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash")

OUTPUT_DIR = os.getenv("GEMINI_OUTPUT_DIR", "./output_gemini")
JSON_OUTPUT_DIR = os.getenv("GEMINI_JSON_OUTPUT_DIR", "./output_gemini/json")
STATE_DIR = os.getenv("GEMINI_STATE_DIR", "./state_gemini")

# Each unit of work is now ~2 API calls instead of ~100, so this is cheap to
# raise. Still start moderate and watch for 429s before maxing it out.
FILE_WORKERS = int(os.getenv("FILE_WORKERS", "6"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
RETRY_BACKOFF_SECONDS = 5

STATE_LOCK = threading.Lock()
CSV_LOCK = threading.Lock()

CSV_FIELDS = ["filename", "record_index", "status", "json_path", "extracted_json"]


# --------------------------------------------------------------------------
# GOOGLE DRIVE HELPERS (same as gdrive_landingai_extract.py)
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


def get_file_by_id(service, file_id: str) -> Dict[str, Any]:
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
        except Exception as e:
            last_exc = e
            wait = RETRY_BACKOFF_SECONDS * attempt
            log.warning("Attempt %s/%s failed (%s) -- retrying in %ss", attempt, max_retries, e, wait)
            time.sleep(wait)
    raise RuntimeError(f"All {max_retries} attempts failed: {last_exc}")


# --------------------------------------------------------------------------
# CHECKPOINT STATE -- coarser than the LandingAI pipeline: one Gemini call
# covers the whole file, so the unit of retry is the whole file.
# --------------------------------------------------------------------------
def _state_path(file_id: str) -> Path:
    return Path(STATE_DIR) / f"{file_id}.json"


def load_state(file_id: str, filename: str) -> Dict[str, Any]:
    path = _state_path(file_id)
    if path.exists():
        with open(path, "r") as f:
            return json.load(f)
    return {"file_id": file_id, "filename": filename, "file_done": False, "records": []}


def save_state(state: Dict[str, Any]) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    path = _state_path(state["file_id"])
    tmp_path = path.with_name(path.name + ".tmp")
    with open(tmp_path, "w") as f:
        json.dump(state, f)
    os.replace(tmp_path, path)


# --------------------------------------------------------------------------
# CSV -- cross-process-safe append (same fcntl approach as the DAG version;
# see gdrive_landingai_extract_dag.py for why this matters if you ever run
# this concurrently from more than one machine)
# --------------------------------------------------------------------------
def get_csv_path(output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    return os.path.join(output_dir, "results_gemini.csv")


def append_csv_row(csv_path: str, row: Dict[str, Any]) -> None:
    with CSV_LOCK:
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            locked = False
            if fcntl is not None:
                try:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                    locked = True
                except OSError:
                    pass
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


# --------------------------------------------------------------------------
# SCHEMA WRAPPING -- wrap the existing per-patient schema.json (used by the
# LandingAI pipeline) in a "records": [...] array, since one Gemini call now
# returns every patient in the file at once instead of one call per patient.
# --------------------------------------------------------------------------
def build_records_schema(patient_schema: Dict[str, Any]) -> Dict[str, Any]:
    patient_item_schema = {
        "type": "object",
        "properties": patient_schema.get("properties", {}),
        "required": patient_schema.get("required", []),
    }
    return {
        "type": "object",
        "properties": {
            "records": {
                "type": "array",
                "description": (
                    "One object per distinct patient record found in the document, "
                    "in the order they appear. A single-patient document still "
                    "produces an array with exactly one item."
                ),
                "items": patient_item_schema,
            }
        },
        "required": ["records"],
    }


EXTRACTION_PROMPT = """This PDF contains one or more patients' outpatient clinical \
consultation forms from a medical camp, scanned back-to-back (a single \
patient's form is typically several pages: demographics/vitals/complaints, \
exam/diagnosis/prescriptions, doctor's notes, and lab results).

Identify EVERY distinct patient record in this document -- do not merge two \
different patients into one record, and do not skip a patient even if their \
pages are partially illegible or a page is blank. For each distinct patient, \
extract every field defined in the schema exactly as written on the form. If \
a field is not present or not legible, use null for a string field or an \
empty array for an array field -- do not guess or invent a value. Preserve \
the order patients appear in the document."""


# --------------------------------------------------------------------------
# GEMINI CALLS
# --------------------------------------------------------------------------
def get_genai_client() -> "genai.Client":
    return genai.Client()


def upload_pdf_to_gemini(client, local_path: Path):
    def _do_upload():
        f = client.files.upload(file=str(local_path), config=dict(mime_type="application/pdf"))
        # Newly uploaded files may briefly be in PROCESSING state before
        # they're usable in a generateContent call.
        waited = 0
        while getattr(f, "state", None) == "PROCESSING" and waited < 60:
            time.sleep(2)
            waited += 2
            f = client.files.get(name=f.name)
        if getattr(f, "state", None) == "FAILED":
            raise RuntimeError(f"Gemini file processing failed for {local_path.name}")
        return f

    return call_with_retry(_do_upload)


def extract_all_records_via_gemini(client, file_obj, wrapped_schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    def _do_extract():
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[file_obj, EXTRACTION_PROMPT],
            config={
                "response_format": {
                    "text": {"mime_type": "application/json", "schema": wrapped_schema}
                }
            },
        )
        parsed = json.loads(response.text)
        records = parsed.get("records")
        if not isinstance(records, list) or not records:
            raise RuntimeError(f"Gemini returned no records (raw: {response.text[:500]!r})")
        return records

    return call_with_retry(_do_extract)


# --------------------------------------------------------------------------
# PER-FILE PIPELINE
# --------------------------------------------------------------------------
def process_one_file(
    client, service, file_meta: Dict[str, Any], wrapped_schema: Dict[str, Any],
    dest_folder_id: str, json_output_dir: str, csv_path: str,
) -> List[Dict[str, Any]]:
    file_id = file_meta["id"]
    file_name = file_meta["name"]
    mime_type = file_meta.get("mimeType", "")

    if mime_type != "application/pdf":
        return [{"ok": False, "filename": file_name, "record_index": 0, "error": f"Unsupported mimeType: {mime_type}"}]

    state = load_state(file_id, file_name)
    if state.get("file_done"):
        log.info("%s already marked done in a previous run -- confirming it's out of the source folder.", file_name)
        try:
            move_file(service, file_id, dest_folder_id)
        except Exception:
            pass
        return []

    tmp_dir = Path(tempfile.mkdtemp(prefix="gemini_extract_"))
    local_path = tmp_dir / file_name
    gemini_file = None
    results: List[Dict[str, Any]] = []

    try:
        log.info("Downloading %s (%s)", file_name, file_id)
        download_file(service, file_id, local_path)

        log.info("Uploading %s to Gemini Files API", file_name)
        gemini_file = upload_pdf_to_gemini(client, local_path)

        log.info("Extracting all records from %s in one Gemini call", file_name)
        records = extract_all_records_via_gemini(client, gemini_file, wrapped_schema)
        log.info("%s: Gemini returned %d record(s)", file_name, len(records))

        for idx, extracted in enumerate(records, start=1):
            try:
                json_filename = f"{Path(file_name).stem}_record{idx:02d}.json"
                json_path = save_json_locally(json_filename, extracted, json_output_dir)
                row = {
                    "filename": file_name, "record_index": idx, "status": "done",
                    "json_path": json_path, "extracted_json": json.dumps(extracted),
                }
                append_csv_row(csv_path, row)
                results.append({"ok": True, **row})
            except Exception as e:
                log.exception("Failed to save record %d of %s", idx, file_name)
                results.append({"ok": False, "filename": file_name, "record_index": idx, "error": str(e)})

        all_ok = all(r.get("ok") for r in results) and len(results) > 0
        with STATE_LOCK:
            state["records"] = results
            state["file_done"] = all_ok
            save_state(state)

        if all_ok:
            move_file(service, file_id, dest_folder_id)
        else:
            log.warning("%s left in source folder: will retry entirely next run.", file_name)

        return results

    except Exception as e:
        log.exception("Failed to process %s", file_name)
        return [{"ok": False, "filename": file_name, "record_index": 0, "error": str(e)}]

    finally:
        if gemini_file is not None:
            try:
                client.files.delete(name=gemini_file.name)
            except Exception:
                pass  # best-effort cleanup; Gemini auto-expires files after 48h anyway
        shutil.rmtree(tmp_dir, ignore_errors=True)


def summarize(results: List[Dict[str, Any]], csv_path: str) -> None:
    total = len(results)
    ok = sum(1 for r in results if r.get("ok"))
    failed = [r for r in results if not r.get("ok")]
    log.info("Done this run. Results CSV (cumulative): %s", csv_path)
    log.info("Records newly extracted this run: %s/%s succeeded", ok, total)
    if failed:
        log.warning("Failures (will be retried automatically next run):")
        for f in failed:
            log.warning("  - %s record %s: %s", f.get("filename"), f.get("record_index"), f.get("error"))


def main():
    global STATE_DIR
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-folder-id", default=SOURCE_FOLDER_ID)
    parser.add_argument("--dest-folder-id", default=DEST_FOLDER_ID)
    parser.add_argument("--schema", default=SCHEMA_PATH)
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    parser.add_argument("--json-output-dir", default=JSON_OUTPUT_DIR)
    parser.add_argument("--state-dir", default=STATE_DIR)
    parser.add_argument("--dry-run", action="store_true", help="List files that would be processed and exit")
    parser.add_argument("--file-id", default=None, help="Only process this one Drive file ID -- use this to validate accuracy before running the whole backlog")
    parser.add_argument("--file-name", default=None)
    parser.add_argument("--reset-state", action="store_true")
    args = parser.parse_args()
    STATE_DIR = args.state_dir

    if args.reset_state:
        if os.path.exists(STATE_DIR):
            shutil.rmtree(STATE_DIR)
        log.info("Cleared state directory %s.", STATE_DIR)
        return

    with open(args.schema, "r") as f:
        patient_schema = json.load(f)
    wrapped_schema = build_records_schema(patient_schema)

    service = get_drive_service()
    client = get_genai_client()

    if args.file_id:
        files = [get_file_by_id(service, args.file_id)]
        log.info("Targeting a single file by id: %s (%s)", args.file_id, files[0].get("name"))
    else:
        files = list_source_files(service, args.source_folder_id)
        if args.file_name:
            files = [f for f in files if f["name"] == args.file_name]

    if args.dry_run:
        for f in files:
            print(f"{f['id']}  {f['name']}")
        return

    csv_path = get_csv_path(args.output_dir)
    log.info("Appending progress to %s as each file finishes.", csv_path)

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=FILE_WORKERS) as executor:
        futures = {
            executor.submit(process_one_file, client, service, f, wrapped_schema,
                             args.dest_folder_id, args.json_output_dir, csv_path): f
            for f in files
        }
        for future in as_completed(futures):
            results.extend(future.result())

    summarize(results, csv_path)


if __name__ == "__main__":
    main()