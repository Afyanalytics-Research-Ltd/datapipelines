"""
siaya_gdrive_gemini_extract.py

Airflow port of the standalone gdrive_gemini_extract.py script: pulls Siaya
Medical Camp outpatient-consultation PDFs out of a Google Drive folder,
extracts every distinct patient record per PDF in a single Gemini
"Structured Outputs" call (one Files-API upload + one generateContent call
per file, covering every patient in that file at once), and persists each
extracted patient record as JSON on S3. A file that is fully extracted is
moved from the source Drive folder into the destination Drive folder, which
doubles as this pipeline's "already processed" marker (see NOTE ON
DEDUP / RESUME below).

This DAG intentionally shares its Drive folder Variables with
dags/document_pipeline.py (the sibling LandingAI ADE pipeline for the same
Siaya batch): both pipelines list GDRIVE_SOURCE_FOLDER_ID and move finished
files into GDRIVE_DEST_FOLDER_ID, so it is safe to run both pipelines
against the same two folders without double-processing a file -- whichever
pipeline finishes a file first moves it out of the other's way. That
property is carried over unchanged from the original standalone script.

Airflow Variables required:
  GDRIVE_SOURCE_FOLDER_ID   Drive folder id holding not-yet-processed camp PDFs.
  GDRIVE_DEST_FOLDER_ID     Drive folder id a file is moved into once every
                             patient record in it has been extracted and
                             persisted to S3.
  GOOGLE_SA_JSON             Full contents of a Google service-account JSON
                             key (Drive scope) as a single Variable string --
                             same Variable already used by dags/document_pipeline.py
                             and dags/v3_api_to_snowflake_raw.py. Replaces the
                             original script's local ./gdrive-sa.json file.

Airflow Connections required:
  aws_default   S3 (bucket "collabmedbucket") -- where extracted per-patient
                 JSON records are written. See "OUTPUT SINK" below.

Env vars (worker-level secrets, not Airflow Variables -- set via .env / Docker):
  GEMINI_API_KEY                     Gemini Developer API key
                                       (https://aistudio.google.com/apikey).
    -- or, to reuse an existing GCP project via Vertex AI instead --
  GOOGLE_GENAI_USE_VERTEXAI=true
  GOOGLE_CLOUD_PROJECT
  GOOGLE_CLOUD_LOCATION
  GOOGLE_APPLICATION_CREDENTIALS      path to a service-account file with the
                                       Vertex AI User role (the same
                                       gdrive-sa.json works if granted it).
  GEMINI_MODEL                        optional override, default gemini-3.5-flash.

Dependency note: `google-genai` is NOT currently in requirements.txt (only
google-auth/google-auth-oauthlib/google-api-python-client are). Add
`pip install google-genai` to the image before deploying this DAG.

OUTPUT SINK (adapted from the original script):
  The standalone script wrote results_gemini.csv + one JSON file per patient
  record to local disk (./output_gemini/...). Airflow workers cannot be
  assumed to share local disk across task retries/executors (see the same
  caveat in dags/document_pipeline.py's "REQUIREMENT -- SHARED STORAGE"
  section), so this DAG instead uploads each extracted patient record as its
  own JSON object to S3:
      s3://collabmedbucket/raw/siaya_gdrive_gemini_extract/
          dt=<ds>/file_id=<drive_file_id>/<pdf_stem>_record<NN>.json
  This mirrors how dags/v3_api_to_snowflake_raw.py persists extracted
  payloads to S3 before any downstream Snowflake load. No Snowflake table
  exists yet for this schema (it's a different shape than
  HOSPITALS.SIAYA_MEDICAL_CAMP.SIAYA_MEDICAL_CAMP_COMPLETE_DATA, which holds
  V2/V3 API visit data, not scanned-form extractions) -- wiring a COPY INTO
  step on top of these S3 objects is a natural follow-up once the schema is
  validated, deliberately left out here to avoid guessing table DDL that
  wasn't specified. The per-record CSV manifest the original script kept is
  dropped in favor of task logs + the summarize_run task, since one JSON
  object per record is already a complete, independently-readable record of
  the run (the CSV was a convenience index over the same local JSON files).

NOTE ON DEDUP / RESUME (replacing the original script's local STATE_DIR):
  The original script's GEMINI_STATE_DIR/<file_id>.json only ever stored a
  single coarse "file_done" flag -- since one Gemini call covers a whole
  file, there was no partial per-record resume within a file to begin with
  (a failed file's records are simply redone in full next run, by the
  original script's own design: "A failed file just retries entirely next
  run -- which is fine, because retrying one call is cheap"). The *real*
  dedup mechanism, already built into the script's own docstring rationale,
  is moving a fully-extracted file out of GDRIVE_SOURCE_FOLDER_ID into
  GDRIVE_DEST_FOLDER_ID: "listing the source folder here naturally only
  returns what's still outstanding." This DAG keeps exactly that mechanism
  and drops the now-redundant local state file entirely:
    - list_new_files lists whatever is still sitting in GDRIVE_SOURCE_FOLDER_ID
      at DAG-run time -- an already-processed file physically cannot appear
      here again, because it was moved out on success.
    - If extract_and_persist_one_file fails partway through a file, the file
      is deliberately left in the source folder (not moved), so it
      reappears in the very next run's listing and is retried whole -- the
      same "coarse retry" behaviour the original script had, now driven by
      Drive folder membership plus Airflow's own task retries
      (default_args retries=3) instead of a hand-rolled JSON state file.
  No Airflow Variable-based processed-ID set or watermark is needed: Drive
  folder membership is already the queue, it is authoritative (unlike a
  Variable, it can't drift out of sync with what's actually left to do), and
  the number of in-flight files at any time is small (a camp PDF batch, not
  an unbounded stream), so there's no volume concern that would push this
  toward an S3/Snowflake dedup table either.

Schedule: @hourly. The original script was a manual/backlog-drain CLI tool
(run whenever someone wanted to process whatever was currently in the source
folder); dags/document_pipeline.py's sibling LandingAI DAG likewise runs on
schedule=None (manual trigger only). This DAG instead runs hourly so newly
scanned/uploaded camp batches get picked up automatically without needing
someone to remember to trigger it -- catchup=False means a backlog of missed
hourly slots never piles up multiple redundant runs, since each run just
lists whatever is currently outstanding in Drive.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

DAG_ID = "siaya_gdrive_gemini_extract"

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]

# gemini-3.5-flash is the current fast/GA model as of this writing and
# supports Structured Outputs + PDF document understanding. For higher
# accuracy at the cost of latency, try gemini-3.1-pro-preview instead.
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
RETRY_BACKOFF_SECONDS = 5

S3_CONN_ID = "aws_default"
S3_BUCKET = "collabmedbucket"
S3_PREFIX = "raw/siaya_gdrive_gemini_extract"

# --------------------------------------------------------------------------
# SCHEMA -- the exact per-patient schema.json from the repo root, inlined
# here so this DAG file is self-contained (only dags/ is deployed to
# Airflow workers per config/airflow.cfg's dags_folder; the repo-root
# schema.json is not guaranteed to be present there). Kept byte-for-byte
# identical to schema.json's fields/descriptions/required list.
# --------------------------------------------------------------------------
SIAYA_PATIENT_SCHEMA_JSON = """
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Siaya Medical Camp outpatient clinical consultation form extraction",
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "camp_file_number": {
      "type": ["string", "null"],
      "description": "The camp-assigned file/reference number handwritten or stamped near the top of the form (e.g. '0296', '056339'), distinct from Patient ID / File No in the demographics table."
    },
    "visit_date": {
      "type": ["string", "null"],
      "description": "Date of visit exactly as written next to 'Date:'."
    },
    "time_in": { "type": ["string", "null"] },
    "time_out": { "type": ["string", "null"] },
    "attending_clinician": { "type": ["string", "null"] },
    "patient_name": { "type": ["string", "null"] },
    "date_of_birth": {
      "type": ["string", "null"],
      "description": "Date of birth exactly as written; may sometimes only be a year."
    },
    "phone_number": { "type": ["string", "null"] },
    "address": { "type": ["string", "null"] },
    "patient_id_file_no": {
      "type": ["string", "null"],
      "description": "Value in the 'Patient ID / File No.' field."
    },
    "age": { "type": ["string", "null"] },
    "sex": { "type": ["string", "null"] },
    "emergency_contact": { "type": ["string", "null"] },
    "blood_pressure": {
      "type": ["string", "null"],
      "description": "BP reading exactly as written, including units, e.g. '149/83 mmHg'. If more than one reading (e.g. a repeat) is recorded, include all of them."
    },
    "heart_rate": { "type": ["string", "null"] },
    "oxygen_saturation": { "type": ["string", "null"] },
    "temperature": { "type": ["string", "null"] },
    "respiratory_rate": { "type": ["string", "null"] },
    "weight": { "type": ["string", "null"] },
    "height": { "type": ["string", "null"] },
    "body_mass_index": { "type": ["string", "null"] },
    "blood_sugar": {
      "type": ["string", "null"],
      "description": "RBS/FBS value exactly as written, including units."
    },
    "nurses_triage_notes": { "type": ["string", "null"] },
    "chief_complaints": {
      "type": "array",
      "items": { "type": "string" },
      "description": "Each numbered chief complaint and its duration, as separate entries."
    },
    "history_of_present_illness": { "type": ["string", "null"] },
    "past_medical_surgical_history": { "type": ["string", "null"] },
    "known_allergies": {
      "type": ["string", "null"],
      "description": "Whether 'None Known' was checked, or the allergy detail written next to 'Yes'."
    },
    "current_medications": { "type": ["string", "null"] },
    "general_appearance": { "type": ["string", "null"] },
    "systemic_exam_cvs": {
      "type": ["string", "null"],
      "description": "Cardiovascular exam finding; note if the 'normal' checkbox was ticked with no further text."
    },
    "systemic_exam_resp": { "type": ["string", "null"] },
    "systemic_exam_git": { "type": ["string", "null"] },
    "systemic_exam_cns": { "type": ["string", "null"] },
    "systemic_exam_msk_skin": { "type": ["string", "null"] },
    "provisional_diagnosis": {
      "type": "array",
      "items": { "type": "string" },
      "description": "Each numbered provisional/confirmed diagnosis line."
    },
    "lab_imaging_investigations_ordered": {
      "type": "array",
      "items": { "type": "string" }
    },
    "treatment_plan_prescriptions": {
      "type": "array",
      "items": { "type": "string" },
      "description": "Each numbered prescription/treatment line, exactly as written (drug, dose, frequency, duration)."
    },
    "follow_up_referral_notes": {
      "type": ["string", "null"],
      "description": "Review-in period, referral destination, and whether admission was marked as required."
    },
    "doctors_notes": {
      "type": ["string", "null"],
      "description": "Free-text narrative from the 'Doctor's Notes' page(s), such as case summaries, impression, and plan."
    },
    "lab_results": {
      "type": "array",
      "items": { "type": "string" },
      "description": "Each distinct lab/imaging result as its own entry, e.g. 'CXR AI Finding: Normal chest, TB Negative', 'Hb - 13.2 g/dL', 'RBS - 4.7 mmol/L (3.5-7.8)', 'MRDT - Negative', 'U/A: pH 6.6, SG 1.020, Leucocytes ++, Blood ++++, Protein ++'."
    },
    "raw_notes": {
      "type": ["string", "null"],
      "description": "Any other handwritten content that does not fit cleanly into the fields above."
    }
  },
  "required": [
    "camp_file_number", "visit_date", "time_in", "time_out",
    "attending_clinician", "patient_name", "date_of_birth", "phone_number",
    "address", "patient_id_file_no", "age", "sex", "emergency_contact",
    "blood_pressure", "heart_rate", "oxygen_saturation", "temperature",
    "respiratory_rate", "weight", "height", "body_mass_index",
    "blood_sugar", "nurses_triage_notes", "chief_complaints",
    "history_of_present_illness", "past_medical_surgical_history",
    "known_allergies", "current_medications", "general_appearance",
    "systemic_exam_cvs", "systemic_exam_resp", "systemic_exam_git",
    "systemic_exam_cns", "systemic_exam_msk_skin", "provisional_diagnosis",
    "lab_imaging_investigations_ordered", "treatment_plan_prescriptions",
    "follow_up_referral_notes", "doctors_notes", "lab_results", "raw_notes"
  ]
}
"""
SIAYA_PATIENT_SCHEMA: Dict[str, Any] = json.loads(SIAYA_PATIENT_SCHEMA_JSON)

# Same prompt as the original script, verbatim.
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


def build_records_schema(patient_schema: Dict[str, Any]) -> Dict[str, Any]:
    """Wrap the per-patient schema in a "records": [...] array, since one
    Gemini call returns every patient in a file at once (same helper as the
    original standalone script)."""
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


WRAPPED_SCHEMA: Dict[str, Any] = build_records_schema(SIAYA_PATIENT_SCHEMA)


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
# GOOGLE DRIVE HELPERS -- Variable.get() is called INSIDE task callables,
# not at module top level, so parsing this DAG file never hits the Airflow
# metadata DB (same rule dags/document_pipeline.py follows).
# --------------------------------------------------------------------------
def _get_service_account_info() -> Dict[str, Any]:
    sa_json_str = Variable.get("GOOGLE_SA_JSON")
    try:
        return json.loads(sa_json_str)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"GOOGLE_SA_JSON Variable is not valid JSON: {e}")


def get_drive_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_info(
        _get_service_account_info(), scopes=SCOPES
    )
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
    log.info("Found %s PDF file(s) in source folder %s", len(files), source_folder_id)
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


# --------------------------------------------------------------------------
# GEMINI CALLS
# --------------------------------------------------------------------------
def get_genai_client():
    from google import genai

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


def extract_all_records_via_gemini(client, file_obj) -> List[Dict[str, Any]]:
    def _do_extract():
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[file_obj, EXTRACTION_PROMPT],
            config={
                "response_format": {
                    "text": {"mime_type": "application/json", "schema": WRAPPED_SCHEMA}
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
# S3 OUTPUT SINK (replaces the original script's local JSON + CSV output)
# --------------------------------------------------------------------------
def persist_record_to_s3(extracted: Dict[str, Any], file_id: str, file_name: str, idx: int, ds: str) -> str:
    key = (
        f"{S3_PREFIX}/dt={ds}/file_id={file_id}/"
        f"{Path(file_name).stem}_record{idx:02d}.json"
    )
    S3Hook(aws_conn_id=S3_CONN_ID).load_bytes(
        bytes_data=json.dumps(extracted, indent=2).encode("utf-8"),
        key=key,
        bucket_name=S3_BUCKET,
        replace=True,
    )
    return key


# --------------------------------------------------------------------------
# TASK CALLABLES
# --------------------------------------------------------------------------
def list_new_files(**context) -> List[Dict[str, Any]]:
    """List whatever is currently outstanding in GDRIVE_SOURCE_FOLDER_ID.
    Returns op_kwargs dicts for extract_and_persist_one_file.expand()."""
    source_folder_id = Variable.get("GDRIVE_SOURCE_FOLDER_ID")
    service = get_drive_service()
    files = list_source_files(service, source_folder_id)
    return [
        {"file_id": f["id"], "file_name": f["name"], "mime_type": f.get("mimeType", "")}
        for f in files
    ]


def extract_and_persist_one_file(file_id: str, file_name: str, mime_type: str, **context) -> Dict[str, Any]:
    """One Airflow mapped task instance per Drive file: download, one Gemini
    call extracting every patient record in the file, persist each record to
    S3, then move the file out of the source folder. On any failure the file
    is deliberately left in place so it is picked up and retried whole on
    the next scheduled run (see NOTE ON DEDUP / RESUME in the module docstring)."""
    if mime_type != "application/pdf":
        log.warning("Skipping %s (%s): unsupported mimeType %s", file_name, file_id, mime_type)
        return {"ok": False, "file_id": file_id, "file_name": file_name, "error": f"Unsupported mimeType: {mime_type}"}

    ds = context.get("ds") or datetime.utcnow().date().isoformat()
    dest_folder_id = Variable.get("GDRIVE_DEST_FOLDER_ID")

    service = get_drive_service()
    client = get_genai_client()

    tmp_dir = Path(tempfile.mkdtemp(prefix="siaya_gemini_"))
    local_path = tmp_dir / file_name
    gemini_file = None

    try:
        log.info("Downloading %s (%s)", file_name, file_id)
        download_file(service, file_id, local_path)

        log.info("Uploading %s to Gemini Files API", file_name)
        gemini_file = upload_pdf_to_gemini(client, local_path)

        log.info("Extracting all records from %s in one Gemini call", file_name)
        records = extract_all_records_via_gemini(client, gemini_file)
        log.info("%s: Gemini returned %d record(s)", file_name, len(records))

        s3_keys = [
            persist_record_to_s3(extracted, file_id, file_name, idx, ds)
            for idx, extracted in enumerate(records, start=1)
        ]

        move_file(service, file_id, dest_folder_id)
        log.info("%s: %d record(s) persisted to s3://%s, file moved to dest folder.",
                  file_name, len(records), S3_BUCKET)

        return {
            "ok": True, "file_id": file_id, "file_name": file_name,
            "record_count": len(records), "s3_keys": s3_keys,
        }

    except Exception as e:
        log.exception("Failed to process %s -- left in source folder for retry next run", file_name)
        return {"ok": False, "file_id": file_id, "file_name": file_name, "error": str(e)}

    finally:
        if gemini_file is not None:
            try:
                client.files.delete(name=gemini_file.name)
            except Exception:
                pass  # best-effort cleanup; Gemini auto-expires files after 48h anyway
        shutil.rmtree(tmp_dir, ignore_errors=True)


def summarize_run(results: List[Dict[str, Any]], **context) -> None:
    results = results or []
    total = len(results)
    ok_results = [r for r in results if r.get("ok")]
    failed = [r for r in results if not r.get("ok")]
    total_records = sum(r.get("record_count", 0) for r in ok_results)

    log.info(
        "Run complete: %d/%d file(s) fully extracted, %d patient record(s) persisted to s3://%s/%s",
        len(ok_results), total, total_records, S3_BUCKET, S3_PREFIX,
    )
    if failed:
        log.warning("Failures (file left in source folder, will retry whole next run):")
        for f in failed:
            log.warning("  - %s (%s): %s", f.get("file_name"), f.get("file_id"), f.get("error"))


# --------------------------------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["siaya", "gdrive", "gemini", "extract"],
) as dag:

    t_list = PythonOperator(
        task_id="list_new_files",
        python_callable=list_new_files,
    )

    t_extract = PythonOperator.partial(
        task_id="extract_and_persist",
        python_callable=extract_and_persist_one_file,
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(op_kwargs=t_list.output)

    t_summary = PythonOperator(
        task_id="summarize_run",
        python_callable=summarize_run,
        op_kwargs={"results": t_extract.output},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_list >> t_extract >> t_summary
