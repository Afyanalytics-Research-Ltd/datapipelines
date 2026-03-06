# dags/gdrive_landingai_extract.py

from __future__ import annotations

import os
import json
import uuid
import tempfile
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Any

import pandas as pd
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
import logging
log = logging.getLogger(__name__)


# -------------------------------
# CONFIG (set these as Airflow Variables or Env)
# -------------------------------
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]

# Prefer Airflow Variables for IDs (so you don't edit code for changes)
SOURCE_FOLDER_ID = Variable.get("GDRIVE_SOURCE_FOLDER_ID", default_var="val")
DEST_FOLDER_ID = Variable.get("GDRIVE_DEST_FOLDER_ID", default_var="val")
JSON_DEST_FOLDER_ID = Variable.get("GDRIVE_JSON_DEST_FOLDER_ID", default_var="val")

# LandingAI
VA_API_KEY = os.getenv("VA_API_KEY")  # put in Airflow Connections/Env
LANDING_HEADERS = {"Authorization": f"Basic {VA_API_KEY}"}

# Where to write final CSV on the Airflow worker
OUTPUT_DIR = Variable.get("LANDING_OUTPUT_DIR", default_var="/opt/airflow/output")

SCHEMA_PATH = '/opt/airflow/dags/jsons/schema.json'
# Google creds locations (mounted into workers)
GOOGLE_CLIENT_SECRET = Variable.get("GOOGLE_CLIENT_SECRET_PATH", default_var="/opt/airflow/credentials.json")
TOKEN_JSON_PATH = Variable.get("GOOGLE_TOKEN_JSON_PATH", default_var="/opt/airflow/token.json")

SERVICE_ACCOUNT_FILE_JSON = {
  "type": os.getenv("GCP_SA_TYPE","default"),
  "project_id": os.getenv("GCP_SA_PROJECT_ID","default"),
  "private_key_id": os.getenv("GCP_SA_PRIVATE_KEY_ID","default"),
  "private_key": os.getenv("GCP_SA_PRIVATE_KEY","default").replace("\\n", "\n"),
  "client_email": os.getenv("GCP_SA_CLIENT_EMAIL","default"),
  "client_id": os.getenv("GCP_SA_CLIENT_ID","default"),
  "token_uri": os.getenv("GCP_SA_TOKEN_URI","default")
}

SERVICE_ACCOUNT_FILE = "/opt/airflow/gdrive-sa.json"

with open(SERVICE_ACCOUNT_FILE, "w") as f:
    json.dump(SERVICE_ACCOUNT_FILE_JSON, f)

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
    )
    return build("drive", "v3", credentials=creds)

# def get_drive_service():
#     """
#     NOTE:
#     - InstalledAppFlow / run_local_server is not suitable for Airflow.
#     - Use an existing token.json (refreshable) OR use a Service Account.
#     """
#     creds = None
#     if os.path.exists(TOKEN_JSON_PATH):
#         creds = Credentials.from_authorized_user_file(TOKEN_JSON_PATH, SCOPES)

#     if not creds or not creds.valid:
#         if creds and creds.expired and creds.refresh_token:
#             creds.refresh(Request())
#             with open(TOKEN_JSON_PATH, "w") as token:
#                 token.write(creds.to_json())
#         else:
#             raise RuntimeError(
#                 "No valid Google token.json found (and InstalledAppFlow is not allowed in Airflow). "
#                 "Generate token.json locally once, upload/mount it to TOKEN_JSON_PATH, or switch to a service account."
#             )

#     return build("drive", "v3", credentials=creds)


def move_file(service, file_id: str, add_parent_id: str):
    try:
        file = service.files().get(fileId=file_id, fields="parents").execute()
        previous_parents = ",".join(file.get("parents", []))

        updated_file = service.files().update(
            fileId=file_id,
            addParents=add_parent_id,
            removeParents=previous_parents,
            fields="id, parents",
        ).execute()

        return updated_file
    except HttpError as error:
        raise RuntimeError(f"Failed to move file {file_id}: {error}")


def upload_json_to_drive(service, filename: str, json_data: dict, folder_id: str):
    try:
        tmp_dir = tempfile.mkdtemp(prefix="landingai_json_")
        local_path = os.path.join(tmp_dir, filename)

        with open(local_path, "w") as f:
            json.dump(json_data, f, indent=2)

        file_metadata = {
            "name": filename,
            "parents": [folder_id],
        }

        from googleapiclient.http import MediaFileUpload

        media = MediaFileUpload(local_path, mimetype="application/json")

        service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()

    except HttpError as error:
        raise RuntimeError(f"Failed to upload JSON {filename}: {error}")
    
with DAG(
    dag_id="document_datapipeline",
    start_date=datetime(2026, 3, 1),
    schedule=None,  # trigger manually or set cron
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["gdrive", "landingai", "pdf"],
) as dag:

    @task
    def load_schema() -> Dict[str, Any]:
        with open(SCHEMA_PATH, "r") as f:
            return json.load(f)

    @task
    def list_source_files() -> List[Dict[str, Any]]:
        service = get_drive_service()

        query = (
            f"'{SOURCE_FOLDER_ID}' in parents "
            f"and trashed = false "
            f"and mimeType = 'application/pdf'"
        )

        results = service.files().list(
            q=query,
            fields="files(id, name, parents, mimeType)"
        ).execute()

        files = results.get("files", [])
        log.info("Found %s PDF files", len(files))
        return files

    @task
    def process_one_file(file_meta: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
        service = get_drive_service()

        file_id = file_meta["id"]
        file_name = file_meta["name"]
        mime_type = file_meta.get("mimeType", "")

        tmp_dir = tempfile.mkdtemp(prefix="landingai_")
        local_path = os.path.join(tmp_dir, file_name)
        
        try:
            if mime_type == "application/pdf":
                request = service.files().get_media(fileId=file_id)
            else:
                return {
                    "ok": False,
                    "filename": file_name,
                    "file_id": file_id,
                    "error": f"Unsupported mimeType for LandingAI pipeline: {mime_type}",
                }

            with open(local_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()

            with open(local_path, "rb") as doc_f:
                parse_response = requests.post(
                    "https://api.va.landing.ai/v1/ade/parse",
                    headers=LANDING_HEADERS,
                    files={"document": (file_name, doc_f, "application/pdf")},
                    data={"model": "dpt-2"},
                    timeout=120,
                )
            parse_response.raise_for_status()

            markdown_content = parse_response.json().get("markdown", "")

            extract_response = requests.post(
                "https://api.va.landing.ai/v1/ade/extract",
                headers=LANDING_HEADERS,
                files={
                    "markdown": ("document.md", markdown_content, "text/markdown"),
                },
                data={
                    "schema": json.dumps(schema),
                },
                timeout=120,
            )

            if not extract_response.ok:
                raise RuntimeError(
                    f"LandingAI extract failed: {extract_response.status_code} - {extract_response.text}"
                )

            extracted = extract_response.json()

            # Upload JSON result to Drive
            # json_filename = file_name.replace(".pdf", ".json")

            # upload_json_to_drive(
            #     service,
            #     json_filename,
            #     extracted,
            #     JSON_DEST_FOLDER_ID
            # )


            move_file(service, file_id, DEST_FOLDER_ID)
            log.info("Processing file: %s (%s)", file_name, file_id)
            log.info("Markdown length: %s", len(markdown_content))
            log.info("Schema keys: %s", list(schema.keys()) if isinstance(schema, dict) else type(schema))
            return {
                "ok": True,
                "filename": file_name,
                "file_id": file_id,
                "extracted_json": json.dumps(extracted),
            }

        except Exception as e:
            return {
                "ok": False,
                "filename": file_name,
                "file_id": file_id,
                "error": str(e),
            }
        finally:
            try:
                if os.path.exists(local_path):
                    os.remove(local_path)
            except Exception:
                pass

    @task
    def write_csv(results: List[Dict[str, Any]]) -> str:
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        csv_path = os.path.join(OUTPUT_DIR, f"results_{uuid.uuid4()}.csv")

        rows = []
        for r in results:
            if r.get("ok"):
                rows.append(
                    {"filename": r["filename"], "extracted_json": r["extracted_json"]}
                )

        pd.DataFrame(rows, columns=["filename", "extracted_json"]).to_csv(csv_path, index=False)
        return csv_path

    @task
    def summarize(results: List[Dict[str, Any]], csv_path: str) -> None:
        total = len(results)
        ok = sum(1 for r in results if r.get("ok"))
        failed = [r for r in results if not r.get("ok")]

        print(f"🎉 Done. CSV: {csv_path}")
        print(f"Processed: {ok}/{total} succeeded")
        if failed:
            print("Failures (first 10):")
            for f in failed[:10]:
                print(f"- {f.get('filename')}: {f.get('error')}")

    schema = load_schema()
    files = list_source_files()
    processed = process_one_file.partial(schema=schema).expand(file_meta=files)
    csv_path = write_csv(processed)
    summarize(processed, csv_path)