import os
import io
import uuid
import json
import requests
import pandas as pd
from io import BytesIO

# Google Drive API
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError


# -------------------------------
# CONFIG
# -------------------------------
VA_API_KEY = os.getenv("VA_API_KEY")
headers = {"Authorization": f"Basic {VA_API_KEY}"}

GOOGLE_CLIENT_SECRET = "credentials.json"
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file'
]

SOURCE_FOLDER_ID_ = "18TWweKvj1wPcKNaJ3FOAmPPZzBMP2XEW"  # Source folder
SOURCE_FOLDER_ID = "165L9frVyU23noT4gCcz5UdjI2meOebBO"
SOURCE_FOLDER_ID = "1IA4cZVcn_Bnz-NxDaXf0F5LxO2rbEfAN"
SOURCE_FOLDER_ID = "1qPLq0MASWF5l5qMxrOHnFA8IG2CGnI5T"
SOURCE_FOLDER_ID = "1s9tS6mzz3nH6Q7Fl2AT4hUuoZQnPVv9l"
DEST_FOLDER_ID = "1unblcJoVK5LWrPhNtgYD4qrkGcMy7KgJ"    # Destination folder after processing

DOWNLOAD_DIR = "downloaded"
CSV_PATH = f"results_{str(uuid.uuid4())}.csv"

# Landing AI extraction schema
SCHEMA_PATH = "schema.json"
with open(SCHEMA_PATH, "r") as f:
    schema = json.load(f)

# -------------------------------
# GOOGLE DRIVE AUTHENTICATION
# -------------------------------
creds = None
if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(GOOGLE_CLIENT_SECRET, SCOPES)
        creds = flow.run_local_server(port=0)
    with open("token.json", "w") as token:
        token.write(creds.to_json())

service = build("drive", "v3", credentials=creds)



#-----------------
# MOVE FILE FUNCTION
#-----------------
def move_file(service, file_id, remove_parent_id, add_parent_id):
    try:
        # Get current parents FIRST (critical step you might be missing)
        file = service.files().get(fileId=file_id, fields='parents').execute()
        previous_parents = ",".join(file.get('parents', []))
        
        # Move file using update with query params only
        updated_file = service.files().update(
            fileId=file_id,
            addParents=add_parent_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()
        
        print(f"✅ Moved file {file_id} to folder {add_parent_id}")
        print(f"New parents: {updated_file.get('parents')}")
        return updated_file
        
    except HttpError as error:
        print(f"❌ Failed to move file: {error}")




# -------------------------------
# LIST FILES IN SOURCE FOLDER
# -------------------------------
query = f"'{SOURCE_FOLDER_ID}' in parents and trashed = false"
results = service.files().list(q=query, fields="files(id, name, parents)").execute()
files = results.get("files", [])

if not files:
    print("No files found in the source Drive folder.")
    exit()

print(f"Found {len(files)} PDF files")

# -------------------------------
# PREP CSV (header once)
# -------------------------------
pd.DataFrame(columns=["filename", "extracted_json"]).to_csv(CSV_PATH, index=False)

df_rows = []
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# -------------------------------
# PROCESS EACH PDF
# -------------------------------
for f in files:
    file_id = f["id"]
    file_name = f["name"]
    current_parents = f.get("parents", [])

    print(f"\nProcessing: {file_name}")

    # -------------------------------
    # 1. DOWNLOAD PDF
    # -------------------------------
    print(f"Downloading: {file_name}")
    request = service.files().get_media(fileId=file_id)
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    try:
        with open(file_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f" → {int(status.progress() * 100)}%")
    except Exception as e:
        print(f"Error downloading {file_name}: {e}")
        continue

    # -------------------------------
    # 2. LANDING.AI PARSE → EXTRACT
    # -------------------------------
    print(f"Extracting data from: {file_name}")
    success = False
    extracted_json = None

    try:
        # Parse
        parse_response = requests.post(
            "https://api.va.landing.ai/v1/ade/parse",
            headers=headers,
            files=[("document", open(file_path, "rb"))],
            data={"model": "dpt-2"},
            timeout=120,
        )
        parse_response.raise_for_status()
        markdown_content = parse_response.json().get("markdown", "")

        # Extract
        extract_response = requests.post(
            "https://api.va.landing.ai/v1/ade/extract",
            headers=headers,
            files=[("markdown", BytesIO(markdown_content.encode("utf-8")))],
            data={"schema": json.dumps(schema)},
            timeout=120,
        )
        extracted_json = extract_response.json()
        
        if extract_response.status_code in [200, 201, 206]:
            success = True

    except Exception as e:
        print(f"Landing AI API error for {file_name}: {e}")

    # -------------------------------
    # 3. SAVE RESULT TO CSV (if successful)
    # -------------------------------
    if success and extracted_json:
        row = {
            "filename": file_name,
            "extracted_json": json.dumps(extracted_json)
        }
        df_rows.append(row)
        try:
            pd.DataFrame([row]).to_csv(CSV_PATH, mode="a", header=False, index=False)
            print(f"✔ Saved extracted data for: {file_name}")
        except Exception as e:
            print(f"Error writing {file_name} to CSV: {e}")

        # -------------------------------
        # 4. MOVE FILE TO DESTINATION FOLDER
        # -------------------------------
        print(f"Moving {file_name} to destination folder...")
        try:
            # Move file: remove current parents, add destination folder
            move_file(service, file_id, current_parents[0], DEST_FOLDER_ID)
            
        except Exception as e:
            print(f"❌ Failed to move {file_name}: {e}")
    
    # -------------------------------
    # 5. DELETE LOCAL PDF
    # -------------------------------
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            print(f"🗑️ Deleted local file: {file_name}")
        except Exception as e:
            print(f"Error deleting local {file_name}: {e}")
    else:
        print(f"⚠️ Local file not found for deletion: {file_name}")

# -------------------------------
# FINAL SUMMARY
# -------------------------------
df = pd.DataFrame(df_rows)
print(f"\n🎉 Processing complete!")
print(f"Results saved to: {CSV_PATH}")
print(f"Successfully processed: {len(df_rows)} / {len(files)} files")
if len(df) > 0:
    print("\nFirst few results:")
    print(df.head())
else:
    print("No files were successfully processed.")

