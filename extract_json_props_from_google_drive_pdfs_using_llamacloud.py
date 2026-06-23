import os
import io
import sys
import time
import uuid
import json
import asyncio
import logging
from datetime import datetime
import pandas as pd

# Google Drive API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

# LlamaCloud — using the high-level `llama-cloud-services` wrapper because the
# underlying `llama-cloud` SDK keeps renaming its namespaces between versions.
# Install with: pip install llama-cloud-services
from llama_cloud_services import LlamaExtract


# -------------------------------
# CONFIG
# -------------------------------
LLAMA_CLOUD_API_KEY = os.getenv("LLAMA_CLOUD_API_KEY","").strip()
# Optional: pin to a specific LlamaCloud project / organization
LLAMA_CLOUD_PROJECT_ID = os.getenv("LLAMA_CLOUD_PROJECT_ID","").strip()  # may be None
# Name of the extraction agent in LlamaCloud (will be created if missing)
LLAMA_EXTRACT_AGENT_NAME = os.getenv("LLAMA_EXTRACT_AGENT_NAME", "drive-pdf-extractor").strip()

# Log verbosity: DEBUG, INFO, WARNING, ERROR
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()

# Path to the service account key JSON downloaded from Google Cloud Console.
# Override with the GOOGLE_SERVICE_ACCOUNT_FILE env var.
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json").strip()
# Optional: email address to impersonate (requires domain-wide delegation on the SA).
# Leave unset for standard service account auth — in that case the SA itself must
# have access to the source/destination folders (share them with the SA's email).
GOOGLE_IMPERSONATE_SUBJECT = os.getenv("GOOGLE_IMPERSONATE_SUBJECT","").strip()

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]

SOURCE_FOLDER_ID = "1Fb_89LIGH8QXJ8sWG5MISMvYhl3k1tmV"
DEST_FOLDER_ID = "1LOX3DxRcohgmlAsu6eQqH5Z85c83KB5V"  # Destination folder after processing

DOWNLOAD_DIR = "downloaded"
RUN_ID = str(uuid.uuid4())
CSV_PATH = f"results_{RUN_ID}.csv"
LOG_PATH = f"run_{RUN_ID}.log"

# Extraction schema (same schema.json you used with Landing AI)
SCHEMA_PATH = "schema.json"


# -------------------------------
# LOGGING SETUP
# -------------------------------
def setup_logging(level: str = "INFO", log_file: str = LOG_PATH) -> logging.Logger:
    """Configure root logger with console + rotating file output."""
    root = logging.getLogger()
    root.setLevel(getattr(logging, level, logging.INFO))

    # Clear any pre-existing handlers (avoid duplicate logs on re-runs in notebooks)
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(getattr(logging, level, logging.INFO))
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # File handler (full DEBUG to file regardless of console level)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    # Tame chatty third-party loggers
    logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
    logging.getLogger("googleapiclient.discovery").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    return logging.getLogger("drive_llamacloud")


log = setup_logging(LOG_LEVEL)


def human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# -------------------------------
# LOAD SCHEMA
# -------------------------------
log.info("Loading extraction schema from %s", SCHEMA_PATH)
try:
    with open(SCHEMA_PATH, "r") as f:
        schema = json.load(f)
    log.debug("Schema loaded with %d top-level keys", len(schema) if isinstance(schema, dict) else -1)
except FileNotFoundError:
    log.critical("Schema file not found at %s", SCHEMA_PATH)
    raise
except json.JSONDecodeError as e:
    log.critical("Schema file is not valid JSON: %s", e)
    raise


# -------------------------------
# GOOGLE DRIVE AUTHENTICATION (service account)
# -------------------------------
def get_drive_service():
    log.info("Authenticating to Google Drive using service account: %s", GOOGLE_SERVICE_ACCOUNT_FILE)

    if not os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        log.critical("Service account key file not found at %s", GOOGLE_SERVICE_ACCOUNT_FILE)
        raise FileNotFoundError(
            f"Service account file not found: {GOOGLE_SERVICE_ACCOUNT_FILE}. "
            "Set GOOGLE_SERVICE_ACCOUNT_FILE or place the key JSON at the default path."
        )

    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
    )

    # Log which identity we're authenticating as (helps debug "file not found"
    # errors that actually mean "the SA doesn't have access to this folder").
    sa_email = getattr(creds, "service_account_email", "(unknown)")
    log.info("Service account email: %s", sa_email)

    if GOOGLE_IMPERSONATE_SUBJECT:
        log.info("Impersonating user via domain-wide delegation: %s", GOOGLE_IMPERSONATE_SUBJECT)
        creds = creds.with_subject(GOOGLE_IMPERSONATE_SUBJECT)

    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    log.info("Google Drive client ready")
    return service


# -------------------------------
# MOVE FILE FUNCTION
# -------------------------------
def move_file(service, file_id, add_parent_id):
    log.debug("move_file: fetching current parents for file_id=%s", file_id)
    try:
        file = service.files().get(fileId=file_id, fields="parents").execute()
        previous_parents = ",".join(file.get("parents", []))
        log.debug("move_file: current parents=%s, target=%s", previous_parents, add_parent_id)

        updated_file = (
            service.files()
            .update(
                fileId=file_id,
                addParents=add_parent_id,
                removeParents=previous_parents,
                fields="id, parents",
            )
            .execute()
        )

        log.info("Moved file_id=%s to folder=%s (new parents=%s)",
                 file_id, add_parent_id, updated_file.get("parents"))
        return updated_file

    except HttpError as error:
        log.error("Failed to move file_id=%s: %s", file_id, error)
        return None


# -------------------------------
# LLAMACLOUD: get-or-create extraction agent
# -------------------------------
def get_or_create_extraction_agent(extractor: LlamaExtract, name: str, data_schema: dict):
    """Return an existing extraction agent by name, or create one with the given schema."""
    log.info("Looking up LlamaCloud extraction agent: %s", name)

    # 1. Try fast-path: get by name
    try:
        agent = extractor.get_agent(name=name)
        if agent is not None:
            log.info("Reusing extraction agent name=%s id=%s", name, agent.id)
            return agent
    except Exception as e:
        log.debug("get_agent(name=%s) failed (continuing): %s", name, e)

    # 2. Fall back to listing (older SDK versions don't support get_agent by name)
    try:
        agents = extractor.list_agents()
        log.debug("Found %d existing extraction agents", len(agents) if agents else 0)
        for a in agents or []:
            log.debug("  agent candidate: name=%s id=%s", getattr(a, "name", "?"), getattr(a, "id", "?"))
            if getattr(a, "name", None) == name:
                log.info("Reusing extraction agent name=%s id=%s", name, a.id)
                return a
    except Exception as e:
        log.warning("Failed to list extraction agents (continuing to create): %s", e)

    # 3. Create new
    log.info("Creating new extraction agent: %s", name)
    agent = extractor.create_agent(name=name, data_schema=data_schema)
    log.info("Created extraction agent name=%s id=%s", name, agent.id)
    return agent


# -------------------------------
# LLAMACLOUD: run extraction on a single local file
# -------------------------------
def extract_with_llamacloud(agent, file_path: str) -> dict | None:
    """Run extraction on a single local file via a LlamaExtract agent."""
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    log.info("[%s] Submitting to LlamaCloud (%s)", file_name, human_bytes(file_size))

    t0 = time.monotonic()
    try:
        result = agent.extract(file_path)
    except Exception as e:
        log.exception("[%s] agent.extract failed: %s", file_name, e)
        return None
    log.info("[%s] Extraction finished in %.2fs", file_name, time.monotonic() - t0)

    # `result.data` is the structured JSON matching the agent's schema.
    # Some SDK versions return a list/wrapper; handle both shapes.
    data = getattr(result, "data", None)
    if data is None and isinstance(result, dict):
        data = result.get("data", result)

    if data is None:
        log.warning("[%s] Result payload was empty (result=%r)", file_name, result)
    else:
        try:
            size = len(json.dumps(data, default=str))
            log.info("[%s] Got result payload (%s of JSON)", file_name, human_bytes(size))
        except Exception:
            log.info("[%s] Got result payload (non-serializable preview skipped)", file_name)

    return data


# -------------------------------
# MAIN ASYNC PIPELINE
# -------------------------------
async def main():
    pipeline_started = datetime.now()
    log.info("=" * 70)
    log.info("Run id: %s", RUN_ID)
    log.info("Started: %s", pipeline_started.isoformat())
    log.info("Log file: %s", LOG_PATH)
    log.info("CSV output: %s", CSV_PATH)
    log.info("Source Drive folder: %s", SOURCE_FOLDER_ID)
    log.info("Destination Drive folder: %s", DEST_FOLDER_ID)
    log.info("LlamaCloud agent name: %s", LLAMA_EXTRACT_AGENT_NAME)
    log.info("LlamaCloud project id: %s", LLAMA_CLOUD_PROJECT_ID or "(default)")
    log.info("=" * 70)

    if not LLAMA_CLOUD_API_KEY:
        log.critical("LLAMA_CLOUD_API_KEY env var is not set")
        raise RuntimeError("LLAMA_CLOUD_API_KEY env var is not set")

    service = get_drive_service()

    extractor_kwargs = {"api_key": LLAMA_CLOUD_API_KEY}
    if LLAMA_CLOUD_PROJECT_ID:
        extractor_kwargs["project_id"] = LLAMA_CLOUD_PROJECT_ID
    extractor = LlamaExtract(**extractor_kwargs)
    log.debug("Initialized LlamaExtract client")

    agent = get_or_create_extraction_agent(extractor, LLAMA_EXTRACT_AGENT_NAME, schema)

    # LIST FILES IN SOURCE FOLDER
    log.info("Listing files in source folder %s", SOURCE_FOLDER_ID)
    query = f"'{SOURCE_FOLDER_ID}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name, parents, mimeType, size)").execute()
    files = results.get("files", [])

    if not files:
        log.warning("No files found in the source Drive folder.")
        return

    log.info("Found %d file(s) to process", len(files))
    for i, f in enumerate(files, 1):
        log.debug("  [%d/%d] name=%s id=%s mime=%s size=%s",
                  i, len(files), f.get("name"), f.get("id"),
                  f.get("mimeType"), f.get("size"))

    # PREP CSV (header once)
    log.info("Initializing CSV at %s", CSV_PATH)
    pd.DataFrame(columns=["filename", "extracted_json"]).to_csv(CSV_PATH, index=False)

    df_rows = []
    failures: list[tuple[str, str]] = []  # (filename, reason)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    log.debug("Ensured download dir exists: %s", DOWNLOAD_DIR)

    # PROCESS EACH PDF
    for idx, f in enumerate(files, 1):
        file_id = f["id"]
        file_name = f["name"]
        file_t0 = time.monotonic()

        log.info("-" * 70)
        log.info("[%d/%d] START %s (id=%s)", idx, len(files), file_name, file_id)

        # 1. DOWNLOAD PDF
        request = service.files().get_media(fileId=file_id)
        file_path = os.path.join(DOWNLOAD_DIR, file_name)

        try:
            t_dl = time.monotonic()
            with open(file_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                last_pct = -1
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        pct = int(status.progress() * 100)
                        # Only log every 10% step to avoid spam
                        if pct // 10 != last_pct // 10:
                            log.info("[%s] Downloading: %d%%", file_name, pct)
                            last_pct = pct
            dl_size = os.path.getsize(file_path)
            log.info("[%s] Downloaded %s in %.2fs",
                     file_name, human_bytes(dl_size), time.monotonic() - t_dl)
        except Exception as e:
            log.exception("[%s] Error downloading: %s", file_name, e)
            failures.append((file_name, f"download_error: {e}"))
            continue

        # 2. LLAMACLOUD EXTRACT
        extracted_json = None
        try:
            extracted_json = extract_with_llamacloud(agent, file_path)
        except Exception as e:
            log.exception("[%s] LlamaCloud extraction raised: %s", file_name, e)

        success = extracted_json is not None

        # 3. SAVE RESULT TO CSV (if successful)
        if success:
            row = {
                "filename": file_name,
                "extracted_json": json.dumps(extracted_json),
            }
            df_rows.append(row)
            try:
                pd.DataFrame([row]).to_csv(CSV_PATH, mode="a", header=False, index=False)
                log.info("[%s] Wrote row to CSV (cumulative rows=%d)", file_name, len(df_rows))
            except Exception as e:
                log.exception("[%s] Error writing to CSV: %s", file_name, e)

            # 4. MOVE FILE TO DESTINATION FOLDER
            log.info("[%s] Moving to destination folder %s", file_name, DEST_FOLDER_ID)
            try:
                moved = move_file(service, file_id, DEST_FOLDER_ID)
                if moved is None:
                    failures.append((file_name, "move_failed"))
            except Exception as e:
                log.exception("[%s] Failed to move: %s", file_name, e)
                failures.append((file_name, f"move_error: {e}"))
        else:
            log.warning("[%s] Marked as FAILED — skipping CSV write and move", file_name)
            failures.append((file_name, "extraction_failed"))

        # 5. DELETE LOCAL PDF
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                log.debug("[%s] Deleted local file at %s", file_name, file_path)
            except Exception as e:
                log.exception("[%s] Error deleting local file: %s", file_name, e)
        else:
            log.warning("[%s] Local file not found for deletion at %s", file_name, file_path)

        log.info("[%d/%d] END %s in %.2fs (success=%s)",
                 idx, len(files), file_name, time.monotonic() - file_t0, success)

    # FINAL SUMMARY
    pipeline_elapsed = (datetime.now() - pipeline_started).total_seconds()
    df = pd.DataFrame(df_rows)
    log.info("=" * 70)
    log.info("Processing complete in %.1fs", pipeline_elapsed)
    log.info("Results CSV: %s", CSV_PATH)
    log.info("Run log:     %s", LOG_PATH)
    log.info("Succeeded: %d / %d", len(df_rows), len(files))
    log.info("Failed:    %d", len(failures))
    if failures:
        log.warning("Failure breakdown:")
        for name, reason in failures:
            log.warning("  - %s: %s", name, reason)
    if len(df) > 0:
        log.info("Preview of first results:\n%s", df.head().to_string(index=False))
    else:
        log.warning("No files were successfully processed.")
    log.info("=" * 70)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Interrupted by user (KeyboardInterrupt)")
    except Exception as e:
        log.exception("Fatal error in main(): %s", e)
        raise