#!/usr/bin/env python3
"""
v3_api_to_snowflake_raw.py  —  Afya V3 multi-service gateway → S3 → Snowflake RAW

Flow:
  1. Authenticate against core.afyaanalytics.ai/api/v1/login  →  cache bearer token (50-min TTL).
     Login also captures organization_id as tenant_id (used in X-Tenant-Id / source_tenant_id).
  2. For each configured service (core, finance, evaluation, reception, inventory,
     theatre, inpatient, dialysis) optionally discover models via action=list.
  3. Build one job per (service, model) pair.
  4. For each job: POST {service_url}/api/v1/gateway action=read → paginate all pages
     → gzipped JSONL → S3 → COPY INTO Snowflake RAW.
  5. Resume-aware: every completed job is checkpointed to .v3_progress.json;
     a re-run skips already-done work so no records are missed.
  6. Watermark only advances when the run completes with ZERO failures.

MISSING-DATA GUARANTEE
  The root cause of missing data in a naive pipeline is: the watermark advances
  even when some jobs failed, so the next run skips those records.
  This script fixes that by:
    a) Writing a per-job checkpoint immediately on success (_mark_done).
    b) Blocking watermark advancement if any job failed.
    c) Providing --no-resume to force a full re-run when needed.

USAGE
  python v3_api_to_snowflake_raw.py
  python v3_api_to_snowflake_raw.py --services reception,finance
  python v3_api_to_snowflake_raw.py --models "reception:patient,finance:invoice"
  python v3_api_to_snowflake_raw.py --since 2025-01-01
  python v3_api_to_snowflake_raw.py --dry-run
  python v3_api_to_snowflake_raw.py --no-resume
  python v3_api_to_snowflake_raw.py --workers 4 --page-workers 4

ENV VARS  (place in a .env file next to this script)
  # Afya credentials
  afya_username=martin
  afya_password=Qwerty123!!

  # Service URLs (optional — defaults shown)
  CORE_URL=https://core.afyaanalytics.ai
  FINANCE_URL=https://finance.afyaanalytics.ai
  EVALUATION_URL=https://evaluation.afyaanalytics.ai
  RECEPTION_URL=https://reception.afyaanalytics.ai
  INVENTORY_URL=https://inventory.afyaanalytics.ai
  THEATRE_URL=https://theatre.afyaanalytics.ai
  INPATIENT_URL=https://inpatient.afyaanalytics.ai
  DIALYSIS_URL=https://dialysis.afyaanalytics.ai

  # Snowflake (key-pair auth)
  SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_PRIVATE_KEY_PATH

  # AWS
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

  # Optional: path to a JSON file listing service:model pairs to extract
  # e.g. ["reception:patient", "finance:invoice"]
  MODELS_FILE=models.json

  # Tuning
  PIPELINE_WORKERS=8   # parallel jobs
  PAGE_WORKERS=4       # parallel pages within a job
  PER_PAGE=100         # records per page
  LOG_LEVEL=INFO
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd
import requests
import requests.adapters
import snowflake.connector
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, Timeout

try:
    import orjson
    def _dumps_bytes(obj) -> bytes:
        return orjson.dumps(obj)
except ImportError:
    def _dumps_bytes(obj) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

# ─── LOGGING ─────────────────────────────────────────────────────────────

log = logging.getLogger("v3_pipeline")
if not log.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(name)s · %(message)s",
        datefmt="%H:%M:%S",
    ))
    log.addHandler(h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ─── CONFIG ──────────────────────────────────────────────────────────────

PIPELINE_NAME = "v3_api_to_snowflake_raw"

# Per-service base URLs — each ends with /api/ so gateway is {url}/v1/gateway
# and login is {core_url}/v1/login.  This mirrors the exact pattern used in
# v2_to_v3_api_migration.py.
def _svc_url(env_key: str, default_host: str) -> str:
    raw = os.getenv(env_key, f"https://{default_host}/api/")
    return raw.rstrip("/") + "/"      # guarantee exactly one trailing slash

V3_SERVICES: dict[str, str] = {
    "core":       _svc_url("CORE_URL",       "core.afyaanalytics.ai"),
    "finance":    _svc_url("FINANCE_URL",    "finance.afyaanalytics.ai"),
    "evaluation": _svc_url("EVALUATION_URL", "evaluation.afyaanalytics.ai"),
    "reception":  _svc_url("RECEPTION_URL",  "reception.afyaanalytics.ai"),
    "inventory":  _svc_url("INVENTORY_URL",  "inventory.afyaanalytics.ai"),
    "theatre":    _svc_url("THEATRE_URL",    "theatre.afyaanalytics.ai"),
    "inpatient":  _svc_url("INPATIENT_URL",  "inpatient.afyaanalytics.ai"),
    "dialysis":   _svc_url("DIALYSIS_URL",   "dialysis.afyaanalytics.ai"),
}

# Auth always against core: {AFYA_CORE_URL}v1/login
AFYA_CORE_URL = V3_SERVICES["core"]

S3_BUCKET = os.getenv("S3_BUCKET", "collabmedbucket")
S3_PREFIX  = "raw/v3_gateway"

SF_DB            = "HOSPITALS"
SF_SHARED_SCHEMA = "SHARED"
SF_STAGE         = f"{SF_DB}.{SF_SHARED_SCHEMA}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT   = f"{SF_DB}.{SF_SHARED_SCHEMA}.JSON_FF"
SF_RAW_SCHEMA    = os.getenv("SF_V3_RAW_SCHEMA", "V3_RAW")
SF_RAW_TABLE     = "GATEWAY_RAW"

WATERMARK_FILE = Path(__file__).resolve().parent / ".v3_watermarks.json"
PROGRESS_FILE  = Path(__file__).resolve().parent / ".v3_progress.json"

DEFAULT_PIPELINE_WORKERS = int(os.getenv("PIPELINE_WORKERS", "8"))
DEFAULT_PAGE_WORKERS     = int(os.getenv("PAGE_WORKERS", "4"))
DEFAULT_PER_PAGE         = int(os.getenv("PER_PAGE", "100"))
TOKEN_TTL_SECONDS        = int(os.getenv("TOKEN_TTL_SECONDS", str(50 * 60)))
# Fallback tenant ID used when the login response does not include organization_id.
# Matches FACILITY_V3_CONFIG["kisumu"]["organization_id"] = 1 in the migration script.
DEFAULT_SOURCE_TENANT_ID = int(os.getenv("SOURCE_TENANT_ID", "1"))

# ─── JSON FILE HELPERS ───────────────────────────────────────────────────

def _load_json(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception as e:
            log.warning("Could not parse %s: %s", path, e)
    return {}

def _save_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True))

# ─── WATERMARKS ──────────────────────────────────────────────────────────

def _wm_key(service: str, model: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]", "_", model)
    return f"{service}_{slug}"

def get_watermark(key: str, default: str = "1970-01-01T00:00:00Z") -> str:
    return _load_json(WATERMARK_FILE).get(key, default)

def set_watermark(key: str, ts_iso: str) -> None:
    wm = _load_json(WATERMARK_FILE)
    wm[key] = ts_iso
    _save_json(WATERMARK_FILE, wm)
    log.info("Watermark [%s] → %s", key, ts_iso)

# ─── PROGRESS / RESUME ───────────────────────────────────────────────────

_progress_lock = threading.Lock()

def _job_key(job: dict) -> str:
    return f"{job['service']}|{job['model']}"

def _mark_done(run_id: str, job: dict, s3_key: str | None) -> None:
    """Checkpoint a completed job so a re-run can skip it."""
    with _progress_lock:
        prog   = _load_json(PROGRESS_FILE)
        bucket = prog.setdefault("current_run", {"run_id": run_id, "completed": {}})
        if bucket.get("run_id") != run_id:
            bucket["run_id"] = run_id
        bucket["completed"][_job_key(job)] = {
            "s3_key": s3_key,
            "at":     datetime.now(timezone.utc).isoformat(),
        }
        _save_json(PROGRESS_FILE, prog)

def _clear_progress() -> None:
    with _progress_lock:
        _save_json(PROGRESS_FILE, {})

def _completed_keys() -> set[str]:
    return set(_load_json(PROGRESS_FILE).get("current_run", {}).get("completed", {}).keys())

# ─── SNOWFLAKE CLIENT ────────────────────────────────────────────────────

class SnowflakeClient:
    def __init__(self, schema_: str | None = None):
        required = {
            "SNOWFLAKE_USER":            os.getenv("SNOWFLAKE_USER"),
            "SNOWFLAKE_ACCOUNT":         os.getenv("SNOWFLAKE_ACCOUNT"),
            "SNOWFLAKE_WAREHOUSE":       os.getenv("SNOWFLAKE_WAREHOUSE"),
            "SNOWFLAKE_DATABASE":        os.getenv("SNOWFLAKE_DATABASE"),
            "SNOWFLAKE_PRIVATE_KEY_PATH": os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError(
                f"Missing Snowflake env var(s): {', '.join(missing)} — "
                f"add them to your .env file."
            )
        key_path = required["SNOWFLAKE_PRIVATE_KEY_PATH"].strip()
        if not Path(key_path).exists():
            raise RuntimeError(
                f"Snowflake private key file not found: {key_path} — "
                f"check SNOWFLAKE_PRIVATE_KEY_PATH in your .env."
            )
        try:
            self._conn = snowflake.connector.connect(
                user=required["SNOWFLAKE_USER"].strip(),
                account=required["SNOWFLAKE_ACCOUNT"].strip(),
                warehouse=required["SNOWFLAKE_WAREHOUSE"].strip(),
                database=required["SNOWFLAKE_DATABASE"].strip(),
                schema=schema_ or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC").strip(),
                private_key_file=key_path,
            )
        except Exception as e:
            raise RuntimeError(
                f"Snowflake connection failed — "
                f"account={required['SNOWFLAKE_ACCOUNT']}, "
                f"user={required['SNOWFLAKE_USER']}, "
                f"warehouse={required['SNOWFLAKE_WAREHOUSE']}, "
                f"database={required['SNOWFLAKE_DATABASE']}. "
                f"Cause: {e}"
            ) from e
        self._lock = threading.Lock()

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    @contextmanager
    def _cursor(self):
        cur = self._conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def query(self, sql: str, label: str | None = None) -> pd.DataFrame:
        label = label or f"q:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-30s SELECT | %s…", label, " ".join(sql.split())[:120])
        t0 = time.perf_counter()
        try:
            with self._lock, self._cursor() as cur:
                cur.execute(sql)
                df = cur.fetch_pandas_all()
            log.info("✓ %-30s %s rows · %.2fs", label, f"{len(df):,}", time.perf_counter() - t0)
            return df
        except Exception as e:
            log.exception("✗ %-30s SELECT failed · %s", label, e)
            raise

    def execute(self, sql: str, label: str | None = None) -> dict:
        label = label or f"x:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-30s WRITE  | %s…", label, " ".join(sql.split())[:120])
        t0 = time.perf_counter()
        try:
            with self._lock, self._cursor() as cur:
                cur.execute(sql)
                rowcount, sfqid = cur.rowcount, cur.sfqid
            log.info("✓ %-30s rowcount=%s · %.2fs", label, rowcount, time.perf_counter() - t0)
            return {"rowcount": rowcount, "sfqid": sfqid}
        except Exception as e:
            log.exception("✗ %-30s WRITE failed · %s", label, e)
            raise

    def __enter__(self): return self
    def __exit__(self, *a): self.close()

# ─── HTTP SESSION + TOKEN / TENANT CACHE ────────────────────────────────

_session_singleton: requests.Session | None = None
_session_lock = threading.Lock()
_token_cache:  tuple[str, float] | None = None  # (token, fetched_at)
_token_lock   = threading.Lock()
_tenant_id:    int | None = None  # from login data.organization_id

def _session() -> requests.Session:
    global _session_singleton
    if _session_singleton is None:
        with _session_lock:
            if _session_singleton is None:
                s = requests.Session()
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=32, pool_maxsize=32, max_retries=0,
                )
                s.mount("https://", adapter)
                s.mount("http://", adapter)
                _session_singleton = s
    return _session_singleton

def _get_token() -> str:
    global _token_cache
    with _token_lock:
        if _token_cache and (time.time() - _token_cache[1]) < TOKEN_TTL_SECONDS:
            return _token_cache[0]
        token = _login()
        _token_cache = (token, time.time())
        return token

def _login() -> str:
    global _tenant_id
    username = os.getenv("afya_username") or os.getenv("AFYA_USERNAME")
    password = os.getenv("afya_password") or os.getenv("AFYA_PASSWORD")
    if not username:
        raise RuntimeError(
            "Missing afya_username env var — add it to your .env file. "
            "This is the Afya platform username used to authenticate against "
            f"{AFYA_CORE_URL}v1/login."
        )
    if not password:
        raise RuntimeError(
            "Missing afya_password env var — add it to your .env file. "
            "This is the Afya platform password for the account: %s." % username
        )
    url = f"{AFYA_CORE_URL}v1/login"
    log.info("Authenticating as %s → %s", username, url)
    try:
        r = _session().post(
            url,
            json={"username": username, "password": password, "facility_id": 6},
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=30,
        )
    except Exception as e:
        raise RuntimeError(
            f"Network error reaching login endpoint {url} — "
            f"check that {AFYA_CORE_URL} is reachable. Cause: {e}"
        ) from e

    if r.status_code == 401:
        raise RuntimeError(
            f"Login rejected (401) — wrong username or password for account '{username}'. "
            f"Response: {r.text[:300]}"
        )
    if r.status_code == 422:
        raise RuntimeError(
            f"Login request rejected (422 Unprocessable) — the request body was invalid. "
            f"Possibly facility_id=6 does not exist or is wrong. Response: {r.text[:300]}"
        )
    if r.status_code != 200:
        raise RuntimeError(
            f"Login failed with unexpected status {r.status_code} from {url}. "
            f"Response body: {r.text[:300]}"
        )
    try:
        body = r.json()
    except Exception:
        raise RuntimeError(
            f"Login returned status 200 but the response is not valid JSON. "
            f"Raw response: {r.text[:300]}"
        )
    token = body.get("access_token")
    if not token:
        raise RuntimeError(
            f"Login succeeded (200) but no 'access_token' field in response. "
            f"Keys present: {list(body.keys())}. "
            f"Full response: {r.text[:500]}"
        )
    tenant_id = (body.get("data") or {}).get("organization_id") or body.get("organization_id")
    if tenant_id is not None:
        _tenant_id = int(tenant_id)
        log.info("Authenticated as %s  (tenant_id=%s from login response)", username, _tenant_id)
    else:
        _tenant_id = DEFAULT_SOURCE_TENANT_ID
        log.warning(
            "Login response has no 'organization_id' — "
            "falling back to SOURCE_TENANT_ID=%s (set SOURCE_TENANT_ID in .env to override). "
            "Response keys: %s",
            _tenant_id, list(body.keys()),
        )
        log.info("Authenticated as %s  (tenant_id=%s from fallback)", username, _tenant_id)
    return token

def _auth_headers() -> dict:
    """Build gateway request headers — mirrors v2_to_v3_api_migration._post_to_v3_batch."""
    headers = {
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type":  "application/json",
    }
    if _tenant_id is not None:
        headers["X-Tenant-Id"] = str(_tenant_id)
    return headers

# ─── SERVICE MODEL DISCOVERY ─────────────────────────────────────────────

def _as_list(payload) -> list:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "items", "results", "models"):
            val = payload.get(key)
            if isinstance(val, list):
                return val
        sv = payload.get("success")
        if isinstance(sv, list):
            return sv
        if isinstance(sv, dict):
            for key in ("data", "items"):
                if isinstance(sv.get(key), list):
                    return sv[key]
    return []

def discover_service_models(service: str, service_url: str) -> list[str]:
    """POST action=list to a service gateway and return readable model aliases.

    Response shape (same as v2_to_v3_api_migration._fetch_available_models):
      {"data": [{"alias": "patient", "operations": ["read","insert"], ...}, ...]}
    """
    url = f"{service_url.rstrip('/')}/v1/gateway"
    try:
        r = _session().post(
            url,
            headers={"Authorization": f"Bearer {_get_token()}", "Content-Type": "application/json"},
            json={"action": "list"},
            timeout=30,
        )
        if r.status_code == 401:
            log.warning(
                "Discovery service=%-12s → 401 Unauthorized at %s — "
                "token may have been rejected by this service. Skipping.",
                service, url,
            )
            return []
        if r.status_code == 404:
            log.warning(
                "Discovery service=%-12s → 404 Not Found at %s — "
                "this service URL may be wrong or the /api/v1/gateway endpoint does not exist. "
                "Check %s_URL in your .env. Skipping.",
                service, url, service.upper(),
            )
            return []
        if not r.ok:
            log.warning(
                "Discovery service=%-12s → status=%s at %s — "
                "response: %s. Skipping.",
                service, r.status_code, url, r.text[:200],
            )
            return []
        try:
            body = r.json()
        except Exception:
            log.warning(
                "Discovery service=%-12s → 200 OK but response is not valid JSON. "
                "Raw: %s. Skipping.",
                service, r.text[:200],
            )
            return []
        entries = body.get("data") or []
        if not isinstance(entries, list):
            log.warning(
                "Discovery service=%-12s → 'data' field is %s, expected list. "
                "Full response keys: %s. Skipping.",
                service, type(entries).__name__, list(body.keys()),
            )
            return []
        models = []
        for e in entries:
            alias = e.get("alias")
            if alias:
                models.append(alias)
        log.info("Discovery: service=%-12s  %d models: %s",
                 service, len(models), ", ".join(sorted(models)))
        return models
    except (Timeout, ConnectionError) as e:
        log.warning(
            "Discovery service=%-12s → could not reach %s — "
            "service may be down or URL is wrong. Check %s_URL in .env. "
            "Error: %s. Skipping.",
            service, url, service.upper(), e,
        )
        return []
    except Exception as e:
        log.warning("Discovery service=%-12s → unexpected error: %s. Skipping.", service, e)
        return []

# ─── MODEL LIST LOADER ───────────────────────────────────────────────────

def load_service_models(
    from_cli: list[str] | None = None,
    only_services: set[str] | None = None,
) -> list[tuple[str, str]]:
    """
    Return a deduplicated list of (service, model) pairs.

    Priority: CLI --models > MODELS_FILE env var > auto-discover from every service.
    Expected format for CLI / file: "service:model" strings.
    """
    def _parse(raw: list[str]) -> list[tuple[str, str]]:
        pairs = []
        for entry in raw:
            entry = entry.strip()
            if ":" in entry:
                svc, mdl = entry.split(":", 1)
                pairs.append((svc.strip(), mdl.strip()))
            else:
                log.warning("Ignoring malformed entry (expected service:model): %s", entry)
        return pairs

    if from_cli:
        pairs = _parse(from_cli)
        log.info("Using %d (service, model) pairs from CLI", len(pairs))
        return pairs

    models_file = os.getenv("MODELS_FILE")
    if models_file:
        p = Path(models_file)
        if not p.exists():
            raise RuntimeError(
                f"MODELS_FILE={models_file} does not exist — "
                f"check the path or remove MODELS_FILE from your .env."
            )
        try:
            raw = json.loads(p.read_text())
        except Exception as e:
            raise RuntimeError(
                f"MODELS_FILE={models_file} is not valid JSON — "
                f"expected a list of 'service:model' strings like "
                f'["reception:patient", "finance:invoice"]. '
                f"Parse error: {e}"
            ) from e
        if not isinstance(raw, list):
            raise RuntimeError(
                f"MODELS_FILE={models_file} must be a JSON array, got {type(raw).__name__}. "
                f'Expected format: ["reception:patient", "finance:invoice"]'
            )
        pairs = _parse([n for n in raw if isinstance(n, str)])
        log.info("Loaded %d (service, model) pairs from %s", len(pairs), models_file)
        return pairs

    log.info("No MODELS_FILE or --models — auto-discovering from all services...")
    pairs = []
    services_to_scan = {
        k: v for k, v in V3_SERVICES.items()
        if only_services is None or k in only_services
    }
    for svc, url in services_to_scan.items():
        for model in discover_service_models(svc, url):
            pairs.append((svc, model))

    if not pairs:
        log.warning("Discovery returned no models. Pass --models service:model to run manually.")
    return pairs

# ─── JOB BUILDER ─────────────────────────────────────────────────────────

def build_jobs(
    service_model_pairs: list[tuple[str, str]],
    since: str | None = None,
) -> list[dict]:
    _get_token()  # ensure _tenant_id is populated before building jobs
    jobs = []
    for service, model in service_model_pairs:
        service_url = V3_SERVICES.get(service)
        if not service_url:
            log.warning("Unknown service '%s' for model '%s' — skipping", service, model)
            continue
        wm = since or get_watermark(_wm_key(service, model))
        jobs.append({
            "service":          service,
            "service_url":      service_url,
            "model":            model,
            "source_tenant_id": _tenant_id,
            "updated_since":    wm,
            "per_page":         DEFAULT_PER_PAGE,
        })
    log.info("Built %d jobs", len(jobs))
    return jobs

# ─── GATEWAY PAGINATION ──────────────────────────────────────────────────

def _extract_gateway_rows(payload: dict) -> tuple[list, dict]:
    """Return (rows_list, pagination_dict) from a gateway read response.

    Matches the pattern in v2_to_v3_api_migration._fetch_v3_records:
      rows = payload.get("data") or []
    with a fallback for nested shapes and pagination metadata.
    """
    rows       = payload.get("data") or []
    pagination = payload.get("pagination") or payload.get("meta") or {}

    # Handle nested data dict: {"data": {"data": [...], "current_page": 1, ...}}
    if isinstance(rows, dict):
        pagination = {**rows, **pagination}          # merge — outer wins
        rows       = rows.get("data") or list(rows.values())

    return (rows if isinstance(rows, list) else []), pagination

def _gateway_request(
    job: dict, page: int, *,
    max_retries: int = 6,
    default_wait: int = 10,
    backoff: int = 2,
) -> dict:
    """Single POST to {service_url}/api/v1/gateway with retry for 401/429/5xx/network."""
    body: dict = {
        "action":   "read",
        "model":    job["model"],
        "page":     page,
        "per_page": job["per_page"],
    }
    if job.get("source_tenant_id") is not None:
        body["source_tenant_id"] = job["source_tenant_id"]
    if job.get("updated_since") and job["updated_since"] != "1970-01-01T00:00:00Z":
        body["updated_since"] = job["updated_since"]

    # URL pattern mirrors v2_to_v3_api_migration: {base}/v1/gateway
    # base already ends with /api/ so this becomes https://service.../api/v1/gateway
    gateway_url = f"{job['service_url'].rstrip('/')}/v1/gateway"
    attempt, wait = 0, default_wait
    while True:
        attempt += 1
        try:
            r = _session().post(
                gateway_url,
                headers=_auth_headers(),
                json=body,
                timeout=60,
            )
            log.info("· service=%-12s model=%-25s page=%-4s status=%s",
                     job["service"], job["model"], page, r.status_code)

            if r.status_code == 401:
                global _token_cache
                with _token_lock:
                    _token_cache = None
                if attempt >= max_retries:
                    raise RuntimeError(
                        f"Gateway 401 Unauthorized after {max_retries} token refreshes — "
                        f"service={job['service']} model={job['model']} page={page} url={gateway_url}. "
                        f"The token is being rejected by this service. "
                        f"Response: {r.text[:300]}"
                    )
                log.warning(
                    "  401 Unauthorized — refreshing token and retrying (%s/%s)",
                    attempt, max_retries,
                )
                continue

            if r.status_code == 404:
                raise RuntimeError(
                    f"Gateway 404 Not Found — "
                    f"service={job['service']} model={job['model']} url={gateway_url}. "
                    f"The model alias '{job['model']}' may not exist on this service, "
                    f"or the service URL ({job['service_url']}) is wrong. "
                    f"Response: {r.text[:300]}"
                )

            if r.status_code == 422:
                raise RuntimeError(
                    f"Gateway 422 Unprocessable — "
                    f"service={job['service']} model={job['model']} page={page}. "
                    f"The request body was rejected. "
                    f"Request body sent: {body}. "
                    f"Response: {r.text[:300]}"
                )

            if r.status_code == 429:
                retry_after = default_wait
                try:
                    retry_after = int(r.json().get("retry_after_seconds", default_wait))
                except Exception:
                    pass
                if attempt >= max_retries:
                    raise RuntimeError(
                        f"Gateway 429 rate-limited after {max_retries} retries — "
                        f"service={job['service']} model={job['model']} page={page}. "
                        f"Response: {r.text[:200]}"
                    )
                log.warning(
                    "  429 rate-limited — sleeping %ss then retrying (%s/%s)",
                    retry_after, attempt, max_retries,
                )
                time.sleep(retry_after)
                continue

            if r.status_code in {500, 502, 503, 504}:
                if attempt >= max_retries:
                    raise RuntimeError(
                        f"Gateway {r.status_code} server error after {max_retries} retries — "
                        f"service={job['service']} model={job['model']} page={page} url={gateway_url}. "
                        f"The service is returning errors. "
                        f"Response: {r.text[:300]}"
                    )
                log.warning(
                    "  %s server error — sleeping %ss then retrying (%s/%s) — %s",
                    r.status_code, wait, attempt, max_retries, r.text[:150],
                )
                time.sleep(wait)
                wait = min(wait * backoff, 120)
                continue

            if not r.ok:
                raise RuntimeError(
                    f"Gateway unexpected status {r.status_code} — "
                    f"service={job['service']} model={job['model']} page={page} url={gateway_url}. "
                    f"Response: {r.text[:300]}"
                )

            try:
                return r.json()
            except Exception:
                raise RuntimeError(
                    f"Gateway returned status 200 but response is not valid JSON — "
                    f"service={job['service']} model={job['model']} page={page}. "
                    f"Raw response: {r.text[:300]}"
                )

        except (Timeout, ConnectionError) as e:
            if attempt >= max_retries:
                raise RuntimeError(
                    f"Network error after {max_retries} retries — "
                    f"service={job['service']} model={job['model']} page={page} url={gateway_url}. "
                    f"The service may be unreachable. Cause: {e}"
                ) from e
            log.warning(
                "  Network error page=%s — sleeping %ss then retrying (%s/%s): %s",
                page, wait, attempt, max_retries, e,
            )
            time.sleep(wait)
            wait = min(wait * backoff, 120)

def fetch_all_pages(
    job: dict, *,
    max_pages: int = 10_000,
    page_workers: int = DEFAULT_PAGE_WORKERS,
) -> list:
    """
    Page 1 fetched sequentially (discovers last_page), then pages 2..N
    fetched concurrently.  Falls back to sequential if last_page unknown.
    """
    payload1          = _gateway_request(job, 1)
    all_rows, pagination = _extract_gateway_rows(payload1)

    last_page = (
        pagination.get("last_page")
        or pagination.get("total_pages")
        or pagination.get("pageCount")
    )
    has_more = (
        bool(pagination.get("has_more_pages"))
        or bool(pagination.get("hasMorePages"))
        or (last_page is not None and int(last_page) > 1)
    )

    # If page 1 returned fewer rows than per_page it's the only page
    if not all_rows or len(all_rows) < job["per_page"]:
        return all_rows

    if not has_more:
        return all_rows

    # ── Fan-out when we know last_page ───────────────────────────────────
    if last_page is not None:
        last_page = min(int(last_page), max_pages)
        pages = list(range(2, last_page + 1))

        def _fetch(p):
            rows, _ = _extract_gateway_rows(_gateway_request(job, p))
            return p, rows

        with ThreadPoolExecutor(max_workers=max(1, page_workers)) as pool:
            page_rows: dict[int, list] = {}
            for fut in as_completed(pool.submit(_fetch, p) for p in pages):
                p, rows = fut.result()
                page_rows[p] = rows

        for p in pages:
            all_rows.extend(page_rows.get(p, []))
        return all_rows

    # ── Sequential fallback (mirrors _fetch_v3_records in migration script) ──
    # Stop when the page returns fewer rows than per_page — that's the last page.
    page = 1
    per_page = job["per_page"]
    while has_more and page < max_pages:
        page += 1
        rows, pagination = _extract_gateway_rows(_gateway_request(job, page))
        all_rows.extend(rows)
        if not rows or len(rows) < per_page:
            break
        has_more = (
            bool(pagination.get("has_more_pages"))
            or bool(pagination.get("hasMorePages"))
            or bool(rows)
        )

    return all_rows

# ─── S3 ──────────────────────────────────────────────────────────────────

_s3_singleton = None
_s3_lock      = threading.Lock()

def _s3():
    global _s3_singleton
    if _s3_singleton is None:
        with _s3_lock:
            if _s3_singleton is None:
                ak = os.getenv("AWS_ACCESS_KEY_ID")
                sk = os.getenv("AWS_SECRET_ACCESS_KEY")
                if not ak:
                    raise RuntimeError(
                        "Missing AWS_ACCESS_KEY_ID env var — add it to your .env file."
                    )
                if not sk:
                    raise RuntimeError(
                        "Missing AWS_SECRET_ACCESS_KEY env var — add it to your .env file."
                    )
                region = os.getenv("AWS_REGION", "us-east-1")
                try:
                    _s3_singleton = boto3.client(
                        "s3",
                        aws_access_key_id=ak,
                        aws_secret_access_key=sk,
                        region_name=region,
                    )
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to create S3 client — "
                        f"region={region}. Cause: {e}"
                    ) from e
    return _s3_singleton

def _safe(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-=\.\+]+", "_", (s or "").strip())

# ─── EXTRACT ONE JOB ─────────────────────────────────────────────────────

def extract_one_job(
    job: dict,
    run_id: str,
    *,
    dry_run: bool = False,
    page_workers: int = DEFAULT_PAGE_WORKERS,
) -> dict | None:
    """Fetch all rows for the job, upload to S3. Returns result dict or None."""
    rows = fetch_all_pages(job, page_workers=page_workers)

    if not rows:
        log.info("    service=%-12s model=%-25s — 0 rows, skipping",
                 job["service"], job["model"])
        return None

    ingested_at = datetime.now(timezone.utc)
    dt          = ingested_at.date().isoformat()
    model_safe  = _safe(job["model"])
    key = (
        f"{S3_PREFIX}/"
        f"service={job['service']}/"
        f"model={model_safe}/"
        f"dt={dt}/"
        f"{run_id}.jsonl.gz"
    )

    jsonl_bytes = b"\n".join(_dumps_bytes(r) for r in rows) + b"\n"

    if dry_run:
        log.info("DRY-RUN ✓ service=%-12s model=%-25s %d rows → s3://%s/%s",
                 job["service"], job["model"], len(rows), S3_BUCKET, key)
        return None

    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(jsonl_bytes)
    try:
        _s3().put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    except Exception as e:
        raise RuntimeError(
            f"S3 upload failed — bucket={S3_BUCKET} key={key} "
            f"service={job['service']} model={job['model']} rows={len(rows)}. "
            f"Cause: {e}"
        ) from e
    log.info("Uploaded s3://%s/%s  rows=%d", S3_BUCKET, key, len(rows))

    return {
        "service":       job["service"],
        "model":         job["model"],
        "updated_since": job.get("updated_since", ""),
        "ingested_at":   ingested_at.isoformat(),
        "s3_key":        key,
        "row_count":     len(rows),
    }

# ─── SNOWFLAKE COPY ──────────────────────────────────────────────────────

def copy_into_snowflake(result: dict, sf: SnowflakeClient | None = None) -> None:
    table       = f"{SF_DB}.{SF_RAW_SCHEMA}.{SF_RAW_TABLE}"
    service     = result["service"].replace("'", "\\'")
    model       = result["model"].replace("'", "\\'")
    ingested_at = result["ingested_at"]
    s3_key      = result["s3_key"]

    sql = f"""
    COPY INTO {table} (service, model, ingested_at, payload)
    FROM (
      SELECT
        '{service}'::STRING               AS service,
        '{model}'::STRING                 AS model,
        '{ingested_at}'::TIMESTAMP_TZ     AS ingested_at,
        PARSE_JSON($1)                    AS payload
      FROM @{SF_STAGE}
    )
    FILES = ('{s3_key}')
    FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
    ON_ERROR = 'CONTINUE';
    """
    label = f"copy:{service}:{model}"
    try:
        if sf is not None:
            sf.execute(sql, label=label)
        else:
            with SnowflakeClient(schema_=f"{SF_DB}.{SF_RAW_SCHEMA}") as new_sf:
                new_sf.execute(sql, label=label)
    except Exception as e:
        raise RuntimeError(
            f"Snowflake COPY INTO failed — "
            f"table={SF_DB}.{SF_RAW_SCHEMA}.{SF_RAW_TABLE} "
            f"service={service} model={model} s3_key={s3_key}. "
            f"Cause: {e}"
        ) from e

# ─── SNOWFLAKE TABLE BOOTSTRAP ───────────────────────────────────────────

def _ensure_table(sf: SnowflakeClient) -> None:
    """Create the raw schema and GATEWAY_RAW table if they don't already exist."""
    table  = f"{SF_DB}.{SF_RAW_SCHEMA}.{SF_RAW_TABLE}"
    schema = f"{SF_DB}.{SF_RAW_SCHEMA}"

    sf.execute(
        f"CREATE SCHEMA IF NOT EXISTS {schema};",
        label="ensure_schema",
    )
    sf.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            service      STRING        NOT NULL,
            model        STRING        NOT NULL,
            ingested_at  TIMESTAMP_TZ  NOT NULL,
            payload      VARIANT
        );
        """,
        label="ensure_table",
    )
    log.info("Table ready: %s", table)

# ─── ORCHESTRATOR ────────────────────────────────────────────────────────

def run_pipeline(
    service_model_pairs: list[tuple[str, str]],
    *,
    since: str | None = None,
    dry_run: bool = False,
    update_watermark: bool = True,
    resume: bool = True,
    workers: int = DEFAULT_PIPELINE_WORKERS,
    page_workers: int = DEFAULT_PAGE_WORKERS,
) -> None:

    run_id     = datetime.now(timezone.utc).strftime("v3__%Y-%m-%dT%H-%M-%SZ")
    started_at = datetime.now(timezone.utc)

    all_jobs = build_jobs(service_model_pairs, since=since)
    if not all_jobs:
        log.warning("No jobs to run.")
        return

    if not resume and not dry_run:
        _clear_progress()
        log.info("⟲ Resume disabled — cleared previous progress")
        jobs = all_jobs
    elif resume and not dry_run:
        done    = _completed_keys()
        skipped = [j for j in all_jobs if _job_key(j) in done]
        jobs    = [j for j in all_jobs if _job_key(j) not in done]
        if skipped:
            log.info("⟲ Resume: skipping %d already-done jobs (use --no-resume to redo)",
                     len(skipped))
    else:
        jobs = all_jobs

    log.info(
        "══════ START %s · run=%s · %d/%d jobs · workers=%d (page=%d) ══════",
        PIPELINE_NAME, run_id, len(jobs), len(all_jobs), workers, page_workers,
    )

    successes: list[dict] = []
    failures:  list[dict] = []

    sf_client = None
    try:
        if not dry_run:
            sf_client = SnowflakeClient(schema_=f"{SF_DB}.{SF_RAW_SCHEMA}")
            _ensure_table(sf_client)

        def _do_one(idx_and_job):
            idx, job = idx_and_job
            svc, mdl = job["service"], job["model"]
            log.info("──[%d/%d] start · service=%-12s model=%s", idx, len(jobs), svc, mdl)
            stage = "api_fetch"
            try:
                result = extract_one_job(job, run_id=run_id,
                                         dry_run=dry_run, page_workers=page_workers)
                if result is None:
                    if not dry_run:
                        _mark_done(run_id, job, s3_key=None)
                    return ("skip", None, job)

                stage = "snowflake_copy"
                copy_into_snowflake(result, sf=sf_client)
                _mark_done(run_id, job, s3_key=result["s3_key"])
                return ("ok", result, job)

            except Exception as e:
                log.error(
                    "✗ [%s] service=%-12s model=%-25s FAILED at stage=%s — %s",
                    idx, svc, mdl, stage, e, exc_info=True,
                )
                return ("err", f"[{stage}] {e}", job)

        if jobs:
            with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
                for status, payload, job in pool.map(
                    _do_one, list(enumerate(jobs, start=1))
                ):
                    if status == "ok":
                        successes.append(payload)
                    elif status == "err":
                        failures.append({"job": job, "error": payload})

    finally:
        if sf_client is not None:
            sf_client.close()

    # Advance per-job watermarks only on a completely clean run
    if not dry_run and update_watermark and not failures:
        ts = started_at.isoformat().replace("+00:00", "Z")
        for job in all_jobs:
            set_watermark(_wm_key(job["service"], job["model"]), ts)
        _clear_progress()
        log.info("✓ Clean run — watermarks advanced, progress file cleared.")

    log.info("══════ END   ✓ %d ok · ✗ %d failed · %s ══════",
             len(successes), len(failures), PIPELINE_NAME)

    if failures:
        log.warning(
            "%d job(s) failed — watermark NOT advanced, re-run will retry them:",
            len(failures),
        )
        for f in failures[:10]:
            job = f.get("job", {})
            err = str(f.get("error", ""))
            log.warning(
                "  · service=%-12s model=%-25s  %s",
                job.get("service"), job.get("model"), err[:200],
            )
        if len(failures) > 10:
            log.warning("  … and %d more (see logs above for details)", len(failures) - 10)
        sys.exit(1)

# ─── CLI ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--models",
        help=(
            'Comma-separated service:model pairs, '
            'e.g. "reception:patient,finance:invoice". '
            "Overrides MODELS_FILE."
        ),
    )
    ap.add_argument(
        "--services",
        help=(
            "Comma-separated service names to restrict to when auto-discovering, "
            "e.g. reception,finance."
        ),
    )
    ap.add_argument(
        "--since",
        help="ISO timestamp override for updated_since on every job (skips per-job watermarks).",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Extract and log only — no S3 or Snowflake writes.",
    )
    ap.add_argument(
        "--no-resume", action="store_true",
        help="Ignore .v3_progress.json and re-run every job from scratch.",
    )
    ap.add_argument(
        "--no-watermark-update", action="store_true",
        help="Do not advance watermarks on a clean run.",
    )
    ap.add_argument(
        "--workers", type=int, default=DEFAULT_PIPELINE_WORKERS,
        help=f"Parallel jobs (default {DEFAULT_PIPELINE_WORKERS}).",
    )
    ap.add_argument(
        "--page-workers", type=int, default=DEFAULT_PAGE_WORKERS,
        help=f"Parallel pages within a job (default {DEFAULT_PAGE_WORKERS}).",
    )
    args = ap.parse_args()

    models_from_cli = None
    if args.models:
        models_from_cli = [m.strip() for m in args.models.split(",") if m.strip()]

    only_services = None
    if args.services:
        only_services = {s.strip() for s in args.services.split(",") if s.strip()}

    pairs = load_service_models(from_cli=models_from_cli, only_services=only_services)
    if not pairs:
        ap.error(
            "No (service, model) pairs to extract. "
            "Use --models, set MODELS_FILE, or configure service URLs for auto-discovery."
        )

    run_pipeline(
        service_model_pairs=pairs,
        since=args.since,
        dry_run=args.dry_run,
        update_watermark=not args.no_watermark_update,
        resume=not args.no_resume,
        workers=args.workers,
        page_workers=args.page_workers,
    )


if __name__ == "__main__":
    main()
