#!/usr/bin/env python3
"""
v3_api_to_snowflake_raw.py  —  Afya Extraction API (Model Gateway) → S3 → Snowflake RAW

Flow:
  1. Authenticate against the Afya Extraction API  →  cache bearer token (50-min TTL)
  2. Discover facilities/systems via the lookup cascade:
       GET /lookup/counties → GET /lookup/facilities?county_id=X → GET /lookup/systems?facility_id=Y
  3. Build one job per (facility, system, namespace) triple.
  4. For each job: POST /gateway action=get → paginate all pages → gzipped JSONL → S3 → COPY INTO Snowflake RAW
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
  python v3_api_to_snowflake_raw.py --facility-ids 2,3
  python v3_api_to_snowflake_raw.py --system-ids 5,6
  python v3_api_to_snowflake_raw.py --namespaces "App\\\\Models\\\\User,App\\\\Models\\\\Store\\\\InventoryItem"
  python v3_api_to_snowflake_raw.py --since 2025-01-01
  python v3_api_to_snowflake_raw.py --dry-run
  python v3_api_to_snowflake_raw.py --no-resume
  python v3_api_to_snowflake_raw.py --skip-discovery --namespaces "App\\\\Models\\\\User"
  python v3_api_to_snowflake_raw.py --workers 4 --page-workers 4

ENV VARS  (place in a .env file next to this script)
  # Afya Extraction API
  AFYA_API_BASE_URL=https://afyapi.afyaanalytics.ai/api
  AFYA_USERNAME=admin
  AFYA_PASSWORD=Afya@extract26

  # Snowflake (key-pair auth — same as the facility pipeline)
  SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_PRIVATE_KEY_PATH

  # AWS
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

  # Optional: path to a JSON file containing the list of namespaces to extract
  # e.g. ["App\\\\Models\\\\User", "App\\\\Models\\\\Store\\\\InventoryItem"]
  NAMESPACES_FILE=namespaces.json

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
from requests.exceptions import ConnectionError, HTTPError, Timeout

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

AFYA_BASE_URL = os.getenv("AFYA_API_BASE_URL", "https://afyapi.afyaanalytics.ai/api").rstrip("/")

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

def _wm_key(facility_id: int, system_id: int, namespace: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]", "_", namespace)
    return f"f{facility_id}_s{system_id}_{slug}"

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
    return f"{job['facility_id']}|{job['system_id']}|{job['namespace']}"

def _mark_done(run_id: str, job: dict, s3_key: str | None) -> None:
    """Checkpoint a completed job so a re-run can skip it."""
    with _progress_lock:
        prog = _load_json(PROGRESS_FILE)
        bucket = prog.setdefault("current_run", {"run_id": run_id, "completed": {}})
        if bucket.get("run_id") != run_id:
            bucket["run_id"] = run_id
        bucket["completed"][_job_key(job)] = {
            "s3_key": s3_key,
            "at": datetime.now(timezone.utc).isoformat(),
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
        with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip(), "rb") as f:
            f.read()  # presence check
        self._conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER").strip(),
            account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(),
            database=os.getenv("SNOWFLAKE_DATABASE").strip(),
            schema=schema_ or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC").strip(),
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip(),
        )
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

# ─── HTTP SESSION + TOKEN CACHE ──────────────────────────────────────────

_session_singleton: requests.Session | None = None
_session_lock = threading.Lock()
_token_cache: tuple[str, float] | None = None  # (token, fetched_at)
_token_lock = threading.Lock()

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
    username = os.getenv("AFYA_USERNAME")
    password = os.getenv("AFYA_PASSWORD")
    if not username or not password:
        raise RuntimeError("Set AFYA_USERNAME + AFYA_PASSWORD in .env")
    r = _session().post(
        f"{AFYA_BASE_URL}/auth/login",
        json={"username": username, "password": password},
        headers={"Accept": "application/json"},
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Afya login failed: {r.status_code} · {r.text[:300]}")
    token = r.json().get("token")
    if not token:
        raise RuntimeError(f"No token in login response: {r.text[:300]}")
    log.info("Authenticated to Afya API as %s", username)
    return token

def _auth_headers() -> dict:
    return {
        "Authorization": f"Bearer {_get_token()}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

# ─── LOOKUP / DISCOVERY ──────────────────────────────────────────────────

def _get(path: str, params: dict | None = None, timeout: int = 30) -> list | dict:
    r = _session().get(
        f"{AFYA_BASE_URL}/{path.lstrip('/')}",
        headers=_auth_headers(),
        params=params,
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()

def _as_list(payload) -> list[dict]:
    """Normalize various response shapes to a plain list."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "items", "results",
                    "counties", "facilities", "systems", "models"):
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

def discover_facilities_and_systems(
    only_facility_ids: set[int] | None = None,
    only_system_ids: set[int] | None = None,
) -> list[dict]:
    """
    Walk the county → facility → system cascade.
    Returns a list of dicts with facility_id, facility_name, system_id, system_name.
    """
    results = []
    counties = _as_list(_get("/lookup/counties"))
    log.info("Discovery: %d counties", len(counties))

    for county in counties:
        county_id   = county.get("id")
        county_name = county.get("name", str(county_id))
        facilities  = _as_list(_get("/lookup/facilities", {"county_id": county_id}))

        for facility in facilities:
            fid   = facility.get("id")
            fname = facility.get("name", str(fid))

            if only_facility_ids and fid not in only_facility_ids:
                continue

            systems = _as_list(_get("/lookup/systems", {"facility_id": fid}))
            for system in systems:
                sid   = system.get("id")
                sname = system.get("name", str(sid))

                if only_system_ids and sid not in only_system_ids:
                    continue

                results.append({
                    "county_id":     county_id,
                    "county_name":   county_name,
                    "facility_id":   fid,
                    "facility_name": fname,
                    "system_id":     sid,
                    "system_name":   sname,
                })

    log.info("Discovery complete: %d (facility, system) pairs", len(results))
    return results

# ─── NAMESPACE LOADER ────────────────────────────────────────────────────

def load_namespaces(from_cli: list[str] | None = None) -> list[str]:
    """
    Priority: CLI --namespaces > NAMESPACES_FILE env var > default examples.
    Returns de-duplicated list.
    """
    if from_cli:
        ns = [n.strip() for n in from_cli if n.strip()]
        log.info("Using %d namespaces from CLI", len(ns))
        return ns

    ns_file = os.getenv("NAMESPACES_FILE")
    if ns_file and Path(ns_file).exists():
        raw = json.loads(Path(ns_file).read_text())
        ns  = [n.strip() for n in raw if isinstance(n, str) and n.strip()]
        log.info("Loaded %d namespaces from %s", len(ns), ns_file)
        return ns

    log.warning(
        "No namespaces configured — set NAMESPACES_FILE or pass --namespaces. "
        "Running with built-in examples only."
    )
    return [
        "App\\Models\\User",
        "App\\Models\\Store\\InventoryItem",
    ]

# ─── JOB BUILDER ─────────────────────────────────────────────────────────

def build_jobs(
    facility_system_pairs: list[dict],
    namespaces: list[str],
    since: str | None = None,
) -> list[dict]:
    jobs = []
    for pair in facility_system_pairs:
        fid, sid = pair["facility_id"], pair["system_id"]
        for namespace in namespaces:
            wm = since or get_watermark(_wm_key(fid, sid, namespace))
            jobs.append({
                **pair,
                "namespace":     namespace,
                "updated_since": wm,
                "per_page":      DEFAULT_PER_PAGE,
            })
    log.info("Built %d jobs", len(jobs))
    return jobs

# ─── GATEWAY PAGINATION ──────────────────────────────────────────────────

def _extract_gateway_rows(payload: dict) -> tuple[list, dict]:
    """Return (rows_list, pagination_dict) from a gateway response."""
    rows = None
    pagination = {}

    if isinstance(payload, dict):
        rows       = payload.get("data")
        pagination = payload.get("pagination") or payload.get("meta") or {}

        if rows is None:
            sv = payload.get("success")
            if isinstance(sv, dict):
                rows       = sv.get("data")
                pagination = sv.get("pagination") or sv.get("meta") or pagination
            elif isinstance(sv, list):
                rows = sv

        if isinstance(rows, dict):
            rows = rows.get("data") or list(rows.values())

    return (rows if isinstance(rows, list) else []), pagination

def _gateway_request(
    job: dict, page: int, *,
    max_retries: int = 6,
    default_wait: int = 10,
    backoff: int = 2,
) -> dict:
    """Single POST to /gateway with retry for 429/5xx/network errors."""
    body: dict = {
        "namespace":   job["namespace"],
        "action":      "get",
        "facility_id": job["facility_id"],
        "system_id":   job["system_id"],
        "page":        page,
        "per_page":    job["per_page"],
    }
    if job.get("updated_since") and job["updated_since"] != "1970-01-01T00:00:00Z":
        body["updated_since"] = job["updated_since"]

    attempt, wait = 0, default_wait
    while True:
        attempt += 1
        try:
            r = _session().post(
                f"{AFYA_BASE_URL}/gateway",
                headers=_auth_headers(),
                json=body,
                timeout=60,
            )
            ns_short = job["namespace"].split("\\")[-1]
            log.info("· f=%s s=%s ns=%s page=%s status=%s",
                     job["facility_id"], job["system_id"], ns_short, page, r.status_code)

            if r.status_code == 401:
                # Token may have expired mid-run — force a refresh
                global _token_cache
                with _token_lock:
                    _token_cache = None
                if attempt >= max_retries:
                    r.raise_for_status()
                continue

            if r.status_code == 429:
                retry_after = default_wait
                try:
                    retry_after = int(r.json().get("retry_after_seconds", default_wait))
                except Exception:
                    pass
                if attempt >= max_retries:
                    r.raise_for_status()
                log.warning("429 — sleeping %ss (%s/%s)", retry_after, attempt, max_retries)
                time.sleep(retry_after)
                continue

            if r.status_code in {500, 502, 503, 504}:
                if attempt >= max_retries:
                    r.raise_for_status()
                log.warning("5xx(%s) — sleeping %ss (%s/%s)", r.status_code, wait, attempt, max_retries)
                time.sleep(wait)
                wait = min(wait * backoff, 120)
                continue

            r.raise_for_status()
            return r.json()

        except (Timeout, ConnectionError) as e:
            if attempt >= max_retries:
                raise
            log.warning("Network error page=%s · %s · sleeping %ss (%s/%s)",
                        page, e, wait, attempt, max_retries)
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
    payload1 = _gateway_request(job, 1)
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

    if not has_more or not all_rows:
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

    # ── Sequential fallback ───────────────────────────────────────────────
    page = 1
    while has_more and page < max_pages:
        page += 1
        rows, pagination = _extract_gateway_rows(_gateway_request(job, page))
        all_rows.extend(rows)
        last_page = pagination.get("last_page") or pagination.get("total_pages")
        has_more  = (
            bool(pagination.get("has_more_pages"))
            or bool(pagination.get("hasMorePages"))
            or (bool(rows) and not pagination)
        )
        if not rows:
            break

    return all_rows

# ─── S3 ──────────────────────────────────────────────────────────────────

_s3_singleton = None
_s3_lock = threading.Lock()

def _s3():
    global _s3_singleton
    if _s3_singleton is None:
        with _s3_lock:
            if _s3_singleton is None:
                ak = os.getenv("AWS_ACCESS_KEY_ID")
                sk = os.getenv("AWS_SECRET_ACCESS_KEY")
                if not (ak and sk):
                    raise RuntimeError("Set AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY")
                _s3_singleton = boto3.client(
                    "s3",
                    aws_access_key_id=ak,
                    aws_secret_access_key=sk,
                    region_name=os.getenv("AWS_REGION", "us-east-1"),
                )
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
        log.info("    f=%s s=%s ns=%s — 0 rows, skipping",
                 job["facility_id"], job["system_id"], job["namespace"].split("\\")[-1])
        return None

    ingested_at = datetime.now(timezone.utc)
    dt          = ingested_at.date().isoformat()
    ns_safe     = _safe(job["namespace"].replace("\\", "_"))
    key = (
        f"{S3_PREFIX}/"
        f"facility_id={job['facility_id']}/"
        f"system_id={job['system_id']}/"
        f"namespace={ns_safe}/"
        f"dt={dt}/"
        f"{run_id}.jsonl.gz"
    )

    jsonl_bytes = b"\n".join(_dumps_bytes(r) for r in rows) + b"\n"

    if dry_run:
        log.info("DRY-RUN ✓ f=%s s=%s ns=%s — %d rows (would → s3://%s/%s)",
                 job["facility_id"], job["system_id"],
                 job["namespace"].split("\\")[-1], len(rows), S3_BUCKET, key)
        return None

    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(jsonl_bytes)
    _s3().put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    log.info("Uploaded s3://%s/%s  rows=%d", S3_BUCKET, key, len(rows))

    return {
        "facility_id":   job["facility_id"],
        "facility_name": job.get("facility_name", ""),
        "system_id":     job["system_id"],
        "system_name":   job.get("system_name", ""),
        "namespace":     job["namespace"],
        "updated_since": job.get("updated_since", ""),
        "ingested_at":   ingested_at.isoformat(),
        "s3_key":        key,
        "row_count":     len(rows),
    }

# ─── SNOWFLAKE COPY ──────────────────────────────────────────────────────

def copy_into_snowflake(result: dict, sf: SnowflakeClient | None = None) -> None:
    table       = f"{SF_DB}.{SF_RAW_SCHEMA}.{SF_RAW_TABLE}"
    ingested_at = result["ingested_at"]
    facility_id = result["facility_id"]
    system_id   = result["system_id"]
    namespace   = result["namespace"].replace("'", "\\'")
    s3_key      = result["s3_key"]

    sql = f"""
    COPY INTO {table} (facility_id, system_id, namespace, ingested_at, payload)
    FROM (
      SELECT
        {facility_id}::INTEGER            AS facility_id,
        {system_id}::INTEGER              AS system_id,
        '{namespace}'::STRING             AS namespace,
        '{ingested_at}'::TIMESTAMP_TZ     AS ingested_at,
        PARSE_JSON($1)                    AS payload
      FROM @{SF_STAGE}
    )
    FILES = ('{s3_key}')
    FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
    ON_ERROR = 'CONTINUE';
    """
    label = f"copy:f{facility_id}:s{system_id}:{namespace.split(chr(92))[-1]}"
    if sf is not None:
        sf.execute(sql, label=label)
    else:
        with SnowflakeClient(schema_=f"{SF_DB}.{SF_RAW_SCHEMA}") as new_sf:
            new_sf.execute(sql, label=label)

# ─── ORCHESTRATOR ────────────────────────────────────────────────────────

def run_pipeline(
    namespaces: list[str],
    *,
    only_facility_ids: set[int] | None = None,
    only_system_ids: set[int] | None = None,
    skip_discovery: bool = False,
    manual_pairs: list[dict] | None = None,
    since: str | None = None,
    dry_run: bool = False,
    update_watermark: bool = True,
    resume: bool = True,
    workers: int = DEFAULT_PIPELINE_WORKERS,
    page_workers: int = DEFAULT_PAGE_WORKERS,
) -> None:

    run_id     = datetime.now(timezone.utc).strftime("v3__%Y-%m-%dT%H-%M-%SZ")
    started_at = datetime.now(timezone.utc)

    # 1. Discover facilities / systems
    if skip_discovery and manual_pairs:
        pairs = manual_pairs
    else:
        pairs = discover_facilities_and_systems(
            only_facility_ids=only_facility_ids,
            only_system_ids=only_system_ids,
        )

    if not pairs:
        log.warning("No (facility, system) pairs found — nothing to do.")
        return

    # 2. Build full job list
    all_jobs = build_jobs(pairs, namespaces, since=since)
    if not all_jobs:
        log.warning("No jobs to run.")
        return

    # 3. Resume filter
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

        def _do_one(idx_and_job):
            idx, job = idx_and_job
            log.info(
                "──[%d/%d] start · f=%s(%s) s=%s(%s) ns=%s",
                idx, len(jobs),
                job["facility_id"], job.get("facility_name", ""),
                job["system_id"],   job.get("system_name", ""),
                job["namespace"].split("\\")[-1],
            )
            try:
                result = extract_one_job(job, run_id=run_id,
                                         dry_run=dry_run, page_workers=page_workers)
                if result is None:
                    if not dry_run:
                        _mark_done(run_id, job, s3_key=None)
                    return ("skip", None, job)
                copy_into_snowflake(result, sf=sf_client)
                _mark_done(run_id, job, s3_key=result["s3_key"])
                return ("ok", result, job)
            except Exception as e:
                log.error("✗ f=%s s=%s ns=%s · %s",
                          job["facility_id"], job["system_id"],
                          job["namespace"].split("\\")[-1], e, exc_info=True)
                return ("err", str(e), job)

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

    # 4. Advance per-job watermarks only on a clean run
    if not dry_run and update_watermark and not failures:
        ts = started_at.isoformat().replace("+00:00", "Z")
        for job in all_jobs:
            set_watermark(_wm_key(job["facility_id"], job["system_id"], job["namespace"]), ts)
        _clear_progress()
        log.info("✓ Clean run — watermarks advanced, progress file cleared.")

    log.info("══════ END   ✓ %d ok · ✗ %d failed · %s ══════",
             len(successes), len(failures), PIPELINE_NAME)

    if failures:
        log.warning(
            "Failed jobs (watermark NOT advanced — re-run will retry them):"
        )
        for f in failures[:10]:
            job = f.get("job", {})
            log.warning("  · f=%s s=%s ns=%s  err=%s",
                        job.get("facility_id"), job.get("system_id"),
                        (job.get("namespace") or "").split("\\")[-1],
                        str(f.get("error", ""))[:120])
        sys.exit(1)

# ─── CLI ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--namespaces",
        help=(
            'Comma-separated full class paths to extract, e.g. '
            '"App\\\\Models\\\\User,App\\\\Models\\\\Store\\\\InventoryItem". '
            "Overrides NAMESPACES_FILE."
        ),
    )
    ap.add_argument(
        "--facility-ids",
        help="Comma-separated integer facility IDs to restrict the run to.",
    )
    ap.add_argument(
        "--system-ids",
        help="Comma-separated integer system IDs to restrict the run to.",
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
        "--skip-discovery", action="store_true",
        help=(
            "Skip the county/facility/system lookup cascade. "
            "Requires --facility-ids and --system-ids."
        ),
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

    ns_list = None
    if args.namespaces:
        ns_list = [n.strip() for n in args.namespaces.split(",") if n.strip()]

    facility_ids = None
    if args.facility_ids:
        facility_ids = {int(x) for x in args.facility_ids.split(",") if x.strip()}

    system_ids = None
    if args.system_ids:
        system_ids = {int(x) for x in args.system_ids.split(",") if x.strip()}

    manual_pairs = None
    if args.skip_discovery:
        if not facility_ids or not system_ids:
            ap.error("--skip-discovery requires both --facility-ids and --system-ids")
        manual_pairs = [
            {"facility_id": fid, "system_id": sid,
             "facility_name": str(fid), "system_name": str(sid),
             "county_id": None, "county_name": ""}
            for fid in facility_ids
            for sid in system_ids
        ]

    namespaces = load_namespaces(ns_list)
    if not namespaces:
        ap.error("No namespaces to extract. Use --namespaces or set NAMESPACES_FILE.")

    run_pipeline(
        namespaces=namespaces,
        only_facility_ids=facility_ids,
        only_system_ids=system_ids,
        skip_discovery=args.skip_discovery,
        manual_pairs=manual_pairs,
        since=args.since,
        dry_run=args.dry_run,
        update_watermark=not args.no_watermark_update,
        resume=not args.no_resume,
        workers=args.workers,
        page_workers=args.page_workers,
    )


if __name__ == "__main__":
    main()
