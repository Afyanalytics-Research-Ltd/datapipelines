#!/usr/bin/env python3
"""
facility_api_to_snowflake.py  —  standalone (no Airflow) pipeline runner.

  1. Reads the data-dictionary Google Sheet to know which (module, table)
     pairs to extract.
  2. For each pair, calls the facility's /api/finance/access/data/point
     endpoint (with namespace + singular/double-namespace fallbacks),
     paginating until done.  Pagination after page 1 runs concurrently.
  3. Writes each model's payload as gzipped JSONL to S3.
  4. COPYs each S3 file into the facility's RAW.EVENTS_RAW table.
  5. MERGEs RAW.EVENTS_RAW → CLEAN.EVENTS.

PERFORMANCE
  · Jobs run in a thread pool (PIPELINE_WORKERS, default 8).
  · One requests.Session + one cached auth token per facility.
  · One Snowflake connection shared across every COPY for the run.
  · Pages 2..N within a job fetched concurrently (PAGE_WORKERS, default 4).
  · orjson used for JSONL encoding when available.

USAGE
  python facility_api_to_snowflake.py --facility kisumu
  python facility_api_to_snowflake.py --facility xanalife --since 2025-09-01
  python facility_api_to_snowflake.py --facility xanalife --skip-merge
  python facility_api_to_snowflake.py --facility xanalife --only-tables sales,patients
  python facility_api_to_snowflake.py --facility xanalife --dry-run
  python facility_api_to_snowflake.py --facility xanalife --workers 4

ENV VARS  (put them in a `.env` file next to this script — auto-loaded)
  # Snowflake (key-pair auth)
  SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_PRIVATE_KEY_PATH

  # AWS  (or use the default boto3 credential chain — ~/.aws/credentials)
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

  # Google service account for the data-dictionary sheet
  GOOGLE_SA_JSON_PATH=/abs/path/to/service-account.json
  IGNITE_SHEET_ID=...
  IGNITE_SHEET_WORKSHEET=Sheet1

  # Facility API credentials  (per facility you want to run)
  FACILITY_KISUMU_USERNAME=...,    FACILITY_KISUMU_PASSWORD=...
  FACILITY_XANALIFE_USERNAME=...,  FACILITY_XANALIFE_PASSWORD=...

  # Tuning
  PIPELINE_WORKERS=8        # parallel jobs (tables) — drop to 4 if you see 429s
  PAGE_WORKERS=4            # parallel pages within a job
  LOG_LEVEL=INFO            # set DEBUG to see per-page response bodies
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
from functools import lru_cache
from io import BytesIO
from pathlib import Path

import boto3
import gspread
import pandas as pd
import requests
import requests.adapters
import snowflake.connector
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, HTTPError, Timeout

# Optional: orjson is ~3× faster than stdlib json for encoding rows.
try:
    import orjson
    def _dumps_bytes(obj) -> bytes:
        return orjson.dumps(obj)
except ImportError:
    def _dumps_bytes(obj) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

# Load .env from same dir as this script
load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

# ─── LOGGING ─────────────────────────────────────────────────────────────

log = logging.getLogger("facility_pipeline")
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

PIPELINE_NAME = "facility_api_to_snowflake"

FACILITIES = {
    "afya_api_auth": {"base_url": "https://staging.afyanalytics.ai", "db": "staging_db"},
    "kakamega":      {"base_url": "https://demo.collabmed.net",      "db": "kakamega_db"},
    "kisumu":        {"base_url": "https://kshospital.collabmed.net","db": "kisumu_db"},
    "lodwar":        {"base_url": "https://lcrh.collabmed.net",      "db": "lodwar_db"},
    "tenri":         {"base_url": "https://stageenv.collabmed.net",  "db": "tenri_db"},
    "xanalife":      {"base_url": "https://xanalife.afyanalytics.ai/", "db": "xanalife_db"},
}

S3_BUCKET = "collabmedbucket"
S3_PREFIX = "raw/facilities"

SF_DB            = "HOSPITALS"
SF_SHARED_SCHEMA = "SHARED"
SF_STAGE         = f"{SF_DB}.{SF_SHARED_SCHEMA}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT   = f"{SF_DB}.{SF_SHARED_SCHEMA}.JSON_FF"

WATERMARK_FILE = Path(__file__).resolve().parent / ".watermarks.json"
PROGRESS_FILE  = Path(__file__).resolve().parent / ".progress.json"

DEFAULT_PIPELINE_WORKERS = int(os.getenv("PIPELINE_WORKERS", "8"))
DEFAULT_PAGE_WORKERS     = int(os.getenv("PAGE_WORKERS", "4"))
TOKEN_TTL_SECONDS        = int(os.getenv("TOKEN_TTL_SECONDS", str(50 * 60)))

# ─── INFLECT (lazy) ──────────────────────────────────────────────────────

_inflect_engine = None
def _get_inflect():
    global _inflect_engine
    if _inflect_engine is None:
        import inflect
        _inflect_engine = inflect.engine()
    return _inflect_engine

# ─── WATERMARKS ──────────────────────────────────────────────────────────

def load_watermarks() -> dict:
    if WATERMARK_FILE.exists():
        try:
            return json.loads(WATERMARK_FILE.read_text())
        except Exception as e:
            log.warning("Could not parse %s: %s — starting fresh", WATERMARK_FILE, e)
    return {}

def get_watermark(facility: str, default: str = "1970-01-01T00:00:00Z") -> str:
    return load_watermarks().get(facility, default)

def set_watermark(facility: str, ts_iso: str) -> None:
    wm = load_watermarks()
    wm[facility] = ts_iso
    WATERMARK_FILE.write_text(json.dumps(wm, indent=2, sort_keys=True))
    log.info("Watermark for %s set → %s", facility, ts_iso)

# ─── PROGRESS / RESUME ───────────────────────────────────────────────────

_progress_lock = threading.Lock()

def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text())
        except Exception as e:
            log.warning("Could not parse %s: %s — ignoring", PROGRESS_FILE, e)
    return {}

def _save_progress(data: dict) -> None:
    PROGRESS_FILE.write_text(json.dumps(data, indent=2, sort_keys=True))

def _job_key(job: dict) -> str:
    return f"{job['module'].lower()}|{job['table'].lower()}"

def _mark_done(facility: str, run_id: str, job: dict, s3_key: str | None) -> None:
    """Record that (facility, module, table) has finished — survives a kill."""
    with _progress_lock:
        prog = _load_progress()
        bucket = prog.setdefault(facility, {"run_id": run_id, "completed": {}})
        if bucket.get("run_id") != run_id:
            bucket["run_id"] = run_id   # rolling resume across multiple invocations
        bucket["completed"][_job_key(job)] = {
            "s3_key": s3_key,
            "at": datetime.now(timezone.utc).isoformat(),
        }
        _save_progress(prog)

def _clear_progress(facility: str) -> None:
    with _progress_lock:
        prog = _load_progress()
        prog.pop(facility, None)
        _save_progress(prog)

def _completed_keys(facility: str) -> set[str]:
    return set(_load_progress().get(facility, {}).get("completed", {}).keys())

# ─── SNOWFLAKE CLIENT  (thread-safe via per-call lock) ───────────────────

class SnowflakeClient:
    """Read (`query`) + write (`execute`) with structured logging.
    Thread-safe across multiple workers as long as `execute` /
    `query` are called via the public methods (each one creates its
    own short-lived cursor under a lock)."""

    def __init__(self, schema_: str | None = None):
        with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip(), "rb") as key:
            key.read()  # presence check; the connector reads the path itself

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
        log.info("▶ %-26s SELECT    | %s…", label, " ".join(sql.split())[:140])
        t0 = time.perf_counter()
        try:
            with self._lock, self._cursor() as cur:
                cur.execute(sql)
                df = cur.fetch_pandas_all()
            elapsed = time.perf_counter() - t0
            from decimal import Decimal
            for col in df.columns:
                if df[col].dtype == "object":
                    nn = df[col].dropna()
                    if len(nn) and isinstance(nn.iloc[0], Decimal):
                        df[col] = df[col].astype(float)
            log.info("✓ %-26s done      | %s rows · %d cols · %.2fs",
                     label, f"{len(df):,}", df.shape[1], elapsed)
            return df
        except Exception as e:
            log.exception("✗ %-26s SELECT failed | %.2fs · %s",
                          label, time.perf_counter() - t0, e)
            raise

    def execute(self, sql: str, label: str | None = None) -> dict:
        label = label or f"x:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-26s WRITE     | %s…", label, " ".join(sql.split())[:140])
        t0 = time.perf_counter()
        try:
            with self._lock, self._cursor() as cur:
                cur.execute(sql)
                rowcount = cur.rowcount
                sfqid = cur.sfqid
            elapsed = time.perf_counter() - t0
            log.info("✓ %-26s done      | rowcount=%s · sfqid=%s · %.2fs",
                     label, rowcount, sfqid, elapsed)
            return {"rowcount": rowcount, "sfqid": sfqid, "elapsed_s": elapsed}
        except Exception as e:
            log.exception("✗ %-26s WRITE failed | %.2fs · %s",
                          label, time.perf_counter() - t0, e)
            raise

    def __enter__(self): return self
    def __exit__(self, *a): self.close()

# ─── HTTP SESSION + TOKEN CACHE  (per facility) ──────────────────────────

_session_cache: dict[str, requests.Session] = {}
_session_lock = threading.Lock()

def _facility_session(facility: str) -> requests.Session:
    """One pooled requests.Session per facility — reuses TCP/TLS connections."""
    with _session_lock:
        s = _session_cache.get(facility)
        if s is None:
            s = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=32, pool_maxsize=32, max_retries=0,
            )
            s.mount("https://", adapter)
            s.mount("http://", adapter)
            _session_cache[facility] = s
        return s

_token_cache: dict[str, tuple[str, float]] = {}   # facility -> (token, fetched_at_epoch)
_token_lock = threading.Lock()

def _facility_token(facility: str) -> str:
    """Cached auth token. Refreshes on TTL expiry (default 50 minutes)."""
    with _token_lock:
        cached = _token_cache.get(facility)
        if cached and (time.time() - cached[1]) < TOKEN_TTL_SECONDS:
            return cached[0]
        token = generate_auth_token(facility)
        _token_cache[facility] = (token, time.time())
        return token

# ─── GOOGLE SHEET ────────────────────────────────────────────────────────

def get_gsheet_client():
    sa_path = os.getenv("GOOGLE_SA_JSON_PATH")
    if sa_path:
        return gspread.service_account(filename=sa_path)
    sa_json = os.getenv("GOOGLE_SA_JSON")
    if sa_json:
        return gspread.service_account_from_dict(json.loads(sa_json))
    raise RuntimeError("Set GOOGLE_SA_JSON_PATH (file path) or GOOGLE_SA_JSON (raw JSON)")

@lru_cache(maxsize=4)
def _read_dictionary_sheet_cached(spreadsheet_id: str, worksheet_name: str) -> tuple[str, ...]:
    """Cached for the lifetime of the process. Stores serialized rows so the
    cache key works (lists/dicts aren't hashable)."""
    gc = get_gsheet_client()
    ws = gc.open_by_key(spreadsheet_id).worksheet(worksheet_name)
    rows = ws.get_all_records()
    return tuple(json.dumps(r, sort_keys=True) for r in rows)

def read_dictionary_sheet(spreadsheet_id: str, worksheet_name: str) -> list[dict]:
    return [json.loads(r) for r in _read_dictionary_sheet_cached(spreadsheet_id, worksheet_name)]

# ─── NAMESPACE BUILDERS ──────────────────────────────────────────────────

def snake_to_pascal(s: str) -> str:
    return "".join(w.capitalize() for w in re.split(r"[_\s]+", s.strip()) if w)

def build_namespace(module: str, table: str) -> str:
    mod = snake_to_pascal(module)
    prefix = module.strip().lower() + "_"
    t = table.strip().lower()
    if t.startswith(prefix):
        t = t[len(prefix):]
    return f"Ignite\\{mod}\\Entities\\{snake_to_pascal(t)}"

def namespace_to_singular_model(namespace: str) -> str:
    parts = namespace.split("\\")
    if not parts:
        return namespace
    class_name = parts[-1]
    singular = _get_inflect().singular_noun(class_name)
    parts[-1] = singular if singular else class_name
    return "\\".join(parts)

def double_namespace_model(namespace: str) -> str:
    parts = namespace.split("\\")
    if not parts:
        return namespace
    parts[-1] = parts[1] + parts[-1]
    return "\\".join(parts)

def _safe_s3_token(s: str) -> str:
    s = (s or "").strip()
    return re.sub(r"[^a-zA-Z0-9_\-=\.\+]+", "_", s)

# ─── JOB BUILDER ─────────────────────────────────────────────────────────

def build_jobs_for_facility(facility: str, since: str | None = None,
                             only_tables: set[str] | None = None) -> list[dict]:
    cfg = FACILITIES[facility]
    last_run = since or get_watermark(facility)

    sheet_id  = os.environ["IGNITE_SHEET_ID"]
    sheet_tab = os.getenv("IGNITE_SHEET_WORKSHEET", "Sheet1")
    rows = read_dictionary_sheet(sheet_id, sheet_tab)

    seen, jobs = set(), []
    for r in rows:
        module = (r.get("module") or "").strip()
        table  = (r.get("table") or "").strip()
        if not module or not table:
            continue
        if only_tables and table.lower() not in only_tables:
            continue

        key = (module.lower(), table.lower())
        if key in seen:
            continue
        seen.add(key)

        jobs.append({
            "facility":      facility,
            "module":        module,
            "table":         table,
            "namespace":     build_namespace(module, table),
            "database":      cfg.get("db"),
            "updated_since": last_run,
            "limit":         500,
        })

    log.info("Prepared %d model jobs for facility=%s (since %s)",
             len(jobs), facility, last_run)
    return jobs

# ─── AUTH ────────────────────────────────────────────────────────────────

def generate_auth_token(facility: str) -> str:
    cfg = FACILITIES[facility]
    upper = facility.upper()
    user = os.getenv(f"FACILITY_{upper}_USERNAME")
    pwd  = os.getenv(f"FACILITY_{upper}_PASSWORD")
    if not user or not pwd:
        raise RuntimeError(
            f"Missing FACILITY_{upper}_USERNAME / FACILITY_{upper}_PASSWORD env vars"
        )

    url = f"{cfg['base_url'].rstrip('/')}/api/users/authenticate/user"
    r = _facility_session(facility).post(
        url, json={"username": user, "password": pwd}, timeout=30,
    )
    if r.status_code != 200:
        raise Exception(f"Auth failed for {facility}: {r.status_code} · {r.text}")
    token = (r.json().get("success") or {}).get("token")
    if not token:
        raise Exception(f"Token not found in response for {facility}")
    return token

# ─── HTTP RETRY + FALLBACK ───────────────────────────────────────────────

def post_with_retry_and_fallback(
    url, headers, bodies, *, session=None, timeout=60, max_retries=6,
    default_retry_wait=10, backoff_factor=2, base_delay=0.0,
):
    """Tries multiple request bodies in order. Handles 404 (next fallback),
    429 (wait + retry same body), 5xx (backoff + retry), Timeout/ConnectionError."""
    poster = (session or requests).post

    for body_index, base_body in enumerate(bodies):
        attempt, wait_time = 0, default_retry_wait
        while True:
            attempt += 1
            try:
                r = poster(url=url, headers=headers, json=base_body, timeout=timeout)

                # Cheap one-line at INFO; full body only at DEBUG
                log.debug("BodyIndex=%s Attempt=%s Status=%s Resp=%.300s",
                          body_index, attempt, r.status_code, r.text)
                log.info("· ns=%s page=%s status=%s",
                         base_body.get("namespace", "?"),
                         base_body.get("page", "?"), r.status_code)

                if r.status_code == 404:
                    log.warning("404 ns=%s — trying next fallback",
                                base_body.get("namespace"))
                    break

                if r.status_code == 429:
                    retry_after = default_retry_wait
                    try:
                        retry_after = int(r.json().get("retry_after_seconds", default_retry_wait))
                    except Exception:
                        pass
                    if attempt >= max_retries:
                        r.raise_for_status()
                    log.warning("429 ns=%s · sleeping %ss (%s/%s)",
                                base_body.get("namespace"), retry_after, attempt, max_retries)
                    time.sleep(retry_after)
                    continue

                if r.status_code in {500, 502, 503, 504}:
                    if attempt >= max_retries:
                        r.raise_for_status()
                    time.sleep(wait_time)
                    wait_time *= backoff_factor
                    continue

                r.raise_for_status()
                if base_delay > 0:
                    time.sleep(base_delay)
                return r, base_body

            except (Timeout, ConnectionError) as e:
                if attempt >= max_retries:
                    raise
                log.warning("Network error ns=%s · %s · sleeping %ss (%s/%s)",
                            base_body.get("namespace"), e, wait_time, attempt, max_retries)
                time.sleep(wait_time)
                wait_time *= backoff_factor
            except HTTPError:
                raise

    raise Exception("All fallback request bodies returned 404")

def extract_all_pages(url, headers, body, singular_body,
                      double_namespace_body, double_namespace_singular_body,
                      *, session=None, timeout=60, max_pages=10000, max_retries=6,
                      default_retry_wait=10, backoff_factor=2, base_delay=0.0,
                      page_workers=DEFAULT_PAGE_WORKERS):
    """Page 1 sequentially (uses fallback chain to discover the right body
    shape), then pages 2..N concurrently with the chosen body."""

    def extract_rows(payload: dict) -> list:
        rows = payload.get("data")
        if rows is None:
            sv = payload.get("success")
            rows = sv.get("data") or [] if isinstance(sv, dict) else []
        if isinstance(rows, dict):
            rows = rows.get("data") or []
        elif not isinstance(rows, list):
            rows = []
        return rows

    candidate_bodies = [
        {**body, "page": 1},
        {**singular_body, "page": 1},
        {**double_namespace_body, "page": 1},
        {**double_namespace_singular_body, "page": 1},
    ]

    r, chosen_body = post_with_retry_and_fallback(
        url=url, headers=headers, bodies=candidate_bodies, session=session,
        timeout=timeout, max_retries=max_retries,
        default_retry_wait=default_retry_wait,
        backoff_factor=backoff_factor, base_delay=base_delay,
    )
    payload  = r.json()
    all_rows = extract_rows(payload)

    pagination = payload.get("pagination") or {}
    has_more   = bool(pagination.get("has_more_pages", False))
    last_page  = pagination.get("last_page")

    if not has_more:
        return all_rows, chosen_body

    # If we know the last page up front → fan-out fetch.
    if last_page is not None:
        last_page = min(int(last_page), max_pages)
        pages = list(range(2, last_page + 1))
        if not pages:
            return all_rows, chosen_body

        def _fetch(p):
            r, _ = post_with_retry_and_fallback(
                url=url, headers=headers,
                bodies=[{**chosen_body, "page": p}], session=session,
                timeout=timeout, max_retries=max_retries,
                default_retry_wait=default_retry_wait,
                backoff_factor=backoff_factor, base_delay=base_delay,
            )
            return p, extract_rows(r.json())

        with ThreadPoolExecutor(max_workers=max(1, page_workers)) as pool:
            page_rows: dict[int, list] = {}
            for fut in as_completed(pool.submit(_fetch, p) for p in pages):
                p, rows = fut.result()
                page_rows[p] = rows
        # preserve page order
        for p in pages:
            all_rows.extend(page_rows.get(p, []))
        return all_rows, chosen_body

    # Server didn't tell us last_page — fall back to sequential pagination.
    page = 1
    while has_more:
        page += 1
        if page > max_pages:
            log.info("Pagination safety stop (max_pages=%s)", max_pages)
            break
        r, chosen_body = post_with_retry_and_fallback(
            url=url, headers=headers,
            bodies=[{**chosen_body, "page": page}], session=session,
            timeout=timeout, max_retries=max_retries,
            default_retry_wait=default_retry_wait,
            backoff_factor=backoff_factor, base_delay=base_delay,
        )
        payload = r.json()
        rows    = extract_rows(payload)
        all_rows.extend(rows)
        pagination = payload.get("pagination") or {}
        has_more   = bool(pagination.get("has_more_pages", False))
        last_page  = pagination.get("last_page", last_page)
        if not rows:
            break

    return all_rows, chosen_body

# ─── PIPELINE STEPS ──────────────────────────────────────────────────────

def sf_schema(facility: str, layer: str) -> str:
    return f"{SF_DB}.{facility.upper()}_{layer}"

# Lazy S3 client — one per process, thread-safe.
_s3_client_singleton = None
_s3_client_lock = threading.Lock()

def _s3_client():
    global _s3_client_singleton
    if _s3_client_singleton is None:
        with _s3_client_lock:
            if _s3_client_singleton is None:
                ak = os.getenv("AWS_ACCESS_KEY_ID")
                sk = os.getenv("AWS_SECRET_ACCESS_KEY")
                if not (ak and sk):
                    raise RuntimeError(
                        "AWS credentials missing — set AWS_ACCESS_KEY_ID + "
                        "AWS_SECRET_ACCESS_KEY in your .env (and AWS_REGION)."
                    )
                _s3_client_singleton = boto3.client(
                    "s3",
                    aws_access_key_id=ak,
                    aws_secret_access_key=sk,
                    region_name=os.getenv("AWS_REGION", "us-east-1"),
                )
    return _s3_client_singleton

def extract_one_model(job: dict, run_id: str, dry_run: bool = False,
                      page_workers: int = DEFAULT_PAGE_WORKERS) -> dict | None:
    facility = job["facility"]
    cfg      = FACILITIES[facility]

    url     = f"{cfg['base_url'].rstrip('/')}/api/finance/access/data/point"
    session = _facility_session(facility)
    headers = {"Authorization": f"Bearer {_facility_token(facility)}",
               "Content-Type": "application/json"}

    body = {
        "namespace": job["namespace"], "action": "get",
        "database": job["database"], "updated_since": job["updated_since"],
        "limit": job["limit"],
    }
    singular_body                  = {**body, "namespace": namespace_to_singular_model(job["namespace"])}
    double_namespace_body          = {**body, "namespace": double_namespace_model(job["namespace"])}
    double_namespace_singular_body = {**body, "namespace": double_namespace_model(namespace_to_singular_model(job["namespace"]))}

    rows, _ = extract_all_pages(
        url=url, headers=headers, body=body,
        singular_body=singular_body,
        double_namespace_body=double_namespace_body,
        double_namespace_singular_body=double_namespace_singular_body,
        session=session, timeout=60, max_pages=10000,
        page_workers=page_workers,
    )

    # No rows? Skip the upload entirely.
    if not rows:
        log.info("    %s · %s — 0 rows, skipping S3", job["module"], job["table"])
        return None

    ingested_at = datetime.now(timezone.utc)
    dt = ingested_at.date().isoformat()
    ns_safe     = _safe_s3_token(job["namespace"].replace("\\", "_"))
    module_safe = _safe_s3_token(job.get("module", ""))
    table_safe  = _safe_s3_token(job.get("table", ""))

    key_prefix = (
        f"{S3_PREFIX}/"
        f"facility_id={facility}/"
        f"module={module_safe or 'unknown'}/"
        f"table={table_safe or 'unknown'}/"
        f"namespace={ns_safe}/"
        f"dt={dt}/"
    )
    key = f"{key_prefix}{run_id}.jsonl.gz"

    # Encode (orjson when available)
    parts = [_dumps_bytes(row) for row in rows]
    jsonl_bytes = b"\n".join(parts) + b"\n"

    if dry_run:
        log.info("DRY-RUN ✓ %-22s %s rows (would upload to s3://%s/%s)",
                 job["table"], len(rows), S3_BUCKET, key)
        return None

    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(jsonl_bytes)
    _s3_client().put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    log.info("Uploaded to s3://%s/%s rows=%s", S3_BUCKET, key, len(rows))

    return {
        "facility":      facility,
        "module":        job.get("module"),
        "table":         job.get("table"),
        "namespace":     job["namespace"],
        "database":      job.get("database"),
        "updated_since": job.get("updated_since"),
        "ingested_at":   ingested_at.isoformat(),
        "s3_key":        key,
        "row_count":     len(rows),
    }

def copy_into_snowflake(job_result: dict, sf: SnowflakeClient | None = None) -> None:
    facility      = job_result["facility"]
    s3_key        = job_result["s3_key"]
    ingested_at   = job_result["ingested_at"]
    module_source = job_result.get("module") or ""
    source_table  = job_result.get("table") or ""
    namespace     = job_result.get("namespace") or ""

    raw_table = f"{sf_schema(facility, 'RAW')}.EVENTS_RAW"
    sql = f"""
    COPY INTO {raw_table} (facility_id, ingested_at, module_source, source_table, namespace, payload)
    FROM (
      SELECT
        '{facility}'::STRING        AS facility_id,
        '{ingested_at}'::TIMESTAMP_TZ AS ingested_at,
        '{module_source}'::STRING   AS module_source,
        '{source_table}'::STRING    AS source_table,
        '{namespace}'::STRING       AS namespace,
        PARSE_JSON($1)              AS payload
      FROM @{SF_STAGE}
    )
    FILES = ('{s3_key}')
    FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
    ON_ERROR = 'CONTINUE';
    """
    label = f"copy:{facility}:{source_table or 'events'}"
    if sf is not None:
        sf.execute(sql, label=label)
    else:
        with SnowflakeClient(schema_=sf_schema(facility, "RAW")) as new_sf:
            new_sf.execute(sql, label=label)

def merge_clean(facility: str, sf: SnowflakeClient | None = None) -> None:
    raw_table   = f"{sf_schema(facility, 'RAW')}.EVENTS_RAW"
    clean_table = f"{sf_schema(facility, 'CLEAN')}.EVENTS"
    sql = f"""
    MERGE INTO {clean_table} AS t
    USING (
        SELECT
            facility_id,
            f.value:id::STRING            AS event_id,
            f.value:event_time::TIMESTAMP AS event_time,
            f.value:type::STRING          AS event_type,
            f.value:amount::NUMBER        AS amount,
            f.value                       AS payload,
            ingested_at
        FROM {raw_table} AS r,
             LATERAL FLATTEN(input => r.payload) AS f
        WHERE f.value:id IS NOT NULL
          AND NULLIF(TRIM(f.value:id::STRING), '') IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY f.value:id::STRING ORDER BY ingested_at DESC
        ) = 1
    ) AS s
    ON t.event_id = s.event_id
    WHEN MATCHED THEN UPDATE SET
        event_time  = s.event_time,
        event_type  = s.event_type,
        amount      = s.amount,
        payload     = s.payload,
        ingested_at = s.ingested_at
    WHEN NOT MATCHED THEN INSERT (event_id, event_time, event_type, amount, payload, ingested_at)
    VALUES (s.event_id, s.event_time, s.event_type, s.amount, s.payload, s.ingested_at);
    """
    label = f"merge_clean:{facility}"
    if sf is not None:
        sf.execute(sql, label=label)
    else:
        with SnowflakeClient(schema_=sf_schema(facility, "RAW")) as new_sf:
            new_sf.execute(sql, label=label)

# ─── ORCHESTRATOR ────────────────────────────────────────────────────────

def run_pipeline(facility: str, *, since: str | None = None,
                 only_tables: set[str] | None = None,
                 skip_merge: bool = False, dry_run: bool = False,
                 update_watermark: bool = True,
                 workers: int = DEFAULT_PIPELINE_WORKERS,
                 page_workers: int = DEFAULT_PAGE_WORKERS,
                 resume: bool = True) -> None:

    if facility not in FACILITIES:
        raise ValueError(f"Unknown facility {facility!r}. Known: {list(FACILITIES)}")

    run_id = datetime.now(timezone.utc).strftime("manual__%Y-%m-%dT%H-%M-%SZ")
    started_at = datetime.now(timezone.utc)

    # 1. Build full job list
    all_jobs = build_jobs_for_facility(facility, since=since, only_tables=only_tables)
    if not all_jobs:
        log.warning("No jobs to run.")
        return

    # 1b. Filter based on resume / progress checkpoint
    if not resume and not dry_run:
        _clear_progress(facility)
        log.info("⟲ Resume disabled · cleared previous progress for %s", facility)
        jobs = all_jobs
    elif resume and not dry_run:
        done = _completed_keys(facility)
        skipped = [j for j in all_jobs if _job_key(j) in done]
        jobs    = [j for j in all_jobs if _job_key(j) not in done]
        if skipped:
            log.info("⟲ Resume mode · skipping %d already-completed jobs "
                     "(use --no-resume to redo)", len(skipped))
    else:
        jobs = all_jobs

    log.info("══════ START %s · facility=%s · run_id=%s · %d/%d jobs · workers=%d (page_workers=%d) ══════",
             PIPELINE_NAME, facility, run_id, len(jobs), len(all_jobs), workers, page_workers)

    if not jobs:
        log.info("✓ Everything was already done in a previous run. Running merge only.")

    successes: list[dict] = []
    failures:  list[dict] = []

    sf_client = None
    try:
        if not dry_run:
            sf_client = SnowflakeClient(schema_=sf_schema(facility, "RAW"))

        def _do_one(idx_and_job):
            idx, job = idx_and_job
            log.info("──[%d/%d] start · %s · %s",
                     idx, len(jobs), job["module"], job["table"])
            try:
                result = extract_one_model(job, run_id=run_id, dry_run=dry_run,
                                           page_workers=page_workers)
                if result is None:
                    if not dry_run:
                        _mark_done(facility, run_id, job, s3_key=None)
                    return ("skip", None, job)
                copy_into_snowflake(result, sf=sf_client)
                _mark_done(facility, run_id, job, s3_key=result["s3_key"])
                return ("ok", result, job)
            except Exception as e:
                log.error("✗ %s · %s · failed: %s",
                          job["module"], job["table"], e, exc_info=True)
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

        # 4. Merge into CLEAN — safe to re-run on resume.
        if not dry_run and not skip_merge:
            try:
                merge_clean(facility, sf=sf_client)
            except Exception as e:
                log.error("merge_clean failed: %s", e, exc_info=True)
                failures.append({"job": "merge_clean", "error": str(e)})

    finally:
        if sf_client is not None:
            sf_client.close()

    # 5. Bump watermark + clear progress only on a clean full run.
    if not dry_run and update_watermark and not failures:
        set_watermark(facility, started_at.isoformat().replace("+00:00", "Z"))
        _clear_progress(facility)
        log.info("✓ Run complete — watermark advanced, progress file cleared.")

    log.info("══════ END   ✓ %d ok · ✗ %d failed · %s ══════",
             len(successes), len(failures), PIPELINE_NAME)
    if failures:
        log.warning("Failures (truncated):")
        for f in failures[:10]:
            log.warning("  · %s",
                        {k: (v if k != "job" or isinstance(v, str)
                             else f"{v.get('module')}.{v.get('table')}")
                         for k, v in f.items()})
        sys.exit(1)

# ─── CLI ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--facility", required=True, choices=list(FACILITIES.keys()))
    ap.add_argument("--since", help="ISO date / timestamp (overrides watermark).")
    ap.add_argument("--only-tables",
                    help="Comma-separated list of tables to limit the run to.")
    ap.add_argument("--skip-merge", action="store_true",
                    help="Skip the CLEAN MERGE step.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Extract only — no S3 / Snowflake writes.")
    ap.add_argument("--no-watermark-update", action="store_true",
                    help="Don't bump the local watermark on a clean run.")
    ap.add_argument("--no-resume", action="store_true",
                    help="Ignore .progress.json and re-run every table from scratch.")
    ap.add_argument("--workers", type=int, default=DEFAULT_PIPELINE_WORKERS,
                    help=f"Parallel jobs (default {DEFAULT_PIPELINE_WORKERS}). "
                         "Drop to 4 if the source API rate-limits.")
    ap.add_argument("--page-workers", type=int, default=DEFAULT_PAGE_WORKERS,
                    help=f"Parallel page fetches within a job (default {DEFAULT_PAGE_WORKERS}).")
    args = ap.parse_args()

    only_tables = None
    if args.only_tables:
        only_tables = {t.strip().lower() for t in args.only_tables.split(",") if t.strip()}

    run_pipeline(
        facility=args.facility,
        since=args.since,
        only_tables=only_tables,
        skip_merge=args.skip_merge,
        dry_run=args.dry_run,
        update_watermark=not args.no_watermark_update,
        workers=args.workers,
        page_workers=args.page_workers,
        resume=not args.no_resume,
    )


if __name__ == "__main__":
    main()
