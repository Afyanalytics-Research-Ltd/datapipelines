#!/usr/bin/env python3
"""
v3_api_to_snowflake.py — V3 Service APIs → S3 → Snowflake

Reads every model from the V3 gateway services (core, finance, evaluation,
reception, inventory, theatre, inpatient) and lands it in a three-layer
Snowflake schema:

  HOSPITALS.STG_V3.<SERVICE>_RAW      Raw VARIANT append, one table per service.
  HOSPITALS.V3_READY.<SERVICE>        Deduplicated upsert — latest record per (model, id).
  HOSPITALS.MIGRATION_AUDIT.*         Per-run and per-job audit records.

Flow per run:
  1. Auth     → POST core /v1/login → bearer token (50-min TTL)
  2. Discover → POST each service /v1/gateway action=list → readable model aliases
  3. Per (service, model):
       paginate /v1/gateway action=read → all rows → gzip JSONL → S3
       → COPY INTO STG_V3.<service>_RAW
       → MERGE INTO V3_READY.<service>  (dedup by _model + _record_id)
       → checkpoint progress + write JOB_RESULTS row
  4. Advance watermarks only when zero failures
  5. Insert PIPELINE_RUNS audit row

USAGE
  python v3_api_to_snowflake.py
  python v3_api_to_snowflake.py --services core,finance
  python v3_api_to_snowflake.py --models invoice,patient
  python v3_api_to_snowflake.py --since 2025-01-01T00:00:00Z
  python v3_api_to_snowflake.py --dry-run
  python v3_api_to_snowflake.py --no-resume
  python v3_api_to_snowflake.py --full-refresh

ENV  (.env next to this script)
  AFYA_USERNAME=...        AFYA_PASSWORD=...         AFYA_FACILITY_ID=6
  SNOWFLAKE_USER=...       SNOWFLAKE_ACCOUNT=...     SNOWFLAKE_WAREHOUSE=...
  SNOWFLAKE_DATABASE=HOSPITALS                       SNOWFLAKE_PRIVATE_KEY_PATH=...
  AWS_ACCESS_KEY_ID=...    AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1
  S3_BUCKET=collabmedbucket
  PIPELINE_WORKERS=4   PAGE_WORKERS=4   PER_PAGE=500   LOG_LEVEL=INFO
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import boto3
import requests
import requests.adapters
import snowflake.connector
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, HTTPError, Timeout

try:
    import orjson
    def _jdumps(obj: Any) -> bytes:
        return orjson.dumps(obj)
except ImportError:
    def _jdumps(obj: Any) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode()

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

# ─── LOGGING ──────────────────────────────────────────────────────────────────

log = logging.getLogger("v3_snowflake")
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S",
    ))
    log.addHandler(_h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ─── CONFIG ───────────────────────────────────────────────────────────────────

V3_SERVICES: dict[str, str] = {
    "core":       "https://core.afyaanalytics.ai/api",
    "finance":    "https://finance.afyaanalytics.ai/api",
    "evaluation": "https://evaluation.afyaanalytics.ai/api",
    "reception":  "https://reception.afyaanalytics.ai/api",
    "inventory":  "https://inventory.afyaanalytics.ai/api",
    "theatre":    "https://theatre.afyaanalytics.ai/api",
    "inpatient":  "https://inpatient.afyaanalytics.ai/api",
}

AFYA_FACILITY_ID = int(os.getenv("AFYA_FACILITY_ID", "6"))
TOKEN_TTL        = int(os.getenv("TOKEN_TTL_SECONDS", str(50 * 60)))
PIPELINE_WORKERS = int(os.getenv("PIPELINE_WORKERS", "4"))
PAGE_WORKERS     = int(os.getenv("PAGE_WORKERS", "4"))
PER_PAGE         = int(os.getenv("PER_PAGE", "500"))

S3_BUCKET = os.getenv("S3_BUCKET", "collabmedbucket")
S3_PREFIX = "raw/v3_service"

SF_DB     = os.getenv("SNOWFLAKE_DATABASE", "HOSPITALS").upper()
SF_SHARED = "SHARED"
SF_STAGE  = f"{SF_DB}.{SF_SHARED}.FACILITY_RAW_STAGE"
SF_FF     = f"{SF_DB}.{SF_SHARED}.JSON_FF"
SF_STG    = "STG_V3"
SF_READY  = "V3_READY"
SF_AUDIT  = "MIGRATION_AUDIT"

WATERMARK_FILE = Path(__file__).resolve().parent / ".v3_watermarks.json"
PROGRESS_FILE  = Path(__file__).resolve().parent / ".v3_progress.json"

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _jload(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception as exc:
            log.warning("Cannot parse %s: %s", path, exc)
    return {}

def _jsave(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True))

def _sq(s: str) -> str:
    """Escape a string for embedding in a single-quoted SQL literal."""
    return (s or "").replace("'", "''")

# ─── WATERMARKS ───────────────────────────────────────────────────────────────

_wm_lock = threading.Lock()

def get_watermark(service: str, model: str) -> str:
    return _jload(WATERMARK_FILE).get(f"{service}|{model}", "1970-01-01T00:00:00Z")

def set_watermark(service: str, model: str, ts: str) -> None:
    with _wm_lock:
        wm = _jload(WATERMARK_FILE)
        wm[f"{service}|{model}"] = ts
        _jsave(WATERMARK_FILE, wm)

# ─── PROGRESS / RESUME ────────────────────────────────────────────────────────

_progress_lock = threading.Lock()

def _job_key(service: str, model: str) -> str:
    return f"{service}|{model}"

def _mark_done(run_id: str, service: str, model: str) -> None:
    with _progress_lock:
        prog = _jload(PROGRESS_FILE)
        prog.setdefault(run_id, {})[_job_key(service, model)] = (
            datetime.now(timezone.utc).isoformat()
        )
        _jsave(PROGRESS_FILE, prog)

def _done_keys(run_id: str) -> set[str]:
    return set(_jload(PROGRESS_FILE).get(run_id, {}).keys())

def _clear_progress(run_id: str) -> None:
    with _progress_lock:
        prog = _jload(PROGRESS_FILE)
        prog.pop(run_id, None)
        _jsave(PROGRESS_FILE, prog)

# ─── SNOWFLAKE ────────────────────────────────────────────────────────────────

class SnowflakeClient:
    def __init__(self) -> None:
        pk_path = (os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or "").strip()
        pw      = (os.getenv("SNOWFLAKE_PASSWORD") or "").strip()
        kwargs: dict[str, Any] = dict(
            user      = os.getenv("SNOWFLAKE_USER",      "").strip(),
            account   = os.getenv("SNOWFLAKE_ACCOUNT",   "").strip(),
            warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "").strip(),
            database  = SF_DB,
            schema    = SF_SHARED,
        )
        if pk_path:
            kwargs["private_key_file"] = pk_path
        elif pw:
            kwargs["password"] = pw
        else:
            raise RuntimeError("Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD in .env")
        self._conn = snowflake.connector.connect(**kwargs)
        self._lock = threading.Lock()

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    @contextmanager
    def _cur(self):
        c = self._conn.cursor()
        try:
            yield c
        finally:
            c.close()

    def execute(self, sql: str, label: str = "") -> int:
        t0 = time.perf_counter()
        log.info("SF %-44s %s…", label or "exec", " ".join(sql.split())[:90])
        with self._lock, self._cur() as c:
            c.execute(sql)
            rows = c.rowcount
        log.info("SF %-44s rowcount=%-6s %.2fs", label or "exec", rows, time.perf_counter() - t0)
        return rows

    def run_ddl(self, sqls: list[str]) -> None:
        with self._lock, self._cur() as c:
            for sql in sqls:
                c.execute(sql)

    def __enter__(self) -> "SnowflakeClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

# ─── HTTP SESSION + TOKEN ─────────────────────────────────────────────────────

_http:        requests.Session | None      = None
_http_lock  = threading.Lock()
_token_cache: tuple[str, float] | None    = None
_token_lock = threading.Lock()

def _session() -> requests.Session:
    global _http
    if _http is None:
        with _http_lock:
            if _http is None:
                s = requests.Session()
                s.mount("https://", requests.adapters.HTTPAdapter(
                    pool_connections=16, pool_maxsize=16, max_retries=0,
                ))
                _http = s
    return _http

def _get_token() -> str:
    global _token_cache
    with _token_lock:
        if _token_cache and time.time() - _token_cache[1] < TOKEN_TTL:
            return _token_cache[0]
        user = os.getenv("AFYA_USERNAME")
        pwd  = os.getenv("AFYA_PASSWORD")
        if not (user and pwd):
            raise RuntimeError("Set AFYA_USERNAME + AFYA_PASSWORD in .env")
        url = f"{V3_SERVICES['core'].rstrip('/')}/v1/login"
        r   = _session().post(
            url,
            json={"username": user, "password": pwd, "facility_id": AFYA_FACILITY_ID},
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=30,
        )
        if not r.ok:
            raise RuntimeError(f"V3 login failed: {r.status_code} · {r.text[:200]}")
        token = r.json().get("access_token")
        if not token:
            raise RuntimeError(f"No access_token in login response: {r.text[:200]}")
        _token_cache = (token, time.time())
        log.info("Authenticated to V3 API")
        return token

def _invalidate_token() -> None:
    global _token_cache
    with _token_lock:
        _token_cache = None

def _headers() -> dict:
    return {
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

# ─── GATEWAY DISCOVERY ────────────────────────────────────────────────────────

def discover_readable_models(services: list[str]) -> dict[str, list[str]]:
    """Return {service: [alias, ...]} for models that expose the 'read' operation."""
    result: dict[str, list[str]] = {}
    for svc in services:
        url = f"{V3_SERVICES[svc].rstrip('/')}/v1/gateway"
        try:
            r = _session().post(
                url, headers=_headers(), json={"action": "list"}, timeout=30,
            )
            if not r.ok:
                log.warning("Gateway list [%s] %s: %s", svc, r.status_code, r.text[:200])
                result[svc] = []
                continue
            entries  = r.json().get("data") or []
            readable = [
                e["alias"] for e in entries
                if e.get("alias") and "read" in e.get("operations", [])
            ]
            result[svc] = readable
            log.info("Service %-12s %d readable models", svc, len(readable))
        except Exception as exc:
            log.warning("Cannot reach gateway [%s]: %s", svc, exc)
            result[svc] = []
    return result

# ─── GATEWAY READ (with retry) ────────────────────────────────────────────────

def _read_page(
    service: str,
    model: str,
    page: int,
    updated_since: str | None,
    *,
    max_retries: int = 6,
    init_wait:   int = 10,
) -> dict:
    url  = f"{V3_SERVICES[service].rstrip('/')}/v1/gateway"
    body: dict = {"action": "read", "model": model, "per_page": PER_PAGE, "page": page}
    if updated_since and updated_since != "1970-01-01T00:00:00Z":
        body["updated_since"] = updated_since

    attempt, wait = 0, init_wait
    while True:
        attempt += 1
        try:
            r = _session().post(url, headers=_headers(), json=body, timeout=90)
            log.info(
                "  · svc=%-12s model=%-30s page=%d status=%d",
                service, model, page, r.status_code,
            )
            if r.status_code == 401:
                _invalidate_token()
                if attempt >= max_retries:
                    r.raise_for_status()
                log.warning("  401 — token refreshed (%d/%d)", attempt, max_retries)
                continue
            if r.status_code == 429:
                pause = init_wait
                try:
                    pause = int(r.json().get("retry_after_seconds", init_wait))
                except Exception:
                    pass
                if attempt >= max_retries:
                    r.raise_for_status()
                log.warning("  429 — sleeping %ds (%d/%d)", pause, attempt, max_retries)
                time.sleep(pause)
                continue
            if r.status_code in {500, 502, 503, 504}:
                if attempt >= max_retries:
                    r.raise_for_status()
                log.warning("  %d — sleeping %ds (%d/%d)", r.status_code, wait, attempt, max_retries)
                time.sleep(wait)
                wait = min(wait * 2, 120)
                continue
            r.raise_for_status()
            return r.json()
        except (Timeout, ConnectionError) as exc:
            if attempt >= max_retries:
                raise
            log.warning("  network error %s — sleeping %ds (%d/%d)", exc, wait, attempt, max_retries)
            time.sleep(wait)
            wait = min(wait * 2, 120)

def _rows_from(payload: dict) -> list[dict]:
    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        inner = data.get("data")
        return inner if isinstance(inner, list) else list(data.values())
    sv = payload.get("success")
    if isinstance(sv, list):
        return sv
    if isinstance(sv, dict):
        inner = sv.get("data")
        return inner if isinstance(inner, list) else []
    return []

def fetch_all_pages(service: str, model: str, updated_since: str | None) -> list[dict]:
    payload1  = _read_page(service, model, 1, updated_since)
    all_rows  = _rows_from(payload1)
    pag       = payload1.get("pagination") or payload1.get("meta") or {}
    last_page = pag.get("last_page") or pag.get("total_pages")
    has_more  = (
        bool(pag.get("has_more_pages") or pag.get("hasMorePages"))
        or (last_page is not None and int(last_page) > 1)
        or len(all_rows) == PER_PAGE
    )

    if not has_more or not all_rows:
        return all_rows

    # Fan-out when last_page is known
    if last_page and int(last_page) > 1:
        pages = list(range(2, min(int(last_page), 10_000) + 1))

        def _fetch(p: int) -> tuple[int, list]:
            return p, _rows_from(_read_page(service, model, p, updated_since))

        page_rows: dict[int, list] = {}
        with ThreadPoolExecutor(max_workers=max(1, PAGE_WORKERS)) as pool:
            for fut in as_completed(pool.submit(_fetch, p) for p in pages):
                p, rows = fut.result()
                page_rows[p] = rows
        for p in pages:
            all_rows.extend(page_rows.get(p, []))
        return all_rows

    # Sequential fallback when last_page unknown
    page = 1
    while True:
        page += 1
        if page > 10_000:
            log.warning("Pagination safety stop at page %d for %s.%s", page, service, model)
            break
        rows = _rows_from(_read_page(service, model, page, updated_since))
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < PER_PAGE:
            break
    return all_rows

# ─── S3 ───────────────────────────────────────────────────────────────────────

_s3_client = None
_s3_lock   = threading.Lock()

def _s3() -> Any:
    global _s3_client
    if _s3_client is None:
        with _s3_lock:
            if _s3_client is None:
                ak = os.getenv("AWS_ACCESS_KEY_ID")
                sk = os.getenv("AWS_SECRET_ACCESS_KEY")
                if not (ak and sk):
                    raise RuntimeError("Set AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY in .env")
                _s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=ak,
                    aws_secret_access_key=sk,
                    region_name=os.getenv("AWS_REGION", "us-east-1"),
                )
    return _s3_client

def upload_to_s3(rows: list[dict], service: str, model: str, run_id: str) -> str:
    dt  = datetime.now(timezone.utc).date().isoformat()
    key = f"{S3_PREFIX}/{service}/model={model}/dt={dt}/{run_id}.jsonl.gz"
    raw = b"\n".join(_jdumps(r) for r in rows) + b"\n"
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(raw)
    _s3().put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    log.info("S3 s3://%s/%s  rows=%d", S3_BUCKET, key, len(rows))
    return key

# ─── SNOWFLAKE DDL ────────────────────────────────────────────────────────────

def _stg_ddl(service: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {SF_DB}.{SF_STG}.{service.upper()}_RAW (
        _run_id    VARCHAR        NOT NULL,
        _service   VARCHAR        NOT NULL,
        _model     VARCHAR        NOT NULL,
        _loaded_at TIMESTAMP_TZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
        _record_id VARCHAR,
        payload    VARIANT        NOT NULL
    )"""

def _ready_ddl(service: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {SF_DB}.{SF_READY}.{service.upper()} (
        _model        VARCHAR        NOT NULL,
        _record_id    VARCHAR        NOT NULL,
        _service      VARCHAR        NOT NULL,
        _first_seen   TIMESTAMP_TZ   NOT NULL,
        _last_updated TIMESTAMP_TZ   NOT NULL,
        _run_id       VARCHAR        NOT NULL,
        payload       VARIANT        NOT NULL
    )"""

_AUDIT_RUNS_DDL = f"""
    CREATE TABLE IF NOT EXISTS {SF_DB}.{SF_AUDIT}.PIPELINE_RUNS (
        run_id         VARCHAR        NOT NULL,
        started_at     TIMESTAMP_TZ   NOT NULL,
        finished_at    TIMESTAMP_TZ,
        status         VARCHAR,
        total_jobs     INTEGER,
        succeeded_jobs INTEGER,
        failed_jobs    INTEGER,
        total_rows     INTEGER
    )"""

_AUDIT_JOBS_DDL = f"""
    CREATE TABLE IF NOT EXISTS {SF_DB}.{SF_AUDIT}.JOB_RESULTS (
        run_id        VARCHAR        NOT NULL,
        service       VARCHAR        NOT NULL,
        model         VARCHAR        NOT NULL,
        started_at    TIMESTAMP_TZ   NOT NULL,
        finished_at   TIMESTAMP_TZ,
        status        VARCHAR,
        row_count     INTEGER,
        s3_key        VARCHAR,
        error_message VARCHAR
    )"""


def ensure_schemas_and_tables(sf: SnowflakeClient, services: list[str]) -> None:
    ddls = [
        f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_STG}",
        f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_READY}",
        f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_AUDIT}",
        _AUDIT_RUNS_DDL,
        _AUDIT_JOBS_DDL,
    ]
    for svc in services:
        ddls.append(_stg_ddl(svc))
        ddls.append(_ready_ddl(svc))
    sf.run_ddl(ddls)
    log.info("Schemas and tables ready: %s", ", ".join(services))

# ─── SNOWFLAKE LOAD ───────────────────────────────────────────────────────────

def copy_into_stg(sf: SnowflakeClient, s3_key: str, service: str, model: str, run_id: str) -> int:
    table = f"{SF_DB}.{SF_STG}.{service.upper()}_RAW"
    sql   = f"""
    COPY INTO {table} (_run_id, _service, _model, _loaded_at, _record_id, payload)
    FROM (
        SELECT
            '{_sq(run_id)}'::VARCHAR,
            '{_sq(service)}'::VARCHAR,
            '{_sq(model)}'::VARCHAR,
            CURRENT_TIMESTAMP::TIMESTAMP_TZ,
            PARSE_JSON($1):id::VARCHAR,
            PARSE_JSON($1)::VARIANT
        FROM @{SF_STAGE}
    )
    FILES = ('{_sq(s3_key)}')
    FILE_FORMAT = (FORMAT_NAME = {SF_FF})
    ON_ERROR = 'CONTINUE'
    """
    return sf.execute(sql, label=f"stg:copy:{service}.{model}")


def merge_into_ready(sf: SnowflakeClient, service: str, model: str, run_id: str) -> int:
    stg_t   = f"{SF_DB}.{SF_STG}.{service.upper()}_RAW"
    ready_t = f"{SF_DB}.{SF_READY}.{service.upper()}"
    sql     = f"""
    MERGE INTO {ready_t} AS tgt
    USING (
        SELECT _model, _record_id, _service, _loaded_at, _run_id, payload
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY _model, _record_id
                    ORDER BY _loaded_at DESC
                ) AS _rn
            FROM {stg_t}
            WHERE _run_id    = '{_sq(run_id)}'
              AND _model     = '{_sq(model)}'
              AND _record_id IS NOT NULL
        )
        WHERE _rn = 1
    ) AS src
    ON  tgt._model = src._model AND tgt._record_id = src._record_id
    WHEN MATCHED THEN UPDATE SET
        payload       = src.payload,
        _last_updated = src._loaded_at,
        _run_id       = src._run_id
    WHEN NOT MATCHED THEN INSERT
        (_model, _record_id, _service, _first_seen, _last_updated, _run_id, payload)
    VALUES
        (src._model, src._record_id, src._service,
         src._loaded_at, src._loaded_at, src._run_id, src.payload)
    """
    return sf.execute(sql, label=f"ready:merge:{service}.{model}")


def _write_job_result(
    sf: SnowflakeClient,
    run_id: str,
    service: str,
    model: str,
    started_at: str,
    finished_at: str,
    status: str,
    row_count: int,
    s3_key: str | None,
    error: str | None,
) -> None:
    s3_val  = f"'{_sq(s3_key)}'"     if s3_key else "NULL"
    err_val = f"'{_sq(error[:2000])}'" if error  else "NULL"
    sql = f"""
    INSERT INTO {SF_DB}.{SF_AUDIT}.JOB_RESULTS
        (run_id, service, model, started_at, finished_at, status, row_count, s3_key, error_message)
    VALUES (
        '{_sq(run_id)}', '{_sq(service)}', '{_sq(model)}',
        '{_sq(started_at)}'::TIMESTAMP_TZ,
        '{_sq(finished_at)}'::TIMESTAMP_TZ,
        '{_sq(status)}', {row_count}, {s3_val}, {err_val}
    )
    """
    sf.execute(sql, label=f"audit:job:{service}.{model}")


def _write_pipeline_run(
    sf: SnowflakeClient,
    run_id: str,
    started_at: str,
    finished_at: str,
    status: str,
    total: int,
    succeeded: int,
    failed: int,
    total_rows: int,
) -> None:
    sql = f"""
    INSERT INTO {SF_DB}.{SF_AUDIT}.PIPELINE_RUNS
        (run_id, started_at, finished_at, status, total_jobs, succeeded_jobs, failed_jobs, total_rows)
    VALUES (
        '{_sq(run_id)}',
        '{_sq(started_at)}'::TIMESTAMP_TZ,
        '{_sq(finished_at)}'::TIMESTAMP_TZ,
        '{_sq(status)}', {total}, {succeeded}, {failed}, {total_rows}
    )
    """
    sf.execute(sql, label="audit:pipeline_run")

# ─── JOB RUNNER ───────────────────────────────────────────────────────────────

def run_job(
    service: str,
    model: str,
    run_id: str,
    sf: SnowflakeClient,
    *,
    updated_since: str | None,
    dry_run: bool,
) -> dict:
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        rows = fetch_all_pages(service, model, updated_since)

        if not rows:
            log.info("  %s.%-30s 0 rows — skipping", service, model)
            return {"status": "skip", "service": service, "model": model, "row_count": 0}

        if dry_run:
            log.info("  DRY-RUN %s.%-30s %d rows", service, model, len(rows))
            return {"status": "dry_run", "service": service, "model": model, "row_count": len(rows)}

        s3_key      = upload_to_s3(rows, service, model, run_id)
        copy_into_stg(sf, s3_key, service, model, run_id)
        merge_into_ready(sf, service, model, run_id)

        finished_at = datetime.now(timezone.utc).isoformat()
        _write_job_result(sf, run_id, service, model, started_at, finished_at,
                          "success", len(rows), s3_key, None)
        return {
            "status": "ok", "service": service, "model": model,
            "row_count": len(rows), "s3_key": s3_key,
        }

    except Exception as exc:
        finished_at = datetime.now(timezone.utc).isoformat()
        log.error("  FAILED %s.%s: %s", service, model, exc, exc_info=True)
        try:
            _write_job_result(sf, run_id, service, model, started_at, finished_at,
                              "failed", 0, None, str(exc))
        except Exception:
            pass
        return {
            "status": "err", "service": service, "model": model,
            "row_count": 0, "error": str(exc),
        }

# ─── PIPELINE ─────────────────────────────────────────────────────────────────

def run_pipeline(
    *,
    services:     list[str] | None = None,
    only_models:  set[str]  | None = None,
    since:        str       | None = None,
    full_refresh: bool             = False,
    dry_run:      bool             = False,
    resume:       bool             = True,
    workers:      int              = PIPELINE_WORKERS,
) -> None:
    run_id     = datetime.now(timezone.utc).strftime("v3__%Y%m%dT%H%M%SZ")
    started_at = datetime.now(timezone.utc)

    active_services = services or list(V3_SERVICES.keys())
    unknown = [s for s in active_services if s not in V3_SERVICES]
    if unknown:
        log.error("Unknown services: %s. Valid: %s", unknown, list(V3_SERVICES.keys()))
        sys.exit(1)

    log.info("══ START run=%s services=[%s] workers=%d ══",
             run_id, ", ".join(active_services), workers)

    # 1. Discover
    model_map = discover_readable_models(active_services)

    # 2. Build job list
    all_jobs: list[tuple[str, str]] = [
        (svc, model)
        for svc in active_services
        for model in model_map.get(svc, [])
        if not only_models or model in only_models
    ]
    if not all_jobs:
        log.warning("No readable models found — nothing to do.")
        return

    # 3. Resume filter
    done_set = _done_keys(run_id) if resume else set()
    if not resume:
        log.info("⟲ --no-resume: re-running all %d jobs", len(all_jobs))
    pending  = [(s, m) for s, m in all_jobs if _job_key(s, m) not in done_set]
    skipped  = len(all_jobs) - len(pending)
    if skipped:
        log.info("⟲ Resume: skipping %d already-done jobs", skipped)
    log.info("Jobs to run: %d / %d", len(pending), len(all_jobs))

    successes:  list[dict] = []
    failures:   list[dict] = []
    total_rows: int        = 0

    with SnowflakeClient() as sf:
        if not dry_run:
            ensure_schemas_and_tables(sf, active_services)

        def _do(job: tuple[str, str]) -> dict:
            svc, model = job
            wm = (
                since                      if since          else
                None                       if full_refresh   else
                get_watermark(svc, model)
            )
            result = run_job(svc, model, run_id, sf, updated_since=wm, dry_run=dry_run)
            if result["status"] == "ok" and not dry_run:
                _mark_done(run_id, svc, model)
            return result

        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            for result in pool.map(_do, pending):
                if result["status"] == "ok":
                    successes.append(result)
                    total_rows += result.get("row_count", 0)
                elif result["status"] == "err":
                    failures.append(result)

    # 4. Watermarks
    run_status = "success" if not failures else ("partial" if successes else "failed")
    if not dry_run:
        ts = started_at.isoformat().replace("+00:00", "Z")
        if not failures:
            for svc, model in all_jobs:
                set_watermark(svc, model, ts)
            _clear_progress(run_id)
            log.info("✓ Clean run — watermarks advanced, progress cleared.")
        else:
            log.warning("Failures detected — watermarks NOT advanced (re-run will retry).")

        with SnowflakeClient() as sf:
            _write_pipeline_run(
                sf, run_id,
                started_at.isoformat(),
                datetime.now(timezone.utc).isoformat(),
                run_status,
                len(all_jobs), len(successes), len(failures), total_rows,
            )

    log.info(
        "══ END run=%s ✓ %d ok · ✗ %d failed · %d rows ══",
        run_id, len(successes), len(failures), total_rows,
    )
    if failures:
        for f in failures[:10]:
            log.warning("  FAILED %s.%s: %s",
                        f["service"], f["model"], f.get("error", "")[:140])
        sys.exit(1)

# ─── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--services",
        help=f"Comma-separated services to run (default: all). "
             f"Options: {', '.join(V3_SERVICES)}",
    )
    ap.add_argument(
        "--models",
        help="Comma-separated model aliases to restrict to (e.g. invoice,patient).",
    )
    ap.add_argument(
        "--since",
        help="ISO timestamp override for updated_since on every job.",
    )
    ap.add_argument(
        "--full-refresh", action="store_true",
        help="Ignore watermarks — extract all records.",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Extract only — no S3 or Snowflake writes.",
    )
    ap.add_argument(
        "--no-resume", action="store_true",
        help="Ignore progress file — re-run all jobs.",
    )
    ap.add_argument(
        "--workers", type=int, default=PIPELINE_WORKERS,
        help=f"Parallel (service, model) jobs (default {PIPELINE_WORKERS}).",
    )
    args = ap.parse_args()

    run_pipeline(
        services=(
            [s.strip() for s in args.services.split(",") if s.strip()]
            if args.services else None
        ),
        only_models=(
            {m.strip() for m in args.models.split(",") if m.strip()}
            if args.models else None
        ),
        since=args.since,
        full_refresh=args.full_refresh,
        dry_run=args.dry_run,
        resume=not args.no_resume,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
