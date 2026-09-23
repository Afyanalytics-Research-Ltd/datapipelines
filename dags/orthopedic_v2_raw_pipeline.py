# dags/orthopedic_v2_raw_pipeline.py
"""
Afya Extraction API gateway → S3 → Snowflake HOSPITALS.ORTHOPEDIC_RAW_V2

DAG counterpart of the standalone orthopedic_v2_raw_pipeline.py CLI script.
Sibling of dags/orthopedic_api_to_snowflake.py (the v1 / 26-namespace DAG,
connection_id=16 → ORTHOPEDIC_RAW), but hits a SECOND Afya gateway
connection (connection_id=20, same facility_id=47) covering 19
clinical-encounter model namespaces (admission notes, triage, requests,
results, the patient master table, etc.) and lands them in the separate
HOSPITALS.ORTHOPEDIC_RAW_V2 schema, one table per model. All dag_id and
task_ids in this file are suffixed/prefixed "orthopedic_v2" so they never
collide with the v1 DAG.

GOVERNANCE — WHY THIS LAYER IS NOT DE-IDENTIFIED
  ORTHOPEDIC_RAW_V2 is a full-fidelity landing zone, deliberately unmasked
  (no anonymize/anonymize_fields/row_transform applied here), because
  de-identifying twice — once here, once in the clean layer — risks the two
  layers drifting and makes reprocessing/backfills lossy. Per data
  governance principle "de-identify once, close to consumption, keep raw
  lineage intact":
    - HOSPITALS.ORTHOPEDIC_RAW_V2 must be access-restricted to the ETL
      service role ONLY (grant to the pipeline's Snowflake role; do NOT
      grant to analyst/BI roles) — mirrors a bronze layer.
    - HOSPITALS.ORTHOPEDIC_CLEAN_V2 (built by a separate clean pipeline) is
      the de-identified, analyst-facing layer. Direct identifiers (patient
      name, phone, email, national ID, next-of-kin contacts, address, etc.)
      are hashed/redacted there instead.

Airflow Variables (optional — all have sane defaults, created on first use):
  ORTHOPEDIC_V2_GATEWAY_CONNECTION_ID   Gateway body "connection_id" (default: 20)
  ORTHOPEDIC_V2_FACILITY_ID             Gateway body "facility_id"   (default: 47)
  orthopedic_v2__orthopedic_v2_raw_pipeline__<table>
                                         Per-model watermark (ISO ts), auto-managed.

Airflow Connections required:
  orthopedic_v2_api_auth   host=https://afyapi.afyaanalytics.ai/api
                            login=<AFYA gateway username>  password=<AFYA gateway password>
  aws_default               S3 credentials (bucket: collabmedbucket)

Env vars (Snowflake key-pair auth, from .env / Docker secrets):
  SNOWFLAKE_USER  SNOWFLAKE_ACCOUNT  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_DATABASE  SNOWFLAKE_PRIVATE_KEY_PATH
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import requests
import snowflake.connector
from dotenv import load_dotenv
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

try:
    import orjson

    def _dumps_bytes(obj: object) -> bytes:
        return orjson.dumps(obj)
except ImportError:
    def _dumps_bytes(obj: object) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

load_dotenv(Path(__file__).parent.parent.parent.parent / ".env")
log = logging.getLogger(__name__)

DAG_ID = "orthopedic_v2_raw_pipeline"

API_CONN_ID = "orthopedic_v2_api_auth"
S3_CONN_ID  = "aws_default"

S3_BUCKET = "collabmedbucket"
S3_PREFIX = "raw/orthopedic_v2"

SF_DB          = "HOSPITALS"
SF_RAW_SCHEMA  = "ORTHOPEDIC_RAW_V2"
SF_SHARED      = "SHARED"
SF_STAGE       = f"{SF_DB}.{SF_SHARED}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT = f"{SF_DB}.{SF_SHARED}.JSON_FF"

DEFAULT_PAGE_WORKERS = 16
DEFAULT_PER_PAGE     = 950

# ─── MODEL REGISTRY ──────────────────────────────────────────────────────────
# Maps PHP namespace → Snowflake table name in ORTHOPEDIC_RAW_V2. Preserved
# verbatim from orthopedic_v2_raw_pipeline.py's MODELS list (19 models,
# connection_id=20). No anonymize / anonymize_fields / row_transform are set
# by default — see the GOVERNANCE note above for why.

MODELS: list[dict] = [
    {"namespace": r"App\Models\Admonotes",       "table": "admnotes"},
    {"namespace": r"App\Models\Cadex",            "table": "cadex"},
    {"namespace": r"App\Models\History",          "table": "history"},
    {"namespace": r"App\Models\ICD10diagnosis",   "table": "icd10diagnosis"},
    {"namespace": r"App\Models\ICD10diseases",    "table": "icd10diseases"},
    {"namespace": r"App\Models\Impression",       "table": "impression"},
    {"namespace": r"App\Models\Inpatients",       "table": "inpatients"},
    {"namespace": r"App\Models\Labrequests",      "table": "labrequests"},
    {"namespace": r"App\Models\Labtestresults",   "table": "labtestresults"},
    {"namespace": r"App\Models\Newprescription",  "table": "newprescription"},
    {"namespace": r"App\Models\Patientsmodel",    "table": "patients"},
    {"namespace": r"App\Models\Pharmrequests",    "table": "pharmrequests"},
    {"namespace": r"App\Models\phyexam",          "table": "phyexam"},
    {"namespace": r"App\Models\Physical",         "table": "physical"},
    {"namespace": r"App\Models\Procrequests",     "table": "procrequests"},
    {"namespace": r"App\Models\Progress",         "table": "progress"},
    {"namespace": r"App\Models\Radrequests",      "table": "radrequests"},
    {"namespace": r"App\Models\Theatrequests",    "table": "theatrequests"},
    {"namespace": r"App\Models\Triage",           "table": "triage"},
]


# ── Snowflake client (key-pair auth required for COPY INTO) ─────────────
class SnowflakeClient:
    def __init__(self, schema_: str | None = None):
        self._conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER").strip(),
            account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(),
            database=os.getenv("SNOWFLAKE_DATABASE").strip(),
            schema=schema_ or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC").strip(),
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip(),
        )

    def close(self):
        if self._conn:
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

    def execute(self, sql: str, label: str | None = None) -> dict:
        label = label or f"x:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-35s | %.120s…", label, " ".join(sql.split()))
        t0 = time.perf_counter()
        with self._cursor() as cur:
            cur.execute(sql)
            result = {"rowcount": cur.rowcount, "sfqid": cur.sfqid}
        log.info("✓ %-35s | rowcount=%s · %.2fs", label, result["rowcount"], time.perf_counter() - t0)
        return result

    def __enter__(self): return self
    def __exit__(self, *a): self.close()


# ── S3 key helpers ───────────────────────────────────────────────────────
def _safe(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-=\.\+]+", "_", (s or "").strip())


def _table_fqn(table: str) -> str:
    return f"{SF_DB}.{SF_RAW_SCHEMA}.{table.upper()}"


def _wm_key(table: str) -> str:
    return f"orthopedic_v2__{DAG_ID}__{table}"


# ── Gateway auth ─────────────────────────────────────────────────────────
def _login(base_url: str, username: str, password: str) -> str:
    login_url = f"{base_url.rstrip('/')}/auth/login"
    log.info("Authenticating as %s → %s", username, login_url)
    r = requests.post(
        login_url,
        json={"username": username, "password": password},
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Login failed [{r.status_code}] against {login_url}: {r.text[:300]}")
    token = (r.json() or {}).get("token")
    if not token:
        raise RuntimeError(f"Login succeeded but no 'token' field: {r.text[:300]}")
    return token


# ── Gateway pagination ───────────────────────────────────────────────────
def _extract_rows_and_pagination(payload: dict) -> tuple[list, dict]:
    """Return (rows, pagination) from a gateway response, handling the
    nested shape where data is {"data": {"data": [...], "current_page": 1}}.
    """
    rows       = payload.get("data") or []
    pagination = payload.get("pagination") or payload.get("meta") or {}
    if isinstance(rows, dict):
        pagination = {**rows, **pagination}
        rows       = rows.get("data") or list(rows.values())
    return (rows if isinstance(rows, list) else []), pagination


def _parse_last_page(pagination: dict) -> int | None:
    for key in ("last_page", "total_pages", "pageCount"):
        v = pagination.get(key)
        if v is not None:
            try:
                lp = int(v)
                return lp if lp > 0 else None
            except (TypeError, ValueError):
                pass
    return None


def _gateway_request(
    base_url: str,
    gateway_connection_id: int,
    facility_id: int,
    namespace: str,
    page: int,
    per_page: int,
    updated_since: str | None,
    token_box: list,
    creds: tuple[str, str],
    *,
    max_retries: int = 6,
    default_wait: int = 10,
    backoff: int = 2,
) -> dict:
    """POST {base_url}/gateway for one page, retrying 401/429/5xx/network.

    token_box is a 1-element list used as a mutable cell so a 401 can
    refresh the cached bearer token for subsequent pages within this task.
    """
    gateway_url = f"{base_url.rstrip('/')}/gateway"
    body: dict = {
        "connection_id": gateway_connection_id,
        "namespace":     namespace,
        "facility_id":   facility_id,
        "page":          page,
        "per_page":      per_page,
    }
    if updated_since and updated_since != "1970-01-01T00:00:00Z":
        body["updated_since"] = updated_since

    attempt, wait = 0, default_wait
    while True:
        attempt += 1
        headers = {
            "Authorization": f"Bearer {token_box[0]}",
            "Accept":        "application/json",
            "Content-Type":  "application/json",
        }
        try:
            r = requests.post(gateway_url, headers=headers, json=body, timeout=120)
            log.info("· namespace=%-30s page=%-4s status=%s", namespace.split("\\")[-1], page, r.status_code)

            if r.status_code == 401:
                if attempt >= max_retries:
                    raise RuntimeError(f"Gateway 401 after {max_retries} token refreshes — namespace={namespace} page={page}")
                token_box[0] = _login(base_url, *creds)
                continue

            if r.status_code == 404:
                raise RuntimeError(f"Gateway 404 — namespace={namespace} not registered for connection_id={gateway_connection_id}. {r.text[:300]}")

            if r.status_code == 422:
                raise RuntimeError(f"Gateway 422 — namespace={namespace} page={page}. body={body}. {r.text[:300]}")

            if r.status_code == 429:
                retry_after = default_wait
                try:
                    retry_after = int(r.json().get("retry_after_seconds", default_wait))
                except Exception:
                    pass
                if attempt >= max_retries:
                    raise RuntimeError(f"Gateway 429 after {max_retries} retries — namespace={namespace} page={page}")
                time.sleep(retry_after)
                continue

            if r.status_code in {500, 502, 503, 504}:
                if attempt >= max_retries:
                    raise RuntimeError(f"Gateway {r.status_code} after {max_retries} retries — namespace={namespace} page={page}")
                time.sleep(wait)
                wait = min(wait * backoff, 120)
                continue

            if not r.ok:
                raise RuntimeError(f"Gateway unexpected status {r.status_code} — namespace={namespace} page={page}. {r.text[:300]}")

            return r.json()

        except (Timeout, ConnectionError, ChunkedEncodingError) as e:
            if attempt >= max_retries:
                raise RuntimeError(f"Network error after {max_retries} retries — namespace={namespace} page={page}") from e
            time.sleep(wait)
            wait = min(wait * backoff, 120)


# ── DAG task callables ───────────────────────────────────────────────────
def ensure_orthopedic_v2_raw_schema(**context):
    """Create ORTHOPEDIC_RAW_V2 schema and all 19 per-model tables if missing."""
    with SnowflakeClient(schema_=SF_RAW_SCHEMA) as sf:
        sf.execute(f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_RAW_SCHEMA};", label="ensure_schema")
        for model in MODELS:
            fqn = _table_fqn(model["table"])
            sf.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {fqn} (
                    _run_id       VARCHAR       NOT NULL,
                    _namespace    VARCHAR       NOT NULL,
                    _ingested_at  TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    payload       VARIANT       NOT NULL
                );
                """,
                label=f"ensure:{model['table']}",
            )
    log.info("Schema and %d tables ready in %s.%s", len(MODELS), SF_DB, SF_RAW_SCHEMA)
    log.warning(
        "GOVERNANCE — confirm %s.%s is granted ONLY to the ETL service role. "
        "This schema is full-fidelity / NOT de-identified.",
        SF_DB, SF_RAW_SCHEMA,
    )


def prepare_all_jobs(**context) -> list[dict]:
    """Build one extraction job per model namespace for dynamic task mapping."""
    gateway_connection_id = int(Variable.get("ORTHOPEDIC_V2_GATEWAY_CONNECTION_ID", default_var="20"))
    facility_id           = int(Variable.get("ORTHOPEDIC_V2_FACILITY_ID", default_var="47"))

    all_jobs = []
    for model in MODELS:
        table = model["table"]
        updated_since = Variable.get(_wm_key(table), default_var="1970-01-01T00:00:00Z")
        all_jobs.append({
            "job": {
                "namespace":              model["namespace"],
                "table":                  table,
                "gateway_connection_id":  gateway_connection_id,
                "facility_id":            facility_id,
                "updated_since":          updated_since,
                "per_page":               DEFAULT_PER_PAGE,
                "page_workers":           DEFAULT_PAGE_WORKERS,
            }
        })
    log.info(
        "Prepared %d orthopedic_v2 model jobs (connection_id=%d facility_id=%d)",
        len(all_jobs), gateway_connection_id, facility_id,
    )
    return all_jobs


def extract_one_model(job: dict, **context) -> dict:
    """Fetch all pages for one model from the gateway, gzip-encode as JSONL,
    and upload a single S3 object for this model+run (all pages bundled)."""
    namespace              = job["namespace"]
    table                  = job["table"]
    gateway_connection_id  = job["gateway_connection_id"]
    facility_id            = job["facility_id"]
    updated_since          = job.get("updated_since")
    per_page               = job.get("per_page", DEFAULT_PER_PAGE)
    page_workers           = job.get("page_workers", DEFAULT_PAGE_WORKERS)
    ns_short                = namespace.split("\\")[-1]

    conn = BaseHook.get_connection(API_CONN_ID)
    base_url = conn.host
    creds = (conn.login, conn.password)
    token_box = [_login(base_url, *creds)]

    def _fetch(page: int) -> dict:
        return _gateway_request(
            base_url, gateway_connection_id, facility_id, namespace, page, per_page,
            updated_since, token_box, creds,
        )

    all_rows: list = []

    payload1 = _fetch(1)
    first_rows, pag1 = _extract_rows_and_pagination(payload1)
    if not first_rows:
        log.info("  %-30s  0 rows on page 1 — nothing to load", ns_short)
        return {
            "namespace": namespace, "table": table, "s3_key": None,
            "ingested_at": datetime.now(timezone.utc).isoformat(), "row_count": 0,
        }
    all_rows.extend(first_rows)
    last_page = _parse_last_page(pag1)

    if last_page is not None:
        remaining = list(range(2, last_page + 1))
        if remaining:
            log.info("  %-30s  last_page=%d  fetching %d page(s) concurrently", ns_short, last_page, len(remaining))
            with ThreadPoolExecutor(max_workers=max(1, page_workers)) as pool:
                futures = {pool.submit(_fetch, p): p for p in remaining}
                for fut in as_completed(futures):
                    rows, _ = _extract_rows_and_pagination(fut.result())
                    all_rows.extend(rows)
    else:
        # last_page unknown — walk sequentially until an empty page.
        log.info("  %-30s  last_page unknown — sequential exhaustion", ns_short)
        page = 1
        while page < 10_000:
            page += 1
            rows, pag = _extract_rows_and_pagination(_fetch(page))
            if not rows:
                break
            all_rows.extend(rows)
            if pag.get("has_more_pages") is False or pag.get("hasMorePages") is False:
                break

    ingested_at = datetime.now(timezone.utc)
    dt          = ingested_at.date().isoformat()
    run_id      = context["run_id"]

    key = (
        f"{S3_PREFIX}/"
        f"model={_safe(table)}/"
        f"dt={dt}/"
        f"{_safe(run_id)}.jsonl.gz"
    )

    jsonl_bytes = b"\n".join(_dumps_bytes(r) for r in all_rows) + b"\n"
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(jsonl_bytes)

    S3Hook(aws_conn_id=S3_CONN_ID).load_bytes(
        bytes_data=buf.getvalue(), key=key, bucket_name=S3_BUCKET, replace=True,
    )
    log.info("  %-30s  uploaded s3://%s/%s  rows=%d", ns_short, S3_BUCKET, key, len(all_rows))

    return {
        "namespace":   namespace,
        "table":       table,
        "s3_key":      key,
        "ingested_at": ingested_at.isoformat(),
        "row_count":   len(all_rows),
    }


def copy_into_orthopedic_v2_raw(**job_result):
    """COPY the model's S3 object into ORTHOPEDIC_RAW_V2.<table>."""
    table       = job_result.get("table")
    s3_key      = job_result.get("s3_key")
    namespace   = job_result.get("namespace") or ""
    ingested_at = job_result.get("ingested_at")
    run_id      = job_result.get("ingested_at", "") or ""

    if not s3_key:
        log.info("  %-30s  no S3 object (0 rows) — skipping COPY INTO", table)
        return {"table": table, "status": "empty"}

    fqn     = _table_fqn(table)
    ns_esc  = namespace.replace("'", "\\'")
    run_esc = run_id.replace("'", "\\'")

    sql = f"""
    COPY INTO {fqn} (_run_id, _namespace, _ingested_at, payload)
    FROM (
      SELECT
        '{run_esc}'::VARCHAR             AS _run_id,
        '{ns_esc}'::VARCHAR              AS _namespace,
        '{ingested_at}'::TIMESTAMP_TZ    AS _ingested_at,
        PARSE_JSON($1)                   AS payload
      FROM @{SF_STAGE}
    )
    FILES = ('{s3_key}')
    FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
    ON_ERROR = 'CONTINUE';
    """
    with SnowflakeClient(schema_=SF_RAW_SCHEMA) as sf:
        sf.execute(sql, label=f"copy:{table}")
    return {"table": table, "status": "ok"}


def update_watermarks(**context):
    """Advance each model's watermark to now, but only for models whose
    extract+copy succeeded this run (per-model gate, via copy task XCom)."""
    ti = context["ti"]
    copy_results = ti.xcom_pull(task_ids="copy_into_orthopedic_v2_raw") or []
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    updated = 0
    for r in copy_results:
        if not r or r.get("status") not in ("ok", "empty"):
            continue
        table = r.get("table")
        if table:
            Variable.set(_wm_key(table), now)
            updated += 1
    log.info("Updated watermarks for %d/%d orthopedic_v2 model(s) → %s", updated, len(MODELS), now)


# ── DAG definition ──────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    max_active_tasks=8,
    tags=["orthopedic", "v2", "api", "snowflake", "ingest"],
) as dag:

    t_ensure = PythonOperator(
        task_id="ensure_orthopedic_v2_raw_schema",
        python_callable=ensure_orthopedic_v2_raw_schema,
    )
    t_prepare = PythonOperator(
        task_id="prepare_orthopedic_v2_jobs",
        python_callable=prepare_all_jobs,
    )
    t_extract = PythonOperator.partial(
        task_id="extract_orthopedic_v2_to_s3",
        python_callable=extract_one_model,
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(op_kwargs=t_prepare.output)
    t_copy = PythonOperator.partial(
        task_id="copy_into_orthopedic_v2_raw",
        python_callable=copy_into_orthopedic_v2_raw,
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(op_kwargs=t_extract.output)
    t_watermark = PythonOperator(
        task_id="update_orthopedic_v2_watermarks",
        python_callable=update_watermarks,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_ensure >> t_prepare >> t_extract >> t_copy >> t_watermark
