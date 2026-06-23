# dags/v3_api_to_snowflake_raw.py
"""
Model Gateway API → S3 → Snowflake HOSPITALS.{FACILITY}_V3_RAW.EVENTS_RAW

Runs all facilities in a single DAG execution (no per-facility param).
Uses a V3-specific model-dictionary sheet so V3 namespaces can diverge
from the V2 migration sheet used by facility_api_to_snowflake.py.

Each model's full page-set is serialised as a JSON array and stored as one
EVENTS_RAW row (payload VARIANT).  v3_raw_to_v3_ready then flattens those
arrays into typed V3_READY tables.

Airflow Variables required:
  V3_IGNITE_SHEET_ID          Google Sheet key for V3 model dictionary
  V3_IGNITE_SHEET_WORKSHEET   Worksheet tab name (default: Sheet1)
  GOOGLE_SA_JSON              Google service-account credentials JSON

Airflow Connections required (one per facility key):
  afya_api_auth  kakamega  kisumu  lodwar  tenri  xanalife
    host=<base_url>  login=<username>  password=<password>

Env vars (from .env / Docker secrets):
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
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path

import gspread
import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, HTTPError, Timeout

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

load_dotenv(Path(__file__).parent.parent.parent.parent / ".env")
log = logging.getLogger(__name__)

DAG_ID = "v3_api_to_snowflake_raw"

FACILITIES: dict[str, dict] = {
    # "afya_api_auth": {"base_url": "https://staging.afyanalytics.ai",  "db": "staging_db",  "tenant_id": "afya"},
    # "kakamega":      {"base_url": "https://demo.collabmed.net",        "db": "kakamega_db", "tenant_id": "kakamega"},
    # "kisumu":        {"base_url": "https://kshospital.collabmed.net",   "db": "kisumu_db",   "tenant_id": "kisumu"},
    # "lodwar":        {"base_url": "https://lcrh.collabmed.net",         "db": "lodwar_db",   "tenant_id": "lodwar"},
    # "tenri":         {"base_url": "https://stageenv.collabmed.net",     "db": "tenri_db",    "tenant_id": "tenri"},
    # "xanalife":      {"base_url": "https://xanalife.afyanalytics.ai/",  "db": "xanalife_db", "tenant_id": "xanalife"},
    "collabmed":      {"base_url": "https://afyapi.afyaanalytics.ai/api/",  "db": "collabmed", "tenant_id": "collabmed"},
}

S3_CONN_ID       = "aws_default"
S3_BUCKET        = "collabmedbucket"
S3_PREFIX        = "raw/v3_facilities"

SF_DB            = "HOSPITALS"
SF_SHARED_SCHEMA = "SHARED"
SF_STAGE         = f"{SF_DB}.{SF_SHARED_SCHEMA}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT   = f"{SF_DB}.{SF_SHARED_SCHEMA}.JSON_FF"

_EVENTS_RAW_DDL = """
    CREATE TABLE IF NOT EXISTS {schema}.EVENTS_RAW (
        facility_id   VARCHAR        NOT NULL,
        source_table  VARCHAR        NOT NULL,
        module        VARCHAR,
        namespace     VARCHAR,
        ingested_at   TIMESTAMP_TZ   NOT NULL,
        payload       VARIANT
    )
"""


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
        log.info("▶ %-28s | %.120s…", label, " ".join(sql.split()))
        t0 = time.perf_counter()
        with self._cursor() as cur:
            cur.execute(sql)
            result = {"rowcount": cur.rowcount, "sfqid": cur.sfqid}
        log.info("✓ %-28s | rowcount=%s · %.2fs", label, result["rowcount"], time.perf_counter() - t0)
        return result

    def __enter__(self): return self
    def __exit__(self, *a): self.close()


# ── Google Sheet helpers ────────────────────────────────────────────────
_inflect_engine = None


def _get_inflect():
    global _inflect_engine
    if _inflect_engine is None:
        import inflect
        _inflect_engine = inflect.engine()
    return _inflect_engine


def _gsheet_client():
    return gspread.service_account_from_dict(json.loads(Variable.get("GOOGLE_SA_JSON")))


def _read_sheet(spreadsheet_id: str, worksheet: str) -> list[dict]:
    ws = _gsheet_client().open_by_key(spreadsheet_id).worksheet(worksheet)
    return ws.get_all_records()


# ── Namespace helpers ───────────────────────────────────────────────────
def _snake_to_pascal(s: str) -> str:
    return "".join(w.capitalize() for w in re.split(r"[_\s]+", s.strip()) if w)


def _build_namespace(module: str, table: str) -> str:
    mod = _snake_to_pascal(module)
    t = table.strip().lower()
    prefix = module.strip().lower() + "_"
    if t.startswith(prefix):
        t = t[len(prefix):]
    return f"Ignite\\{mod}\\Entities\\{_snake_to_pascal(t)}"


def _to_singular(ns: str) -> str:
    parts = ns.split("\\")
    if not parts:
        return ns
    singular = _get_inflect().singular_noun(parts[-1])
    parts[-1] = singular if singular else parts[-1]
    return "\\".join(parts)


def _double_ns(ns: str) -> str:
    parts = ns.split("\\")
    if len(parts) >= 2:
        parts[-1] = parts[1] + parts[-1]
    return "\\".join(parts)


# ── S3 / HTTP helpers ───────────────────────────────────────────────────
def _safe_token(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-=\.\+]+", "_", (s or "").strip())


def _post_with_retry(url, headers, bodies, timeout=60, max_retries=6,
                     retry_wait=10, backoff=2, base_delay=0.5):
    for idx, body in enumerate(bodies):
        attempt, wait = 0, retry_wait
        while True:
            attempt += 1
            try:
                r = requests.post(url=url, headers=headers, json=body, timeout=timeout)
                log.info("body=%d attempt=%d status=%d", idx, attempt, r.status_code)
                if r.status_code == 404:
                    log.warning("404 ns=%s – trying next body", body.get("namespace"))
                    break
                if r.status_code == 429:
                    delay = retry_wait
                    try:
                        delay = int(r.json().get("retry_after_seconds", retry_wait))
                    except Exception:
                        pass
                    if attempt >= max_retries:
                        r.raise_for_status()
                    time.sleep(delay)
                    continue
                if r.status_code in {500, 502, 503, 504}:
                    if attempt >= max_retries:
                        r.raise_for_status()
                    time.sleep(wait)
                    wait *= backoff
                    continue
                r.raise_for_status()
                if base_delay > 0:
                    time.sleep(base_delay)
                return r, body
            except (Timeout, ConnectionError) as e:
                if attempt >= max_retries:
                    raise
                log.warning("network err ns=%s: %s – retry %d/%d in %ds",
                            body.get("namespace"), e, attempt, max_retries, wait)
                time.sleep(wait)
                wait *= backoff
            except HTTPError:
                raise
    raise Exception("All fallback bodies returned 404")


def _extract_pages(url, headers, body, singular_body, double_body, double_singular_body,
                   timeout=60, max_pages=10_000, max_retries=6,
                   retry_wait=10, backoff=2, base_delay=0.5) -> list:
    def _rows(payload):
        rows = payload.get("data")
        if rows is None:
            sv = payload.get("success")
            rows = (sv.get("data") or []) if isinstance(sv, dict) else []
        if isinstance(rows, dict):
            rows = rows.get("data") or []
        return rows if isinstance(rows, list) else []

    page = 1
    r, chosen = _post_with_retry(url, headers, [
        {**body,                  "page": page},
        {**singular_body,         "page": page},
        {**double_body,           "page": page},
        {**double_singular_body,  "page": page},
    ], timeout=timeout, max_retries=max_retries,
       retry_wait=retry_wait, backoff=backoff, base_delay=base_delay)

    payload   = r.json()
    all_rows  = _rows(payload)
    pag       = payload.get("pagination") or {}
    has_more  = bool(pag.get("has_more_pages", False))
    last_page = pag.get("last_page")

    while has_more:
        page += 1
        if page > max_pages or (last_page and page > int(last_page)):
            break
        r, chosen = _post_with_retry(url, headers, [{**chosen, "page": page}],
                                      timeout=timeout, max_retries=max_retries,
                                      retry_wait=retry_wait, backoff=backoff, base_delay=base_delay)
        payload  = r.json()
        rows     = _rows(payload)
        all_rows.extend(rows)
        pag       = payload.get("pagination") or {}
        has_more  = bool(pag.get("has_more_pages", False))
        last_page = pag.get("last_page", last_page)
        if not rows:
            break

    return all_rows


def _auth_token(connection_id: str) -> str:
    conn = BaseHook.get_connection(connection_id)
    r = requests.post(f"{conn.host.rstrip('/')}/auth/login",
                      json={"username": conn.login, "password": conn.password})
    if r.status_code != 200:
        raise Exception(f"Auth failed [{connection_id}]: {r.text}")
    data = r.json()
    success = data.get("success", {})
    token = success.get("token") if isinstance(success, dict) else data.get("token")
    if not token:
        raise Exception(f"Token missing for {connection_id}")
    return token


# ── Schema / watermark helpers ──────────────────────────────────────────
def _v3_raw_schema(facility: str) -> str:
    return f"{SF_DB}.{facility.upper()}_V3_RAW"


def _wm_key(facility: str) -> str:
    return f"v3__{DAG_ID}__{facility}"


# ── DAG task callables ──────────────────────────────────────────────────
def ensure_v3_raw_schemas(**context):
    """Create per-facility V3_RAW schema + EVENTS_RAW table for every facility."""
    with SnowflakeClient() as sf:
        for facility in FACILITIES:
            schema = _v3_raw_schema(facility)
            sf.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}",    label=f"schema:{facility}")
            sf.execute(_EVENTS_RAW_DDL.format(schema=schema),      label=f"events_raw:{facility}")
    log.info("Ensured V3_RAW schemas for %d facilities", len(FACILITIES))


def prepare_all_jobs(**context) -> list[dict]:
    """
    Build one extraction job per (facility, model) from the V3 Google Sheet.
    Returns a list of {"job": {...}} dicts for dynamic task mapping.
    """
    sheet_id  = Variable.get("IGNITE_SHEET_ID")
    sheet_tab = Variable.get("IGNITE_SHEET_WORKSHEET", default_var="Sheet1")
    rows      = _read_sheet(sheet_id, sheet_tab)

    all_jobs = []
    for facility, cfg in FACILITIES.items():
        last_run = Variable.get(_wm_key(facility), default_var="1970-01-01T00:00:00Z")
        seen: set[tuple] = set()
        count = 0
        for r in rows:
            module = (r.get("module") or "").strip()
            table  = (r.get("table")  or "").strip()
            if not module or not table:
                continue
            key = (module.lower(), table.lower())
            if key in seen:
                continue
            seen.add(key)
            count += 1
            all_jobs.append({
                "job": {
                    "facility":      facility,
                    "module":        module,
                    "table":         table,
                    "namespace":     _build_namespace(module, table),
                    "database":      cfg["db"],
                    "updated_since": last_run,
                    "limit":         500,
                }
            })
        log.info("Prepared %d model jobs for facility=%s", count, facility)

    log.info("Total jobs across all facilities: %d", len(all_jobs))
    return all_jobs


def extract_one_model(job: dict, **context):
    """Fetch all pages for one model from the gateway API and upload to S3."""
    facility = job["facility"]
    cfg      = FACILITIES[facility]
    ns       = job["namespace"]

    url   = f"{cfg['base_url'].rstrip('/')}gateway"
    token = _auth_token(facility)
    hdrs  = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    base = {
        "namespace":     ns,
        "action":        "get",
        "database":      job["database"],
        "updated_since": job["updated_since"],
        "limit":         job["limit"],
    }

    all_rows = _extract_pages(
        url=url, headers=hdrs,
        body=base,
        singular_body={**base,       "namespace": _to_singular(ns)},
        double_body={**base,         "namespace": _double_ns(ns)},
        double_singular_body={**base,"namespace": _double_ns(_to_singular(ns))},
    )

    ingested_at = datetime.now(timezone.utc)
    dt          = ingested_at.date().isoformat()
    run_id      = context["run_id"]

    key = (
        f"{S3_PREFIX}/"
        f"facility_id={facility}/"
        f"module={_safe_token(job.get('module', ''))}/"
        f"table={_safe_token(job.get('table', ''))}/"
        f"namespace={_safe_token(ns.replace(chr(92), '_'))}/"
        f"dt={dt}/"
        f"{run_id}.jsonl.gz"
    )

    # Serialise the whole page as a single JSON array on one JSONL line so
    # COPY INTO stores one VARIANT array per model call (LATERAL FLATTEN
    # in v3_raw_to_v3_ready expands it back to individual records).
    jsonl_line = json.dumps(all_rows, separators=(",", ":")) + "\n"

    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(jsonl_line.encode("utf-8"))

    S3Hook(aws_conn_id=S3_CONN_ID).load_bytes(
        bytes_data=buf.getvalue(), key=key, bucket_name=S3_BUCKET, replace=True,
    )
    log.info("Uploaded s3://%s/%s  rows=%d", S3_BUCKET, key, len(all_rows))

    return {
        "facility":    facility,
        "module":      job.get("module"),
        "table":       job.get("table"),
        "namespace":   ns,
        "ingested_at": ingested_at.isoformat(),
        "s3_key":      key,
        "row_count":   len(all_rows),
    }


def copy_into_v3_raw(**job_result):
    """COPY one S3 file into the facility's V3_RAW.EVENTS_RAW table."""
    facility     = job_result["facility"]
    s3_key       = job_result["s3_key"]
    ingested_at  = job_result["ingested_at"]
    source_table = job_result.get("table")    or ""
    module       = job_result.get("module")   or ""
    namespace    = job_result.get("namespace")or ""

    raw_table = f"{_v3_raw_schema(facility)}.EVENTS_RAW"

    sql = f"""
    COPY INTO {raw_table} (facility_id, source_table, module, namespace, ingested_at, payload)
    FROM (
      SELECT
        '{facility}'::VARCHAR         AS facility_id,
        '{source_table}'::VARCHAR     AS source_table,
        '{module}'::VARCHAR           AS module,
        '{namespace}'::VARCHAR        AS namespace,
        '{ingested_at}'::TIMESTAMP_TZ AS ingested_at,
        PARSE_JSON($1)                AS payload
      FROM @{SF_STAGE}
    )
    FILES = ('{s3_key}')
    FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
    ON_ERROR = 'CONTINUE';
    """
    with SnowflakeClient(schema_=_v3_raw_schema(facility)) as sf:
        sf.execute(sql, label=f"copy_v3_raw:{facility}:{source_table or '?'}")


def update_watermarks(**context):
    """Stamp each facility's watermark to now for the next incremental run."""
    now = datetime.now(timezone.utc).isoformat()
    for facility in FACILITIES:
        Variable.set(_wm_key(facility), now)
    log.info("Updated V3 watermarks → %s", now)


# ── DAG definition ──────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    max_active_tasks=8,
    tags=["v3", "api", "snowflake", "ingest"],
) as dag:

    t_ensure = PythonOperator(
        task_id="ensure_v3_raw_schemas",
        python_callable=ensure_v3_raw_schemas,
    )
    t_prepare = PythonOperator(
        task_id="prepare_all_jobs",
        python_callable=prepare_all_jobs,
    )
    t_extract = PythonOperator.partial(
        task_id="extract_to_s3",
        python_callable=extract_one_model,
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(op_kwargs=t_prepare.output)
    t_copy = PythonOperator.partial(
        task_id="copy_into_v3_raw",
        python_callable=copy_into_v3_raw,
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(op_kwargs=t_extract.output)
    t_watermark = PythonOperator(
        task_id="update_watermarks",
        python_callable=update_watermarks,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_ensure >> t_prepare >> t_extract >> t_copy >> t_watermark
