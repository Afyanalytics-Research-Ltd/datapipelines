# dags/orthopedic_api_to_snowflake.py
"""
Afya Extraction API gateway → S3 → Snowflake HOSPITALS.ORTHOPEDIC_RAW

DAG (v1) sibling of orthopedic_v2_raw_pipeline.py / dags/orthopedic_v2_raw_pipeline.py.
Converted from the standalone script orthopedic_api_to_snowflake.py (repo root).

Extracts 26 model namespaces from the Afya Extraction platform (tenant="orthopedic",
gateway connection_id=16, facility_id=47 by default) into HOSPITALS.ORTHOPEDIC_RAW
per-model tables.

Flow:
  1. POST <base_url>/auth/login (Afya Extraction credentials) → bearer token.
  2. For each of the 26 configured namespaces, POST <base_url>/gateway with
     connection_id, facility_id, namespace, page, per_page, and (per-model)
     anonymize / anonymize_fields / anonymize_skip.
  3. Paginate all pages (fan out once last_page is known from page 1; otherwise
     exhaust sequentially).
  4. Apply local PII redaction for fields the gateway's own anonymizer cannot
     reach (see _redact_orderitementries_pii / _redact_supplier_contacts below
     — preserved verbatim from the source script; this matters for compliance).
  5. Gzip-encode rows as JSONL (one JSON object per line) → upload to S3.
  6. COPY INTO HOSPITALS.ORTHOPEDIC_RAW.<model_table> from the S3 object.

Resumability note: the standalone script used local JSON files
(.orthopedic_watermarks.json / .orthopedic_progress.json) for page-level
resume across CLI invocations. In this DAG, resumability/idempotency is
instead handled by Airflow itself (task retries, one mapped task per model,
one S3 object + one COPY INTO per model per DAG run) and incremental loads
use Airflow Variables as watermarks, keyed per model/table. There is no
local-file, page-level checkpointing in this version.

Airflow Variables required:
  ORTHOPEDIC_AFYA_CONNECTION_ID   Afya Extraction gateway connection_id (default: 16)
  ORTHOPEDIC_AFYA_FACILITY_ID     Afya Extraction gateway facility_id   (default: 47)
  (watermarks are managed automatically: orthopedic__orthopedic_api_to_snowflake__<table>)

Airflow Connections required:
  orthopedic_api_auth   host=<Afya Extraction base URL, e.g. https://afyapi.afyaanalytics.ai/api>
                         login=<username>  password=<password>
  aws_default            S3 credentials (used via S3Hook)

Env vars (from .env / Docker secrets):
  SNOWFLAKE_USER  SNOWFLAKE_ACCOUNT  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_DATABASE  SNOWFLAKE_PRIVATE_KEY_PATH
  S3_BUCKET       (default: collabmedbucket)
  PER_PAGE        (default: 950)
  PAGE_WORKERS    (default: 4)
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

load_dotenv(Path(__file__).parent.parent.parent.parent / ".env")
log = logging.getLogger(__name__)

DAG_ID = "orthopedic_api_to_snowflake"
TENANT = "orthopedic"

AFYA_CONN_ID = "orthopedic_api_auth"

S3_CONN_ID = "aws_default"
S3_BUCKET  = os.getenv("S3_BUCKET", "collabmedbucket")
S3_PREFIX  = "raw/orthopedic"

SF_DB            = os.getenv("SNOWFLAKE_DATABASE", "HOSPITALS")
SF_RAW_SCHEMA    = "ORTHOPEDIC_RAW"
SF_SHARED_SCHEMA = "SHARED"
SF_STAGE         = f"{SF_DB}.{SF_SHARED_SCHEMA}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT   = f"{SF_DB}.{SF_SHARED_SCHEMA}.JSON_FF"

DEFAULT_PER_PAGE     = int(os.getenv("PER_PAGE", "950"))
DEFAULT_PAGE_WORKERS = int(os.getenv("PAGE_WORKERS", "4"))

_ORTHOPEDIC_RAW_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS {fqn} (
        _run_id       VARCHAR       NOT NULL,
        _namespace    VARCHAR       NOT NULL,
        _ingested_at  TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
        payload       VARIANT       NOT NULL
    )
"""


# ── MODEL REGISTRY ──────────────────────────────────────────────────────
# Maps PHP namespace -> Snowflake table name in ORTHOPEDIC_RAW.
# Preserved verbatim from orthopedic_api_to_snowflake.py (root script).

_ORDERITEMENTRIES_STAFF_NAME_FIELDS = {"surgeon", "anaesthetist"}


def _redact_orderitementries_pii(rows: list) -> list:
    """OrderItemEntry.fields[] is a nested array the gateway's anonymize_fields
    cannot reach (confirmed empirically — it only touches top-level fields).
    Redact ourselves:
      - free-text form entries hold clinical notes and staff names typed by
        hand. Matched on 'textarea' as a case-insensitive substring of type,
        not an exact match — the form builder emits 'Textarea',
        'IncrementalTextarea', and lowercase 'textarea' for the same kind of
        free-text field, and an exact match silently missed the other two.
      - Surgeon/Anaesthetist entries (type SearchableFromPrevious) hold real
        theatre staff names (e.g. "DR MARANYA", "DR WANJALA"), confirmed."""
    for r in rows:
        fields = r.get("fields")
        if isinstance(fields, list):
            for f in fields:
                if not isinstance(f, dict):
                    continue
                ftype = (f.get("type") or "").lower()
                fname = (f.get("name") or "").strip().lower()
                if "textarea" in ftype or fname in _ORDERITEMENTRIES_STAFF_NAME_FIELDS:
                    f["value"] = ["[REDACTED]"]
    return rows


def _redact_supplier_contacts(rows: list) -> list:
    """Supplier.emails/phones are arrays — passing them in anonymize_fields
    500s the gateway ("Array to string conversion", confirmed empirically: the
    vendor's anonymizer can't mask array-typed fields). Redact them ourselves."""
    for r in rows:
        if isinstance(r.get("emails"), list) and r["emails"]:
            r["emails"] = ["[REDACTED]"]
        if isinstance(r.get("phones"), list) and r["phones"]:
            r["phones"] = ["[REDACTED]"]
    return rows


# row_transform is stored as a string key (not a function reference) so each
# job dict stays JSON-serialisable across XCom for dynamic task mapping.
_ROW_TRANSFORMS = {
    "orderitementries_pii": _redact_orderitementries_pii,
    "supplier_contacts":    _redact_supplier_contacts,
}

MODELS: list[dict] = [
    {"namespace": r"App\Models\OrderItemEntry",       "table": "orderitementries", "row_transform": "orderitementries_pii"},
    {"namespace": r"App\Models\SingleOrderItem",      "table": "singleorderitems", "anonymize": False},
    {"namespace": r"App\Models\Order",                "table": "orders"},
    {"namespace": r"App\Models\LedgerEntry",          "table": "ledgerentries", "anonymize_fields": ["subjectName", "notes"]},
    {"namespace": r"App\Models\StatementEntry",       "table": "statemententries", "anonymize_fields": ["notes"]},
    {"namespace": r"App\Models\InventoryLedgerEntry", "table": "inventoryledgerentries", "anonymize_fields": ["desc"]},
    {"namespace": r"App\Models\Payment",              "table": "payments", "anonymize_fields": ["subjectName"]},
    {"namespace": r"App\Models\Request",              "table": "requests"},
    {"namespace": r"App\Models\QueueEntry",           "table": "queueentries", "anonymize_fields": ["speech"]},
    {"namespace": r"App\Models\Coding",               "table": "codings"},
    {"namespace": r"App\Models\PatientScheme",        "table": "patientschemes"},
    {"namespace": r"App\Models\SystemLog",            "table": "systemlogs"},
    {"namespace": r"App\Models\ErrorLog",             "table": "errorlogs"},
    {"namespace": r"App\Models\PatientPlan",          "table": "patientplans"},
    {"namespace": r"App\Models\ReorderLevel",         "table": "reorderlevels"},
    {"namespace": r"App\Models\SaleItem",             "table": "saleitems", "anonymize": False},
    {"namespace": r"App\Models\InventoryItem",        "table": "inventoryitems"},
    {"namespace": r"App\Models\PurchaseOrder",        "table": "purchaseorders"},
    {"namespace": r"App\Models\Report",               "table": "reports"},
    {"namespace": r"App\Models\Shift",                "table": "shifts"},
    {"namespace": r"App\Models\Supplier",             "table": "suppliers", "anonymize_fields": ["name"], "row_transform": "supplier_contacts"},
    {"namespace": r"App\Models\PatientInvoice",       "table": "patientinvoices"},
    {"namespace": r"App\Models\Diagnosis2",           "table": "diagnoses2", "anonymize": False},
    {"namespace": r"App\Models\Patient2",             "table": "patients2", "anonymize_fields": ["name", "phone", "nokName", "nokPhone", "email"]},
    {"namespace": r"App\Models\Invoice2",             "table": "invoices2"},
    {"namespace": r"App\Models\Users2",               "table": "users2"},
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
        log.info("▶ %-28s | %.120s…", label, " ".join(sql.split()))
        t0 = time.perf_counter()
        with self._cursor() as cur:
            cur.execute(sql)
            result = {"rowcount": cur.rowcount, "sfqid": cur.sfqid}
        log.info("✓ %-28s | rowcount=%s · %.2fs", label, result["rowcount"], time.perf_counter() - t0)
        return result

    def __enter__(self): return self
    def __exit__(self, *a): self.close()


def _table_fqn(table: str) -> str:
    return f"{SF_DB}.{SF_RAW_SCHEMA}.{table.upper()}"


def _safe(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-=\.\+]+", "_", (s or "").strip())


def _wm_key(table: str) -> str:
    return f"{TENANT}__{DAG_ID}__{table}"


# ── Afya Extraction gateway auth + pagination ───────────────────────────
def _get_afya_connection_id() -> int:
    return int(Variable.get("ORTHOPEDIC_AFYA_CONNECTION_ID", default_var="16"))


def _get_afya_facility_id() -> int:
    return int(Variable.get("ORTHOPEDIC_AFYA_FACILITY_ID", default_var="47"))


def _afya_login() -> tuple[str, str]:
    """Authenticate against the Afya Extraction gateway using the
    orthopedic_api_auth Airflow Connection. Returns (token, base_url)."""
    conn = BaseHook.get_connection(AFYA_CONN_ID)
    base_url = conn.host.rstrip("/")
    login_url = f"{base_url}/auth/login"
    username = conn.login
    password = conn.password
    if not username or not password:
        raise RuntimeError(
            f"Airflow Connection '{AFYA_CONN_ID}' is missing login/password."
        )
    r = requests.post(
        login_url,
        json={"username": username, "password": password},
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=30,
    )
    if r.status_code == 401:
        raise RuntimeError(f"Afya login rejected (401) for '{username}' at {login_url}. {r.text[:300]}")
    r.raise_for_status()
    try:
        body = r.json()
    except Exception:
        raise RuntimeError(f"Afya login returned 200 but non-JSON body. Raw: {r.text[:300]}")
    token = body.get("token")
    if not token:
        raise RuntimeError(f"Afya login succeeded but no 'token' field. Keys: {list(body.keys())}")
    log.info("Authenticated as %s -> %s", username, login_url)
    return token, base_url


def _extract_rows_and_pagination(payload: dict) -> tuple[list, dict]:
    """Return (rows, pagination) from a gateway response. Handles nested
    shapes where data is {"data": {"data": [...], "current_page": 1, ...}}."""
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
    token_ref: dict,
    namespace: str,
    page: int,
    per_page: int,
    updated_since: str | None,
    connection_id: int,
    facility_id: int,
    anonymize: bool | None,
    anonymize_fields: list[str] | None,
    anonymize_skip: list[str] | None,
    *,
    max_retries: int = 6,
    default_wait: int = 10,
    backoff: int = 2,
) -> dict:
    """POST <base_url>/gateway for a single page with retry for 401/429/5xx/network.

    anonymize_fields and anonymize_skip are mutually exclusive gateway options:
      - anonymize_fields: ONLY these fields are anonymized; everything else is real.
      - anonymize_skip:   everything is anonymized per the gateway's own default
                           field set EXCEPT these fields, which are left real.
    """
    if anonymize_fields and anonymize_skip:
        raise ValueError("anonymize_fields and anonymize_skip are mutually exclusive")

    body: dict = {
        "connection_id": connection_id,
        "namespace":     namespace,
        "facility_id":   facility_id,
        "page":          page,
        "per_page":      per_page,
    }
    if updated_since and updated_since != "1970-01-01T00:00:00Z":
        body["updated_since"] = updated_since
    if anonymize is not None:
        body["anonymize"] = anonymize
    if anonymize_fields:
        body["anonymize_fields"] = anonymize_fields
    if anonymize_skip:
        body["anonymize_skip"] = anonymize_skip

    gateway_url = f"{base_url}/gateway"
    ns_short = namespace.split("\\")[-1]
    attempt, wait = 0, default_wait
    while True:
        attempt += 1
        try:
            r = requests.post(
                gateway_url,
                headers={
                    "Authorization": f"Bearer {token_ref['token']}",
                    "Accept":        "application/json",
                    "Content-Type":  "application/json",
                },
                json=body,
                timeout=120,
            )
            log.info("  namespace=%-45s page=%-4s status=%s", ns_short, page, r.status_code)

            if r.status_code == 401:
                if attempt >= max_retries:
                    raise RuntimeError(
                        f"Gateway 401 Unauthorized after {max_retries} token refreshes — "
                        f"namespace={namespace} page={page}. Response: {r.text[:300]}"
                    )
                log.warning("  401 — refreshing token and retrying (%d/%d)", attempt, max_retries)
                token_ref["token"], _ = _afya_login()
                continue

            if r.status_code == 404:
                raise RuntimeError(
                    f"Gateway 404 Not Found — namespace={namespace} url={gateway_url}. "
                    f"Response: {r.text[:300]}"
                )

            if r.status_code == 422:
                raise RuntimeError(
                    f"Gateway 422 Unprocessable — namespace={namespace} page={page}. "
                    f"Request body: {body}. Response: {r.text[:300]}"
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
                        f"namespace={namespace} page={page}. Response: {r.text[:200]}"
                    )
                log.warning("  429 rate-limited — sleeping %ds (%d/%d)", retry_after, attempt, max_retries)
                time.sleep(retry_after)
                continue

            if r.status_code in {500, 502, 503, 504}:
                if attempt >= max_retries:
                    raise RuntimeError(
                        f"Gateway {r.status_code} server error after {max_retries} retries — "
                        f"namespace={namespace} page={page}. Response: {r.text[:300]}"
                    )
                log.warning("  %s server error — sleeping %ds (%d/%d)", r.status_code, wait, attempt, max_retries)
                time.sleep(wait)
                wait = min(wait * backoff, 120)
                continue

            if not r.ok:
                raise RuntimeError(
                    f"Gateway unexpected status {r.status_code} — "
                    f"namespace={namespace} page={page}. Response: {r.text[:300]}"
                )

            try:
                return r.json()
            except Exception:
                raise RuntimeError(
                    f"Gateway returned 200 but response is not valid JSON — "
                    f"namespace={namespace} page={page}. Raw: {r.text[:300]}"
                )

        except (Timeout, ConnectionError, ChunkedEncodingError) as e:
            if attempt >= max_retries:
                raise RuntimeError(
                    f"Network error after {max_retries} retries — "
                    f"namespace={namespace} page={page} url={gateway_url}. Cause: {e}"
                ) from e
            log.warning("  network error page=%d — sleeping %ds (%d/%d): %s", page, wait, attempt, max_retries, e)
            time.sleep(wait)
            wait = min(wait * backoff, 120)


def fetch_all_pages(
    base_url: str,
    token_ref: dict,
    namespace: str,
    per_page: int,
    updated_since: str | None,
    connection_id: int,
    facility_id: int,
    anonymize: bool | None,
    anonymize_fields: list[str] | None,
    anonymize_skip: list[str] | None,
    *,
    page_workers: int = DEFAULT_PAGE_WORKERS,
    max_pages: int = 10_000,
) -> list:
    """Exhaust all pages for a namespace.

    Page 1 is fetched first to discover last_page. If last_page > 1 the
    remaining pages are fetched concurrently (fan-out). When the API does not
    include a last_page the fallback is sequential — pages are fetched until
    an empty response is returned. The row-count vs per_page heuristic is
    intentionally NOT used as a stop condition (some APIs enforce their own
    internal page cap regardless of the requested per_page value).
    """
    def _req(page: int) -> dict:
        return _gateway_request(
            base_url, token_ref, namespace, page, per_page, updated_since,
            connection_id, facility_id, anonymize, anonymize_fields, anonymize_skip,
        )

    ns_short = namespace.split("\\")[-1]
    payload1 = _req(1)
    first_rows, pag1 = _extract_rows_and_pagination(payload1)
    if not first_rows:
        return []

    all_rows  = list(first_rows)
    last_page = _parse_last_page(pag1)

    if last_page is not None and last_page <= 1:
        log.info("  %-35s  page 1/1  rows=%d", ns_short, len(all_rows))
        return all_rows

    if last_page is not None:
        last_page = min(last_page, max_pages)
        pages     = list(range(2, last_page + 1))
        log.info("  %-35s  last_page=%d — fanning out %d page(s)", ns_short, last_page, len(pages))

        with ThreadPoolExecutor(max_workers=max(1, page_workers)) as pool:
            page_rows: dict[int, list] = {}
            futs = {pool.submit(_req, p): p for p in pages}
            for fut in as_completed(futs):
                p = futs[fut]
                rows, _ = _extract_rows_and_pagination(fut.result())
                page_rows[p] = rows

        for p in pages:
            all_rows.extend(page_rows.get(p, []))
        log.info("  %-35s  done  total_rows=%d", ns_short, len(all_rows))
        return all_rows

    # Unknown page count — sequential until the API returns an empty page.
    log.info("  %-35s  no last_page — sequential exhaustion", ns_short)
    page = 1
    while page < max_pages:
        page += 1
        rows, pag = _extract_rows_and_pagination(_req(page))
        if not rows:
            break
        all_rows.extend(rows)

        lp = _parse_last_page(pag)
        if lp is not None and page >= lp:
            break
        if (pag.get("has_more_pages") is False or pag.get("hasMorePages") is False) and lp is None:
            break

    log.info("  %-35s  done  pages=%d  total_rows=%d", ns_short, page, len(all_rows))
    return all_rows


# ── DAG task callables ──────────────────────────────────────────────────
def ensure_orthopedic_raw_schema(**context):
    """Create ORTHOPEDIC_RAW schema and all per-model tables if they don't exist."""
    with SnowflakeClient(schema_=SF_RAW_SCHEMA) as sf:
        sf.execute(f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_RAW_SCHEMA}", label="ensure_schema")
        for model in MODELS:
            fqn = _table_fqn(model["table"])
            sf.execute(_ORTHOPEDIC_RAW_TABLE_DDL.format(fqn=fqn), label=f"ensure:{model['table']}")
    log.info("Schema and %d tables ready in %s.%s", len(MODELS), SF_DB, SF_RAW_SCHEMA)


def prepare_all_jobs(**context) -> list[dict]:
    """Build one extraction job per model namespace, carrying its current
    watermark. Returns a list of {"job": {...}} dicts for dynamic task mapping."""
    jobs = []
    for model in MODELS:
        watermark = Variable.get(_wm_key(model["table"]), default_var="1970-01-01T00:00:00Z")
        jobs.append({
            "job": {
                "table":            model["table"],
                "namespace":        model["namespace"],
                "anonymize":        model.get("anonymize"),
                "anonymize_fields": model.get("anonymize_fields"),
                "anonymize_skip":   model.get("anonymize_skip"),
                "row_transform":    model.get("row_transform"),
                "updated_since":    watermark,
            }
        })
    log.info("Prepared %d model job(s) for tenant=%s", len(jobs), TENANT)
    return jobs


def extract_one_model(job: dict, **context) -> dict:
    """Fetch all pages for one model namespace, apply local PII redaction,
    and upload the result to S3 as a single gzip JSONL object."""
    table     = job["table"]
    namespace = job["namespace"]
    ns_short  = namespace.split("\\")[-1]

    token, base_url = _afya_login()
    token_ref = {"token": token}

    updated_since = job.get("updated_since")
    if updated_since == "1970-01-01T00:00:00Z":
        updated_since = None

    rows = fetch_all_pages(
        base_url, token_ref, namespace,
        DEFAULT_PER_PAGE, updated_since,
        _get_afya_connection_id(), _get_afya_facility_id(),
        job.get("anonymize"), job.get("anonymize_fields"), job.get("anonymize_skip"),
        page_workers=DEFAULT_PAGE_WORKERS,
    )

    transform_key = job.get("row_transform")
    if transform_key and rows:
        rows = _ROW_TRANSFORMS[transform_key](rows)

    ingested_at = datetime.now(timezone.utc)
    dt          = ingested_at.date().isoformat()
    run_id      = context["run_id"]

    if not rows:
        log.info("  %-35s  0 rows — nothing to upload", ns_short)
        return {
            "table": table, "namespace": namespace,
            "ingested_at": ingested_at.isoformat(), "run_id": run_id,
            "s3_key": None, "row_count": 0,
        }

    key = (
        f"{S3_PREFIX}/"
        f"model={_safe(table)}/"
        f"dt={dt}/"
        f"{_safe(run_id)}.jsonl.gz"
    )

    # One JSON object per line (not one array per file) so COPY INTO's
    # PARSE_JSON($1) yields exactly one ORTHOPEDIC_RAW row per source record.
    jsonl_bytes = b"\n".join(json.dumps(r, separators=(",", ":")).encode("utf-8") for r in rows) + b"\n"
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(jsonl_bytes)

    S3Hook(aws_conn_id=S3_CONN_ID).load_bytes(
        bytes_data=buf.getvalue(), key=key, bucket_name=S3_BUCKET, replace=True,
    )
    log.info("  %-35s  uploaded s3://%s/%s  rows=%d", ns_short, S3_BUCKET, key, len(rows))

    return {
        "table": table, "namespace": namespace,
        "ingested_at": ingested_at.isoformat(), "run_id": run_id,
        "s3_key": key, "row_count": len(rows),
    }


def copy_into_orthopedic_raw(**job_result):
    """COPY one S3 file into HOSPITALS.ORTHOPEDIC_RAW.<table>."""
    table       = job_result["table"]
    namespace   = job_result["namespace"]
    s3_key      = job_result.get("s3_key")
    ingested_at = job_result["ingested_at"]
    run_id      = job_result["run_id"]

    if not s3_key:
        log.info("  %-35s  no S3 key (0 rows) — skipping COPY INTO", table)
        return

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


def update_watermarks(**context):
    """Stamp each model's watermark to now for the next incremental run."""
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    for model in MODELS:
        Variable.set(_wm_key(model["table"]), now)
    log.info("Updated orthopedic watermarks -> %s for %d model(s)", now, len(MODELS))


# ── DAG definition ──────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    max_active_tasks=6,
    tags=["orthopedic", "v1", "api", "snowflake", "ingest"],
) as dag:

    t_ensure = PythonOperator(
        task_id="ensure_orthopedic_raw_schema",
        python_callable=ensure_orthopedic_raw_schema,
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
        task_id="copy_into_orthopedic_raw",
        python_callable=copy_into_orthopedic_raw,
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(op_kwargs=t_extract.output)
    t_watermark = PythonOperator(
        task_id="update_watermarks",
        python_callable=update_watermarks,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_ensure >> t_prepare >> t_extract >> t_copy >> t_watermark
