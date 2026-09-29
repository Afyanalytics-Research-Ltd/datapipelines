# dags/api_v2_incremental_to_snowflake.py
"""
V2 (Ignite) API → S3 → Snowflake HOSPITALS.{FACILITY}_RAW.EVENTS_RAW  (incremental)

The V2 sibling of api_incremental_to_snowflake.py (V3), and the incremental
replacement for facility_api_to_snowflake.py, which re-reads every model
from 1970 on every run because its watermark Variable is never written.

The incremental logic is the same as the V3 DAG's -- read that file's
docstring for the reasoning behind each rule:

  1. The watermark advances ONLY for units whose COPY committed.
  2. Every window is read with a LOOKBACK overlap.
  3. The watermark is per (facility, namespace), stored in Snowflake
     (HOSPITALS.SHARED.INGESTION_WATERMARK, shared with V3; SOURCE_KEY is
     prefixed with this DAG_ID so the two never collide).

What is different from V3, all of it dictated by the V2 API and by what
already reads {FACILITY}_RAW.EVENTS_RAW:

  * Auth at {host}/api/users/authenticate/user, data at
    {base_url}/api/finance/access/data/point.
  * Namespaces are Ignite\\<Module>\\Entities\\<Model>, and Ignite is
    inconsistent about spelling, so page 1 tries the same four variants
    facility_api_to_snowflake does. A model that 404s under all four is
    NOT_FOUND (absent at that facility), not FAILED -- the model sheet is
    shared across facilities. A facility where EVERY model is NOT_FOUND
    fails the run: that is a moved endpoint or wrong db, not an empty site.
  * One JSON object per JSONL line, not one array: flatten_jsons_schemas
    only reads rows where IS_OBJECT(payload).
  * The module goes in `module_source`, the column the existing table uses.
  * A unit with no watermark is seeded from MAX(ingested_at) already in its
    RAW table. The old DAG always did full loads, so everything before that
    is already landed; seeding from START_FROM would instead spend hundreds
    of 7-day catch-up runs re-reading it.

Retire facility_api_to_snowflake (and _multiple_schemas) once this is on, or
both will write the same RAW tables.

Airflow Connections required (one per facility key, as today):
  afya_api_auth  kakamega  kisumu  lodwar  tenri  xanalife
    host=<base_url>  login=<username>  password=<password>

Airflow Variables required:
  IGNITE_SHEET_ID            Google Sheet key for the model dictionary
  IGNITE_SHEET_WORKSHEET     Worksheet tab name (default: Sheet1)
  GOOGLE_SA_JSON             Google service-account credentials JSON

Env vars (from .env / Docker secrets):
  SNOWFLAKE_USER  SNOWFLAKE_ACCOUNT  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_DATABASE  SNOWFLAKE_PRIVATE_KEY_PATH

Prerequisite:
  sql/incremental_bootstrap.sql  (creates HOSPITALS.SHARED.INGESTION_WATERMARK)
  INGESTION_RUN_LOG.STATUS must accept 'NOT_FOUND' if it is constrained.
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

# Part of every watermark SOURCE_KEY -- renaming it re-seeds every unit.
DAG_ID = "api_v2_incremental_to_snowflake"

# Same facilities, hosts and dbs as facility_api_to_snowflake.py.
FACILITIES: dict[str, dict] = {
    "afya_api_auth": {"base_url": "https://staging.afyanalytics.ai",  "db": "staging_db"},
    "kakamega":      {"base_url": "https://demo.collabmed.net",       "db": "kakamega_db"},
    "kisumu":        {"base_url": "https://kshospital.collabmed.net", "db": "kisumu_db"},
    "lodwar":        {"base_url": "https://lcrh.collabmed.net",       "db": "lodwar_db"},
    "tenri":         {"base_url": "https://stageenv.collabmed.net",   "db": "tenri_db"},
    "xanalife":      {"base_url": "https://xanalife.afyanalytics.ai", "db": "xanalife_db"},
}
# None of the V2 hosts is known to honour an upper time bound. Flip per
# facility ("supports_updated_before": True) once confirmed.

AUTH_PATH = "/api/users/authenticate/user"
DATA_PATH = "/api/finance/access/data/point"

# ── Incremental tuning (same values and reasoning as the V3 DAG) ────────
LOOKBACK_MINUTES  = 15
END_LAG_MINUTES   = 2
MAX_WINDOW_HOURS  = 168          # 7 days

# Only used when a unit has no watermark AND nothing for it has landed in
# EVENTS_RAW yet. V2 history predates V3's 2025 seed.
START_FROM        = "2020-01-01T00:00:00"

PAGE_LIMIT        = 500
MAX_PAGES         = 10_000

S3_CONN_ID       = "aws_default"
S3_BUCKET        = "collabmedbucket"
S3_PREFIX        = "raw/facilities_incremental"

SF_DB            = "HOSPITALS"
SF_SHARED_SCHEMA = "SHARED"
SF_STAGE         = f"{SF_DB}.{SF_SHARED_SCHEMA}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT   = f"{SF_DB}.{SF_SHARED_SCHEMA}.JSON_FF"
SF_WATERMARK     = f"{SF_DB}.{SF_SHARED_SCHEMA}.INGESTION_WATERMARK"
SF_RUN_LOG       = f"{SF_DB}.{SF_SHARED_SCHEMA}.INGESTION_RUN_LOG"

# Column names match what facility_api_to_snowflake already lands, so
# flatten_jsons_schemas reads both sources' rows the same way.
_EVENTS_RAW_DDL = """
    CREATE TABLE IF NOT EXISTS {schema}.EVENTS_RAW (
        facility_id    VARCHAR,
        ingested_at    TIMESTAMP_TZ,
        module_source  VARCHAR,
        source_table   VARCHAR,
        namespace      VARCHAR,
        payload        VARIANT
    )
"""


class NamespaceNotFound(HTTPError):
    """404 from the data endpoint: this namespace spelling does not exist."""


# ── Snowflake client (key-pair auth required for COPY INTO) ─────────────
class SnowflakeClient:
    """Same as the one in api_incremental_to_snowflake.py: key-pair auth
    plus bind params, so sheet-sourced values never go into f-strings."""

    def __init__(self, schema_: str | None = None):
        missing = [
            v for v in ("SNOWFLAKE_USER", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_WAREHOUSE",
                        "SNOWFLAKE_DATABASE", "SNOWFLAKE_PRIVATE_KEY_PATH")
            if not os.getenv(v)
        ]
        if missing:
            raise RuntimeError(f"Missing Snowflake env vars: {', '.join(missing)}")

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

    def execute(self, sql: str, label: str | None = None, params: dict | None = None) -> dict:
        label = label or f"x:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-28s | %.120s…", label, " ".join(sql.split()))
        t0 = time.perf_counter()
        with self._cursor() as cur:
            cur.execute(sql, params or {})
            result = {"rowcount": cur.rowcount, "sfqid": cur.sfqid, "rows": cur.fetchall()}
        log.info("✓ %-28s | rowcount=%s · %.2fs", label, result["rowcount"],
                 time.perf_counter() - t0)
        return result

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


# ── Sheet / namespace helpers ───────────────────────────────────────────
_inflect_engine = None


def _get_inflect():
    """Lazy -- inflect's import is too slow for Airflow's DAG-parse budget."""
    global _inflect_engine
    if _inflect_engine is None:
        import inflect
        _inflect_engine = inflect.engine()
    return _inflect_engine


def _safe_token(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", (s or "").strip()).strip("_").lower()


def _to_singular(word: str) -> str:
    singular = _get_inflect().singular_noun(word)
    return singular if singular else word


def _snake_to_pascal(s: str) -> str:
    return "".join(w.capitalize() for w in re.split(r"[_\s]+", s.strip()) if w)


def _build_namespace(module: str, table: str) -> str:
    """module=Core, table=core_approvals -> Ignite\\Core\\Entities\\Approvals

    Same rule as facility_api_to_snowflake.build_namespace, so the RAW
    `namespace` column is unchanged for existing readers.
    """
    prefix = module.strip().lower() + "_"
    t = table.strip().lower()
    if t.startswith(prefix):
        t = t[len(prefix):]
    return f"Ignite\\{_snake_to_pascal(module)}\\Entities\\{_snake_to_pascal(t)}"


def _namespace_candidates(namespace: str) -> list[str]:
    """Spellings to try, in the same fallback order as facility_api_to_snowflake:
    as-is, singular model, module-prefixed model, module-prefixed singular."""
    parts = namespace.split("\\")
    singular = parts[:-1] + [_to_singular(parts[-1])]
    doubled = parts[:-1] + [parts[1] + parts[-1]]
    doubled_singular = parts[:-1] + [parts[1] + singular[-1]]
    out: list[str] = []
    for p in (parts, singular, doubled, doubled_singular):
        ns = "\\".join(p)
        if ns not in out:
            out.append(ns)
    return out


def _get_gsheet_client():
    return gspread.service_account_from_dict(json.loads(Variable.get("GOOGLE_SA_JSON")))


def _read_sheet(sheet_id: str, worksheet: str) -> list[dict]:
    ws = _get_gsheet_client().open_by_key(sheet_id).worksheet(worksheet)
    return ws.get_all_records()


# ── API helpers ─────────────────────────────────────────────────────────
def _auth_token(connection_id: str) -> str:
    conn = BaseHook.get_connection(connection_id)
    r = requests.post(
        f"{conn.host.rstrip('/')}{AUTH_PATH}",
        json={"username": conn.login, "password": conn.password},
        timeout=60,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Auth failed [{connection_id}]: {r.text[:500]}")
    data = r.json()
    success = data.get("success", {})
    token = success.get("token") if isinstance(success, dict) else data.get("token")
    if not token:
        raise RuntimeError(f"Token missing for {connection_id}")
    return token


def _post_with_retry(url, headers, body, timeout=60, max_retries=6,
                     retry_wait=10, backoff=2, base_delay=0.5):
    """POST with bounded backoff. 429 honours retry_after_seconds.
    404 raises NamespaceNotFound so the caller can try the next spelling."""
    delay = retry_wait
    last_exc: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            time.sleep(base_delay)
            r = requests.post(url, headers=headers, json=body, timeout=timeout)

            if r.status_code == 429:
                payload = {}
                try:
                    payload = r.json()
                except ValueError:
                    pass
                wait = int(payload.get("retry_after_seconds") or delay)
                log.warning("429 from %s -- sleeping %ss (attempt %d/%d)",
                            url, wait, attempt, max_retries)
                time.sleep(wait)
                delay *= backoff
                continue

            if r.status_code == 401:
                # Distinct from "no new data" -- must not be swallowed.
                raise RuntimeError(f"401 Unauthorized from {url}: token rejected")

            if r.status_code == 404:
                raise NamespaceNotFound(
                    f"404 for namespace={body.get('namespace')!r}", response=r)

            if 500 <= r.status_code < 600:
                log.warning("HTTP %s from %s -- backoff %ss (attempt %d/%d)",
                            r.status_code, url, delay, attempt, max_retries)
                time.sleep(delay)
                delay *= backoff
                continue

            r.raise_for_status()
            return r.json()

        except (Timeout, ConnectionError) as exc:
            last_exc = exc
            log.warning("%s on %s -- backoff %ss (attempt %d/%d)",
                        type(exc).__name__, url, delay, attempt, max_retries)
            time.sleep(delay)
            delay *= backoff
        except HTTPError as exc:
            raise exc

    raise RuntimeError(f"Exhausted {max_retries} retries for {url}") from last_exc


def _extract_rows(payload: dict) -> list:
    """V2 puts rows under data, success.data or data.data depending on the
    endpoint. Anything that is not a list means no rows."""
    rows = payload.get("data")
    if rows is None:
        success = payload.get("success")
        rows = success.get("data") if isinstance(success, dict) else None
    if isinstance(rows, dict):
        rows = rows.get("data")
    return rows if isinstance(rows, list) else []


# ── Time / watermark helpers ────────────────────────────────────────────
def _utcnow() -> datetime:
    """UTC-naive now. Everything here is TIMESTAMP_NTZ."""
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def _source_key(facility: str, namespace: str) -> str:
    return f"{DAG_ID}::{facility}::{namespace}"


def _compute_window(watermark: datetime, now: datetime | None = None) -> tuple[datetime, datetime]:
    """watermark -> (start, end): start = watermark - LOOKBACK,
    end = now - END_LAG, clamped to MAX_WINDOW_HOURS per run."""
    now = now or _utcnow()
    start = watermark - timedelta(minutes=LOOKBACK_MINUTES)
    end = now - timedelta(minutes=END_LAG_MINUTES)

    cap = start + timedelta(hours=MAX_WINDOW_HOURS)
    if end > cap:
        log.warning("window > %sh for watermark %s -- clamping end to %s "
                    "(catch-up run; schedule will converge)",
                    MAX_WINDOW_HOURS, watermark.isoformat(), cap.isoformat())
        end = cap

    if end <= start:
        end = start
    return start, end


def _as_naive_utc(ts) -> datetime:
    if not isinstance(ts, datetime):
        ts = datetime.fromisoformat(str(ts))
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    return ts


def _read_watermarks(sf: SnowflakeClient, keys: list[str]) -> dict[str, datetime]:
    """Bulk-read every unit's watermark in one query."""
    if not keys:
        return {}
    placeholders = ", ".join(f"%(k{i})s" for i in range(len(keys)))
    params = {f"k{i}": k for i, k in enumerate(keys)}
    res = sf.execute(
        f"SELECT SOURCE_KEY, WATERMARK_TS FROM {SF_WATERMARK} "
        f"WHERE SOURCE_KEY IN ({placeholders})",
        label="read_watermarks",
        params=params,
    )
    return {row[0]: row[1] for row in res["rows"]}


def _bootstrap_from_raw(sf: SnowflakeClient, facility: str) -> dict[str, datetime]:
    """namespace -> MAX(ingested_at) already landed in this facility's RAW.

    Safe as a seed only because facility_api_to_snowflake always read from
    1970: whatever it landed at time T covers everything updated before T.
    (The V3 DAG deliberately does NOT do this -- its predecessor skipped
    windows, so V3_RAW is not proof of completeness.)
    """
    try:
        res = sf.execute(
            f"SELECT namespace, MAX(ingested_at) FROM {_raw_schema(facility)}.EVENTS_RAW "
            f"GROUP BY namespace",
            label=f"bootstrap:{facility}",
        )
    except Exception as exc:
        log.warning("no RAW bootstrap for %s (%s) -- seeding from START_FROM", facility, exc)
        return {}
    return {ns: _as_naive_utc(ts) for ns, ts in res["rows"] if ns and ts}


def _advance_watermark(sf: SnowflakeClient, *, source_key: str, facility: str,
                       namespace: str, new_ts: datetime, run_id: str, rows: int) -> None:
    """Move one unit's watermark forward. Called ONLY after its COPY committed.
    GREATEST() stops a late-finishing run rewinding a newer watermark."""
    sf.execute(
        f"""
        MERGE INTO {SF_WATERMARK} t
        USING (SELECT %(key)s AS SOURCE_KEY) s
           ON t.SOURCE_KEY = s.SOURCE_KEY
        WHEN MATCHED THEN UPDATE SET
            WATERMARK_TS       = GREATEST(t.WATERMARK_TS, %(ts)s::TIMESTAMP_NTZ),
            LAST_RUN_ID        = %(run_id)s,
            LAST_RUN_AT        = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
            ROWS_LAST_RUN      = %(rows)s,
            CONSECUTIVE_ERRORS = 0,
            LAST_ERROR         = NULL,
            UPDATED_AT         = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
        WHEN NOT MATCHED THEN INSERT
            (SOURCE_KEY, DAG_ID, FACILITY, NAMESPACE, WATERMARK_TS,
             LAST_RUN_ID, LAST_RUN_AT, ROWS_LAST_RUN)
        VALUES
            (%(key)s, %(dag)s, %(facility)s, %(namespace)s, %(ts)s::TIMESTAMP_NTZ,
             %(run_id)s, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, %(rows)s)
        """,
        label=f"wm_advance:{namespace}",
        params={
            "key": source_key, "dag": DAG_ID, "facility": facility,
            "namespace": namespace, "ts": new_ts, "run_id": run_id, "rows": rows,
        },
    )


def _record_error(sf: SnowflakeClient, *, source_key: str, message: str) -> None:
    """Note a failure without touching WATERMARK_TS."""
    sf.execute(
        f"""
        UPDATE {SF_WATERMARK}
           SET CONSECUTIVE_ERRORS = COALESCE(CONSECUTIVE_ERRORS, 0) + 1,
               LAST_ERROR         = %(msg)s,
               UPDATED_AT         = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
         WHERE SOURCE_KEY = %(key)s
        """,
        label="wm_error",
        params={"key": source_key, "msg": (message or "")[:4000]},
    )


def _log_run(sf: SnowflakeClient, unit: dict, **overrides) -> None:
    # unit already carries "status" (and maybe "error"); overrides win.
    kw = {**unit, **overrides}
    sf.execute(
        f"""
        INSERT INTO {SF_RUN_LOG}
            (RUN_ID, SOURCE_KEY, DAG_ID, FACILITY, NAMESPACE, WINDOW_START,
             WINDOW_END, STATUS, ROWS_EXTRACTED, ROWS_COPIED, S3_KEY, ERROR_MESSAGE)
        SELECT %(run_id)s, %(key)s, %(dag)s, %(facility)s, %(namespace)s,
               %(ws)s::TIMESTAMP_NTZ, %(we)s::TIMESTAMP_NTZ, %(status)s,
               %(extracted)s, %(copied)s, %(s3_key)s, %(error)s
        """,
        label="run_log",
        params={
            "run_id": kw.get("run_id"), "key": kw.get("source_key"), "dag": DAG_ID,
            "facility": kw.get("facility"), "namespace": kw.get("namespace"),
            "ws": kw.get("window_start"), "we": kw.get("window_end"),
            "status": kw.get("status"), "extracted": kw.get("rows_extracted", 0),
            "copied": kw.get("rows_copied", 0), "s3_key": kw.get("s3_key"),
            "error": (kw.get("error") or "")[:4000] or None,
        },
    )


def _raw_schema(facility: str) -> str:
    return f"{SF_DB}.{facility.upper()}_RAW"


# ── DAG task callables ──────────────────────────────────────────────────
def ensure_schemas(**context):
    """Create per-facility RAW schema + EVENTS_RAW if missing."""
    with SnowflakeClient() as sf:
        for facility in FACILITIES:
            schema = _raw_schema(facility)
            sf.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}", label=f"schema:{facility}")
            sf.execute(_EVENTS_RAW_DDL.format(schema=schema), label=f"events_raw:{facility}")
    log.info("Ensured RAW schemas for %d facilities", len(FACILITIES))


def prepare_jobs(**context) -> list[dict]:
    """One job per (facility, model), each carrying its OWN window, so the
    watermark advances to the window actually requested -- never to a later
    wall-clock "now"."""
    sheet_id  = Variable.get("IGNITE_SHEET_ID")
    sheet_tab = Variable.get("IGNITE_SHEET_WORKSHEET", default_var="Sheet1")
    rows      = _read_sheet(sheet_id, sheet_tab)
    now       = _utcnow()
    seed      = datetime.fromisoformat(START_FROM)

    units: list[tuple[str, str, str, str]] = []      # facility, module, table, namespace
    for facility in FACILITIES:
        seen: set[tuple] = set()
        for r in rows:
            module = (r.get("module") or "").strip()
            table  = (r.get("table")  or "").strip()
            if not module or not table:
                continue
            key = (module.lower(), table.lower())
            if key in seen:
                continue
            seen.add(key)
            units.append((facility, module, table, _build_namespace(module, table)))

    keys = [_source_key(f, ns) for f, _, _, ns in units]
    with SnowflakeClient() as sf:
        stored = _read_watermarks(sf, keys)
        # Only pay for the RAW scan on facilities that have unseeded units.
        unseeded = {f for f, _, _, ns in units if _source_key(f, ns) not in stored}
        landed = {f: _bootstrap_from_raw(sf, f) for f in unseeded}

    jobs: list[dict] = []
    for facility, module, table, namespace in units:
        skey = _source_key(facility, namespace)
        watermark = stored.get(skey) or landed.get(facility, {}).get(namespace) or seed
        start, end = _compute_window(_as_naive_utc(watermark), now=now)

        if end <= start:
            log.info("skip %s -- empty window", skey)
            continue

        jobs.append({
            "job": {
                "facility":      facility,
                "module":        module,
                "table":         table,
                "namespace":     namespace,
                "source_key":    skey,
                "database":      FACILITIES[facility]["db"],
                "window_start":  start.isoformat(),
                "window_end":    end.isoformat(),
                "updated_since": start.isoformat() + "Z",
                "limit":         PAGE_LIMIT,
            }
        })

    log.info("Prepared %d jobs across %d facilities", len(jobs), len(FACILITIES))
    return jobs


def extract_one_unit(job: dict, **context):
    """Fetch every page for one model over its window and upload to S3.

    Returns a status dict (EXTRACTED / EMPTY / NOT_FOUND / FAILED) instead of
    raising, so one bad model neither aborts the others nor -- being FAILED
    -- lets its watermark move.
    """
    facility   = job["facility"]
    cfg        = FACILITIES[facility]
    ns         = job["namespace"]
    source_key = job["source_key"]
    run_id     = context["run_id"]

    base = {
        "facility": facility, "module": job.get("module"), "table": job.get("table"),
        "namespace": ns, "source_key": source_key,
        "window_start": job["window_start"], "window_end": job["window_end"],
        "run_id": run_id,
    }

    try:
        token   = _auth_token(facility)
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        url     = f"{cfg['base_url'].rstrip('/')}{DATA_PATH}"

        def body_for(namespace: str, page: int) -> dict:
            body = {
                "namespace":     namespace,
                "action":        "get",
                "database":      job["database"],
                "updated_since": job["updated_since"],
                "limit":         job["limit"],
                "page":          page,
            }
            if cfg.get("supports_updated_before"):
                body["updated_before"] = job["window_end"] + "Z"
            return body

        # Page 1 picks the spelling; later pages reuse it.
        payload, resolved_ns = None, None
        for candidate in _namespace_candidates(ns):
            try:
                payload = _post_with_retry(url, headers, body_for(candidate, 1))
                resolved_ns = candidate
                break
            except NamespaceNotFound:
                log.info("%s/%s -- 404 as %r, trying next spelling", facility, ns, candidate)
        if resolved_ns is None:
            log.warning("%s/%s -- no namespace spelling exists at this facility", facility, ns)
            return {**base, "status": "NOT_FOUND", "row_count": 0, "s3_key": None,
                    "ingested_at": _utcnow().isoformat()}
        if resolved_ns != ns:
            log.info("%s/%s -- resolved to %r", facility, ns, resolved_ns)

        all_rows: list[dict] = []
        page = 1
        while page <= MAX_PAGES:
            if page > 1:
                payload = _post_with_retry(url, headers, body_for(resolved_ns, page))
            rows = _extract_rows(payload)
            if not rows:
                break
            all_rows.extend(rows)

            pagination = payload.get("pagination") or {}
            if not pagination.get("has_more_pages"):
                break
            last_page = pagination.get("last_page")
            if last_page is not None and page >= int(last_page):
                break
            page += 1
        else:
            raise RuntimeError(f"{ns}: exceeded MAX_PAGES={MAX_PAGES}; pagination stuck")

        if not all_rows:
            log.info("%s/%s -- window empty", facility, ns)
            return {**base, "status": "EMPTY", "row_count": 0, "s3_key": None,
                    "ingested_at": _utcnow().isoformat()}

        ingested_at = datetime.now(timezone.utc)
        dt = ingested_at.strftime("%Y-%m-%d")
        key = (
            f"{S3_PREFIX}/"
            f"facility_id={facility}/"
            f"module={_safe_token(job.get('module'))}/"
            f"table={_safe_token(job.get('table'))}/"
            f"namespace={_safe_token(ns)}/"
            f"dt={dt}/"
            f"{run_id}.jsonl.gz"
        )

        # One object per line -- the existing V2 RAW contract that
        # flatten_jsons_schemas' IS_OBJECT(payload) depends on.
        text = "".join(json.dumps(r, separators=(",", ":"), default=str) + "\n"
                       for r in all_rows)
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(text.encode("utf-8"))

        S3Hook(aws_conn_id=S3_CONN_ID).load_bytes(
            bytes_data=buf.getvalue(), key=key, bucket_name=S3_BUCKET, replace=True,
        )
        log.info("Uploaded s3://%s/%s rows=%d", S3_BUCKET, key, len(all_rows))

        return {**base, "status": "EXTRACTED", "row_count": len(all_rows),
                "s3_key": key, "ingested_at": ingested_at.isoformat()}

    except Exception as exc:
        log.error("extract failed %s/%s: %s", facility, ns, exc, exc_info=True)
        return {**base, "status": "FAILED", "row_count": 0, "s3_key": None,
                "ingested_at": _utcnow().isoformat(), "error": str(exc)[:2000]}


def copy_and_advance(**unit):
    """COPY one S3 file into EVENTS_RAW, then advance THAT unit's watermark,
    in that order, in one task. ON_ERROR = 'ABORT_STATEMENT' so a bad
    payload fails the unit instead of being skipped while the watermark
    moves past it."""
    facility   = unit["facility"]
    ns         = unit["namespace"]
    source_key = unit["source_key"]
    status     = unit.get("status")
    window_end = datetime.fromisoformat(unit["window_end"])
    run_id     = unit["run_id"]

    with SnowflakeClient(schema_=_raw_schema(facility)) as sf:
        if status == "FAILED":
            # Watermark deliberately untouched -- next run re-reads this window.
            _record_error(sf, source_key=source_key, message=unit.get("error", "extract failed"))
            _log_run(sf, unit, status="FAILED", rows_extracted=0, rows_copied=0,
                     error=unit.get("error"))
            log.warning("holding watermark for %s -- extract failed", source_key)
            return {"source_key": source_key, "status": "FAILED"}

        if status == "NOT_FOUND":
            # Model absent at this facility. Watermark untouched: if it
            # appears later we read it from its seed, not from "now".
            _log_run(sf, unit, status="NOT_FOUND", rows_extracted=0, rows_copied=0)
            return {"source_key": source_key, "status": "NOT_FOUND"}

        if status == "EMPTY":
            # Window was queried successfully, so it advances -- otherwise a
            # quiet model re-reads an ever-widening window.
            _advance_watermark(sf, source_key=source_key, facility=facility,
                               namespace=ns, new_ts=window_end, run_id=run_id, rows=0)
            _log_run(sf, unit, status="EMPTY", rows_extracted=0, rows_copied=0)
            return {"source_key": source_key, "status": "EMPTY"}

        raw_table = f"{_raw_schema(facility)}.EVENTS_RAW"
        s3_key = unit["s3_key"]
        if not re.fullmatch(r"[A-Za-z0-9!_.*'()/=\-]+", s3_key or ""):
            raise ValueError(f"refusing to interpolate suspicious S3 key: {s3_key!r}")

        try:
            res = sf.execute(
                f"""
                COPY INTO {raw_table}
                     (facility_id, ingested_at, module_source, source_table, namespace, payload)
                FROM (
                  SELECT %(facility)s::VARCHAR,
                         %(ingested_at)s::TIMESTAMP_TZ,
                         %(module)s::VARCHAR,
                         %(source_table)s::VARCHAR,
                         %(namespace)s::VARCHAR,
                         PARSE_JSON($1)
                  FROM @{SF_STAGE}
                )
                FILES = ('{s3_key}')
                FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
                ON_ERROR = 'ABORT_STATEMENT'
                """,
                label=f"copy:{facility}:{ns}",
                params={
                    "facility":     facility,
                    "ingested_at":  unit["ingested_at"],
                    "module":       unit.get("module") or "",
                    "source_table": unit.get("table") or "",
                    "namespace":    ns,
                },
            )
            copied = sum(r[3] for r in res["rows"] if len(r) > 3 and isinstance(r[3], int))

            # Only now is it safe to move.
            _advance_watermark(sf, source_key=source_key, facility=facility,
                               namespace=ns, new_ts=window_end, run_id=run_id,
                               rows=unit.get("row_count", 0))
            _log_run(sf, unit, status="COPIED",
                     rows_extracted=unit.get("row_count", 0), rows_copied=copied)
            return {"source_key": source_key, "status": "COPIED"}

        except Exception as exc:
            log.error("copy failed %s/%s: %s", facility, ns, exc, exc_info=True)
            _record_error(sf, source_key=source_key, message=str(exc))
            _log_run(sf, unit, status="FAILED",
                     rows_extracted=unit.get("row_count", 0), rows_copied=0, error=str(exc))
            return {"source_key": source_key, "status": "FAILED"}


def report_failures(**context):
    """Fail the run if any unit FAILED, or if any facility returned
    NOT_FOUND for every model -- after the healthy units have committed.

    Filtered on DAG_ID as well as RUN_ID: the V3 DAG writes to the same run
    log on the same @hourly schedule, so scheduled run_ids collide.
    """
    run_id = context["run_id"]
    params = {"run_id": run_id, "dag": DAG_ID}
    with SnowflakeClient() as sf:
        res = sf.execute(
            f"""
            SELECT SOURCE_KEY, ERROR_MESSAGE
              FROM {SF_RUN_LOG}
             WHERE RUN_ID = %(run_id)s AND DAG_ID = %(dag)s AND STATUS = 'FAILED'
            """,
            label="report_failures",
            params=params,
        )
        missing = sf.execute(
            f"""
            SELECT FACILITY,
                   COUNT_IF(STATUS = 'NOT_FOUND') AS not_found,
                   COUNT(*)                       AS total
              FROM {SF_RUN_LOG}
             WHERE RUN_ID = %(run_id)s AND DAG_ID = %(dag)s
             GROUP BY FACILITY
            """,
            label="report_not_found",
            params=params,
        )
    failed = res["rows"]
    all_missing = [f for f, nf, total in missing["rows"] if total and nf == total]
    for facility, nf, total in missing["rows"]:
        if nf:
            log.warning("%s: %d/%d namespaces not found", facility, nf, total)

    if failed or all_missing:
        for key, err in failed[:20]:
            log.error("FAILED unit %s :: %s", key, (err or "")[:300])
        for facility in all_missing:
            log.error("facility %s: every namespace 404'd -- endpoint or db misconfigured?",
                      facility)
        raise RuntimeError(
            f"{len(failed)} unit(s) failed and {len(all_missing)} facility(ies) returned "
            f"no namespaces this run; their watermarks were NOT advanced and will be "
            f"retried next run."
        )
    log.info("All units succeeded for run %s", run_id)


# ── DAG definition ──────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    max_active_tasks=8,
    # Concurrent runs would read the same watermark and double-extract.
    max_active_runs=1,
    tags=["v2", "api", "snowflake", "ingest", "incremental"],
) as dag:

    t_ensure = PythonOperator(
        task_id="ensure_schemas",
        python_callable=ensure_schemas,
    )
    t_prepare = PythonOperator(
        task_id="prepare_jobs",
        python_callable=prepare_jobs,
    )
    t_extract = PythonOperator.partial(
        task_id="extract_to_s3",
        python_callable=extract_one_unit,
    ).expand(op_kwargs=t_prepare.output)

    t_copy = PythonOperator.partial(
        task_id="copy_and_advance_watermark",
        python_callable=copy_and_advance,
    ).expand(op_kwargs=t_extract.output)

    t_report = PythonOperator(
        task_id="report_failures",
        python_callable=report_failures,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_ensure >> t_prepare >> t_extract >> t_copy >> t_report
