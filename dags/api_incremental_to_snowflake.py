# dags/api_incremental_to_snowflake.py
"""
Gateway API → S3 → Snowflake HOSPITALS.{FACILITY}_V3_RAW.EVENTS_RAW  (incremental)

A correctness-focused replacement for the watermark half of
v3_api_to_snowflake_raw.py. Same house plumbing -- gzip JSONL to
s3://collabmedbucket, COPY INTO from HOSPITALS.SHARED.FACILITY_RAW_STAGE,
key-pair SnowflakeClient -- but the incremental logic is rebuilt around
three fixes. See MIGRATION_NOTES.md for the full reasoning.

  1. The watermark advances ONLY for units that actually succeeded.
     The current DAG runs update_watermarks with TriggerRule.ALL_DONE and
     stamps wall-clock now for every facility, so a run where every extract
     task failed still moves the watermark forward. Those windows are then
     never re-read. That is silent, permanent data loss.

  2. Every window is read with a LOOKBACK overlap.
     `updated_since = last_run` with no overlap loses any row whose
     transaction committed after we queried but carries an earlier
     updated_at. The overlap re-reads those; the MERGE downstream dedupes.

  3. The watermark is per (facility, namespace), stored in Snowflake.
     One key per facility means a failed model poisons every other model for
     that facility. And an Airflow Variable does not survive a metadata DB
     rebuild -- the warehouse does.

Airflow Connections required (one per facility key, as today):
  afya_api_auth  kakamega  kisumu  lodwar  tenri  xanalife  collabmed
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

DAG_ID = "api_incremental_to_snowflake"

FACILITIES: dict[str, dict] = {
    "collabmed": {
        "base_url":  "https://afyapi.afyaanalytics.ai/api/",
        "db":        "collabmed",
        "tenant_id": "collabmed",
        # Set True only if the gateway honours an upper time bound. When False
        # we still compute a window end and advance the watermark to it; we
        # just cannot ask the API to respect it, so the end-lag below is what
        # keeps us off the source's write edge.
        "supports_updated_before": False,
    },
}

# ── Incremental tuning ──────────────────────────────────────────────────
# LOOKBACK_MINUTES is the single most important knob here.
#
# A source's updated_at is not a monotonic clock from our point of view: a
# transaction can commit at 10:05 carrying updated_at=10:02, the source's
# clock can drift behind ours, and walking 40 pages takes real time. Reading
# from exactly `last_run` loses every row that lands in those gaps, with no
# error and no way to notice.
#
# The overlap re-reads some rows. That costs a few seconds of API time and is
# cleaned up by the QUALIFY ROW_NUMBER() dedup that v3_raw_to_v3_ready already
# does. Losing a row is unrecoverable; re-reading one is free. When unsure,
# raise this.
LOOKBACK_MINUTES  = 15

# Stay this far back from "now" so we never read a window the source is still
# writing into.
END_LAG_MINUTES   = 2

# Cap on how much history one run will claim. Without this, a DAG that has
# been paused for three weeks tries to pull three weeks in one request and
# times out forever. With it, the catch-up happens over several runs.
MAX_WINDOW_HOURS  = 168          # 7 days

# Watermark seed for a unit that has never run.
START_FROM        = "2025-01-01T00:00:00"

PAGE_LIMIT        = 500
MAX_PAGES         = 10_000

S3_CONN_ID       = "aws_default"
S3_BUCKET        = "collabmedbucket"
S3_PREFIX        = "raw/v3_facilities_incremental"

SF_DB            = "HOSPITALS"
SF_SHARED_SCHEMA = "SHARED"
SF_STAGE         = f"{SF_DB}.{SF_SHARED_SCHEMA}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT   = f"{SF_DB}.{SF_SHARED_SCHEMA}.JSON_FF"
SF_WATERMARK     = f"{SF_DB}.{SF_SHARED_SCHEMA}.INGESTION_WATERMARK"
SF_RUN_LOG       = f"{SF_DB}.{SF_SHARED_SCHEMA}.INGESTION_RUN_LOG"

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
    """Same shape as the one in v3_api_to_snowflake_raw.py, plus bind params.

    The `params` argument is the one addition: the existing COPY INTO builds
    its literals with f-strings, which breaks the moment a facility name or
    namespace contains an apostrophe and makes the statement unsafe to build
    from sheet-sourced values. Binds remove that whole class of problem.
    """

    def __init__(self, schema_: str | None = None):
        missing = [
            v for v in ("SNOWFLAKE_USER", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_WAREHOUSE",
                        "SNOWFLAKE_DATABASE", "SNOWFLAKE_PRIVATE_KEY_PATH")
            if not os.getenv(v)
        ]
        if missing:
            # Beats the AttributeError on None.strip() that the current client
            # raises -- that one sends you looking in the wrong place.
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


# ── Google Sheet helpers ────────────────────────────────────────────────
_inflect_engine = None


def _get_inflect():
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


def _build_namespace(module: str, table: str) -> str:
    return f"{_safe_token(module)}.{_safe_token(table)}"


def _get_gsheet_client():
    return gspread.service_account_from_dict(json.loads(Variable.get("GOOGLE_SA_JSON")))


def _read_sheet(sheet_id: str, worksheet: str) -> list[dict]:
    ws = _get_gsheet_client().open_by_key(sheet_id).worksheet(worksheet)
    return ws.get_all_records()


# ── API helpers ─────────────────────────────────────────────────────────
def _auth_token(connection_id: str) -> str:
    conn = BaseHook.get_connection(connection_id)
    r = requests.post(
        f"{conn.host.rstrip('/')}/auth/login",
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
    """POST with bounded backoff. 429 honours retry_after_seconds."""
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


# ── Time / watermark helpers ────────────────────────────────────────────
def _utcnow() -> datetime:
    """UTC-naive now. Everything here is TIMESTAMP_NTZ; mixing naive and
    aware datetimes is the classic way to lose or duplicate three hours."""
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def _source_key(facility: str, namespace: str) -> str:
    return f"{DAG_ID}::{facility}::{namespace}"


def _compute_window(watermark: datetime, now: datetime | None = None) -> tuple[datetime, datetime]:
    """watermark -> (start, end) for this run.

    start = watermark - LOOKBACK   (the overlap; see the note at the top)
    end   = now - END_LAG          (stay off the source's write edge)

    Clamped to MAX_WINDOW_HOURS so a long outage becomes several catch-up
    runs instead of one request that can never finish.
    """
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


def _advance_watermark(sf: SnowflakeClient, *, source_key: str, facility: str,
                       namespace: str, new_ts: datetime, run_id: str, rows: int) -> None:
    """Move one unit's watermark forward. Called ONLY after its COPY committed.

    GREATEST() guards against a late-finishing run rewinding a watermark that
    a newer run already advanced -- the kind of thing that only bites you once
    you enable concurrency, and then bites hard.
    """
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


def _log_run(sf: SnowflakeClient, **kw) -> None:
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


def _v3_raw_schema(facility: str) -> str:
    return f"{SF_DB}.{facility.upper()}_V3_RAW"


# ── DAG task callables ──────────────────────────────────────────────────
def ensure_schemas(**context):
    """Create per-facility V3_RAW schema + EVENTS_RAW for every facility."""
    with SnowflakeClient() as sf:
        for facility in FACILITIES:
            schema = _v3_raw_schema(facility)
            sf.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}", label=f"schema:{facility}")
            sf.execute(_EVENTS_RAW_DDL.format(schema=schema), label=f"events_raw:{facility}")
    log.info("Ensured V3_RAW schemas for %d facilities", len(FACILITIES))


def prepare_jobs(**context) -> list[dict]:
    """One job per (facility, model), each carrying its OWN window.

    The window is computed here and carried through to the copy task, rather
    than recomputed later. That matters: the watermark must advance to the
    window we actually asked the API for, never to a later wall-clock "now".
    Advancing to "now" is how the current DAG skips data.
    """
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

    jobs: list[dict] = []
    for facility, module, table, namespace in units:
        skey = _source_key(facility, namespace)
        watermark = stored.get(skey) or seed
        if not isinstance(watermark, datetime):
            watermark = datetime.fromisoformat(str(watermark))
        start, end = _compute_window(watermark, now=now)

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

    Returns a status dict instead of raising on a per-unit API failure. One
    bad model must not abort the other 60, and -- crucially -- a failed unit
    must still flow downstream carrying status=FAILED so the copy task knows
    NOT to advance its watermark. The final report task turns any FAILED into
    a red DAG run.
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
        url     = f"{cfg['base_url'].rstrip('/')}/gateway"

        all_rows: list[dict] = []
        page = 1
        while page <= MAX_PAGES:
            body = {
                "namespace":     ns,
                "action":        "get",
                "database":      job["database"],
                "updated_since": job["updated_since"],
                "limit":         job["limit"],
                "page":          page,
            }
            if cfg.get("supports_updated_before"):
                body["updated_before"] = job["window_end"] + "Z"

            payload = _post_with_retry(url, headers, body)
            rows = payload.get("data") or (payload.get("success") or {}).get("data") or []
            if not rows:
                break
            all_rows.extend(rows)

            pagination = payload.get("pagination") or {}
            if not pagination.get("has_more_pages"):
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

        # One JSON array on one JSONL line, matching the existing RAW contract
        # so v3_raw_to_v3_ready's LATERAL FLATTEN keeps working unchanged.
        line = json.dumps(all_rows, separators=(",", ":"), default=str) + "\n"
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(line.encode("utf-8"))

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
    """COPY one S3 file into EVENTS_RAW, then advance THAT unit's watermark.

    The advance lives in the same task as the COPY, immediately after it
    commits. That ordering is the whole point: there is no path where the
    watermark moves without the data having landed, and no path where one
    unit's failure moves another unit's watermark.

    ON_ERROR = 'ABORT_STATEMENT' rather than 'CONTINUE'. With CONTINUE, a
    malformed payload is skipped, the COPY reports success, the watermark
    advances, and the row is gone for good. Better to fail the unit and
    re-read the window next run.
    """
    facility   = unit["facility"]
    ns         = unit["namespace"]
    source_key = unit["source_key"]
    status     = unit.get("status")
    window_end = datetime.fromisoformat(unit["window_end"])
    run_id     = unit["run_id"]

    with SnowflakeClient(schema_=_v3_raw_schema(facility)) as sf:
        if status == "FAILED":
            # Watermark deliberately untouched -- next run re-reads this window.
            _record_error(sf, source_key=source_key, message=unit.get("error", "extract failed"))
            _log_run(sf, **unit, status="FAILED", rows_extracted=0, rows_copied=0,
                     error=unit.get("error"))
            log.warning("holding watermark for %s -- extract failed", source_key)
            return {"source_key": source_key, "status": "FAILED"}

        if status == "EMPTY":
            # Nothing to load, but the window WAS successfully queried, so the
            # watermark advances. Otherwise a quiet source re-reads the same
            # widening window forever.
            _advance_watermark(sf, source_key=source_key, facility=facility,
                               namespace=ns, new_ts=window_end, run_id=run_id, rows=0)
            _log_run(sf, **unit, status="EMPTY", rows_extracted=0, rows_copied=0)
            return {"source_key": source_key, "status": "EMPTY"}

        raw_table = f"{_v3_raw_schema(facility)}.EVENTS_RAW"
        s3_key = unit["s3_key"]
        if not re.fullmatch(r"[A-Za-z0-9!_.*'()/=\-]+", s3_key or ""):
            raise ValueError(f"refusing to interpolate suspicious S3 key: {s3_key!r}")

        try:
            res = sf.execute(
                f"""
                COPY INTO {raw_table}
                     (facility_id, source_table, module, namespace, ingested_at, payload)
                FROM (
                  SELECT %(facility)s::VARCHAR,
                         %(source_table)s::VARCHAR,
                         %(module)s::VARCHAR,
                         %(namespace)s::VARCHAR,
                         %(ingested_at)s::TIMESTAMP_TZ,
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
                    "source_table": unit.get("table") or "",
                    "module":       unit.get("module") or "",
                    "namespace":    ns,
                    "ingested_at":  unit["ingested_at"],
                },
            )
            copied = sum(r[3] for r in res["rows"] if len(r) > 3 and isinstance(r[3], int))

            # Only now is it safe to move.
            _advance_watermark(sf, source_key=source_key, facility=facility,
                               namespace=ns, new_ts=window_end, run_id=run_id,
                               rows=unit.get("row_count", 0))
            _log_run(sf, **unit, status="COPIED",
                     rows_extracted=unit.get("row_count", 0), rows_copied=copied)
            return {"source_key": source_key, "status": "COPIED"}

        except Exception as exc:
            log.error("copy failed %s/%s: %s", facility, ns, exc, exc_info=True)
            _record_error(sf, source_key=source_key, message=str(exc))
            _log_run(sf, **unit, status="FAILED",
                     rows_extracted=unit.get("row_count", 0), rows_copied=0, error=str(exc))
            return {"source_key": source_key, "status": "FAILED"}


def report_failures(**context):
    """Fail the DAG run if any unit failed, AFTER the healthy units committed.

    This is what lets a single broken model turn the run red without also
    blocking or corrupting the other sixty. The repo has no alerting at all
    today, so a red run plus V_INGESTION_FRESHNESS is the signal.
    """
    run_id = context["run_id"]
    with SnowflakeClient() as sf:
        res = sf.execute(
            f"""
            SELECT SOURCE_KEY, ERROR_MESSAGE
              FROM {SF_RUN_LOG}
             WHERE RUN_ID = %(run_id)s AND STATUS = 'FAILED'
            """,
            label="report_failures",
            params={"run_id": run_id},
        )
    failed = res["rows"]
    if failed:
        for key, err in failed[:20]:
            log.error("FAILED unit %s :: %s", key, (err or "")[:300])
        raise RuntimeError(
            f"{len(failed)} unit(s) failed this run; their watermarks were NOT "
            f"advanced and will be retried next run."
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
    # Concurrent runs would read the same watermark and double-extract. The
    # GREATEST() guard keeps that from corrupting state, but there is no point
    # paying for it.
    max_active_runs=1,
    tags=["v3", "api", "snowflake", "ingest", "incremental"],
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