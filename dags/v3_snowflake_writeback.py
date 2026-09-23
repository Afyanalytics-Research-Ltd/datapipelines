# dags/v3_snowflake_writeback.py
"""
v3_snowflake_writeback.py — Snowflake HOSPITALS.V3_READY.<SERVICE> → V3 Service APIs
(reverse of v3_api_to_snowflake_raw.py / v3_raw_to_v3_ready.py)

Pushes corrected or newly staged records in HOSPITALS.V3_READY.<SERVICE> back into
the V3 gateway APIs. Analysts (or another Snowflake job) edit a row's `payload` in
V3_READY and flag it `_dirty = TRUE`; this DAG finds every dirty row, POSTs it to
the originating V3 service's gateway, and clears the flag on success.

STAGING CONTRACT (columns this DAG adds to every V3_READY.<SERVICE> table, same
contract used by the original standalone v3_snowflake_writeback.py script)
  _dirty             BOOLEAN       Set TRUE to queue the row for writeback.
  _writeback_action  VARCHAR       'update' (default) or 'insert'.
                                      'update' → the row's _record_id already exists in V3;
                                                 PATCH/update that record.
                                      'insert' → brand-new record; _record_id may hold a
                                                 caller-assigned placeholder (any unique string),
                                                 which is overwritten with the real V3 id — in
                                                 both _record_id and payload:id — once the
                                                 insert succeeds.
  _writeback_at      TIMESTAMP_TZ  Set on successful writeback.
  _writeback_run_id  VARCHAR       run_id that last touched the row.
  _writeback_error   VARCHAR       Last error message; cleared on success.

Permanent failures (400/404/409/422/500 — validation or unsupported request) clear
`_dirty` but leave `_writeback_error` populated: fix the data and re-flag `_dirty =
TRUE` to retry. Transient failures (network error, exhausted 429/502/503/504
retries) leave `_dirty = TRUE` so the next run retries automatically. The flag is
only ever cleared *after* a confirmed API response for that row (see
clear_dirty_flag / _apply_result below) — never speculatively.

Task shape:
  ensure_writeback_schema >> find_dirty_rows >> writeback_to_api (expand)
    >> clear_dirty_flag (expand) >> record_writeback_run

Airflow Variables (all optional — defaults mirror the standalone script's .env):
  V3_WRITEBACK_SERVICES         Comma list to restrict services (default: all of
                                 core, finance, evaluation, reception, inventory,
                                 theatre, inpatient).
  V3_WRITEBACK_MODELS           Comma list of model aliases to restrict to.
  V3_WRITEBACK_FACILITY_ID      Int, default "6" (matches AFYA_FACILITY_ID).
  V3_WRITEBACK_ORGANIZATION_ID  Int, default "1" (matches AFYA_ORGANIZATION_ID).
  V3_WRITEBACK_BATCH_SIZE       Dirty rows fetched from Snowflake per round (default 200).
  V3_WRITEBACK_UPDATE_ACTION    Gateway "update" verb override (default "update").

Airflow Connections required:
  afya_api_auth   login=<username>  password=<password>
                  (same connection used by v3_api_to_snowflake_raw.py for the
                  "afya" facility; POSTed to <core service>/v1/login.)

Env vars (Snowflake key-pair auth, same as v3_api_to_snowflake_raw.py's SnowflakeClient):
  SNOWFLAKE_USER  SNOWFLAKE_ACCOUNT  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_DATABASE (default HOSPITALS)  SNOWFLAKE_PRIVATE_KEY_PATH (or SNOWFLAKE_PASSWORD)

NOTE — the gateway's update verb has not been confirmed against a live call; only
action="insert" is proven. Verify with a single record before relying on this
broadly, and set V3_WRITEBACK_UPDATE_ACTION if your gateway uses a different verb.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any

import requests
import requests.adapters
import snowflake.connector
from requests.exceptions import ConnectionError, HTTPError, Timeout

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

DAG_ID = "v3_snowflake_writeback"

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

AFYA_AUTH_CONN_ID = "afya_api_auth"
TOKEN_TTL         = 50 * 60  # 50-minute bearer token TTL
WRITE_INSERT_ACTION = "insert"

SF_DB    = os.getenv("SNOWFLAKE_DATABASE", "HOSPITALS").upper()
SF_READY = "V3_READY"
SF_AUDIT = "MIGRATION_AUDIT"


def _sq(s: str) -> str:
    """Escape a string for embedding in a single-quoted SQL literal."""
    return (s if s is not None else "").replace("'", "''")


def _active_services() -> list[str]:
    override = Variable.get("V3_WRITEBACK_SERVICES", default_var="")
    if override.strip():
        services = [s.strip() for s in override.split(",") if s.strip()]
        unknown = [s for s in services if s not in V3_SERVICES]
        if unknown:
            raise ValueError(f"Unknown V3_WRITEBACK_SERVICES entries: {unknown}. Valid: {list(V3_SERVICES)}")
        return services
    return list(V3_SERVICES.keys())


# ─── SNOWFLAKE (mirrors v3_api_to_snowflake_raw.py's SnowflakeClient) ─────────

class SnowflakeClient:
    def __init__(self, schema_: str | None = None):
        pk_path = (os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or "").strip()
        pw      = (os.getenv("SNOWFLAKE_PASSWORD") or "").strip()
        kwargs: dict[str, Any] = dict(
            user      = os.getenv("SNOWFLAKE_USER", "").strip(),
            account   = os.getenv("SNOWFLAKE_ACCOUNT", "").strip(),
            warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "").strip(),
            database  = SF_DB,
            schema    = schema_ or SF_AUDIT,
        )
        if pk_path:
            kwargs["private_key_file"] = pk_path
        elif pw:
            kwargs["password"] = pw
        else:
            raise RuntimeError("Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")
        self._conn = snowflake.connector.connect(**kwargs)

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

    def query(self, sql: str, label: str | None = None) -> list[dict]:
        label = label or f"q:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-28s | %.120s…", label, " ".join(sql.split()))
        t0 = time.perf_counter()
        with self._cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        log.info("✓ %-28s | rows=%d · %.2fs", label, len(rows), time.perf_counter() - t0)
        return rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


# ─── HTTP SESSION + TOKEN (per facility_id) ───────────────────────────────────

_http: requests.Session | None = None
_http_lock  = threading.Lock()
_token_cache: dict[int, tuple[str, float]] = {}
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


def _get_token(facility_id: int) -> str:
    with _token_lock:
        cached = _token_cache.get(facility_id)
        if cached and time.time() - cached[1] < TOKEN_TTL:
            return cached[0]
        conn = BaseHook.get_connection(AFYA_AUTH_CONN_ID)
        url  = f"{V3_SERVICES['core'].rstrip('/')}/v1/login"
        r    = _session().post(
            url,
            json={"username": conn.login, "password": conn.password, "facility_id": facility_id},
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=30,
        )
        if not r.ok:
            raise RuntimeError(f"V3 login failed: {r.status_code} · {r.text[:200]}")
        token = r.json().get("access_token")
        if not token:
            raise RuntimeError(f"No access_token in login response: {r.text[:200]}")
        _token_cache[facility_id] = (token, time.time())
        log.info("Authenticated to V3 API (facility_id=%s)", facility_id)
        return token


def _invalidate_token(facility_id: int) -> None:
    with _token_lock:
        _token_cache.pop(facility_id, None)


def _headers(facility_id: int) -> dict:
    return {
        "Authorization": f"Bearer {_get_token(facility_id)}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }


def discover_gateway_meta(service: str, facility_id: int) -> dict[str, dict]:
    """POST <service>/v1/gateway action=list → {alias: {operations, tenant, facility}}."""
    url = f"{V3_SERVICES[service].rstrip('/')}/v1/gateway"
    try:
        r = _session().post(url, headers=_headers(facility_id), json={"action": "list"}, timeout=30)
        if not r.ok:
            log.warning("Gateway list [%s] %s: %s", service, r.status_code, r.text[:200])
            return {}
        entries = r.json().get("data") or []
        meta = {
            e["alias"]: {
                "operations": e.get("operations", []),
                "tenant":     e.get("tenant"),
                "facility":   e.get("facility"),
            }
            for e in entries if e.get("alias")
        }
        log.info("Service %-12s gateway metadata for %d models", service, len(meta))
        return meta
    except Exception as exc:
        log.warning("Cannot reach gateway [%s]: %s", service, exc)
        return {}


# ─── SNOWFLAKE DDL ────────────────────────────────────────────────────────────

_WRITEBACK_RUNS_DDL = f"""
    CREATE TABLE IF NOT EXISTS {SF_DB}.{SF_AUDIT}.WRITEBACK_RUNS (
        run_id         VARCHAR        NOT NULL,
        started_at     TIMESTAMP_TZ   NOT NULL,
        finished_at    TIMESTAMP_TZ,
        status         VARCHAR,
        total_jobs     INTEGER,
        succeeded_jobs INTEGER,
        failed_jobs    INTEGER,
        total_rows     INTEGER
    )"""

_WRITEBACK_RESULTS_DDL = f"""
    CREATE TABLE IF NOT EXISTS {SF_DB}.{SF_AUDIT}.WRITEBACK_RESULTS (
        run_id        VARCHAR        NOT NULL,
        service       VARCHAR        NOT NULL,
        model         VARCHAR        NOT NULL,
        record_id     VARCHAR        NOT NULL,
        action        VARCHAR        NOT NULL,
        status        VARCHAR        NOT NULL,
        new_record_id VARCHAR,
        error_message VARCHAR,
        attempted_at  TIMESTAMP_TZ   NOT NULL
    )"""

_WRITEBACK_COLUMNS: list[tuple[str, str]] = [
    ("_dirty",             "BOOLEAN DEFAULT FALSE"),
    ("_writeback_action",  "VARCHAR DEFAULT 'update'"),
    ("_writeback_at",      "TIMESTAMP_TZ"),
    ("_writeback_run_id",  "VARCHAR"),
    ("_writeback_error",   "VARCHAR"),
]


def _fetch_dirty_batch(sf: SnowflakeClient, service: str, model: str, limit: int) -> list[dict]:
    t = f"{SF_DB}.{SF_READY}.{service.upper()}"
    sql = f"""
    SELECT _record_id, _writeback_action, payload
    FROM {t}
    WHERE _model = '{_sq(model)}' AND _dirty = TRUE
    LIMIT {int(limit)}
    """
    return sf.query(sql, label=f"fetch:{service}.{model}")


def _apply_result(
    sf: SnowflakeClient, service: str, model: str, record_id: str,
    status: str, new_id: str | None, error: str | None, run_id: str,
) -> None:
    """Clear `_dirty` ONLY after a confirmed API result for this row.

    ok               → clear _dirty, stamp success, adopt the (possibly new) record id.
    failed_permanent → clear _dirty (validation/unsupported request — re-flag to retry
                        after fixing the data), keep the error message.
    failed_transient → leave _dirty = TRUE so the next scheduled run retries automatically.
    """
    t = f"{SF_DB}.{SF_READY}.{service.upper()}"
    if status == "ok":
        sql = f"""
        UPDATE {t}
        SET _dirty            = FALSE,
            _writeback_action = 'update',
            _writeback_at     = CURRENT_TIMESTAMP(),
            _writeback_run_id = '{_sq(run_id)}',
            _writeback_error  = NULL,
            _record_id        = '{_sq(new_id)}',
            payload           = OBJECT_INSERT(
                                     payload, 'id',
                                     COALESCE(TO_VARIANT(TRY_CAST('{_sq(new_id)}' AS NUMBER)),
                                              TO_VARIANT('{_sq(new_id)}')),
                                     TRUE)
        WHERE _model = '{_sq(model)}' AND _record_id = '{_sq(record_id)}'
        """
    else:
        clear_dirty = "FALSE" if status == "failed_permanent" else "TRUE"
        sql = f"""
        UPDATE {t}
        SET _dirty            = {clear_dirty},
            _writeback_run_id = '{_sq(run_id)}',
            _writeback_error  = '{_sq((error or '')[:2000])}'
        WHERE _model = '{_sq(model)}' AND _record_id = '{_sq(record_id)}'
        """
    sf.execute(sql, label=f"apply:{status}:{service}.{model}")


def _write_audit_results(sf: SnowflakeClient, results: list[dict]) -> None:
    if not results:
        return
    rows_sql = []
    for r in results:
        rows_sql.append("(" + ", ".join([
            f"'{_sq(r['run_id'])}'",
            f"'{_sq(r['service'])}'",
            f"'{_sq(r['model'])}'",
            f"'{_sq(r['record_id'])}'",
            f"'{_sq(r['action'])}'",
            f"'{_sq(r['status'])}'",
            (f"'{_sq(r['new_id'])}'" if r.get("new_id") else "NULL"),
            (f"'{_sq((r.get('error') or '')[:2000])}'" if r.get("error") else "NULL"),
            "CURRENT_TIMESTAMP()",
        ]) + ")")
    sql = f"""
    INSERT INTO {SF_DB}.{SF_AUDIT}.WRITEBACK_RESULTS
        (run_id, service, model, record_id, action, status, new_record_id, error_message, attempted_at)
    VALUES {", ".join(rows_sql)}
    """
    sf.execute(sql, label="audit:writeback_results")


# ─── V3 WRITE (with retry) ─────────────────────────────────────────────────────

def _write_row(
    service: str,
    model: str,
    action: str,
    record_id: str,
    payload: dict,
    gw_meta: dict,
    facility_id: int,
    organization_id: int | None,
    update_action: str,
    *,
    max_retries: int = 5,
    init_wait:   int = 5,
) -> tuple[str, str | None, str | None]:
    """POST/UPDATE one record via the V3 gateway.

    Returns (status, new_record_id, error) where status is one of:
      "ok"                — succeeded
      "failed_permanent"  — validation/unsupported request; do not auto-retry
      "failed_transient"  — network/5xx exhausted retries; safe to retry later
    """
    url = f"{V3_SERVICES[service].rstrip('/')}/v1/gateway"

    data = dict(payload)
    if action == WRITE_INSERT_ACTION:
        data.pop("id", None)
        body = {"action": WRITE_INSERT_ACTION, "model": model, "data": data}
    else:
        data["id"] = record_id
        body = {"action": update_action, "model": model, "id": record_id, "data": data}
    if organization_id is not None:
        body["destination_tenant_id"] = organization_id

    headers = _headers(facility_id)
    if organization_id is not None:
        headers["X-Tenant-Id"] = str(organization_id)
    if gw_meta.get("facility") and facility_id is not None:
        headers["X-Facility-Id"] = str(facility_id)

    attempt, wait = 0, init_wait
    while True:
        attempt += 1
        try:
            r = _session().post(url, headers=headers, json=body, timeout=90)
            log.info("  V3 %-6s svc=%-12s model=%-30s id=%-10s status=%d",
                      action, service, model, record_id, r.status_code)

            if r.status_code == 401:
                _invalidate_token(facility_id)
                headers["Authorization"] = f"Bearer {_get_token(facility_id)}"
                if attempt >= max_retries:
                    return "failed_transient", None, "401 — token refresh exhausted"
                continue

            if r.status_code == 429:
                pause = init_wait
                try:
                    pause = int(r.json().get("retry_after_seconds", init_wait))
                except Exception:
                    pass
                if attempt >= max_retries:
                    return "failed_transient", None, "429 — retries exhausted"
                time.sleep(pause)
                continue

            if r.status_code in {502, 503, 504}:
                if attempt >= max_retries:
                    return "failed_transient", None, f"{r.status_code} — retries exhausted"
                time.sleep(wait)
                wait = min(wait * 2, 60)
                continue

            if r.status_code in {400, 404, 409, 422, 500}:
                try:
                    err_body = r.json()
                except Exception:
                    err_body = {"raw": r.text[:500]}
                return "failed_permanent", None, json.dumps(err_body)[:2000]

            if not r.ok:
                return "failed_permanent", None, f"HTTP {r.status_code}: {r.text[:500]}"

            resp = r.json() if r.content else {}
            new_id = (
                resp.get("id")
                or (resp.get("data") or {}).get("id")
                or (resp.get("success") or {}).get("id")
                or record_id
            )
            return "ok", str(new_id), None

        except (Timeout, ConnectionError) as exc:
            if attempt >= max_retries:
                return "failed_transient", None, f"network error: {exc}"
            time.sleep(wait)
            wait = min(wait * 2, 60)
        except HTTPError as exc:
            return "failed_permanent", None, str(exc)


# ─── DAG TASK CALLABLES ─────────────────────────────────────────────────────

def ensure_writeback_schema(**context) -> None:
    """Add the _dirty / _writeback_* staging columns and audit tables if missing."""
    services = _active_services()
    with SnowflakeClient() as sf:
        for svc in services:
            t = f"{SF_DB}.{SF_READY}.{svc.upper()}"
            for col, decl in _WRITEBACK_COLUMNS:
                try:
                    sf.execute(f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {col} {decl}",
                               label=f"ddl:{svc}:{col}")
                except Exception as exc:
                    log.warning("Cannot add %s to %s (has the read pipeline created it yet?): %s",
                                col, t, exc)
                    break
        try:
            sf.execute(_WRITEBACK_RUNS_DDL, label="ddl:writeback_runs")
            sf.execute(_WRITEBACK_RESULTS_DDL, label="ddl:writeback_results")
        except Exception as exc:
            log.error("Cannot create writeback audit tables: %s", exc)
            raise


def find_dirty_rows(**context) -> list[dict]:
    """Per active service: SELECT _model, COUNT(*) FROM V3_READY.<service> WHERE _dirty GROUP BY _model."""
    services = _active_services()
    only_models_raw = Variable.get("V3_WRITEBACK_MODELS", default_var="")
    only_models = {m.strip() for m in only_models_raw.split(",") if m.strip()} or None

    jobs: list[dict] = []
    with SnowflakeClient() as sf:
        for svc in services:
            t = f"{SF_DB}.{SF_READY}.{svc.upper()}"
            sql = f"SELECT _model, COUNT(*) AS cnt FROM {t} WHERE _dirty = TRUE GROUP BY _model"
            try:
                rows = sf.query(sql, label=f"find:{svc}")
            except Exception as exc:
                log.warning("Cannot query %s (table missing?): %s", t, exc)
                continue
            for row in rows:
                model = row["_MODEL"]
                if only_models and model not in only_models:
                    continue
                jobs.append({"service": svc, "model": model, "count": int(row["CNT"])})

    if not jobs:
        log.info("No dirty rows found across %d service(s) — nothing to write back.", len(services))
    else:
        log.info("Jobs to run: %d  (%s)", len(jobs),
                  ", ".join(f"{j['service']}.{j['model']}={j['count']}" for j in jobs))
    return jobs


def writeback_to_api(job: dict, **context) -> dict:
    """For one (service, model) job: fetch dirty rows in batches and POST each to the V3 gateway.

    Does NOT touch Snowflake beyond reading — the _dirty flag is only cleared by the
    downstream clear_dirty_flag task, once each row's API result is known.
    """
    service = job["service"]
    model   = job["model"]

    facility_id      = int(Variable.get("V3_WRITEBACK_FACILITY_ID", default_var="6"))
    org_id_raw        = Variable.get("V3_WRITEBACK_ORGANIZATION_ID", default_var="1").strip()
    organization_id   = int(org_id_raw) if org_id_raw else None
    batch_size        = int(Variable.get("V3_WRITEBACK_BATCH_SIZE", default_var="200"))
    update_action     = Variable.get("V3_WRITEBACK_UPDATE_ACTION", default_var="update")
    run_id            = context["run_id"]

    gw_meta = discover_gateway_meta(service, facility_id).get(model, {})

    results: list[dict] = []
    with SnowflakeClient() as sf:
        while True:
            rows = _fetch_dirty_batch(sf, service, model, batch_size)
            if not rows:
                break
            for row in rows:
                record_id = row["_RECORD_ID"]
                action    = (row["_WRITEBACK_ACTION"] or "update").lower()
                raw       = row["PAYLOAD"]
                payload   = json.loads(raw) if isinstance(raw, str) else raw
                status, new_id, err = _write_row(
                    service, model, action, record_id, payload, gw_meta,
                    facility_id, organization_id, update_action,
                )
                results.append({
                    "run_id": run_id, "service": service, "model": model,
                    "record_id": record_id, "action": action,
                    "status": status, "new_id": new_id, "error": err,
                })
            if len(rows) < batch_size:
                break

    log.info("writeback_to_api %s.%s: %d row(s) attempted", service, model, len(results))
    return {"service": service, "model": model, "results": results}


def clear_dirty_flag(job_result: dict, **context) -> dict:
    """Apply each row's confirmed API result back to Snowflake and write the per-row audit trail.

    Clears _dirty only for "ok" and "failed_permanent" rows; "failed_transient" rows
    are left dirty so the next scheduled run retries them.
    """
    service = job_result["service"]
    model   = job_result["model"]
    results = job_result["results"]
    run_id  = context["run_id"]

    succeeded = failed = 0
    with SnowflakeClient() as sf:
        for r in results:
            _apply_result(sf, service, model, r["record_id"], r["status"], r["new_id"], r["error"], run_id)
            if r["status"] == "ok":
                succeeded += 1
            else:
                failed += 1
        if results:
            _write_audit_results(sf, results)

    log.info("clear_dirty_flag %s.%s: ✓ %d ok · ✗ %d failed", service, model, succeeded, failed)
    return {
        "service": service, "model": model,
        "total": len(results), "succeeded": succeeded, "failed": failed,
    }


def record_writeback_run(summaries: list, **context) -> None:
    """Aggregate all clear_dirty_flag summaries into one WRITEBACK_RUNS audit row."""
    run_id      = context["run_id"]
    started_at  = context["data_interval_start"] or context["logical_date"]
    finished_at = datetime.now(started_at.tzinfo) if started_at.tzinfo else datetime.utcnow()

    summaries   = summaries or []
    total_jobs  = len(summaries)
    total_rows  = sum(s["total"] for s in summaries)
    succeeded   = sum(s["succeeded"] for s in summaries)
    failed      = sum(s["failed"] for s in summaries)
    status      = "success" if not failed else ("partial" if succeeded else "failed")

    if total_jobs == 0:
        log.info("No writeback jobs ran this cycle — skipping WRITEBACK_RUNS insert.")
        return

    with SnowflakeClient() as sf:
        sql = f"""
        INSERT INTO {SF_DB}.{SF_AUDIT}.WRITEBACK_RUNS
            (run_id, started_at, finished_at, status, total_jobs, succeeded_jobs, failed_jobs, total_rows)
        VALUES (
            '{_sq(run_id)}',
            '{_sq(started_at.isoformat())}'::TIMESTAMP_TZ,
            '{_sq(finished_at.isoformat())}'::TIMESTAMP_TZ,
            '{_sq(status)}', {total_jobs}, {succeeded}, {failed}, {total_rows}
        )
        """
        sf.execute(sql, label="audit:writeback_run")

    log.info("══ writeback run=%s status=%s jobs=%d rows=%d ✓%d ✗%d ══",
              run_id, status, total_jobs, total_rows, succeeded, failed)
    if failed:
        raise RuntimeError(
            f"{failed} row(s) failed writeback this run (see WRITEBACK_RESULTS for run_id={run_id})"
        )


# ─── DAG DEFINITION ──────────────────────────────────────────────────────────

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    max_active_tasks=8,
    tags=["v3", "writeback", "snowflake", "api"],
) as dag:

    t_schema = PythonOperator(
        task_id="ensure_writeback_schema",
        python_callable=ensure_writeback_schema,
    )
    t_find = PythonOperator(
        task_id="find_dirty_rows",
        python_callable=find_dirty_rows,
    )
    t_writeback = PythonOperator.partial(
        task_id="writeback_to_api",
        python_callable=writeback_to_api,
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(op_kwargs=t_find.output.map(lambda j: {"job": j}))
    t_clear = PythonOperator.partial(
        task_id="clear_dirty_flag",
        python_callable=clear_dirty_flag,
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(op_kwargs=t_writeback.output.map(lambda r: {"job_result": r}))
    t_record = PythonOperator(
        task_id="record_writeback_run",
        python_callable=record_writeback_run,
        op_kwargs={"summaries": t_clear.output},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_schema >> t_find >> t_writeback >> t_clear >> t_record
