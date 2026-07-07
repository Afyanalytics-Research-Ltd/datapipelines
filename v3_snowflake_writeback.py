#!/usr/bin/env python3
"""
v3_snowflake_writeback.py — Snowflake → V3 Service APIs (reverse of v3_api_to_snowflake.py)

Pushes corrected or newly staged records in HOSPITALS.V3_READY.<SERVICE> back into
the V3 gateway APIs. Analysts (or another Snowflake job) edit a row's `payload` in
V3_READY and flag it `_dirty = TRUE`; this pipeline finds every dirty row, POSTs it
to the originating V3 service's gateway, and clears the flag on success.

STAGING CONTRACT (columns this script adds to every V3_READY.<SERVICE> table)
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

Why an explicit action column instead of inferring insert-vs-update from _record_id:
V3_READY._record_id is NOT NULL for every row (it's the MERGE key from the read
pipeline), so nullness can't signal "this is a new record." The flag makes intent
explicit instead of guessing.

Flow per run:
  1. Auth      → POST core /v1/login → bearer token (50-min TTL)
  2. Discover  → POST each service /v1/gateway action=list → per-alias operations/tenant/facility
  3. Find work → per service: SELECT _model, COUNT(*) FROM V3_READY.<service> WHERE _dirty GROUP BY _model
  4. Per (service, model), in batches:
       fetch dirty rows → POST /v1/gateway action=insert|update
       → update the row (_dirty / _record_id / payload:id / _writeback_error)
       → append a WRITEBACK_RESULTS audit row
  5. Insert a WRITEBACK_RUNS audit row

Permanent failures (400/404/409/422/500 — validation or unsupported request) clear
`_dirty` but leave `_writeback_error` populated: fix the data and re-flag `_dirty =
TRUE` to retry. Transient failures (network error, exhausted 429/502/503/504
retries) leave `_dirty = TRUE` so the next run retries automatically.

NOTE — the gateway's update verb has not been confirmed against a live call; only
action="insert" is proven (see v2_to_v3_api_migration.py). Verify with --dry-run
and a single record before relying on this broadly, and set V3_UPDATE_ACTION if
your gateway uses a different verb (e.g. "edit"/"modify").

USAGE
  python v3_snowflake_writeback.py
  python v3_snowflake_writeback.py --services finance,reception
  python v3_snowflake_writeback.py --models invoice,patient
  python v3_snowflake_writeback.py --dry-run
  python v3_snowflake_writeback.py --limit 500

ENV  (.env next to this script)
  AFYA_USERNAME=...   AFYA_PASSWORD=...   AFYA_FACILITY_ID=6   AFYA_ORGANIZATION_ID=1
  SNOWFLAKE_USER=...  SNOWFLAKE_ACCOUNT=...  SNOWFLAKE_WAREHOUSE=...
  SNOWFLAKE_DATABASE=HOSPITALS            SNOWFLAKE_PRIVATE_KEY_PATH=...
  PIPELINE_WORKERS=4  RECORD_WORKERS=3    BATCH_SIZE=200        LOG_LEVEL=INFO
  V3_UPDATE_ACTION=update   (override if your gateway's update verb differs)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import requests.adapters
import snowflake.connector
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, HTTPError, Timeout

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

# ─── LOGGING ──────────────────────────────────────────────────────────────────

log = logging.getLogger("v3_writeback")
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

AFYA_FACILITY_ID     = int(os.getenv("AFYA_FACILITY_ID", "6"))
_org_id_env          = (os.getenv("AFYA_ORGANIZATION_ID") or "").strip()
ORG_CFG: dict[str, Any] = {
    "organization_id": int(_org_id_env) if _org_id_env else None,
    "facility_id":     AFYA_FACILITY_ID,
}

TOKEN_TTL         = int(os.getenv("TOKEN_TTL_SECONDS", str(50 * 60)))
PIPELINE_WORKERS  = int(os.getenv("PIPELINE_WORKERS", "4"))
RECORD_WORKERS    = int(os.getenv("RECORD_WORKERS", "3"))
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "200"))
WRITE_UPDATE_ACTION = os.getenv("V3_UPDATE_ACTION", "update")
WRITE_INSERT_ACTION = "insert"

SF_DB    = os.getenv("SNOWFLAKE_DATABASE", "HOSPITALS").upper()
SF_READY = "V3_READY"
SF_AUDIT = "MIGRATION_AUDIT"

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _sq(s: str) -> str:
    """Escape a string for embedding in a single-quoted SQL literal."""
    return (s if s is not None else "").replace("'", "''")

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
            schema    = SF_AUDIT,
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
        with self._lock, self._cur() as c:
            c.execute(sql)
            rows = c.rowcount
        log.info("SF %-44s rowcount=%-6s %.2fs", label or "exec", rows, time.perf_counter() - t0)
        return rows

    def query(self, sql: str, label: str = "") -> list[dict]:
        t0 = time.perf_counter()
        with self._lock, self._cur() as c:
            c.execute(sql)
            cols = [d[0] for d in c.description]
            rows = [dict(zip(cols, row)) for row in c.fetchall()]
        log.info("SF %-44s rows=%-6d %.2fs", label or "query", len(rows), time.perf_counter() - t0)
        return rows

    def __enter__(self) -> "SnowflakeClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

# ─── HTTP SESSION + TOKEN ─────────────────────────────────────────────────────

_http:        requests.Session | None   = None
_http_lock  = threading.Lock()
_token_cache: tuple[str, float] | None  = None
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

_gateway_meta: dict[str, dict[str, dict]] = {}   # service -> alias -> {operations, tenant, facility}

def discover_gateway_meta(services: list[str]) -> dict[str, dict[str, dict]]:
    meta: dict[str, dict[str, dict]] = {}
    for svc in services:
        url = f"{V3_SERVICES[svc].rstrip('/')}/v1/gateway"
        try:
            r = _session().post(url, headers=_headers(), json={"action": "list"}, timeout=30)
            if not r.ok:
                log.warning("Gateway list [%s] %s: %s", svc, r.status_code, r.text[:200])
                meta[svc] = {}
                continue
            entries = r.json().get("data") or []
            meta[svc] = {
                e["alias"]: {
                    "operations": e.get("operations", []),
                    "tenant":     e.get("tenant"),
                    "facility":   e.get("facility"),
                }
                for e in entries if e.get("alias")
            }
            log.info("Service %-12s gateway metadata for %d models", svc, len(meta[svc]))
        except Exception as exc:
            log.warning("Cannot reach gateway [%s]: %s", svc, exc)
            meta[svc] = {}
    return meta

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
    ("_dirty",            "BOOLEAN DEFAULT FALSE"),
    ("_writeback_action",  "VARCHAR DEFAULT 'update'"),
    ("_writeback_at",      "TIMESTAMP_TZ"),
    ("_writeback_run_id",  "VARCHAR"),
    ("_writeback_error",   "VARCHAR"),
]

def ensure_writeback_schema(sf: SnowflakeClient, services: list[str]) -> None:
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

# ─── FIND WORK ────────────────────────────────────────────────────────────────

def discover_dirty_jobs(
    sf: SnowflakeClient, services: list[str], only_models: set[str] | None,
) -> list[tuple[str, str, int]]:
    jobs: list[tuple[str, str, int]] = []
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
            jobs.append((svc, model, int(row["CNT"])))
    return jobs

def _fetch_dirty_batch(sf: SnowflakeClient, service: str, model: str, limit: int) -> list[dict]:
    t = f"{SF_DB}.{SF_READY}.{service.upper()}"
    sql = f"""
    SELECT _record_id, _writeback_action, payload
    FROM {t}
    WHERE _model = '{_sq(model)}' AND _dirty = TRUE
    LIMIT {int(limit)}
    """
    return sf.query(sql, label=f"fetch:{service}.{model}")

# ─── V3 WRITE (with retry) ────────────────────────────────────────────────────

def _write_row(
    service: str,
    model: str,
    action: str,
    record_id: str,
    payload: dict,
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
    url  = f"{V3_SERVICES[service].rstrip('/')}/v1/gateway"
    meta = _gateway_meta.get(service, {}).get(model, {})

    data = dict(payload)
    if action == WRITE_INSERT_ACTION:
        data.pop("id", None)
        body = {"action": WRITE_INSERT_ACTION, "model": model, "data": data}
    else:
        data["id"] = record_id
        body = {"action": WRITE_UPDATE_ACTION, "model": model, "id": record_id, "data": data}
    if ORG_CFG.get("organization_id") is not None:
        body["destination_tenant_id"] = ORG_CFG["organization_id"]

    headers = _headers()
    if ORG_CFG.get("organization_id") is not None:
        headers["X-Tenant-Id"] = str(ORG_CFG["organization_id"])
    if meta.get("facility") and ORG_CFG.get("facility_id") is not None:
        headers["X-Facility-Id"] = str(ORG_CFG["facility_id"])

    attempt, wait = 0, init_wait
    while True:
        attempt += 1
        try:
            r = _session().post(url, headers=headers, json=body, timeout=90)
            log.info("  V3 %-6s svc=%-12s model=%-30s id=%-10s status=%d",
                      action, service, model, record_id, r.status_code)

            if r.status_code == 401:
                _invalidate_token()
                headers["Authorization"] = f"Bearer {_get_token()}"
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

# ─── APPLY RESULT BACK TO SNOWFLAKE ───────────────────────────────────────────

def _apply_result(
    sf: SnowflakeClient, service: str, model: str, record_id: str,
    status: str, new_id: str | None, error: str | None, run_id: str,
) -> None:
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

def _write_writeback_run(
    sf: SnowflakeClient, run_id: str, started_at: str, finished_at: str,
    status: str, total_jobs: int, succeeded: int, failed: int, total_rows: int,
) -> None:
    sql = f"""
    INSERT INTO {SF_DB}.{SF_AUDIT}.WRITEBACK_RUNS
        (run_id, started_at, finished_at, status, total_jobs, succeeded_jobs, failed_jobs, total_rows)
    VALUES (
        '{_sq(run_id)}',
        '{_sq(started_at)}'::TIMESTAMP_TZ,
        '{_sq(finished_at)}'::TIMESTAMP_TZ,
        '{_sq(status)}', {total_jobs}, {succeeded}, {failed}, {total_rows}
    )
    """
    sf.execute(sql, label="audit:writeback_run")

# ─── JOB RUNNER ───────────────────────────────────────────────────────────────

def run_job(
    service: str, model: str, sf: SnowflakeClient, run_id: str, *,
    dry_run: bool, record_workers: int, batch_size: int, max_rows: int | None,
) -> list[dict]:
    results: list[dict] = []
    processed = 0

    while max_rows is None or processed < max_rows:
        fetch_limit = batch_size if max_rows is None else min(batch_size, max_rows - processed)
        rows = _fetch_dirty_batch(sf, service, model, fetch_limit)
        if not rows:
            break

        if dry_run:
            for row in rows:
                log.info("  DRY-RUN %s.%-30s id=%-10s action=%s",
                          service, model, row["_RECORD_ID"], row["_WRITEBACK_ACTION"])
            processed += len(rows)
            continue

        def _do(row: dict) -> dict:
            record_id = row["_RECORD_ID"]
            action    = (row["_WRITEBACK_ACTION"] or "update").lower()
            raw       = row["PAYLOAD"]
            payload   = json.loads(raw) if isinstance(raw, str) else raw
            status, new_id, err = _write_row(service, model, action, record_id, payload)
            _apply_result(sf, service, model, record_id, status, new_id, err, run_id)
            return {
                "run_id": run_id, "service": service, "model": model,
                "record_id": record_id, "action": action,
                "status": status, "new_id": new_id, "error": err,
            }

        with ThreadPoolExecutor(max_workers=max(1, record_workers)) as pool:
            results.extend(pool.map(_do, rows))

        processed += len(rows)

    return results

# ─── PIPELINE ─────────────────────────────────────────────────────────────────

def run_writeback(
    *,
    services:       list[str] | None = None,
    only_models:    set[str]  | None = None,
    dry_run:        bool             = False,
    workers:        int              = PIPELINE_WORKERS,
    record_workers: int              = RECORD_WORKERS,
    batch_size:     int              = BATCH_SIZE,
    limit:          int | None       = None,
) -> None:
    run_id     = datetime.now(timezone.utc).strftime("v3wb__%Y%m%dT%H%M%SZ")
    started_at = datetime.now(timezone.utc)

    active_services = services or list(V3_SERVICES.keys())
    unknown = [s for s in active_services if s not in V3_SERVICES]
    if unknown:
        log.error("Unknown services: %s. Valid: %s", unknown, list(V3_SERVICES.keys()))
        sys.exit(1)

    log.info("══ START writeback run=%s services=[%s] ══", run_id, ", ".join(active_services))

    global _gateway_meta
    _gateway_meta = discover_gateway_meta(active_services)

    all_results: list[dict] = []

    with SnowflakeClient() as sf:
        if not dry_run:
            ensure_writeback_schema(sf, active_services)

        jobs = discover_dirty_jobs(sf, active_services, only_models)
        if not jobs:
            log.info("No dirty rows found — nothing to write back.")
            return
        log.info("Jobs to run: %d  (%s)", len(jobs),
                  ", ".join(f"{s}.{m}={c}" for s, m, c in jobs))

        def _do_job(job: tuple[str, str, int]) -> list[dict]:
            svc, model, _cnt = job
            return run_job(svc, model, sf, run_id, dry_run=dry_run,
                            record_workers=record_workers, batch_size=batch_size, max_rows=limit)

        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            for res_list in pool.map(_do_job, jobs):
                all_results.extend(res_list)

        succeeded = [r for r in all_results if r["status"] == "ok"]
        failed    = [r for r in all_results if r["status"] != "ok"]

        if not dry_run and all_results:
            _write_audit_results(sf, all_results)
            _write_writeback_run(
                sf, run_id, started_at.isoformat(), datetime.now(timezone.utc).isoformat(),
                "success" if not failed else ("partial" if succeeded else "failed"),
                len(jobs), len(succeeded), len(failed), len(all_results),
            )

    log.info("══ END writeback run=%s ✓ %d ok · ✗ %d failed ══",
              run_id, len(succeeded), len(failed))
    if failed:
        for f in failed[:10]:
            log.warning("  FAILED %s.%s id=%s [%s]: %s",
                        f["service"], f["model"], f["record_id"], f["status"], (f.get("error") or "")[:140])
        sys.exit(1)

# ─── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--services",
        help=f"Comma-separated services to run (default: all). Options: {', '.join(V3_SERVICES)}",
    )
    ap.add_argument(
        "--models",
        help="Comma-separated model aliases to restrict to (e.g. invoice,patient).",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Log what would be sent — no V3 POSTs, no Snowflake writes.",
    )
    ap.add_argument(
        "--workers", type=int, default=PIPELINE_WORKERS,
        help=f"Parallel (service, model) jobs (default {PIPELINE_WORKERS}).",
    )
    ap.add_argument(
        "--record-workers", type=int, default=RECORD_WORKERS,
        help=f"Parallel row POSTs within a job (default {RECORD_WORKERS}).",
    )
    ap.add_argument(
        "--batch-size", type=int, default=BATCH_SIZE,
        help=f"Dirty rows fetched from Snowflake per round (default {BATCH_SIZE}).",
    )
    ap.add_argument(
        "--limit", type=int, default=None,
        help="Max rows to process per (service, model) job in this run (default: all dirty rows).",
    )
    args = ap.parse_args()

    run_writeback(
        services=(
            [s.strip() for s in args.services.split(",") if s.strip()]
            if args.services else None
        ),
        only_models=(
            {m.strip() for m in args.models.split(",") if m.strip()}
            if args.models else None
        ),
        dry_run=args.dry_run,
        workers=args.workers,
        record_workers=args.record_workers,
        batch_size=args.batch_size,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
