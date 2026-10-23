# dags/portbell_moh204a_dag.py
"""
Portbell Medical Centre — MOH 204 A register (Excel) → Snowflake → V3 migrate platform, org 7 / facility 9
(target key "portbell", AFYA_PORTBELL_* in .env). Airflow driver for load_portbell_to_snowflake.py and
portbell_to_v3.py:

  snowflake_load  the workbook → HOSPITALS.PORTBELL_RAW (cell-for-cell text) → HOSPITALS.PORTBELL_CLEAN (patients,
                  visits, attendance, daily totals, bookings, data issues), both re-created on every run
  v3_load         PORTBELL_CLEAN → V3: patients, visits, vitals, doctor_notes, payments (match_on uuid, so a re-run
                  updates instead of duplicating)
  reconcile       V3 vs PORTBELL_CLEAN: every uuid present once, links intact, payments add up (read-only)

The workbook lives at downloads/portbell/ in the repo (git-ignored; patient data). For an updated register from the
clinic, drop the new file there (or point `xlsx` at it) and run with dry_run off. Dry run on: the workbook is parsed
and cleaned and the V3 payloads built, but nothing is written to Snowflake or V3.
Report: output/portbell_to_v3/portbell-<time>.json
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
except ImportError:   # older Airflow 3.0.x
    from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import PIPELINES_DIR, clean_list, use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "portbell_moh204a"
TARGET = "portbell"
STEPS = ["patients", "visits", "vitals", "doctor_notes", "payments"]      # = portbell_to_v3.STEPS
DEFAULT_XLSX = str(PIPELINES_DIR / "downloads" / "portbell" / "moh204a_register.xlsx")


@dag(
    dag_id=DAG_ID,
    description="Portbell MOH 204A register (Excel) → Snowflake PORTBELL_RAW / PORTBELL_CLEAN → V3 org 7",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["portbell", "v3", "migration", "snowflake"],
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=5)},
    params={
        "xlsx": Param(DEFAULT_XLSX, type="string", title="Register workbook",
                      description="Path inside the worker (the repo is mounted at /opt/airflow/pipelines)."),
        "load_snowflake": Param(True, type="boolean", title="Load Snowflake",
                                description="Off: V3 is loaded from the PORTBELL_CLEAN already in Snowflake."),
        "load_v3": Param(True, type="boolean", title="Load V3"),
        "steps": Param(STEPS, type="array", items={"type": "string", "enum": STEPS}, title="V3 steps",
                       description="Run in this order; visits need patients, vitals / notes need visits."),
        "limit": Param(0, type="integer", minimum=0, title="Limit per step",
                       description="Post at most N rows per step (canary). 0 = all."),
        "workers": Param(8, type="integer", minimum=1, maximum=32, title="Parallel V3 writes"),
        "only_missing": Param(False, type="boolean", title="Only missing",
                              description="Post only rows V3 doesn't have yet (after a partial failure)."),
        "reconcile": Param(True, type="boolean", title="Reconcile V3 against CLEAN afterwards"),
        "dry_run": Param(True, type="boolean", title="Dry run",
                         description="On: parse, clean and build payloads only — write nothing anywhere."),
    },
)
def portbell_moh204a():

    @task(execution_timeout=timedelta(minutes=30))
    def snowflake_load() -> dict:
        p = get_current_context()["params"]
        if not p["load_snowflake"]:
            raise AirflowSkipException("load_snowflake is off — using the PORTBELL_CLEAN already in Snowflake")
        use_pipelines_dir([])
        import load_portbell_to_snowflake as pb
        xlsx = Path(p["xlsx"].strip())
        if not xlsx.exists():
            raise AirflowFailException(f"workbook not found: {xlsx} (copy it into downloads/portbell/ in the repo)")
        out = pb.run(xlsx, dry_run=bool(p["dry_run"]))
        log.info("snowflake: %s", json.dumps(out, default=str))
        return out

    @task(execution_timeout=timedelta(hours=2), trigger_rule="none_failed")
    def v3_load() -> dict:
        p = get_current_context()["params"]
        if not p["load_v3"]:
            raise AirflowSkipException("load_v3 is off")
        use_pipelines_dir([TARGET])
        import portbell_to_v3 as pv
        rep = pv.run(clean_list(p["steps"]) or STEPS, limit=int(p["limit"]), workers=int(p["workers"]),
                     dry_run=bool(p["dry_run"]), only_missing=bool(p["only_missing"]))
        for s, r in rep["steps"].items():
            log.info("%-13s %s", s, json.dumps(r, default=str))
        bad = {s: r for s, r in rep["steps"].items() if "error" in r or r.get("failed")}
        if bad:
            raise AirflowFailException(f"V3 steps with errors / failed rows (re-run with only_missing on): {bad}")
        return {"report": rep["report"], "steps": rep["steps"]}

    @task(execution_timeout=timedelta(minutes=30), trigger_rule="none_failed")
    def reconcile() -> dict:
        p = get_current_context()["params"]
        if not p["reconcile"]:
            raise AirflowSkipException("reconcile is off")
        if p["dry_run"]:
            raise AirflowSkipException("dry run: nothing was written to compare")
        if int(p["limit"]):
            raise AirflowSkipException("canary run (limit set): V3 is partial by design")
        use_pipelines_dir([TARGET])
        import portbell_to_v3 as pv
        out = pv.Run(dry_run=True, limit=0, workers=8).reconcile()
        log.info("reconcile: %s", json.dumps(out, default=str))
        if out["problems"]:
            raise AirflowFailException(f"V3 doesn't match PORTBELL_CLEAN: {out['problems']}")
        return out

    snowflake_load() >> v3_load() >> reconcile()


portbell_moh204a()
