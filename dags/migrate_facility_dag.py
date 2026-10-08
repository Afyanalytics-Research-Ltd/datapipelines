# dags/migrate_facility_dag.py
"""
Migrate one V2 facility to V3 end to end, prerequisites first
(Airflow driver for migrate_facility.py — same phases, same code).

  setup → to_snowflake → users → lookup_tables → reconcile_ids → departments
        → gate → main_tables → report

  setup          V2 login ok; V3 login lands in FACILITY_V3_CONFIG's org/facility
  to_snowflake   every V2 table (sheet + old-system-history sets) → {FAC}_RAW,
                 CLEAN views rebuilt
  users          V2 staff → V3 users (email "<orig>.v2-<V2 id>.invalid", random
                 password, reset forced); existing accounts kept
  lookup_tables  units, categories, stores, bed/admission/discharge types,
                 procedure categories, procedures, wards, beds, products
  reconcile_ids  lookups V3 inserted without returning an id, re-matched by key
  departments    one V3 department per V2 visit-destination name
  gate           FAILS the run if any prerequisite isn't fully in V3 (unless
                 allow_gaps) — main_tables then doesn't start
  main_tables    patients, visits, admissions, clinical tables in tier order;
                 records whose parent isn't in V3 yet are held back for the next run
  report         per-table status (always runs)

Trigger form: facility, phases (snowflake / prereqs / main), dry_run,
allow_gaps, workers, record_workers. Re-runs are safe — every step skips
what's already in V3. One facility migration at a time (max_active_runs=1),
and write steps refuse to start while another loader/migration process runs.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task
from airflow.task.trigger_rule import TriggerRule

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import facility_keys, use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "migrate_facility"
FACILITIES = facility_keys()


def _start(write: bool = True):
    """Common task preamble: config + modules, V3 login for the facility.
    Returns (migrate_facility module, params, org_cfg)."""
    p = get_current_context()["params"]
    facility = p["facility"]
    os.environ["RECORD_WORKERS"] = str(int(p["record_workers"]))
    use_pipelines_dir([facility])
    import migrate_facility as mf
    import snowflake_to_v3_migration as s2v3
    import v2_to_v3_api_migration as v2v3
    s2v3.RECORD_WORKERS = v2v3.RECORD_WORKERS = int(p["record_workers"])
    if write and not p["dry_run"]:
        others = mf._other_processes()
        if others:
            raise AirflowFailException("Another loader/migration is running — wait for it:\n  " + "\n  ".join(others))
    org = mf.phase_setup(facility)
    return mf, p, org


def _wants(p: dict, phase: str) -> bool:
    return phase in (p["phases"] or [])


@dag(
    dag_id=DAG_ID,
    description="One V2 facility → Snowflake → V3, prerequisites (users, lookups, departments) first",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["migration", "v2", "v3", "snowflake"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "facility": Param(FACILITIES[0] if FACILITIES else "", type="string", minLength=1, examples=FACILITIES,
                          title="Facility",
                          description="Facility key. A new facility needs only its Airflow Connections: "
                                      "<facility> (V2: host = base URL, schema = db, login/password) and "
                                      "afya_v3_<facility> (V3 login/password; Extra organization_id, facility_id, "
                                      "url_template or urls)."),
        "phases": Param(["snowflake", "prereqs", "main"], type="array",
                        items={"type": "string", "enum": ["snowflake", "prereqs", "main"]}, title="Phases",
                        description="Untick snowflake if the facility's data is already loaded."),
        "dry_run": Param(False, type="boolean", title="Dry run", description="Transform and count; post nothing to V3."),
        "allow_gaps": Param(False, type="boolean", title="Allow gaps",
                            description="Run main tables even if the gate finds prerequisites missing from V3."),
        "workers": Param(8, type="integer", minimum=1, maximum=32, title="Tables in parallel per tier"),
        "record_workers": Param(8, type="integer", minimum=1, maximum=32, title="Parallel POSTs per table"),
    },
)
def migrate_facility():

    @task
    def setup() -> dict:
        mf, p, org = _start(write=False)
        return {"facility": p["facility"], "organization_id": org["organization_id"], "facility_id": org["facility_id"]}

    @task(execution_timeout=timedelta(hours=24))
    def to_snowflake(_setup: dict) -> str:
        mf, p, _ = _start()
        if not _wants(p, "snowflake"):
            return "skipped (phase not selected)"
        mf.phase_snowflake(p["facility"])
        return "done"

    @task(execution_timeout=timedelta(hours=6))
    def users(_prev) -> dict:
        mf, p, _ = _start()
        if not _wants(p, "prereqs"):
            return {"skipped": True}
        return {k: (len(v) if k == "failed" else v) for k, v in mf.sync_users(p["facility"], p["dry_run"]).items()}

    @task(execution_timeout=timedelta(hours=12))
    def lookup_tables(_prev) -> list:
        mf, p, _ = _start()
        if not _wants(p, "prereqs"):
            return []
        entries = mf._discover(p["facility"])
        tables = [t for t in mf.PREREQUISITE_TABLES if entries.get(t, {}).get("v3")]
        return mf.s2v3.run_migration(p["facility"], tables, workers=int(p["workers"]), dry_run=p["dry_run"]) or []

    @task
    def reconcile_ids(_prev) -> str:
        mf, p, org = _start()
        if not _wants(p, "prereqs"):
            return "skipped"
        mf.reconcile_id_maps(p["facility"], mf._discover(p["facility"]), org, p["dry_run"])
        return "done"

    @task
    def departments(_prev) -> str:
        mf, p, _ = _start()
        if not _wants(p, "prereqs") or p["dry_run"]:
            return "skipped"
        if "evaluation_visit_destinations" not in mf._discover(p["facility"]):
            return "no visit destinations in Snowflake"
        mf.s2v3.ensure_departments_from_destinations(p["facility"])
        return "done"

    @task
    def gate(user_stats: dict, _prev) -> list:
        mf, p, _ = _start(write=False)
        if not _wants(p, "prereqs"):
            return []
        stats = user_stats if "v2" in user_stats else {"v2": 0, "matched": 0}
        gaps = mf.gate(p["facility"], mf._discover(p["facility"]), stats)
        if gaps and _wants(p, "main") and not p["allow_gaps"] and not p["dry_run"]:
            raise AirflowFailException("Prerequisites not all in V3 — main tables not started:\n  " + "\n  ".join(gaps)
                                       + "\nRe-run after fixing them, or trigger with allow_gaps.")
        return gaps

    @task(execution_timeout=timedelta(hours=48))
    def main_tables(_gate) -> list:
        mf, p, _ = _start()
        if not _wants(p, "main"):
            return []
        failed = mf.phase_main(p["facility"], int(p["workers"]), p["dry_run"])
        if failed:
            log.warning("tables with failed records: %s — re-run to retry", failed)
        return failed

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def report(_main) -> None:
        mf, p, _ = _start(write=False)
        mf.report(p["facility"])

    s = setup()
    snow = to_snowflake(s)
    u = users(snow)
    lk = lookup_tables(u)
    rc = reconcile_ids(lk)
    dp = departments(rc)
    g = gate(u, dp)
    m = main_tables(g)
    report(m)


migrate_facility()
