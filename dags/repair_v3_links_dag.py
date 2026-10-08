# dags/repair_v3_links_dag.py
"""
Fill the V3 record links the migration never set (visit → patient,
prescription / investigation / doctor note / vital → visit) and repair the
facility's migration state so re-runs update instead of re-inserting
(Airflow driver for repair_v3_links.py — same code).

  plan      snapshot V3 + Snowflake, match every V3 row to its V2 row and
            report what would change (writes nothing)
  prepare   [execute] under the tables' migration locks: repair id map /
            progress / V3 uuids, save each table's updates
  apply     [execute] one task per table, ONE AT A TIME: update the links via
            the gateway (match_on=uuid), then re-read V3 to verify
  report    totals; fails the run if any row failed or still differs

Gentle on V3: each table task starts with `start_workers` parallel requests,
adds one while replies stay fast, halves on an error or a reply slower than
`slow_seconds` (and pauses 15s on errors), never exceeds `max_workers` or
`max_rps` requests/second. Only one table is updated at a time.

Resumable: re-trigger with `resume` ticked to skip plan/prepare and carry on
from the last saved position (or clear a failed apply task to retry it).
Refuses to run while a migration is writing one of the tables on this host.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import Param, dag, get_current_context, task
from airflow.task.trigger_rule import TriggerRule

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import clean_list, facility_keys, use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "repair_v3_links"
LINK_TABLES = ["visits", "prescriptions", "investigations", "doctor_notes", "vitals"]


def _setup():
    p = get_current_context()["params"]
    facility = p["facility"].strip()
    use_pipelines_dir([facility])
    import repair_v3_links as rl
    rl.ALLOW_GROWTH = bool(p["allow_growth"])
    tables = [t for t in LINK_TABLES if t in set(clean_list(p["tables"]))] or LINK_TABLES
    pace = rl.Pace(int(p["start_workers"]), int(p["max_workers"]), float(p["max_rps"]), float(p["slow_seconds"]))
    return rl, p, facility, tables, pace


def _busy_to_fail(fn):
    import snowflake_to_v3_migration as s2v3
    try:
        return fn()
    except s2v3.TableBusy as e:
        raise AirflowFailException(str(e))


@dag(
    dag_id=DAG_ID,
    description="Set missing V3 record links via the gateway (throttled) and repair the migration state",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration", "repair"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "facility": Param("kisumu_v3", type="string", title="Facility", examples=facility_keys()),
        "tables": Param([], type="array", items={"type": "string", "enum": LINK_TABLES}, title="Tables",
                        description="Empty = all five."),
        "execute": Param(False, type="boolean", title="Execute",
                         description="Writes to V3 and the state files. Off = plan only."),
        "resume": Param(False, type="boolean", title="Resume",
                        description="Skip plan/prepare and continue applying the last prepared updates."),
        "allow_growth": Param(True, type="boolean", title="Allow growth",
                              description="Go on while V3 rows are still being added (they get linked on a later run)."),
        "start_workers": Param(2, type="integer", minimum=1, maximum=32, title="Start with N parallel requests"),
        "max_workers": Param(8, type="integer", minimum=1, maximum=32, title="Never more than N parallel requests"),
        "max_rps": Param(15, type="number", minimum=0, title="Max requests per second (0 = no cap)"),
        "slow_seconds": Param(5, type="number", minimum=1, title="Back off when a reply takes longer than (s)"),
    },
)
def repair_v3_links():

    @task
    def plan() -> dict:
        rl, p, facility, tables, _ = _setup()
        if p["resume"]:
            raise AirflowSkipException("Resume — using the last prepared updates.")
        report = rl.prepare(facility, tables, execute=False)
        log.info("%s", json.dumps(report, indent=2, default=str))
        return report

    @task(trigger_rule=TriggerRule.NONE_FAILED, execution_timeout=timedelta(hours=1))
    def prepare(_plan) -> list[str]:
        rl, p, facility, tables, _ = _setup()
        if not p["execute"]:
            raise AirflowSkipException("Plan only — tick `execute` to apply.")
        if not p["resume"]:
            # plan's snapshot is minutes old; matching again under the locks
            report = _busy_to_fail(lambda: rl.prepare(facility, tables, execute=True, snapshot_first=False))
            log.info("%s", json.dumps(report, indent=2, default=str))
        return tables

    @task(max_active_tis_per_dagrun=1, execution_timeout=timedelta(hours=12))
    def apply(table: str) -> dict:
        rl, p, facility, _, pace = _setup()
        out = _busy_to_fail(lambda: rl.apply_table(facility, table, pace))
        log.info("%s: %s", table, json.dumps(out, default=str))
        return {"table": table, **out}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def report(results) -> None:
        results = [r for r in (results or []) if r]
        for r in results:
            log.info("%-14s updated %s · failed %s · still differ %s", r["table"], r["apply"]["updated"],
                     r["apply"]["failed"], (r.get("verify") or {}).get("still_wrong"))
        bad = [r["table"] for r in results if r["apply"]["failed"] or (r.get("verify") or {}).get("still_wrong")]
        if bad:
            raise AirflowFailException(f"Not fully linked: {bad} — re-trigger with resume ticked")

    report(apply.expand(table=prepare(plan())))


repair_v3_links()
