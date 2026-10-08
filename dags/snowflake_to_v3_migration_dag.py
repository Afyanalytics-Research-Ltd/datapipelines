# dags/snowflake_to_v3_migration_dag.py
"""
Snowflake {FACILITY}_CLEAN.<table> → V3 gateway
(Airflow driver for snowflake_to_v3_migration.py)

ONE TASK INSTANCE PER TABLE. `plan` lists the tables to migrate and their
dependency tier; tier_1 … tier_6 are mapped tasks, one instance per table
(labelled "<facility> · <table>"), and each tier starts once the previous
tier has finished, so parents (patients, visits, admissions …) land before
the records that point at them. Within a tier, up to
V3_MIGRATION_PARALLEL_TABLES tables (env, default 8, read at parse time)
run at once, each in its own process.

Running tables in parallel processes is safe: every shared state file
(.migration_record_progress.json, .migration_id_map.json, the done list, the
visit maps …) is merged with what's on disk and replaced atomically on each
write, never overwritten with one process's view.

Grid colours per table:
  green    migrated
  skipped  WAITING — records held back until their parent / V3 user exists
           (re-run later; with fail_on_waiting ticked it's red instead)
  red      failed — dead-lettered records (error_ids in the task log), or a
           crash (retried once automatically)
Clear a single table's task to re-run just that table; records already
in V3 are skipped, never posted twice.

Trigger form options:
  facilities      one or more facility keys.
  tables          only these Snowflake source tables (one per line).
                  Empty = every mapped, insertable table except the
                  old-system-history set.
  exclude_tables  skip these source tables (e.g. ones already migrated).
  mode            migrate     — POST to V3
                  dry_run     — fetch + transform only, nothing posted
                  list_tables — log each table's tier and V3 target, then stop
  record_workers  parallel POSTs within one table (RECORD_WORKERS).
  record_limit    canary: post at most N not-yet-migrated records per table
                  and leave each job open; run again with 0 for the rest.
  allow_history_tables
                  old-system-history tables (reception_patients,
                  evaluation_prescriptions, ...) are refused unless ticked:
                  they're archive copies for old_system_history.
  fail_on_waiting mark WAITING tables as failed instead of skipped.

The destination org/facility come from FACILITY_V3_CONFIG and the V3
account in AFYA_<FACILITY>_USERNAME/PASSWORD (falls back to AFYA_USERNAME);
a table stops before posting anything if they disagree.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

try:
    from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
except ImportError:   # older Airflow 3.0.x
    from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import Param, dag, get_current_context, task
from airflow.task.trigger_rule import TriggerRule

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import (clean_list, facility_keys, int_env, old_system_history_tables,
                              use_pipelines_dir)

log = logging.getLogger(__name__)

DAG_ID = "snowflake_to_v3_migration"
FACILITIES = facility_keys()
# v2_to_v3_api_migration._namespace_tier: 1 + len(_TIER_BOUNDARIES)
TIERS = range(1, 7)
PARALLEL_TABLES = int_env("V3_MIGRATION_PARALLEL_TABLES", 8)


def _setup_modules(p: dict, facility: str):
    os.environ["RECORD_WORKERS"] = str(int(p["record_workers"]))
    use_pipelines_dir([facility])
    import snowflake_to_v3_migration as s2v3
    import v2_to_v3_api_migration as v2v3
    # Read at import time by both modules — set them in case either was
    # already imported in this process.
    s2v3.RECORD_WORKERS = v2v3.RECORD_WORKERS = int(p["record_workers"])
    s2v3.RECORD_LIMIT = int(p["record_limit"])
    return s2v3


@dag(
    dag_id=DAG_ID,
    description="Snowflake CLEAN views → V3 gateway, one task instance per table, tier by tier",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "snowflake", "migration"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "facilities": Param(["kisumu_v3"], type="array",
                            items={"type": "string", "enum": FACILITIES}, title="Facilities"),
        "tables": Param([], type="array", items={"type": "string"}, title="Tables",
                        description="Only these source tables (one per line). Empty = all mapped tables."),
        "exclude_tables": Param([], type="array", items={"type": "string"}, title="Exclude tables",
                                description="Skip these source tables (one per line), e.g. ones already migrated."),
        "mode": Param("migrate", type="string", enum=["migrate", "dry_run", "list_tables"], title="Mode"),
        "record_workers": Param(8, type="integer", minimum=1, maximum=32,
                                title="Parallel POSTs per table",
                                description="Lower it if V3 starts returning 504s. Tables in parallel = "
                                            f"V3_MIGRATION_PARALLEL_TABLES (currently {PARALLEL_TABLES})."),
        "record_limit": Param(0, type="integer", minimum=0, title="Record limit (canary)",
                              description="Post at most this many new records per table and leave the "
                                          "job open, to check a table before the full load. 0 = no limit."),
        "allow_history_tables": Param(False, type="boolean", title="Allow old-system-history tables"),
        "fail_on_waiting": Param(False, type="boolean", title="Fail on waiting tables",
                                 description="Mark tables whose records are only held back (parents/users "
                                             "not yet in V3) as failed instead of skipped."),
    },
)
def snowflake_to_v3_migration():

    @task
    def plan() -> list[dict]:
        p = get_current_context()["params"]
        facilities = clean_list(p["facilities"])
        tables = clean_list(p["tables"])
        if not facilities:
            raise AirflowFailException("Pick at least one facility.")
        unknown = [f for f in facilities if f not in FACILITIES]
        if unknown:
            raise AirflowFailException(f"Unknown facilities {unknown}; known: {FACILITIES}")
        history = sorted(set(tables) & set(old_system_history_tables()))
        if history and not p["allow_history_tables"]:
            raise AirflowFailException(
                f"{history} are old-system-history tables; tick allow_history_tables to load "
                f"them table-by-table anyway.")

        jobs: list[dict] = []
        for facility in facilities:
            s2v3 = _setup_modules(p, facility)
            if p["mode"] == "list_tables":
                s2v3.list_tables(facility)
                continue
            try:
                planned = s2v3.plan_table_jobs(facility, tables or None, clean_list(p["exclude_tables"]))
            except SystemExit as e:
                raise AirflowFailException(f"[{facility}] setup failed (exit {e.code}); see log.")
            for j in planned["jobs"]:
                j["tier"] = min(max(int(j["tier"]), TIERS[0]), TIERS[-1])
            jobs.extend(planned["jobs"])
            for t in planned["unmapped"]:
                log.warning("[%s] SKIPPED %s — no NAMESPACE_MAP entry", facility, t)
            for t in planned["not_insertable"]:
                log.warning("[%s] SKIPPED %s — not insertable in the gateway", facility, t)
        for n in TIERS:
            names = [f"{j['facility']}·{j['table']}" for j in jobs if j["tier"] == n]
            if names:
                log.info("tier %d — %d table(s): %s", n, len(names), ", ".join(names))
        return jobs

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def tables_in_tier(tier: int) -> list[dict]:
        """This tier's tables. ALL_DONE: runs once plan and the previous tier
        are finished whatever their state — a failed table in an earlier tier
        must not stop the next one (its children are just held back), and a
        tier with no tables (skipped) must not skip every later tier, which
        is what the *_min_one_success rules do. A failed plan leaves no XCom:
        skip then (the run still fails — `done` depends on plan)."""
        jobs = get_current_context()["ti"].xcom_pull(task_ids="plan")
        if jobs is None:
            raise AirflowSkipException("plan didn't produce a table list")
        return [j for j in jobs if j["tier"] == tier]

    @task(
        map_index_template="{{ map_label }}",
        max_active_tis_per_dagrun=PARALLEL_TABLES,
        execution_timeout=timedelta(hours=24),
        retries=1,
        retry_delay=timedelta(minutes=3),
    )
    def migrate_table(job: dict) -> dict:
        ctx = get_current_context()
        ctx["map_label"] = f"{job['facility']} · {job['table']}"
        p = ctx["params"]
        s2v3 = _setup_modules(p, job["facility"])
        try:
            result = s2v3.run_one_table(job["facility"], job["table"], dry_run=p["mode"] == "dry_run")
        except SystemExit as e:
            raise RuntimeError(f"setup failed (exit {e.code}); see log")   # retried once
        status, detail = result["status"], result["detail"]
        log.info("[%s] %s → %s %s", job["facility"], job["table"], status.upper(), detail)
        if status == "error":
            raise RuntimeError(detail)                                     # crash: retried once
        if status == "failed":
            raise AirflowFailException(detail)                             # dead letters: no retry
        if status == "waiting":
            if p["fail_on_waiting"]:
                raise AirflowFailException(f"WAITING — {detail}")
            raise AirflowSkipException(f"WAITING — {detail}")
        if status == "skipped":
            raise AirflowSkipException(detail)
        return {**job, **result}

    @task(trigger_rule=TriggerRule.NONE_FAILED)
    def done() -> None:
        """Runs only if no table failed — so the DAG run's own state says
        whether everything went in (skipped = waiting tables)."""
        log.info("No table failed.")

    planned = plan()
    previous = planned
    tier_tasks = []
    for n in TIERS:
        selected = tables_in_tier.override(task_id=f"tier_{n}_tables")(n)
        [planned, previous] >> selected
        ran = migrate_table.override(task_id=f"tier_{n}").expand(job=selected)
        tier_tasks.append(ran)
        previous = ran
    [planned, *tier_tasks] >> done()


snowflake_to_v3_migration()
