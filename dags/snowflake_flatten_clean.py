# dags/snowflake_flatten_clean.py
"""
Snowflake {FACILITY}_RAW.EVENTS_RAW → typed {FACILITY}_CLEAN.<table> views
(Airflow driver for flatten_jsons_schemas.py)

One mapped task per (facility, source_table): each discovers that table's
JSON fields/types and does CREATE OR REPLACE VIEW, so tables flatten in
parallel and a failed table can be re-run alone. The schema pairs are
derived from the chosen facilities ({FAC}_RAW → {FAC}_CLEAN) instead of the
script's hard-coded SCHEMA_PAIRS list.

Trigger form options (all optional):
  facilities  one or more facility keys.
  tables      only these source tables (one per line). Empty = every
              source_table currently in the facility's EVENTS_RAW.
  trigger_v3  on success, trigger snowflake_to_v3_migration for the same
              facilities/tables. Old-system-history tables are never passed
              on — they aren't meant to be inserted table-by-table into V3.

v2_facility_to_snowflake triggers this DAG with {"facilities", "tables"} conf.

Views per run in parallel: env FLATTEN_PARALLEL_TABLES (default 16).
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, get_current_context, task
from airflow.task.trigger_rule import TriggerRule

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import (clean_list, clean_schema, facility_keys, int_env,
                              old_system_history_tables, raw_schema, use_pipelines_dir)

log = logging.getLogger(__name__)

DAG_ID = "snowflake_flatten_clean"
V3_DAG_ID = "snowflake_to_v3_migration"
FACILITIES = facility_keys()


@dag(
    dag_id=DAG_ID,
    description="Flatten RAW.EVENTS_RAW JSON into typed CLEAN views, one mapped task per table",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=2,
    render_template_as_native_obj=True,
    tags=["snowflake", "transform", "migration"],
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=1)},
    params={
        "facilities": Param(["kisumu_v3"], type="array",
                            items={"type": "string"}, title="Facilities",
                            description="Facility keys, one per line — any {NAME}_RAW schema, e.g. silverwood."),
        "tables": Param([], type="array", items={"type": "string"}, title="Tables",
                        description="Only these source tables (one per line). Empty = all in EVENTS_RAW."),
        "trigger_v3": Param(False, type="boolean", title="Trigger V3 migration after flatten",
                            description="Writes to V3. Old-system-history tables are excluded."),
    },
)
def snowflake_flatten_clean():

    @task
    def plan() -> list[dict]:
        use_pipelines_dir()
        import flatten_jsons_schemas as fl

        p = get_current_context()["params"]
        facilities = clean_list(p["facilities"])
        wanted = set(clean_list(p["tables"]))
        if not facilities:
            raise AirflowFailException("Pick at least one facility.")

        items: list[dict] = []
        conn = fl._snowflake_connect()
        try:
            cur = conn.cursor()
            for facility in facilities:
                try:
                    present = fl.get_source_tables(cur, raw_schema(facility))
                except Exception as e:
                    raise AirflowFailException(f"[{facility}] cannot read {raw_schema(facility)}.EVENTS_RAW: {e}")
                by_lower = {t.lower(): t for t in present}
                if wanted:
                    missing = sorted(wanted - set(by_lower))
                    if missing:
                        log.warning("[%s] not in EVENTS_RAW, skipped: %s", facility, missing)
                    chosen = [by_lower[t] for t in sorted(wanted & set(by_lower))]
                else:
                    chosen = present
                log.info("[%s] %d tables to flatten", facility, len(chosen))
                items += [{"facility": facility, "table": t} for t in chosen]
        finally:
            conn.close()
        if not items:
            raise AirflowFailException("No tables to flatten for that selection.")
        return items

    @task(
        map_index_template="{{ map_label }}",
        max_active_tis_per_dagrun=int_env("FLATTEN_PARALLEL_TABLES", 16),
        execution_timeout=timedelta(hours=1),
    )
    def flatten_one(item: dict) -> dict:
        ctx = get_current_context()
        ctx["map_label"] = f"{item['facility']} · {item['table']}"
        use_pipelines_dir()
        import flatten_jsons_schemas as fl

        raw, clean = raw_schema(item["facility"]), clean_schema(item["facility"])
        conn = fl._snowflake_connect()
        try:
            cur = conn.cursor()
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {clean}")
            fields = fl.discover_fields(cur, raw, item["table"])
            if not fields:
                raise AirflowFailException(f"No JSON fields found for {raw}.{item['table']}")
            expanded = fl.expand_objects(cur, raw, item["table"], fields)
            cur.execute(fl.build_flatten_sql(raw, clean, item["table"], expanded))
        finally:
            conn.close()
        log.info("✓ %s.%s — %d columns", clean, item["table"], len(expanded))
        return {**item, "columns": len(expanded)}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize(items: list[dict], results) -> dict:
        done = [r for r in (results or []) if r]
        ok = {(r["facility"], r["table"]) for r in done}
        failed = [f"{i['facility']} · {i['table']}" for i in items if (i["facility"], i["table"]) not in ok]
        history = set(old_system_history_tables())
        return {
            "failed": failed,
            "flattened": len(done),
            "v3_conf": {
                "facilities": sorted({r["facility"] for r in done}),
                "tables": sorted({r["table"].lower() for r in done} - history),
            },
        }

    @task.short_circuit
    def should_trigger_v3(summary: dict) -> bool:
        p = get_current_context()["params"]
        return bool(p["trigger_v3"] and not summary["failed"] and summary["v3_conf"]["tables"])

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def check(summary: dict) -> None:
        if summary["failed"]:
            raise AirflowFailException(f"{len(summary['failed'])} view(s) failed: {summary['failed']}")
        log.info("All %d views rebuilt", summary["flattened"])

    trigger_v3 = TriggerDagRunOperator(
        task_id="trigger_v3_migration",
        trigger_dag_id=V3_DAG_ID,
        conf="{{ ti.xcom_pull(task_ids='summarize')['v3_conf'] }}",
        wait_for_completion=False,
    )

    items = plan()
    flattened = flatten_one.expand(item=items)
    summary = summarize(items, flattened)
    should_trigger_v3(summary) >> trigger_v3
    check(summary)


snowflake_flatten_clean()
