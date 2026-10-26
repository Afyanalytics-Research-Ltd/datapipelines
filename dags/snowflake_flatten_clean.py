# dags/snowflake_flatten_clean.py
"""
Snowflake {FACILITY}_RAW.EVENTS_RAW → deduplicated, typed {FACILITY}_CLEAN.<table> views
(Airflow driver for flatten_jsons_schemas.py and dedup.py)

  discover  one mapped task per (facility, source_table): the table's JSON fields/types
  dedup     one task per facility (dedup.py), on those fields: merges → HOSPITALS.DEDUP.CROSSWALK
  flatten   one mapped task per table: CREATE OR REPLACE VIEW, leaving out the records
            merged away and pointing links at the survivors

Tables discover and flatten in parallel and a failed table can be re-run alone.
The schema pairs are derived from the chosen facilities ({FAC}_RAW → {FAC}_CLEAN)
instead of the script's hard-coded SCHEMA_PAIRS list.

Trigger form options (all optional):
  facilities  one or more facility keys.
  tables      only these source tables (one per line). Empty = every
              source_table currently in the facility's EVENTS_RAW.
  dedup       find duplicates before building CLEAN (default on). Off: CLEAN
              still leaves out the last dedup run's merges.
  trigger_v3  on success, trigger snowflake_to_v3_migration for the same
              facilities/tables. Old-system-history tables are never passed
              on — they aren't meant to be inserted table-by-table into V3.
  materialize auto  (default) keep what's there: a CLEAN object that is a
                    TABLE is refreshed in place from RAW (fresh records,
                    still a table); anything else is (re)made a VIEW
              view  always a view — always as fresh as RAW; a leftover
                    table is replaced
              table always a table — a snapshot refreshed on every run
              A table is cloned to {FAC}_CLEAN_BACKUP.<TABLE>__<date> before
              it is replaced (zero-copy), so nothing is lost.

Dedup always covers every table with a dedup rule, whatever `tables` says, since
merges carry across tables. Review pairs are answered in HOSPITALS.DEDUP.DECISIONS;
the next run applies them.

v2_facility_to_snowflake triggers this DAG with {"facilities", "tables"} conf.

Views per run in parallel: env FLATTEN_PARALLEL_TABLES (default 16).
"""
from __future__ import annotations

import logging
import sys
from collections import defaultdict
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
    description="Flatten RAW.EVENTS_RAW JSON into deduplicated, typed CLEAN views",
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
        "materialize": Param("auto", type="string", enum=["auto", "view", "table"], title="Materialize as",
                             description="auto = keep each object's type (a table is refreshed from RAW, "
                                         "otherwise a view); view = always views; table = always tables. "
                                         "Tables are backed up to {FAC}_CLEAN_BACKUP before being replaced."),
        "dedup": Param(True, type="boolean", title="Dedup",
                       description="Find duplicates in RAW before building CLEAN (results in HOSPITALS.DEDUP)."),
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
    def discover_one(item: dict) -> dict:
        get_current_context()["map_label"] = f"{item['facility']} · {item['table']}"
        use_pipelines_dir()
        import flatten_jsons_schemas as fl

        raw = raw_schema(item["facility"])
        conn = fl._snowflake_connect()
        try:
            cur = conn.cursor()
            fields = fl.discover_fields(cur, raw, item["table"])
            if not fields:
                raise AirflowFailException(f"No JSON fields found for {raw}.{item['table']}")
            expanded = fl.expand_objects(cur, raw, item["table"], fields)
        finally:
            conn.close()
        return {**item, "fields": expanded}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def discovered_tables(discovered) -> list[dict]:
        return [d for d in (discovered or []) if d]

    @task
    def dedup_plan(tables: list[dict]) -> list[dict]:
        if not get_current_context()["params"].get("dedup", True):
            return []
        fields = defaultdict(dict)
        for d in tables:
            fields[d["facility"]][d["table"]] = d["fields"]
        return [{"facility": f, "fields": t} for f, t in fields.items()]

    @task(map_index_template="{{ map_label }}", execution_timeout=timedelta(hours=2))
    def dedup_one(item: dict) -> dict:
        get_current_context()["map_label"] = item["facility"]
        use_pipelines_dir([item["facility"]])
        import dedup

        results = dedup.run(f"HOSPITALS.{raw_schema(item['facility'])}", facility=item["facility"],
                            fields=item["fields"])
        merged = sum(r.merged_away for r in results.values())
        review = sum(len(r.review) for r in results.values())
        log.info("%s: %d records merged away, %d pairs for review", item["facility"], merged, review)
        return {"facility": item["facility"], "merged": merged, "review": review}

    # runs after dedup even if it failed: CLEAN then keeps the previous run's merges
    @task(
        map_index_template="{{ map_label }}",
        max_active_tis_per_dagrun=int_env("FLATTEN_PARALLEL_TABLES", 16),
        execution_timeout=timedelta(hours=1),
        trigger_rule=TriggerRule.ALL_DONE,
    )
    def flatten_one(item: dict) -> dict:
        ctx = get_current_context()
        ctx["map_label"] = f"{item['facility']} · {item['table']}"
        use_pipelines_dir()
        import flatten_jsons_schemas as fl

        raw, clean = raw_schema(item["facility"]), clean_schema(item["facility"])
        expanded = item["fields"]
        conn = fl._snowflake_connect()
        try:
            cur = conn.cursor()
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {clean}")
            done = fl.flatten_table(cur, raw, clean, item["table"], expanded,
                                    materialize=ctx["params"].get("materialize") or "auto",
                                    facility=item["facility"])
        finally:
            conn.close()
        log.info("✓ %s.%s — %d columns, %s%s%s", clean, item["table"], len(expanded), done["object"],
                 f" (was a {done['was']})" if done["was"] and done["was"] != done["object"] else "",
                 f" — previous table backed up to {done['backup']}" if done["backup"] else "")
        return {"facility": item["facility"], "table": item["table"], "columns": len(expanded),
                "object": done["object"], "backup": done["backup"]}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize(items: list[dict], results, dedup_items, deduped) -> dict:
        done = [r for r in (results or []) if r]
        ok = {(r["facility"], r["table"]) for r in done}
        failed = [f"{i['facility']} · {i['table']}" for i in items if (i["facility"], i["table"]) not in ok]
        dedup_ok = {r["facility"] for r in (deduped or []) if r}
        failed += [f"{i['facility']} · dedup" for i in (dedup_items or []) if i["facility"] not in dedup_ok]
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
            raise AirflowFailException(f"{len(summary['failed'])} step(s) failed: {summary['failed']}")
        log.info("All %d views rebuilt", summary["flattened"])

    trigger_v3 = TriggerDagRunOperator(
        task_id="trigger_v3_migration",
        trigger_dag_id=V3_DAG_ID,
        conf="{{ ti.xcom_pull(task_ids='summarize')['v3_conf'] }}",
        wait_for_completion=False,
    )

    items = plan()
    tables = discovered_tables(discover_one.expand(item=items))
    to_dedup = dedup_plan(tables)
    deduped = dedup_one.expand(item=to_dedup)
    flattened = flatten_one.expand(item=tables)
    deduped >> flattened
    summary = summarize(items, flattened, to_dedup, deduped)
    should_trigger_v3(summary) >> trigger_v3
    check(summary)


snowflake_flatten_clean()
