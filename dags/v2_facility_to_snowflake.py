# dags/v2_facility_to_snowflake.py
"""
V2 facility API → S3 → Snowflake {FACILITY}_RAW.EVENTS_RAW
(Airflow driver for facility_to_snowflake_fast_resume.py)

One mapped task per (facility, table), so tables load in parallel and each
one can be retried or re-run on its own from the grid view ("Clear" on a
single map index re-extracts just that table).

Trigger form options (all optional):
  facilities       one or more facility keys (multi-select).
  table_set        "sheet"              — tables from the data-dictionary sheet
                   "old_system_history" — the V2 tables behind old_system_history
  tables           limit to these tables (one per line). Empty = whole table set.
                   A name that matches nothing in the set fails the plan step.
  full_refresh     extract everything (updated_since = 1970) instead of the watermark.
  since            explicit updated_since (ISO date/timestamp); beats full_refresh.
  dry_run          extract and count only — nothing written to S3/Snowflake.
  skip_merge       skip the RAW → CLEAN.EVENTS merge.
  update_watermark advance the watermark (only when every table in the run
                   succeeded AND no `tables` filter was given — a partial run
                   must not move the watermark past tables it didn't load).
  page_workers     parallel page fetches inside one table.
  trigger_flatten  on success, trigger snowflake_flatten_clean for the same
                   facilities/tables so the CLEAN views pick up new columns.

Tables in parallel per run: env V2_LOADER_PARALLEL_TABLES (default 8) — read
at parse time; lower it if a facility API starts returning 429s.

Config: any setting missing from the environment is read from Airflow
(pipelines_common.load_airflow_config) — Variables IGNITE_SHEET_ID,
IGNITE_SHEET_WORKSHEET (or WORKSHEET), GOOGLE_SA_JSON, Connection
aws_default, and Connection <facility> (login/password) for V2 credentials;
then the repo .env fills whatever is still unset.

Watermarks/progress are shared with the CLI (same .watermarks.json), keyed
per facility and table set exactly like
`python facility_to_snowflake_fast_resume.py --facility X --table-set Y`.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, get_current_context, task
from airflow.task.trigger_rule import TriggerRule

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import (clean_list, facility_keys, int_env,
                              old_system_history_tables, use_pipelines_dir)

log = logging.getLogger(__name__)

DAG_ID = "v2_facility_to_snowflake"
FLATTEN_DAG_ID = "snowflake_flatten_clean"
FULL_REFRESH_SINCE = "1970-01-01T00:00:00Z"
FACILITIES = facility_keys()


def _job_label(job: dict) -> str:
    return f"{job['facility']} · {job['table']}"


@dag(
    dag_id=DAG_ID,
    description="V2 facility API → S3 → Snowflake RAW, one mapped task per table",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["v2", "snowflake", "ingestion", "migration"],
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
    params={
        "facilities": Param(["kisumu_v3"], type="array",
                            items={"type": "string", "enum": FACILITIES},
                            title="Facilities", description="One or more facility keys."),
        "table_set": Param("sheet", type="string", enum=["sheet", "old_system_history"],
                           title="Table set",
                           description="sheet = data-dictionary sheet tables; "
                                       "old_system_history = V2 tables behind old_system_history "
                                       f"({len(old_system_history_tables())} tables)."),
        "tables": Param([], type="array", items={"type": "string"}, title="Tables",
                        description="Only these tables (one per line). Empty = the whole set. "
                                    "Old-system-history names: " + ", ".join(old_system_history_tables())),
        "full_refresh": Param(False, type="boolean", title="Full refresh",
                              description="Ignore the watermark and extract everything."),
        "since": Param("", type="string", title="Since",
                       description="Explicit updated_since, e.g. 2026-09-01. Overrides full_refresh."),
        "dry_run": Param(False, type="boolean", title="Dry run",
                         description="Extract and count only; write nothing."),
        "skip_merge": Param(False, type="boolean", title="Skip CLEAN.EVENTS merge"),
        "update_watermark": Param(True, type="boolean", title="Update watermark"),
        "page_workers": Param(4, type="integer", minimum=1, maximum=64, title="Page workers per table"),
        "trigger_flatten": Param(True, type="boolean", title="Trigger flatten DAG after load"),
    },
)
def v2_facility_to_snowflake():

    @task
    def plan() -> dict:
        p = get_current_context()["params"]
        facilities = clean_list(p["facilities"])
        tables = clean_list(p["tables"])
        table_set = p["table_set"]
        if not facilities:
            raise AirflowFailException("Pick at least one facility.")
        use_pipelines_dir(facilities)
        import facility_to_snowflake_fast_resume as loader

        if table_set == "sheet" and not (os.environ.get("IGNITE_SHEET_ID") or "").strip():
            raise AirflowFailException(
                "IGNITE_SHEET_ID is not set — add it as an Airflow Variable (or to the "
                "environment / repo .env). The sheet table set needs it to list tables; "
                "table_set=old_system_history doesn't.")
        unknown_fac = [f for f in facilities if f not in loader.FACILITIES]
        if unknown_fac:
            raise AirflowFailException(f"Unknown facilities {unknown_fac}; known: {sorted(loader.FACILITIES)}")

        since = (p["since"] or "").strip() or (FULL_REFRESH_SINCE if p["full_refresh"] else None)
        started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        run_id = datetime.now(timezone.utc).strftime("airflow__%Y-%m-%dT%H-%M-%SZ")

        jobs: list[dict] = []
        for facility in facilities:
            fjobs = loader.build_jobs_for_facility(facility, since=since,
                                                   only_tables=set(tables) or None,
                                                   table_set=table_set)
            if tables:
                missing = sorted(set(tables) - {j["table"].lower() for j in fjobs})
                if missing:
                    raise AirflowFailException(
                        f"[{facility}] tables not in table set '{table_set}': {missing}")
            for j in fjobs:
                j["run_id"] = run_id
            jobs.extend(fjobs)
            log.info("[%s] %d tables · updated_since=%s · state key=%s", facility, len(fjobs),
                     fjobs[0]["updated_since"] if fjobs else "-", loader.state_key(facility, table_set))

        if not jobs:
            raise AirflowFailException("Nothing to run — the table filter matched no tables.")
        return {"jobs": jobs, "facilities": facilities, "tables": tables,
                "table_set": table_set, "started_at": started_at}

    @task
    def jobs_of(plan_out: dict) -> list[dict]:
        return plan_out["jobs"]

    @task(
        map_index_template="{{ map_label }}",
        max_active_tis_per_dagrun=int_env("V2_LOADER_PARALLEL_TABLES", 8),
        execution_timeout=timedelta(hours=6),
    )
    def extract_load(job: dict) -> dict:
        ctx = get_current_context()
        ctx["map_label"] = _job_label(job)
        use_pipelines_dir([job["facility"]])
        import facility_to_snowflake_fast_resume as loader

        p = ctx["params"]
        result = loader.extract_one_model(job, run_id=job["run_id"], dry_run=p["dry_run"],
                                          page_workers=int(p["page_workers"]))
        if result is None:
            # 0 rows, or a dry run (which logs its row count itself)
            return {"facility": job["facility"], "table": job["table"], "rows": 0,
                    "status": "dry_run" if p["dry_run"] else "empty"}
        loader.copy_into_snowflake(result)
        return {"facility": job["facility"], "table": job["table"],
                "rows": result["row_count"], "status": "loaded", "s3_key": result["s3_key"]}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finalize(plan_out: dict, results) -> dict:
        use_pipelines_dir(plan_out["facilities"])
        import facility_to_snowflake_fast_resume as loader

        p = get_current_context()["params"]
        done = [r for r in (results or []) if r]
        done_keys = {(r["facility"], r["table"]) for r in done}
        failed = [_job_label(j) for j in plan_out["jobs"]
                  if (j["facility"], j["table"]) not in done_keys]

        for facility in plan_out["facilities"]:
            fac_failed = [f for f in failed if f.startswith(f"{facility} · ")]
            loaded = [r for r in done if r["facility"] == facility and r["status"] == "loaded"]
            log.info("[%s] loaded %d tables · %d rows · %d failed", facility, len(loaded),
                     sum(r["rows"] for r in loaded), len(fac_failed))
            if p["dry_run"]:
                continue
            if loaded and not p["skip_merge"]:
                loader.merge_clean(facility)
            if fac_failed or not p["update_watermark"]:
                continue
            if plan_out["tables"]:
                log.info("[%s] watermark NOT advanced — run was limited to %s",
                         facility, plan_out["tables"])
                continue
            loader.set_watermark(loader.state_key(facility, plan_out["table_set"]),
                                 plan_out["started_at"])

        return {
            "failed": failed,
            "rows": sum(r["rows"] for r in done),
            "flatten_conf": {
                "facilities": plan_out["facilities"],
                "tables": sorted({r["table"].lower() for r in done if r["status"] == "loaded"}),
            },
        }

    @task.short_circuit
    def should_flatten(summary: dict) -> bool:
        p = get_current_context()["params"]
        return bool(p["trigger_flatten"] and not p["dry_run"] and summary["flatten_conf"]["tables"])

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def check(summary: dict) -> None:
        if summary["failed"]:
            raise AirflowFailException(f"{len(summary['failed'])} table(s) failed: {summary['failed']}")
        log.info("All tables succeeded · %d rows", summary["rows"])

    trigger_flatten = TriggerDagRunOperator(
        task_id="trigger_flatten",
        trigger_dag_id=FLATTEN_DAG_ID,
        conf="{{ ti.xcom_pull(task_ids='finalize')['flatten_conf'] }}",
        wait_for_completion=False,
    )

    planned = plan()
    loaded = extract_load.expand(job=jobs_of(planned))
    summary = finalize(planned, loaded)
    should_flatten(summary) >> trigger_flatten
    check(summary)


v2_facility_to_snowflake()
