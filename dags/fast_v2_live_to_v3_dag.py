# dags/fast_v2_live_to_v3_dag.py
"""
Post the V2 rows V3 is missing — read from the LIVE V2 API — through fast_v2_live_to_v3.py
(Airflow driver; same code as the command line).

Per table: read V2 (the Snowflake snapshot + the live API's newer rows, or with `live` the whole table
from the API), transform it exactly as snowflake_to_v3_migration would, drop every row V3 already holds
(by uuid, by the migration's id map, and by content — V3 holds many kisumu_v3 rows under uuids the
migration didn't make), and post the rest many at a time. Parents missing from V3 hold a row back; a
missing Snowflake CLEAN view is built from RAW first.

Lessons of 2026-10-15 built in:
  * ONE process for all tables, one table after another — several loaders at once ran the WSL VM out
    of memory (wsl --shutdown) and tripped the V2 API's rate limit (429s)
  * `audit`: a reconcile summary CSV — a table may post at most its V2−V3 gap (+5 %), so a key that
    fails to recognise rows V3 already has can't flood V3 with duplicates
  * the report (per table: live V2 rows, already in V3, held + why, refused + why, posted) is written
    to output/fast_v2_live_to_v3/<facility>_<run>.json, and with `audit` the reasons go into the CSV's
    `why` column (output/org4_reload_20261015/write_why.py)

Trigger form:
  facility      V2 facility key (its V3 org/facility come from FACILITY_V3_CONFIG / afya_v3_<facility>)
  tables        CLEAN table names, one per line, in load order (parents first)
  live          read every table whole from the live V2 API (slow: ~2 pages of 100 rows a second)
  audit         path of a reconcile summary CSV (relative to the pipelines dir), optional
  workers       parallel V3 posts (default 24); page_workers: parallel V2 API pages (default 8)
  dry_run       read, transform and count only
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import clean_list, facility_keys, use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "fast_v2_live_to_v3"


@dag(
    dag_id=DAG_ID,
    description="Post the V2 rows V3 lacks (live V2 API) — duplicate-safe, one process, reasons per table",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration", "v2-live"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "facility": Param("kisumu_v3", type="string", title="Facility", examples=facility_keys()),
        "tables": Param([], type="array", items={"type": "string"}, title="Tables",
                        description="CLEAN table names, one per line, parents first (e.g. visits before "
                                    "evaluation_visit_destinations)."),
        "live": Param(False, type="boolean", title="Read whole tables from the live V2 API",
                      description="Off: the Snowflake snapshot + the API's rows newer than it (fast). On: every "
                                  "row from the API (slow, but only what V2 has today)."),
        "audit": Param("", type="string", title="Reconcile summary CSV (optional)",
                       description="Bounds each table's posts by its V2−V3 gap and gets its `why` column filled."),
        "workers": Param(24, type="integer", minimum=1, maximum=64, title="Parallel V3 posts"),
        "page_workers": Param(8, type="integer", minimum=1, maximum=32, title="Parallel V2 API pages"),
        "dry_run": Param(True, type="boolean", title="Dry run", description="On: count only, post nothing."),
        "corrections": Param(True, type="boolean", title="Run the org corrections after",
                             description="v3_org_corrections: visit destinations, discharges, destination "
                                         "categories (skipped on a dry run)."),
    },
)
def fast_v2_live_to_v3():

    @task(execution_timeout=timedelta(hours=12))
    def load() -> list[dict]:
        ctx = get_current_context()
        p = ctx["params"]
        facility = p["facility"].strip()
        tables = clean_list(p["tables"])
        if not tables:
            raise AirflowFailException("Name the tables to load (one per line).")
        use_pipelines_dir([facility])
        import fast_v2_live_to_v3 as F
        pipelines = Path(F.__file__).resolve().parent
        gaps = {}
        audit = Path(pipelines) / p["audit"] if p.get("audit") else None
        if audit:
            from export_missing_records import parse_audit
            gaps = {j["v2_table"]: j["gap"] for j in parse_audit(audit.read_text())}
        out = Path(pipelines) / "output" / "fast_v2_live_to_v3"
        out.mkdir(parents=True, exist_ok=True)
        report = out / f"{facility}_{ctx['run_id'].replace(':', '').replace('+', '')[:40]}.json"
        loader = F.Loader(facility, int(p["workers"]), int(p["page_workers"]), bool(p["dry_run"]), 0,
                          live=bool(p["live"]), gaps=gaps, report_path=report)
        loader.run(tables)
        if audit and not p["dry_run"]:
            # the CSV's `why` column from this run's report (and any earlier ones next to the CSV)
            import shutil, subprocess
            shutil.copy2(report, audit.parent / f"report_dag_{report.stem}.json")
            subprocess.run([sys.executable, str(audit.parent / "write_why.py"), str(audit)], check=False)
        log.info("report: %s", report)
        return loader.report

    @task
    def summary(report: list[dict]) -> None:
        posted = sum(r.get("posted") or 0 for r in report)
        failed = sum(r.get("failed") or 0 for r in report)
        for r in report:
            log.info("%-42s %s", r["table"], json.dumps({k: r.get(k) for k in (
                "live_v2", "in_v3_by_uuid", "in_v3_by_content", "held", "to_post", "posted", "failed",
                "duplicate_key", "status")}, default=str))
        log.info("%d table(s): %d posted, %d refused by V3", len(report), posted, failed)
        errors = [r["table"] for r in report if str(r.get("status") or "").startswith("error")]
        if errors:
            raise AirflowFailException(f"tables that errored (re-run them): {errors}")

    @task(execution_timeout=timedelta(hours=6))
    def corrections(_) -> None:
        p = get_current_context()["params"]
        if p["dry_run"] or not p.get("corrections", True):
            log.info("corrections skipped (dry run / turned off)")
            return
        from v3_org_corrections_dag import run_corrections
        run_corrections(p["facility"].strip(), source=None, facility=p["facility"].strip(), dry_run=False,
                        steps=["destination_categories", "eval_visit_destinations", "eval_consumables", "admission_discharges"])

    loaded = load()
    summary(loaded)
    corrections(loaded)


fast_v2_live_to_v3()
