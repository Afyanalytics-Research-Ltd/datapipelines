# dags/from_json_mappings_to_snowflake_dag.py
"""
Afya Extraction tool → Snowflake <NAME>_RAW.EVENTS_RAW in the V3 data structure
(Airflow driver for from_json_mappings_to_snowflake.py)

The only input is a connection's mappings export (JSON in the repo root, e.g.
silverwood_data_mappings.json). The connection's name is read from the
extraction tool and becomes the schemas <NAME>_RAW / <NAME>_CLEAN.

  plan          read the mappings, resolve the name, create the schemas
  extract_load  one mapped task per source table: extract → map to V3 → load
                (each V3 table is replaced, so re-runs don't duplicate)
  report        rows per V3 table + mapped-field coverage; fails if any table failed

Trigger form options:
  mappings  mappings JSON file name (repo root) or absolute path
  tables    only these source tables (one per line). Empty = all in the file.
  dry_run   extract and map, write nothing

Credentials: Variables AFYA_EXTRACTION_USERNAME / _PASSWORD (/ _BASE_URL),
else Connection afya_extraction (host = API base URL, login, password).
Snowflake / AWS as for the other DAGs. Page workers per table: env
EXTRACTION_PAGE_WORKERS (default 4).
"""
from __future__ import annotations

import logging
import sys
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import PIPELINES_DIR, clean_list, int_env, use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "from_json_mappings_to_snowflake"


def _mappings_path(value: str) -> Path:
    path = Path(value.strip())
    path = path if path.is_absolute() else PIPELINES_DIR / path
    if not path.is_file():
        raise AirflowFailException(f"Mappings file not found: {path}")
    return path


@dag(
    dag_id=DAG_ID,
    description="Afya Extraction → Snowflake <NAME>_RAW.EVENTS_RAW (V3 structure), driven by a mappings JSON",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["snowflake", "extraction", "ingest"],
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=2)},
    params={
        "mappings": Param("silverwood_data_mappings.json", type="string", title="Mappings JSON",
                          description="File in the repo root, or an absolute path."),
        "tables": Param([], type="array", items={"type": "string"}, title="Source tables",
                        description="Only these source tables (one per line). Empty = all in the file."),
        "dry_run": Param(False, type="boolean", title="Dry run", description="Extract and map; write nothing."),
    },
)
def from_json_mappings_to_snowflake():

    @task
    def plan() -> list[dict]:
        use_pipelines_dir()
        import from_json_mappings_to_snowflake as fj

        p = get_current_context()["params"]
        path = _mappings_path(p["mappings"])
        spec = fj.Mappings.load(path)
        wanted = clean_list(p["tables"])
        unknown = sorted(set(wanted) - set(spec.source_tables))
        if unknown:
            raise AirflowFailException(f"Not in {path.name}: {unknown}")
        tables = [t for t in spec.source_tables if not wanted or t in wanted]

        name = fj.resolve_name(spec, fj.ExtractionClient(spec.connection_id))
        wh = fj.Warehouse(name)
        if not p["dry_run"]:
            with fj.SnowflakeClient() as sf:
                wh.ensure(sf)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        log.info("connection %s (%s) → %s / %s · %d source tables · run %s%s", spec.connection_id, name,
                 wh.raw, wh.clean, len(tables), run_id, " (dry run)" if p["dry_run"] else "")
        return [{"mappings": str(path), "name": name, "run_id": run_id, "source_table": t,
                 "dry_run": bool(p["dry_run"])} for t in tables]

    @task(max_active_tis_per_dagrun=int_env("EXTRACTION_PARALLEL_TABLES", 6))
    def extract_load(item: dict) -> dict:
        use_pipelines_dir()
        import from_json_mappings_to_snowflake as fj

        spec = fj.Mappings.load(Path(item["mappings"]))
        client = fj.ExtractionClient(spec.connection_id, page_workers=int_env("EXTRACTION_PAGE_WORKERS", 4))
        result = fj.process_table(spec, fj.Warehouse(item["name"]), client, item["source_table"],
                                  run_id=item["run_id"], dry_run=item["dry_run"])
        return asdict(result)

    @task
    def report(results: list[dict]) -> None:
        use_pipelines_dir()
        import from_json_mappings_to_snowflake as fj

        results = [fj.TableResult(**r) for r in results]
        fj.print_report(results)
        failed = [r.source_table for r in results if r.status == "failed"]
        if failed:
            raise AirflowFailException(f"{len(failed)} source table(s) failed: {failed}")

    report(extract_load.expand(item=plan()))


from_json_mappings_to_snowflake()
