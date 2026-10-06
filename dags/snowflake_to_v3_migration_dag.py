# dags/snowflake_to_v3_migration_dag.py
"""
Snowflake {FACILITY}_CLEAN.<table> → V3 gateway
(Airflow driver for snowflake_to_v3_migration.py)

One mapped task per facility. Facilities run one at a time — on purpose:
every run reads and rewrites the same state files (.migration_record_progress.json,
.migration_id_map.json, ...), and two processes writing them at once lose
each other's updates (or truncate the file). Speed comes from inside the
task instead: `workers` tables in parallel per dependency tier, and
`record_workers` parallel POSTs per table.

Don't run the CLI against the same repo while this DAG is running, for
the same reason.

Trigger form options:
  facilities      one or more facility keys.
  tables          only these Snowflake source tables (one per line), e.g.
                  patients / visits. Empty = every mapped, insertable table,
                  in tier order (patients before visits, ...).
  mode            migrate     — POST to V3
                  dry_run     — fetch + transform only, nothing posted
                  list_tables — log each table's tier and V3 target, then stop
  workers         tables in parallel within a tier.
  record_workers  parallel POSTs within one table (RECORD_WORKERS).
  allow_history_tables
                  old-system-history tables (reception_patients,
                  evaluation_prescriptions, ...) are refused unless this is
                  ticked: they're archive copies for old_system_history, and
                  loading them table-by-table would e.g. insert every
                  patient a second time.

The destination org/facility come from FACILITY_V3_CONFIG and the V3
account in AFYA_<FACILITY>_USERNAME/PASSWORD (falls back to AFYA_USERNAME);
the run stops before posting anything if they disagree.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import (clean_list, facility_keys, old_system_history_tables,
                              use_pipelines_dir)

log = logging.getLogger(__name__)

DAG_ID = "snowflake_to_v3_migration"
FACILITIES = facility_keys()


@dag(
    dag_id=DAG_ID,
    description="Snowflake CLEAN views → V3 gateway, one facility at a time",
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
        "mode": Param("migrate", type="string", enum=["migrate", "dry_run", "list_tables"], title="Mode"),
        "workers": Param(8, type="integer", minimum=1, maximum=32, title="Tables in parallel per tier"),
        "record_workers": Param(8, type="integer", minimum=1, maximum=32,
                                title="Parallel POSTs per table",
                                description="Lower it if V3 starts returning 504s."),
        "allow_history_tables": Param(False, type="boolean", title="Allow old-system-history tables"),
    },
)
def snowflake_to_v3_migration():

    @task
    def plan() -> list[str]:
        p = get_current_context()["params"]
        facilities = clean_list(p["facilities"])
        tables = set(clean_list(p["tables"]))
        if not facilities:
            raise AirflowFailException("Pick at least one facility.")
        unknown = [f for f in facilities if f not in FACILITIES]
        if unknown:
            raise AirflowFailException(f"Unknown facilities {unknown}; known: {FACILITIES}")
        history = sorted(tables & set(old_system_history_tables()))
        if history and not p["allow_history_tables"]:
            raise AirflowFailException(
                f"{history} are old-system-history tables; tick allow_history_tables to load "
                f"them table-by-table anyway.")
        return facilities

    @task(
        map_index_template="{{ map_label }}",
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(hours=24),
    )
    def migrate(facility: str) -> dict:
        ctx = get_current_context()
        ctx["map_label"] = facility
        p = ctx["params"]
        record_workers = str(int(p["record_workers"]))
        os.environ["RECORD_WORKERS"] = record_workers
        use_pipelines_dir()
        import snowflake_to_v3_migration as s2v3
        import v2_to_v3_api_migration as v2v3

        # Read at import time by both modules — set them in case either was
        # already imported in this process.
        s2v3.RECORD_WORKERS = v2v3.RECORD_WORKERS = int(record_workers)

        if p["mode"] == "list_tables":
            s2v3.list_tables(facility)
            return {"facility": facility, "failed": []}

        tables = clean_list(p["tables"]) or None
        try:
            failed = s2v3.run_migration(facility, tables, workers=int(p["workers"]),
                                        dry_run=p["mode"] == "dry_run")
        except SystemExit as e:
            # run_migration exits on a setup error (V3 login/org mismatch, no
            # Snowflake tables) after logging the reason above.
            raise AirflowFailException(f"[{facility}] migration stopped during setup (exit {e.code}); see log.")
        if failed:
            raise AirflowFailException(f"[{facility}] {len(failed)} table(s) failed: {failed}")
        return {"facility": facility, "failed": []}

    migrate.expand(facility=plan())


snowflake_to_v3_migration()
