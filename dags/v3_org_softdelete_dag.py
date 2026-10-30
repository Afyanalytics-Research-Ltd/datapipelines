# dags/v3_org_softdelete_dag.py
"""
V3 org soft delete (Airflow driver for v3_org_softdelete.py): hide listed rows of one V3 org the way a
DBA's <db>_softdelete.sql would — deleted_at = now, finance patient_accounts status = 'closed' —
through the model gateway. Backs up first (every listed id must be the org's), skips rows already
soft-deleted (safe to re-run / resume), writes children first, re-reads to verify.

  ids       a JSON id list in the repo (mapping/softdelete/org168_20261019.json), or a directory / zip
            of the DBA's *_softdelete.sql files
  rollback  a backup CSV of an earlier run (output/v3_org_softdelete/<target>-<time>-backup.csv):
            puts every row back as it was

Report + backup: output/v3_org_softdelete/<target>-<time>.json / -backup.csv
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:   # older Airflow 3.0.x
    from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "v3_org_softdelete"


@dag(
    dag_id=DAG_ID,
    description="Soft-delete listed rows of a V3 org (deleted_at / status closed), with backup and rollback",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration", "softdelete"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "target": Param("org168", type="string", title="Target", description="The V3 org (Connection afya_v3_<key>)."),
        "ids": Param("mapping/softdelete/org168_20261019.json", type="string", title="Id list",
                     description="Repo path: JSON {\"<service>.<table>\": [ids]}, or a directory / zip of "
                                 "*_softdelete.sql. Ignored when rollback is set."),
        "tables": Param([], type="array", items={"type": "string"}, title="Only these tables",
                        description="e.g. finance.invoices. Empty: every table in the list."),
        "deleted_at": Param("", type="string", title="deleted_at",
                            description="Wall time to stamp (Africa/Nairobi). Empty: now."),
        "rollback": Param("", type="string", title="Rollback from backup CSV",
                          description="output/v3_org_softdelete/<target>-<time>-backup.csv. Empty: soft delete."),
        "workers": Param(12, type="integer", minimum=1, maximum=48, title="Parallel V3 writes"),
        "dry_run": Param(True, type="boolean", title="Dry run",
                         description="On: back up and count only, write nothing."),
    },
)
def v3_org_softdelete():

    @task(execution_timeout=timedelta(hours=12))
    def softdelete() -> str:
        p = get_current_context()["params"]
        target = p["target"].strip()
        use_pipelines_dir([target])
        import v3_org_softdelete as sd
        rep = sd.run(target, ids_path=(p.get("ids") or "").strip() or None,
                     rollback_csv=(p.get("rollback") or "").strip() or None, dry_run=bool(p["dry_run"]),
                     workers=int(p["workers"]), deleted_at=(p.get("deleted_at") or "").strip() or None,
                     tables=[t.strip() for t in (p.get("tables") or []) if t.strip()] or None)
        for t, r in rep["tables"].items():
            log.info("%-28s %s", t, json.dumps(r, default=str)[:600])
        bad = {t: r.get("error") or f"{r['failed']} failed" for t, r in rep["tables"].items()
               if r.get("error") or r.get("failed") or r.get("still_not_deleted")}
        if bad:
            raise AirflowFailException(f"not finished (re-run: done rows are skipped): {bad} — report {rep['report']}")
        return rep["report"]

    softdelete()


v3_org_softdelete()
