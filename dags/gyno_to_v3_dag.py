# dags/gyno_to_v3_dag.py
"""
Gyno Solutions HMS → V3 migrate platform (org 6 / facility 8, target key "gyno") through gyno_to_v3.py
(Airflow driver; same code as the command line). Reads the Snowflake mirror
HOSPITALS.GYNOSOLUTIONS_HISTORICAL_DATA (load_gyno_to_snowflake.py) — nothing on the client's server.

Three groups run side by side, each one step after another (a step's parents must be in V3 first):
  clinical   vitals → doctor_notes → discharges → nurse_notes → anc → investigations → prescriptions
  money      receipts → documents → stock → procurement
  structure  destinations → visits (re-post: clinic = facility) → payment_links
plus `base`, run first and alone when asked for (patients, visits, catalogue, products, payments,
appointments, users, insurance, invoices, extra_notes — all already loaded 2026-10-20).

With only_missing (default) every step posts only rows whose uuid V3 doesn't have yet, so a run resumes
where the last one stopped. Rows are matched by stable uuid5s, so nothing is duplicated either way.
Needs AFYA_GYNO_* in the server's .env. The documents step reads the S3 manifest at `manifest`
(sizes only; the files themselves aren't copied yet).
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "gyno_to_v3"
GROUPS = {
    "clinical": ["vitals", "doctor_notes", "discharges", "nurse_notes", "anc", "investigations", "prescriptions"],
    "money": ["receipts", "documents", "stock", "procurement"],
    "structure": ["destinations", "visits", "payment_links"],
}
BASE = ["patients", "visits", "catalogue", "products", "payments", "appointments", "users", "insurance",
        "invoices", "extra_notes"]


def run_steps(steps: list[str], *, only_missing: bool, dry_run: bool, workers: int, limit: int, manifest: str) -> dict:
    """One gyno_to_v3 Run, the steps in order; a failed step fails the task (the rest of the group waits)."""
    use_pipelines_dir(["gyno"])
    import gyno_to_v3 as G
    run = G.Run(dry_run, limit, workers, only_missing)
    run.manifest = manifest
    out = {}
    for s in steps:
        # the clinic re-post updates every visit, so it never skips the ones V3 already has
        run.only_missing = only_missing and s != "visits"
        log.info("── %s", s)
        out[s] = getattr(run, s)()
        log.info("%s: %s", s, out[s])
        if isinstance(out[s], dict) and out[s].get("failed"):
            log.warning("%s: %s row(s) refused by V3", s, out[s]["failed"])
    return out


@dag(
    dag_id=DAG_ID,
    description="Gyno Solutions → V3 migrate org 6 — resumable (only_missing), three groups in parallel",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration", "gyno"],
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=5)},
    params={
        "groups": Param(list(GROUPS), type="array", items={"type": "string", "enum": list(GROUPS)}, title="Groups",
                        description="clinical / money / structure — run side by side."),
        "base": Param(False, type="boolean", title="Run the base steps first",
                      description="patients, visits, catalogue, products, payments, … (already loaded; with "
                                  "only_missing a re-run posts only what is missing)."),
        "only_missing": Param(True, type="boolean", title="Only rows V3 doesn't have yet"),
        "workers": Param(12, type="integer", minimum=1, maximum=32, title="Parallel V3 posts per group"),
        "limit": Param(0, type="integer", minimum=0, title="At most N rows per step (0 = all; 3 = canary)"),
        "dry_run": Param(False, type="boolean", title="Dry run", description="On: read and count only."),
        "manifest": Param("/opt/airflow/pipelines/input/gyno/s3_files_manifest.json", type="string",
                          title="S3 files manifest (documents step: file sizes)"),
    },
)
def gyno_to_v3():

    def args() -> dict:
        p = get_current_context()["params"]
        return {"only_missing": bool(p["only_missing"]), "dry_run": bool(p["dry_run"]), "workers": int(p["workers"]),
                "limit": int(p["limit"]), "manifest": p["manifest"]}

    @task(execution_timeout=timedelta(hours=8))
    def base() -> dict:
        p = get_current_context()["params"]
        if not p["base"]:
            log.info("base steps skipped (already loaded)")
            return {}
        return run_steps(BASE, **args())

    def group_task(name: str):
        @task(task_id=name, execution_timeout=timedelta(hours=14))
        def _run(_) -> dict:
            p = get_current_context()["params"]
            if name not in p["groups"]:
                log.info("%s not selected", name)
                return {}
            return run_steps(GROUPS[name], **args())
        return _run

    @task(trigger_rule="all_done")
    def summary(*results) -> None:
        bad = {}
        for r in results:
            for step, res in (r or {}).items():
                if isinstance(res, dict) and (res.get("failed") or res.get("error")):
                    bad[step] = res.get("failed") or res.get("error")
                log.info("%-16s %s", step, res)
        if bad:
            log.warning("refused / errored: %s", bad)
        ctx = get_current_context()
        failed = [t.task_id for t in ctx["dag_run"].get_task_instances() if t.state == "failed"]
        if failed:
            raise AirflowFailException(f"groups that failed (re-run; only_missing resumes them): {failed}")

    b = base()
    results = [group_task(n)(b) for n in GROUPS]
    summary(*results)


gyno_to_v3()
