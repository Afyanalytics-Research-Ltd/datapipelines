# dags/medhanit_to_v3_dag.py
"""
Medhanit Healthcare (Collabmed V1, https://medhanit.collabmed.net) → V3: target medhanit = migrate org 16 / facility 20,
target medhanit_prod = production org 71 / facility 69.

medhanit_prod already holds an earlier load of this V1 (patients, visits, notes, orders, results, payments up to
2026-10-06, under uuids of its own), so it always runs with --adopt: each V1 row is linked to the row already there
(patient MH-<no> / MH-V2-<id>, visit V2-<id>, order by visit + time + price, receipt) and left as is; only what is
missing is posted. The org's own catalogue, prices and inventory are kept — only tests V1 ordered that the catalogue
has no test of that name for are added, and V1 products / stock are not loaded there.

  extract   medhanit_v1_to_snowflake.py: V1's read-only data gateway → Snowflake HOSPITALS.MEDHANIT_V1 (resumes; the
            gateway allows 60 requests a minute). Fails on reception_patients / reception_patients_nok while V1's
            gateway can't decrypt them — the other tables are still extracted.
  base      medhanit_to_v3.py steps everything else links to: users, insurance, patients, catalogue, lab templates, visits.
  groups    side by side, each one step after another:
              clinical   visit_destinations → vitals → doctor_notes → nursing_notes
              orders     investigations → lab_results
              money      payments → payment_details → invoices
              stock      products → stock
`steps` (when not empty) runs just those medhanit_to_v3 steps, in order, instead of base + groups. `patients` (V1
patient ids) limits a run to those patients — the canary. `limit` posts at most N rows per model.

Settings: the server .env (or Airflow) needs
  AFYA_MEDHANIT_USERNAME / _PASSWORD / _URL_TEMPLATE / _ORGANIZATION_ID / _FACILITY_ID  (or Connection afya_v3_medhanit:
      login/password, Extra {"organization_id": 16, "facility_id": 20, "url_template": "https://{service}migrate.afyaanalytics.ai/api/"})
  AFYA_MEDHANIT_PROD_*  (or Connection afya_v3_medhanit_prod: Extra {"organization_id": 71, "facility_id": 69,
      "url_template": "https://{service}.afyaanalytics.ai/api/"})
  MEDHANIT_V1_BASE_URL / _USERNAME / _PASSWORD  (or Connection medhanit_v1: host, login, password)
Rows are matched by stable uuid5s, so nothing is duplicated: by default every row is posted again and updated in place
(fixing links a limited canary left empty); only_missing posts only rows whose uuid V3 doesn't have yet.
"""
from __future__ import annotations

import logging
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import _connection, use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "medhanit_to_v3"
BASE = ["users", "user_profiles", "insurance", "patients", "patient_insurance", "catalogue", "procedure_prices",
        "lab_templates", "visits"]
GROUPS = {
    "clinical": ["visit_destinations", "vitals", "doctor_notes", "nursing_notes"],
    "orders": ["investigations", "lab_results"],
    "money": ["payments", "payment_details", "invoices"],
    "stock": ["products", "stock"],
}
TARGETS = ["medhanit", "medhanit_prod"]


def step_names(raw) -> list[str]:
    """The trigger form's array box takes one item per line; a pasted '["a", "b"]' or 'a, b' arrives as one item."""
    items = raw if isinstance(raw, list) else [raw]
    return [s for item in items for s in re.split(r"[\s,\[\]\"']+", str(item)) if s]


def v1_settings() -> None:
    """MEDHANIT_V1_* from the .env, else from the Airflow Connection medhanit_v1."""
    if (os.environ.get("MEDHANIT_V1_USERNAME") or "").strip():
        return
    conn = _connection("medhanit_v1")
    if conn is None:
        raise AirflowFailException("set MEDHANIT_V1_BASE_URL / _USERNAME / _PASSWORD in the server .env, or add the "
                                   "Airflow Connection medhanit_v1 (host, login, password)")
    host = (conn.host or "").strip()
    os.environ["MEDHANIT_V1_BASE_URL"] = host if host.startswith("http") else f"https://{host}"
    os.environ["MEDHANIT_V1_USERNAME"] = conn.login or ""
    os.environ["MEDHANIT_V1_PASSWORD"] = conn.password or ""


def run_steps(steps: list[str], *, target: str, only_missing: bool, dry_run: bool, workers: int, limit: int,
              patients: list[str]) -> dict:
    """One medhanit_to_v3 Run, the steps in order; an errored step fails the task."""
    use_pipelines_dir([target])
    import medhanit_to_v3 as M
    unknown = [s for s in steps if s not in M.STEPS]
    if unknown:
        raise AirflowFailException(f"unknown steps {unknown}; valid: {list(M.STEPS)}")
    run = M.Run(target, dry_run, limit, workers, only_missing, adopt=target in M.ADOPT_TARGETS)   # prod: fill the gaps only
    run.only_patients = {str(M.as_int(p)) for p in patients} or None
    out = {}
    for s in steps:
        log.info("── %s", s)
        try:
            out[s] = getattr(run, s)()
        except Exception as e:
            log.exception("%s failed", s)
            raise AirflowFailException(f"{s}: {type(e).__name__}: {str(e)[:300]}") from e
        log.info("%s: %s", s, out[s])
        for model, rows in run.failures.items():
            if rows:
                log.warning("%s: %d refused, e.g. %s", model, len(rows), rows[:3])
        run.failures.clear()
    return out


@dag(
    dag_id=DAG_ID,
    description="Medhanit Healthcare (Collabmed V1) → V3 (migrate org 16 / prod org 71, gaps only) — resumable",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration", "medhanit"],
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=5)},   # a retry resumes (only_missing)
    params={
        "target": Param("medhanit", type="string", enum=TARGETS, title="Target",
                        description="medhanit = migrate org 16 / facility 20 (AFYA_MEDHANIT_*)."),
        "extract": Param(True, type="boolean", title="Extract from V1 first",
                         description="Read the V1 gateway into Snowflake (resumes; only new rows) before loading."),
        "full_extract": Param(False, type="boolean", title="Re-read every V1 table from the start"),
        "extract_tables": Param([], type="array", items={"type": "string"}, title="Only these V1 tables (extract)",
                                description="Empty = every allowlisted table. e.g. reception_patients, reception_patients_nok"),
        "steps": Param([], type="array", items={"type": "string"}, title="Steps (instead of base + groups)",
                       description="Run just these medhanit_to_v3 steps, in order. Empty = base, then the groups."),
        "base": Param(True, type="boolean", title="Run the base steps"),
        "groups": Param(list(GROUPS), type="array", items={"type": "string", "enum": list(GROUPS)}, title="Groups"),
        # off by default: Medhanit is small (~15k rows), and re-posting updates the rows a canary left half-linked
        "only_missing": Param(False, type="boolean", title="Only rows V3 doesn't have yet",
                              description="Off: every row is posted again and updated in place (by uuid)."),
        "workers": Param(12, type="integer", minimum=1, maximum=48, title="Parallel V3 posts per group"),
        "patients": Param([], type="array", items={"type": "string"}, title="Canary patients (V1 patient ids)",
                          description="Only these patients and their visits, orders and money — e.g. 62709. Empty = everyone."),
        "limit": Param(0, type="integer", minimum=0, title="At most N rows per model (0 = all; 3 = canary)"),
        "dry_run": Param(False, type="boolean", title="Dry run", description="On: read and count only (extract still runs)."),
    },
)
def medhanit_to_v3():

    def args() -> dict:
        p = get_current_context()["params"]
        return {"target": p["target"], "only_missing": bool(p["only_missing"]), "dry_run": bool(p["dry_run"]),
                "workers": int(p["workers"]), "limit": int(p["limit"]), "patients": step_names(p["patients"])}

    @task(execution_timeout=timedelta(hours=8))
    def extract() -> dict:
        p = get_current_context()["params"]
        if not p["extract"]:
            log.info("extract skipped")
            return {}
        use_pipelines_dir([])
        v1_settings()
        import medhanit_v1_to_snowflake as X
        argv = (["--full"] if p["full_extract"] else []) + (["--tables", *step_names(p["extract_tables"])]
                                                           if step_names(p["extract_tables"]) else [])
        code = X.main(argv)
        if code:
            # patients can't be decrypted until V1's gateway is fixed: the rest is extracted and loads anyway
            log.warning("some V1 tables failed to extract (see above) — the load runs with what is in Snowflake")
        return {"exit_code": code}

    @task(execution_timeout=timedelta(hours=8))
    def chosen_steps(_) -> dict:
        p = get_current_context()["params"]
        if not p["steps"]:
            log.info("no steps listed — base + groups run")
            return {}
        return run_steps(step_names(p["steps"]), **args())

    @task(execution_timeout=timedelta(hours=8))
    def base(_) -> dict:
        p = get_current_context()["params"]
        if p["steps"] or not p["base"]:
            log.info("base skipped")
            return {}
        return run_steps(BASE, **args())

    def group_task(name: str):
        @task(task_id=name, execution_timeout=timedelta(hours=8))
        def _run(_) -> dict:
            p = get_current_context()["params"]
            if p["steps"] or name not in p["groups"]:
                log.info("%s not selected", name)
                return {}
            return run_steps(GROUPS[name], **args())
        return _run

    @task(trigger_rule="all_done")
    def summary(*results) -> None:
        bad = {}
        for r in results:
            for step, res in (r or {}).items():
                log.info("%-20s %s", step, res)
                if isinstance(res, dict) and (res.get("failed") or res.get("error")):
                    bad[step] = res.get("failed") or res.get("error")
        if bad:
            log.warning("refused / errored: %s", bad)
        # a failed / upstream_failed task has no return XCom, so it arrives here as None
        failed = [n for n, r in zip(["extract", "chosen_steps", "base", *GROUPS], results) if r is None]
        if failed:
            raise AirflowFailException(f"tasks that failed (re-run; only_missing resumes them): {failed}")

    x = extract()
    c = chosen_steps(x)
    b = base(c)
    summary(x, c, b, *[group_task(n)(b) for n in GROUPS])


medhanit_to_v3()
