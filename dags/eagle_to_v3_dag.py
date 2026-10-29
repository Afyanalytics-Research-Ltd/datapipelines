# dags/eagle_to_v3_dag.py
"""
Eagle Nursing Home (Patcom HMS) → V3 through eagle_to_v3.py (Airflow driver; same code as the command line). Reads
the Snowflake mirror HOSPITALS.EAGLE_NURSING_HOME_HISTORICAL_DATA (load_eagle_to_snowflake.py) — nothing on the
client's server.

`target`: eagle = migrate org 13 / facility 14; eagle_prod = production org 199 / facility 196 (AFYA_EAGLE_* /
AFYA_EAGLE_PROD_* in the server's .env). Trigger the DAG once per target; the two can run at the same time.

`base` runs first and alone (patients, visits, catalogue, products, admissions … — what everything else links to),
then four groups side by side, each one step after another:
  clinical   vitals → doctor_notes → diagnoses
  orders     investigations → lab_results (1.5M result lines)
  pharmacy   stock → prescriptions (1.1M)
  money      payments → payment_details → invoices → suppliers → appointments
`steps` (when not empty) runs just those eagle_to_v3 steps, in order, instead. `patients` (Patcom codes) limits a run
to those patients — the canary.

Server .env needs AFYA_EAGLE_* / AFYA_EAGLE_PROD_* (account, URL template, organization, facility) and
AFYA_EAGLE_BRANCH_FACILITIES / AFYA_EAGLE_PROD_BRANCH_FACILITIES (Patcom branch → V3 facility, e.g. '001:15,002:16').
The account must have access to every branch facility: rows are written through one login per facility, with
X-Facility-Id (V3 ignores facility_id in the payload).

With only_missing (default) every step posts only rows whose uuid V3 doesn't have yet, so a run resumes where the
last one stopped. Rows are matched by stable uuid5s, so nothing is duplicated either way.
"""
from __future__ import annotations

import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "eagle_to_v3"
BASE = ["users", "user_profiles", "insurance", "patients", "patient_insurance", "visits", "catalogue", "procedure_prices",
        "stores", "products", "wards", "admissions", "ward_stays", "discharges", "inpatient_notes"]
GROUPS = {
    "clinical": ["vitals", "doctor_notes", "diagnoses"],
    "orders": ["investigations", "lab_results"],
    "pharmacy": ["stock", "prescriptions"],
    "money": ["payments", "payment_details", "invoices", "suppliers", "appointments"],
}
TARGETS = ["eagle", "eagle_prod"]


def step_names(raw) -> list[str]:
    """The trigger form's array box takes one item per line; a pasted '["a", "b"]' or 'a, b' arrives as one item."""
    items = raw if isinstance(raw, list) else [raw]
    return [s for item in items for s in re.split(r"[\s,\[\]\"']+", str(item)) if s]


def run_steps(steps: list[str], *, target: str, only_missing: bool, dry_run: bool, workers: int, limit: int,
              patients: list[str]) -> dict:
    """One eagle_to_v3 Run, the steps in order; an errored step fails the task."""
    use_pipelines_dir([target])
    import eagle_to_v3 as E
    unknown = [s for s in steps if s not in E.STEPS]
    if unknown:
        raise AirflowFailException(f"unknown steps {unknown}; valid: {list(E.STEPS)}")
    run = E.Run(target, dry_run, limit, workers, only_missing)
    run.only_patients = set(patients) or None
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
    description="Eagle Nursing Home (Patcom) → V3 migrate org 13 / production org 199 — resumable (only_missing)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=2,
    tags=["v3", "migration", "eagle"],
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=5)},
    params={
        "target": Param("eagle", type="string", enum=TARGETS, title="Target",
                        description="eagle = migrate org 13 / facility 14; eagle_prod = production org 199 / facility 196."),
        "steps": Param([], type="array", items={"type": "string"}, title="Steps (instead of base + groups)",
                       description="Run just these eagle_to_v3 steps, in order. Empty = base, then the groups."),
        "base": Param(True, type="boolean", title="Run the base steps first"),
        "groups": Param(list(GROUPS), type="array", items={"type": "string", "enum": list(GROUPS)}, title="Groups"),
        "only_missing": Param(True, type="boolean", title="Only rows V3 doesn't have yet"),
        "workers": Param(32, type="integer", minimum=1, maximum=48, title="Parallel V3 posts per group",
                         description="32 ran ~4,600 rows/min on migrate with no refusals (12: ~1,770)."),
        "patients": Param([], type="array", items={"type": "string"}, title="Canary patients (Patcom codes)",
                          description="Only these patients and their visits, orders and money — e.g. ENH-097984. "
                                      "Empty = everyone."),
        "limit": Param(0, type="integer", minimum=0, title="At most N rows per step (0 = all; 3 = canary)"),
        "dry_run": Param(False, type="boolean", title="Dry run", description="On: read and count only."),
    },
)
def eagle_to_v3():

    def args() -> dict:
        p = get_current_context()["params"]
        return {"target": p["target"], "only_missing": bool(p["only_missing"]), "dry_run": bool(p["dry_run"]),
                "workers": int(p["workers"]), "limit": int(p["limit"]), "patients": step_names(p["patients"])}

    @task(execution_timeout=timedelta(hours=20))
    def chosen_steps() -> dict:
        p = get_current_context()["params"]
        if not p["steps"]:
            log.info("no steps listed — base + groups run")
            return {}
        return run_steps(step_names(p["steps"]), **args())

    @task(execution_timeout=timedelta(hours=12))
    def base(_) -> dict:
        p = get_current_context()["params"]
        if p["steps"] or not p["base"]:
            log.info("base skipped")
            return {}
        return run_steps(BASE, **args())

    def group_task(name: str):
        @task(task_id=name, execution_timeout=timedelta(hours=20))
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
        failed = [t.task_id for t in get_current_context()["dag_run"].get_task_instances() if t.state == "failed"]
        if failed:
            raise AirflowFailException(f"tasks that failed (re-run; only_missing resumes them): {failed}")

    c = chosen_steps()
    b = base(c)
    summary(c, b, *[group_task(n)(b) for n in GROUPS])


eagle_to_v3()
