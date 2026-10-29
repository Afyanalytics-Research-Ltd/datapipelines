# dags/v3_org_corrections_dag.py
"""
V3 org corrections (Airflow driver for v3_org_corrections.py): what a V3 org needs after a V2 → V3 load,
a V3 → V3 copy or a V2 delta — each step compares first and writes only what differs (re-runnable):

  restore_destinations     service destinations the source org has and the target lost / soft-deleted
  destination_categories   associated_procedure_category → the org's own procedure category ids
  eval_visit_destinations  V2 visit destinations → evaluation_visit_destinations (what the frontend reads),
                           each pointed at the org's current destination (relink re-points changed ones)
  eval_consumables         V2 consumables → evaluation consumables (V2 dates kept, products / invoices linked)
  admission_discharges     admissions discharged / admitted as in live V2 (discharge_date = discharged_at),
                           open ward stays so current inpatients show bed and ward
  facility_check           rows on another facility than the org's → an SQL file for the DBA (read-only)

Also run at the end of v2_delta_to_v3, fast_v2_live_to_v3 and v3_to_v3_migration (param `corrections`).
Report: output/v3_org_corrections/<target>-<time>.json
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
from pipelines_common import clean_list, use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "v3_org_corrections"
STEPS = ["restore_destinations", "destination_categories", "eval_visit_destinations", "eval_consumables",
         "eval_investigation_links", "admission_discharges", "facility_check"]


def run_corrections(target: str, *, source: str | None, facility: str, steps: list[str], dry_run: bool,
                    relink: bool = True, workers: int = 12) -> dict:
    """Shared by this DAG and the migration DAGs' final `corrections` task."""
    use_pipelines_dir([facility, target] + ([source] if source else []))
    import v3_org_corrections as oc
    rep = oc.run(target, source=source, facility=facility, steps=steps, dry_run=dry_run, relink=relink,
                 workers=workers)
    for s, r in rep["steps"].items():
        log.info("%-24s %s", s, json.dumps(r, default=str)[:600])
    errors = {s: r["error"] for s, r in rep["steps"].items() if "error" in r}
    if errors:
        raise AirflowFailException(f"corrections that failed (re-run them): {errors}")
    return rep


@dag(
    dag_id=DAG_ID,
    description="Corrections a V3 org needs after a load / copy / delta (destinations, visit destinations, "
                "discharges, facility)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration", "corrections"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "target": Param("org284", type="string", title="Target", description="The V3 org (Connection afya_v3_<key>)."),
        "source": Param("kisumu_v3", type="string", title="Source",
                        description="The org it was copied from (V3 → V3) — for restore_destinations. Empty: none."),
        "facility": Param("kisumu_v3", type="string", title="V2 facility",
                          description="The V2 facility key (its live API and Snowflake snapshot)."),
        "steps": Param(STEPS[:5], type="array", items={"type": "string", "enum": STEPS}, title="Steps",
                       description="facility_check is read-only and slow (reads every table): add it when needed."),
        "relink": Param(True, type="boolean", title="Re-link visit destinations to the current destinations"),
        "workers": Param(12, type="integer", minimum=1, maximum=48, title="Parallel V3 writes"),
        "dry_run": Param(True, type="boolean", title="Dry run", description="On: count only, write nothing."),
    },
)
def v3_org_corrections():

    @task(execution_timeout=timedelta(hours=6))
    def corrections() -> str:
        p = get_current_context()["params"]
        source = (p.get("source") or "").strip() or None
        rep = run_corrections(p["target"].strip(), source=source, facility=p["facility"].strip(),
                              steps=clean_list(p["steps"]) or STEPS[:5], dry_run=bool(p["dry_run"]),
                              relink=bool(p["relink"]), workers=int(p["workers"]))
        return rep["report"]

    corrections()


v3_org_corrections()
