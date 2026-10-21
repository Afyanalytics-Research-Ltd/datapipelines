# dags/active_visits_to_v3_dag.py
"""
Active V2 visits → V3 (Airflow driver for active_visits_to_v3.py): the visits still open in V2 at cut-over
(Kisumu → production org 284), each with its patient and insurance, its bill (investigations, prescriptions,
dispensing, ward stays, theatre, consumables), invoices and receipts — updated where the target has them,
inserted where it doesn't. Then the org corrections for the same visits (evaluation visit destinations,
consumables, admission states).

Params:
  rule     which visits count as active (active_visits_to_v3.RULES): default = an open admission, or created
           in the last `days` days with a destination not checked out.
  days     the recent rules' window (V2 local time).
  dry_run  read, transform and decide — post nothing (the default here: look at the report first).

Report: output/active_visits/<source>-to-<target>-<rule>-<time>.csv (+ .json: rule counts, visit ids,
corrections, tables not sent and why).

Needs what v2_delta_to_v3 needs: Connections afya_v3_<source> / afya_v3_<target>, the V2 load's state and the
V3 → V3 copy's id maps on this server.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "active_visits_to_v3"
RULES = ["default", "open_admission", "recent", "recent_open_destination", "recent_unbilled"]


@dag(
    dag_id=DAG_ID,
    description="V2 visits still active → the target V3 org with their patient, bill, invoices and receipts",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration", "delta", "visits"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "rule": Param("default", type="string", enum=RULES, title="Active rule",
                      description="default: an open admission, or created in the last N days with a destination "
                                  "not checked out."),
        "days": Param(1, type="integer", minimum=1, maximum=60, title="Days",
                      description="The recent rules: visits created in the last N days (V2 local time)."),
        "source": Param("kisumu_v3", type="string", title="Source", description="The V2 load's facility key."),
        "target": Param("org284", type="string", title="Target", description="The V3 org (Connection afya_v3_<key>)."),
        "workers": Param(16, type="integer", minimum=1, maximum=64, title="Parallel V3 posts"),
        "corrections": Param(True, type="boolean", title="Org corrections for the same visits",
                             description="Evaluation visit destinations, consumables, admission states."),
        "dry_run": Param(True, type="boolean", title="Dry run", description="Read, transform and decide; post nothing."),
    },
)
def active_visits_to_v3():

    @task(execution_timeout=timedelta(hours=6))
    def send() -> str:
        p = get_current_context()["params"]
        use_pipelines_dir([p["source"], p["target"]])
        import active_visits_to_v3 as a
        out = a.run(p["source"], p["target"], p["rule"], int(p["days"]), workers=int(p["workers"]),
                    dry_run=bool(p["dry_run"]), corrections_steps=None if p["corrections"] else [])
        return str(out)

    @task
    def report(path: str) -> None:
        import json
        s = json.loads(Path(path).with_suffix(".json").read_text())
        log.info("%d active visits (%s: %s) — rule counts %s", s["active_visits"], s["rule"], s["rule_means"],
                 s["rule_counts"])
        log.info("report: %s", path)
        for t, why in s["not_sent"].items():
            log.info("  not sent %-38s %s", t, why)

    report(send())


active_visits_to_v3()
