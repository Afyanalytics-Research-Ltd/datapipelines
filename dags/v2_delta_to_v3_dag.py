# dags/v2_delta_to_v3_dag.py
"""
V2 → V3 delta (Airflow driver for v2_delta_to_v3.py): bring a V3 organisation that was copied from a
V2-fed organisation up to date with V2 — every V2 record created or changed on/after `since` goes straight
into the target org (Kisumu at go-live: V2 kept in use after the migration → production org 284).

  delta   reads the live V2 API, transforms with the V2 load's own rules (same stable uuids as the copied
          org), chains links V2 → source org → target, then UPDATES records the target already has (under
          the target's uuid) and INSERTS new ones. Edited old records the target can't be matched to are
          skipped (could duplicate) and reported.
  report  the per-table CSV: output/v2_delta/<source>-to-<target>-since-<date>-<time>.csv

Params:
  since   "YYYY-MM-DD" or "YYYY-MM-DD HH:MM" (V2 local time). Required.
  mode    new_and_edits: new + changed records (reads every table whole: ~200 rows/s, hours for a big
          facility) · new_only: only records created since (minutes).
  dry_run reads, transforms and decides — posts nothing; the CSV shows what would happen.

Needs: Connection afya_v3_<source> (the V2 load's facility) and afya_v3_<target> (a SUPERADMIN in the target
org, Extra {"organization_id", "facility_id", "url_template", "migration_key"}); the V2 load's state for the
source (.migration_state/<source>/.migration_id_map.json) and the V3 → V3 copy's id maps (.migration_state/
v3v3_<source>_to_<target>/id_map/) — both on this server.

Run before go-live with mode=new_and_edits, then a short new_only run at cut-over.
"""
from __future__ import annotations

import logging
import re
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

DAG_ID = "v2_delta_to_v3"
_SINCE = re.compile(r"^\d{4}-\d\d-\d\d( \d\d:\d\d(:\d\d)?)?$")


@dag(
    dag_id=DAG_ID,
    description="V2 records created or changed since a date → straight into the target V3 org (update or insert)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration", "delta"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "since": Param("", type="string", title="Since",
                       description='V2 records created or changed on/after this: "YYYY-MM-DD" or "YYYY-MM-DD HH:MM" '
                                   "(V2 local time)."),
        "mode": Param("new_and_edits", type="string", enum=["new_and_edits", "new_only"], title="Mode",
                      description="new_and_edits reads every table whole (hours); new_only only new records "
                                  "(minutes)."),
        "source": Param("kisumu_v3", type="string", title="Source",
                        description="The V2 load's facility key (its V2 connection, rules and uuids)."),
        "target": Param("org284", type="string", title="Target",
                        description="The V3 org to update (Connection afya_v3_<key>)."),
        "tables": Param([], type="array", items={"type": "string"}, title="Tables",
                        description="V2 tables (e.g. evaluation_visits). Empty = everything the load migrates."),
        "workers": Param(32, type="integer", minimum=1, maximum=64, title="Parallel V3 posts"),
        "page_workers": Param(32, type="integer", minimum=1, maximum=64, title="Parallel V2 page reads"),
        "dry_run": Param(False, type="boolean", title="Dry run",
                         description="Read, transform and decide; post nothing."),
        "corrections": Param(True, type="boolean", title="Run the org corrections after",
                             description="v3_org_corrections: visit destinations, discharges, destination "
                                         "categories (skipped on a dry run)."),
    },
)
def v2_delta_to_v3():

    @task(execution_timeout=timedelta(hours=24))
    def delta() -> str:
        p = get_current_context()["params"]
        since = str(p["since"] or "").strip()
        if not _SINCE.match(since):
            raise AirflowFailException(f'since must be "YYYY-MM-DD" or "YYYY-MM-DD HH:MM", got {since!r}')
        use_pipelines_dir([p["source"], p["target"]])
        import v2_delta_to_v3 as d
        out = d.run(p["source"], p["target"], since, new_only=p["mode"] == "new_only",
                    tables=clean_list(p["tables"]) or None, workers=int(p["workers"]),
                    page_workers=int(p["page_workers"]), dry_run=bool(p["dry_run"]))
        return str(out)

    @task
    def report(path: str) -> None:
        import csv
        import io
        text = Path(path).read_text()
        rows = list(csv.DictReader(io.StringIO(text.split("\n\n", 1)[0])))
        num = lambda k: sum(int(r[k]) for r in rows if (r.get(k) or "").isdigit())
        log.info("delta report: %s", path)
        log.info("V2 changed %d · updated %d · inserted %d · held %d · skipped %d · failed %d", num("v2_changed"),
                 num("updated"), num("inserted"), num("held"), num("skipped"), num("failed"))
        for r in rows:
            if r["why"] != "all changes applied":
                log.info("  %-42s %s", r["table"], r["why"][:400])

    @task(execution_timeout=timedelta(hours=6))
    def corrections(_: str) -> None:
        p = get_current_context()["params"]
        if p["dry_run"] or not p.get("corrections", True):
            log.info("corrections skipped (dry run / turned off)")
            return
        from v3_org_corrections_dag import run_corrections
        run_corrections(p["target"], source=None, facility=p["source"], dry_run=False,
                        steps=["destination_categories", "eval_visit_destinations", "eval_consumables", "eval_investigation_links",
                               "admission_discharges"])

    out = delta()
    report(out)
    corrections(out)


v2_delta_to_v3()
