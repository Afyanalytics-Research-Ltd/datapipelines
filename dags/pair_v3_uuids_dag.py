# dags/pair_v3_uuids_dag.py
"""
Record which V3 row each already-migrated V2 record became, by V3's own uuid,
before re-migrating a facility from a uuid-era V2 extract (Airflow driver for
pair_v3_uuids.py — same code, same state file).

Why: rows migrated before V2 had uuids carry a uuid V3 made up. A re-extract
brings every record's real V2 uuid, and the migration posts with
match_on=uuid — without this, every already-migrated record would be
inserted a second time. With the pairs saved in
.migration_state/<facility>/.migration_v3_uuid.json, the migration posts
with the V3 row's own uuid and updates it in place.

  pair     for every model in the facility's id map (or the ones named):
           V2 id → V3 id (id map) → V3 uuid (read from V3) — saved, the
           previous file kept as a .bak_pair_* backup
  report   per model: pairs found, added, changed, V3 rows missing

Reads V3 only; writes the facility's state file (never with dry_run ticked).
Run it once before snowflake_to_v3_migration on a re-extracted facility; a
fresh load into an empty V3 org doesn't need it.
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

DAG_ID = "pair_v3_uuids"


@dag(
    dag_id=DAG_ID,
    description="Save the V3 uuid of every already-migrated V2 record so a re-migration updates instead of duplicating",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration", "repair"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "facility": Param("kisumu_v3", type="string", title="Facility", examples=facility_keys()),
        "models": Param([], type="array", items={"type": "string"}, title="Only these V3 models",
                        description="Gateway aliases (visits, patient, prescriptions …). Empty = every model "
                                    "in the facility's id map."),
        "dry_run": Param(True, type="boolean", title="Dry run",
                         description="On: report only. Off: write .migration_v3_uuid.json (old copy backed up)."),
    },
)
def pair_v3_uuids():

    @task(execution_timeout=timedelta(hours=6))
    def pair() -> dict:
        p = get_current_context()["params"]
        facility = p["facility"].strip()
        use_pipelines_dir([facility])
        import pair_v3_uuids as pv
        report = pv.pair(facility, clean_list(p["models"]) or None, dry_run=bool(p["dry_run"]))
        log.info("%s", json.dumps(report, indent=2))
        return report

    @task
    def report(result: dict) -> None:
        p = get_current_context()["params"]
        paired = {m: r for m, r in result.items() if "pairs" in r}
        skipped = {m: r["skipped"] for m, r in result.items() if "skipped" in r}
        for m, r in sorted(paired.items()):
            log.info("%-26s %7d paired · %6d added · %4d changed · %6d V3 row missing",
                     m, r["pairs"], r["added"], r["changed"], r["v3_missing"])
        for m, why in sorted(skipped.items()):
            log.info("%-26s skipped — %s", m, why)
        log.info("%d model(s) paired, %d skipped%s", len(paired), len(skipped),
                 " — DRY RUN, nothing written" if p["dry_run"] else "")
        changed = sorted(m for m, r in paired.items() if r["changed"])
        if changed:
            # a V2 id paired earlier (repair_v3_links) now maps to a different V3
            # row: the newer pairing is written, but it's worth a look
            raise AirflowFailException(f"pairings changed for {changed} — check the id map for these models")

    report(pair())


pair_v3_uuids()
