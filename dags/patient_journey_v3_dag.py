# dags/patient_journey_v3_dag.py
"""
Make every migrated patient's journey hang together in V3, using the V2
Patient Journey API as the source of truth (Airflow driver for
patient_journey_v3.py — same code, same state files).

  check     `describe` the journey API — route deployed, token good,
            identity map populated (fails fast otherwise)
  walk      page through every patient into <state>/journey/journeys.ndjson.
            Resumable: a retry or re-trigger carries on from the saved cursor;
            a finished walk is reused unless `rewalk` is ticked
  plan      snapshot the V3 models the journeys touch, find each journey
            record's V3 row (uuid, else the migration id map) and work out the
            patient / visit links it should have. Writes nothing to V3 —
            see plan_report.json and problems.csv in <state>/journey/
  apply     [execute] one task per V3 model, ONE AT A TIME: set the missing /
            wrong links via the gateway (match_on=uuid), then re-read V3 to
            verify
  report    totals; fails the run if any row failed or still differs

`execute` is off by default: the first run of a facility is a dry run whose
plan you read before letting it write. Gentle on V3 the same way as
repair_v3_links (adaptive parallelism, rate cap, one model at a time).
Refuses to write a model while a migration / repair is writing it on this host.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import Param, dag, get_current_context, task
from airflow.task.trigger_rule import TriggerRule

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import clean_list, facility_keys, use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "patient_journey_v3"


def _names(values) -> list[str]:
    """A list param as given (journey module names are case-sensitive: "Finance")."""
    if isinstance(values, str):
        values = values.replace("\n", ",").split(",")
    return [str(v).strip() for v in values or [] if str(v).strip()]


def _setup():
    p = get_current_context()["params"]
    facility = p["facility"].strip()
    use_pipelines_dir([facility])
    import patient_journey_v3 as pj
    pace = pj.rl.Pace(int(p["start_workers"]), int(p["max_workers"]), float(p["max_rps"]), float(p["slow_seconds"]))
    return pj, p, facility, (p["journey_url"] or "").strip() or None, pace


def _fail_on(pj, fn):
    """Problems a retry can't fix fail the task outright."""
    import snowflake_to_v3_migration as s2v3
    try:
        return fn()
    except (pj.JourneyUnavailable, s2v3.TableBusy) as e:
        raise AirflowFailException(str(e))


@dag(
    dag_id=DAG_ID,
    description="Link every V3 record to its patient and visit from the V2 Patient Journey API",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration", "repair", "journey"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "facility": Param("kisumu_v3", type="string", title="Facility", examples=facility_keys()),
        "journey_url": Param("", type="string", title="Journey API host",
                             description="Scheme + host, no trailing slash. Empty = the facility's V2 host "
                                         "(or JOURNEY_<FACILITY>_BASE_URL)."),
        "walk": Param(True, type="boolean", title="Walk the journeys",
                      description="Off = plan from the journeys already on disk."),
        "rewalk": Param(False, type="boolean", title="Re-walk from the start",
                        description="Fetch every patient again instead of reusing / resuming the last walk."),
        "per_page": Param(100, type="integer", minimum=1, maximum=100, title="Patients per page"),
        "modules": Param([], type="array", items={"type": "string"}, title="Only these journey modules",
                         description="e.g. Evaluation, Reception. Empty = all."),
        "tables": Param([], type="array", items={"type": "string"}, title="Only these journey tables",
                        description="V2 table names. Empty = all."),
        "models": Param([], type="array", items={"type": "string"}, title="Only update these V3 models",
                        description="Gateway aliases (visits, prescriptions, …). Empty = all."),
        "overwrite": Param("strong", type="string", enum=["strong", "all", "never"], title="Overwrite wrong links",
                           description="Empty links are always filled. A link holding a different value is "
                                       "replaced: strong = only when record and parent were found by uuid; "
                                       "all = also via the id map; never = left alone."),
        "execute": Param(False, type="boolean", title="Apply changes to V3",
                         description="Off = check + walk + plan only (read the plan first)."),
        "resume": Param(False, type="boolean", title="Resume applying",
                        description="Skip check/walk/plan and carry on applying the last plan."),
        "start_workers": Param(2, type="integer", minimum=1, maximum=32, title="Start with N parallel requests"),
        "max_workers": Param(8, type="integer", minimum=1, maximum=32, title="Never more than N parallel requests"),
        "max_rps": Param(15, type="number", minimum=0, title="Max requests per second (0 = no cap)"),
        "slow_seconds": Param(5, type="number", minimum=1, title="Back off when a reply takes longer than (s)"),
    },
)
def patient_journey_v3():

    @task
    def check() -> dict:
        pj, p, facility, url, _ = _setup()
        if p["resume"] or not p["walk"]:
            raise AirflowSkipException("Not walking — nothing to check.")
        return _fail_on(pj, lambda: pj.run_check(facility, url))

    # resumable from its saved cursor, so a retry carries on where it stopped
    @task(trigger_rule=TriggerRule.NONE_FAILED, retries=3, retry_delay=timedelta(minutes=5),
          execution_timeout=timedelta(hours=24))
    def walk(_check) -> dict:
        pj, p, facility, url, _ = _setup()
        if p["resume"] or not p["walk"]:
            raise AirflowSkipException("Using the journeys already on disk.")
        # a retry of this task must resume, not start over
        fresh = bool(p["rewalk"]) and get_current_context()["ti"].try_number <= 1
        return _fail_on(pj, lambda: pj.run_walk(facility, url, per_page=int(p["per_page"]),
                                                modules=_names(p["modules"]), tables=_names(p["tables"]),
                                                fresh=fresh))

    @task(trigger_rule=TriggerRule.NONE_FAILED, execution_timeout=timedelta(hours=3))
    def plan(_walk) -> list[dict]:
        pj, p, facility, _, _ = _setup()
        if p["resume"]:
            report = pj.State(facility).load("plan_report.json")
            log.info("Resume — applying the last plan: %s", [i["alias"] for i in report.get("apply") or []])
        else:
            report = pj.run_plan(facility, overwrite=p["overwrite"], models=clean_list(p["models"]) or None)
            log.info("%s", json.dumps({k: v for k, v in report.items() if k != "tables"}, indent=2, default=str))
        if not p["execute"] and not p["resume"]:
            log.info("Plan only — tick `execute` to apply %d row update(s).", report.get("rows_to_update", 0))
            return []
        return report.get("apply") or []

    @task(max_active_tis_per_dagrun=1, execution_timeout=timedelta(hours=12))
    def apply(item: dict) -> dict:
        pj, p, facility, _, pace = _setup()
        out = _fail_on(pj, lambda: pj.run_apply(facility, item["alias"], item["service"], pace))
        log.info("%s: %s", item["alias"], json.dumps(out, default=str))
        return {"alias": item["alias"], **out}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def report(results) -> None:
        results = [r for r in (results or []) if r]
        for r in results:
            log.info("%-22s updated %s · failed %s · still differ %s", r["alias"], r["apply"]["updated"],
                     r["apply"]["failed"], (r.get("verify") or {}).get("still_wrong"))
        bad = [r["alias"] for r in results if r["apply"]["failed"] or (r.get("verify") or {}).get("still_wrong")]
        if bad:
            raise AirflowFailException(f"Not fully linked: {bad} — re-trigger with resume ticked")

    report(apply.expand(item=plan(walk(check()))))


patient_journey_v3()
