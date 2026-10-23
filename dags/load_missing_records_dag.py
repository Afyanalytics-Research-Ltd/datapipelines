# dags/load_missing_records_dag.py
"""
CSVs from export_missing_records (on S3) → posted into a V3 organisation through the model gateway
(Airflow driver for load_missing_records.py)

  plan        lists <table>.csv in the S3 folder (quarantine/ is never loaded), minus exclude_tables
  tier_1..3   one task per table, tier by tier — reference data, then the records others hang off
              (visits, admissions, invoices, theatre bookings …), then everything else. Rows go in
              `workers` at a time, each an insert match_on uuid: a re-run updates, never duplicates.
  summary     load_report.json next to the CSVs; rows not inserted are in rejects/<table>.csv:
                duplicate       already in V3 under another uuid (its unique key is taken) — fine
                parent missing  its patient / admission … isn't in V3 — load the parent, re-run
                rejected        V3 refused the value — fix by hand

The account is the facility's (Connection afya_v3_<facility>): rows land in its organisation.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
except ImportError:   # older Airflow 3.0.x
    from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import Param, dag, get_current_context, task
from airflow.task.trigger_rule import TriggerRule

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import clean_list, int_env, use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "load_missing_records"
PARALLEL = int_env("V3_LOAD_PARALLEL_TABLES", 4)
TIERS = (1, 2, 3)


def _s3():
    import boto3
    return boto3.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                        region_name=os.getenv("AWS_REGION", "us-east-1"))


def _split(uri: str) -> tuple[str, str]:
    bucket, _, prefix = uri.removeprefix("s3://").partition("/")
    return bucket, prefix.rstrip("/") + "/"


@dag(
    dag_id=DAG_ID,
    description="Export CSVs on S3 → V3 organisation via the model gateway (duplicates skipped, rejects reported)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration", "load"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "facility": Param("kisumu_v3", type="string", title="Facility",
                          description="Its V3 account (Connection afya_v3_<facility>) decides the organisation."),
        "s3_folder": Param("s3://collabmedbucket/v3_missing_records/kisumu_v3/20261014-073719/", type="string",
                           title="Export folder on S3"),
        "tables": Param([], type=["null", "array"], items={"type": "string"}, title="Only these tables (optional)",
                        description="DB table names as in the CSV file names, one per line. Empty = all."),
        "exclude_tables": Param([], type=["null", "array"], items={"type": "string"},
                                title="Exclude tables (optional)"),
        "organization_id": Param(4, type="integer", title="V3 organization id"),
        "facility_id": Param(4, type="integer", title="V3 facility id"),
        "workers": Param(8, type="integer", minimum=1, maximum=32, title="Parallel posts per table"),
    },
)
def load_missing_records():

    @task
    def plan() -> list[dict]:
        p = get_current_context()["params"]
        use_pipelines_dir([p["facility"]])
        import load_missing_records as lmr
        bucket, prefix = _split(p["s3_folder"])
        s3 = _s3()
        names = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            names += [o["Key"][len(prefix):] for o in page.get("Contents", [])]
        tables = sorted(n[:-4] for n in names if n.endswith(".csv"))          # top level only: no quarantine/
        only, skip = set(clean_list(p["tables"])), set(clean_list(p["exclude_tables"]))
        tables = [t for t in tables if (not only or t in only) and t not in skip]
        jobs = []
        for t in tables:
            model = None
            if f"{t}.model.json" in names:
                model = json.loads(s3.get_object(Bucket=bucket, Key=f"{prefix}{t}.model.json")["Body"].read())
            model = model or dict(zip(("service", "alias"), lmr.TABLE_MODEL.get(t, (None, None))))
            if not model.get("alias"):
                log.warning("%s: no gateway model known — skipped (add it to TABLE_MODEL)", t)
                continue
            jobs.append({"table": t, "tier": lmr.tier_of(t), **model})
        if not jobs:
            raise AirflowSkipException("no loadable CSVs in that folder")
        for n in TIERS:
            log.info("tier %d: %s", n, ", ".join(j["table"] for j in jobs if j["tier"] == n) or "-")
        return jobs

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def in_tier(tier: int) -> list[dict]:
        jobs = get_current_context()["ti"].xcom_pull(task_ids="plan")
        if jobs is None:
            raise AirflowSkipException("plan produced nothing")
        return [j for j in jobs if j["tier"] == tier]

    @task(map_index_template="{{ map_label }}", max_active_tis_per_dagrun=PARALLEL,
          execution_timeout=timedelta(hours=6), retries=1, retry_delay=timedelta(minutes=2))
    def load(job: dict) -> dict:
        ctx = get_current_context()
        ctx["map_label"] = job["table"]
        p = ctx["params"]
        use_pipelines_dir([p["facility"]])
        import load_missing_records as lmr
        from v3_to_v3_client import V3Client
        bucket, prefix = _split(p["s3_folder"])
        s3 = _s3()
        rows = lmr.parse_csv(s3.get_object(Bucket=bucket, Key=f"{prefix}{job['table']}.csv")["Body"].read().decode("utf-8"))
        header, data = rows[0], rows[1:]
        client = V3Client(p["facility"], organization_id=int(p["organization_id"]), facility_id=int(p["facility_id"]))
        log.info("%s → %s/%s: %d rows", job["table"], job["service"], job["alias"], len(data))
        counts, misses = lmr.load_rows(client, job["service"], job["alias"], header, data, workers=int(p["workers"]))
        if misses:
            s3.put_object(Bucket=bucket, Key=f"{prefix}rejects/{job['table']}.csv",
                          Body=lmr.misses_csv(misses).encode("utf-8"))
        log.info("%s: %s", job["table"], counts)
        res = {**job, "rows": len(data), **counts}
        if counts["error"]:
            raise RuntimeError(f"{counts['error']} row(s) could not be posted (gateway errors) — retried once: {res}")
        return res

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summary(jobs: list, tier_1: list, tier_2: list, tier_3: list) -> None:
        p = get_current_context()["params"]
        bucket, prefix = _split(p["s3_folder"])
        done = [r for rs in (tier_1, tier_2, tier_3) if rs for r in rs if r]
        report = {"facility": p["facility"], "organization_id": p["organization_id"], "tables": done,
                  "not_finished": sorted({j["table"] for j in jobs or []} - {r["table"] for r in done})}
        _s3().put_object(Bucket=bucket, Key=f"{prefix}load_report.json",
                         Body=json.dumps(report, indent=1, default=str).encode())
        for r in sorted(done, key=lambda r: (r["tier"], r["table"])):
            log.info("tier %s %-34s rows %6d | inserted %6d | duplicate %6d | parent missing %5d | rejected %5d",
                     r["tier"], r["table"], r["rows"], r["inserted"], r["duplicate"], r["parent missing"], r["rejected"])
        if report["not_finished"]:
            raise AirflowFailException(f"not finished: {report['not_finished']} — see their task logs")

    jobs = plan()
    previous, ran = jobs, []
    for n in TIERS:
        selected = in_tier.override(task_id=f"tier_{n}_tables")(n)
        [jobs, previous] >> selected
        out = load.override(task_id=f"tier_{n}").expand(job=selected)
        ran.append(out)
        previous = out
    summary(jobs, *ran)          # tier_1, tier_2, tier_3 results


load_missing_records()
