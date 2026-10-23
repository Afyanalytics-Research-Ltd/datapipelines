# dags/export_missing_records_dag.py
"""
Audit CSV → CSVs of the V2 rows missing from V3 + MySQL load scripts, uploaded to S3
(Airflow driver for export_missing_records.py)

  plan     reads the reconcile summary CSV (pasted into `audit_csv`, or `audit_s3_uri`) and lists every
           table where V3 has fewer rows than V2 (minus `exclude_tables`, or only `tables`).
  export   ONE TASK PER TABLE, up to V3_EXPORT_PARALLEL at once: the pipeline transforms the table's
           rows exactly as it would post them (nothing is posted), rows V3 already holds (same uuid) are
           dropped, and <table>.csv + <table>_load.sql are uploaded to S3 as soon as that table is done.
           A table that would load more rows than the audit gap goes to quarantine/ (likely duplicates).
  summary  README.txt — load order (parents first), counts, and every table not loaded with the reason.

Output: s3://<bucket>/<prefix>/<facility>/<run time>/{README.txt, report.json, *.csv, *_load.sql, quarantine/}
Muniu (or any DBA): per table, in its service's database:
    mysql --local-infile=1 <db> < <table>_load.sql      (with <table>.csv next to it)

Rows whose parent (patient, visit, admission, user …) isn't in V3 are never exported — load the parents,
then trigger again with the new audit to pick their children up.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
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

DAG_ID = "export_missing_records"
PARALLEL = int_env("V3_EXPORT_PARALLEL", 6)


def _s3():
    import boto3
    return boto3.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                        region_name=os.getenv("AWS_REGION", "us-east-1"))


def _split_s3(uri: str) -> tuple[str, str]:
    path = uri.removeprefix("s3://")
    bucket, _, key = path.partition("/")
    return bucket, key


@dag(
    dag_id=DAG_ID,
    description="Reconcile audit → CSVs of V2 rows missing from V3 + MySQL load scripts, on S3",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration", "export"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "facility": Param("kisumu_v3", type="string", title="Facility",
                          description="Facility key (Snowflake {NAME}_CLEAN + Connection afya_v3_<name>)."),
        # optional (null allowed) — fill ONE of the two; the form otherwise insists on both
        "audit_csv": Param(None, type=["null", "string"], format="multiline", title="Audit CSV (paste)",
                           description="Paste the reconcile *-summary.csv content here, OR leave empty and give "
                                       "audit_s3_uri."),
        "audit_s3_uri": Param(None, type=["null", "string"], title="Audit CSV on S3 (optional)",
                              description="e.g. s3://collabmedbucket/v3_audits/4-reconcile-20261014-090333-summary.csv"),
        "tables": Param([], type=["null", "array"], items={"type": "string"}, title="Only these V2 tables (optional)",
                        description="One per line. Empty = every table the audit shows short."),
        "exclude_tables": Param([], type=["null", "array"], items={"type": "string"},
                                title="Exclude V2 tables (optional)", description="One per line."),
        "organization_id": Param(4, type="integer", title="V3 organization id"),
        "facility_id": Param(4, type="integer", title="V3 facility id"),
        "s3_bucket": Param("collabmedbucket", type="string", title="S3 bucket"),
        "s3_prefix": Param("v3_missing_records", type="string", title="S3 prefix"),
    },
)
def export_missing_records():

    @task
    def plan() -> dict:
        ctx = get_current_context()
        p = ctx["params"]
        use_pipelines_dir([p["facility"]])
        import export_missing_records as emr
        text = (p["audit_csv"] or "").strip()
        name = "pasted audit"
        if not text:
            if not p["audit_s3_uri"]:
                raise AirflowFailException("Paste the audit CSV into audit_csv, or give audit_s3_uri.")
            bucket, key = _split_s3(p["audit_s3_uri"])
            text = _s3().get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
            name = key.rsplit("/", 1)[-1]
        jobs = emr.parse_audit(text)
        only, skip = set(clean_list(p["tables"])), set(clean_list(p["exclude_tables"]))
        jobs = [j for j in jobs if (not only or j["v2_table"] in only) and j["v2_table"] not in skip]
        if not jobs:
            raise AirflowSkipException("the audit shows no table short in V3")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        prefix = f"{p['s3_prefix'].strip('/')}/{p['facility']}/{stamp}"
        log.info("%d table(s) to export → s3://%s/%s: %s", len(jobs), p["s3_bucket"], prefix,
                 ", ".join(f"{j['v2_table']}({j['gap']})" for j in jobs))
        return {"prefix": prefix, "audit": name, "jobs": [{**j, "prefix": prefix} for j in jobs]}

    @task
    def jobs_of(planned: dict) -> list[dict]:
        return planned["jobs"]

    @task(map_index_template="{{ map_label }}", max_active_tis_per_dagrun=PARALLEL,
          execution_timeout=timedelta(hours=8), retries=1, retry_delay=timedelta(minutes=2))
    def export(job: dict) -> list[dict]:
        ctx = get_current_context()
        ctx["map_label"] = job["v2_table"]
        p = ctx["params"]
        use_pipelines_dir([p["facility"]])
        import export_missing_records as emr
        import v2_to_v3_api_migration as v2v3
        out = Path(tempfile.mkdtemp(prefix=f"emr_{job['v2_table']}_"))
        reports = emr.export_table(p["facility"], job["v2_table"], job["gap"], out,
                                   organization_id=int(p["organization_id"]), facility_id=int(p["facility_id"]),
                                   state_root=v2v3.STATE_ROOT)
        s3 = _s3()
        for r in reports:
            for rel in r.get("files", []):
                key = f"{job['prefix']}/{rel}"
                s3.upload_file(str(out / rel), p["s3_bucket"], key)
                log.info("uploaded s3://%s/%s", p["s3_bucket"], key)
        if any(r.get("status") == "ERROR" for r in reports):
            raise RuntimeError(f"V3 could not be read for {job['v2_table']}: {reports}")   # retried once
        return reports

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summary(planned: dict, results: list) -> None:
        ctx = get_current_context()
        p = ctx["params"]
        use_pipelines_dir([p["facility"]])
        import export_missing_records as emr
        reports = [r for rs in (results or []) if rs for r in rs]
        done = {r["v2_table"] for r in reports}
        for j in planned["jobs"]:
            if j["v2_table"] not in done:
                reports.append({"v2_table": j["v2_table"], "status": "FAILED — see the task log; re-run this table",
                                "audit_gap": j["gap"], "to_load": 0})
        text = emr.readme(reports, p["facility"], int(p["organization_id"]), planned["audit"])
        s3 = _s3()
        s3.put_object(Bucket=p["s3_bucket"], Key=f"{planned['prefix']}/README.txt", Body=text.encode())
        s3.put_object(Bucket=p["s3_bucket"], Key=f"{planned['prefix']}/report.json",
                      Body=json.dumps(reports, indent=1, default=str).encode())
        log.info("README → s3://%s/%s/README.txt\n%s", p["s3_bucket"], planned["prefix"], text)

    planned = plan()
    results = export.expand(job=jobs_of(planned))
    summary(planned, results)


export_missing_records()
