# dags/v3_to_snowflake_sync_dag.py
"""
V3 → Snowflake, kept up to date (v3_to_snowflake_sync.py — same code as the command line).

Every run, for each source (a V3 tenant login, AFYA_<KEY>_* in the server's .env) and each gateway model:
read all its rows, MERGE into HOSPITALS.V3_SYNC.<SERVICE>__<ALIAS> only the rows new or edited since the
model's watermark, flag rows V3 no longer returns as _IS_DELETED, then move the watermark. Watermarks and
the last run's counts / errors per model: HOSPITALS.V3_SYNC._SYNC_STATE.

    plan ──► sync_service (one mapped task per source × service) ──► summary

A model that fails doesn't stop the others; its service task fails at the end, its watermark stays put,
and the next run re-sends the same window (the MERGE is idempotent). No Airflow retries for that reason.

Sources: the `sources` param, else V3_SYNC_SOURCES (comma list) from the .env or an Airflow Variable.
`full_refresh` re-sends every row (hash-guarded) — run it after edits made straight in a V3 database,
which don't move updated_at.
"""
from __future__ import annotations

import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, Variable, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parent))  # for pipelines_common
from pipelines_common import clean_list, use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "v3_to_snowflake_sync"
SERVICES = ["core", "reception", "evaluation", "inventory", "finance", "theatre", "inpatient", "dialysis"]


def sources_from(param) -> list[str]:
    """The `sources` param, else V3_SYNC_SOURCES from the environment / an Airflow Variable."""
    chosen = clean_list(param)
    if chosen:
        return chosen
    return clean_list(os.getenv("V3_SYNC_SOURCES") or Variable.get("V3_SYNC_SOURCES", default="") or "")


def group_by_service(source: str, models) -> list[dict]:
    """One job per (source, service) — the unit a mapped task syncs."""
    by_service: dict[str, list[str]] = defaultdict(list)
    for m in models:
        by_service[m.service].append(m.name)
    return [{"source": source, "service": s, "models": names} for s, names in sorted(by_service.items())]


@dag(
    dag_id=DAG_ID,
    description="V3 gateway → Snowflake HOSPITALS.V3_SYNC — watermarked upserts, deletes flagged",
    schedule="0 */3 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "snowflake", "incremental"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "sources": Param([], type="array", items={"type": "string"}, title="V3 sources (AFYA_<KEY>_* logins)",
                         description="e.g. org284, gyno159. Empty = V3_SYNC_SOURCES from .env / Variables."),
        "models": Param([], type="array", items={"type": "string"}, title="Models (service:alias)",
                        description="e.g. reception:patient, finance:invoice. Empty = every readable model "
                                    "on the services below."),
        "services": Param(SERVICES, type="array", items={"type": "string", "enum": SERVICES}, title="Services"),
        "full_refresh": Param(False, type="boolean", title="Full refresh",
                              description="Ignore watermarks and re-send every row (unchanged rows are "
                                          "still skipped by hash)."),
        "lookback_minutes": Param(60, type="integer", minimum=0, title="Watermark overlap (minutes)"),
        "detect_deletes": Param(True, type="boolean", title="Flag rows V3 no longer returns as deleted"),
        "max_delete_fraction": Param(0.2, type="number", minimum=0, maximum=1, title="Delete guard",
                                     description="Skip flagging when more than this share of live rows "
                                                 "would be flagged in one run."),
        "workers": Param(4, type="integer", minimum=1, maximum=16, title="Models at once per service"),
        "page_workers": Param(4, type="integer", minimum=1, maximum=16, title="Pages read ahead per model"),
        "batch_size": Param(5000, type="integer", minimum=100, maximum=100_000, title="Rows per MERGE"),
        "limit": Param(0, type="integer", minimum=0, title="At most N rows per model (0 = all; 3 = canary)"),
        "dry_run": Param(False, type="boolean", title="Dry run", description="Read and count only."),
        "database": Param("HOSPITALS", type="string", title="Snowflake database"),
        "schema": Param("V3_SYNC", type="string", title="Snowflake schema"),
    },
)
def v3_to_snowflake_sync():

    @task
    def plan() -> list[dict]:
        p = get_current_context()["params"]
        use_pipelines_dir()
        sources = sources_from(p["sources"])
        if not sources:
            raise AirflowFailException("no V3 sources: pass `sources` or set V3_SYNC_SOURCES (e.g. org284,gyno159)")
        use_pipelines_dir(sources)
        import v3_to_snowflake_sync as S
        from v3_to_v3_client import V3Client
        chosen = S.parse_models(p["models"])
        jobs = []
        for source in sources:
            models = chosen or S.discover_models(V3Client(source), p["services"])
            jobs += group_by_service(source, models)
            log.info("%s: %d model(s) on %s", source, len(models), sorted({m.service for m in models}))
        return jobs

    @task(map_index_template="{{ job_label }}", execution_timeout=timedelta(hours=12))
    def sync_service(job: dict) -> dict:
        ctx = get_current_context()
        ctx["job_label"] = f"{job['source']} · {job['service']}"
        p = ctx["params"]
        use_pipelines_dir([job["source"]])
        import v3_to_snowflake_sync as S
        opts = S.SyncOptions(full_refresh=p["full_refresh"], lookback=timedelta(minutes=p["lookback_minutes"]),
                             batch_size=p["batch_size"], detect_deletes=p["detect_deletes"],
                             max_delete_fraction=p["max_delete_fraction"], page_workers=p["page_workers"],
                             limit=p["limit"], dry_run=p["dry_run"])
        results = S.sync(job["source"], [S.parse_model(m) for m in job["models"]], opts=opts,
                         workers=p["workers"], database=p["database"], schema=p["schema"])
        log.info("%s\n%s", ctx["job_label"], S.summarize(results))
        failed = [r.model for r in results if r.status == "failed"]
        if failed:
            raise AirflowFailException(f"{len(failed)} of {len(results)} model(s) failed: {failed} — the rest are "
                                       f"in; these keep their watermark and are retried next run")
        return {**job, "results": [r.as_dict() for r in results]}

    @task(trigger_rule="all_done")
    def summary(jobs: list[dict], done) -> None:
        # Airflow 3 tasks can't query task states; a failed mapped task has no return XCom, so it's simply
        # absent from `done` — compare against the plan to name it.
        done = [d for d in (done or []) if d]
        reported = {(d["source"], d["service"]) for d in done}
        failed = [f"{j['source']} · {j['service']}" for j in (jobs or []) if (j["source"], j["service"]) not in reported]
        use_pipelines_dir()
        import v3_to_snowflake_sync as S
        rows = [r for d in done for r in d["results"]]
        if rows:
            log.info("\n%s", S.summarize(rows))
        if jobs is None:
            raise AirflowFailException("plan failed — nothing was synced")
        if failed:
            raise AirflowFailException(f"service tasks that failed (see their logs; the next run retries): {failed}")

    jobs = plan()
    summary(jobs, sync_service.expand(job=jobs))


v3_to_snowflake_sync()
