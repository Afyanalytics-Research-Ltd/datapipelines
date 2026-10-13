# dags/v3_to_v3_migration_dag.py
"""
V3 organisation → V3 organisation (the two may be on different V3 instances)
(Airflow driver for v3_to_v3_migration.py)

  plan     catalogue the source org's models, order them (parents first) and
           audit every link rule against the source's own data. Reads only.
           Fails if an id column has no rule or a rule points at the wrong
           model — fix v3_to_v3_links.py before copying.
  migrate  tier by tier (tier_1 … tier_N, each tier's models in parallel),
           then `relink` (optional links whose parent came later), then
           `verify`.
  (a canary — record_limit > 0 — verifies the rows it copied, field by field)
  verify   only the comparison: every source row on the target by uuid, every
           field equal with links translated. A model that differs is red.

Each side is a facility key with a Connection afya_v3_<key>: login /
password = an account in that org (the gateway needs the SUPERADMIN role),
Extra {"organization_id": …, "facility_id": …,
       "url_template": "https://{service}.afyaanalytics.ai/api/",
       "migration_key": "<that instance's X-Migration-Key>"}.

Re-running is safe: rows keep their uuid and are inserted match_on uuid, so
a row already on the target is mapped, never posted twice. record_limit > 0
is a canary: at most N new rows per model.

State: .migration_state/v3v3_<source>_to_<target>/ (id maps, held rows,
dead letters, reports, verify results) — one file per model.
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

DAG_ID = "v3_to_v3_migration"
MAX_TIERS = 12
PARALLEL_MODELS = int_env("V3V3_PARALLEL_MODELS", 4)


def _setup(p: dict):
    use_pipelines_dir([p["source"], p["target"]])
    import v3_to_v3_migration as vv
    src, dst = vv.clients(p["source"], p["target"])
    return vv, src, dst, vv.State(p["source"], p["target"])


def _plan(vv, src, state, p, refresh=False):
    return vv.load_or_build_plan(src, state, clean_list(p["models"]) or None,
                                 clean_list(p["exclude_models"]) or None, refresh=refresh)


@dag(
    dag_id=DAG_ID,
    description="Copy one V3 organisation into another through the gateways, then verify they match",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "source": Param("kisumu_v3", type="string", title="Source",
                        description="Facility key of the org to copy FROM (Connection afya_v3_<key>)."),
        "target": Param("org250", type="string", title="Target",
                        description="Facility key of the org to copy INTO (Connection afya_v3_<key>)."),
        "mode": Param("plan", type="string", enum=["plan", "migrate", "verify"], title="Mode",
                      description="plan: read-only audit. migrate: copy + relink + verify. verify: compare only."),
        "models": Param([], type="array", items={"type": "string"}, title="Models",
                        description="Only these, as service/alias (e.g. reception/patient). Empty = all."),
        "exclude_models": Param([], type="array", items={"type": "string"}, title="Exclude models"),
        "record_limit": Param(0, type="integer", minimum=0, title="Record limit (canary)",
                              description="Post at most this many new rows per model. 0 = everything."),
        "workers": Param(4, type="integer", minimum=1, maximum=32, title="Parallel POSTs per model"),
        "refresh_plan": Param(False, type="boolean", title="Rebuild the plan",
                              description="Re-read the source catalogue (new models / columns)."),
        "skip_verify": Param(False, type="boolean", title="Skip verify after migrate"),
    },
)
def v3_to_v3_migration():

    @task
    def plan() -> list[dict]:
        p = get_current_context()["params"]
        vv, src, dst, state = _setup(p)
        log.info("source %r → target %r", src, dst)
        if p["mode"] != "plan":
            dst.token()                       # wrong account / org fails here, before anything moves
        pl = _plan(vv, src, state, p, refresh=p["refresh_plan"] or p["mode"] == "plan")
        if p["mode"] == "plan":
            audit = vv.audit_links(src, pl)
            state.save("audit", "all", audit)
            problems = vv.print_plan(pl, audit)
            if problems:
                raise AirflowFailException(f"{len(problems)} link problem(s) — see the log")
            return []
        vv.print_plan(pl)
        return [{"model": m, "tier": x["tier"]} for m, x in sorted(pl["models"].items()) if x["selected"]]

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def models_in_tier(tier: int) -> list[dict]:
        ctx = get_current_context()
        jobs = ctx["ti"].xcom_pull(task_ids="plan")
        if jobs is None:
            raise AirflowSkipException("plan didn't produce a model list")
        if ctx["params"]["mode"] != "migrate":
            return []
        last = max((j["tier"] for j in jobs), default=0)
        if tier == MAX_TIERS:                 # the last task also takes any deeper tier
            return [j for j in jobs if j["tier"] >= MAX_TIERS]
        if tier > last:
            return []
        return [j for j in jobs if j["tier"] == tier]

    @task(map_index_template="{{ map_label }}", max_active_tis_per_dagrun=PARALLEL_MODELS,
          execution_timeout=timedelta(hours=24), retries=1, retry_delay=timedelta(minutes=3))
    def migrate_model(job: dict) -> dict:
        ctx = get_current_context()
        ctx["map_label"] = job["model"]
        p = ctx["params"]
        vv, src, dst, state = _setup(p)
        pl = _plan(vv, src, state, p)
        copier = vv.Copier(src, dst, state, pl, workers=int(p["workers"]), record_limit=int(p["record_limit"]))
        res = copier.copy_model(job["model"])
        log.info("%s: %s", job["model"], res)
        if res["status"] == "failed":
            raise AirflowFailException(f"{res['failed']} row(s) dead-lettered — see dead_letters/{job['model']}")
        if res["status"] == "waiting":
            raise AirflowSkipException(f"WAITING — {res['held']} row(s) held for a parent not copied yet")
        return res

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def relink() -> dict:
        p = get_current_context()["params"]
        if p["mode"] != "migrate":
            raise AirflowSkipException("migrate mode only")
        vv, src, dst, state = _setup(p)
        pl = _plan(vv, src, state, p)
        copier = vv.Copier(src, dst, state, pl, workers=int(p["workers"]))
        out = {m: copier.relink(m) for m, x in sorted(pl["models"].items(), key=lambda kv: kv[1]["tier"] or 0)
               if x["selected"]}
        left = {m: r["still_missing"] for m, r in out.items() if r["still_missing"]}
        if left:
            log.warning("links still unresolved after relink: %s", left)
        return out

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def verify_list() -> list[dict]:
        ctx = get_current_context()
        p = ctx["params"]
        if p["mode"] == "plan" or (p["mode"] == "migrate" and p["skip_verify"]):
            return []
        jobs = ctx["ti"].xcom_pull(task_ids="plan") or []
        return [{"model": j["model"]} for j in jobs]

    @task(map_index_template="{{ map_label }}", max_active_tis_per_dagrun=PARALLEL_MODELS,
          execution_timeout=timedelta(hours=12))
    def verify_model(job: dict) -> dict:
        ctx = get_current_context()
        ctx["map_label"] = job["model"]
        vv, src, dst, state = _setup(ctx["params"])
        pl = _plan(vv, src, state, ctx["params"])
        # a canary run compares just the rows it copied, field by field
        canary = ctx["params"]["mode"] == "migrate" and int(ctx["params"]["record_limit"]) > 0
        res = vv.verify_model(vv.Copier(src, dst, state, pl), job["model"], canary=canary)
        state.save("verify", job["model"], res)
        log.info("%s: %s", job["model"], json.dumps(res, default=str)[:4000])
        if not res["ok"]:
            raise AirflowFailException(
                f"target differs: {res.get('missing', 0)} missing, fields {res.get('mismatched_fields') or {}}")
        return {k: v for k, v in res.items() if not k.endswith("examples")}

    @task(trigger_rule=TriggerRule.NONE_FAILED)
    def done() -> None:
        log.info("No model failed.")

    planned = plan()
    previous = planned
    tier_tasks = []
    for n in range(1, MAX_TIERS + 1):
        selected = models_in_tier.override(task_id=f"tier_{n}_models")(n)
        [planned, previous] >> selected
        ran = migrate_model.override(task_id=f"tier_{n}").expand(job=selected)
        tier_tasks.append(ran)
        previous = ran
    linked = relink()
    previous >> linked
    to_verify = verify_list()
    linked >> to_verify
    checked = verify_model.override(task_id="verify").expand(job=to_verify)
    [planned, *tier_tasks, linked, checked] >> done()


v3_to_v3_migration()
