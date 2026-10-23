# dags/v3_to_v3_audit_dag.py
"""
Audit a V3 → V3 copy (Airflow driver for v3_to_v3_audit.py): does the target
org mirror the source org — every row, every field, every link? Missing rows
are reloaded and differing fields patched; whatever still can't be made to
match gets a reason in the report.

  plan      access check on both sides (a refused account fails here), the
            source catalogue, what the target's gateway has
  tier_N    reload, parents first (each tier's models in parallel): rows on
            the target are paired by uuid, missing ones posted
  relink    links whose parent came later
  verify    every model in parallel: every source row on the target, every
            field equal (links in target ids); differences patched
            (match_on uuid) and verified again
  report    the reconcile-style CSV (tier, model, source_rows, … mirror, why)
            under output/v3v3_audit/, and a summary in the log

Params: reload=false + sync=false is a read-only audit. resume=true keeps the
models an earlier run already brought to a mirror and redoes the rest.

Each side is a facility key with a Connection afya_v3_<key> (see the
v3_to_v3_migration DAG): login/password of a SUPERADMIN account in that org,
Extra {"organization_id": …, "facility_id": …, "url_template": …,
"migration_key": …}. For org 284 the facility is 283.
"""
from __future__ import annotations

import logging
import sys
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

DAG_ID = "v3_to_v3_audit"
MAX_TIERS = 12
PARALLEL_MODELS = int_env("V3V3_PARALLEL_MODELS", 4)


def _setup(p: dict, **kw):
    use_pipelines_dir([p["source"], p["target"]])
    import v3_to_v3_audit as au
    import v3_to_v3_migration as vv
    src, dst, state, plan, copier = au.setup(p["source"], p["target"], workers=int(p["workers"]),
                                             read_workers=int(p["read_workers"]), reload=bool(p["reload"]), **kw)
    return au, vv, src, dst, state, plan, copier


def _jobs(ti) -> list[dict]:
    jobs = ti.xcom_pull(task_ids="plan")
    if jobs is None:
        raise AirflowSkipException("plan didn't produce a model list")
    return jobs


@dag(
    dag_id=DAG_ID,
    description="Audit a V3 → V3 copy row by row and field by field; reload what's missing, patch what differs",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["v3", "migration", "audit"],
    default_args={"owner": "data-eng", "retries": 0},
    params={
        "source": Param("kisumu_v3", type="string", title="Source",
                        description="Facility key of the org copied FROM (Connection afya_v3_<key>)."),
        "target": Param("org284", type="string", title="Target",
                        description="Facility key of the org copied INTO (Connection afya_v3_<key>)."),
        "reload": Param(True, type="boolean", title="Reload missing rows",
                        description="Post source rows the target lacks. Off = report them only."),
        "sync": Param(True, type="boolean", title="Patch differing fields",
                      description="Rewrite target fields that differ from the source. Off = report them only."),
        "resume": Param(False, type="boolean", title="Resume",
                        description="Keep models an earlier audit already brought to a mirror; redo the rest."),
        "models": Param([], type="array", items={"type": "string"}, title="Models",
                        description="Only these, as service/alias (e.g. reception/patient). Empty = all."),
        "workers": Param(32, type="integer", minimum=1, maximum=64, title="Parallel POSTs per model",
                         description=f"Inserts / patches in flight per model. Up to {PARALLEL_MODELS} models run "
                                     f"at once (V3V3_PARALLEL_MODELS)."),
        "read_workers": Param(8, type="integer", minimum=1, maximum=32, title="Parallel page reads per table"),
    },
)
def v3_to_v3_audit():

    @task
    def plan() -> list[dict]:
        p = get_current_context()["params"]
        au, vv, src, dst, state, pl, copier = _setup(p, refresh=True)
        try:
            vv.preflight(src)
            vv.preflight(dst)
        except vv.GatewayError as e:
            raise AirflowFailException(str(e))
        registered, down = au.target_catalogue(dst)
        if down:
            log.warning("target services that can't be listed: %s", down)
        only = clean_list(p["models"]) or None
        models = pl["models"]
        todo = [m for m, x in models.items() if x["selected"] and m in registered and (not only or m in only)]
        prev = au.saved_rows(state) if p["resume"] else {}
        if not p["resume"]:                   # a fresh audit: no earlier row stands in for this one
            for d in ("audit_row", "audit_entry"):
                for f in (state.dir / d).glob("*.json") if (state.dir / d).exists() else ():
                    f.unlink()
            (state.dir / "audit_v3v3" / "rows.json").unlink(missing_ok=True)
        run = [m for m in todo if not au.mirrored(prev.get(m))]
        log.info("%d model(s) to audit (%d already mirrored, kept); not on the target's gateway: %s",
                 len(run), len(todo) - len(run),
                 sorted(m for m, x in models.items() if x["selected"] and m not in registered))
        return [{"model": m, "tier": models[m]["tier"]} for m in sorted(run, key=lambda m: (models[m]["tier"], m))]

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def models_in_tier(tier: int) -> list[dict]:
        jobs = _jobs(get_current_context()["ti"])
        if tier == MAX_TIERS:                 # the last task also takes any deeper tier
            return [j for j in jobs if j["tier"] >= MAX_TIERS]
        return [j for j in jobs if j["tier"] == tier]

    @task(map_index_template="{{ map_label }}", max_active_tis_per_dagrun=PARALLEL_MODELS,
          execution_timeout=timedelta(hours=24), retries=1, retry_delay=timedelta(minutes=3))
    def reload_model(job: dict) -> dict:
        ctx = get_current_context()
        ctx["map_label"] = job["model"]
        au, vv, src, dst, state, pl, copier = _setup(ctx["params"])
        entry = au.reload_step(copier, job["model"])
        res = entry["copy"]
        log.info("%s", vv.describe_result(job["model"], res, state) if res else entry["why"])
        # a model the target refuses doesn't stop the audit: verify and the report say why
        return {k: res.get(k) for k in ("read", "already", "posted", "held", "failed", "status")}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def relink() -> None:
        ctx = get_current_context()
        if not ctx["params"]["reload"]:
            raise AirflowSkipException("reload is off")
        au, vv, src, dst, state, pl, copier = _setup(ctx["params"])
        for j in _jobs(ctx["ti"]):
            au.relink_step(copier, j["model"])

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def verify_list() -> list[dict]:
        return [{"model": j["model"]} for j in _jobs(get_current_context()["ti"])]

    @task(map_index_template="{{ map_label }}", max_active_tis_per_dagrun=max(1, PARALLEL_MODELS - 1),
          execution_timeout=timedelta(hours=24))
    def verify_model(job: dict) -> dict:
        ctx = get_current_context()
        ctx["map_label"] = job["model"]
        au, vv, src, dst, state, pl, copier = _setup(ctx["params"])
        row = au.verify_step(copier, job["model"], sync=bool(ctx["params"]["sync"]))
        log.info("%s — mirror: %s\n%s", job["model"], row.get("mirror"), row.get("why", "").replace(" | ", "\n"))
        return {k: row.get(k) for k in ("source_rows", "target_rows_after", "still_missing", "still_mismatched",
                                         "posted_now", "patched", "mirror")}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def report() -> str:
        ctx = get_current_context()
        p = ctx["params"]
        au, vv, src, dst, state, pl, copier = _setup(p)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        out = Path("output") / "v3v3_audit" / f"{p['source']}-to-{p['target']}-audit-{stamp}.csv"
        au.write_report(state, pl, dst, out, only=clean_list(p["models"]) or None)
        rows = au.saved_rows(state)
        bad = sorted((m, r) for m, r in rows.items() if m in pl["models"] and not au.mirrored(r))
        log.info("audit written: %s (absolute: %s)", out, out.resolve())
        log.info("%d model(s) mirror the source, %d don't:", len(rows) - len(bad), len(bad))
        for m, r in bad:
            log.info("  ✗ %s — missing %s, differing %s — %s", m, r.get("still_missing"), r.get("still_mismatched"),
                     str(r.get("why", ""))[:600])
        return str(out.resolve())

    planned = plan()
    previous = planned
    for n in range(1, MAX_TIERS + 1):
        selected = models_in_tier.override(task_id=f"tier_{n}_models")(n)
        [planned, previous] >> selected
        ran = reload_model.override(task_id=f"tier_{n}").expand(job=selected)
        previous = ran
    linked = relink()
    previous >> linked
    to_verify = verify_list()
    linked >> to_verify
    checked = verify_model.override(task_id="verify").expand(job=to_verify)
    checked >> report()


v3_to_v3_audit()
