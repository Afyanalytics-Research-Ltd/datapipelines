"""
v3_to_v3_audit.py — audit a V3 → V3 copy (v3_to_v3_migration.py): does the
target org mirror the source org, model by model, row by row, field by field?
Missing rows are reloaded and differing fields patched; whatever still can't
be made to match gets a reason. Writes a reconcile-style CSV:

  tier, model, kind, source_rows, target_rows_before, target_rows_after,
  difference, paired, posted_now, held, refused, still_missing,
  mismatched_rows, patched, still_mismatched, extra_on_target, why

Steps (per model, parents first; a tier's models side by side):
  1. count the target before anything changes
  2. reload  — the copy itself: rows already on the target are matched by
               uuid (which also rebuilds the id maps here), the rest posted
  3. relink  — links whose parent came later
  4. verify  — every source row on the target by uuid, every field equal
               (links translated to target ids); differing fields patched
               (match_on uuid) and the model verified again

    python v3_to_v3_audit.py --source kisumu_v3 --target org284
    python v3_to_v3_audit.py --source kisumu_v3 --target org284 --no-reload --no-sync   # read-only
    python v3_to_v3_audit.py … --resume        # skip models a previous run already audited
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import v3_to_v3_links as L
import v3_to_v3_migration as vv
from v3_to_v3_client import SERVICES, GatewayError

log = logging.getLogger("v3_to_v3_audit")

COLS = ["tier", "model", "kind", "source_rows", "target_rows_before", "target_rows_after", "difference",
        "paired", "posted_now", "held", "refused", "still_missing", "mismatched_rows", "patched",
        "still_mismatched", "facility_differs", "extra_on_target", "mirror", "why"]


def target_catalogue(dst) -> tuple[set[str], dict[str, str]]:
    """Models the target's gateways have, and services it can't list (why)."""
    have, down = set(), {}
    for svc in SERVICES:
        try:
            have |= {f"{svc}/{m['alias']}" for m in dst.models(svc)}
        except GatewayError as e:
            down[svc] = vv.describe_error(str(e).split(": ", 1)[-1])[0]
    return have, down


def count(client, x) -> int | None:
    try:
        return client.read_page(x["service"], x["alias"], 1, per_page=1)[1].get("total")
    except GatewayError:
        return None


def _join(parts) -> str:
    return " | ".join(p for p in parts if p)


def audit_model(copier: vv.Copier, model: str, reload: bool, sync: bool) -> dict:
    x = copier.models[model]
    row = {"tier": x.get("tier"), "model": model, "kind": x["kind"]}
    row["target_rows_before"] = count(copier.dst, x)
    why, res = [], {}
    try:
        # reload: posts what's missing. read-only (map_only): just pairs rows by
        # uuid so links can be compared in target ids
        res = copier.copy_model(model)
    except GatewayError as e:
        why.append(f"reload stopped: {e}")
        res = {}
    row["posted_now"] = res.get("posted", 0)
    if res.get("errors"):
        why.append(f"{res['failed']} row(s) refused by the target: " + "; ".join(
            f"{e['count']}× {e['meaning']} (V3: {e['v3_message'][:160]}; e.g. source id {e['example']['source_id']})"
            for e in res["errors"][:4]))
    if res.get("held_because"):
        why.append("held — " + "; ".join(
            f"{h['rows']} row(s) need {model}.{c} → {h['parent']} ids {h['missing_parent_ids'][:3]} which aren't on "
            f"the target (that parent is missing there: see its own row)" for c, h in res["held_because"].items()))
    if res.get("v2_ids_not_in_map"):
        why.append("copied with an empty link — the source row holds a V2 id the V2→V3 id map doesn't have "
                   "(source data): " + ", ".join(f"{c} ×{n}" for c, n in res["v2_ids_not_in_map"].items()))
    return {"row": row, "why": why, "copy": res}


def verify_and_sync(copier: vv.Copier, model: str, entry: dict, sync: bool) -> dict:
    row, why = entry["row"], entry["why"]
    try:
        v = vv.verify_model(copier, model, collect=sync)
    except GatewayError as e:
        why.append(f"verify failed: {e}")
        row["why"] = _join(why)
        return row
    patched = 0
    mism_rows = v.get("mismatched_rows")
    if mism_rows is None:
        mism_rows = sum((v.get("mismatched_fields") or {}).values())
    if sync and v.get("_patches"):
        s = vv.sync_fields(copier, model, v.pop("_patches"))
        patched = s["patched"]
        if s["failed"]:
            why.append(f"{s['failed']} differing row(s) could not be patched: " + copier.error_summary(model, top=2)
                       .replace("\n", " "))
        if s.get("uuid_aligned"):
            why.append(f"{s['uuid_aligned']} record(s) the target already had under another uuid were rewritten to "
                       f"the source's values, uuid included (matched on their key)")
        if s.get("uuid_kept"):
            why.append(f"{s['uuid_kept']} record(s) the target already had under another uuid now carry every source "
                       f"value except the uuid: the gateway can't change a uuid on this model (uuid is the only match "
                       f"key it accepts here, and update skips it as not fillable) — needs a backend change")
        first = v
        v = vv.verify_model(copier, model)
        v["mismatch_before"] = first["mismatched_fields"]
    else:
        v.pop("_patches", None)
    src_n = v.get("source", 0)
    row.update({
        "source_rows": src_n,
        "target_rows_after": v.get("target", v.get("linked")),
        "paired": src_n - v.get("missing", 0) - v.get("source_without_uuid", 0),
        "held": entry["copy"].get("held", 0),
        "refused": entry["copy"].get("failed", 0),
        "still_missing": v.get("missing", 0),
        "mismatched_rows": mism_rows if mism_rows is not None else "",
        "patched": patched,
        "still_mismatched": sum((v.get("mismatched_fields") or {}).values()),
        "extra_on_target": v.get("extra_on_target", 0),
        "facility_differs": sum((v.get("facility_differs") or {}).values()),
    })
    t = row["target_rows_after"]
    row["difference"] = (t - src_n) if isinstance(t, int) else ""
    if x_kind(copier, model) == "reference":
        row["why"] = _join(why + [f"shared reference data: {v.get('linked')} of {src_n} rows linked to the target's own "
                                  f"rows (by uuid / {', '.join(L.REFERENCE_MATCH.get(model, ('code', 'name')))})"
                                  + (f", {v['missing']} not found and not posted" if v.get("missing") else "")])
        row["mirror"] = "no" if v.get("missing") else "yes (linked to the target's own reference rows)"
        return row
    if v.get("missing"):
        why.append(f"{v['missing']} source row(s) still not on the target — " + "; ".join(
            f"{n}× {k}" for k, n in (v.get("missing_why") or {}).items()) +
            "".join(f"; e.g. source id {e['id']} uuid {e['uuid']}" + (f" (needs {e['held_for']})" if e.get("held_for")
                                                                      else "") for e in v["missing_examples"][:2]))
    if v.get("source_without_uuid"):
        why.append(f"{v['source_without_uuid']} source row(s) have no uuid — can't be matched on the target")
    if v.get("mismatched_fields"):
        why.append("fields still different: " + "; ".join(
            f"{c} on {n} row(s) (e.g. source {ex[0]['expected']!r} vs target {ex[0]['target']!r})"
            for c, n in sorted(v["mismatched_fields"].items(), key=lambda kv: -kv[1])[:5]
            for ex in [v["mismatch_examples"].get(c) or [{}]]))
    if patched:
        why.append(f"{patched} row(s) had fields that differed and were patched to match the source: " +
                   ", ".join(f"{c} ×{n}" for c, n in (v.get("mismatch_before") or {}).items()))
    if v.get("extra_on_target"):
        why.append(f"{v['extra_on_target']} target row(s) aren't in the source (by uuid) — the target's own rows or "
                   f"rows deleted from the source since the copy; left as they are")
    if row["posted_now"]:
        why.append(f"{row['posted_now']} missing row(s) reloaded now")
    if v.get("paired_by_key"):
        pd = v.get("paired_fields_differ") or {}
        why.append(f"{v['paired_by_key']} row(s) were already in the target as its own records under other uuids "
                   f"(same username / email / code — the insert was refused as a duplicate): paired with them, so "
                   f"every link to these rows resolves" +
                   (" — fields that still differ there: " + ", ".join(f"{c} ×{n}" for c, n in
                                                                sorted(pd.items(), key=lambda kv: -kv[1])[:6])
                    if pd else ""))
    for c, n in (v.get("target_computed_differ") or {}).items():
        why.append(f"{c} differs on {n} row(s) — not stored by the target: "
                   f"{L.TARGET_COMPUTED.get(model, {}).get(c, 'computed by the target')}")
    if v.get("updated_at_only"):
        why.append(f"{v['updated_at_only']} row(s) differ only in updated_at (last-modified time): the target stamps its "
                   f"own when a row is written, and the source's changes whenever anything touches the row — every "
                   f"other field is equal, so they weren't rewritten")
    if v.get("filled_with_migration_user"):
        why.append(f"{v['filled_with_migration_user']} empty 'who' field(s) (created_by …) hold the migration account "
                   f"(user {getattr(copier.dst, "user_id", None)}): the target requires them, the source row left them empty")
    if v.get("renamed"):
        why.append(f"{v['renamed']} row(s) created with {v.get('renamed_column')} = '<source value>."
                   f"{copier.dst.organization_id}': the source's value is taken by another organisation's row on the "
                   f"target instance (unique instance-wide), everything else identical — those users log in with "
                   f"the new username")
    perfect = not (v.get("missing") or v.get("mismatched_fields") or v.get("source_without_uuid"))
    ts = v.get("facility_differs") or {}
    if ts:
        ex = next(iter((v.get("facility_example") or {}).values()), {})
        why.append(f"facility differs on {sum(ts.values())} row(s): the target holds {ex.get('target')}, the org's "
                   f"facility is {ex.get('sent')} — those rows were written by an earlier copy whose Connection had "
                   f"facility {ex.get('target')} (rows this audit inserts get {ex.get('sent')}); the gateway won't move "
                   f"them: insert keeps the facility a row was created with and update ignores facility_id as "
                   f"'not fillable' — needs a backend change (let migration updates set facility_id), then re-run "
                   f"this audit")
    if model == L.USERS:
        why.append("passwords are not copied (the gateway never returns them): each copied user got a random "
                   "password and must reset it at first login")
    caveats = ((["facility_id"] if ts else []) + (["records paired by key under other uuids"]
                                                  if v.get("paired_by_key") else [])
               + ([f"{v['renamed']} renamed {v.get('renamed_column')}"] if v.get("renamed") else []))
    row["mirror"] = "no" if not perfect else f"yes, except {' and '.join(caveats)}" if caveats else "yes"
    if perfect:
        why.insert(0, "mirrors the source: every row present, every field equal")
    row["why"] = _join(why)
    return row


def x_kind(copier, model):
    return copier.models[model]["kind"]


def not_copied_row(model: str, x: dict, dst, registered: set, down: dict) -> dict:
    svc = x["service"]
    row = {"tier": x.get("tier") or "", "model": model, "kind": x["kind"], "source_rows": x.get("count") or 0,
           "target_rows_before": "", "target_rows_after": ""}
    if x["kind"] == "skip":
        why = f"not copied by design — {L.SKIP_MODELS.get(model)}"
    elif x["kind"] == "error":
        why = f"source can't be read: {vv.describe_error(str(x.get('error')).split(': ', 1)[-1])[0]}"
        row["source_rows"] = ""
    elif x["kind"] == "empty":
        why = "the source org has no rows — nothing to copy"
    elif svc in down:
        why = f"the target's {svc} gateway can't be listed: {down[svc]}"
    elif model not in registered:
        why = (f"REFUSED: the target instance's {svc} gateway has no model '{x['alias']}' (\"Model [{x['alias']}] is "
               f"not registered\") — its backend build lacks it; the backend team must register it, then re-run "
               f"this audit to load the {x.get('count') or 0} row(s)")
    else:
        why = "not selected"
    if model in registered and x["kind"] in ("skip", "empty"):
        row["target_rows_after"] = count(dst, x)
    row["why"] = why
    row["mirror"] = "n/a" if x["kind"] in ("skip", "empty") else "no"
    return row


# ─── per-model steps (the CLI below and dags/v3_to_v3_audit_dag.py) ────────
# Each step leaves its result in the pair's state folder, so the steps can run
# in separate processes (Airflow tasks): audit_entry/<model> after reload +
# relink, audit_row/<model> after verify.

def setup(source: str, target: str, *, workers: int = 24, read_workers: int = 8, reload: bool = True,
          refresh: bool = False):
    src, dst = vv.clients(source, target)
    state = vv.State(source, target)
    plan = vv.load_or_build_plan(src, state, refresh=refresh)
    copier = vv.Copier(src, dst, state, plan, workers=workers, read_workers=read_workers,
                       v2_map=vv.load_v2_map(source))
    copier.map_only = not reload
    return src, dst, state, plan, copier


def saved_rows(state: vv.State) -> dict[str, dict]:
    """Every model's audit row so far (this and earlier runs)."""
    rows = dict(state.load("audit_v3v3", "rows", {}))
    d = state.dir / "audit_row"
    if d.exists():
        for p in d.glob("*.json"):
            r = json.loads(p.read_text())
            rows[r["model"]] = r
    return rows


def mirrored(row: dict | None) -> bool:
    return bool(row) and str(row.get("mirror", "")).startswith("yes")


def reload_step(copier: vv.Copier, model: str) -> dict:
    try:
        entry = audit_model(copier, model, not copier.map_only, True)
    except Exception as e:                       # one model's failure never stops the others
        log.exception("%s: reload failed", model)
        x = copier.models[model]
        entry = {"row": {"tier": x.get("tier"), "model": model, "kind": x["kind"]},
                 "why": [f"reload failed: {type(e).__name__}: {str(e)[:300]} — re-run the audit for this model"],
                 "copy": {}}
    copier.state.save("audit_entry", model, entry)
    return entry


def relink_step(copier: vv.Copier, model: str) -> None:
    entry = copier.state.load("audit_entry", model, None) or {"row": {"model": model}, "why": [], "copy": {}}
    try:
        r = copier.relink(model)
        if r.get("still_missing"):
            entry["why"].append(f"{r['still_missing']} row(s) keep an empty optional link — its parent isn't on "
                                f"the target (not the source org's, or refused)")
    except GatewayError as e:
        entry["why"].append(f"relink stopped: {e}")
    copier.state.save("audit_entry", model, entry)


def verify_step(copier: vv.Copier, model: str, sync: bool = True) -> dict:
    entry = copier.state.load("audit_entry", model, None) or {"row": {"model": model, "tier": copier.models[model].get(
        "tier"), "kind": copier.models[model]["kind"]}, "why": [], "copy": {}}
    try:
        row = verify_and_sync(copier, model, entry, sync)
    except Exception as e:                       # one model's failure never stops the others
        log.exception("%s: verify failed", model)
        row = {**entry["row"], "mirror": "no",
               "why": _join(entry["why"] + [f"verify failed: {type(e).__name__}: {str(e)[:300]} — re-run the "
                                             f"audit for this model"])}
    copier.state.save("audit_row", model, row)
    return row


def write_report(state: vv.State, plan: dict, dst, out: Path, only: list[str] | None = None) -> Path:
    """The CSV: every model's audit row, plus rows for what isn't copied (and why)."""
    models = plan["models"]
    registered, down = target_catalogue(dst)
    rows = {m: r for m, r in saved_rows(state).items() if m in models and (not only or m in only)}
    for m, x in models.items():
        if m not in rows and (not only or m in only):
            rows[m] = not_copied_row(m, x, dst, registered, down)
    for m, why in getattr(L, "NOT_IN_GATEWAY", {}).items():
        rows.setdefault(m, {"model": m, "kind": "no gateway model", "mirror": "no",
                            "why": f"REFUSED: not reachable through any gateway — {why}"})
    out.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(rows.values(), key=lambda r: (r.get("tier") in ("", None), r.get("tier") or 0, r["model"]))
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(ordered)
        # totals, like the V2 → V3 reconcile summary
        f.write("\n")
        t = csv.writer(f)
        t.writerow(["summary", "models", "source_rows", "target_rows_after", "posted_now", "patched",
                    "still_missing", "still_mismatched", "mirrored_models", "not_mirrored_models"])
        num = lambda k: sum(r.get(k) or 0 for r in ordered if isinstance(r.get(k), int))
        copied = [r for r in ordered if r.get("kind") in ("copy", "org_rows", "parent_rows", "reference")]
        t.writerow(["all", len(ordered), num("source_rows"), num("target_rows_after"), num("posted_now"),
                    num("patched"), num("still_missing"), num("still_mismatched"),
                    sum(1 for r in copied if mirrored(r)), sum(1 for r in copied if not mirrored(r))])
    return out


def write_interim(state: vv.State, plan: dict, out: Path, pending: list[str], note: str = "") -> Path:
    """The CSV as it stands mid-run (no network): finished models with their result,
    the rest marked pending — rewritten after every model, so it's always current."""
    models = plan["models"]
    rows = {m: r for m, r in saved_rows(state).items() if m in models}
    for m in pending:
        if m not in rows or not mirrored(rows.get(m)):
            prev = rows.get(m) or {}
            rows[m] = {**{k: prev.get(k, "") for k in COLS}, "tier": models[m].get("tier"), "model": m,
                       "kind": models[m]["kind"], "source_rows": prev.get("source_rows") or models[m].get("count"),
                       "mirror": "pending",
                       "why": "verify + patch in progress / queued" + (f" — last result: {prev['why']}"
                                                                        if prev.get("why") else "")}
    for m, x in models.items():
        if m not in rows and x["kind"] in ("skip", "error", "empty"):
            rows[m] = {"tier": x.get("tier") or "", "model": m, "kind": x["kind"], "mirror": "n/a",
                       "why": L.SKIP_MODELS.get(m) or (f"source can't be read: {str(x.get('error'))[:200]}"
                                                       if x["kind"] == "error" else "the source org has no rows")}
    out.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(rows.values(), key=lambda r: (r.get("tier") in ("", None), r.get("tier") or 0, r["model"]))
    tmp = out.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(ordered)
        f.write("\n")
        t = csv.writer(f)
        t.writerow(["summary", "as_of", "models", "mirrored", "not_mirrored", "pending", "posted_now", "patched",
                    "still_missing", "note"])
        cnt = lambda pred: sum(1 for r in ordered if pred(r))
        num = lambda k: sum(r.get(k) or 0 for r in ordered if isinstance(r.get(k), int))
        t.writerow(["all", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), len(ordered),
                    cnt(mirrored), cnt(lambda r: r.get("mirror") == "no"), cnt(lambda r: r.get("mirror") == "pending"),
                    num("posted_now"), num("patched"), num("still_missing"), note])
    tmp.replace(out)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("--no-reload", action="store_true", help="don't post missing rows")
    ap.add_argument("--no-sync", action="store_true", help="don't patch differing fields")
    ap.add_argument("--models", nargs="*", default=None)
    ap.add_argument("--workers", type=int, default=24)
    ap.add_argument("--read-workers", type=int, default=8)
    ap.add_argument("--parallel-models", type=int, default=3)
    ap.add_argument("--resume", action="store_true", help="reuse models a previous run of this audit finished")
    ap.add_argument("--skip-reload", action="store_true",
                    help="straight to verify + patch, with the id maps an earlier reload saved")
    ap.add_argument("--heaviest-first", action="store_true", help="verify + patch the biggest tables first")
    ap.add_argument("--reload-only", nargs="*", default=None,
                    help="with --skip-reload: still reload these models first (e.g. core/users)")
    ap.add_argument("--out", type=Path, default=None)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    t0 = time.time()
    reload, sync = not a.no_reload, not a.no_sync

    src, dst, state, plan, copier = setup(a.source, a.target, workers=a.workers, read_workers=a.read_workers,
                                          reload=reload, refresh=True)
    vv.preflight(src)
    vv.preflight(dst)
    models = plan["models"]
    registered, _ = target_catalogue(dst)
    todo = sorted((m for m, x in models.items() if x["selected"] and m in registered
                   and (not a.models or m in a.models)), key=lambda m: (models[m]["tier"] or 0, m))
    # resume: models a previous run already brought to a mirror are kept; everything else is redone
    prev = saved_rows(state) if a.resume else {}
    run = [m for m in todo if not mirrored(prev.get(m))]
    if not a.resume:                        # a fresh audit: no row of an earlier run may stand in for this one
        for d in ("audit_row", "audit_entry"):
            for p in (state.dir / d).glob("*.json") if (state.dir / d).exists() else ():
                p.unlink()
        (state.dir / "audit_v3v3" / "rows.json").unlink(missing_ok=True)

    # 1–2: reload, tier by tier (a tier's models share no links)
    to_reload = [m for m in run if m in (a.reload_only or [])] if a.skip_reload else run
    for tier in sorted({models[m]["tier"] for m in to_reload}):
        batch = [m for m in to_reload if models[m]["tier"] == tier]
        log.info("──── tier %s: %s", tier, ", ".join(batch))
        with ThreadPoolExecutor(a.parallel_models) as pool:
            list(pool.map(lambda m: reload_step(copier, m), batch))
    # 3: relink (cycles, parents that came later)
    if reload:
        for m in to_reload:
            relink_step(copier, m)
    # 4: verify + patch
    order = sorted(run, key=lambda m: -(models[m].get("count") or 0)) if a.heaviest_first else run
    log.info("──── verify + patch: %s", ", ".join(f"{m}({models[m].get('count')})" for m in order[:12]))
    with ThreadPoolExecutor(max(1, a.parallel_models - 1)) as pool:        # verify holds a table in memory
        out = a.out or Path("output") / (f"v3v3-{a.source}-to-{a.target}-audit-"
                                         f"{datetime.now(timezone.utc):%Y%m%d-%H%M%S}.csv")
        left = list(order)
        write_interim(state, plan, out, left, "in progress")
        for m, r in zip(order, pool.map(lambda m: verify_step(copier, m, sync), order)):
            left.remove(m)
            write_interim(state, plan, out, left, "in progress")
            log.info("%s: %s", m, {k: r.get(k) for k in ("source_rows", "target_rows_after", "still_missing",
                                                           "still_mismatched", "posted_now", "patched", "mirror")})
    out = a.out or Path("output") / (f"v3v3-{a.source}-to-{a.target}-audit-"
                                     f"{datetime.now(timezone.utc):%Y%m%d-%H%M%S}.csv")
    write_report(state, plan, dst, out, only=a.models)
    log.info("audit written: %s (%.0f min)", out, (time.time() - t0) / 60)
    print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
