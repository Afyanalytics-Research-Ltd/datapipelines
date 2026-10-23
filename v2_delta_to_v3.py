"""
v2_delta_to_v3.py — bring a V3 organisation that was copied from a V2-fed organisation up to date with V2,
straight away: every V2 record created — or changed — on/after a date goes into the TARGET org.

Used at go-live, when a facility kept working in V2 after the migration (Kisumu: V2 → V3 org 4 on the migrate
servers, then org 4 → production org 284; the delta goes V2 → org 284 directly).

  source key (kisumu_v3)  the V2 load that built the copied org: its V2 connection, Snowflake schemas,
                          transform rules and STABLE UUIDS — a record gets the uuid it already has there
  target key (org284)     the org to update: AFYA_<TARGET>_USERNAME / _PASSWORD / _ORGANIZATION_ID /
                          _FACILITY_ID / _URL_TEMPLATE / _MIGRATION_KEY (or Connection afya_v3_<target>)
  links                   V2 id → source-org V3 id (the V2 load's id map) → target id (the V3 → V3 copy's
                          id maps), chained into a delta state of its own; the real states aren't touched
  rows                    the live V2 API (the only up-to-date copy): created_at or updated_at ≥ since
                          (new_only: just rows created since — fast, reads only the API's newest pages;
                          with edits the whole table is read, the API can't filter by date: ~200 rows/s)
  posting                 a record the target already has (by uuid, by the chained id map, or by content)
                          is UPDATED in place under the target's uuid; a new one is INSERTED. An edited
                          record created before `since` that can't be matched is NOT inserted (it would
                          be a duplicate) — it's reported.

    python v2_delta_to_v3.py --source kisumu_v3 --target org284 --since "2026-10-16" --dry-run
    python v2_delta_to_v3.py --source kisumu_v3 --target org284 --since "2026-10-16 08:00" [--new-only]

Writes output/v2_delta/<source>-to-<target>-since-<date>-<time>.csv (per table: V2 rows changed, updated,
inserted, held, failed, and why).
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import shutil
import sys
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "dags"))

log = logging.getLogger("v2_delta_to_v3")

# the target's settings the source key's V3 side is pointed at (the V2 side stays the source's)
TARGET_SETTINGS = ("USERNAME", "PASSWORD", "ORGANIZATION_ID", "FACILITY_ID", "URL_TEMPLATE", "MIGRATION_KEY",
                   "CORE_URL", "RECEPTION_URL", "EVALUATION_URL", "INVENTORY_URL", "FINANCE_URL", "THEATRE_URL",
                   "INPATIENT_URL", "DIALYSIS_URL")


# ─── state: the V2 load's id map, chained through the V3 → V3 copy to the target ────────────────

def compose_id_map(v2_map: dict[str, dict], v3v3_maps: dict[str, dict], alias_service: dict[str, str]) \
        -> tuple[dict[str, dict], dict[str, int], dict[str, str]]:
    """{alias: {V2 id: target id}} = V2 load's {alias: {V2 id: source V3 id}} ∘ V3→V3 {"svc/alias": {source
    V3 id: target id}}. Returns (map, entries dropped per alias — their parent isn't on the target,
    {alias: service} — the service the copy used for each alias)."""
    by_alias: dict[str, list[str]] = defaultdict(list)
    for model in v3v3_maps:
        if v3v3_maps[model]:                    # an empty map (the source had no rows) says nothing
            by_alias[model.split("/", 1)[1]].append(model)
    out, dropped, services = {}, {}, {}
    for alias, m in v2_map.items():
        models = by_alias.get(alias, [])
        model = next((x for x in models if x.split("/", 1)[0] == alias_service.get(alias)), None) or \
            (models[0] if len(models) == 1 else None)
        if not model:
            dropped[alias] = len(m)
            continue
        services[alias] = model.split("/", 1)[0]
        hop = v3v3_maps[model]
        out[alias] = {k: hop[str(v)] for k, v in m.items() if v is not None and str(v) in hop}
        if len(out[alias]) < len(m):
            dropped[alias] = len(m) - len(out[alias])
    return out, dropped, services


def build_delta_state(source: str, target: str, delta_root: Path) -> Path:
    """delta_root/<source>/: the source's V2-load state (visit maps …) with its id map chained to the
    target. Rebuilt from scratch each run — the id maps it chains are the source of truth."""
    import v2_to_v3_api_migration as v2v3
    import v3_to_v3_migration as vv
    src_state = Path(v2v3.STATE_ROOT) / source
    if not (src_state / ".migration_id_map.json").exists():
        raise SystemExit(f"no V2→V3 id map at {src_state} — the V2 load's state for {source} is needed")
    v3v3 = vv.State(source, target)
    if not (v3v3.dir / "id_map").exists():
        raise SystemExit(f"no V3→V3 id maps at {v3v3.dir} — run the v3_to_v3 copy/audit {source} → {target} first")
    dst = delta_root / source
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)
    for f in src_state.glob(".migration_visit_*.json"):          # V2 → V2 maps: valid as they are
        shutil.copy2(f, dst / f.name)
    v2_map = json.loads((src_state / ".migration_id_map.json").read_text())
    v3v3_maps = {p.stem.replace("__", "/"): json.loads(p.read_text()) for p in (v3v3.dir / "id_map").glob("*.json")}
    composed, dropped, services = compose_id_map(v2_map, v3v3_maps, {})
    (dst / ".migration_id_map.json").write_text(json.dumps(composed))
    # an alias several target services share (patient: reception AND dialysis) goes where the copy put it
    (dst / ".delta_alias_services.json").write_text(json.dumps(services))
    log.info("delta state %s: %d aliases chained to %s (%d V2 ids), %s without a target counterpart",
             dst, len(composed), target, sum(len(v) for v in composed.values()),
             {a: n for a, n in dropped.items() if n} or "none")
    return dst


def point_v3_side_at(source: str, target: str) -> None:
    """In this process only: the source key's V3 settings = the target's (its V2 side stays)."""
    s, t = source.upper(), target.upper()
    for k in TARGET_SETTINGS:
        v = os.getenv(f"AFYA_{t}_{k}")
        if v:
            os.environ[f"AFYA_{s}_{k}"] = v
        else:
            os.environ.pop(f"AFYA_{s}_{k}", None)
    key = os.getenv(f"AFYA_{t}_MIGRATION_KEY")
    if key:
        os.environ["MODEL_GATEWAY_MIGRATION_KEY"] = key
    for k in ("USERNAME", "PASSWORD", "ORGANIZATION_ID"):
        if not os.getenv(f"AFYA_{t}_{k}"):
            raise SystemExit(f"target {target}: set AFYA_{t}_{k} (or the Connection afya_v3_{target})")


# ─── which payloads update what, and which are new ─────────────────────────────────────────────

def plan_delta(payloads: list[dict], existing: list[dict], id_map: dict[int, int], key, since: str) \
        -> tuple[list[tuple[dict, int | None]], Counter]:
    """[(payload to post, target id or None for an insert)] and a Counter of what was decided.
    A record the target has (uuid / chained id map / content) is posted under the target's uuid, which
    updates it in place; one created since `since` is inserted; an edited older one nobody can match is
    skipped — inserting it could duplicate a record the target holds under another uuid."""
    from fast_v2_live_to_v3 import _s, _when
    uuid_of = {r["id"]: str(r.get("uuid") or "") for r in existing}
    have_uuid = {u.lower(): i for i, u in uuid_of.items() if u}
    have_key: dict[tuple, list] = defaultdict(list)
    if key:
        for r in sorted(existing, key=lambda x: int(x["id"])):
            have_key[key(r)].append(r["id"])
        for p in payloads:                      # rows matched by uuid don't use up a content match
            u = str(p.get("uuid") or "").lower()
            if u in have_uuid and have_uuid[u] in have_key.get(key(p), []):
                have_key[key(p)].remove(have_uuid[u])
    out, n, seen = [], Counter(), set()
    s = _when(since) or since
    for p in payloads:
        u = str(p.get("uuid") or "").lower()
        if u and u in seen:
            continue
        seen.add(u)
        v2 = int(p["id"]) if str(p.get("id") or "").isdigit() else None
        tid = have_uuid.get(u)
        how = "update (uuid)"
        if tid is None and v2 is not None and id_map.get(v2) in uuid_of:
            tid, how = id_map[v2], "update (id map)"
        if tid is None and key and have_key.get(key(p)):
            tid, how = have_key[key(p)].pop(0), "update (content)"
        if tid is not None:
            out.append(({**p, "uuid": uuid_of[tid] or p.get("uuid")}, tid))
            n[how] += 1
        elif (_when(p.get("created_at")) or "") >= s:
            out.append((p, None))
            n["insert"] += 1
        else:
            n["skip: edited, not found in target"] += 1
    return out, n


# ─── the loader ────────────────────────────────────────────────────────────────────────────────

def make_loader(source: str, target: str, since: str, new_only: bool, workers: int, page_workers: int,
                dry_run: bool, delta_root: Path):
    from pipelines_common import use_pipelines_dir
    use_pipelines_dir([source, target])
    point_v3_side_at(source, target)
    import v2_to_v3_api_migration as v2v3
    import fast_v2_live_to_v3 as fl
    build_delta_state(source, target, delta_root)
    v2v3.STATE_ROOT = delta_root                          # every read/write of state goes to the delta copy

    class DeltaLoader(fl.Loader):
        pinned: dict[str, str] = {}

        def capture(self, table):
            # the migration may rebuild its alias → service lookup from the target's catalogue on any
            # table: pin the shared aliases again (and after, for the post that follows)
            self.m._alias_to_service.update(self.pinned)
            try:
                return super().capture(table)
            finally:
                self.m._alias_to_service.update(self.pinned)

        def post(self, table, svc, alias, ns, payloads, held):
            if alias in self.pinned and svc != self.pinned[alias]:
                # the target lists this alias in several services (patient: reception + dialysis): use
                # the one the copy put these records in
                log.info("%s: %s routed to %s (the copy's service), not %s", table, alias, self.pinned[alias], svc)
                svc = self.pinned[alias]
            existing = self.v3_rows(svc, alias)
            key = fl.CONTENT_KEY.get(alias) or self.natural_key(payloads, existing)
            id_map = {**{int(k): int(v) for k, v in (self.m._id_map.get(alias) or {}).items()
                         if str(k).isdigit() and str(v).isdigit()}, **self.extra.get(alias, {})}
            todo, decided = plan_delta(payloads, existing, id_map, key, self.since)
            rep = {"table": table, "v3": f"{svc}/{alias}", "v2_changed": self._live_rows, "transformed": len(payloads),
                   "held": held, "held_why": dict(self._held_why.most_common(8)), "decided": dict(decided),
                   "updated": 0, "inserted": 0, "failed": 0, "duplicate_key": 0, "failed_why": {}, "status": "",
                   # the base loader's end-of-run summary reads these
                   "in_v3_by_uuid": sum(v for k, v in decided.items() if k.startswith("update")),
                   "in_v3_by_content": 0, "to_post": len(todo), "posted": 0}
            self.report.append(rep)
            log.info("%s → %s: %d changed in V2, %d transformed, held %d — %s", table, alias, self._live_rows,
                     len(payloads), held, dict(decided))
            if self.dry_run or not todo:
                rep["status"] = "dry run" if self.dry_run else "nothing to post"
                self._save_report()
                return
            lock, why = threading.Lock(), Counter()
            real_dead_letter = self.m._write_dead_letter

            def dead_letter(v3_namespace, record, error_body):
                with lock:
                    why[fl._reason(error_body)] += 1
                real_dead_letter(v3_namespace, record, error_body)

            def one(job):
                p, tid = job
                try:
                    v3_id = self.m._post_to_v3_batch(ns, self.org, p, alias_override=alias, service_override=svc)
                    ok = "updated" if tid is not None else "inserted" if v3_id else "duplicate_key"
                    if v3_id and str(p.get("id") or "").isdigit():
                        with lock:
                            self.extra[alias][int(p["id"])] = int(v3_id)
                except self.m.RecordDeadLettered:
                    ok = "failed"
                except Exception as e:                    # network, after the poster's retries
                    with lock:
                        why[fl._reason(f"{type(e).__name__}: {e}")] += 1
                    ok = "failed"
                with lock:
                    rep[ok] += 1

            self.m._write_dead_letter = dead_letter
            try:
                from concurrent.futures import ThreadPoolExecutor
                with ThreadPoolExecutor(self.workers) as ex:
                    list(ex.map(one, todo))
            finally:
                self.m._write_dead_letter = real_dead_letter
            rep["failed_why"] = dict(why.most_common(8))
            rep["posted"] = rep["updated"] + rep["inserted"]
            rep["status"] = "posted"
            self._save_report()
            log.info("%s → %s: %d updated, %d inserted, %d already there (unique key), %d failed", table, alias,
                     rep["updated"], rep["inserted"], rep["duplicate_key"], rep["failed"])

        def persist_new_ids(self) -> None:
            """New target ids into the delta id map, so later tables' children link to them."""
            p = Path(self.m.STATE_ROOT) / self.facility / ".migration_id_map.json"
            m = json.loads(p.read_text()) if p.exists() else {}
            for alias, ids in self.extra.items():
                m.setdefault(alias, {}).update({str(k): v for k, v in ids.items()})
            p.write_text(json.dumps(m))

    loader = DeltaLoader(source, workers, page_workers, dry_run, 0, live=True, report_path=None)
    loader.since, loader.new_only = since, new_only
    # the target's catalogue lists some aliases in several services (patient: reception and dialysis);
    # the loader resolves an alias through _alias_to_service — pin each to the service the copy used
    pinned = json.loads((delta_root / source / ".delta_alias_services.json").read_text())
    clash = {a: (v2v3._alias_to_service.get(a), s) for a, s in pinned.items() if v2v3._alias_to_service.get(a) != s}
    v2v3._alias_to_service.update(pinned)
    loader.pinned = pinned
    if clash:
        log.info("aliases pinned to the copy's service (target catalogue said otherwise): %s", clash)
    return loader


def default_tables(source: str) -> list[str]:
    """Every table the V2 load migrates for the source, parents first."""
    import snowflake_to_v3_migration as s2v3
    return [j["table"] for j in s2v3.plan_table_jobs(source)["jobs"]]


def write_csv(report: list[dict], out: Path, since: str, mode: str) -> Path:
    cols = ["table", "v3_model", "v2_changed", "transformed", "updated", "inserted", "already_there", "held",
            "skipped", "failed", "why"]
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in report:
            d = r.get("decided") or {}
            why = []
            if r.get("held"):
                why.append(f"{r['held']} held — their parent isn't in the target: "
                           + "; ".join(f"{n}× {k}" for k, n in (r.get("held_why") or {}).items()))
            if d.get("skip: edited, not found in target"):
                why.append(f"{d['skip: edited, not found in target']} edited V2 record(s) created before {since} "
                           f"aren't in the target under any uuid / id / content match — not inserted (could "
                           f"duplicate)")
            if r.get("failed"):
                why.append(f"{r['failed']} refused by V3: "
                           + "; ".join(f"{n}× {k}" for k, n in (r.get("failed_why") or {}).items()))
            changed = r.get("v2_changed", r.get("live_v2"))
            if changed == 0:
                why.append(f"no V2 record created or changed since {since}")
            elif r.get("status", "").startswith(("error", "nothing")):
                why.append(r["status"])
            w.writerow({"table": r["table"], "v3_model": r.get("v3", ""), "v2_changed": changed if changed is not None else "",
                        "transformed": r.get("transformed", ""), "updated": r.get("updated", ""),
                        "inserted": r.get("inserted", ""), "already_there": r.get("duplicate_key", ""),
                        "held": r.get("held", ""), "skipped": d.get("skip: edited, not found in target", ""),
                        "failed": r.get("failed", ""), "why": " | ".join(why) or "all changes applied"})
        f.write(f"\nsince,{since}\nmode,{mode}\nwritten,{datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}\n")
    return out


def run(source: str, target: str, since: str, new_only: bool = False, tables: list[str] | None = None,
        workers: int = 32, page_workers: int = 32, dry_run: bool = False, out: Path | None = None) -> Path:
    delta_root = ROOT / ".migration_state" / f"v2delta_{source}_to_{target}"
    loader = make_loader(source, target, since, new_only, workers, page_workers, dry_run, delta_root)
    tables = tables or default_tables(source)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    out = out or ROOT / "output" / "v2_delta" / f"{source}-to-{target}-since-{since[:10]}-{stamp}.csv"
    # the loader saves the JSON report while the first table posts — before write_csv() has made the folder
    out.parent.mkdir(parents=True, exist_ok=True)
    loader.report_path = out.with_suffix(".json")
    log.info("delta %s → %s since %s (%s), %d table(s)%s", source, target, since,
             "new only" if new_only else "new + edits", len(tables), " — DRY RUN" if dry_run else "")
    t0 = time.time()
    for table in tables:                                  # parents first: children link to new parents
        loader.run([table])
        if not dry_run:
            loader.persist_new_ids()
        write_csv(loader.report, out, since, "new only" if new_only else "new + edits")
    log.info("delta done in %.0f min: %s", (time.time() - t0) / 60, out)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", default="kisumu_v3")
    ap.add_argument("--target", required=True)
    ap.add_argument("--since", required=True, help='"YYYY-MM-DD" or "YYYY-MM-DD HH:MM" (V2 local time)')
    ap.add_argument("--new-only", action="store_true", help="only records created since (fast)")
    ap.add_argument("--tables", nargs="*", default=None, help="V2 tables (default: all the load migrates)")
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--page-workers", type=int, default=32)
    ap.add_argument("--dry-run", action="store_true", help="read + transform + decide; post nothing")
    ap.add_argument("--out", type=Path, default=None)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run(a.source, a.target, a.since, a.new_only, a.tables, a.workers, a.page_workers, a.dry_run, a.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
