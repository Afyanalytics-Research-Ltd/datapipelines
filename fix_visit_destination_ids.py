"""
fix_visit_destination_ids.py — set destination_id on V3 visit destinations already loaded (2026-10-15).

The migration's keep-list left destination_id out, so every migrated visit destination has none.
The keep-list and FK map are fixed (v2_to_v3_api_migration: V2 settings destination → V3 service
destination). This corrects the rows already in V3: each V2 row is paired with its V3 row exactly as
fast_v2_live_to_v3.py pairs them (uuid, id map, content), and V3 rows whose destination_id differs
get it through the gateway's update action — one call per destination, ids in chunks.

    python fix_visit_destination_ids.py --dry-run
    python fix_visit_destination_ids.py
"""
from __future__ import annotations

import argparse
import logging
from collections import defaultdict

import fast_v2_live_to_v3 as F

log = logging.getLogger("fix_visit_destination_ids")
TABLE, ALIAS, SERVICE = "evaluation_visit_destinations", "visit_destination", "reception"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--facility", default="kisumu_v3")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--chunk", type=int, default=200)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    L = F.Loader(a.facility, 8, 16, True, 0)          # dry run: post() only pairs rows, posts nothing
    captured, held = L.capture(TABLE)
    payloads = [p for (svc, alias, ns), ps in captured.items() if alias == ALIAS for p in ps]
    for (svc, alias, ns), ps in captured.items():
        if alias == ALIAS:
            L.post(TABLE, svc, alias, ns, ps, held)     # fills L.extra[ALIAS]: V2 id → V3 id
    pairs = L.extra.get(ALIAS, {})
    dest_by_v2 = {int(p["id"]): p.get("destination_id") for p in payloads if str(p.get("id") or "").isdigit()}
    current = {int(r["id"]): r.get("destination_id") for r in L.v3_rows(SERVICE, ALIAS)}

    todo: dict[int, list[int]] = defaultdict(list)
    no_dest = same = 0
    exact: dict[int, int] = {}
    for v2_id, v3_id in pairs.items():
        dest = dest_by_v2.get(v2_id)
        if dest is None:
            no_dest += 1
        elif current.get(int(v3_id)) == dest:
            same += 1
        else:
            exact[int(v3_id)] = int(dest)
            todo[int(dest)].append(int(v3_id))
    # V3 rows no V2 row pairs with (earlier loads; their visits are gone from V2): the service destination
    # whose name is the row's department_name — the same name every load wrote. Only names that are unique
    # among V3 service destinations, and only if name matching agrees with the exact V2 pairing.
    dests = L.v3_rows("core", "service_destinations")
    names = defaultdict(set)
    for d in dests:
        names[F._s(d.get("name"))].add(int(d["id"]))
    by_name = {k: next(iter(v)) for k, v in names.items() if k and len(v) == 1}
    rows = {int(r["id"]): r for r in L.v3_rows(SERVICE, ALIAS)}
    agree = sum(1 for v3_id, dest in exact.items() if by_name.get(F._s(rows.get(v3_id, {}).get("department_name"))) == dest)
    log.info("name matching agrees with the exact V2 pairing on %d of %d paired rows", agree, len(exact))
    paired_v3 = set(map(int, pairs.values()))
    by_name_n = no_name = 0
    if exact and agree >= 0.98 * len(exact):
        for v3_id, r in rows.items():
            if v3_id in paired_v3 or r.get("destination_id"):
                continue
            dest = by_name.get(F._s(r.get("department_name")))
            if dest:
                todo[dest].append(v3_id)
                by_name_n += 1
            else:
                no_name += 1
    else:
        log.warning("name matching disagrees with the V2 pairing too often — unpaired rows left alone")
    n = sum(len(v) for v in todo.values())
    log.info("V3 rows paired with V2: %d | no V3 destination for its V2 one: %d | already right: %d | "
             "set from V2: %d | set by name (unpaired rows): %d | unpaired, no matching name: %d | "
             "to set: %d over %d destinations", len(pairs), no_dest, same, len(exact), by_name_n, no_name,
             n, len(todo))
    if a.dry_run or not n:
        return
    org = L.org["organization_id"]
    done = failed = 0
    for dest, ids in sorted(todo.items()):
        for i in range(0, len(ids), a.chunk):
            chunk = ids[i:i + a.chunk]
            body = {"action": "update", "model": ALIAS, "source_tenant_id": org, "destination_tenant_id": org,
                    "ids": chunk, "data": {"destination_id": dest}}
            r = L.m._gateway_post(SERVICE, body, timeout=120)
            res = r.json() if r.ok else {}
            if not r.ok or (res.get("data") or [{}])[0].get("ignored"):
                failed += len(chunk)
                log.error("  destination %s: %s %s", dest, r.status_code, r.text[:300])
            else:
                done += (res.get("summary") or {}).get("updated", len(chunk))
        log.info("  destination %s: %d rows", dest, len(ids))
    log.info("destination_id set on %d V3 visit destinations, %d failed", done, failed)


if __name__ == "__main__":
    main()
