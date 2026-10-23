"""
fix_visit_destination_slugs.py — set visit_destinations.destination_slug from each row's destination
(core service_destinations.slug), through the gateway (2026-10-15, once VisitDestination's $fillable
took destination_slug). One update per destination, ids in chunks; rows already right are skipped.

    python fix_visit_destination_slugs.py --dry-run
    python fix_visit_destination_slugs.py
"""
from __future__ import annotations

import argparse
import logging
from collections import defaultdict

import fast_v2_live_to_v3 as F

log = logging.getLogger("fix_visit_destination_slugs")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--facility", default="kisumu_v3")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--chunk", type=int, default=200)
    ap.add_argument("--workers", type=int, default=4, help="parallel update calls")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    L = F.Loader(a.facility, 4, 4, True, 0)
    org = L.org["organization_id"]
    slugs = {int(d["id"]): d.get("slug") for d in L.v3_rows("core", "service_destinations") if d.get("slug")}
    todo: dict[int, list[int]] = defaultdict(list)
    right = no_dest = 0
    for r in L.v3_rows("reception", "visit_destination"):
        dest = int(r["destination_id"]) if r.get("destination_id") else None
        if dest is None or dest not in slugs:
            no_dest += 1
        elif r.get("destination_slug") == slugs[dest]:
            right += 1
        else:
            todo[dest].append(int(r["id"]))
    n = sum(len(v) for v in todo.values())
    log.info("already right: %d | no destination/slug: %d | to set: %d over %d destinations", right, no_dest, n, len(todo))
    if a.dry_run or not n:
        return
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor
    lock = threading.Lock()
    tally = {"done": 0, "failed": 0}

    def update(job):
        dest, chunk = job
        body = {"action": "update", "model": "visit_destination", "source_tenant_id": org,
                "destination_tenant_id": org, "ids": chunk, "data": {"destination_slug": slugs[dest]}}
        for attempt in range(5):                     # a dropped connection / 5xx is retried, not fatal
            try:
                r = L.m._gateway_post("reception", body, timeout=180)
                if r.status_code < 500:
                    break
            except Exception as e:                   # noqa: BLE001
                r = None
                log.warning("  %s: %s — retry %d", slugs[dest], str(e)[:120], attempt + 1)
            time.sleep(5 * (attempt + 1))
        res = r.json() if r is not None and r.ok else {}
        ok = r is not None and r.ok and not any((x or {}).get("ignored") for x in (res.get("data") or []))
        with lock:
            if ok:
                s = res.get("summary") or {}
                tally["done"] += s.get("updated", 0) + s.get("unchanged", 0)
            else:
                tally["failed"] += len(chunk)
                log.error("  %s: %s", slugs[dest], (r.text[:300] if r is not None else "no response"))
            n = tally["done"] + tally["failed"]
            if n % 10000 < len(chunk):
                log.info("  %d / %d", n, sum(len(v) for v in todo.values()))

    jobs = [(dest, ids[i:i + a.chunk]) for dest, ids in sorted(todo.items()) for i in range(0, len(ids), a.chunk)]
    with ThreadPoolExecutor(a.workers) as ex:
        list(ex.map(update, jobs))
    log.info("destination_slug set on %d visit destinations, %d failed", tally["done"], tally["failed"])


if __name__ == "__main__":
    main()
