"""
fix_theatre_scheduled_date.py — fill V3 theatre_schedules.scheduled_date where it's empty (2026-10-15).

V2 schedules carry no date of their own, so migrated rows have scheduled_date empty. The day is the
day the schedule starts: scheduled_date = the date part of schedule_start_time (the migration now
injects the same for new rows — v2_to_v3_api_migration _PER_KEY_INJECT["theatre_schedule"]). One
gateway update per date, ids in chunks.

    python fix_theatre_scheduled_date.py --dry-run
    python fix_theatre_scheduled_date.py
"""
from __future__ import annotations

import argparse
import logging
import re
from collections import defaultdict

import fast_v2_live_to_v3 as F

log = logging.getLogger("fix_theatre_scheduled_date")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--facility", default="kisumu_v3")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--chunk", type=int, default=200)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    L = F.Loader(a.facility, 4, 4, True, 0)
    rows = L.v3_rows("theatre", "theatre_schedule")
    todo: dict[str, list[int]] = defaultdict(list)
    no_start = 0
    for r in rows:
        if r.get("scheduled_date"):
            continue
        start = str(r.get("schedule_start_time") or "")
        if re.match(r"\d{4}-\d{2}-\d{2}", start):
            todo[start[:10]].append(int(r["id"]))
        else:
            no_start += 1
    n = sum(len(v) for v in todo.values())
    log.info("V3 theatre schedules: %d | scheduled_date empty: %d (to set %d over %d dates, %d without a "
             "start time)", len(rows), n + no_start, n, len(todo), no_start)
    if a.dry_run or not n:
        return
    org = L.org["organization_id"]
    done = failed = 0
    for day, ids in sorted(todo.items()):
        for i in range(0, len(ids), a.chunk):
            chunk = ids[i:i + a.chunk]
            r = L.m._gateway_post("theatre", {"action": "update", "model": "theatre_schedule", "source_tenant_id": org,
                                              "destination_tenant_id": org, "ids": chunk,
                                              "data": {"scheduled_date": day}}, timeout=120)
            res = r.json() if r.ok else {}
            if not r.ok or (res.get("data") or [{}])[0].get("ignored"):
                failed += len(chunk)
                log.error("  %s: %s %s", day, r.status_code, r.text[:300])
            else:
                done += (res.get("summary") or {}).get("updated", len(chunk))
    log.info("scheduled_date set on %d theatre schedules, %d failed", done, failed)


if __name__ == "__main__":
    main()
