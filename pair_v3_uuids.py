#!/usr/bin/env python3
"""
pair_v3_uuids.py — before re-migrating a facility from a uuid-era V2 extract,
record which V3 row each already-migrated V2 record became, by V3's own uuid.

Why: rows migrated before V2 had uuids carry a uuid V3 (or the migration)
made up. Since V2's uuid install every extracted record brings its own V2
uuid, and the migration posts with match_on=uuid — a V2 uuid matches no
existing V3 row, so every already-migrated record would be inserted a second
time. The migration (snowflake_to_v3_migration._with_stable_uuid) posts with
the V3 row's own uuid whenever .migration_v3_uuid.json names one for that V2
id, which updates the row in place. This fills that file for every model in
the facility's id map:

  V2 id ─(.migration_id_map.json)→ V3 id ─(V3 gateway read)→ V3 uuid

Reads V3 only; writes the state file (a backup of the old one is kept).
Entries repair_v3_links.py already wrote are kept unless this run finds the
same V2 id paired differently, which is reported.

  python pair_v3_uuids.py --facility kisumu_v3                   # every model in the id map
  python pair_v3_uuids.py --facility kisumu_v3 --models visits prescriptions
  python pair_v3_uuids.py --facility kisumu_v3 --dry-run         # report only

A fresh load into an empty V3 org needs none of this.
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import time

import patient_journey_v3 as pj
import snowflake_to_v3_migration as m
import v2_to_v3_api_migration as v2v3

log = logging.getLogger("pair_v3_uuids")

V3_UUID_FILE = ".migration_v3_uuid.json"


def pair(facility: str, models: list[str] | None = None, *, dry_run: bool = False, read=None) -> dict:
    """{alias: {"pairs": n, "added": n, "changed": n, "v3_missing": n, "skipped": reason}}"""
    d = m.use_state_dir(facility)
    id_map = json.loads((d / ".migration_id_map.json").read_text()) if (d / ".migration_id_map.json").exists() else {}
    path = d / V3_UUID_FILE
    known = json.loads(path.read_text()) if path.exists() else {}
    if read is None:
        v2v3.set_v3_target_facility(facility)
        org = v2v3.v3_login_org_cfg()["organization_id"]
        v2v3._fetch_available_models()
        read = lambda service, alias: pj.read_model(service, alias, org)["rows"]   # noqa: E731

    report = {}
    for alias in sorted(models or id_map):
        mp = {k: v for k, v in (id_map.get(alias) or {}).items() if str(k).isdigit()}   # V2 ids only
        if not mp:
            report[alias] = {"skipped": "no V2-id entries in the id map"}
            continue
        service = v2v3._alias_to_service.get(alias)
        if not service:
            report[alias] = {"skipped": "not exposed by the V3 gateway"}
            continue
        t0 = time.time()
        rows = read(service, alias)
        uuid_of = {r["id"]: r.get("uuid") for r in rows}
        if rows and not any(uuid_of.values()):
            report[alias] = {"skipped": "V3 rows have no uuid column"}
            continue
        mine = known.setdefault(alias, {})
        rep = {"pairs": 0, "added": 0, "changed": 0, "v3_missing": 0}
        for v2, v3 in mp.items():
            u = uuid_of.get(int(v3)) if str(v3).isdigit() else None
            if not u:
                rep["v3_missing"] += 1
                continue
            rep["pairs"] += 1
            old = mine.get(str(v2))
            if old is None:
                rep["added"] += 1
            elif old != u:
                rep["changed"] += 1
            mine[str(v2)] = u
        report[alias] = rep
        log.info("%-24s %s (%.0fs)", alias, json.dumps(rep), time.time() - t0)
    if not dry_run:
        with v2v3._file_lock(path):
            if path.exists():
                shutil.copy2(path, path.with_name(f"{path.name}.bak_pair_{time.strftime('%Y%m%dT%H%M%S')}"))
            v2v3._atomic_write(path, json.dumps(known, indent=2))
        log.info("written %s", path)
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--facility", required=True)
    ap.add_argument("--models", nargs="+", help="Gateway aliases (default: every model in the id map)")
    ap.add_argument("--dry-run", action="store_true", help="Report only, don't write the state file")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    for noisy in ("snowflake.connector", "urllib3", "v2_to_v3_migration", "snowflake_to_v3_migration"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    report = pair(args.facility, args.models, dry_run=args.dry_run)
    print(json.dumps(report, indent=2))
    sys.exit(1 if any(r.get("changed") for r in report.values()) else 0)


if __name__ == "__main__":
    main()
