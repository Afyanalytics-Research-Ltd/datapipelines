#!/usr/bin/env python3
"""
merge_migration_state.py — union two copies of the migration state.

When the same facility has been migrated from two machines (local + online
Airflow), each copy of .migration_state/<facility>/ records rows that really
are in V3. The right merge is the UNION: an id map / progress list / done list
missing an entry means that record gets posted again (a duplicate in V3 for
uuid-less tables). A git text merge of these JSON files is wrong either way.

  python merge_migration_state.py SOURCE_DIR TARGET_DIR [--dry-run]

Merges every .migration_*.json under SOURCE_DIR into the same relative path
under TARGET_DIR (a facility folder, or the whole .migration_state/):
  dict  → keys unioned, recursively
  list  → items unioned (order kept, then SOURCE's new items appended)
  value → TARGET wins on a clash; every clash is reported (e.g. one V2 record
          mapped to two different V3 ids = it exists twice in V3)
TARGET files are backed up as *.bak_merge_<timestamp> before being written.
Run it only when no migration is running on either side.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path


def merge(target, source, path, clashes):
    if isinstance(target, dict) and isinstance(source, dict):
        out = dict(target)
        for k, v in source.items():
            out[k] = merge(target[k], v, f"{path}.{k}", clashes) if k in target else v
        return out
    if isinstance(target, list) and isinstance(source, list):
        seen = {json.dumps(x, sort_keys=True) for x in target}
        return target + [x for x in source if json.dumps(x, sort_keys=True) not in seen]
    if target != source:
        clashes.append((path, target, source))
    return target


def size(obj) -> int:
    if isinstance(obj, dict):
        return sum(size(v) if isinstance(v, (dict, list)) else 1 for v in obj.values())
    if isinstance(obj, list):
        return len(obj)
    return 1


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("source", type=Path)
    ap.add_argument("target", type=Path)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if not args.source.is_dir() or not args.target.is_dir():
        sys.exit("SOURCE and TARGET must both be directories")
    stamp = time.strftime("%Y%m%dT%H%M%S")
    total_clashes = 0
    for src in sorted(args.source.rglob(".migration_*.json")):
        rel = src.relative_to(args.source)
        dst = args.target / rel
        s = json.loads(src.read_text() or "{}")
        if not dst.exists():
            print(f"  {rel}: only in SOURCE — copied")
            if not args.dry_run:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)
            continue
        t = json.loads(dst.read_text() or "{}")
        clashes: list = []
        merged = merge(t, s, rel.name, clashes)
        added = size(merged) - size(t)
        total_clashes += len(clashes)
        print(f"  {rel}: +{added} entries from SOURCE" + (f", {len(clashes)} CLASH(ES)" if clashes else ""))
        for p, tv, sv in clashes[:10]:
            print(f"      clash {p}: kept {tv!r}, SOURCE had {sv!r}")
        if not args.dry_run and added:
            shutil.copy2(dst, dst.with_name(dst.name + f".bak_merge_{stamp}"))
            tmp = dst.with_name(dst.name + ".tmp")
            tmp.write_text(json.dumps(merged, indent=2))
            tmp.replace(dst)
    if total_clashes:
        print(f"\n{total_clashes} clash(es): the same record mapped to different V3 ids on the two sides — "
              f"those records are probably in V3 twice; check them before re-running.")


if __name__ == "__main__":
    main()
