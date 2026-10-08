"""Temporary guard while old-code migration processes overwrite each other's
state files: every few seconds, read .migration_record_progress.json and
.migration_id_map.json (when they parse) and keep the UNION of everything
ever seen in *.union.json. Read-only on the real files. Merge back with:
    python .state_union_keeper.py --merge
once the old processes have finished."""
import json, os, sys, time
from pathlib import Path

D = Path(__file__).resolve().parent
FILES = {"progress": D / ".migration_record_progress.json", "idmap": D / ".migration_id_map.json"}
UNION = {k: p.with_suffix(".union.json") for k, p in FILES.items()}


def load(p):
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return None


def save(p, data):
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(data))
    os.replace(tmp, p)


def merge_into(kind, acc, new):
    for k, v in new.items():
        if kind == "progress":
            acc.setdefault(k, set()).update(v)
        else:
            acc.setdefault(k, {}).update(v)   # id map: later value wins


def main():
    acc = {"progress": {}, "idmap": {}}
    for kind, p in UNION.items():
        old = load(p)
        if old:
            merge_into(kind, acc[kind], old)
    last = {}
    while True:
        for kind, p in FILES.items():
            try:
                m = p.stat().st_mtime_ns
            except OSError:
                continue
            if last.get(kind) == m:
                continue
            data = load(p)
            if data is None:
                continue
            last[kind] = m
            merge_into(kind, acc[kind], data)
            out = {k: sorted(v, key=str) for k, v in acc[kind].items()} if kind == "progress" else acc[kind]
            save(UNION[kind], out)
        time.sleep(3)


def merge_back():
    for kind, p in FILES.items():
        union, cur = load(UNION[kind]) or {}, load(p)
        if cur is None:
            sys.exit(f"{p.name} unreadable — is a process still writing it?")
        acc = {}
        merge_into(kind, acc, union)
        merge_into(kind, acc, cur)
        out = {k: sorted(v, key=str) for k, v in acc.items()} if kind == "progress" else acc
        p.with_name(p.name + f".bak_premerge_{time.strftime('%Y%m%dT%H%M%S')}").write_text(p.read_text())
        save(p, out)
        print(kind, {k: len(v) for k, v in out.items() if "kisumu_v3" in k or kind == "idmap"})


if __name__ == "__main__":
    merge_back() if "--merge" in sys.argv else main()
