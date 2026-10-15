"""
annotate_audit.py — add the live V2 → V3 reload results (report_*.json from fast_v2_live_to_v3.py --live)
to the reconcile summary CSV: per negative row, what live V2 really has, what was posted, and why the
rest refused to go in.

    python annotate_audit.py 4-reconcile-20261015-121332-summary.csv report_*.json
"""
from __future__ import annotations

import csv
import io
import json
import sys
from collections import defaultdict
from pathlib import Path

NEW_COLS = ["live_v2_rows", "already_in_v3", "posted", "held_parent_missing", "refused_by_v3",
            "v3_rows_after", "reload_why"]


def _why(reports: list[dict], v2_rows: int) -> str:
    parts = []
    live = next((r.get("live_v2") for r in reports if r.get("live_v2") is not None), None)
    if live is not None and v2_rows and live < v2_rows:
        parts.append(f"live V2 now has {live} rows, not {v2_rows} — {v2_rows - live} are gone from V2 itself "
                     f"(in the snapshot only), nothing to load")
    for r in reports:
        st = r.get("status") or ""
        if "v3" not in r:
            parts.append(st)
            continue
        dropped = (r.get("live_v2") or 0) - r.get("transformed", 0) - r.get("held", 0)
        if r.get("live_v2") is not None and dropped > 0 and len(reports) == 1:
            parts.append(f"{dropped} dropped by the migration's transform (a required field is empty, or the row "
                         f"is a twin/history copy the migration never posts)")
        if r.get("held"):
            why = "; ".join(f"{n}× {k}" for k, n in (r.get("held_why") or {}).items())
            parts.append(f"{r['held']} held — parent row not in V3, so V3 can't take them ({why})")
        if r.get("unverifiable"):
            parts.append(f"{r['unverifiable']} not posted — no visit/parent key to check V3 for them (duplicate risk)")
        if st.startswith("not posted"):
            parts.append(st)
        if r.get("failed"):
            why = "; ".join(f"{n}× {k}" for k, n in (r.get("failed_why") or {}).items())
            parts.append(f"{r['failed']} refused by V3: {why}")
        if r.get("duplicate_key"):
            parts.append(f"{r['duplicate_key']} skipped — V3 already holds the same record (unique key taken)")
    return " | ".join(p for p in parts if p) or "all live V2 rows are in V3"


def main(audit: Path, report_files: list[Path]) -> Path:
    by_table: dict[str, list[dict]] = defaultdict(list)
    for f in report_files:
        for r in json.loads(f.read_text()):
            by_table[r["table"]].append(r)
    text = audit.read_text()
    body, _, tail = text.replace("\r\n", "\n").partition("\n,,,,,,,,,,\n")
    rows = list(csv.DictReader(io.StringIO(body)))
    cols = list(rows[0].keys()) + NEW_COLS
    for row in rows:
        try:
            short = int(row["v2_rows"] or 0) - int(row["v3_rows"] or 0)
        except ValueError:
            short = 0
        reps = by_table.get(row["table"], [])
        if short <= 0 or not reps or (row.get("why") or "").strip():
            continue
        target = row["target"]
        mine = [r for r in reps if r.get("v3") == target] or [r for r in reps if "v3" not in r] or reps
        live = next((r.get("live_v2") for r in reps if r.get("live_v2") is not None), "")
        posted = sum(r.get("posted", 0) for r in mine)
        row.update({
            "live_v2_rows": live,
            "already_in_v3": sum(r.get("in_v3_by_uuid", 0) + r.get("in_v3_by_content", 0) for r in mine),
            "posted": posted,
            "held_parent_missing": sum(r.get("held", 0) for r in mine),
            "refused_by_v3": sum(r.get("failed", 0) for r in mine),
            "v3_rows_after": int(row["v3_rows"] or 0) + posted,
            "reload_why": _why(mine, int(row["v2_rows"] or 0)),
        })
    out = audit.with_name(audit.stem + "-reloaded.csv")
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    out.write_text(buf.getvalue() + ",,,,,,,,,,\n" + tail)
    return out


if __name__ == "__main__":
    print(main(Path(sys.argv[1]), [Path(p) for p in sys.argv[2:]]))
