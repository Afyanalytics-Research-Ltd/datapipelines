"""
write_why.py — fill the `why` column of the reconcile summary for every negative row (V3 short of V2):
from the live reload reports (report_*.json, fast_v2_live_to_v3.py --live) and from what was
established table by table on 2026-10-14/15. A backup of the CSV is kept next to it.

    python write_why.py 4-reconcile-20261015-121332-summary.csv
"""
from __future__ import annotations

import csv
import glob
import io
import json
import re
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from annotate_audit import _why

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))       # the pipelines repo
try:
    from v3_extra_mappings import TWINS                              # twin table → the main V2 table
except Exception:                                                    # noqa: BLE001
    TWINS = {}

# established by hand (investigations, dry runs and loads on 2026-10-14/15) — these win over a report
KNOWN = {
    ("evaluation_procedure_prices", "evaluation/procedure_prices"):
        "V2 keeps every price a procedure ever had (12,814 rows of price history for ~1,640 procedures); V3 "
        "holds one price per organisation + procedure + facility (unique key). Every procedure carries its "
        "latest V2 price — the rest can't go in by design",
    ("settings_scheme_exclusions", "core/scheme_pricing"):
        "1 V2 exclusion points at a scheme V2 doesn't have — 2,928 is everything linkable (2,638 link to "
        "inactive placeholder procedures for 10 procedures deleted from V2)",
    ("settings_scheme_exclusions", "finance/insurance_scheme_pricing"):
        "1 V2 exclusion points at a scheme V2 doesn't have — 2,928 is everything linkable",
    ("evaluation_procedure_tags", "evaluation/procedure_tags"):
        "not copied per org by design: V3 keeps procedure tags as shared reference data for every organisation "
        "(no tenant column)",
    ("evaluation_subprocedures", "evaluation/subprocedures"):
        "not copied per org by design: V3 SubProcedure is shared lab config ($fillable only organization_id, "
        "no tenant column in the gateway)",
    ("reception_patient_schemes", "reception/patient_insurance"):
        "duplicate of V2 patient_insurance, which is the copy migrated — these rows are in V3 through it",
    ("evaluation_reference_range", "evaluation/reference_ranges"):
        "not copied per org by design: V3 keeps reference ranges as shared reference data for every "
        "organisation (no tenant column). (Its missing Snowflake CLEAN view was built 2026-10-15: 236 rows)",
    ("inventory_store_products", "inventory/stock"):
        "163 held: they point at stores 388/389/393 and 7 products that V2's own stores/products tables don't "
        "have (all 26 V2 stores and 3,234 products are in V3) — orphans in V2, can't link",
    ("inventory_stock_movements", "inventory/stock_movement"):
        "1,400 posted on 2026-10-15 (created in V2 after the snapshot). 3,100 held: 1,880 point at products "
        "and/or stores V2 itself no longer has (product 37219, store 2 …) — orphans; 1,220 only lack a V3 user "
        "for V2 user 1 (can be mapped to V3's system user if agreed)",
    ("evaluation_visit_destinations", "reception/visit_destination"):
        "409,221 of the live V2 API's 705,488 point at visits V2 itself doesn't have (visit ids below 308,736 — "
        "older visits are gone from V2), so they can never link. Every linkable destination is in V3: 137,525 "
        "were there, 759 posted 2026-10-15 (V3 now 249,377 — the rest of V3's are from earlier loads). "
        "destination_id and destination_slug set on all 249,377 on 2026-10-15",
    ("evaluation_investigation_result_details", "evaluation/investigation_result_details"):
        "~283,000 belong to lab results that V2 itself doesn't have (checked against the live V2 API) — "
        "orphans, can't link. Every linkable detail is in V3 (26,851 posted 2026-10-15, after posting the "
        "missing 11,488 investigations and 2,670 lab results they hang off)",
    ("evaluation_result_details_ranges", "evaluation/result_detail_ranges"):
        "ranges of result details that can't load (their lab result isn't in V2) — orphans. Every linkable "
        "range is in V3 (13,355 posted 2026-10-15)",
    ("users_user_profiles", "core/user_profile"):
        "220 posted 2026-10-15 through the new core/user_profile gateway model (live V2: 785, all also in "
        "Snowflake RAW + CLEAN). Every V2 user with a V3 account now has a profile (70 had one already). "
        "495 belong to V2 users V3 has no account for — they wait for those users to be created",
    ("inpatient_daily_charges_verifications", "inpatient/daily_charges_verification"):
        "every linkable verification is in V3 (46 + 1 already under V3's unique admission+date key). The other "
        "52 were recorded on visits that were never admitted in V2 — V3 hangs a verification on an admission "
        "(admission_id NOT NULL), so they can't go in",
    ("inpatient_visit_wards", "inpatient/visit_ward"):
        "2,777 loaded 2026-10-15 (was 0: V2 ward charges mapped to V3 stays on the visit's admission). 143 "
        "visits were never admitted in V2 (V3 needs admission_id). NOTE: VisitWard's $fillable lacks status / "
        "charges, so loaded stays show status 'active' and 0 charges until the backend or a DBA sets them",
}


def main(path: Path) -> None:
    reports = defaultdict(list)
    # oldest report first, so a table's latest run (a fix rerun) is the one that counts
    for f in sorted(glob.glob(str(path.parent / "report_*.json")), key=lambda p: Path(p).stat().st_mtime):
        if "dry" in f:
            continue
        for r in json.loads(Path(f).read_text()):
            reports[r["table"]].append(r)
    text = path.read_text()
    body, _, tail = text.replace("\r\n", "\n").partition("\n,,,,,,,,,,\n")
    rows = list(csv.DictReader(io.StringIO(body)))
    cols = list(rows[0].keys())
    filled = pending = 0
    for row in rows:
        try:
            short = int(row["v2_rows"] or 0) - int(row["v3_rows"] or 0)
        except ValueError:
            continue
        if short <= 0:
            continue
        key = (row["table"], row["target"])
        reps = [r for r in reports.get(row["table"], []) if r.get("v3") in (None, row["target"])] or \
            reports.get(row["table"], [])
        # the latest report per table wins (fix reruns are written after the first pass)
        if reps:
            reps = [reps[-1]]
        if key in KNOWN:
            row["why"] = KNOWN[key]
        elif reps:
            why = _why(reps, int(row["v2_rows"] or 0))
            twin = TWINS.get(row["table"])
            if twin:
                # a second copy of another V2 table: it posts only the ids that table lacks, so most of
                # its rows are "dropped" on purpose — they are in V3 through the main table
                why = re.sub(r"\d+ dropped by the migration's transform \([^)]*\)",
                             f"second copy (twin) of V2 {twin}: only the ids {twin} lacks are posted from it — "
                             f"the rest are in V3 through {twin}", why)
            posted = sum(r.get("posted") or 0 for r in reps)
            if posted:
                why = f"{posted} posted 2026-10-15 (live V2 reload) | " + why
            if "002003" in why or "does not exist" in why:
                why = ("can't be read: its Snowflake CLEAN view doesn't exist (the table was never flattened), so "
                       "the migration has no source rows — needs the CLEAN view built first")
            row["why"] = why
        else:
            row["why"] = "live V2 reload still running for this table (2026-10-15) — reason to follow"
            pending += 1
            continue
        filled += 1
    backup = path.with_name(path.stem + f".bak_{datetime.now():%Y%m%dT%H%M%S}.csv")
    shutil.copy2(path, backup)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    path.write_text(buf.getvalue() + (",,,,,,,,,,\n" + tail if tail else ""))
    print(f"why written for {filled} negative rows, {pending} still pending; backup {backup.name}")


if __name__ == "__main__":
    main(Path(sys.argv[1]).resolve())
