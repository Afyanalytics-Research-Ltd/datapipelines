"""
visit_wards_fix_sql.py — SQL that sets the inp_visit_wards columns the gateway can't (2026-10-15).

VisitWard's $fillable lacks status, daily_charge, days_stayed, total_charge and notes: the gateway
drops them on insert and refuses them on update ("not fillable on VisitWard"). So the 2,777
kisumu_v3 visit wards loaded by fast_v2_live_to_v3.py are all status 'active' (= occupying a bed)
with zero charges. This writes one UPDATE per row, matched by its uuid, from the same V2 rows and
transform — for a DBA to run against the inpatient service database:

    python visit_wards_fix_sql.py /opt/airflow/pipelines/output/inp_visit_wards_fix.sql
    mysql <inpatient db> < inp_visit_wards_fix.sql
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

import fast_v2_live_to_v3 as F

COLS = ("status", "daily_charge", "days_stayed", "total_charge", "notes")


def sql_value(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return str(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "''") + "'"


def main(out_path: str) -> None:
    L = F.Loader("kisumu_v3", 8, 32, True, 0)
    org = L.org["organization_id"]
    # admissions first, as in the load: their V2→V3 pairs let every visit ward resolve
    for table in ("inpatient_admissions", "inpatient_visit_wards"):
        captured, held = L.capture(table)
        for (svc, alias, ns), payloads in captured.items():
            if table == "inpatient_admissions":
                L.post(table, svc, alias, ns, payloads, held)       # dry run: records the pairs only
            else:
                rows = payloads
    in_v3 = {str(r.get("uuid") or "").lower() for r in L.v3_rows("inpatient", "visit_ward")}
    fixes = [p for p in rows if str(p.get("uuid") or "").lower() in in_v3]
    when = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"-- INPATIENT service database: set status / charges / notes on {len(fixes)} kisumu_v3 visit wards\n"
                f"-- (organization {org}) the gateway can't write. Generated {when} UTC by visit_wards_fix_sql.py.\n"
                f"SELECT status, COUNT(*) FROM inp_visit_wards WHERE organization_id = {org} GROUP BY status;\n"
                "START TRANSACTION;\n")
        for p in fixes:
            sets = ", ".join(f"{c} = {sql_value(p.get(c))}" for c in COLS if c in p)
            f.write(f"UPDATE inp_visit_wards SET {sets} WHERE uuid = '{p['uuid']}' AND organization_id = {org};\n")
        f.write(f"-- expect {len(fixes)} rows matched; check the status counts below, then COMMIT (or ROLLBACK)\n"
                f"SELECT status, COUNT(*) FROM inp_visit_wards WHERE organization_id = {org} GROUP BY status;\n")
    print(f"{len(fixes)} UPDATEs → {out_path} ({len(rows)} transformed, {len(in_v3)} visit wards in V3)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "inp_visit_wards_fix.sql")
