"""
SUPERSEDED (2026-10-15): core's gateway now has `user_profile`, and v3_extra_mappings maps
users_user_profiles to it — the migration / fast_v2_live_to_v3.py post profiles directly. Kept only as a
fallback for an instance whose gateway lacks the model.

user_profiles_sql.py — V2 users_user_profiles → V3 core-service user_profiles, as SQL (2026-10-15).

V3's gateway has no user_profiles model (core or reception), so profiles can't be posted; this writes an
INSERT IGNORE load for a DBA instead: user_id unique, so a profile V3 already has (made with its user) is
left as it is and a re-run adds nothing twice. Each V2 user_id → the V3 user the migration knows for it
(_v3_user_for_v2_id: placeholder-email pairs + the id map); profiles of V2 users V3 has no account for are
left out (listed). Read from the facility's CLEAN view (kept current from the live V2 API).

    python user_profiles_sql.py kisumu_v3 output/user_profiles_load.sql
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone

COLS = ["user_id", "title", "first_name", "middle_name", "last_name", "dob", "gender", "phone", "secondary_phone",
        "work_email", "secondary_email", "photo", "coverphoto", "address", "town", "county", "district", "country",
        "location", "job_description", "staff_number", "roll_no", "date_of_employment", "mpdb", "pin", "id_number",
        "clinics", "organization_id", "created_at", "updated_at"]
GENDER = {"m": "male", "male": "male", "f": "female", "female": "female", "o": "other", "other": "other"}


def q(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return str(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "''") + "'"


def blank(v):
    return None if v is None or (isinstance(v, str) and (not v.strip() or v.strip().lower() == "null")) else v


def date(v):
    v = blank(v)
    return str(v)[:10] if v and re.match(r"(19|20)\d\d-\d\d-\d\d", str(v)) else None


def row_for(p: dict, v3_user: int, org: int) -> dict:
    title = str(blank(p.get("title")) or "1")
    clinics = blank(p.get("clinics"))
    return {
        "user_id": v3_user, "title": int(title) if title.isdigit() and 1 <= int(title) <= 5 else 1,
        "first_name": blank(p.get("first_name")) or "", "middle_name": blank(p.get("middle_name")),
        "last_name": blank(p.get("last_name")) or "", "dob": date(p.get("dob")),
        "gender": GENDER.get(str(p.get("gender") or "").strip().lower()),
        **{c: blank(p.get(c)) for c in ("phone", "secondary_phone", "work_email", "secondary_email", "photo",
                                        "coverphoto", "address", "town", "county", "district", "country",
                                        "location", "job_description", "staff_number", "roll_no", "mpdb", "pin",
                                        "id_number")},
        "date_of_employment": date(p.get("date_of_employment")),
        "clinics": json.dumps(clinics) if isinstance(clinics, (list, dict)) else clinics,
        "organization_id": org, "created_at": blank(p.get("created_at")), "updated_at": blank(p.get("updated_at")),
    }


def main(facility: str, out: str) -> None:
    sys.path.insert(0, "/opt/airflow/dags")
    from pipelines_common import use_pipelines_dir
    use_pipelines_dir([facility])
    import snowflake_to_v3_migration as s
    import v2_to_v3_api_migration as m
    m.set_v3_target_facility(facility)
    m._fetch_available_models()
    s._start(facility)                              # loads the id map the user lookup falls back to
    org = m.facility_v3_config(facility)["organization_id"]
    s.ensure_clean_view(facility, "users_user_profiles")
    with s._snowflake_connect() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {s.sf_schema(facility, 'CLEAN')}.USERS_USER_PROFILES")
        names = [d[0] for d in cur.description]
        profiles = [s._row_to_record(r, names) for r in cur.fetchall()]
    rows, no_user = [], []
    for p in sorted(profiles, key=lambda x: int(x["id"])):
        v3_user = m._v3_user_for_v2_id(p.get("user_id"))
        if v3_user:
            rows.append(row_for(p, int(v3_user), org))
        else:
            no_user.append(p.get("user_id"))
    seen, unique = set(), []
    for r in rows:                                  # user_id is unique: one profile per V3 user
        if r["user_id"] not in seen:
            seen.add(r["user_id"])
            unique.append(r)
    when = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with open(out, "w", encoding="utf-8") as f:
        f.write(f"-- CORE service database: {len(unique)} V2 user profiles ({facility}) → user_profiles, organization "
                f"{org}. Generated {when} UTC by user_profiles_sql.py.\n"
                f"-- INSERT IGNORE: a user that already has a profile keeps it (user_id is unique).\n"
                f"SELECT COUNT(*) AS profiles_before FROM user_profiles WHERE organization_id = {org};\n"
                "START TRANSACTION;\n")
        for i in range(0, len(unique), 200):
            chunk = unique[i:i + 200]
            f.write(f"INSERT IGNORE INTO user_profiles ({', '.join(COLS)}) VALUES\n")
            f.write(",\n".join("  (" + ", ".join(q(r[c]) for c in COLS) + ")" for r in chunk) + ";\n")
        f.write(f"SELECT COUNT(*) AS profiles_after FROM user_profiles WHERE organization_id = {org};\n"
                "-- after − before = profiles added (≤ the count above; users who had one are skipped) → COMMIT;"
                "   otherwise → ROLLBACK;\n")
    print(json.dumps({"v2_profiles": len(profiles), "with_v3_user": len(rows), "in_sql": len(unique),
                      "v2_users_without_v3_account": len(no_user), "examples": no_user[:10], "file": out}))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "kisumu_v3",
         sys.argv[2] if len(sys.argv) > 2 else "user_profiles_load.sql")
