"""
visit_destinations_slug_sql.py — SQL that fills visit_destinations.destination_slug (2026-10-15).

VisitDestination's $fillable lacks destination_slug, so the gateway drops it on insert and refuses it
on update ("not fillable on VisitDestination"). Every row now has destination_id, so the slug is its
service destination's slug: one UPDATE … CASE destination_id for a DBA to run against the reception
service database (core's service destinations live in another database, hence the literal slugs).

    python visit_destinations_slug_sql.py output/visit_destinations_slug.sql
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

import fast_v2_live_to_v3 as F


def q(s: str) -> str:
    return "'" + str(s).replace("\\", "\\\\").replace("'", "''") + "'"


def main(out: str) -> None:
    L = F.Loader("kisumu_v3", 4, 4, True, 0)
    org = L.org["organization_id"]
    slugs = {int(d["id"]): d.get("slug") for d in L.v3_rows("core", "service_destinations") if d.get("slug")}
    rows = L.v3_rows("reception", "visit_destination")
    used = sorted({int(r["destination_id"]) for r in rows if r.get("destination_id")})
    missing = [d for d in used if d not in slugs]
    cases = "\n".join(f"    WHEN {d} THEN {q(slugs[d])}" for d in used if d in slugs)
    ids = ", ".join(str(d) for d in used if d in slugs)
    n = sum(1 for r in rows if r.get("destination_id") and int(r["destination_id"]) in slugs)
    when = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with open(out, "w", encoding="utf-8") as f:
        f.write(f"-- RECEPTION service database: set destination_slug on {n} visit_destinations of organization {org}\n"
                f"-- from their destination (core service_destinations.slug). Generated {when} UTC.\n"
                f"SELECT COUNT(*) AS without_slug FROM visit_destinations WHERE organization_id = {org} "
                f"AND (destination_slug IS NULL OR destination_slug = '');\n"
                "START TRANSACTION;\n"
                f"UPDATE visit_destinations SET destination_slug = CASE destination_id\n{cases}\n  END\n"
                f"WHERE organization_id = {org} AND destination_id IN ({ids});\n"
                f"SELECT ROW_COUNT() AS updated, {n} AS expected;\n"
                f"SELECT COUNT(*) AS without_slug FROM visit_destinations WHERE organization_id = {org} "
                f"AND (destination_slug IS NULL OR destination_slug = '');\n"
                "-- updated ≈ expected (rows already holding the right slug don't count) and without_slug = 0 → "
                "COMMIT;   otherwise → ROLLBACK;\n")
    print(f"{n} rows over {len(used)} destinations → {out}" + (f"; destinations without a slug: {missing}" if missing else ""))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "visit_destinations_slug.sql")
