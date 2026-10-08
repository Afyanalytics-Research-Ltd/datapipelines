#!/usr/bin/env python3
"""
create_v3_users.py — create V2 staff (admitting doctors) as V3 users so the
records that need a V3 user id (admissions.admitting_doctor_id, discharges,
discharge requests) can be migrated.

V3 users can't be inserted through the migration gateway (`users` is
read-only there), so this calls core-service's normal POST /v1/users with
the migration account's own login (a superadmin in the destination org).

NO EMAILS: core emails every new user their username + plain-text password
(UsersRepository::create → WelcomeUserMail) unless the address ends in
".invalid". So each user is created with a placeholder address

    <real V2 email>.v2-<V2 user id>.invalid

which also carries the V2 id, so v2_to_v3_api_migration can rebuild the
V2→V3 user mapping from V3's own user list on any host. The accounts get no
roles and a random password nobody holds, so nobody can log in with them
until someone sets a real email/password in V3.

Idempotent: users whose placeholder (or real) address already exists in V3
are skipped. Source: the doctors embedded in <FACILITY>_CLEAN.ADMISSIONS.

USAGE
  python create_v3_users.py --facility kisumu_v3 --dry-run
  python create_v3_users.py --facility kisumu_v3 --limit 1
  python create_v3_users.py --facility kisumu_v3
"""
from __future__ import annotations

import argparse
import logging
import secrets
import sys

import snowflake_to_v3_migration as s2v3
import v2_to_v3_api_migration as v2v3

log = logging.getLogger("create_v3_users")
logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s",
                    datefmt="%H:%M:%S", stream=sys.stdout)


def placeholder_email(v2_id: int, email: str | None, facility: str) -> str:
    base = (email or f"v2-user-{v2_id}@{facility.replace('_', '-')}").strip().lower()
    return f"{base}.v2-{v2_id}.invalid"


def v2_doctors(facility: str) -> list[dict]:
    with s2v3._snowflake_connect() as conn:
        rows = conn.cursor().execute(f"""
            SELECT doctor_id, ANY_VALUE(doctor_email), ANY_VALUE(doctor_profile_first_name),
                   ANY_VALUE(doctor_profile_last_name), ANY_VALUE(doctor_full_name), COUNT(*)
            FROM {s2v3.sf_schema(facility, 'CLEAN')}.ADMISSIONS
            WHERE doctor_id IS NOT NULL
            GROUP BY doctor_id
            ORDER BY COUNT(*) DESC
        """).fetchall()
    out = []
    for v2_id, email, first, last, full, n in rows:
        names = str(full or "").split()
        out.append({
            "v2_id": int(v2_id), "email": email, "admissions": n,
            "first_name": (first or (names[0] if names else "") or "Unknown").strip()[:255],
            "last_name": (last or (names[-1] if len(names) > 1 else "") or "Doctor").strip()[:255],
        })
    return out


def create_user(doc: dict, facility: str, org_cfg: dict) -> int:
    password = secrets.token_urlsafe(24)
    body = {
        "user": {"email": placeholder_email(doc["v2_id"], doc["email"], facility),
                 "password": password, "password_confirmation": password},
        "profile": {"first_name": doc["first_name"], "last_name": doc["last_name"]},
        "facility_ids": [org_cfg["facility_id"]],
    }
    url = f"{v2v3.V3_SERVICES['core'].rstrip('/')}/v1/users"
    headers = {"Authorization": f"Bearer {v2v3._v3_token()}", "Accept": "application/json",
               "X-Facility-Id": str(org_cfg["facility_id"])}
    r = v2v3._v3_session().post(url, json=body, headers=headers, timeout=60)
    if r.status_code != 201:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:400]}")
    resp = r.json()
    if resp.get("email_sent"):
        log.warning("  core reports an email WAS sent for %s", body["user"]["email"])
    return (resp.get("data") or {})["id"]


def main() -> None:
    ap = argparse.ArgumentParser(description="Create V2 admitting doctors as V3 users (no emails sent).")
    ap.add_argument("--facility", "-f", required=True)
    ap.add_argument("--dry-run", action="store_true", help="List what would be created; create nothing")
    ap.add_argument("--limit", type=int, default=0, help="Create at most N users (0 = all)")
    args = ap.parse_args()

    s2v3.use_state_dir(args.facility)
    v2v3.set_v3_target_facility(args.facility)
    org_cfg = v2v3.v3_login_org_cfg()
    v2v3._load_id_map()
    existing = {str(u.get("email") or "").strip().lower(): u["id"]
                for u in v2v3._fetch_v3_records("users", org_cfg, service_name="core") if u.get("email")}
    docs = v2_doctors(args.facility)
    log.info("V3 org %s / facility %s — %d V2 admitting doctors, %d V3 users already",
             org_cfg["organization_id"], org_cfg["facility_id"], len(docs), len(existing))

    created = skipped = failed = 0
    pairs = []
    for doc in docs:
        ph = placeholder_email(doc["v2_id"], doc["email"], args.facility)
        v3_id = existing.get(ph) or existing.get(str(doc["email"] or "").strip().lower())
        if v3_id:
            pairs.append((doc["v2_id"], v3_id))
            skipped += 1
            continue
        if args.dry_run:
            log.info("  would create %-55s %s %s (%d admissions)", ph, doc["first_name"],
                     doc["last_name"], doc["admissions"])
            continue
        if args.limit and created >= args.limit:
            break
        try:
            v3_id = create_user(doc, args.facility, org_cfg)
        except Exception as e:
            failed += 1
            log.error("  ✗ %s: %s", ph, e)
            continue
        created += 1
        pairs.append((doc["v2_id"], v3_id))
        log.info("  ✓ V2 user %s → V3 user %s (%s)", doc["v2_id"], v3_id, ph)

    if pairs and not args.dry_run:
        v2v3._store_id_mappings(v2v3._v3_alias(r"App\Models\User"), pairs)
    log.info("Done — %d created, %d already in V3, %d failed", created, skipped, failed)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
