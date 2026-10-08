#!/usr/bin/env python3
"""
migrate_facility.py — migrate one V2 facility to V3 end to end, prerequisites first.

  PHASE 0  setup check     V3 login lands in the org/facility FACILITY_V3_CONFIG
                           expects; V2 credentials work. Stops if not.
  PHASE 1  → Snowflake     every V2 table (sheet set + old-system-history set,
                           which includes the prerequisite lookups and users)
                           into {FAC}_RAW, then rebuild the {FAC}_CLEAN views.
  PHASE 2  prerequisites   → V3, in dependency order:
                           a. users (V2 staff → V3 users via the gateway; email
                              "<original>.v2-<V2 id>.invalid", random password,
                              reset forced on first login)
                           b. lookup tables (units, categories, stores, bed types,
                              admission/discharge types, procedure categories,
                              procedures, wards, beds, products …)
                           c. id-map reconcile — some services answer 201 without
                              returning the new id; those rows are re-matched to V3
                              by their natural key (code / name)
                           d. departments, derived from visit-destination names
                              (V2 has no departments table)
                           e. GATE: every prerequisite mapped, or stop
  PHASE 3  main tables     everything else (patients, visits, admissions, clinical
                           tables …) in tier order. Records whose parent isn't in V3
                           yet are held back and retried by the next run.
  REPORT   per table: done / waiting / failed.

Re-runs are safe: every phase skips what's already in V3. One facility
migration at a time — refuses to start if another loader/migration runs.

USAGE
  python migrate_facility.py --facility <name> --plan            # what would run, no writes
  python migrate_facility.py --facility <name>                   # all phases
  python migrate_facility.py --facility <name> --phases prereqs main
  python migrate_facility.py --facility <name> --skip-snowflake  # data already loaded
  python migrate_facility.py --facility <name> --dry-run         # transform + count, post nothing
"""
from __future__ import annotations

import argparse
import collections
import json
import logging
import os
import secrets
import subprocess
import sys
import time

import facility_to_snowflake_fast_resume as loader
import flatten_jsons_schemas as fl
import snowflake_to_v3_migration as s2v3
import v2_to_v3_api_migration as v2v3

log = logging.getLogger("migrate_facility")

# Lookup tables other tables point at, in load order (Snowflake source-table
# names). Users and departments are handled separately (sync_users,
# ensure_departments_from_destinations).
PREREQUISITE_TABLES = [
    "settings_clinics",
    "inventory_units",
    "inventory_categories",
    "inventory_stores",
    "inpatient_bed_types",
    "inpatient_admission_types",
    "inpatient_discharge_types",
    "evaluation_procedure_categories",
    "evaluation_procedures",
    "inpatient_wards",
    "inpatient_beds",
    "inventory_products",
]
NOT_MIGRATED_DIRECTLY = {"users"}   # handled by sync_users, never by the table job

# Natural keys for re-matching prerequisites whose V3 insert didn't return an
# id (V3 field names, compared case/space-insensitively after the transform).
RECONCILE_KEYS: dict[str, list[str]] = {
    "settings_clinics":                ["name"],      # → V3 core facilities
    "inventory_units":                 ["name"],
    "inventory_categories":            ["name", "code"],
    "inventory_stores":                ["name", "code"],
    "inpatient_bed_types":             ["name"],
    "inpatient_admission_types":       ["code"],
    "inpatient_discharge_types":       ["name"],
    "evaluation_procedure_categories": ["name", "code"],
    "evaluation_procedures":           ["code"],
    "inpatient_wards":                 ["name"],
}


# ─── HELPERS ─────────────────────────────────────────────────────────────

def _norm(v) -> str:
    return " ".join(str(v if v is not None else "").split()).lower()


def _other_processes() -> list[str]:
    try:
        out = subprocess.run(
            ["pgrep", "-af", r"python.*(facility_to_snowflake_fast_resume|snowflake_to_v3_migration|reingest|migrate_facility)\.py"],
            capture_output=True, text=True).stdout
    except FileNotFoundError:
        return []
    mine = str(os.getpid())
    return [l for l in out.splitlines() if l.strip() and l.split()[0] != mine and "pgrep" not in l]


def _load_id_map_safely() -> None:
    for _ in range(20):           # another writer may be mid-write; retry briefly
        try:
            v2v3._load_id_map()
            return
        except Exception:
            time.sleep(0.5)
    v2v3._load_id_map()


def _discover(facility: str) -> dict[str, dict]:
    with s2v3._snowflake_connect() as conn:
        cur = conn.cursor()
        entries = {e["table"]: e for e in s2v3.discover_tables(cur, facility)}
        cur.close()
    return entries


def _alias_and_service(entry: dict) -> tuple[str, str]:
    tk = entry["transform"]
    alias = s2v3._ALIAS_OVERRIDE.get(tk) or v2v3._v3_alias(entry["v3"])
    service = s2v3._SERVICE_OVERRIDE.get(tk) or v2v3._alias_to_service.get(alias, "core")
    return alias, service


def _clean_rows(facility: str, table: str) -> list[dict]:
    view = f"{s2v3.sf_schema(facility, 'CLEAN')}.{table.upper()}"
    with s2v3._snowflake_connect() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {view}")
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _mapped_count(entry: dict, rows: list[dict]) -> int:
    alias, _ = _alias_and_service(entry)
    m = s2v3._id_map_for(alias)
    keys = {str(k) for k in m}
    return sum(1 for r in rows if str(r.get("uuid") or r.get("id")) in keys
               or str(r.get("id")) in keys)


# ─── PHASE 0 ─────────────────────────────────────────────────────────────

def phase_setup(facility: str) -> dict:
    try:
        v2 = loader.facility_config(facility)            # FACILITIES, or FACILITY_<F>_BASE_URL/_DB
    except KeyError as e:
        sys.exit(str(e))
    expected = v2v3.facility_v3_config(facility)       # FACILITY_V3_CONFIG, or AFYA_<F>_ORGANIZATION_ID/_FACILITY_ID
    if expected.get("organization_id") is None or expected.get("facility_id") is None:
        up = facility.upper()
        sys.exit(f"No V3 org/facility for {facility!r}: set AFYA_{up}_ORGANIZATION_ID / AFYA_{up}_FACILITY_ID "
                 f"(Airflow: Extra of Connection afya_v3_{facility}) or add it to FACILITY_V3_CONFIG.")
    log.info("PHASE 0 · V2 %s (db %s) → V3 org %s facility %s", v2["base_url"], v2["db"],
             expected["organization_id"], expected["facility_id"])
    loader._facility_token(facility)          # V2 credentials
    v2v3.set_v3_target_facility(facility)
    org = v2v3.v3_login_org_cfg()             # raises on org/facility mismatch
    v2v3._fetch_available_models()
    log.info("PHASE 0 ✓ V2 login ok · V3 org %s facility %s", org["organization_id"], org["facility_id"])
    return org


# ─── PHASE 1 ─────────────────────────────────────────────────────────────

def phase_snowflake(facility: str) -> None:
    for table_set in ("sheet", "old_system_history"):
        log.info("PHASE 1 · V2 → Snowflake [%s]", table_set)
        try:
            loader.run_pipeline(facility, table_set=table_set, resume=True)
        except SystemExit as e:
            # a failed table keeps the watermark; re-running resumes it
            log.warning("PHASE 1 · [%s] finished with failed tables (exit %s) — continuing; "
                        "re-run to retry them", table_set, e.code)
    raw, clean = f"{facility.upper()}_RAW", f"{facility.upper()}_CLEAN"
    conn = fl._snowflake_connect()
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {clean}")
        tables = fl.get_source_tables(cur, raw)
        for t in tables:
            try:
                cur.execute(fl.build_flatten_sql(raw, clean, t, fl.expand_objects(cur, raw, t, fl.discover_fields(cur, raw, t))))
            except Exception as e:
                log.warning("  view %s.%s not rebuilt: %s", clean, t, e)
        log.info("PHASE 1 ✓ %d CLEAN views rebuilt", len(tables))
    finally:
        conn.close()


# ─── PHASE 2 ─────────────────────────────────────────────────────────────

def sync_users(facility: str, dry_run: bool) -> dict:
    """V2 staff → V3 users. Existing accounts (matched by the V2 id in their
    email) are kept; missing ones are inserted with a random password."""
    try:
        src = _clean_rows(facility, "users")
    except Exception as e:
        log.warning("  users: no %s users view (%s) — skipped", facility, str(e)[:80])
        return {"v2": 0, "matched": 0, "created": 0, "failed": []}
    org = v2v3.v3_login_org_cfg()
    v2v3.reset_v3_user_cache()
    have = {r["id"] for r in src if v2v3._v3_user_id_for_v2(r.get("id")) is not None}
    missing = [r for r in src if r.get("id") is not None and r["id"] not in have]
    log.info("  users: %d in V2, %d already in V3, %d to create", len(src), len(have), len(missing))
    created, failed = 0, []
    if not dry_run:
        for r in missing:
            base = {"username": str(r.get("username") or f"user{r['id']}").strip(),
                    "email": v2v3.v2_user_email(r.get("email"), r.get("username"), r["id"]),
                    "password": secrets.token_urlsafe(18),       # V2 hashes can't be migrated
                    "enforce_password_reset": 1}
            if r.get("employee_number"):
                base["employee_number"] = str(r["employee_number"])
            for attempt in (base, {**base, "username": f"{base['username']}.v2-{r['id']}"}):
                resp = v2v3._gateway_post("core", {"action": "insert", "model": "users",
                                                   "destination_tenant_id": org["organization_id"],
                                                   "data": attempt}, timeout=60)
                if resp.ok:
                    created += 1
                    break
            else:
                failed.append((r["id"], r.get("username"), resp.status_code, resp.text[:120]))
        v2v3.reset_v3_user_cache()
    matched = sum(1 for r in src if v2v3._v3_user_id_for_v2(r.get("id")) is not None)
    log.info("  users: created %d · now %d/%d V2 users in V3%s", created, matched, len(src),
             f" · {len(failed)} failed: {failed[:5]}" if failed else "")
    return {"v2": len(src), "matched": matched, "created": created, "failed": failed}


def reconcile_id_maps(facility: str, entries: dict, org: dict, dry_run: bool) -> None:
    """Prerequisites already in V3 but missing from the id map (V3 answered
    201 without an id, or a concurrent run overwrote the map): match by
    natural key and record them, so they're never posted twice."""
    _load_id_map_safely()
    for table, keys in RECONCILE_KEYS.items():
        e = entries.get(table)
        if not e or not e["v3"]:
            continue
        alias, service = _alias_and_service(e)
        rows = _clean_rows(facility, table)
        mapped = {str(k) for k in s2v3._id_map_for(alias)}
        todo = [r for r in rows if str(r.get("id")) not in mapped]
        if not todo:
            continue
        v3 = v2v3._fetch_v3_records(alias, org, service_name=service)
        kf = lambda rec: tuple(_norm(rec.get(k)) for k in keys)
        v3_by = collections.defaultdict(list)
        for rec in v3:
            v3_by[kf(rec)].append(rec["id"])
        transformed = [(r, v2v3.transform_record(dict(r), e["transform"], org, facility)) for r in todo]
        src_ct = collections.Counter(kf(t) for _, t in transformed if t)
        pairs = [(int(r["id"]), v3_by[kf(t)][0]) for r, t in transformed
                 if t and src_ct[kf(t)] == 1 and len(v3_by.get(kf(t), [])) == 1]
        log.info("  reconcile %s: %d unmapped, %d matched in V3 by %s", table, len(todo), len(pairs), keys)
        if pairs and not dry_run:
            v2v3._store_id_mappings(alias, pairs)
            v2v3._load_record_progress()
            for v2_id, _ in pairs:
                v2v3._mark_record_inserted(f"{facility}|sf:{table}", v2_id)
            v2v3._flush_record_progress()


def gate(facility: str, entries: dict, users: dict) -> list[str]:
    _load_id_map_safely()
    gaps = []
    print(f"\n  {'prerequisite':34s} {'in Snowflake':>12s} {'mapped to V3':>12s}")
    for table in PREREQUISITE_TABLES:
        e = entries.get(table)
        if not e or not e["v3"]:
            continue
        rows = _clean_rows(facility, table)
        n_map = _mapped_count(e, rows)
        print(f"  {table:34s} {len(rows):>12} {n_map:>12}")
        if rows and n_map < len(rows):
            gaps.append(f"{table}: {len(rows) - n_map} not in V3")
    print(f"  {'users':34s} {users['v2']:>12} {users['matched']:>12}\n")
    if users["v2"] and users["matched"] < users["v2"]:
        gaps.append(f"users: {users['v2'] - users['matched']} not in V3")
    return gaps


def phase_prereqs(facility: str, org: dict, workers: int, dry_run: bool) -> list[str]:
    entries = _discover(facility)
    log.info("PHASE 2a · users")
    users = sync_users(facility, dry_run)
    tables = [t for t in PREREQUISITE_TABLES if entries.get(t, {}).get("v3")]
    log.info("PHASE 2b · lookup tables: %s", ", ".join(tables))
    failed = s2v3.run_migration(facility, tables, workers=workers, dry_run=dry_run) or []
    log.info("PHASE 2c · id-map reconcile")
    reconcile_id_maps(facility, entries, org, dry_run)
    if "evaluation_visit_destinations" in entries and not dry_run:
        log.info("PHASE 2d · departments")
        s2v3.ensure_departments_from_destinations(facility)
    log.info("PHASE 2e · gate")
    gaps = gate(facility, entries, users)
    if failed:
        log.warning("  lookup tables with failed records: %s", failed)
    return gaps


# ─── PHASE 3 ─────────────────────────────────────────────────────────────

def phase_main(facility: str, workers: int, dry_run: bool) -> list[str]:
    entries = _discover(facility)
    tables = [t for t, e in entries.items()
              if e["v3"] and t not in PREREQUISITE_TABLES and t not in NOT_MIGRATED_DIRECTLY]
    log.info("PHASE 3 · %d tables in tier order", len(tables))
    return s2v3.run_migration(facility, tables, workers=workers, dry_run=dry_run) or []


# ─── REPORT ──────────────────────────────────────────────────────────────

def report(facility: str) -> None:
    entries = _discover(facility)
    done = {k.split("sf:", 1)[1] for k in json.load(open(v2v3.DONE_FILE))["done"] if k.startswith(f"{facility}|sf:")} \
        if v2v3.DONE_FILE.exists() else set()
    prog = json.load(open(v2v3.RECORD_PROGRESS_FILE)) if v2v3.RECORD_PROGRESS_FILE.exists() else {}
    print(f"\n  {'table':36s} {'status':10s} records in V3 (progress)")
    for t in sorted(entries, key=lambda x: (x not in PREREQUISITE_TABLES, x)):
        e = entries[t]
        if t in NOT_MIGRATED_DIRECTLY:
            continue
        if not e["v3"]:
            status = "unmapped"
        elif t in done:
            status = "DONE"
        else:
            status = "open"
        n = len(prog.get(f"{facility}|sf:{t}", []))
        print(f"  {t:36s} {status:10s} {n}")


# ─── MAIN ────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--facility", required=True)
    ap.add_argument("--phases", nargs="+", choices=["snowflake", "prereqs", "main"],
                    default=["snowflake", "prereqs", "main"])
    ap.add_argument("--skip-snowflake", action="store_true", help="Data is already in Snowflake")
    ap.add_argument("--plan", action="store_true", help="Setup check + show what would run; write nothing")
    ap.add_argument("--dry-run", action="store_true", help="Transform and count, post nothing to V3")
    ap.add_argument("--allow-gaps", action="store_true", help="Run the main phase even if the gate finds gaps")
    ap.add_argument("--workers", type=int, default=s2v3.PIPELINE_WORKERS)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    for noisy in ("snowflake.connector", "botocore", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    others = _other_processes()
    if others and not args.plan:
        sys.exit("Another loader/migration is running — wait for it to finish:\n  " + "\n  ".join(others))

    org = phase_setup(args.facility)
    phases = [p for p in args.phases if not (p == "snowflake" and args.skip_snowflake)]

    if args.plan:
        entries = _discover(args.facility)
        print(f"\nPlan for {args.facility} (V3 org {org['organization_id']}): phases {phases}")
        print("  prerequisites:", ", ".join(t for t in PREREQUISITE_TABLES if entries.get(t, {}).get("v3")) or "none in Snowflake yet")
        print("  users view:", "yes" if "users" in entries else "not loaded yet")
        main_tables = [t for t, e in entries.items() if e["v3"] and t not in PREREQUISITE_TABLES and t not in NOT_MIGRATED_DIRECTLY]
        print("  main tables:", ", ".join(sorted(main_tables)) or "none in Snowflake yet")
        print("  unmapped (not migrated):", ", ".join(sorted(t for t, e in entries.items() if not e["v3"] and t not in NOT_MIGRATED_DIRECTLY)) or "none")
        return

    if "snowflake" in phases:
        phase_snowflake(args.facility)
    if "prereqs" in phases:
        gaps = phase_prereqs(args.facility, org, args.workers, args.dry_run)
        if gaps and "main" in phases and not args.allow_gaps and not args.dry_run:
            report(args.facility)
            sys.exit("GATE: prerequisites not all in V3 — main tables not started:\n  " + "\n  ".join(gaps) +
                     "\nFix those (or re-run), or pass --allow-gaps to go ahead anyway.")
    if "main" in phases:
        failed = phase_main(args.facility, args.workers, args.dry_run)
        if failed:
            log.warning("PHASE 3 · tables with failures: %s", failed)
    report(args.facility)


if __name__ == "__main__":
    main()
