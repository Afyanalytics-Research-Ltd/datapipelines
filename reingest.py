#!/usr/bin/env python3
"""
reingest.py — re-load some tables cleanly: V3 delete → V2 → Snowflake → V3.

Re-inserted V3 rows get NEW ids, so a table can't be re-ingested on its own:
every table that links to it (directly or through another table) has to be
re-ingested with it, or its rows keep pointing at deleted ids. This tool
works out that cascade from the migration's FK config, then:

  plan (default, read-only)
      Prints the cascade, how many rows each table has in V3 / Snowflake,
      the order the V3 backend must delete them in, and the local state that
      will be reset. Nothing is changed.

  --execute
      1. Refuses to start if another loader / migration process is running.
      2. Checks V3 really is empty for every table in the cascade (the
         backend's hard delete — step 1 — can't be done through the gateway).
      3. Backs up and resets this facility's local state for those tables:
         .migration_done.json, .migration_record_progress.json (+ ::uncertain),
         the facility's own keys in .migration_id_map.json (other facilities'
         mappings for the same models are left alone), .progress.json and
         .page_progress/.
      4. Clears those tables from Snowflake: their rows in {FACILITY}_RAW.EVENTS_RAW
         and the matching rows in {FACILITY}_CLEAN.EVENTS.
      5. Re-extracts them from V2 (from 1970, watermark untouched).
      6. Rebuilds their CLEAN views.
      7. Migrates them to V3 in dependency order.

USAGE
  python reingest.py --facility kisumu_v3 --table admissions
  python reingest.py --facility kisumu_v3 --table admissions --execute
  python reingest.py --facility kisumu_v3 --table admissions --execute --no-flatten   # stop after fresh RAW load
  python reingest.py --facility kisumu_v3 --table admissions --execute --no-migrate   # stop after CLEAN views
  python reingest.py --facility kisumu_v3 --table settings_clinics --execute --skip-v3-check
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

import facility_to_snowflake_fast_resume as loader
import flatten_jsons_schemas as fl
import snowflake_to_v3_migration as s2v3
import v2_to_v3_api_migration as v2v3

log = logging.getLogger("reingest")
ROOT = Path(__file__).resolve().parent
FULL_SINCE = "1970-01-01T00:00:00Z"

# Links set by _PER_KEY_INJECT / the vitals split rather than _FK_REMAP, so
# the FK config alone doesn't show them (transform key -> parent aliases).
_INJECTED_DEPS: dict[str, list[str]] = {
    "evaluation_doctor_note": ["patient", "visit"],
    "inpatient_vital":        ["admission", "patient", "visit"],
    "outpatient_vital":       ["patient", "visit"],
}


def _same_alias(a: str, b: str) -> bool:
    """Gateway aliases mix singular and plural for the same model (the id
    map holds 'visits' while FK config says 'visit')."""
    def stem(x: str) -> str:
        x = x.lower()
        if x.endswith("ies"):
            return x[:-3] + "y"
        return x[:-1] if x.endswith("s") else x
    return stem(a) == stem(b)


def _other_processes() -> list[str]:
    try:
        out = subprocess.run(["pgrep", "-af", r"python.*(facility_to_snowflake_fast_resume|snowflake_to_v3_migration|v2_to_v3_api_migration)\.py"],
                             capture_output=True, text=True).stdout
    except FileNotFoundError:
        return []
    return [line for line in out.splitlines() if line.strip() and str(os.getpid()) != line.split()[0]]


# ─── CASCADE ─────────────────────────────────────────────────────────────

def table_meta(facility: str) -> dict[str, dict]:
    """One entry per Snowflake source table that maps to a V3 model."""
    with s2v3._snowflake_connect() as conn:
        cur = conn.cursor()
        entries = s2v3.discover_tables(cur, facility)
        cur.close()
    meta = {}
    for e in entries:
        if not e["v3"]:
            continue
        tk = e["transform"]
        alias = s2v3._ALIAS_OVERRIDE.get(tk) or v2v3._v3_alias(e["v3"])
        parents = set(v2v3._NS_FK_REMAP.get(e["v3"], {}).values()) | set(v2v3._FK_REMAP.get(tk, {}).values())
        parents |= {v2v3._v3_alias(ns) for ns in v2v3._PER_KEY_EXTRA_ID_DEPS.get(tk, [])}
        parents |= set(_INJECTED_DEPS.get(tk, []))
        if tk == "inpatient_vital":   # vitals rows go to both vital models
            parents |= set(_INJECTED_DEPS["outpatient_vital"])
        meta[e["table"]] = {
            "table": e["table"], "v3": e["v3"], "transform": tk, "alias": alias,
            "service": s2v3._SERVICE_OVERRIDE.get(tk) or v2v3._alias_to_service.get(alias),
            "parents": sorted(parents), "tier": v2v3._namespace_tier(e["namespace"]),
        }
    return meta


def cascade(meta: dict[str, dict], tables: list[str]) -> list[str]:
    """The requested tables plus everything that links to them, transitively."""
    chosen, queue = set(tables), list(tables)
    while queue:
        alias = meta[queue.pop()]["alias"]
        for t, m in meta.items():
            if t not in chosen and any(_same_alias(p, alias) for p in m["parents"]):
                chosen.add(t)
                queue.append(t)
    return sorted(chosen, key=lambda t: (meta[t]["tier"], t))


# ─── COUNTS / CHECKS ─────────────────────────────────────────────────────

def v3_has_rows(m: dict, org_cfg: dict) -> bool | None:
    try:
        r = v2v3._gateway_post(m["service"] or "core", {
            "action": "read", "model": m["alias"], "source_tenant_id": org_cfg["organization_id"],
            "per_page": 1, "page": 1}, timeout=60)
        if not r.ok:
            return None
        data = r.json().get("data")
        rows = data if isinstance(data, list) else (data or {}).get("data") if isinstance(data, dict) else []
        return bool(rows)
    except Exception:
        return None


def raw_counts(facility: str, tables: list[str]) -> dict[str, int]:
    names = ", ".join(f"'{t}'" for t in tables)
    with s2v3._snowflake_connect() as conn:
        rows = conn.cursor().execute(
            f"SELECT source_table, COUNT(*) FROM {s2v3.sf_schema(facility, 'RAW')}.EVENTS_RAW "
            f"WHERE source_table IN ({names}) GROUP BY 1").fetchall()
    return dict(rows)


def source_keys(facility: str, table: str) -> set[str]:
    """The id-map / progress keys this facility's rows use: uuid when the
    table has one, else the V2 id (same rule as post_table_to_v3)."""
    view = f"{s2v3.sf_schema(facility, 'CLEAN')}.{table.upper()}"
    with s2v3._snowflake_connect() as conn:
        cur = conn.cursor()
        cols = {d[0].lower() for d in cur.execute(f"SELECT * FROM {view} LIMIT 0").description}
        sql = (f"SELECT COALESCE(LOWER(TRIM(uuid::STRING)), id::STRING) FROM {view}" if "uuid" in cols
               else f"SELECT id::STRING FROM {view}")
        return {r[0] for r in cur.execute(sql).fetchall() if r[0] is not None}


# ─── RESET ───────────────────────────────────────────────────────────────

def _backup(path: Path, stamp: str) -> None:
    if path.exists():
        shutil.copy2(path, path.with_name(f"{path.name}.bak_reingest_{stamp}"))


def reset_local_state(facility: str, meta: dict, tables: list[str], stamp: str) -> None:
    job_keys = {f"{facility}|sf:{t}" for t in tables}

    done_file = v2v3.DONE_FILE
    if done_file.exists():
        _backup(done_file, stamp)
        d = json.loads(done_file.read_text())
        before = len(d.get("done", []))
        d["done"] = [k for k in d.get("done", []) if k not in job_keys]
        done_file.write_text(json.dumps(d, indent=2))
        log.info("  %s: %d job(s) un-marked", done_file.name, before - len(d["done"]))

    prog_file = v2v3.RECORD_PROGRESS_FILE
    if prog_file.exists():
        _backup(prog_file, stamp)
        p = json.loads(prog_file.read_text())
        dropped = {k: len(p.pop(k)) for k in list(p) if k in job_keys or k.split("::")[0] in job_keys}
        prog_file.write_text(json.dumps(p, indent=2))
        log.info("  %s: cleared %s", prog_file.name, dropped or "nothing")

    map_file = v2v3.ID_MAP_FILE
    if map_file.exists() and map_file.read_text().strip():
        _backup(map_file, stamp)
        idmap = json.loads(map_file.read_text())
        for t in tables:
            keys = source_keys(facility, t)
            for alias in [a for a in idmap if _same_alias(a, meta[t]["alias"])]:
                before = len(idmap[alias])
                idmap[alias] = {k: v for k, v in idmap[alias].items() if str(k).lower() not in keys}
                log.info("  %s[%s]: removed %d of %s's keys (%d other entries kept)",
                         map_file.name, alias, before - len(idmap[alias]), t, len(idmap[alias]))
        map_file.write_text(json.dumps(idmap, indent=2))

    progress = loader.PROGRESS_FILE
    if progress.exists():
        _backup(progress, stamp)
        pr = json.loads(progress.read_text())
        for skey in (facility, f"{facility}|old_system_history"):
            completed = (pr.get(skey) or {}).get("completed", {})
            for k in [k for k in completed if k.split("|", 1)[-1] in tables]:
                completed.pop(k)
        progress.write_text(json.dumps(pr, indent=2, sort_keys=True))

    page_dir = loader.PAGE_STATE_DIR / facility
    if page_dir.exists():
        for t in tables:
            prefix = f"{loader._safe_s3_token(t)}__"
            for p in page_dir.glob(prefix + "*"):
                shutil.rmtree(p) if p.is_dir() else p.unlink()


def delete_snowflake_rows(facility: str, tables: list[str]) -> None:
    """Clear the tables from Snowflake: first their rows in CLEAN.EVENTS (the
    loader's merged table — it has no source_table column, so rows are
    matched to RAW on id + full payload), then RAW.EVENTS_RAW itself. The
    per-table CLEAN views read RAW, so they're empty from this point until
    rebuild_views() runs on the fresh load."""
    names = ", ".join(f"'{t}'" for t in tables)
    raw = f"{s2v3.sf_schema(facility, 'RAW')}.EVENTS_RAW"
    events = f"{s2v3.sf_schema(facility, 'CLEAN')}.EVENTS"
    with s2v3._snowflake_connect() as conn:
        cur = conn.cursor()
        n_events = cur.execute(f"""
            DELETE FROM {events} e USING (
                SELECT DISTINCT payload:id::STRING AS event_id, payload
                FROM {raw} WHERE source_table IN ({names}) AND IS_OBJECT(payload)
            ) r
            WHERE e.event_id = r.event_id AND e.payload = r.payload""").rowcount
        n_raw = cur.execute(f"DELETE FROM {raw} WHERE source_table IN ({names})").rowcount
    log.info("  deleted %s RAW rows and %s CLEAN.EVENTS rows", n_raw, n_events)


# ─── RELOAD ──────────────────────────────────────────────────────────────

def reload_from_v2(facility: str, tables: list[str]) -> None:
    by_set: dict[str, list[str]] = defaultdict(list)
    for t in tables:
        by_set["old_system_history" if t in loader.OLD_SYSTEM_HISTORY_TABLES else "sheet"].append(t)
    for table_set, ts in by_set.items():
        log.info("  V2 → Snowflake [%s]: %s", table_set, ", ".join(ts))
        try:
            loader.run_pipeline(facility, since=FULL_SINCE, only_tables=set(ts), table_set=table_set,
                                update_watermark=False, resume=True)
        except SystemExit as e:
            raise RuntimeError(f"V2 → Snowflake load failed for {ts} (exit {e.code}); see log above. "
                               f"Re-run the same reingest --execute command to resume.") from e
    missing = [t for t in tables if not raw_counts(facility, [t])]
    if missing:
        raise RuntimeError(f"No rows came back from V2 for {missing} — stopping before V3.")


def rebuild_views(facility: str, tables: list[str]) -> None:
    raw, clean = f"{facility.upper()}_RAW", f"{facility.upper()}_CLEAN"
    conn = fl._snowflake_connect()
    try:
        cur = conn.cursor()
        for t in tables:
            fields = fl.discover_fields(cur, raw, t)
            cur.execute(fl.build_flatten_sql(raw, clean, t, fl.expand_objects(cur, raw, t, fields)))
            # Gate before V3: the view must hold exactly the fresh load — every
            # RAW row for the table (they were all deleted before the reload, so
            # nothing stale can be in it) and nothing else.
            n_raw = cur.execute(f"SELECT COUNT(*) FROM {raw}.EVENTS_RAW "
                                f"WHERE source_table = '{t}' AND IS_OBJECT(payload)").fetchone()[0]
            n_view = cur.execute(f"SELECT COUNT(*) FROM {clean}.{t}").fetchone()[0]
            n_ids = cur.execute(f"SELECT COUNT(DISTINCT id) FROM {clean}.{t}").fetchone()[0]
            if n_view == 0 or n_view != n_raw:
                raise RuntimeError(f"{clean}.{t} has {n_view} rows but the fresh RAW load has {n_raw} — "
                                   f"not pushing to V3. Check the load, then re-run with --execute.")
            log.info("  view %s.%s rebuilt · %d rows (%d distinct ids) — matches the fresh load",
                     clean, t, n_view, n_ids)
    finally:
        conn.close()


# ─── MAIN ────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--facility", required=True)
    ap.add_argument("--table", nargs="+", required=True, help="Snowflake source tables to re-ingest")
    ap.add_argument("--execute", action="store_true", help="Do it (default: print the plan only)")
    ap.add_argument("--no-flatten", action="store_true", help="Stop after the fresh V2 → RAW load (steps 1-5)")
    ap.add_argument("--no-migrate", action="store_true", help="Stop after rebuilding the CLEAN views (steps 1-6)")
    ap.add_argument("--skip-v3-check", action="store_true",
                    help="Don't require V3 to be empty first (e.g. shared lookup tables)")
    ap.add_argument("--workers", type=int, default=s2v3.PIPELINE_WORKERS)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s",
                        datefmt="%H:%M:%S")

    v2v3.set_v3_target_facility(args.facility)
    org_cfg = v2v3.v3_login_org_cfg()
    v2v3._fetch_available_models()
    meta = table_meta(args.facility)
    unknown = [t for t in args.table if t not in meta]
    if unknown:
        sys.exit(f"Not a mapped Snowflake table for {args.facility}: {unknown}. Known: {sorted(meta)}")

    tables = cascade(meta, args.table)
    raw = raw_counts(args.facility, tables)
    in_v3 = {t: v3_has_rows(meta[t], org_cfg) for t in tables}

    print(f"\nRe-ingest plan for {args.facility} (V3 org {org_cfg['organization_id']}):")
    print(f"  requested: {', '.join(args.table)}")
    extra = [t for t in tables if t not in args.table]
    print(f"  + linked tables that must come along: {', '.join(extra) or 'none'}\n")
    print(f"  {'table':34s} {'V3 model':24s} {'service':11s} {'links to':28s} {'RAW rows':>9s}  V3 rows?")
    for t in tables:
        m = meta[t]
        flag = {True: "yes", False: "empty", None: "can't read"}[in_v3[t]]
        print(f"  {t:34s} {m['alias']:24s} {str(m['service']):11s} {','.join(m['parents'])[:28]:28s} {raw.get(t, 0):>9}  {flag}")
    print("\n  STEP 1 — V3 backend must HARD-delete (no deleted_at left behind, UNIQUE(uuid) is enforced),")
    print(f"           org {org_cfg['organization_id']} only, children first:")
    for i, t in enumerate(reversed(tables), 1):
        print(f"     {i}. {meta[t]['service']}-service · model '{meta[t]['alias']}'  (source table {t})")
    print("  Then: python reingest.py --facility", args.facility, "--table", *args.table, "--execute\n")
    if not args.execute:
        return

    others = _other_processes()
    if others:
        sys.exit("Another loader/migration is running — wait for it to finish:\n  " + "\n  ".join(others))
    if not args.skip_v3_check:
        still = [t for t in tables if in_v3[t]]
        unreadable = [t for t in tables if in_v3[t] is None]
        if still or unreadable:
            sys.exit(f"V3 isn't empty yet for {still}{' (unreadable: ' + str(unreadable) + ')' if unreadable else ''}. "
                     f"Have the backend delete them first, or pass --skip-v3-check if that's intended.")

    stamp = time.strftime("%Y%m%dT%H%M%S")
    log.info("Resetting local state (backups: *.bak_reingest_%s)", stamp)
    reset_local_state(args.facility, meta, tables, stamp)
    log.info("Clearing the tables from Snowflake (RAW + CLEAN.EVENTS)")
    delete_snowflake_rows(args.facility, tables)
    log.info("Re-extracting from V2")
    reload_from_v2(args.facility, tables)
    if args.no_flatten:
        log.info("Stopped after the fresh load (--no-flatten). Next: rebuild the CLEAN views "
                 "(python flatten_jsons_schemas.py), then python snowflake_to_v3_migration.py --facility %s --table %s",
                 args.facility, " ".join(tables))
        return
    log.info("Rebuilding CLEAN views")
    rebuild_views(args.facility, tables)
    if args.no_migrate:
        log.info("Stopped before V3 (--no-migrate). Next: python snowflake_to_v3_migration.py --facility %s --table %s",
                 args.facility, " ".join(tables))
        return
    log.info("Migrating to V3")
    failed = s2v3.run_migration(args.facility, tables, workers=args.workers, dry_run=False)
    if failed:
        sys.exit(f"Re-ingest finished with failed tables: {failed} — re-run the migration for them.")
    log.info("Re-ingest complete: %s", ", ".join(tables))


if __name__ == "__main__":
    main()
