#!/usr/bin/env python3
"""
snowflake_to_v3_migration.py — Snowflake (flattened CLEAN views) → V3 API migration.

Sibling to v2_to_v3_api_migration.py: same V3 destination, same NAMESPACE_MAP,
transform_record() field-mapping rules, _FK_REMAP tables, and tiered
dependency ordering — but reads source rows from the already-ingested,
already-flattened Snowflake views (<FACILITY>_CLEAN.<table>, built by
flatten_jsons_schemas.py) instead of re-hitting the live V2 facility API.
Everything V3-facing (auth, gateway discovery, POST/retry/dead-letter, the
V2→V3 id map, FK-remap tables, field transforms) is imported unchanged from
v2_to_v3_api_migration.py — this file only replaces the *extraction* layer
and adds the uuid-based FK resolution described below.

WHY UUID, NOT THE RAW "id" COLUMN, FOR FOREIGN KEYS
  v2_to_v3_api_migration.py keys its V2→V3 id map by the raw V2 "id" because
  it reads records live from the V2 API — one row per record, always current.
  flatten_jsons_schemas.py's CLEAN views are built differently: their dedup
  step is `DISTINCT facility_id, payload` over RAW.EVENTS_RAW, which dedupes
  whole-payload duplicates, not by id. A V2 record that was ingested more
  than once (e.g. re-extracted after being updated) can therefore appear as
  MULTIPLE rows in a CLEAN view sharing the same "id" with different content.
  Treating "id" as a unique identity here would risk inserting the same
  real-world record twice, and would corrupt the id map (last write wins,
  arbitrarily). "uuid" is the one column that's guaranteed stable and unique
  per real-world record regardless of how many times it was re-ingested, so
  this pipeline:
    1. Dedupes every table's rows by uuid (keeping the most-recently-updated
       version) before doing anything else.
    2. Keys the shared V2→V3 id map by uuid instead of by raw id.
    3. Still has to translate a child record's *raw integer* FK (e.g.
       visit_id=42 — that's what's actually in the JSON; V2 never stored
       FKs as uuids) into the parent's uuid before it can look the parent up
       in the id map. That one extra hop is what _remap_fks_via_uuid() below
       adds on top of the imported _FK_REMAP / _NS_FK_REMAP tables. It falls
       back to a direct id-based lookup (v2_to_v3_api_migration.py's own
       behaviour, via its sync_id_map() safety net) whenever the parent
       table isn't one of this facility's flattened Snowflake tables.

COVERAGE CAVEAT — read this before running
  Not every Snowflake table has a NAMESPACE_MAP entry yet (e.g. notifications,
  payments_mpesa/cash/card, credit_notes, most theatre_before_*/*_checks
  tables) — those are skipped with a clear reason, never guessed at. And for
  afya_api_auth specifically, Visits and Admissions were never part of this
  facility's ingested table set, so doctor_notes' patient_id injection and
  the vitals inpatient/outpatient split (both of which key off visit_id) have
  nothing to resolve against and will end up dead-lettered — this is a real
  gap in this facility's source data, not a bug in this script. Run with
  --list-tables first to see exactly what will and won't be migrated.

USAGE
  python snowflake_to_v3_migration.py --list-tables --facility afya_api_auth
  python snowflake_to_v3_migration.py --facility afya_api_auth --dry-run
  python snowflake_to_v3_migration.py --facility afya_api_auth
  python snowflake_to_v3_migration.py --facility afya_api_auth --table evaluation_prescriptions
  python snowflake_to_v3_migration.py --facility afya_api_auth --workers 4 --batch-size 100

ENV VARS  (same .env as the rest of the repo)
  # Snowflake (key-pair auth)
  SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_PRIVATE_KEY_PATH

  # V3 destination — same as v2_to_v3_api_migration.py. destination_tenant_id
  # is NOT a config value you pick: it's derived from this account's own
  # login response (v2v3.v3_login_org_cfg()) — a foreign tenant id 403s.
  AFYA_USERNAME, AFYA_PASSWORD

  # core-service only (HMAC signing — its bearer token is ignored):
  CORE_APP_ID, CORE_APP_SECRET
  # theatre/dialysis only (X-Migration-Key):
  MODEL_GATEWAY_MIGRATION_KEY

  # Only needed if _ensure_id_maps() has to fall back to live V2 lookups for
  # a parent table this facility never ingested into Snowflake:
  FACILITY_<NAME>_USERNAME, FACILITY_<NAME>_PASSWORD

COVERAGE REALITY CHECK (as of the 2026-09-20 V3 field-mapping audit)
  Only 5 "journey" models currently accept insert at all: patient, visit,
  patient_sample (reception), prescriptions (patient-evaluation), invoice
  (finance) — everything else clinical (doctor_notes, investigations,
  vitals, etc.) is read/describe-only and will be excluded automatically by
  the live gateway-discovery check in run_migration(), the same way an
  unmapped table is. ~80 reference-data models (banks, theatre_types, sample
  types, etc.) also accept insert. Run --list-tables, then compare its tiers
  against a fresh `_fetch_available_models()` result (logged at the top of
  a real run) to see what will actually go through this time.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

import v2_to_v3_api_migration as v2v3
from facility_to_snowflake_fast_resume import build_namespace, snake_to_pascal

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

# ─── LOGGING ─────────────────────────────────────────────────────────────────

log = logging.getLogger("snowflake_to_v3_migration")
if not log.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s",
        datefmt="%H:%M:%S",
    ))
    log.addHandler(h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ─── CONFIG ──────────────────────────────────────────────────────────────────

SF_DB              = "HOSPITALS"
PIPELINE_WORKERS   = int(os.getenv("PIPELINE_WORKERS", "8"))   # parallel tables within a tier
RECORD_WORKERS     = int(os.getenv("RECORD_WORKERS", "3"))     # parallel V3 POSTs per table
RECORD_LOG_EVERY   = int(os.getenv("RECORD_LOG_EVERY", "100"))

# NOTE on shared state: v2v3.ID_MAP_FILE / DONE_FILE / RECORD_PROGRESS_FILE /
# DEAD_LETTER_FILE / VISIT_PATIENT_FILE / VISIT_ADMISSION_FILE are reused
# on purpose — both migration paths write to the same V3 destination, so
# sharing "what's already migrated" and the id map lets either pipeline pick
# up where the other left off (e.g. if Visits/Admissions are ever migrated
# for afya_api_auth via the live V2 API, this script's doctor_notes/vitals
# jobs will start resolving correctly with no code change).
#
# IMPORTANT: everything from v2_to_v3_api_migration is accessed via the
# `v2v3.` prefix, never via `from v2_to_v3_api_migration import x`. Several
# of its module-level names (_id_map, _gateway_model_meta, _inserted_ids,
# _completed_jobs, _permanently_done, _visit_patient_map, _visit_admission_map)
# get REASSIGNED wholesale by its own loader functions — a `from...import`
# binding would keep pointing at the stale pre-reload object.


# ─── SNOWFLAKE ───────────────────────────────────────────────────────────────

def _snowflake_connect():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER").strip(),
        account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(),
        database=os.getenv("SNOWFLAKE_DATABASE").strip(),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC").strip(),
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip(),
    )


def sf_schema(facility: str, layer: str) -> str:
    return f"{SF_DB}.{facility.upper()}_{layer}"


def _row_to_record(row: tuple, columns: list[str]) -> dict:
    """Snowflake column names come back UPPERCASE (build_flatten_sql quotes
    every alias as "COL"); lowercase them so transform_record() etc. see the
    same field names they'd get from the V2 API's JSON. VARIANT/OBJECT/ARRAY
    columns come back as JSON text — parse them back into dict/list."""
    rec = {}
    for col, val in zip(columns, row):
        if isinstance(val, str) and val[:1] in "{[":
            try:
                val = json.loads(val)
            except (ValueError, TypeError):
                pass
        rec[col.lower()] = val
    return rec


def _candidate_namespaces(module: str, table: str) -> list[str]:
    """NAMESPACE_MAP keys don't consistently strip the module name from the
    entity class name — e.g. evaluation_age_groups -> AgeGroups (stripped)
    but theatre_bookings -> TheatreBookings (module kept). build_namespace()
    always strips; try that first, then the un-stripped PascalCase form, and
    let the caller use whichever actually exists in NAMESPACE_MAP."""
    stripped = build_namespace(module, table)
    unstripped = f"Ignite\\{snake_to_pascal(module)}\\Entities\\{snake_to_pascal(table)}"
    return [stripped] if stripped == unstripped else [stripped, unstripped]


def discover_tables(cur, facility: str) -> list[dict]:
    """One entry per distinct source_table ingested for this facility, each
    carrying its resolved V2 namespace and NAMESPACE_MAP lookup (v3 namespace
    + transform key are None when there's no mapping yet — the caller must
    skip those, never guess at a V3 target)."""
    raw_schema = sf_schema(facility, "RAW")
    rows = cur.execute(f"""
        SELECT DISTINCT source_table, module_source
        FROM {raw_schema}.EVENTS_RAW
        WHERE IS_OBJECT(payload)
    """).fetchall()

    entries = []
    for source_table, module_source in rows:
        candidates = _candidate_namespaces(module_source or "", source_table)
        namespace = next((ns for ns in candidates if ns in v2v3.NAMESPACE_MAP), candidates[0])
        mapping = v2v3.NAMESPACE_MAP.get(namespace)
        entries.append({
            "table":     source_table,
            "module":    module_source,
            "namespace": namespace,
            "v3":        mapping["v3"] if mapping else None,
            "transform": mapping["transform"] if mapping else None,
        })
    return entries


def fetch_clean_rows(cur, facility: str, table: str) -> list[dict]:
    clean_schema = sf_schema(facility, "CLEAN")
    cur.execute(f"SELECT * FROM {clean_schema}.{table}")
    columns = [d[0] for d in cur.description]
    return [_row_to_record(row, columns) for row in cur.fetchall()]


# ─── UUID-BASED DEDUPE + FK RESOLUTION ───────────────────────────────────────

def _record_timestamp(rec: dict) -> str:
    return str(rec.get("updated_at") or rec.get("created_at") or "")


def dedupe_by_uuid(records: list[dict], table: str) -> list[dict]:
    """Keep one row per uuid — the most recently updated version. See the
    module docstring: CLEAN views DISTINCT on the whole payload, not on
    id/uuid, so a re-ingested-after-update record can appear more than once."""
    by_key: dict[str, dict] = {}
    no_uuid = 0
    for rec in records:
        u = rec.get("uuid")
        if not u:
            no_uuid += 1
            key = f"__no_uuid_id_{rec.get('id')}__"
        else:
            key = u
        existing = by_key.get(key)
        if existing is None or _record_timestamp(rec) >= _record_timestamp(existing):
            by_key[key] = rec
    deduped = list(by_key.values())
    dropped = len(records) - len(deduped)
    if dropped:
        log.info("  %-32s deduped %d → %d rows by uuid (%d duplicate snapshot(s) collapsed)%s",
                  table, len(records), len(deduped), dropped,
                  f" — {no_uuid} row(s) had no uuid at all" if no_uuid else "")
    return deduped


# alias -> source_table, populated as each table is discovered+mapped
_alias_to_table: dict[str, str] = {}
_alias_to_table_lock = threading.Lock()

# source_table -> {v2_id: uuid}, populated as each table's rows are fetched
_id_to_uuid: dict[str, dict] = {}
_id_to_uuid_lock = threading.Lock()


def _register_table(alias: str, table: str, records: list[dict]) -> None:
    id_uuid = {r["id"]: r["uuid"] for r in records if r.get("id") is not None and r.get("uuid")}
    with _alias_to_table_lock:
        _alias_to_table.setdefault(alias, table)
    with _id_to_uuid_lock:
        _id_to_uuid[table] = id_uuid


def _store_uuid_mapping(alias: str, uuid_or_id, v3_id) -> None:
    """Same storage as v2v3._store_id_mapping, just reused directly since it's
    key-agnostic (uuid strings and int ids can coexist in the same dict)."""
    v2v3._store_id_mapping(alias, uuid_or_id, v3_id)


def _remap_fks_via_uuid(record: dict, transform_key: str, v3_namespace: str) -> dict:
    """Like v2v3._remap_fks, but resolves each declared FK field through the
    parent's uuid instead of assuming the raw V2 id is a safe id-map key.

    Resolution order per FK field:
      1. If the parent table was one of this facility's Snowflake tables:
         raw id -> parent uuid (via _id_to_uuid) -> id_map[alias][uuid].
      2. Fall back to a direct id_map[alias][raw id] lookup — this is where
         v2v3._ensure_id_maps()'s sync_id_map() safety net pays off for
         parent tables this facility never ingested into Snowflake at all
         (it populates id_map with plain int V2 ids, not uuids).
      3. Otherwise log the same "no mapping" warning v2v3._remap_fks would.
    """
    fk_config = {
        **v2v3._NS_FK_REMAP.get(v3_namespace, {}),
        **v2v3._FK_REMAP.get(transform_key, {}),
    }
    if not fk_config:
        return record

    out = dict(record)
    for field, alias in fk_config.items():
        raw_id = out.get(field)
        if raw_id is None:
            continue
        raw_id = int(raw_id) if str(raw_id).isdigit() else raw_id

        v3_id = None
        parent_table = _alias_to_table.get(alias)
        if parent_table is not None:
            parent_uuid = _id_to_uuid.get(parent_table, {}).get(raw_id)
            if parent_uuid is not None:
                v3_id = v2v3._id_map.get(alias, {}).get(parent_uuid)

        if v3_id is None:
            v3_id = v2v3._id_map.get(alias, {}).get(raw_id)

        if v3_id is not None:
            out[field] = v3_id
        else:
            log.warning("  No V3 ID mapping for %s id=%s (via %s) — %s will fail FK constraint",
                        alias, raw_id, parent_table or "no Snowflake table for this facility", field)
    return out


# ─── PER-TABLE JOB ───────────────────────────────────────────────────────────

def post_table_to_v3(v3_namespace: str, org_cfg: dict, records: list[dict],
                     *, transform_key: str, alias: str, job_key: str, dry_run: bool) -> None:
    """Mirrors v2v3.post_to_v3()'s threading/resume/dead-letter behaviour,
    but keys resume-tracking and the id map by uuid (falling back to id for
    the rare table with no uuid column) instead of the raw V2 id — see the
    module docstring for why."""
    if dry_run:
        log.info("DRY-RUN ✓ would POST %d records → %s", len(records), v3_namespace)
        if records:
            log.info("  sample: %s", json.dumps(records[0], default=str)[:400])
        return

    pending = [r for r in records
               if not v2v3._record_inserted(job_key, r.get("uuid") or r.get("id"))]
    skipped = len(records) - len(pending)
    if skipped:
        log.info("  Skipping %d already-migrated records, posting %d", skipped, len(pending))

    done_count = 0
    progress_lock = threading.Lock()

    def _post_one(record: dict) -> None:
        nonlocal done_count
        remapped = _remap_fks_via_uuid(record, transform_key, v3_namespace)
        rec_key = record.get("uuid") or record.get("id")
        try:
            v3_id = v2v3._post_to_v3_batch(v3_namespace, org_cfg, remapped)
        except v2v3.RecordDeadLettered:
            pass  # not inserted — do not mark done, so a fixed re-run retries it
        else:
            if rec_key is not None:
                v2v3._mark_record_inserted(job_key, rec_key)
            if v3_id is not None and rec_key is not None:
                _store_uuid_mapping(alias, rec_key, v3_id)
        with progress_lock:
            done_count += 1
            n = done_count
        if n % RECORD_LOG_EVERY == 0 or n == len(pending):
            log.info("  Posted %d / %d → %s", n, len(pending), v3_namespace)

    with ThreadPoolExecutor(max_workers=RECORD_WORKERS) as pool:
        futures = {pool.submit(_post_one, r): r for r in pending}
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                rec = futures[fut]
                log.error("  Failed record uuid=%s: %s", rec.get("uuid"), e)


def _run_vitals_split(entry: dict, facility: str, rows: list[dict], org_cfg: dict,
                      job_key: str, label: str, dry_run: bool) -> None:
    """Same inpatient/outpatient vitals split as v2v3._run_vitals_split_job,
    reusing its persisted visit->admission / visit->patient side-channel maps
    (populated only if Visits/Admissions have ever been migrated for this
    facility via v2_to_v3_api_migration.py — see the coverage caveat above)."""
    admitted, outpatient, orphans = [], [], []
    for r in rows:
        visit_id = r.get("visit_id")
        if visit_id in v2v3._visit_admission_map:
            admitted.append(r)
        elif visit_id in v2v3._visit_patient_map:
            outpatient.append(r)
        else:
            orphans.append(r)

    log.info("  %s — split %d rows: %d admitted, %d outpatient, %d unroutable (visit not migrated)",
              label, len(rows), len(admitted), len(outpatient), len(orphans))

    if orphans and not dry_run:
        for r in orphans:
            v2v3._write_dead_letter(
                "App\\Models\\Vital[unrouted]", r,
                {"reason": f"visit_id={r.get('visit_id')} not found in visit->admission or "
                           f"visit->patient map — Visits/Admissions not migrated for this facility"},
            )

    def _prep(partition: list[dict], tk: str) -> list[dict]:
        raw = [v2v3.transform_record(r, tk, org_cfg) for r in partition]
        return [r for r in raw if r is not None]

    admitted_t   = _prep(admitted, "inpatient_vital")
    outpatient_t = _prep(outpatient, v2v3.OUTPATIENT_VITAL_TRANSFORM)

    if not dry_run:
        v2v3._ensure_id_maps("inpatient_vital", facility)
        v2v3._ensure_id_maps(v2v3.OUTPATIENT_VITAL_TRANSFORM, facility)

    if admitted_t:
        post_table_to_v3(r"App\Models\Vital", org_cfg, admitted_t,
                          transform_key="inpatient_vital",
                          alias=v2v3._v3_alias(r"App\Models\Vital"),
                          job_key=f"{job_key}::inpatient", dry_run=dry_run)
    if outpatient_t:
        post_table_to_v3(v2v3.OUTPATIENT_VITAL_V3_NAMESPACE, org_cfg, outpatient_t,
                          transform_key=v2v3.OUTPATIENT_VITAL_TRANSFORM,
                          alias=v2v3._v3_alias(v2v3.OUTPATIENT_VITAL_V3_NAMESPACE),
                          job_key=f"{job_key}::outpatient", dry_run=dry_run)


def run_table_job(entry: dict, facility: str, org_cfg: dict, dry_run: bool) -> bool:
    """Fetch one table's CLEAN view, dedupe by uuid, transform, and migrate.
    Returns True on success (including "nothing to do"), False on failure."""
    table, v3_namespace, transform_key = entry["table"], entry["v3"], entry["transform"]
    alias = v2v3._v3_alias(v3_namespace)
    job_key = f"{facility}|sf:{table}"
    label = f"[{facility}] {table} → {alias}"
    log.info("▶ %s", label)
    t0 = time.perf_counter()

    if job_key in v2v3._permanently_done:
        log.info("⊘ %s — already migrated in a previous run", label)
        return True

    try:
        with _snowflake_connect() as conn:
            cur = conn.cursor()
            rows = fetch_clean_rows(cur, facility, table)
            cur.close()
    except Exception as e:
        log.error("✗ Snowflake fetch FAILED %s: %s", label, e)
        return False

    if not rows:
        log.info("⊘ %s — 0 rows in Snowflake", label)
        v2v3._mark_done(_run_id, job_key)
        return True

    rows = dedupe_by_uuid(rows, table)
    _register_table(alias, table, rows)

    if transform_key == "inpatient_vital":
        try:
            _run_vitals_split(entry, facility, rows, org_cfg, job_key, label, dry_run)
        except Exception as e:
            log.error("✗ V3 POST FAILED %s: %s", label, e)
            return False
        if not dry_run:
            v2v3._mark_done(_run_id, job_key)
        return True

    transformed_raw = [v2v3.transform_record(r, transform_key, org_cfg) for r in rows]
    transformed = [r for r in transformed_raw if r is not None]
    n_dropped = len(transformed_raw) - len(transformed)
    if n_dropped:
        log.warning("  %s — %d/%d records dropped by required-field check",
                    label, n_dropped, len(transformed_raw))

    if not transformed:
        log.warning("⊘ %s — nothing left to post after required-field check", label)
        if not dry_run:
            v2v3._mark_done(_run_id, job_key)
        return True

    if not dry_run:
        v2v3._ensure_id_maps(transform_key, facility)

    try:
        post_table_to_v3(v3_namespace, org_cfg, transformed,
                          transform_key=transform_key, alias=alias,
                          job_key=job_key, dry_run=dry_run)
    except v2v3.GatewayModelNotRegistered as e:
        log.warning("⊘ %s — model not registered in gateway: %s", label, e)
        return True
    except Exception as e:
        log.error("✗ V3 POST FAILED %s: %s", label, e)
        return False

    elapsed = time.perf_counter() - t0
    log.info("✓ %s — %d records migrated in %.2fs", label, len(transformed), elapsed)
    if not dry_run:
        v2v3._mark_done(_run_id, job_key)
    return True


# ─── ORCHESTRATOR ────────────────────────────────────────────────────────────

_run_id: str = ""


def run_migration(facility: str, only_tables: list[str] | None,
                  *, workers: int, dry_run: bool) -> None:
    global _run_id
    _run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

    v2v3._load_id_map()
    v2v3._load_visit_patient_map()
    v2v3._load_visit_admission_map()
    v2v3._load_record_progress()
    v2v3._load_permanently_done()

    # destination_tenant_id is derived from THIS account's own login response,
    # not a static per-facility table — source/destination_tenant_id must
    # equal the authenticated account's own organization or every gateway
    # call 403s ("foreign tenant"), so whatever org the AFYA_USERNAME/
    # AFYA_PASSWORD account belongs to is the only valid destination.
    org_cfg = v2v3.v3_login_org_cfg()
    if org_cfg.get("organization_id") is None:
        log.error(
            "Could not derive organization_id from the V3 login response — "
            "check AFYA_USERNAME/AFYA_PASSWORD and the /v1/login response shape."
        )
        sys.exit(1)
    log.info("V3 destination — organization_id=%s facility_id=%s",
              org_cfg.get("organization_id"), org_cfg.get("facility_id"))

    log.info("Discovering gateway models …")
    available_models = v2v3._fetch_available_models()

    with _snowflake_connect() as conn:
        cur = conn.cursor()
        entries = discover_tables(cur, facility)
        cur.close()

    if only_tables:
        entries = [e for e in entries if e["table"] in only_tables]

    mapped   = [e for e in entries if e["v3"]]
    unmapped = [e for e in entries if not e["v3"]]

    def _insertable(e: dict) -> bool:
        return not available_models or v2v3._v3_alias(e["v3"]) in available_models

    no_insert = [e for e in mapped if not _insertable(e)]
    runnable  = [e for e in mapped if _insertable(e)]

    log.info(
        "Facility %s — %d Snowflake tables discovered: %d mapped+insertable, "
        "%d mapped but not insertable in gateway, %d with no NAMESPACE_MAP entry",
        facility, len(entries), len(runnable), len(no_insert), len(unmapped),
    )
    if unmapped:
        log.warning("SKIPPED — no NAMESPACE_MAP entry (%d): %s",
                    len(unmapped), ", ".join(sorted(e["table"] for e in unmapped)))
    if no_insert:
        log.warning("SKIPPED — mapped but not insertable per gateway (%d): %s",
                    len(no_insert), ", ".join(sorted(e["table"] for e in no_insert)))

    tier_groups: dict[int, list] = defaultdict(list)
    for e in runnable:
        tier_groups[v2v3._namespace_tier(e["namespace"])].append(e)

    failures: list[str] = []
    for tier_num in sorted(tier_groups):
        tier_entries = tier_groups[tier_num]
        log.info("── Tier %d ── %d table(s): %s", tier_num, len(tier_entries),
                  ", ".join(e["table"] for e in tier_entries))
        if workers <= 1:
            for e in tier_entries:
                if not run_table_job(e, facility, org_cfg, dry_run):
                    failures.append(e["table"])
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                future_to_entry = {
                    pool.submit(run_table_job, e, facility, org_cfg, dry_run): e
                    for e in tier_entries
                }
                for fut in as_completed(future_to_entry):
                    e = future_to_entry[fut]
                    try:
                        ok = fut.result()
                    except Exception as ex:
                        log.error("Unhandled error [%s]: %s", e["table"], ex)
                        ok = False
                    if not ok:
                        failures.append(e["table"])

    log.info(
        "\n══════════════════════  MIGRATION SUMMARY  ══════════════════════\n"
        "  Run ID    : %s\n"
        "  Migrated  : %d / %d runnable tables (%d failed)\n"
        "  Skipped   : %d no NAMESPACE_MAP entry | %d not insertable in gateway\n"
        "%s"
        "═════════════════════════════════════════════════════════════════",
        _run_id, len(runnable) - len(failures), len(runnable), len(failures),
        len(unmapped), len(no_insert),
        f"  FAILED: {', '.join(failures)}\n" if failures else "",
    )


def list_tables(facility: str) -> None:
    with _snowflake_connect() as conn:
        cur = conn.cursor()
        entries = discover_tables(cur, facility)
        cur.close()
    entries.sort(key=lambda e: (e["v3"] is None, v2v3._namespace_tier(e["namespace"]), e["table"]))
    print(f"{'table':<38} {'tier':<5} {'v3 target':<40} transform")
    print("-" * 110)
    for e in entries:
        if e["v3"]:
            tier = v2v3._namespace_tier(e["namespace"])
            print(f"{e['table']:<38} {tier:<5} {e['v3']:<40} {e['transform']}")
        else:
            print(f"{e['table']:<38} {'—':<5} {'(no NAMESPACE_MAP entry)':<40} —")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate flattened Snowflake CLEAN views to the V3 Afya API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--facility", "-f", required=True,
                        help="Facility key (matches its Snowflake schema prefix, e.g. afya_api_auth)")
    parser.add_argument("--table", "-t", nargs="+", metavar="NAME",
                        help="One or more Snowflake source_table names to migrate (default: all mapped)")
    parser.add_argument("--workers", "-w", type=int, default=PIPELINE_WORKERS,
                        help=f"Parallel tables per tier (default: {PIPELINE_WORKERS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch + transform without posting to V3")
    parser.add_argument("--list-tables", action="store_true",
                        help="Print discovered tables, their tier and V3 mapping, and exit")
    args = parser.parse_args()

    if args.list_tables:
        list_tables(args.facility)
        return

    run_migration(args.facility, args.table, workers=args.workers, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
