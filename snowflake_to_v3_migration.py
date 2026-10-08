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
import re
import logging
import os
import socket
import sys
import threading
import time
import uuid as uuid_lib
from collections import defaultdict
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

import v2_to_v3_api_migration as v2v3
import v3_passthrough as v3pt
from facility_to_snowflake_fast_resume import (
    OLD_SYSTEM_HISTORY_TABLES, build_namespace, snake_to_pascal,
)

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

# _id_to_uuid (raw V2 id -> uuid, per source table) used to be purely
# in-memory, rebuilt fresh every run from whatever tables that run itself
# fetched. That's a real gap, not just a theoretical one: confirmed 2026-10
# that running `--table visits` on its own (patients already migrated in an
# earlier, separate process) leaves _id_to_uuid["patients"] empty, so
# _remap_fks_via_uuid can't translate a visit's raw patient id into the
# parent's uuid, falls through to a direct id_map lookup that's keyed by
# uuid (not raw id) because patients went through this same uuid-keyed
# pipeline — and silently leaves patient_id as the raw V2 int, which fails
# V3's FK constraint on every single visit. Persisting this table to disk,
# exactly like v2v3's ID_MAP_FILE, closes that gap: a table's id->uuid
# mapping survives past the run that ingested it, so any later run
# migrating a child table can still resolve the parent correctly.
ID_TO_UUID_FILE = Path(__file__).resolve().parent / ".migration_id_to_uuid.json"


def use_state_dir(facility: str) -> Path:
    """This facility's own state directory (v2v3.use_state_dir) — every
    entry point calls it before loading state."""
    global ID_TO_UUID_FILE
    d = v2v3.use_state_dir(facility)
    ID_TO_UUID_FILE = d / ".migration_id_to_uuid.json"
    return d

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
        # Long table jobs read Snowflake page by page for well over an hour;
        # without keep-alive the session token lapses at ~60 minutes.
        client_session_keep_alive=True,
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


def _as_list(value) -> list:
    if isinstance(value, list):
        return value
    try:
        return json.loads(value) if value else []
    except (TypeError, ValueError):
        return []


def _stored_namespace(value: str | None) -> str | None:
    """RAW.namespace as written by the loader. Its COPY literal ate the
    backslashes ('IgniteReceptionEntitiesNextOfKin'), so put them back."""
    if not value:
        return None
    if "\\" in value:
        return value
    m = re.match(r"^Ignite([A-Z][a-z]+)Entities([A-Za-z0-9_]+)$", value)
    return f"Ignite\\{m.group(1)}\\Entities\\{m.group(2)}" if m else None


def _tier(entry: dict) -> int:
    """Dependency tier: V3 pass-through tables carry their own (from their links)."""
    return entry.get("tier") or v2v3._namespace_tier(entry["namespace"] or "")


def _is_v3_shaped(module_source) -> bool:
    """from_json_mappings_to_snowflake stores the V3 service ("reception-service")
    as module_source; V2 loads store the V2 module ("Reception", "Evaluation")."""
    return str(module_source or "").lower().endswith("-service")


def discover_tables(cur, facility: str) -> list[dict]:
    """One entry per distinct source_table ingested for this facility, each
    carrying its resolved V2 namespace and NAMESPACE_MAP lookup (v3 namespace
    + transform key are None when there's no mapping yet — the caller must
    skip those, never guess at a V3 target)."""
    raw_schema = sf_schema(facility, "RAW")
    rows = cur.execute(f"""
        SELECT source_table, ANY_VALUE(module_source), ARRAY_AGG(DISTINCT namespace)
        FROM {raw_schema}.EVENTS_RAW
        WHERE IS_OBJECT(payload)
        GROUP BY source_table
    """).fetchall()

    entries = []
    v3_rows = [(t, ms, _as_list(st)) for t, ms, st in rows if _is_v3_shaped(ms)]
    if v3_rows:
        # Loaded by from_json_mappings_to_snowflake: rows are already V3 records
        # (module_source = the V3 service). The V2 transforms would re-shape
        # them and remap their links as V2 ids — they go through
        # v3_passthrough instead (gateway model by table, links via the id map).
        fields = {t: set(_as_list(keys)) for t, keys in cur.execute(f"""
            SELECT source_table, ARRAY_UNION_AGG(OBJECT_KEYS(payload))
            FROM {raw_schema}.EVENTS_RAW
            WHERE IS_OBJECT(payload) AND module_source ILIKE '%-service'
            GROUP BY source_table""").fetchall()}
        entries += v3pt.prepare(facility, v3_rows, fields)
    for source_table, module_source, stored in rows:
        if _is_v3_shaped(module_source):
            continue
        candidates = _candidate_namespaces(module_source or "", source_table)
        # Fallback: the V2 class the table was actually extracted with (stored
        # per row in RAW). Tables loaded with an explicit class — the old-
        # system-history set: NextOfKin, Sample, Discharge … — don't follow the
        # name pattern the guesses above rely on. Guesses stay first so no
        # table that already resolves changes target.
        candidates += [ns for ns in (_stored_namespace(x) for x in _as_list(stored)) if ns]
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
    sql = _FETCH_SQL.get(table)
    cur.execute(sql.format(clean=clean_schema) if sql else f"SELECT * FROM {clean_schema}.{table}")
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

# source_table -> {v2_id: uuid}, populated as each table's rows are fetched.
# Persisted to ID_TO_UUID_FILE (see its comment above) so a later run
# migrating a child table can still resolve a parent it didn't itself fetch.
_id_to_uuid: dict[str, dict] = {}
_id_to_uuid_lock = threading.Lock()


def _load_id_to_uuid() -> None:
    """Load both _id_to_uuid AND _alias_to_table — _remap_fks_via_uuid needs
    both together (alias -> table name, then table name -> {id: uuid}), so
    persisting only one of them would still leave the other empty on a fresh
    process and silently break FK resolution exactly like the bug this was
    written to fix."""
    global _id_to_uuid, _alias_to_table
    if not ID_TO_UUID_FILE.exists():
        _id_to_uuid = {}
        _alias_to_table = {}
        return
    try:
        raw = json.loads(ID_TO_UUID_FILE.read_text())
        _id_to_uuid = {
            table: {(int(k) if k.isdigit() else k): v for k, v in mapping.items()}
            for table, mapping in raw.get("id_to_uuid", {}).items()
        }
        _alias_to_table = dict(raw.get("alias_to_table", {}))
        total = sum(len(v) for v in _id_to_uuid.values())
        if total:
            log.info("id->uuid map loaded — %d entries across %d tables (%d aliases)",
                      total, len(_id_to_uuid), len(_alias_to_table))
    except Exception as e:
        log.warning("Could not load %s: %s — starting fresh", ID_TO_UUID_FILE.name, e)
        _id_to_uuid = {}
        _alias_to_table = {}


def _register_table(alias: str, table: str, records: list[dict]) -> None:
    """Updates _id_to_uuid/_alias_to_table in memory AND on disk.

    The disk write merges with whatever is currently on disk rather than
    overwriting it with just this process's in-memory state — confirmed
    this matters in practice: a process that only ever fetches "visits"
    never loads "patients" into its own memory, so writing its in-memory
    dict verbatim would silently erase the "patients" entries an earlier,
    separate process had already persisted. This isn't airtight against two
    processes writing at the exact same instant, but it closes the much
    more common case of sequential separate-table runs stepping on each
    other, which is exactly what happened before this fix existed.
    """
    id_uuid = {r["id"]: r["uuid"] for r in records if r.get("id") is not None and r.get("uuid")}
    with _alias_to_table_lock, _id_to_uuid_lock, v2v3._file_lock(ID_TO_UUID_FILE):
        _alias_to_table.setdefault(alias, table)
        _id_to_uuid.setdefault(table, {}).update(id_uuid)   # page by page
        try:
            on_disk = json.loads(ID_TO_UUID_FILE.read_text()) if ID_TO_UUID_FILE.exists() else {}
        except Exception:
            on_disk = {}
        merged_id_to_uuid = {**on_disk.get("id_to_uuid", {}), **_id_to_uuid}
        merged_alias_to_table = {**on_disk.get("alias_to_table", {}), **_alias_to_table}
        v2v3._atomic_write(ID_TO_UUID_FILE, json.dumps(
            {"id_to_uuid": merged_id_to_uuid, "alias_to_table": merged_alias_to_table}, indent=2,
        ))


def _store_uuid_mapping(alias: str, uuid_or_id, v3_id) -> None:
    """Same storage as v2v3._store_id_mapping, just reused directly since it's
    key-agnostic (uuid strings and int ids can coexist in the same dict)."""
    v2v3._store_id_mapping(alias, uuid_or_id, v3_id)


# FK fields that MUST resolve for a record to be worth posting at all — if
# one of these is still unresolved after _remap_fks_via_uuid, the insert is
# guaranteed to fail on V3's FK constraint (confirmed: this is exactly what
# was happening for every afya_api_auth visit whose patient hadn't reached
# V3 yet). Posting anyway just burns an error_id on a doomed request and
# dead-letters a record that would succeed on its own a few minutes later
# once the parent catches up — so these are held back instead, same as the
# existing orphan-handling for vitals/doctor_notes with an unresolved visit.
_CRITICAL_FK_FIELDS: dict[str, list] = {
    "reception_visit": ["patient_id"],
    # A record whose patient/visit can't be resolved is held back, not posted
    # with the raw V2 id (which is how 12,280 doctor notes ended up pointing
    # at visits that don't exist in V3).
    "inpatient_admission":    ["patient_id", "visit_id"],
    "evaluation_doctor_note": ["visit_id"],
    "evaluation_visit_destination": ["visit_id"],
    "evaluation_sample":      ["patient_id", "visit_id"],
    # results whose investigation isn't in V3 yet wait for it instead of
    # going out with the V2 investigation id
    "evaluation_inv_result":  ["investigation_id", "visit_id"],
    "evaluation_investigation": ["visit_id"],
    "evaluation_prescription":  ["visit_id"],
    # an unresolved patient went out as the raw V2 id → FK 500 on every record
    "reception_patient_nok":      ["patient_id"],
    "reception_patient_document": ["patient_id"],
    # discharge_request_id is only set when the request exists in this
    # facility's data (see _FETCH_SQL), so a set one must resolve.
    "inpatient_discharge_request": ["admission_id", "discharge_type_id"],
    "inpatient_discharge":    ["admission_id", "discharge_type_id", "discharge_request_id"],
    "inventory_dispensing":   ["store_id", "patient_id"],
    "inpatient_vital":        ["admission_id"],
    "outpatient_vital":       ["visit_id", "patient_id"],
}

# Optional FKs: when the parent isn't in V3, send null rather than the raw
# V2 id (which would point at an unrelated V3 row, or fail the FK).
_NULL_IF_UNRESOLVED: dict[str, set[str]] = {
    "evaluation_sample":           {"investigation_id"},
    "inpatient_discharge_request": {"visit_id"},
    # stores are posted in parallel, so a parent may not be in V3 yet
    "inventory_store":             {"parent_store_id"},
}

# Rows whose parent isn't part of this facility's migrated data at all are
# skipped (and logged) rather than held back forever: e.g. 568k of 704k V2
# visit destinations hang off 2017-era visits that were never extracted.
# A parent that IS in the facility's Snowflake table but not yet in V3 is
# still held back by the critical-FK check and retried later.
# transform key -> [(V2 field on the row, parent Snowflake source table, parent column), …]
_SKIP_IF_PARENT_NOT_IN_FACILITY: dict[str, list[tuple[str, str, str]]] = {
    "evaluation_visit_destination": [("visit_id", "visits", "id")],
    "evaluation_sample":            [("visit_id", "visits", "id")],
    # a request whose visit never had an admission can't get an admission_id
    "inpatient_discharge_request":  [("visit_id", "admissions", "visit_id")],
    "inpatient_discharge":          [("admission_id", "admissions", "id")],
    # 56 dispensings have no store (their prescription is missing / store 0)
    "inventory_dispensing":         [("store_id", "inventory_stores", "id"),
                                     ("visit_id", "visits", "id")],
}

# Source tables read with their own query instead of SELECT *. V2 discharges
# hold no clinical text — it's on their discharge request — so it's joined in
# as request_*; discharge_request_id is nulled when that request isn't in
# this facility's data (it could never resolve, and would hold the discharge
# back forever).
_FETCH_SQL: dict[str, str] = {
    "discharges": """
        SELECT d.* EXCLUDE (discharge_request_id),
               IFF(r.id IS NULL, NULL, d.discharge_request_id) AS discharge_request_id,
               r.principal  AS request_principal,  r.conditions AS request_conditions,
               r.tca        AS request_tca,        r.treatment  AS request_treatment,
               r.procedures AS request_procedures
        FROM {clean}.DISCHARGES d
        LEFT JOIN {clean}.INPATIENT_DISCHARGE_REQUESTS r ON r.id = d.discharge_request_id
    """,
    # V2 next of kin store the relationship only as relationship_id → a
    # SettingsOption row (option = 'relationship': Spouse, Mother, …); their
    # own relationship text is empty. Send that option's name as relationship.
    "reception_patients_nok": """
        SELECT n.* EXCLUDE (relationship),
               COALESCE(o.item_name, NULLIF(TRIM(n.relationship::STRING), '')) AS relationship
        FROM {clean}.RECEPTION_PATIENTS_NOK n
        LEFT JOIN {clean}.SETTINGS_OPTIONS o
               ON o.id = n.relationship_id AND o.option = 'relationship'
    """,
    # V2 dispensing rows carry no store — the store is on their prescription
    "inventory_evaluation_dispensing": """
        SELECT d.*, NULLIF(p.store_id, 0) AS store_id
        FROM {clean}.INVENTORY_EVALUATION_DISPENSING d
        LEFT JOIN {clean}.EVALUATION_PRESCRIPTIONS p ON p.id = d.prescription
    """,
}

# Canary runs: post at most this many not-yet-migrated records per table and
# leave the job open (not marked done), so the next run carries on. 0 = no limit.
RECORD_LIMIT = int(os.getenv("RECORD_LIMIT", "0"))

# records sent so far in this table under a RECORD_LIMIT canary (reset per table)
_canary_sent = 0

# job_key -> records held back (parent not in V3 yet) by the last post of that job
_held_back: dict[str, int] = {}

# "visit" (singular, reception service) is ALSO a registered, insertable
# gateway alias — a separate, parallel table from this one, in reception's
# own database. v2v3._v3_alias() always prefers the singular form whenever
# both exist, with no way to know they're two different tables rather than
# a naming variant of the same one. Per explicit instruction, "visits"
# (plural, evaluation service, database "evaluationmigrate") is the one to
# use — confirmed live via that service's own Laravel error log (SQLSTATE
# in_morgue/inpatient NOT NULL violations on a visits insert, same
# service/database). Note: `describe` on this model shows
# "excluded": [..., "patient_id", ...] — the gateway silently drops
# patient_id on insert there (confirmed: a test insert didn't error, just
# never stored it) — patient linkage for visits migrated into this table is
# not established through this field; flag to the backend team if this
# needs to be resolved differently.
# eval_procedure: V2 procedures are the procedure *catalog* -> evaluation-service
# `procedures`. The bare alias `procedure` resolves to inpatient-service's
# inp_procedures (procedures done during an admission, needs admission_id).
# outpatient_vital: both V3 vitals models are App\Models\Vital, so the alias
# derived from the class is inpatient-service's `vital` (inp_vitals, needs
# admission_id → every outpatient vital 500'd). Outpatient vitals belong in
# evaluation-service's `vitals`.
_ALIAS_OVERRIDE: dict[str, str] = {"reception_visit": "visits", "settings_clinic": "facilities",
                                   "eval_procedure": "procedures", "outpatient_vital": "vitals",
                                   "inpatient_vital": "vital"}
_SERVICE_OVERRIDE: dict[str, str] = {"reception_visit": "evaluation", "settings_clinic": "core",
                                     "eval_procedure": "evaluation", "outpatient_vital": "evaluation",
                                     "inpatient_vital": "inpatient"}
# Upsert column per transform when it isn't uuid. kisumu patients have no
# uuid; patient_no ("kisumu_v3-org4-<no>-<v2 id>") is unique per patient and
# enabled as a match_on column for `patient`, so a re-post updates in place.
_MATCH_ON_OVERRIDE: dict[str, str] = {"reception_patient": "patient_no"}

# Final V3 column names, applied to the payload right before posting (AFTER
# _remap_fks_via_uuid, which works on the transform's renamed names like
# visit_id / patient_id). transform_record's global FK renames (patient ->
# patient_id, visit -> visit_id) don't match several evaluation-service
# tables, which kept V2's own column names — V3 silently drops the unknown
# field, so these links were 0% filled on kisumu_v3 (org 4). Measured live:
# visits has `patient`, prescriptions / investigations have `visit`;
# doctor_notes and evaluation `vitals` have both visit_id and visit.
# transform key -> {payload field: V3 column}; "move" renames the field,
# "copy" also keeps the original.
_V3_COLUMN_RENAMES: dict[str, dict[str, dict[str, str]]] = {
    "reception_visit":          {"move": {"patient_id": "patient"}},
    "evaluation_prescription":  {"move": {"visit_id": "visit"}},
    "evaluation_investigation": {"move": {"visit_id": "visit"}},   # patient_id is a real column there
    "evaluation_doctor_note":   {"copy": {"visit_id": "visit"}},
    "outpatient_vital":         {"copy": {"visit_id": "visit"}},
}


def _apply_v3_column_names(payload: dict, transform_key: str) -> dict:
    """The payload under V3's real column names (see _V3_COLUMN_RENAMES).
    Only the posted payload changes — progress / id-map keys and the
    critical-FK check keep using the transform's field names."""
    rules = _V3_COLUMN_RENAMES.get(transform_key)
    if not rules:
        return payload
    out = dict(payload)
    for src, dst in rules.get("move", {}).items():
        if src in out:
            out[dst] = out.pop(src)
    for src, dst in rules.get("copy", {}).items():
        if src in out:
            out[dst] = out[src]
    return out


# ─── STABLE UUIDS (no duplicate inserts) ─────────────────────────────────────
# A V2 record without a uuid has no match_on, so every re-post (lost progress,
# two runs at once) INSERTS a second V3 row — kisumu_v3 got ~16k duplicate
# investigations that way. Such records now always carry a uuid, and posts
# match on it, so a re-post updates in place:
#   1. the V3 row's own uuid when repair_v3_links.py has matched that V2 id
#      to an existing V3 row (.migration_v3_uuid.json in the state dir), else
#   2. a deterministic uuid5(facility, alias, V2 id) — the same on every run.
# Only for gateway models whose rows have a uuid column (checked once per
# alias with a 1-row read); any other model posts exactly as before.
_STABLE_UUID_NS = uuid_lib.UUID("8d3f0f5e-2a51-4c3e-9b0e-6a1f3c2d7e41")
_V3_UUID_FILE = "migration_v3_uuid.json"
_known_v3_uuids: dict[str, dict[str, str]] | None = None
_alias_has_uuid: dict[str, bool] = {}
_uuid_lock = threading.Lock()


def _v3_uuid_for(alias: str, v2_id) -> str | None:
    """The V3 uuid already matched to this V2 id (see repair_v3_links.py)."""
    global _known_v3_uuids
    with _uuid_lock:
        if _known_v3_uuids is None:
            path = v2v3.ID_MAP_FILE.with_name("." + _V3_UUID_FILE)
            _known_v3_uuids = json.loads(path.read_text()) if path.exists() else {}
        return (_known_v3_uuids.get(alias) or {}).get(str(v2_id))


def _model_has_uuid(alias: str, service: str | None) -> bool:
    with _uuid_lock:
        if alias in _alias_has_uuid:
            return _alias_has_uuid[alias]
    has = False
    try:
        svc = service or v2v3._alias_to_service.get(alias, "core")
        org = v2v3.v3_login_org_cfg().get("organization_id")
        r = v2v3._gateway_post(svc, {"action": "read", "model": alias, "source_tenant_id": org, "per_page": 1},
                               timeout=60)
        rows = r.json().get("data") or [] if r.ok else []
        has = bool(rows) and "uuid" in rows[0]
    except Exception as e:
        log.warning("  couldn't check %s for a uuid column (%s) — posting without one", alias, e)
    with _uuid_lock:
        _alias_has_uuid[alias] = has
    log.info("  %s: %s", alias, "uuid column — uuid-less records get a stable uuid" if has
             else "no uuid column seen — records post without one")
    return has


def _with_stable_uuid(payload: dict, *, facility: str, alias: str, rec_key, transform_key: str) -> dict:
    if payload.get("uuid") or rec_key is None or transform_key in _MATCH_ON_OVERRIDE:
        return payload
    if not _model_has_uuid(alias, _SERVICE_OVERRIDE.get(transform_key)):
        return payload
    value = _v3_uuid_for(alias, rec_key) or str(uuid_lib.uuid5(_STABLE_UUID_NS, f"{facility}/{alias}/{rec_key}"))
    return {**payload, "uuid": value}


# ─── ONE WRITER PER TABLE ────────────────────────────────────────────────────
# Two processes posting the same table at once both see a record as not yet
# inserted and both post it. Each table job (and repair_v3_links.py) holds an
# exclusive non-blocking lock on .run_<table>.lock in the facility's state dir.

class TableBusy(RuntimeError):
    pass


@contextmanager
def table_lock(table: str):
    if v2v3.fcntl is None:
        yield
        return
    path = v2v3.ID_MAP_FILE.with_name(f".run_{table}.lock")
    with open(path, "a+") as fh:
        try:
            v2v3.fcntl.flock(fh, v2v3.fcntl.LOCK_EX | v2v3.fcntl.LOCK_NB)
        except BlockingIOError:
            fh.seek(0)
            raise TableBusy(f"{table} is being written by another process ({fh.read().strip() or 'unknown'}) "
                            f"— wait for it to finish") from None
        fh.seek(0); fh.truncate(); fh.write(f"pid {os.getpid()} on {socket.gethostname()} since "
                                            f"{time.strftime('%Y-%m-%d %H:%M:%S')}"); fh.flush()
        try:
            yield
        finally:
            v2v3.fcntl.flock(fh, v2v3.fcntl.LOCK_UN)


# Jobs (facility|sf:table) whose records are re-posted even if already
# recorded as inserted — set by --reprocess. Only safe for tables with a
# match key (uuid or _MATCH_ON_OVERRIDE): a re-post updates in place.
REPROCESS_JOB_KEYS: set[str] = set()

# Gateway aliases that must be inserted without the V2 `id` (see post_table_to_v3).
_NO_V2_ID_ON_POST: set[str] = {"ward"}

# Some source tables carry the parent's uuid directly as a sibling column,
# which is a more direct and robust resolution path than bridging through a
# raw int id + the separately persisted _id_to_uuid table (which depends on
# that parent table having been fetched by *some* run, ever, and that run's
# write never getting clobbered by a concurrent one — confirmed fragile in
# practice). Checked empirically against real afya_api_auth data: visits'
# "reception_patient_uuid" column matches the patient's real migrated uuid
# 20/20 — the similarly-named "patient_uuid" column does NOT (0/20; it's a
# different uuid, likely the Evaluation-side patient record's own identity,
# since visits live in V2's Evaluation module) — so don't use that one.
_DIRECT_UUID_SOURCE: dict[str, dict[str, str]] = {
    "reception_visit": {"patient_id": "reception_patient_uuid"},
}


def _alias_variants(alias: str) -> list[str]:
    """The same gateway model under its singular and plural alias. FK config
    names parents one way (visit_id -> 'visit') while the parent's own
    migration may store its ids under the other ('visits', via
    _ALIAS_OVERRIDE), so lookups must try both."""
    out = [alias]
    if alias.endswith("ies"):
        out.append(alias[:-3] + "y")
    elif alias.endswith("s"):
        out.append(alias[:-1])
    elif alias.endswith("y"):
        out.append(alias[:-1] + "ies")
    else:
        out.append(alias + "s")
    return out


def _id_map_for(alias: str) -> dict:
    for a in _alias_variants(alias):
        if v2v3._id_map.get(a):
            return v2v3._id_map[a]
    return {}


def _table_for_alias(alias: str) -> str | None:
    return next((_alias_to_table[a] for a in _alias_variants(alias) if a in _alias_to_table), None)


def _remap_fks_via_uuid(record: dict, transform_key: str, v3_namespace: str) -> tuple[dict, list[str]]:
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

    Returns (record_with_resolved_fks, unresolved_critical_fields) — the
    second element lists any field named in _CRITICAL_FK_FIELDS that
    couldn't be resolved, for the caller to hold the record back on.
    """
    fk_config = {
        **({} if v3pt.is_passthrough(transform_key) else v2v3._NS_FK_REMAP.get(v3_namespace, {})),
        **v2v3._FK_REMAP.get(transform_key, {}),
    }
    critical = set(_CRITICAL_FK_FIELDS.get(transform_key, []))
    if not fk_config:
        return record, []

    direct_sources = _DIRECT_UUID_SOURCE.get(transform_key, {})
    out = dict(record)
    unresolved = []
    for field, alias in fk_config.items():
        raw_id = out.get(field)
        if raw_id is None:
            continue
        raw_id = int(raw_id) if str(raw_id).isdigit() else raw_id

        v3_id = None

        direct_uuid_field = direct_sources.get(field)
        if direct_uuid_field:
            direct_uuid = record.get(direct_uuid_field)
            if direct_uuid:
                v3_id = _id_map_for(alias).get(direct_uuid)

        if v3_id is None:
            parent_table = _table_for_alias(alias)
            if parent_table is not None:
                parent_uuid = _id_to_uuid.get(parent_table, {}).get(raw_id)
                if parent_uuid is not None:
                    v3_id = _id_map_for(alias).get(parent_uuid)

        if v3_id is None:
            v3_id = _id_map_for(alias).get(raw_id)

        if v3_id is not None:
            out[field] = v3_id
        elif field in _NULL_IF_UNRESOLVED.get(transform_key, ()):
            out[field] = None
        else:
            log.warning("  No V3 ID mapping for %s id=%s (via %s) — %s will fail FK constraint",
                        alias, raw_id, parent_table or "no Snowflake table for this facility", field)
            if field in critical:
                unresolved.append(field)
    return out, unresolved


# ─── PER-TABLE JOB ───────────────────────────────────────────────────────────

def post_table_to_v3(v3_namespace: str, org_cfg: dict, records: list[dict],
                     *, transform_key: str, alias: str, job_key: str, dry_run: bool) -> int:
    """Mirrors v2v3.post_to_v3()'s threading/resume/dead-letter behaviour,
    but keys resume-tracking and the id map by uuid (falling back to id for
    the rare table with no uuid column) instead of the raw V2 id — see the
    module docstring for why.

    Returns the number of records dead-lettered. A dead-lettered record
    doesn't raise, so this is the only signal callers have that the job
    wasn't fully clean — a table where every record 500'd would otherwise
    look identical to one that fully succeeded. Callers MUST treat a nonzero
    count the same way as any other failure: do not mark the job done.
    """
    if dry_run:
        log.info("DRY-RUN ✓ would POST %d records → %s", len(records), v3_namespace)
        if records:
            log.info("  sample: %s", json.dumps(records[0], default=str)[:400])
        return 0

    pending = [r for r in records
               if job_key in REPROCESS_JOB_KEYS
               or not v2v3._record_inserted(job_key, r.get("uuid") or r.get("id"))]
    skipped = len(records) - len(pending)
    if skipped:
        log.info("  Skipping %d already-migrated records, posting %d", skipped, len(pending))
    global _canary_sent
    if RECORD_LIMIT:
        room = max(RECORD_LIMIT - _canary_sent, 0)
        if len(pending) > room:
            log.info("  Canary — posting %d of %d pending records (limit %d per table)",
                     room, len(pending), RECORD_LIMIT)
            pending = pending[:room]
        _canary_sent += len(pending)

    done_count = 0
    dead_letter_count = 0
    orphan_count = 0
    progress_lock = threading.Lock()

    def _post_one(record: dict) -> None:
        nonlocal done_count, dead_letter_count, orphan_count
        remapped, unresolved = _remap_fks_via_uuid(record, transform_key, v3_namespace)
        rec_key = record.get("uuid") or record.get("id")
        if unresolved:
            # Parent not migrated yet — posting is guaranteed to fail on
            # V3's FK constraint, so skip it entirely rather than burn a
            # request and dead-letter a record that'll succeed on its own
            # once the parent catches up. Not marked inserted, so the next
            # run retries it automatically.
            v2v3._write_dead_letter(v3_namespace, record, {
                "reason": f"{', '.join(unresolved)} unresolved — parent not migrated to V3 yet",
            })
            with progress_lock:
                orphan_count += 1
                done_count += 1
                n = done_count
            if n % RECORD_LOG_EVERY == 0 or n == len(pending):
                log.info("  Posted %d / %d → %s", n, len(pending), v3_namespace)
            return
        # Models whose V3 primary key is shared across every org: posting the
        # V2 id collides with ids other orgs' rows already hold (a duplicate-
        # key 500). Let V3 assign the id; the V2 id is still the progress /
        # id-map key via rec_key.
        payload = ({k: val for k, val in remapped.items() if k != "id"}
                   if alias in _NO_V2_ID_ON_POST else remapped)
        payload = _apply_v3_column_names(payload, transform_key)
        payload = _with_stable_uuid(payload, facility=job_key.split("|")[0], alias=alias,
                                    rec_key=rec_key, transform_key=transform_key)
        garbled = payload.get(v2v3.GARBLED_KEY) or {}
        post_kwargs = dict(alias_override=_ALIAS_OVERRIDE.get(transform_key),
                           service_override=_SERVICE_OVERRIDE.get(transform_key),
                           match_on=_MATCH_ON_OVERRIDE.get(transform_key))
        try:
            try:
                v3_id = v2v3._post_to_v3_batch(v3_namespace, org_cfg, payload, **post_kwargs)
            except v2v3.RecordDeadLettered:
                if not garbled:
                    raise
                # V3 refused the nulled garbled field(s) — retry once encoded
                log.info("  id=%s refused with garbled %s as null — retrying with encoded values",
                         rec_key, ", ".join(sorted(garbled)))
                v3_id = v2v3._post_to_v3_batch(v3_namespace, org_cfg, {**payload, **garbled}, **post_kwargs)
        except v2v3.RecordDeadLettered:
            with progress_lock:
                dead_letter_count += 1
        else:
            # mapping first: a progress flush always writes the id map before
            # the inserted-ids, so no record is ever saved as inserted while
            # its V3 id is only in memory (see v2v3._flush_record_progress)
            if v3_id is not None and rec_key is not None:
                _store_uuid_mapping(alias, rec_key, v3_id)
            if rec_key is not None:
                v2v3._mark_record_inserted(job_key, rec_key)
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
                # 403 / exhausted 504 retries / network errors: the record was
                # NOT inserted and not dead-lettered either. Count it, or the
                # job gets marked done and these records are never retried
                # (same fix as v2v3.post_to_v3).
                rec = futures[fut]
                log.error("  Failed record uuid=%s id=%s: %s", rec.get("uuid"), rec.get("id"), e)
                with progress_lock:
                    dead_letter_count += 1

    # Final flush: _mark_record_inserted only writes every RECORD_FLUSH_EVERY
    # records, and a job that isn't marked done relies on this list to skip
    # what already landed — for uuid-less records a lost id means a duplicate
    # insert on the next run.
    with v2v3._record_progress_lock:
        v2v3._flush_record_progress()

    if dead_letter_count:
        log.warning("  %d / %d record(s) dead-lettered → %s", dead_letter_count, len(pending), v3_namespace)
    if orphan_count:
        log.warning("  %d / %d record(s) held back (parent not migrated yet) → %s",
                    orphan_count, len(pending), v3_namespace)
    _held_back[job_key] = orphan_count
    return dead_letter_count + orphan_count


def _run_vitals_split(entry: dict, facility: str, rows: list[dict], org_cfg: dict,
                      job_key: str, label: str, dry_run: bool) -> int:
    """Same inpatient/outpatient vitals split as v2v3._run_vitals_split_job,
    reusing its persisted visit->admission / visit->patient side-channel maps
    (populated only if Visits/Admissions have ever been migrated for this
    facility via v2_to_v3_api_migration.py — see the coverage caveat above).

    Returns the total count of problem records (unroutable orphans, already
    dead-lettered above, plus any dead-lettered during posting) — nonzero
    means the caller must not mark this job done."""
    admitted, outpatient, orphans = [], [], []
    for r in rows:
        # V2 vitals call the column `visit` (visit_id only after the
        # transform's global rename), and Snowflake hands ids back as
        # strings while both maps are int-keyed — normalise before routing.
        raw = r.get("visit_id") if r.get("visit_id") is not None else r.get("visit")
        try:
            visit_id = int(raw)
        except (TypeError, ValueError):
            visit_id = None
        if visit_id in v2v3._visit_admission_map:
            admitted.append(r)
        elif visit_id in v2v3._visit_patient_map:
            outpatient.append(r)
        else:
            orphans.append(r)

    # A visit in neither map was never extracted for this facility (the
    # visit→patient map covers every extracted visit), so its vitals can
    # never be routed: skip and log them, like _SKIP_IF_PARENT_NOT_IN_FACILITY,
    # instead of dead-lettering them on every run and keeping the job open.
    log.info("  %s — split %d rows: %d admitted, %d outpatient, %d skipped (visit never extracted)",
              label, len(rows), len(admitted), len(outpatient), len(orphans))
    orphans = []

    held = 0

    def _prep(partition: list[dict], tk: str) -> list[dict]:
        nonlocal held
        raw = [v2v3.transform_record(r, tk, org_cfg) for r in partition]
        out = [r for r in raw if r is not None]
        if len(raw) - len(out):
            # e.g. inpatient vitals whose nurse isn't a V3 user (recorded_by
            # is NOT NULL) — held back, not dropped: the job stays open
            held += len(raw) - len(out)
            log.warning("  %s — %d/%d %s record(s) held back by the required-field check",
                        label, len(raw) - len(out), len(raw), tk)
        return out

    admitted_t   = _prep(admitted, "inpatient_vital")
    outpatient_t = _prep(outpatient, v2v3.OUTPATIENT_VITAL_TRANSFORM)

    if not dry_run:
        v2v3._ensure_id_maps("inpatient_vital", facility)
        v2v3._ensure_id_maps(v2v3.OUTPATIENT_VITAL_TRANSFORM, facility)

    failed = 0
    for part, ns, tk, sub in ((admitted_t, r"App\Models\Vital", "inpatient_vital", "inpatient"),
                              (outpatient_t, v2v3.OUTPATIENT_VITAL_V3_NAMESPACE,
                               v2v3.OUTPATIENT_VITAL_TRANSFORM, "outpatient")):
        if not part:
            continue
        # post_table_to_v3 returns dead-lettered + held back (parent not in V3)
        problems = post_table_to_v3(ns, org_cfg, part, transform_key=tk,
                                    alias=_ALIAS_OVERRIDE.get(tk) or v2v3._v3_alias(ns),
                                    job_key=f"{job_key}::{sub}", dry_run=dry_run)
        post_held = _held_back.pop(f"{job_key}::{sub}", 0)
        failed += problems - post_held
        held += post_held
    return failed, held   # held = waiting on a parent / user that isn't in V3 yet


def _department_code(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")[:50] or "department"


def ensure_departments_from_destinations(facility: str) -> None:
    """V2 has no departments table; V3 visit destinations need a
    department_id. One V3 department per distinct V2 destination name
    (Triage, Pharmacy, ENT, Gynaecology Dr Mitei …) is created in core-service
    if it doesn't exist yet (matched by name, so re-runs create nothing)."""
    view = f"{sf_schema(facility, 'CLEAN')}.EVALUATION_VISIT_DESTINATIONS"
    with _snowflake_connect() as conn:
        names = sorted({" ".join(str(r[0]).split()) for r in conn.cursor().execute(
            f"SELECT DISTINCT destination_name FROM {view} WHERE NULLIF(TRIM(destination_name), '') IS NOT NULL"
        ).fetchall()})
    v2v3.reset_v3_department_cache()
    missing = [n for n in names if v2v3._v3_department_id_by_name(n) is None]
    log.info("Departments: %d destination names, %d already in V3, %d to create",
             len(names), len(names) - len(missing), len(missing))
    org_id = v2v3.v3_login_org_cfg()["organization_id"]
    created, failed = 0, []
    for name in missing:
        base = {"name": name, "code": _department_code(name), "status": "active",
                "description": "Migrated from V2 visit destination"}
        # V3's allowed `type` values aren't published: try clinical, then the
        # model default, then the one value seen on an existing row.
        for dept_type in ("clinical", None, "administrative"):
            data = {**base, **({"type": dept_type} if dept_type else {})}
            r = v2v3._gateway_post("core", {"action": "insert", "model": "departments",
                                            "destination_tenant_id": org_id, "data": data}, timeout=60)
            if r.ok:
                created += 1
                break
        else:
            failed.append((name, r.status_code, r.text[:160]))
    v2v3.reset_v3_department_cache()
    log.info("Departments: created %d%s", created, f", failed {failed}" if failed else "")


CHUNK_ROWS = int(os.getenv("CHUNK_ROWS", "5000"))


def iter_clean_chunks(facility: str, table: str, chunk_rows: int = CHUNK_ROWS):
    """Yield a table's CLEAN rows in pages of `chunk_rows`, so a task's memory
    stays flat however big the table is (loading evaluation_visit_destinations
    whole took ~2.7 GB and took the server down). Each page is its own query —
    keyset on ID (`ID > last ORDER BY ID LIMIT n`), never a result set held
    open for hours. Duplicate snapshots (the views DISTINCT on the whole
    payload, see dedupe_by_uuid) are removed in Snowflake: newest row per
    uuid, or per id for rows without one."""
    clean_schema = sf_schema(facility, "CLEAN")
    sql = _FETCH_SQL.get(table)
    base = sql.format(clean=clean_schema) if sql else f"SELECT * FROM {clean_schema}.{table}"
    with _snowflake_connect() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM ({base}) LIMIT 0")
        cols = {d[0].upper() for d in cur.description}
        key = ("COALESCE(TO_VARCHAR(\"UUID\"), 'id:' || TO_VARCHAR(\"ID\"))" if "UUID" in cols
               else "TO_VARCHAR(\"ID\")")
        newest = next((f'"{c}"' for c in ("UPDATED_AT", "CREATED_AT") if c in cols), '"ID"')
        deduped = (f"SELECT * FROM ({base}) QUALIFY ROW_NUMBER() OVER "
                   f"(PARTITION BY {key} ORDER BY {newest} DESC NULLS LAST) = 1")
        last_id = None
        while True:
            if last_id is None:
                cur.execute(f'SELECT * FROM ({deduped}) ORDER BY "ID" LIMIT {int(chunk_rows)}')
            else:
                cur.execute(f'SELECT * FROM ({deduped}) WHERE "ID" > %s ORDER BY "ID" LIMIT {int(chunk_rows)}',
                            (last_id,))
            columns = [d[0] for d in cur.description]
            rows = [_row_to_record(r, columns) for r in cur.fetchall()]
            if not rows:
                return
            last_id = rows[-1].get("id")
            yield rows
            if len(rows) < chunk_rows or last_id is None:
                return


def run_table_job(entry: dict, facility: str, org_cfg: dict, dry_run: bool) -> bool:
    """_run_table_job under the table's one-writer lock (see table_lock)."""
    if dry_run:
        return _run_table_job(entry, facility, org_cfg, dry_run)
    try:
        with table_lock(entry["table"]):
            return _run_table_job(entry, facility, org_cfg, dry_run)
    except TableBusy as e:
        log.error("✗ [%s] %s", facility, e)
        return False


def _run_table_job(entry: dict, facility: str, org_cfg: dict, dry_run: bool) -> bool:
    """Migrate one table: read its CLEAN view page by page (iter_clean_chunks),
    transform and post each page, and decide done / waiting / failed from the
    totals. Returns True on success (including "nothing to do")."""
    table, v3_namespace, transform_key = entry["table"], entry["v3"], entry["transform"]
    alias = _ALIAS_OVERRIDE.get(transform_key) or v2v3._v3_alias(v3_namespace)
    job_key = f"{facility}|sf:{table}"
    label = f"[{facility}] {table} → {alias}"
    log.info("▶ %s", label)
    t0 = time.perf_counter()

    if job_key in v2v3._permanently_done:
        log.info("⊘ %s — already migrated in a previous run", label)
        return True

    global _canary_sent
    _canary_sent = 0
    vitals = transform_key == "inpatient_vital"
    # once per table, not per page
    parent_sets = []
    try:
        for field, parent_table, parent_col in _SKIP_IF_PARENT_NOT_IN_FACILITY.get(transform_key, []):
            with _snowflake_connect() as conn:
                ids = {str(r[0]) for r in conn.cursor().execute(
                    f"SELECT DISTINCT {parent_col} FROM {sf_schema(facility, 'CLEAN')}.{parent_table.upper()}"
                ).fetchall()}
            parent_sets.append((field, parent_table, parent_col, ids))
    except Exception as e:
        log.error("✗ Snowflake fetch FAILED %s: %s", label, e)
        return False
    if not vitals and not dry_run:
        if transform_key == "evaluation_visit_destination":
            # Sequencing: the departments these records point at must exist in
            # V3 before they're transformed (department_id is looked up by name).
            ensure_departments_from_destinations(facility)
        if not v3pt.is_passthrough(transform_key):
            v2v3._ensure_id_maps(transform_key, facility)

    total = skipped = n_dropped = posted = failed = held = 0
    pages = 0
    try:
        for rows in iter_clean_chunks(facility, table):
            pages += 1
            total += len(rows)
            _register_table(alias, table, rows)

            if vitals:
                f, h = _run_vitals_split(entry, facility, rows, org_cfg, job_key, label, dry_run)
                failed += f
                held += h
                posted += len(rows) - f - h
            else:
                convert = (v3pt.transform if v3pt.is_passthrough(transform_key) else
                           lambda r, tk, cfg: v2v3.transform_record(r, tk, cfg, facility))
                pairs = [(r, convert(r, transform_key, org_cfg)) for r in rows]
                for field, parent_table, parent_col, ids in parent_sets:
                    # transformed value first; else the source row, under the
                    # field's own name or V2's bare FK name (visit for visit_id)
                    # — the keep-list may drop a field the target has no column for
                    bare = field[:-3] if field.endswith("_id") else field
                    before = len(pairs)
                    pairs = [(r, t) for r, t in pairs
                             if str((t or {}).get(field) or r.get(field) or r.get(bare)) in ids]
                    skipped += before - len(pairs)
                transformed = [t for _, t in pairs if t is not None]
                n_dropped += len(pairs) - len(transformed)
                if transformed:
                    problems = post_table_to_v3(v3_namespace, org_cfg, transformed,
                                                transform_key=transform_key, alias=alias,
                                                job_key=job_key, dry_run=dry_run)
                    page_held = _held_back.pop(job_key, 0)
                    held += page_held
                    failed += problems - page_held
                    posted += len(transformed) - problems
            log.info("  %s — page %d: %d rows read so far (%d posted, %d skipped, %d held, %d failed)",
                     label, pages, total, posted, skipped, held + n_dropped, failed)
            if RECORD_LIMIT and _canary_sent >= RECORD_LIMIT:
                break
    except v2v3.GatewayModelNotRegistered as e:
        log.warning("⊘ %s — model not registered in gateway: %s", label, e)
        return True
    except Exception as e:
        log.error("✗ %s FAILED on page %d: %s", label, pages + 1, e)
        return False

    elapsed = time.perf_counter() - t0
    if skipped:
        log.info("  %s — skipped %d record(s) whose parent was never extracted for this facility",
                 label, skipped)
    if total == 0:
        log.info("⊘ %s — 0 rows in Snowflake", label)
        if not dry_run:
            v2v3._mark_done(_run_id, job_key)
        return True
    waiting = held + n_dropped
    if failed:
        log.warning("◐ %s — %d of %d rows: %d migrated, %d dead-lettered%s in %.0fs "
                    "(job NOT marked done — re-run will retry)", label, total - skipped, total, posted,
                    failed, f", {waiting} held back" if waiting else "", elapsed)
        return False
    if waiting:
        _waiting[table] = f"{waiting} record(s) waiting on parents/users not in V3 yet"
        log.warning("⏸ %s — %d migrated, %d held back until their parent/user exists in V3, in %.0fs "
                    "(job NOT marked done — re-run will retry them)", label, posted, waiting, elapsed)
        return False
    if RECORD_LIMIT:
        log.info("✓ %s — canary batch posted cleanly in %.0fs (job left open; run without "
                 "a record limit to post the rest)", label, elapsed)
        return True
    log.info("✓ %s — %d records migrated in %.0fs (%d page(s))", label, posted, elapsed, pages)
    if not dry_run:
        v2v3._mark_done(_run_id, job_key)
    return True


# ─── ORCHESTRATOR ────────────────────────────────────────────────────────────

_run_id: str = ""

# table -> why it isn't done yet, for tables where nothing failed but some
# records are held back until their parent / user exists in V3. Reported
# separately from failures: re-running is all they need, once that data lands.
_waiting: dict[str, str] = {}


def _start(facility: str) -> tuple[dict, set]:
    """Per-process setup shared by every entry point: load the shared state
    files, log in to V3 as this facility's tenant, discover gateway models.
    Returns (org_cfg, insertable gateway aliases); exits on a setup error."""
    global _run_id
    _run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    _waiting.clear()
    log.info("State directory: %s", use_state_dir(facility))

    v2v3._load_id_map()
    _load_id_to_uuid()
    v2v3._load_visit_patient_map()
    v2v3._load_visit_admission_map()
    v2v3._load_record_progress()
    v2v3._load_permanently_done()
    # The id map is local state (gitignored): a server that never got this
    # file posts children with raw V2 parent ids. Show what this run has.
    log.info("ID map on this host (%s): %s", v2v3.ID_MAP_FILE,
             ", ".join(f"{a}={len(m)}" for a, m in sorted(v2v3._id_map.items())) or "EMPTY")

    # destination_tenant_id is derived from THIS account's own login response,
    # not a static per-facility table — source/destination_tenant_id must
    # equal the authenticated account's own organization or every gateway
    # call 403s ("foreign tenant"), so whatever org the AFYA_USERNAME/
    # AFYA_PASSWORD account belongs to is the only valid destination.
    # Target this facility's tenant: AFYA_<FACILITY>_USERNAME/PASSWORD if set,
    # and FACILITY_V3_CONFIG[facility] picks/validates the org + facility.
    v2v3.set_v3_target_facility(facility)
    try:
        org_cfg = v2v3.v3_login_org_cfg()
    except RuntimeError as e:
        log.error("%s", e)
        sys.exit(1)
    if org_cfg.get("organization_id") is None:
        log.error(
            "Could not derive organization_id from the V3 login response — "
            "check AFYA_USERNAME/AFYA_PASSWORD and the /v1/login response shape."
        )
        sys.exit(1)
    log.info("V3 destination — organization_id=%s facility_id=%s",
              org_cfg.get("organization_id"), org_cfg.get("facility_id"))

    log.info("Discovering gateway models …")
    return org_cfg, v2v3._fetch_available_models()


def _select_entries(facility: str, only_tables: list[str] | None, exclude_tables: list[str] | None,
                    available_models: set) -> tuple[list[dict], list[dict], list[dict]]:
    """Discover this facility's Snowflake tables and pick the ones to run.
    Returns (runnable, unmapped, not insertable in the gateway)."""
    with _snowflake_connect() as conn:
        cur = conn.cursor()
        entries = discover_tables(cur, facility)
        cur.close()

    if exclude_tables:
        log.info("Excluding %d table(s): %s", len(exclude_tables), ", ".join(sorted(exclude_tables)))
        entries = [e for e in entries if e["table"] not in exclude_tables]

    if only_tables:
        unknown = sorted(set(only_tables) - {e["table"] for e in entries})
        if unknown:
            log.warning("Not in %s RAW, ignored: %s — check the spelling, or load them with "
                        "v2_facility_to_snowflake first", facility, ", ".join(unknown))
        entries = [e for e in entries if e["table"] in only_tables]
    else:
        # The old-system-history tables are archive copies meant for
        # old_system_history, not per-table V3 inserts — several of them
        # (reception_patients, evaluation_prescriptions, ...) resolve to real
        # V3 models and would otherwise be loaded a second time. Only an
        # explicit --table sends one of them through this path.
        history = [e for e in entries if e["table"] in OLD_SYSTEM_HISTORY_TABLES]
        if history:
            log.info("Skipping %d old-system-history tables (name one with --table to force): %s",
                     len(history), ", ".join(sorted(e["table"] for e in history)))
            entries = [e for e in entries if e["table"] not in OLD_SYSTEM_HISTORY_TABLES]

    mapped   = [e for e in entries if e["v3"]]
    unmapped = [e for e in entries if not e["v3"]]

    def _insertable(e: dict) -> bool:
        alias = _ALIAS_OVERRIDE.get(e["transform"]) or v2v3._v3_alias(e["v3"])
        return not available_models or alias in available_models

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
    return runnable, unmapped, no_insert


def plan_table_jobs(facility: str, only_tables: list[str] | None = None,
                    exclude_tables: list[str] | None = None) -> dict:
    """What a run would migrate, one entry per table with its dependency
    tier — for drivers (the Airflow DAG) that run each table in its own
    process. Nothing is posted."""
    _, available_models = _start(facility)
    runnable, unmapped, no_insert = _select_entries(facility, only_tables, exclude_tables, available_models)
    return {
        "jobs": [{"facility": facility, "table": e["table"], "tier": _tier(e)}
                 for e in sorted(runnable, key=lambda e: (_tier(e), e["table"]))],
        "unmapped": sorted(e["table"] for e in unmapped),
        "not_insertable": sorted(e["table"] for e in no_insert),
    }


def run_one_table(facility: str, table: str, *, dry_run: bool) -> dict:
    """Migrate ONE table in this process (its own login, state load and
    gateway discovery). Safe to run several at once in separate processes:
    the shared state files are merged on write, never overwritten.
    Returns {"table", "status": ok | waiting | failed | error | skipped, "detail"}
    — "error" is a crash (worth a retry), "failed" means records were
    dead-lettered (a retry would only repeat them)."""
    org_cfg, available_models = _start(facility)
    runnable, unmapped, no_insert = _select_entries(facility, [table], None, available_models)
    if not runnable:
        why = ("no NAMESPACE_MAP entry" if unmapped else
               "not insertable in the gateway" if no_insert else f"not in {facility} RAW")
        return {"table": table, "status": "skipped", "detail": why}
    try:
        ok = run_table_job(runnable[0], facility, org_cfg, dry_run)
    except Exception as e:
        log.exception("Unhandled error [%s]", table)
        return {"table": table, "status": "error", "detail": str(e)[:500]}
    finally:
        if v2v3._record_progress_unflushed:
            with v2v3._record_progress_lock:
                v2v3._flush_record_progress()   # writes buffered id-map entries first
        else:
            v2v3._flush_id_map()
    if ok:
        return {"table": table, "status": "ok", "detail": ""}
    if table in _waiting:
        return {"table": table, "status": "waiting", "detail": _waiting[table]}
    return {"table": table, "status": "failed", "detail": "dead-lettered records — see the task log"}


def run_migration(facility: str, only_tables: list[str] | None,
                  *, workers: int, dry_run: bool,
                  exclude_tables: list[str] | None = None) -> list[str]:
    """All selected tables in one process, tier by tier (`workers` tables in
    parallel per tier). Returns the tables that FAILED (dead-lettered
    records, fetch/post errors); tables only waiting on parents are in
    _waiting."""
    org_cfg, available_models = _start(facility)
    runnable, unmapped, no_insert = _select_entries(facility, only_tables, exclude_tables, available_models)

    tier_groups: dict[int, list] = defaultdict(list)
    for e in runnable:
        tier_groups[_tier(e)].append(e)

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

    waiting = [t for t in failures if t in _waiting]
    failures = [t for t in failures if t not in _waiting]
    log.info(
        "\n══════════════════════  MIGRATION SUMMARY  ══════════════════════\n"
        "  Run ID    : %s\n"
        "  Migrated  : %d / %d runnable tables (%d failed, %d waiting)\n"
        "  Skipped   : %d no NAMESPACE_MAP entry | %d not insertable in gateway\n"
        "%s%s"
        "═════════════════════════════════════════════════════════════════",
        _run_id, len(runnable) - len(failures) - len(waiting), len(runnable), len(failures), len(waiting),
        len(unmapped), len(no_insert),
        f"  FAILED: {', '.join(failures)}\n" if failures else "",
        "".join(f"  WAITING: {t} — {_waiting[t]}\n" for t in waiting),
    )
    return failures


def list_tables(facility: str) -> None:
    with _snowflake_connect() as conn:
        cur = conn.cursor()
        entries = discover_tables(cur, facility)
        cur.close()
    entries.sort(key=lambda e: (e["v3"] is None, _tier(e), e["table"]))
    print(f"{'table':<38} {'tier':<5} {'v3 target':<40} transform")
    print("-" * 110)
    for e in entries:
        if e["v3"]:
            tier = _tier(e)
            print(f"{e['table']:<38} {tier:<5} {e['v3']:<40} {e['transform']}")
        else:
            print(f"{e['table']:<38} {'—':<5} {'(no NAMESPACE_MAP entry)':<40} —")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main() -> None:
    global RECORD_LIMIT
    parser = argparse.ArgumentParser(
        description="Migrate flattened Snowflake CLEAN views to the V3 Afya API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--facility", "-f", required=True,
                        help="Facility key (matches its Snowflake schema prefix, e.g. afya_api_auth)")
    parser.add_argument("--table", "-t", nargs="+", metavar="NAME",
                        help="One or more Snowflake source_table names to migrate (default: all mapped)")
    parser.add_argument("--reprocess", nargs="+", metavar="NAME", default=[],
                        help="Re-post these tables' records even if already migrated — updates in place via "
                             "their match key (e.g. --reprocess patients after V2 started decrypting)")
    parser.add_argument("--exclude", "-x", nargs="+", metavar="NAME", default=[],
                        help="Source tables to skip, e.g. ones already migrated (--exclude patients visits)")
    parser.add_argument("--workers", "-w", type=int, default=PIPELINE_WORKERS,
                        help=f"Parallel tables per tier (default: {PIPELINE_WORKERS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch + transform without posting to V3")
    parser.add_argument("--list-tables", action="store_true",
                        help="Print discovered tables, their tier and V3 mapping, and exit")
    parser.add_argument("--record-limit", type=int, default=RECORD_LIMIT, metavar="N",
                        help="Canary: post at most N new records per table and leave the job open")
    args = parser.parse_args()
    RECORD_LIMIT = args.record_limit

    if args.reprocess:
        # Re-posting is only an update when the gateway has a key to match on;
        # without one every record would be inserted a second time.
        with _snowflake_connect() as conn:
            cur = conn.cursor()
            transforms = {e["table"]: e["transform"] for e in discover_tables(cur, args.facility)}
            unsafe = []
            for t in args.reprocess:
                if t in transforms and transforms[t] in _MATCH_ON_OVERRIDE:
                    continue
                cols = {d[0].lower() for d in cur.execute(
                    f"SELECT * FROM {sf_schema(args.facility, 'CLEAN')}.{t.upper()} LIMIT 0").description}
                if "uuid" not in cols:
                    unsafe.append(t)
            cur.close()
        if unsafe:
            sys.exit(f"--reprocess refused for {unsafe}: no uuid or match_on override, so re-posting "
                     f"would insert duplicates instead of updating.")
        REPROCESS_JOB_KEYS.update(f"{args.facility}|sf:{t}" for t in args.reprocess)
        log.info("Reprocessing (re-posting as updates): %s", ", ".join(args.reprocess))

    if args.list_tables:
        list_tables(args.facility)
        return

    run_migration(args.facility, args.table, workers=args.workers, dry_run=args.dry_run,
                  exclude_tables=args.exclude)


if __name__ == "__main__":
    main()
