from collections import defaultdict
from datetime import datetime, timedelta, timezone
import argparse
import os
import re
import sys
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

import dedup

load_dotenv(Path(__file__).resolve().parent / ".env")

SCHEMA_PAIRS = [
    #("KISUMU_RAW", "KISUMU_CLEAN"),
    #("KAKAMEGA_RAW", "KAKAMEGA_CLEAN"),
    #("LODWAR_RAW", "LODWAR_CLEAN"),
    #("XANALIFE_RAW", "XANALIFE_CLEAN"),
    # ("AFYA_API_AUTH_RAW", "AFYA_API_AUTH_CLEAN"),
    # ("KISUMU_V3_RAW", "KISUMU_V3_CLEAN"),
    ("SILVERWOOD_RAW", "SILVERWOOD_CLEAN"),
 
]

# SNOWFLAKE CONNECTION
#
# Key-pair auth — the same setup the ingestion pipelines use
# (facility_to_snowflake_fast_resume.py's SnowflakeClient): no username/
# password/MFA involved, just SNOWFLAKE_PRIVATE_KEY_PATH. The previous
# version of this file imported a SnowflakeClient from a sibling
# `analytics_app` repo via a sys.path hack that didn't actually resolve
# (it pointed at /home/luther, which has no analytics_app directory) and
# layered a password/MFA flow on top that the real connection never used.

def _snowflake_connect():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER").strip(),
        account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(),
        database=os.getenv("SNOWFLAKE_DATABASE").strip(),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC").strip(),
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip(),
    )

# HELPER FUNCTIONS

# resolves when we have multiple observed types for a field, e.g. ["INTEGER", "NULL_VALUE"] or ["INTEGER", "VARCHAR"]
def resolve_type(types):
    # Remove NULL_VALUE — it's "absence of data," not a real type
    real_types = [t for t in types if t != 'NULL_VALUE']
    
    # All observations were null (shouldn't happen if SQL filter works, but defensive)
    if not real_types:
        return 'VARCHAR'
    
    # Only one real type (possibly after removing NULL_VALUE)
    types_set = set(real_types)
    if len(types_set) == 1:
        return real_types[0]
    
    # Mixed scalar + nested → preserve as VARIANT
    if types_set & {'OBJECT', 'ARRAY'}:
        print(f"    ⚠ mixed scalar/nested {types} → VARIANT")
        return 'VARIANT'
    
    # All numeric → widen to biggest
    if types_set <= {'INTEGER', 'DECIMAL', 'DOUBLE'}:
        if 'DOUBLE' in types_set:
            return 'DOUBLE'
        if 'DECIMAL' in types_set:
            return 'DECIMAL'
        return 'INTEGER'  # unreachable given types_set has >1 element, but explicit
    
    # Genuinely mixed scalars → VARCHAR
    print(f"    ⚠ mixed scalars {types} → VARCHAR")
    return 'VARCHAR'

# Map Snowflake types to our target types for casting in the final SELECT statement
def infer_type(snowflake_type):
    mapping = {
        'INTEGER': 'NUMBER',
        'DECIMAL': 'NUMBER(18,2)',
        'DOUBLE': 'FLOAT',
        'VARCHAR': 'STRING',
        'TEXT': 'STRING',
        'BOOLEAN': 'BOOLEAN',
        'DATE': 'DATE',
        'TIMESTAMP_NTZ': 'TIMESTAMP',
        'TIMESTAMP_TZ': 'TIMESTAMP',
        'TIMESTAMP_LTZ': 'TIMESTAMP',
        'ARRAY': 'ARRAY',
        'OBJECT': 'OBJECT',
    }
    return mapping.get(snowflake_type.upper(), 'STRING')  # safe fallback

# Check how often a JSON path is populated to decide whether to expand it or keep as VARIANT
def check_fill_rate(cursor, raw_schema, table, json_path):
    """Return fraction of records where json_path is populated. Accepts dotted paths like 'bed_type.type'."""
    sql_path = ':'.join(f'"{p}"' for p in json_path.split('.'))
    query = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(payload:{sql_path}) AS populated
        FROM {raw_schema}.EVENTS_RAW
        WHERE source_table = %(table)s
          AND IS_OBJECT(payload)
    """
    total, populated = cursor.execute(query, {"table": table}).fetchone()
    if total == 0:
        return 0.0
    return populated / total

# Discover inner fields for a given JSON path (used for expanding nested objects) nested columns. 
# For example, if we have a top-level field "type" that is an OBJECT, we might want to discover "type.id", "type.name", etc.
def _discover_inner_fields(cursor, raw_schema: str, table: str, json_path: str) -> list[tuple[str, str]]:
    """Discover fields at json_path. Accepts dotted paths like 'bed_type.type'."""
    sql_path = ':'.join(f'"{p}"' for p in json_path.split('.'))

    discover_inner_query = f"""
    SELECT
        obj.key AS field_name,
        TYPEOF(obj.value) AS field_type
        FROM {raw_schema}.EVENTS_RAW,
        LATERAL FLATTEN(input => payload:{sql_path}) obj
        WHERE source_table = %(table)s
        AND IS_OBJECT(payload:{sql_path})
        GROUP BY 1, 2;
    """
    rows = cursor.execute(discover_inner_query, {"table": table}).fetchall()

    type_by_field = defaultdict(list)
    for name, dtype in rows:
        type_by_field[name.replace('-', '_')].append(dtype)

    return [(name, resolve_type(types)) for name, types in type_by_field.items()]

# MAIN LOGIC

# get every source_table present in the raw events for this schema.
def get_source_tables(cursor, raw_schema: str) -> list[str]:
    tables = cursor.execute(
            f"""SELECT
                SOURCE_TABLE, COUNT(*)
            FROM {raw_schema}.EVENTS_RAW
            WHERE IS_OBJECT(PAYLOAD)
            GROUP BY 1
            ORDER BY 2 DESC""" ).fetchall()
    return [row[0] for row in tables]

# Discover top-level fields and their types for a given table by inspecting the JSON payloads.
# This is the first step in our flattening process, where we identify what fields exist and what types they have,
# so we can decide how to handle them (e.g. whether to expand nested objects or keep as VARIANT).
def discover_fields(cursor, raw_schema: str, table: str) -> list[tuple[str, str]]:
    discover_query = f"""
    SELECT
        obj.key AS field_name,
        TYPEOF(obj.value) AS field_type
        FROM {raw_schema}.EVENTS_RAW,
        LATERAL FLATTEN(input => payload) obj
        WHERE source_table = %(table)s
        AND IS_OBJECT(payload)
        GROUP BY 1, 2;
    """

    fields = cursor.execute(discover_query, {"table": table}).fetchall()
    #2. Resolve conflicts and map to Snowflake data types
    type_by_field = defaultdict(list)
    
    for name, dtype in fields:
        type_by_field[name.replace('-', '_')].append(dtype)


    return [(name, resolve_type(types)) for name, types in type_by_field.items()]


def _add_field(result, seen_names, col, json_path, dtype):
    # Appends '_' until col is unique — prevents collisions like BED_ID (top-level) vs bed.id (expanded)
    while col.upper() in seen_names:
        col = col + '_'
    seen_names.add(col.upper())
    result.append((col, json_path, dtype))

# expand OBJECT fields into their inner fields, but only if they are sufficiently populated (e.g. >20% fill rate).
def expand_objects(cursor, raw_schema, table,
                   top_level_fields) -> list[tuple[str, str, str]]:

    result = []
    seen_names = set()

    for name, dtype in top_level_fields:
        if dtype != 'OBJECT':
            _add_field(result, seen_names, name, name, dtype)
            continue

        if check_fill_rate(cursor, raw_schema, table, name) < 0.2:
            _add_field(result, seen_names, name, name, 'VARIANT')
            continue

        inner_fields = _discover_inner_fields(cursor, raw_schema, table, name)
        for inner_name, inner_type in inner_fields:
            inner_path = f"{name}.{inner_name}"
            col_l1 = f"{name}_{inner_name}"

            if inner_type != 'OBJECT':
                _add_field(result, seen_names, col_l1, inner_path, inner_type)
                continue

            # Level 2 expansion
            inner2_fields = _discover_inner_fields(cursor, raw_schema, table, inner_path)
            if not inner2_fields:
                _add_field(result, seen_names, col_l1, inner_path, 'VARIANT')
                continue

            for inner2_name, inner2_type in inner2_fields:
                col_l2 = f"{name}_{inner_name}_{inner2_name}"
                _add_field(result, seen_names, col_l2, f"{inner_path}.{inner2_name}", inner2_type)

    return result


# Build the final SQL statement to create or replace the flattened view for a given table, based on the discovered and expanded fields. 
# This function constructs a dynamic SELECT list that extracts the appropriate JSON paths and casts them to the inferred types
NESTED_TYPES = ("ARRAY", "OBJECT", "VARIANT")


def flatten_select(raw_schema: str, table: str, expanded_fields: list[tuple[str, str, str]], as_text: bool = False) -> str:
    """The query behind {clean_schema}.{table}: RAW's JSON as typed columns. as_text=True reads every scalar field as
    raw text (no TRY_CAST), which is what the V3 type check needs: it must see the value as it was sent."""
    # 3. Build the dynamic SELECT list
    select_parts = []
    for name, json_path, dtype in expanded_fields:
        path_parts = json_path.split('.')
        sql_path = ':'.join(f'"{p}"' for p in path_parts)  # "id" or "type":"name"

        col = f'"{name.upper()}"'  # quoted — some field names (e.g. "group") are reserved SQL keywords
        if dtype in NESTED_TYPES:
            select_parts.append(f'record:{sql_path} AS {col}')
        elif as_text:
            select_parts.append(f'record:{sql_path}::STRING AS {col}')
        else:
            select_parts.append(
                f'TRY_CAST(record:{sql_path}::STRING AS {infer_type(dtype)}) AS {col}'
            )
    select_list = ",\n".join(select_parts)

    return f"""
    WITH deduped AS (
        SELECT
            DISTINCT facility_id,
            payload as record
        FROM {raw_schema}.EVENTS_RAW
        WHERE source_table = '{table}'
        AND IS_OBJECT(payload)
    )
    SELECT
        facility_id as source_schema, 
        {select_list}
    FROM deduped
    """


def flatten_columns(expanded_fields) -> list[tuple[str, str | None]]:
    """(column, SQL type) per flattened field; None for ARRAY / OBJECT / VARIANT."""
    return [(name.upper(), None if dtype in ('ARRAY', 'OBJECT', 'VARIANT') else infer_type(dtype))
            for name, _, dtype in expanded_fields]


def build_flatten_sql(raw_schema: str, clean_schema: str, table: str, expanded_fields: list[tuple[str, str, str]],
                      materialize: str = "view", query: str | None = None) -> str:
    """CREATE OR REPLACE VIEW (default — always as fresh as RAW) or TABLE (a
    snapshot of the same query, refreshed each time this runs)."""
    if materialize not in ("view", "table"):
        raise ValueError(f"materialize must be view or table, not {materialize!r}")
    # 4. Construct the final CREATE OR REPLACE VIEW / TABLE statement
    return f"""
    CREATE OR REPLACE {materialize.upper()} {clean_schema}.{table} AS
    {query or flatten_select(raw_schema, table, expanded_fields)};
    """

MATERIALIZE_MODES = ("auto", "view", "table")


def existing_object_type(cursor, clean_schema: str, table: str) -> str | None:
    """'view', 'table' or None — what <clean_schema>.<table> is now."""
    db, schema = clean_schema.split(".") if "." in clean_schema else (None, clean_schema)
    info = f"{db}.INFORMATION_SCHEMA.TABLES" if db else "INFORMATION_SCHEMA.TABLES"
    cursor.execute(f"SELECT table_type FROM {info} WHERE table_schema = %s AND LOWER(table_name) = %s",
                   (schema.upper(), table.lower()))
    row = cursor.fetchone()
    if not row:
        return None
    return "view" if "VIEW" in str(row[0]).upper() else "table"


def backup_table(cursor, clean_schema: str, table: str) -> str:
    """Zero-copy clone of a CLEAN table into <clean_schema>_BACKUP before it's
    replaced — kept out of CLEAN so it never looks like source data. One per
    table per day, and never overwritten: a second run the same day keeps the
    day's first (pre-refresh) copy."""
    backup_schema = f"{clean_schema}_BACKUP"
    target = f"{backup_schema}.{table.upper()}__{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {backup_schema}")
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {target} CLONE {clean_schema}.{table}")
    return target


# BACKUP CLEAN-UP
#
# Every table replaced in CLEAN is first cloned to <clean_schema>_BACKUP.<TABLE>__YYYYMMDD. Nothing removed them, so they piled up.
# Rule: a backup older than BACKUP_KEEP_DAYS (default 7) is dropped, but the newest backup of a table is always kept.
# Only tables in the _BACKUP schema whose name ends in __YYYYMMDD are ever touched. BACKUP_KEEP_DAYS=0 switches the clean-up off.
BACKUP_NAME = re.compile(r"^(?P<base>.+)__(?P<day>\d{8})$")


def backup_keep_days() -> int:
    """How many days a backup is kept (env BACKUP_KEEP_DAYS, default 7; 0 = never drop any)."""
    try:
        return max(0, int(os.getenv("BACKUP_KEEP_DAYS", "7")))
    except ValueError:
        return 7


def prune_backups(cursor, clean_schema: str, table: str, keep_days: int | None = None, today=None) -> list[str]:
    """Drops this table's backups that are older than keep_days, never the newest one. Returns the names dropped.
    A problem here is printed and ignored: it must never make a flatten fail."""
    keep_days = backup_keep_days() if keep_days is None else keep_days
    if keep_days <= 0:
        return []
    dropped: list[str] = []
    try:
        db, schema = clean_schema.split(".") if "." in clean_schema else (None, clean_schema)
        backup_schema = f"{schema}_BACKUP"
        info = f"{db}.INFORMATION_SCHEMA.TABLES" if db else "INFORMATION_SCHEMA.TABLES"
        cursor.execute(f"SELECT table_name FROM {info} WHERE table_schema = %s AND table_type = 'BASE TABLE'",
                       (backup_schema.upper(),))
        mine = []
        for (name,) in cursor.fetchall():
            m = BACKUP_NAME.match(str(name))
            if m and m.group("base").upper() == table.upper():
                try:
                    mine.append((datetime.strptime(m.group("day"), "%Y%m%d").date(), str(name)))
                except ValueError:
                    pass                      # looks like a backup but the date is not one: leave it alone
        mine.sort()
        cutoff = (today or datetime.now(timezone.utc).date()) - timedelta(days=keep_days)
        for day, name in mine[:-1]:           # the newest is never in this list
            if day < cutoff:
                cursor.execute(f"DROP TABLE IF EXISTS {(db + '.') if db else ''}{backup_schema}.{name}")
                dropped.append(name)
        if dropped:
            print(f"    backups older than {keep_days} days dropped: {', '.join(dropped)}")
    except Exception as e:                    # noqa: BLE001 - best effort
        print(f"    backup clean-up skipped ({table}): {e}")
    return dropped


# V3 TYPE CHECK (cast_clean_to_v3_types.py)
#
# A table that is in the facility's mapping file (and known to V3) is not built as the usual TRY_CAST view. Instead:
#   1. its fields are read AS TEXT into a temporary table (nothing is silently turned into NULL),
#   2. cast_clean_to_v3_types checks that text against V3's types (links are checked against the parents in RAW),
#   3. OK     -> CLEAN.<table> becomes a TABLE (a snapshot rebuilt on every run) holding the typed, defaulted data,
#      FAILED -> nothing is created or changed: the previous object stays, and TypingErrors is raised so the table fails.
# Tables not in the mapping, and facilities with no mapping file, are built exactly as before.

def _facility_of(clean_schema: str) -> str:
    """SILVERWOOD_CLEAN (or HOSPITALS.SILVERWOOD_CLEAN) -> SILVERWOOD."""
    name = clean_schema.split(".")[-1].upper()
    return name[:-len("_CLEAN")] if name.endswith("_CLEAN") else name


def _raw_parents(cursor, raw_schema: str, clean_schema: str, cast, table: str, structure, service: str, columns, derived) -> tuple[dict, list]:
    """Copies the id (and patient id) of every parent table this table points to out of RAW into temporary tables, so
    links are checked against the data as it is in RAW right now. Returns the names for check_table_for_flatten, and the
    temporary tables made (to drop)."""
    wanted = {p for _, p in cast.PARENTS} | set(cast.link_candidates(structure, table, service, columns, derived).values())
    if not wanted:
        return {}, []
    marks = ", ".join(["%s"] * len(wanted))
    cursor.execute(f"""SELECT source_table, COUNT(payload:"id"), COUNT(payload:"patient_id"), COUNT(payload:"patient")
                       FROM {raw_schema}.EVENTS_RAW
                       WHERE source_table IN ({marks}) AND IS_OBJECT(payload)
                       GROUP BY 1""", tuple(sorted(wanted)))
    out, made = {}, []
    for source_table, n_id, n_patient_id, n_patient in cursor.fetchall():
        if not n_id:
            continue
        patient = "patient_id" if n_patient_id else "patient" if n_patient else None
        tmp = f"{clean_schema}.V3CHK_P_{str(source_table).upper()}"
        extra = f', payload:"{patient}"::STRING AS "PATIENT_ID"' if patient else ""
        cursor.execute(f"""CREATE OR REPLACE TEMPORARY TABLE {tmp} AS
                           SELECT DISTINCT payload:"id"::STRING AS "ID"{extra}
                           FROM {raw_schema}.EVENTS_RAW
                           WHERE source_table = '{source_table}' AND IS_OBJECT(payload)""")
        made.append(tmp)
        out[str(source_table).lower()] = {"table": tmp, "id": '"ID"', "value": '"PATIENT_ID"' if patient else None}
    return out, made


def _raw_users(cursor, raw_schema: str, clean_schema: str, cast, table: str, columns) -> tuple[dict | None, list]:
    """For tables with user columns (created_by, recorded_by, user...): copies the id and username of every user in RAW into a
    temporary table, so a column that holds a username ('admin') is replaced by that user's id. Returns the names for
    check_table_for_flatten (None when there is nothing to look up or RAW has no users), and the temporary table made (to drop)."""
    if table.lower() == "users" or not any(cast.is_user_column(c["name"]) for c in columns):
        return None, []
    cursor.execute(f"""SELECT COUNT(payload:"id"), COUNT(payload:"username") FROM {raw_schema}.EVENTS_RAW
                       WHERE source_table = 'users' AND IS_OBJECT(payload)""")
    n_id, n_name = cursor.fetchone()
    if not n_id or not n_name:
        return None, []
    tmp = f"{clean_schema}.V3CHK_P_USERNAMES"
    cursor.execute(f"""CREATE OR REPLACE TEMPORARY TABLE {tmp} AS
                       SELECT DISTINCT payload:"id"::STRING AS "ID", payload:"username"::STRING AS "USERNAME"
                       FROM {raw_schema}.EVENTS_RAW WHERE source_table = 'users' AND IS_OBJECT(payload)""")
    return {"table": tmp, "id": '"ID"', "username": '"USERNAME"'}, [tmp]


def _flatten_checked(cursor, raw_schema: str, clean_schema: str, table: str, expanded_fields,
                     dedup_facility: str | None = None, audit=None) -> dict | None:
    """The V3-checked build of one table (see V3 TYPE CHECK above). None = this table does not use it.
    Raises cast_clean_to_v3_types.TypingErrors when the check fails, before anything in CLEAN is touched.
    With dedup_facility, records dedup merged away are left out first and links point at the survivors, as in the
    ordinary build, so the check sees exactly the records that would be built."""
    import cast_clean_to_v3_types as cast
    facility = _facility_of(clean_schema)
    if table.lower() in cast.NOT_TYPE_CHECKED:
        print(f"    {table}: left out of the V3 type check (not migrated) - built as before")
        return None
    mapped, derived = cast.load_mapping(facility)
    service = mapped.get(table.lower())
    if not service:
        return None
    structure = cast.get_v3_structure()      # a V3_SCHEMA_PATH that does not exist raises here, naming the path
    if not structure:
        raise RuntimeError(f"{table} is in the mapping but the V3 structure file was not found. Looked in: {cast.v3_schema_tried()}. "
                           f"Set {cast.V3_SCHEMA_ENV} to its full path. Nothing was changed in {clean_schema}.")
    if structure.key(table, service) is None:
        print(f"    {table}: not found in the V3 structure - built as before (no type check)")
        return None
    columns = ([{"name": "SOURCE_SCHEMA", "type": "TEXT", "nullable": True}] +
               [{"name": n.upper(), "type": "VARIANT" if dtype in NESTED_TYPES else "TEXT", "nullable": True}
                for n, _, dtype in expanded_fields])
    if audit is None:                      # a single table run on its own (the Airflow task): its findings are stored under a run of its own
        audit = cast.Audit(cursor, facility, raw_schema, clean_schema, source="airflow")
        audit.save_run("single table", finished=True)
    tmp = f"{clean_schema}.V3CHK_{table.upper()}"
    made = [tmp]
    try:
        query = flatten_select(raw_schema, table, expanded_fields, as_text=True)
        if dedup_facility:
            query = dedup.crosswalk_select(cursor, dedup_facility, table, query,
                                           [(n.upper(), None if dtype in NESTED_TYPES else "STRING") for n, _, dtype in expanded_fields])
        cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {tmp} AS\n{query}")
        raw_parents, parent_tmps = _raw_parents(cursor, raw_schema, clean_schema, cast, table, structure, service, columns, derived)
        made += parent_tmps
        users, user_tmps = _raw_users(cursor, raw_schema, clean_schema, cast, table, columns)
        made += user_tmps
        result = cast.check_table_for_flatten(cursor, facility, table, tmp, columns, service,
                                              structure=structure, derived=derived, raw_parents=raw_parents, audit=audit, users=users)
        if not result["ok"]:
            shown = result["errors"][:10]
            for e in shown:
                print(f"    ✗ {table} record {e['record_id']}: {e['error']}")
            failure = cast.TypingErrors(
                f"{table}: {result['failed_records']} record(s) have errors; nothing was changed in {clean_schema} "
                f"(the previous {table} stays as it was). Details: {audit.schema}.ERRORS, run {audit.run_id}", {table: result["failed_records"]})
            failure.errors = result["errors"][:20]      # kept in the run state, so the reason survives the run
            raise failure
        print(f"    V3 type check OK ({result['skipped_records']} skipped, {len(result['changes'])} values changed or listed"
              f"{', user names looked up' if users else ''}; "
              f"schema file {structure.source})")
        # build next to the old object, then swap: the old one is only replaced once the new one exists
        was = existing_object_type(cursor, clean_schema, table)
        backup = backup_table(cursor, clean_schema, table) if was == "table" else None
        new = f"{table}__V3NEW"
        cursor.execute(f"CREATE OR REPLACE TABLE {clean_schema}.{new} AS\n{result['select_sql']}")
        if was == "table":
            cursor.execute(f"ALTER TABLE {clean_schema}.{table} SWAP WITH {clean_schema}.{new}")
            cursor.execute(f"DROP TABLE {clean_schema}.{new}")
        else:
            if was == "view":
                cursor.execute(f"DROP VIEW {clean_schema}.{table}")
            cursor.execute(f"ALTER TABLE {clean_schema}.{new} RENAME TO {clean_schema}.{table}")
        return {"object": "table", "was": was, "backup": backup, "typed": True,
                "skipped": result["skipped_records"], "changes": len(result["changes"])}
    finally:
        for name in made:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {name}")
            except Exception:              # noqa: BLE001 - temporary tables vanish with the session anyway
                pass


def flatten_table(cursor, raw_schema: str, clean_schema: str, table: str, expanded_fields,
                  materialize: str = "auto", facility: str | None = None, audit=None) -> dict:
    """Flatten one RAW source table into CLEAN.

      auto   keep what's there: an existing TABLE is refreshed in place
             (CREATE OR REPLACE TABLE … AS the flatten query); otherwise a VIEW
      view   always a view — an existing table is backed up, dropped, replaced
      table  always a table, refreshed from RAW on every run

    With `facility`, records dedup merged away are left out and links point at
    the survivors (dedup.crosswalk_select).

    A table is cloned to <clean_schema>_BACKUP before it's replaced (older backups are cleaned up, see BACKUP CLEAN-UP).
    A table in the facility's mapping file is built by _flatten_checked instead (always a table; `materialize` is
    ignored for it; dedup still applies first). Returns {"object": view|table, "was": view|table|None, "backup": name|None}."""
    if materialize not in MATERIALIZE_MODES:
        raise ValueError(f"materialize must be one of {MATERIALIZE_MODES}, not {materialize!r}")
    checked = _flatten_checked(cursor, raw_schema, clean_schema, table, expanded_fields, dedup_facility=facility, audit=audit)
    if checked is not None:
        prune_backups(cursor, clean_schema, table)
        return checked
    was = existing_object_type(cursor, clean_schema, table)
    target = materialize if materialize != "auto" else ("table" if was == "table" else "view")
    backup = backup_table(cursor, clean_schema, table) if was == "table" else None
    if was == "table" and target == "view":
        cursor.execute(f"DROP TABLE {clean_schema}.{table}")
    elif was == "view" and target == "table":
        cursor.execute(f"DROP VIEW {clean_schema}.{table}")
    query = flatten_select(raw_schema, table, expanded_fields)
    if facility:
        query = dedup.crosswalk_select(cursor, facility, table, query, flatten_columns(expanded_fields))
    cursor.execute(build_flatten_sql(raw_schema, clean_schema, table, expanded_fields, materialize=target,
                                     query=query))
    prune_backups(cursor, clean_schema, table)
    return {"object": target, "was": was, "backup": backup}


# RUN STATE (runs started by hand: python flatten_jsons_schemas.py)
#
# Kept in Snowflake, in the audit schema of cast_clean_to_v3_types (HOSPITALS.V3_CAST_AUDIT): one RUNS row per run and one TABLE_STATE row
# per table (ok, failed with the reason, still running when the run stopped, or pending). The errors, changes and log of the run are in
# the same schema under the same RUN_ID. `--retry-failed` reads the latest run of the RAW schema and runs only the tables that are not OK;
# each of those is checked and built again from scratch, and its findings replace its earlier ones in that run.
# A normal run (no flag) starts a new run. Every write is one transaction, so a crash never leaves a half-written state.
# The Airflow DAG keeps its own task status and does not use this (its findings are still stored, per table).
STATUS_OK = "ok"


def load_state(audit) -> dict | None:
    """The state of the latest run by hand of this RAW schema (and the audit's run id becomes that run's), or None when there is none."""
    return audit.load_latest()


def save_state(audit, state: dict, status: str = "running", finished: bool = False) -> None:
    """Writes the run row: when it started, the RAW row count, whether dedup is done."""
    audit.save_run(status, raw_rows=state.get("raw_rows"), dedup=state.get("dedup", "pending"), finished=finished)


def new_state(audit, tables: list[str], raw_rows: int | None) -> dict:
    """A fresh run: every table pending."""
    return {"version": 2, "raw_schema": audit.raw_schema, "run_id": audit.run_id, "started_at": audit.started_at.isoformat(timespec="seconds"),
            "raw_rows": raw_rows, "dedup": "pending", "tables": {t: {"status": "pending"} for t in tables}}


def mark_table(audit, state: dict, table: str, status: str, **info) -> None:
    """Records one table's status (and the reason, for a failure) in memory and in Snowflake."""
    state["tables"][table] = {"status": status, "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), **info}
    audit.save_table(table, status, object_type=info.get("object"), error=info.get("error"))


def pending_tables(state: dict, tables: list[str]) -> list[str]:
    """The tables to run again: everything not recorded as OK
    (failed, still running when the run stopped, pending, or new in RAW)."""
    return [t for t in tables if state["tables"].get(t, {}).get("status") != STATUS_OK]


def raw_row_count(cursor, raw_schema: str) -> int | None:
    """Rows in the RAW events table (a cheap marker for '
    has RAW changed since the saved run?')."""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {raw_schema}.EVENTS_RAW")
        return int(cursor.fetchone()[0])
    except Exception:                      # noqa: BLE001 - only a warning marker
        return None


def cleanup_leftovers(cursor, clean_schema: str) -> list[str]:
    """Drops half-built `<table>__V3NEW` tables a crashed run left in CLEAN 
    (they are never the real table, and the migration must not see them)."""
    db, schema = clean_schema.split(".") if "." in clean_schema else (None, clean_schema)
    info = f"{db}.INFORMATION_SCHEMA.TABLES" if db else "INFORMATION_SCHEMA.TABLES"
    dropped = []
    try:
        cursor.execute(f"SELECT table_name FROM {info} WHERE table_schema = %s AND table_type = 'BASE TABLE' AND table_name LIKE '%%V3NEW'",
                       (schema.upper(),))
        for (name,) in cursor.fetchall():
            if str(name).upper().endswith("__V3NEW"):
                cursor.execute(f"DROP TABLE IF EXISTS {(db + '.') if db else ''}{schema}.{name}")
                dropped.append(str(name))
        if dropped:
            print(f"  half-built tables from an earlier run dropped: {', '.join(dropped)}")
    except Exception as e:                 # noqa: BLE001 - best effort
        print(f"  leftover clean-up skipped: {e}")
    return dropped


# Main function to orchestrate the flattening process for all tables in all schema pairs.
# It iterates through each raw/clean schema pair, discovers tables, fields, expands objects, builds SQL, and executes it to create the flattened views. 
# It also keeps track of successes and failures for reporting, and saves them in the run state (see RUN STATE).
def flatten_all(conn, retry_failed: bool = False) -> dict:
    """Flattens every table of every schema pair. retry_failed=True runs only the tables the last run did not finish OK.
    The run state, what the V3 check found and what the run printed are stored in the audit schema (see RUN STATE).
    Returns {raw_schema: {"ok": n, "failed": [tables]}}."""
    import cast_clean_to_v3_types as cast
    cursor = conn.cursor()
    total_ok, total_fail = 0, 0
    summary: dict = {}
    try:
        for raw_schema, clean_schema in SCHEMA_PAIRS:
            facility = raw_schema.removesuffix("_RAW")
            audit = cast.Audit(cursor, facility, raw_schema, clean_schema, source="manual")
            audit.ensure()                 # fails here, before anything is touched, if the audit schema cannot be written
            ok, failed, state = 0, [], None
            with audit.capture_log():
                print(f"\n=== {raw_schema} → {clean_schema} ===")
                tables = get_source_tables(cursor, raw_schema)
                print(f"  Found {len(tables)} tables to process")
                rows_now = raw_row_count(cursor, raw_schema)
                state = load_state(audit) if retry_failed else None
                if retry_failed and state is None:
                    print("  No earlier run recorded for this schema - running every table")
                if state is not None:
                    todo = pending_tables(state, tables)
                    print(f"  Retrying {len(todo)} of {len(tables)} tables (run {state['run_id']}); "
                          f"{len(tables) - len(todo)} finished OK and are skipped")
                    if state.get("raw_rows") is not None and rows_now is not None and state["raw_rows"] != rows_now:
                        print(f"  WARNING: RAW has changed since that run ({state['raw_rows']} -> {rows_now} rows). "
                              f"Tables that finished OK may be out of date; run without --retry-failed to redo everything.")
                    state["tables"] = {t: state["tables"].get(t, {"status": "pending"}) for t in tables}
                else:
                    state = new_state(audit, tables, rows_now)
                    todo = list(tables)
                save_state(audit, state)
                audit.save_pending([t for t in tables if state["tables"][t]["status"] == "pending"])
                cleanup_leftovers(cursor, clean_schema)
                expanded = {t: expand_objects(cursor, raw_schema, t, discover_fields(cursor, raw_schema, t))
                            for t in tables}
                if state.get("dedup") != STATUS_OK:
                    try:
                        dedup.run(f"HOSPITALS.{raw_schema}", facility=facility, fields=expanded)
                    except BaseException:
                        state["dedup"] = "failed"
                        save_state(audit, state, status="dedup failed", finished=True)
                        raise
                    state["dedup"] = STATUS_OK
                    save_state(audit, state)
                for table in todo:
                    audit.flush_log()
                    audit.current_table = table
                    mark_table(audit, state, table, "running")
                    try:
                        done = flatten_table(cursor, raw_schema, clean_schema, table, expanded[table],
                                             facility=facility, audit=audit)
                        print(f"  ✓ {table} ({len(expanded[table])} cols, {done['object']}"
                              f"{', backup ' + done['backup'] if done['backup'] else ''})")
                        mark_table(audit, state, table, STATUS_OK, object=done["object"])
                        ok += 1
                    except Exception as e:
                        print(f"  ✗ {table}: {e}")
                        mark_table(audit, state, table, "failed", error=str(e)[:2000])
                        failed.append(table)
                audit.current_table = None
                print(f"  {raw_schema} done: {ok} ok, {len(failed)} failed")
                if failed:
                    print(f"  To run only the failed tables again after fixing them: python flatten_jsons_schemas.py --retry-failed")
                    print(f"  Why they failed: SELECT * FROM {audit.schema}.ERRORS WHERE RUN_ID = '{audit.run_id}' AND FACILITY = '{audit.facility}'")
            save_state(audit, state, status=f"failed: {len(failed)} table(s)" if failed else STATUS_OK, finished=True)
            audit.prune()
            total_ok += ok
            total_fail += len(failed)
            summary[raw_schema] = {"ok": ok, "failed": failed}
        print(f"\n=== TOTAL: {total_ok} ok, {total_fail} failed ===")
    finally:
        cursor.close()
    return summary

# When this script is run, it will execute the main function which connects to Snowflake, 
# processes all tables, and creates the flattened views and closes the connection.
def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Flatten RAW.EVENTS_RAW into CLEAN for every schema pair.")
    ap.add_argument("--retry-failed", action="store_true",
                    help="run only the tables the last run did not finish OK (each is checked and built again from scratch)")
    args = ap.parse_args(argv)
    conn = None
    try:
        conn = _snowflake_connect()
        summary = flatten_all(conn, retry_failed=args.retry_failed)
    finally:
        if conn:
            conn.close()
    if any(s["failed"] for s in summary.values()):
        sys.exit(1)

# TEST FUNCTION 
def test_expand_objects():
    conn = _snowflake_connect()
    try:
        cursor = conn.cursor()
        
        test_top_level = [
            ("id", "INTEGER"),
            ("name", "VARCHAR"),
            ("type", "OBJECT"),              # should expand into type_id, type_name, etc.
            ("consumable_type_id", "INTEGER"),
        ]
        
        result = expand_objects(
            cursor=cursor,
            raw_schema="KISUMU_RAW",
            table="evaluation_consumables",
            top_level_fields=test_top_level
        )
        
        print(f"\nGot {len(result)} expanded fields:\n")
        print(f"{'column_name':<30} {'json_path':<30} {'dtype'}")
        print("-" * 75)
        for col_name, json_path, dtype in result:
            print(f"{col_name:<30} {json_path:<30} {dtype}")
        
        cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
    #test_expand_objects()


# test_fields = [
#         ("id", "INTEGER"),
#         ("amount", "DECIMAL"),
#         ("invoice_no", "VARCHAR"),
#         ("admission_request", "OBJECT"),  # tests the VARIANT branch
#     ]

#     sql = build_flatten_sql("KISUMU_RAW", "KISUMU_CLEAN", "finance_invoices", test_fields)
#     print(sql)
