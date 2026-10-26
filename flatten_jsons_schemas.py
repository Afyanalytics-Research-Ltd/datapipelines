from collections import defaultdict
from datetime import datetime, timezone
import os
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
def flatten_select(raw_schema: str, table: str, expanded_fields: list[tuple[str, str, str]]) -> str:
    """The query behind {clean_schema}.{table}: RAW's JSON as typed columns."""
    # 3. Build the dynamic SELECT list
    select_parts = []
    for name, json_path, dtype in expanded_fields:
        path_parts = json_path.split('.')
        sql_path = ':'.join(f'"{p}"' for p in path_parts)  # "id" or "type":"name"

        col = f'"{name.upper()}"'  # quoted — some field names (e.g. "group") are reserved SQL keywords
        if dtype in ('ARRAY', 'OBJECT', 'VARIANT'):
            select_parts.append(f'record:{sql_path} AS {col}')
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


def flatten_table(cursor, raw_schema: str, clean_schema: str, table: str, expanded_fields,
                  materialize: str = "auto", facility: str | None = None) -> dict:
    """Flatten one RAW source table into CLEAN.

      auto   keep what's there: an existing TABLE is refreshed in place
             (CREATE OR REPLACE TABLE … AS the flatten query); otherwise a VIEW
      view   always a view — an existing table is backed up, dropped, replaced
      table  always a table, refreshed from RAW on every run

    With `facility`, records dedup merged away are left out and links point at
    the survivors (dedup.crosswalk_select).

    A table is cloned to <clean_schema>_BACKUP before it's replaced. Returns
    {"object": view|table, "was": view|table|None, "backup": name|None}."""
    if materialize not in MATERIALIZE_MODES:
        raise ValueError(f"materialize must be one of {MATERIALIZE_MODES}, not {materialize!r}")
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
    return {"object": target, "was": was, "backup": backup}


# Main function to orchestrate the flattening process for all tables in all schema pairs.
# It iterates through each raw/clean schema pair, discovers tables, fields, expands objects, builds SQL, and executes it to create the flattened views. 
# It also keeps track of successes and failures for reporting.
def flatten_all(conn):
    cursor = conn.cursor()
    total_ok, total_fail = 0, 0
    try:
        for raw_schema, clean_schema in SCHEMA_PAIRS:
            print(f"\n=== {raw_schema} → {clean_schema} ===")
            tables = get_source_tables(cursor, raw_schema)
            print(f"  Found {len(tables)} tables to process")
            facility = raw_schema.removesuffix("_RAW")
            expanded = {t: expand_objects(cursor, raw_schema, t, discover_fields(cursor, raw_schema, t))
                        for t in tables}
            dedup.run(f"HOSPITALS.{raw_schema}", facility=facility, fields=expanded)
            ok, fail = 0, 0
            for table in tables:
                try:
                    done = flatten_table(cursor, raw_schema, clean_schema, table, expanded[table],
                                         facility=facility)
                    print(f"  ✓ {table} ({len(expanded[table])} cols, {done['object']}"
                          f"{', backup ' + done['backup'] if done['backup'] else ''})")
                    ok += 1
                except Exception as e:
                    print(f"  ✗ {table}: {e}")
                    fail += 1
            print(f"  {raw_schema} done: {ok} ok, {fail} failed")
            total_ok += ok
            total_fail += fail
        print(f"\n=== TOTAL: {total_ok} ok, {total_fail} failed ===")
    finally:
        cursor.close()

# When this script is run, it will execute the main function which connects to Snowflake, 
# processes all tables, and creates the flattened views and closes the connection.
def main():
    conn = None
    try:
        conn = _snowflake_connect()
        flatten_all(conn)
    finally:
        if conn:
            conn.close()    
        
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
