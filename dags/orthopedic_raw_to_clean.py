# dags/orthopedic_raw_to_clean.py
"""
Snowflake HOSPITALS.ORTHOPEDIC_RAW (VARIANT payloads) → HOSPITALS.ORTHOPEDIC_CLEAN (typed tables)

Transform stage of the orthopedic pipeline. For every table found in
ORTHOPEDIC_RAW, samples the payload VARIANT column to discover all top-level
JSON keys and infer their Snowflake types, then generates typed DDL/DML that
explodes each key into its own column:

  * scalar keys              → plain typed column on the parent table
  * OBJECT keys               → sub-keys expanded inline as parent__subkey
  * ARRAY-of-OBJECT keys      → a separate child table (<table>__<key>),
                                 one row per array element, keyed by
                                 _run_id/_namespace/_array_index
  * ARRAY of scalars          → kept as a single VARIANT column on the parent

First run per table issues CREATE OR REPLACE TABLE ... AS SELECT (CTAS).
Subsequent runs issue INSERT ... WHERE _run_id NOT ALREADY PRESENT (anti-join
via EXCEPT), so re-running this DAG only appends RAW rows that haven't been
flattened into CLEAN yet — there is no separate watermark file/Variable to
maintain, the incremental cursor is the _run_id set difference between RAW
and CLEAN, computed fresh on every run.

This DAG depends on orthopedic_api_to_snowflake having already landed rows
into HOSPITALS.ORTHOPEDIC_RAW for the day. There is no ExternalTaskSensor /
dataset wiring between the two DAGs yet (kept out to avoid guessing at the
upstream DAG's still-in-progress task ids) — schedule this DAG's daily run
time far enough after the raw-ingest DAG's typical completion, or trigger it
manually / via a TriggerDagRunOperator once that DAG's shape is finalised.

Airflow Variables (all optional — sane defaults match the standalone
script's CLI defaults):
  ORTHOPEDIC_CLEAN_TABLES        Comma-separated RAW table names to process.
                                  Default: "" → all BASE TABLEs in
                                  HOSPITALS.ORTHOPEDIC_RAW.
  ORTHOPEDIC_CLEAN_SAMPLE_SIZE   Rows sampled per table for type inference.
                                  Default: "2000".
  ORTHOPEDIC_CLEAN_FULL_REFRESH  "true" → drop/recreate every CLEAN table
                                  (CTAS) instead of incremental INSERT.
                                  Default: "false".
  ORTHOPEDIC_CLEAN_DRY_RUN       "true" → log generated SQL without
                                  executing it against Snowflake.
                                  Default: "false".

Airflow Connections required: none — Snowflake credentials are read directly
from environment variables (see below), matching the standalone script and
dags/v3_api_to_snowflake_raw.py's SnowflakeClient.

Env vars (from .env / Docker secrets, same as the extraction pipeline):
  SNOWFLAKE_USER  SNOWFLAKE_ACCOUNT  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_PRIVATE_KEY_PATH   (key-pair auth, preferred)
  SNOWFLAKE_PASSWORD           (password auth, fallback)

Adapted from the standalone orthopedic_raw_to_clean.py script (repo root):
same schema-inference / DDL-generation logic, wrapped as Airflow tasks with
per-table dynamic task mapping in place of the script's ThreadPoolExecutor.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Mirrors dags/v3_api_to_snowflake_raw.py — harmless no-op if the path
# doesn't exist; docker-compose already injects SNOWFLAKE_* into the
# container environment via env_file, this is just a local-dev fallback.
load_dotenv(Path(__file__).parent.parent.parent.parent / ".env")

log = logging.getLogger(__name__)

DAG_ID = "orthopedic_raw_to_clean"

SF_DB           = "HOSPITALS"        # always HOSPITALS — orthopedic pipeline is DB-specific
SF_RAW_SCHEMA   = "ORTHOPEDIC_RAW"
SF_CLEAN_SCHEMA = "ORTHOPEDIC_CLEAN"

# Maps Snowflake TYPEOF() result → SQL cast expression suffix
_TYPE_CAST: dict[str, str] = {
    "TEXT":        "::VARCHAR",
    "INTEGER":     "::NUMBER",
    "DECIMAL":     "::FLOAT",
    "BOOLEAN":     "::BOOLEAN",
    "TIMESTAMP":   "::TIMESTAMP_TZ",
    "DATE":        "::DATE",
    "ARRAY":       "::VARIANT",
    "OBJECT":      "::VARIANT",
    "NULL_VALUE":  "::VARCHAR",
}
_DEFAULT_CAST = "::VARCHAR"


# ─── COLUMN NAME SANITISATION ─────────────────────────────────────────────────

def _col(key: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", key).strip("_").lower()
    if not s:
        return "col"
    if s[0].isdigit():
        s = "c_" + s
    return s


def _alias(col: str) -> str:
    """Double-quoted alias — safe against reserved words (current, start, end…)."""
    return f'"{col.replace(chr(34), chr(34) + chr(34))}"'


def _key_sql(key: str) -> str:
    """Single-quoted JSON key — escapes internal single-quotes."""
    return f"'{key.replace(chr(39), chr(39) + chr(39))}'"


# ─── SNOWFLAKE CLIENT ─────────────────────────────────────────────────────────

class SnowflakeClient:
    def __init__(self, schema_: str | None = None):
        user      = os.getenv("SNOWFLAKE_USER", "").strip()
        account   = os.getenv("SNOWFLAKE_ACCOUNT", "").strip()
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "").strip()
        key_path  = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()
        password  = os.getenv("SNOWFLAKE_PASSWORD", "").strip()

        for name, val in [
            ("SNOWFLAKE_USER", user),
            ("SNOWFLAKE_ACCOUNT", account),
            ("SNOWFLAKE_WAREHOUSE", warehouse),
        ]:
            if not val:
                raise RuntimeError(f"Missing env var {name}.")

        if not key_path and not password:
            raise RuntimeError(
                "Set SNOWFLAKE_PRIVATE_KEY_PATH (key-pair) or SNOWFLAKE_PASSWORD."
            )

        kwargs: dict = dict(
            user=user, account=account, warehouse=warehouse,
            database=SF_DB,
            schema=schema_ or SF_RAW_SCHEMA,
        )
        if key_path:
            if not Path(key_path).exists():
                raise RuntimeError(f"Private key not found: {key_path}")
            kwargs["private_key_file"] = key_path
        else:
            kwargs["password"] = password

        try:
            self._conn = snowflake.connector.connect(**kwargs)
        except Exception as e:
            raise RuntimeError(
                f"Snowflake connection failed — account={account} user={user}. Cause: {e}"
            ) from e

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    @contextmanager
    def _cursor(self):
        cur = self._conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def execute(self, sql: str, label: str | None = None) -> dict:
        label = label or f"x:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("%-40s | %s…", label, " ".join(sql.split())[:100])
        t0 = time.perf_counter()
        with self._cursor() as cur:
            cur.execute(sql)
            rowcount, sfqid = cur.rowcount, cur.sfqid
        log.info("%-40s rowcount=%s  %.2fs", label, rowcount, time.perf_counter() - t0)
        return {"rowcount": rowcount, "sfqid": sfqid}

    def query(self, sql: str, label: str | None = None) -> list[tuple]:
        label = label or f"q:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        t0 = time.perf_counter()
        with self._cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        log.debug("%-40s rows=%d  %.2fs", label, len(rows), time.perf_counter() - t0)
        return rows

    def __enter__(self): return self
    def __exit__(self, *_): self.close()


# ─── TABLE DISCOVERY ─────────────────────────────────────────────────────────

def list_raw_tables(sf: SnowflakeClient) -> list[str]:
    rows = sf.query(
        f"""
        SELECT TABLE_NAME
        FROM   {SF_DB}.INFORMATION_SCHEMA.TABLES
        WHERE  TABLE_SCHEMA = '{SF_RAW_SCHEMA}'
          AND  TABLE_TYPE   = 'BASE TABLE'
        ORDER  BY TABLE_NAME
        """,
        label="list_raw_tables",
    )
    return [r[0].lower() for r in rows]


def _table_exists(table: str, schema: str, sf: SnowflakeClient) -> bool:
    rows = sf.query(
        f"""
        SELECT COUNT(*)
        FROM   {SF_DB}.INFORMATION_SCHEMA.TABLES
        WHERE  TABLE_SCHEMA = '{schema}'
          AND  TABLE_NAME   = '{table.upper()}'
          AND  TABLE_TYPE   = 'BASE TABLE'
        """,
        label=f"exists:{table[:30]}",
    )
    return bool(rows and rows[0][0])


def _get_existing_columns(table: str, schema: str, sf: SnowflakeClient) -> set[str]:
    """Return the set of non-metadata column names already in a clean table (lowercase)."""
    rows = sf.query(
        f"""
        SELECT COLUMN_NAME
        FROM   {SF_DB}.INFORMATION_SCHEMA.COLUMNS
        WHERE  TABLE_SCHEMA = '{schema}'
          AND  TABLE_NAME   = '{table.upper()}'
          AND  COLUMN_NAME  NOT IN ('_RUN_ID','_NAMESPACE','_INGESTED_AT','_ARRAY_INDEX')
        ORDER  BY ORDINAL_POSITION
        """,
        label=f"cols:{table[:30]}",
    )
    return {r[0].lower() for r in rows}


# ─── TYPE INFERENCE ───────────────────────────────────────────────────────────

def _query_key_types(fqn: str, access_expr: str, sf: SnowflakeClient,
                     sample_size: int, label: str) -> list[tuple[str, str]]:
    """
    Enumerate sub-keys of a JSON expression and return (key_name, type_name) pairs.
    access_expr: e.g. 'payload' for top-level, or 'payload[\'address\']' for nested.
    """
    rows = sf.query(
        f"""
        WITH src AS (
            SELECT payload FROM {fqn} WHERE payload IS NOT NULL LIMIT {sample_size}
        ),
        keys_found AS (
            SELECT DISTINCT fk.value::VARCHAR AS key_name
            FROM   src,
                   LATERAL FLATTEN(input => OBJECT_KEYS({access_expr})) fk
            WHERE  {access_expr} IS NOT NULL
        ),
        type_counts AS (
            SELECT k.key_name,
                   COALESCE(TYPEOF(s.{access_expr}[k.key_name]), 'NULL_VALUE') AS type_name,
                   COUNT(*) AS cnt
            FROM   src s CROSS JOIN keys_found k
            GROUP  BY 1, 2
        )
        SELECT key_name, type_name
        FROM   type_counts
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY key_name
            ORDER BY CASE WHEN type_name != 'NULL_VALUE' THEN 1 ELSE 0 END DESC, cnt DESC
        ) = 1
        ORDER  BY key_name
        """,
        label=label,
    )
    return [(r[0], r[1]) for r in rows]


def _is_object_array(fqn: str, key: str, sf: SnowflakeClient, sample_size: int) -> bool:
    """Return True when the array column contains predominantly JSON objects."""
    rows = sf.query(
        f"""
        WITH src AS (
            SELECT payload FROM {fqn}
            WHERE  TYPEOF(payload[{_key_sql(key)}]) = 'ARRAY'
            LIMIT  {sample_size}
        ),
        elements AS (
            SELECT f.value AS item
            FROM   src, LATERAL FLATTEN(input => payload[{_key_sql(key)}]) f
            LIMIT  2000
        )
        SELECT
            SUM(CASE WHEN TYPEOF(item) = 'OBJECT' THEN 1 ELSE 0 END) AS obj_cnt,
            COUNT(*) AS total
        FROM elements
        """,
        label=f"chk_arr:{key[:30]}",
    )
    if not rows or not rows[0][1]:
        return False
    obj_cnt, total = rows[0]
    return (obj_cnt or 0) / total > 0.5


def _discover_array_element_keys(fqn: str, key: str, sf: SnowflakeClient,
                                  sample_size: int) -> list[tuple[str, str]]:
    """Return (nested_key, type_name) pairs for objects inside an array column."""
    rows = sf.query(
        f"""
        WITH src AS (
            SELECT payload FROM {fqn}
            WHERE  TYPEOF(payload[{_key_sql(key)}]) = 'ARRAY'
            LIMIT  {sample_size}
        ),
        elements AS (
            SELECT f.value AS item
            FROM   src, LATERAL FLATTEN(input => payload[{_key_sql(key)}]) f
            WHERE  TYPEOF(f.value) = 'OBJECT'
            LIMIT  2000
        ),
        keys_found AS (
            SELECT DISTINCT k.value::VARCHAR AS key_name
            FROM   elements, LATERAL FLATTEN(input => OBJECT_KEYS(item)) k
        ),
        type_counts AS (
            SELECT kf.key_name,
                   COALESCE(TYPEOF(e.item[kf.key_name]), 'NULL_VALUE') AS type_name,
                   COUNT(*) AS cnt
            FROM   elements e CROSS JOIN keys_found kf
            GROUP  BY 1, 2
        )
        SELECT key_name, type_name
        FROM   type_counts
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY key_name
            ORDER BY CASE WHEN type_name != 'NULL_VALUE' THEN 1 ELSE 0 END DESC, cnt DESC
        ) = 1
        ORDER  BY key_name
        """,
        label=f"arr_keys:{key[:30]}",
    )
    return [(r[0], r[1]) for r in rows]


def _dedup_cols(pairs: list[tuple[str, str]], seen: set[str]) -> list[tuple[str, str, str]]:
    """
    Turn (key_name, type_name) pairs into (key_name, col_name, cast) triples,
    deduplicating sanitised names against `seen` (updated in-place).
    """
    result = []
    for key_name, type_name in pairs:
        col = _col(key_name)
        base = col
        n = 1
        while col in seen:
            col = f"{base}_{n}"
            n += 1
        seen.add(col)
        cast = _TYPE_CAST.get(type_name, _DEFAULT_CAST)
        result.append((key_name, col, cast))
    return result


# ─── CTAS / INSERT BUILDERS ────────────────────────────────────────────────────

def _col_expr(access: str, key: str, cast: str, alias: str) -> str:
    """Single SELECT expression: <access>[<key>]<cast> AS <alias>"""
    return f"{access}[{_key_sql(key)}]{cast} AS {_alias(alias)}"


def _build_parent_ctas(
    table: str,
    scalar_cols: list[tuple[str, str, str]],
    object_cols: list[tuple[str, str, list[tuple[str, str, str]]]],
    array_cols: list[tuple[str, str, str]],
) -> str:
    """
    Build the parent table CTAS.

    scalar_cols : [(key, col, cast)]
    object_cols : [(key, col_prefix, [(sub_key, sub_col, sub_cast)])]
                  — each object becomes sub_col = col_prefix__sub_col inline
    array_cols  : [(key, col, cast='::VARIANT')]  — kept as VARIANT in parent
    """
    raw_fqn   = f"{SF_DB}.{SF_RAW_SCHEMA}.{table.upper()}"
    clean_fqn = f"{SF_DB}.{SF_CLEAN_SCHEMA}.{table.upper()}"

    exprs: list[str] = []

    for key, col, cast in scalar_cols:
        exprs.append(_col_expr("payload", key, cast, col))

    for parent_key, prefix, sub_cols in object_cols:
        for sub_key, sub_col, sub_cast in sub_cols:
            alias = f"{prefix}__{sub_col}"
            exprs.append(
                f"payload[{_key_sql(parent_key)}][{_key_sql(sub_key)}]{sub_cast} AS {_alias(alias)}"
            )

    for key, col, cast in array_cols:
        exprs.append(_col_expr("payload", key, "::VARIANT", col))

    col_block = ",\n    ".join(exprs)
    return (
        f"CREATE OR REPLACE TABLE {clean_fqn} AS\n"
        f"SELECT\n"
        f"    _run_id,\n"
        f"    _namespace,\n"
        f"    _ingested_at,\n"
        f"    {col_block}\n"
        f"FROM {raw_fqn};"
    )


def _build_child_ctas(
    parent_table: str,
    array_key: str,
    child_table: str,
    element_cols: list[tuple[str, str, str]],
) -> str:
    """
    Build a child table CTAS by LATERAL FLATTENing an array column.
    One row per array element; _array_index tracks position.
    """
    raw_fqn   = f"{SF_DB}.{SF_RAW_SCHEMA}.{parent_table.upper()}"
    clean_fqn = f"{SF_DB}.{SF_CLEAN_SCHEMA}.{child_table.upper()}"

    exprs = [
        _col_expr("f.value", key, cast, col)
        for key, col, cast in element_cols
    ]
    col_block = ",\n    ".join(exprs)

    return (
        f"CREATE OR REPLACE TABLE {clean_fqn} AS\n"
        f"SELECT\n"
        f"    p._run_id,\n"
        f"    p._namespace,\n"
        f"    p._ingested_at,\n"
        f"    f.index AS \"_array_index\",\n"
        f"    {col_block}\n"
        f"FROM {raw_fqn} p,\n"
        f"LATERAL FLATTEN(input => p.payload[{_key_sql(array_key)}]) f\n"
        f"WHERE TYPEOF(p.payload[{_key_sql(array_key)}]) = 'ARRAY';"
    )


def _build_parent_insert(
    table: str,
    scalar_cols: list[tuple[str, str, str]],
    object_cols: list[tuple[str, str, list]],
    array_cols: list[tuple[str, str, str]],
) -> str:
    """INSERT INTO the existing clean table, skipping run_ids already present."""
    raw_fqn   = f"{SF_DB}.{SF_RAW_SCHEMA}.{table.upper()}"
    clean_fqn = f"{SF_DB}.{SF_CLEAN_SCHEMA}.{table.upper()}"

    col_names: list[str] = ["_run_id", "_namespace", "_ingested_at"]
    select_exprs: list[str] = ["_run_id", "_namespace", "_ingested_at"]

    for key, col, cast in scalar_cols:
        col_names.append(_alias(col))
        select_exprs.append(f"payload[{_key_sql(key)}]{cast}")

    for parent_key, prefix, sub_cols in object_cols:
        for sub_key, sub_col, sub_cast in sub_cols:
            alias = f"{prefix}__{sub_col}"
            col_names.append(_alias(alias))
            select_exprs.append(
                f"payload[{_key_sql(parent_key)}][{_key_sql(sub_key)}]{sub_cast}"
            )

    for key, col, _ in array_cols:
        col_names.append(_alias(col))
        select_exprs.append(f"payload[{_key_sql(key)}]::VARIANT")

    cols_sql   = ", ".join(col_names)
    select_sql = ",\n    ".join(select_exprs)

    return (
        f"INSERT INTO {clean_fqn} ({cols_sql})\n"
        f"SELECT\n"
        f"    {select_sql}\n"
        f"FROM {raw_fqn}\n"
        f"WHERE _run_id IN (\n"
        f"    SELECT DISTINCT _run_id FROM {raw_fqn}\n"
        f"    EXCEPT\n"
        f"    SELECT DISTINCT _run_id FROM {clean_fqn}\n"
        f");"
    )


def _build_child_insert(
    parent_table: str,
    array_key: str,
    child_table: str,
    element_cols: list[tuple[str, str, str]],
) -> str:
    """INSERT INTO existing child table, skipping run_ids already present."""
    raw_fqn   = f"{SF_DB}.{SF_RAW_SCHEMA}.{parent_table.upper()}"
    clean_fqn = f"{SF_DB}.{SF_CLEAN_SCHEMA}.{child_table.upper()}"

    col_names: list[str] = ["_run_id", "_namespace", "_ingested_at", '"_array_index"']
    select_exprs: list[str] = [
        "p._run_id", "p._namespace", "p._ingested_at", "f.index"
    ]

    for key, col, cast in element_cols:
        col_names.append(_alias(col))
        select_exprs.append(f"f.value[{_key_sql(key)}]{cast}")

    cols_sql   = ", ".join(col_names)
    select_sql = ",\n    ".join(select_exprs)

    return (
        f"INSERT INTO {clean_fqn} ({cols_sql})\n"
        f"SELECT\n"
        f"    {select_sql}\n"
        f"FROM {raw_fqn} p,\n"
        f"LATERAL FLATTEN(input => p.payload[{_key_sql(array_key)}]) f\n"
        f"WHERE TYPEOF(p.payload[{_key_sql(array_key)}]) = 'ARRAY'\n"
        f"  AND p._run_id IN (\n"
        f"    SELECT DISTINCT _run_id FROM {raw_fqn}\n"
        f"    EXCEPT\n"
        f"    SELECT DISTINCT _run_id FROM {clean_fqn}\n"
        f");"
    )


# ─── PER-TABLE FLATTEN LOGIC ───────────────────────────────────────────────────

def _flatten_table(
    table: str,
    sf: SnowflakeClient,
    *,
    sample_size: int,
    dry_run: bool,
    full_refresh: bool,
) -> dict:
    t0 = time.perf_counter()
    fqn = f"{SF_DB}.{SF_RAW_SCHEMA}.{table.upper()}"
    log.info("%-35s discovering top-level keys …", table)

    top_pairs = _query_key_types(fqn, "payload", sf, sample_size, label=f"disc:{table}")
    if not top_pairs:
        log.warning("%-35s no payload keys — table empty, skipping", table)
        return {"table": table, "status": "skipped", "columns": 0}

    seen: set[str] = set()
    scalar_cols:  list[tuple[str, str, str]] = []
    object_cols:  list[tuple[str, str, list]] = []
    array_cols:   list[tuple[str, str, str]] = []
    child_tables: list[tuple[str, str, list]] = []  # (array_key, child_table, element_cols)

    for key_name, type_name in top_pairs:
        col = _col(key_name)
        base = col
        n = 1
        while col in seen:
            col = f"{base}_{n}"
            n += 1
        seen.add(col)

        if type_name == "OBJECT":
            log.info("%-35s ↳ OBJECT '%s' — discovering sub-keys …", table, key_name)
            sub_pairs = _query_key_types(
                fqn, f"payload[{_key_sql(key_name)}]", sf, sample_size,
                label=f"obj:{table}.{key_name[:20]}",
            )
            sub_cols = _dedup_cols(sub_pairs, set())
            object_cols.append((key_name, col, sub_cols))
            log.info("%-35s   └─ %d sub-keys", table, len(sub_cols))

        elif type_name == "ARRAY":
            cast = "::VARIANT"
            array_cols.append((key_name, col, cast))
            log.info("%-35s ↳ ARRAY '%s' — checking element type …", table, key_name)
            if _is_object_array(fqn, key_name, sf, sample_size):
                elem_pairs = _discover_array_element_keys(fqn, key_name, sf, sample_size)
                if elem_pairs:
                    elem_cols = _dedup_cols(elem_pairs, set())
                    child_name = f"{table}__{col}"[:255]
                    child_tables.append((key_name, child_name, elem_cols))
                    log.info("%-35s   └─ object-array → child table %s (%d cols)",
                             table, child_name, len(elem_cols))
        else:
            cast = _TYPE_CAST.get(type_name, _DEFAULT_CAST)
            scalar_cols.append((key_name, col, cast))

    total_parent_cols = (
        len(scalar_cols)
        + sum(len(s) for _, _, s in object_cols)
        + len(array_cols)
    )

    # ── Decide: first-run CTAS  vs  incremental INSERT ────────────────────────
    parent_exists = not full_refresh and _table_exists(table, SF_CLEAN_SCHEMA, sf)

    if parent_exists:
        # Filter discovered columns to only those already in the clean table
        # schema. New payload keys that appeared after the initial CTAS are
        # intentionally ignored here — set ORTHOPEDIC_CLEAN_FULL_REFRESH=true
        # to pick them up.
        existing = _get_existing_columns(table, SF_CLEAN_SCHEMA, sf)

        scalar_cols = [(k, c, t) for k, c, t in scalar_cols if c in existing]

        object_cols = [
            (pk, pfx, [(sk, sc, st) for sk, sc, st in subs if f"{pfx}__{sc}" in existing])
            for pk, pfx, subs in object_cols
        ]
        object_cols = [(pk, pfx, subs) for pk, pfx, subs in object_cols if subs]

        array_cols = [(k, c, t) for k, c, t in array_cols if c in existing]

        parent_sql = _build_parent_insert(table, scalar_cols, object_cols, array_cols)
        mode = "insert"
    else:
        parent_sql = _build_parent_ctas(table, scalar_cols, object_cols, array_cols)
        mode = "ctas"

    # Build child table statements (CTAS or INSERT per child)
    child_stmts: list[tuple[str, str]] = []
    for arr_key, child_name, elem_cols in child_tables:
        child_exists = not full_refresh and _table_exists(child_name, SF_CLEAN_SCHEMA, sf)
        if child_exists:
            existing_child = _get_existing_columns(child_name, SF_CLEAN_SCHEMA, sf)
            filtered_elem = [(k, c, t) for k, c, t in elem_cols if c in existing_child]
            child_stmts.append(
                (child_name, _build_child_insert(table, arr_key, child_name, filtered_elem))
            )
        else:
            child_stmts.append(
                (child_name, _build_child_ctas(table, arr_key, child_name, elem_cols))
            )

    if dry_run:
        log.info("DRY-RUN %-35s mode=%s parent=%d cols children=%d\n%s",
                 table, mode, total_parent_cols, len(child_stmts), parent_sql)
        for child_name, sql in child_stmts:
            log.info("DRY-RUN child=%s\n%s", child_name, sql)
        return {"table": table, "status": "dry_run", "columns": total_parent_cols}

    label_prefix = "ctas" if mode == "ctas" else "insert"
    sf.execute(parent_sql, label=f"{label_prefix}:{table}")
    for child_name, sql in child_stmts:
        child_mode = "insert" if sql.lstrip().startswith("INSERT") else "ctas"
        sf.execute(sql, label=f"{child_mode}:{child_name[:35]}")

    elapsed = time.perf_counter() - t0
    log.info("%-35s %s parent=%d cols children=%d %.1fs",
             table, mode, total_parent_cols, len(child_stmts), elapsed)
    return {"table": table, "status": "ok", "mode": mode, "columns": total_parent_cols,
            "children": [c for c, _ in child_stmts]}


# ─── DAG TASK CALLABLES ─────────────────────────────────────────────────────────

def ensure_clean_schema(**context) -> None:
    with SnowflakeClient() as sf:
        sf.execute(
            f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_CLEAN_SCHEMA};",
            label="ensure_clean_schema",
        )
    log.info("Schema %s.%s ensured", SF_DB, SF_CLEAN_SCHEMA)


def list_target_tables(**context) -> list[dict]:
    """
    Resolve the RAW tables to flatten this run and return one op_kwargs dict
    per table for downstream dynamic task mapping. Honors
    ORTHOPEDIC_CLEAN_TABLES as a comma-separated override, mirroring the
    standalone script's --tables CLI flag; defaults to every BASE TABLE
    currently in ORTHOPEDIC_RAW.
    """
    tables_override = Variable.get("ORTHOPEDIC_CLEAN_TABLES", default_var="").strip()

    with SnowflakeClient() as sf:
        if tables_override:
            targets = [t.strip().lower() for t in tables_override.split(",") if t.strip()]
        else:
            targets = list_raw_tables(sf)

    if not targets:
        log.info("No tables found in %s.%s — nothing to do.", SF_DB, SF_RAW_SCHEMA)
        return []

    log.info("Resolved %d target table(s): %s", len(targets), ", ".join(targets))
    return [{"table": t} for t in targets]


def flatten_table_task(table: str, **context) -> dict:
    """Per-table mapped task: opens its own Snowflake connection and flattens one table."""
    sample_size  = int(Variable.get("ORTHOPEDIC_CLEAN_SAMPLE_SIZE", default_var="2000"))
    full_refresh = Variable.get("ORTHOPEDIC_CLEAN_FULL_REFRESH", default_var="false").strip().lower() == "true"
    dry_run      = Variable.get("ORTHOPEDIC_CLEAN_DRY_RUN", default_var="false").strip().lower() == "true"

    with SnowflakeClient() as sf:
        try:
            result = _flatten_table(
                table, sf,
                sample_size=sample_size,
                dry_run=dry_run,
                full_refresh=full_refresh,
            )
        except Exception as e:
            log.error("%-35s FAILED: %s", table, e, exc_info=True)
            result = {"table": table, "status": "error", "error": str(e)}
    return result


def summarize_results(results: list, **context) -> None:
    results = results or []
    ok      = [r for r in results if r.get("status") == "ok"]
    skipped = [r for r in results if r.get("status") == "skipped"]
    dry_run = [r for r in results if r.get("status") == "dry_run"]
    errors  = [r for r in results if r.get("status") == "error"]

    log.info(
        "Summary — ok=%d  skipped=%d  dry_run=%d  errors=%d",
        len(ok), len(skipped), len(dry_run), len(errors),
    )
    if errors:
        for r in errors:
            log.error("FAILED: %s — %s", r.get("table"), r.get("error"))
        raise RuntimeError(f"{len(errors)} table(s) failed to flatten: "
                            f"{[r.get('table') for r in errors]}")


# ─── DAG DEFINITION ──────────────────────────────────────────────────────────
# Runs daily; depends on orthopedic_api_to_snowflake (raw ingest DAG) having
# already landed the day's rows into HOSPITALS.ORTHOPEDIC_RAW. No
# ExternalTaskSensor / dataset dependency is wired up yet — see module
# docstring — so make sure this DAG's schedule trails the raw DAG's typical
# completion time (or trigger manually) until that wiring is added.
default_args = {
    "owner":       "airflow",
    "retries":     3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    max_active_tasks=4,
    tags=["orthopedic", "v1", "transform", "clean"],
) as dag:

    t_schema = PythonOperator(
        task_id="ensure_clean_schema",
        python_callable=ensure_clean_schema,
    )
    t_list = PythonOperator(
        task_id="list_target_tables",
        python_callable=list_target_tables,
    )
    t_flatten = PythonOperator.partial(
        task_id="flatten_table",
        python_callable=flatten_table_task,
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(op_kwargs=t_list.output)
    t_summary = PythonOperator(
        task_id="summarize_results",
        python_callable=summarize_results,
        op_kwargs={"results": t_flatten.output},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_schema >> t_list >> t_flatten >> t_summary
