#!/usr/bin/env python3
"""
orthopedic_raw_to_clean.py — Flatten HOSPITALS.ORTHOPEDIC_RAW → HOSPITALS.ORTHOPEDIC_CLEAN.

For each table in ORTHOPEDIC_RAW, samples the payload VARIANT column to discover
all top-level keys and infer their types, then generates a
CREATE OR REPLACE TABLE ... AS SELECT that explodes every key into its own
typed column.  Metadata columns (_run_id, _namespace, _ingested_at) are carried
through unchanged.

USAGE
  python orthopedic_raw_to_clean.py                       # all RAW tables
  python orthopedic_raw_to_clean.py --tables orders,payments
  python orthopedic_raw_to_clean.py --dry-run             # print DDL, no execution
  python orthopedic_raw_to_clean.py --workers 4           # parallel table jobs
  python orthopedic_raw_to_clean.py --sample-size 5000    # rows sampled for type inference

ENV VARS  (same .env used by the extraction pipeline)
  SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE
  SNOWFLAKE_PRIVATE_KEY_PATH  (key-pair auth, preferred)
  SNOWFLAKE_PASSWORD          (password auth, fallback)
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

# ─── LOGGING ─────────────────────────────────────────────────────────────────

log = logging.getLogger("orth_raw_to_clean")
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s",
        datefmt="%H:%M:%S",
    ))
    log.addHandler(_h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ─── CONFIG ──────────────────────────────────────────────────────────────────

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
    return f'"{col.replace(chr(34), chr(34)+chr(34))}"'

def _key_sql(key: str) -> str:
    """Single-quoted JSON key — escapes internal single-quotes."""
    return f"'{key.replace(chr(39), chr(39)+chr(39))}'"

# ─── SNOWFLAKE CLIENT ─────────────────────────────────────────────────────────

class SnowflakeClient:
    def __init__(self):
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
                raise RuntimeError(f"Missing env var {name} — add it to .env.")

        if not key_path and not password:
            raise RuntimeError(
                "Set SNOWFLAKE_PRIVATE_KEY_PATH (key-pair) or SNOWFLAKE_PASSWORD."
            )

        kwargs: dict = dict(
            user=user, account=account, warehouse=warehouse,
            database=SF_DB,           # always HOSPITALS
            schema=SF_RAW_SCHEMA,
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

        self._lock = threading.Lock()

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
        log.info("▶ %-40s | %s…", label, " ".join(sql.split())[:100])
        t0 = time.perf_counter()
        with self._lock, self._cursor() as cur:
            cur.execute(sql)
            rowcount, sfqid = cur.rowcount, cur.sfqid
        log.info("✓ %-40s rowcount=%s  %.2fs", label, rowcount, time.perf_counter() - t0)
        return {"rowcount": rowcount, "sfqid": sfqid}

    def query(self, sql: str, label: str | None = None) -> list[tuple]:
        label = label or f"q:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.debug("▶ %-40s | %s…", label, " ".join(sql.split())[:100])
        t0 = time.perf_counter()
        with self._lock, self._cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        log.debug("✓ %-40s rows=%d  %.2fs", label, len(rows), time.perf_counter() - t0)
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

def _dominant_type_query(fqn: str, key_expr: str, sample_size: int) -> str:
    """
    Generic CTE pattern: given a JSON key expression (e.g. payload['k'] or
    f.value['k']), return the dominant non-null TYPEOF across a sample.
    """
    return f"""
        WITH src AS (
            SELECT payload FROM {fqn} WHERE payload IS NOT NULL LIMIT {sample_size}
        ),
        keys_found AS (
            SELECT DISTINCT fk.value::VARCHAR AS key_name
            FROM   src, LATERAL FLATTEN(input => OBJECT_KEYS({key_expr})) fk
            WHERE  TYPEOF({key_expr}) IN ('OBJECT', 'NULL_VALUE') OR {key_expr} IS NOT NULL
        ),
        type_counts AS (
            SELECT k.key_name,
                   COALESCE(TYPEOF(src.{key_expr}[k.key_name]), 'NULL_VALUE') AS type_name,
                   COUNT(*) AS cnt
            FROM   src CROSS JOIN keys_found k
            GROUP  BY 1, 2
        )
        SELECT key_name, type_name
        FROM   type_counts
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY key_name
            ORDER BY CASE WHEN type_name != 'NULL_VALUE' THEN 1 ELSE 0 END DESC, cnt DESC
        ) = 1
        ORDER  BY key_name
    """

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

# ─── CTAS BUILDERS ────────────────────────────────────────────────────────────

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

    # Scalar columns
    for key, col, cast in scalar_cols:
        exprs.append(_col_expr("payload", key, cast, col))

    # Object columns — expand sub-keys inline as parent__subkey
    for parent_key, prefix, sub_cols in object_cols:
        for sub_key, sub_col, sub_cast in sub_cols:
            alias = f"{prefix}__{sub_col}"
            exprs.append(
                f"payload[{_key_sql(parent_key)}][{_key_sql(sub_key)}]{sub_cast} AS {_alias(alias)}"
            )

    # Array columns — keep as VARIANT (child tables handle their rows)
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
    Build a child table CTAS by LATERAl FLATTENing an array column.
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

# ─── PER-TABLE WORKER ─────────────────────────────────────────────────────────

def flatten_table(
    table: str,
    sf: SnowflakeClient,
    *,
    sample_size: int,
    dry_run: bool,
    full_refresh: bool = False,
) -> dict:
    t0 = time.perf_counter()
    fqn = f"{SF_DB}.{SF_RAW_SCHEMA}.{table.upper()}"
    log.info("  %-35s  discovering top-level keys …", table)

    top_pairs = _query_key_types(fqn, "payload", sf, sample_size, label=f"disc:{table}")
    if not top_pairs:
        log.warning("  %-35s  no payload keys — table empty, skipping", table)
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
            log.info("  %-35s  ↳ OBJECT  '%s' — discovering sub-keys …", table, key_name)
            sub_pairs = _query_key_types(
                fqn, f"payload[{_key_sql(key_name)}]", sf, sample_size,
                label=f"obj:{table}.{key_name[:20]}",
            )
            sub_seen: set[str] = set()
            sub_cols = _dedup_cols(sub_pairs, sub_seen)
            object_cols.append((key_name, col, sub_cols))
            log.info("  %-35s    └─ %d sub-keys", table, len(sub_cols))

        elif type_name == "ARRAY":
            cast = "::VARIANT"
            array_cols.append((key_name, col, cast))
            log.info("  %-35s  ↳ ARRAY   '%s' — checking element type …", table, key_name)
            if _is_object_array(fqn, key_name, sf, sample_size):
                elem_pairs = _discover_array_element_keys(fqn, key_name, sf, sample_size)
                if elem_pairs:
                    elem_cols = _dedup_cols(elem_pairs, set())
                    child_name = f"{table}__{col}"[:255]
                    child_tables.append((key_name, child_name, elem_cols))
                    log.info("  %-35s    └─ object-array → child table %s (%d cols)",
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
        # Filter discovered columns to only those already in the clean table schema.
        # New payload keys that appeared after the initial CTAS are intentionally
        # ignored here — run with --full-refresh to pick them up.
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
        log.info("  DRY-RUN  %-35s  mode=%s  parent=%d cols  children=%d\n%s",
                 table, mode, total_parent_cols, len(child_stmts), parent_sql)
        for child_name, sql in child_stmts:
            log.info("  DRY-RUN  child=%s\n%s", child_name, sql)
        return {"table": table, "status": "dry_run", "columns": total_parent_cols}

    label_prefix = "ctas" if mode == "ctas" else "insert"
    sf.execute(parent_sql, label=f"{label_prefix}:{table}")
    for child_name, sql in child_stmts:
        child_mode = "insert" if "INSERT" in sql[:10] else "ctas"
        sf.execute(sql, label=f"{child_mode}:{child_name[:35]}")

    elapsed = time.perf_counter() - t0
    log.info("  %-35s  %s  parent=%d cols  children=%d  %.1fs",
             table, mode, total_parent_cols, len(child_stmts), elapsed)
    return {"table": table, "status": "ok", "mode": mode, "columns": total_parent_cols,
            "children": [c for c, _ in child_stmts]}

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Flatten ORTHOPEDIC_RAW payload → ORTHOPEDIC_CLEAN typed tables."
    )
    parser.add_argument(
        "--tables",
        help="Comma-separated table names to process (default: all tables in ORTHOPEDIC_RAW)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print generated DDL without executing against Snowflake",
    )
    parser.add_argument(
        "--workers", type=int, default=4,
        help="Number of tables to flatten in parallel (default: 4)",
    )
    parser.add_argument(
        "--sample-size", type=int, default=2000,
        help="Rows sampled per table for type inference (default: 2000)",
    )
    parser.add_argument(
        "--full-refresh", action="store_true",
        help="Drop and recreate all clean tables instead of appending new data",
    )
    args = parser.parse_args()

    with SnowflakeClient() as sf:
        # Ensure destination schema exists
        sf.execute(
            f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_CLEAN_SCHEMA};",
            label="ensure_clean_schema",
        )

        # Resolve table list
        if args.tables:
            targets = [t.strip().lower() for t in args.tables.split(",") if t.strip()]
        else:
            targets = list_raw_tables(sf)

        if not targets:
            log.info("No tables found in %s.%s — nothing to do.", SF_DB, SF_RAW_SCHEMA)
            return

        mode_label = "full-refresh" if args.full_refresh else "incremental"
        log.info(
            "Flattening %d table(s) → %s.%s  (mode=%s, workers=%d, sample=%d)",
            len(targets), SF_DB, SF_CLEAN_SCHEMA, mode_label, args.workers, args.sample_size,
        )

        results: list[dict] = []
        kw = dict(sample_size=args.sample_size, dry_run=args.dry_run, full_refresh=args.full_refresh)

        if args.workers <= 1:
            for table in targets:
                results.append(flatten_table(table, sf, **kw))
        else:
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {
                    pool.submit(flatten_table, t, sf, **kw): t
                    for t in targets
                }
                for fut in as_completed(futures):
                    table = futures[fut]
                    try:
                        results.append(fut.result())
                    except Exception as e:
                        log.error("  %-35s  FAILED: %s", table, e)
                        results.append({"table": table, "status": "error", "error": str(e)})

    ok      = [r for r in results if r["status"] == "ok"]
    skipped = [r for r in results if r["status"] == "skipped"]
    errors  = [r for r in results if r["status"] == "error"]

    log.info(
        "Summary — ok=%d  skipped=%d  errors=%d",
        len(ok), len(skipped), len(errors),
    )
    if errors:
        for r in errors:
            log.error("  FAILED: %s — %s", r["table"], r.get("error"))
        sys.exit(1)


if __name__ == "__main__":
    main()
