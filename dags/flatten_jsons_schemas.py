# dags/flatten_jsons_schemas.py
"""
Snowflake {FACILITY}_RAW.EVENTS_RAW → {FACILITY}_CLEAN.<table>

Downstream transform step that flattens the JSON VARIANT payloads landed
by the raw ingestion DAGs (e.g. facility_api_to_snowflake.py,
facility_api_snowflake_multiple_schemas.py) into typed CLEAN-schema views,
one per distinct `source_table` value found in each RAW schema's
EVENTS_RAW table.

For every (raw_schema, clean_schema) pair in SCHEMA_PAIRS:
  1. Discover every `source_table` present in {raw_schema}.EVENTS_RAW.
  2. For each table, inspect the JSON payload to discover top-level field
     names/types (LATERAL FLATTEN + TYPEOF), resolving conflicting types
     observed across records (widen numerics, fall back to VARIANT for
     mixed scalar/nested, VARCHAR for mixed scalars).
  3. Expand OBJECT-typed fields into their inner fields (up to two levels
     deep), but only when the field is populated in >=20% of records —
     sparse nested objects are kept as a single VARIANT column instead.
  4. Build and run a `CREATE OR REPLACE VIEW {clean_schema}.{table}` that
     TRY_CASTs each discovered JSON path into its inferred type.

This is a straight port of the schema-discovery / DDL-generation logic in
the standalone root script `flatten_jsons_schemas.py` — the type
resolution, fill-rate heuristic, and object-expansion rules are unchanged.

No Airflow Variables or Connections are required. Snowflake credentials
come from the same key-pair auth env vars used by the ingestion pipelines
(SnowflakeClient pattern, e.g. v3_api_to_snowflake_raw.py):

Env vars (from .env / Docker secrets):
  SNOWFLAKE_USER  SNOWFLAKE_ACCOUNT  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_DATABASE  SNOWFLAKE_PRIVATE_KEY_PATH  SNOWFLAKE_SCHEMA (optional)

Schedule: @daily, intended to run after the raw ingestion DAGs have landed
the day's data into EVENTS_RAW.
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

load_dotenv(Path(__file__).parent.parent.parent.parent / ".env")
log = logging.getLogger(__name__)

DAG_ID = "flatten_jsons_schemas"

# Same RAW → CLEAN schema pairs as the standalone script. Only the active
# (uncommented) pairs are processed; the rest are kept here, commented out,
# exactly as in the original so re-enabling a facility is a one-line change.
SCHEMA_PAIRS = [
    # ("KISUMU_RAW", "KISUMU_CLEAN"),
    # ("KAKAMEGA_RAW", "KAKAMEGA_CLEAN"),
    # ("LODWAR_RAW", "LODWAR_CLEAN"),
    # ("XANALIFE_RAW", "XANALIFE_CLEAN"),
    ("AFYA_API_AUTH_RAW", "AFYA_API_AUTH_CLEAN"),
]


# ── Snowflake client (key-pair auth) ─────────────────────────────────────
class SnowflakeClient:
    def __init__(self, schema_: str | None = None):
        self._conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER").strip(),
            account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(),
            database=os.getenv("SNOWFLAKE_DATABASE").strip(),
            schema=schema_ or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC").strip(),
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip(),
        )

    def close(self):
        if self._conn:
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
        """DDL/DML with ▶/✓ logging, rowcount, and timing."""
        label = label or f"x:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-28s | %.120s…", label, " ".join(sql.split()))
        t0 = time.perf_counter()
        with self._cursor() as cur:
            cur.execute(sql)
            result = {"rowcount": cur.rowcount, "sfqid": cur.sfqid}
        log.info("✓ %-28s | rowcount=%s · %.2fs", label, result["rowcount"], time.perf_counter() - t0)
        return result

    def fetchall(self, sql: str, params: dict | None = None) -> list:
        """Read-only query helper used by the discovery functions below."""
        with self._cursor() as cur:
            return cur.execute(sql, params).fetchall()

    def __enter__(self): return self
    def __exit__(self, *a): self.close()


# ── Schema discovery / type inference (ported from the root script) ─────
def resolve_type(types):
    """Resolve multiple observed TYPEOF() values for one field into one Snowflake type."""
    real_types = [t for t in types if t != 'NULL_VALUE']

    if not real_types:
        return 'VARCHAR'

    types_set = set(real_types)
    if len(types_set) == 1:
        return real_types[0]

    if types_set & {'OBJECT', 'ARRAY'}:
        log.warning("mixed scalar/nested %s → VARIANT", types)
        return 'VARIANT'

    if types_set <= {'INTEGER', 'DECIMAL', 'DOUBLE'}:
        if 'DOUBLE' in types_set:
            return 'DOUBLE'
        if 'DECIMAL' in types_set:
            return 'DECIMAL'
        return 'INTEGER'

    log.warning("mixed scalars %s → VARCHAR", types)
    return 'VARCHAR'


def infer_type(snowflake_type):
    """Map a Snowflake TYPEOF() result to the target cast type used in the final SELECT."""
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
    return mapping.get(snowflake_type.upper(), 'STRING')


def check_fill_rate(sf: SnowflakeClient, raw_schema: str, table: str, json_path: str) -> float:
    """Fraction of records where json_path is populated. Accepts dotted paths like 'bed_type.type'."""
    sql_path = ':'.join(f'"{p}"' for p in json_path.split('.'))
    query = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(payload:{sql_path}) AS populated
        FROM {raw_schema}.EVENTS_RAW
        WHERE source_table = %(table)s
          AND IS_OBJECT(payload)
    """
    total, populated = sf.fetchall(query, {"table": table})[0]
    if total == 0:
        return 0.0
    return populated / total


def _discover_inner_fields(sf: SnowflakeClient, raw_schema: str, table: str, json_path: str) -> list[tuple[str, str]]:
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
    rows = sf.fetchall(discover_inner_query, {"table": table})

    type_by_field = defaultdict(list)
    for name, dtype in rows:
        type_by_field[name.replace('-', '_')].append(dtype)

    return [(name, resolve_type(types)) for name, types in type_by_field.items()]


def get_source_tables(sf: SnowflakeClient, raw_schema: str) -> list[str]:
    """Every distinct source_table present in this RAW schema's EVENTS_RAW."""
    tables = sf.fetchall(f"""SELECT
                SOURCE_TABLE, COUNT(*)
            FROM {raw_schema}.EVENTS_RAW
            WHERE IS_OBJECT(PAYLOAD)
            GROUP BY 1
            ORDER BY 2 DESC""")
    return [row[0] for row in tables]


def discover_fields(sf: SnowflakeClient, raw_schema: str, table: str) -> list[tuple[str, str]]:
    """Top-level field names/types for a table, inspected from its JSON payloads."""
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

    fields = sf.fetchall(discover_query, {"table": table})
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


def expand_objects(sf: SnowflakeClient, raw_schema, table, top_level_fields) -> list[tuple[str, str, str]]:
    """Expand OBJECT fields into inner fields (up to two levels), gated on a >=20% fill rate."""
    result = []
    seen_names = set()

    for name, dtype in top_level_fields:
        if dtype != 'OBJECT':
            _add_field(result, seen_names, name, name, dtype)
            continue

        if check_fill_rate(sf, raw_schema, table, name) < 0.2:
            _add_field(result, seen_names, name, name, 'VARIANT')
            continue

        inner_fields = _discover_inner_fields(sf, raw_schema, table, name)
        for inner_name, inner_type in inner_fields:
            inner_path = f"{name}.{inner_name}"
            col_l1 = f"{name}_{inner_name}"

            if inner_type != 'OBJECT':
                _add_field(result, seen_names, col_l1, inner_path, inner_type)
                continue

            # Level 2 expansion
            inner2_fields = _discover_inner_fields(sf, raw_schema, table, inner_path)
            if not inner2_fields:
                _add_field(result, seen_names, col_l1, inner_path, 'VARIANT')
                continue

            for inner2_name, inner2_type in inner2_fields:
                col_l2 = f"{name}_{inner_name}_{inner2_name}"
                _add_field(result, seen_names, col_l2, f"{inner_path}.{inner2_name}", inner2_type)

    return result


def build_flatten_sql(raw_schema: str, clean_schema: str, table: str, expanded_fields: list[tuple[str, str, str]]) -> str:
    """Build the CREATE OR REPLACE VIEW statement for one flattened table."""
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
    CREATE OR REPLACE VIEW {clean_schema}.{table} AS
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
    FROM deduped;
    """


# ── DAG task callables ────────────────────────────────────────────────────
def ensure_clean_schemas(**context):
    """Create each target CLEAN schema if it doesn't already exist."""
    with SnowflakeClient() as sf:
        for _raw_schema, clean_schema in SCHEMA_PAIRS:
            sf.execute(f"CREATE SCHEMA IF NOT EXISTS {clean_schema}", label=f"schema:{clean_schema}")
    log.info("Ensured %d CLEAN schema(s)", len(SCHEMA_PAIRS))


def discover_flatten_jobs(**context) -> list[dict]:
    """
    Build one flatten job per (raw_schema, clean_schema, table) across all
    SCHEMA_PAIRS. Returns a list of {"job": {...}} dicts for dynamic task
    mapping, mirroring get_source_tables() + the outer loop in the root
    script's flatten_all().
    """
    jobs = []
    with SnowflakeClient() as sf:
        for raw_schema, clean_schema in SCHEMA_PAIRS:
            tables = get_source_tables(sf, raw_schema)
            log.info("%s → %s: found %d tables to process", raw_schema, clean_schema, len(tables))
            for table in tables:
                jobs.append({
                    "job": {
                        "raw_schema": raw_schema,
                        "clean_schema": clean_schema,
                        "table": table,
                    }
                })
    log.info("Total flatten jobs across all schema pairs: %d", len(jobs))
    return jobs


def flatten_table(job: dict, **context):
    """
    Discover fields, expand OBJECT columns, build the flatten SQL, and
    create/replace the CLEAN view for one RAW table.
    """
    raw_schema = job["raw_schema"]
    clean_schema = job["clean_schema"]
    table = job["table"]

    with SnowflakeClient() as sf:
        fields = discover_fields(sf, raw_schema, table)
        expanded = expand_objects(sf, raw_schema, table, fields)
        sql = build_flatten_sql(raw_schema, clean_schema, table, expanded)
        sf.execute(sql, label=f"flatten:{raw_schema}.{table}")
    log.info("Flattened %s.%s → %s.%s (%d columns)", raw_schema, table, clean_schema, table, len(expanded))


# ── DAG definition ────────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    max_active_tasks=8,
    tags=["snowflake", "transform", "clean"],
) as dag:

    t_ensure = PythonOperator(
        task_id="ensure_clean_schemas",
        python_callable=ensure_clean_schemas,
    )
    t_discover = PythonOperator(
        task_id="discover_flatten_jobs",
        python_callable=discover_flatten_jobs,
    )
    t_flatten = PythonOperator.partial(
        task_id="flatten_table",
        python_callable=flatten_table,
        trigger_rule=TriggerRule.ALL_DONE,
    ).expand(op_kwargs=t_discover.output)

    t_ensure >> t_discover >> t_flatten
