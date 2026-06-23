# dags/v3_raw_to_v3_ready.py
"""
Snowflake HOSPITALS.{FACILITY}_V3_RAW.EVENTS_RAW → HOSPITALS.V3_READY

Second stage of the V3 model-gateway pipeline.  Reads the JSON-array
payloads written by v3_api_to_snowflake_raw, double-flattens them into
individual records, discovers column names via OBJECT_KEYS, cross-references
V2_RAW for type hints, then MERGEs deduplicated rows (keyed on tenant_id +
record id) into typed V3_READY tables.

V3_READY is already consumed by snowflake_to_v3_mysql, so this DAG
completes the full API → Snowflake → MySQL path for V3 data.

Airflow Variables (optional):
  V3_TENANT_IDS   JSON dict overriding per-facility tenant_id values
                  Example: {"kisumu": "KSH-001", "lodwar": "LCRH-001"}
                  Falls back to the tenant_id baked into FACILITIES below.

Run manually or trigger after v3_api_to_snowflake_raw completes.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

SF_DB      = "HOSPITALS"
DST_SCHEMA = "V3_READY"

FACILITIES: dict[str, dict] = {
    "afya_api_auth": {"tenant_id": "afya"},
    "kakamega":      {"tenant_id": "kakamega"},
    "kisumu":        {"tenant_id": "kisumu"},
    "lodwar":        {"tenant_id": "lodwar"},
    "tenri":         {"tenant_id": "tenri"},
    "xanalife":      {"tenant_id": "xanalife"},
}

# Internal bookkeeping columns that should never appear in V3_READY
_SKIP_COLS = frozenset({"_ingested_at", "_migrated_at"})

# Heuristic type inference — used when V2_RAW has no type information
_TS_SUFFIXES  = ("_at", "_date", "_time", "_on", "_timestamp", "_datetime")
_NUM_SUFFIXES = ("_count", "_total", "_amount", "_price", "_qty", "_quantity")


def _sf_type(col: str, v2_type: str | None = None) -> str:
    """Resolve Snowflake cast type for a JSON path extraction."""
    if v2_type:
        t = v2_type.upper()
        if "TIMESTAMP" in t:                 return "TIMESTAMP_NTZ"
        if "DATE" in t:                      return "DATE"
        if "BOOLEAN" in t:                   return "BOOLEAN"
        if "FLOAT" in t:                     return "FLOAT"
        if "NUMBER" in t or "BIGINT" in t or "INT" in t: return "NUMBER"
        return "VARCHAR"
    c = col.lower()
    if any(c.endswith(s) for s in _TS_SUFFIXES):  return "TIMESTAMP_NTZ"
    if any(c.endswith(s) for s in _NUM_SUFFIXES): return "NUMBER"
    return "VARCHAR"


default_args = {
    "owner":        "airflow",
    "start_date":   datetime(2025, 1, 1),
    "retries":      2,
    "retry_delay":  timedelta(minutes=3),
}

with DAG(
    dag_id="v3_raw_to_v3_ready",
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_tasks=4,
    tags=["v3", "snowflake", "transform"],
) as dag:

    @task
    def ensure_v3_ready_schema() -> None:
        sf = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = sf.get_conn()
        conn.autocommit(True)
        conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{DST_SCHEMA}")
        conn.close()
        log.info("Schema %s.%s ensured", SF_DB, DST_SCHEMA)

    @task
    def list_raw_table_specs() -> list:
        """
        Return one spec dict per (facility, source_table) found in any V3_RAW
        EVENTS_RAW table.  Skips facilities whose schema does not exist yet.
        """
        tenant_override: dict = json.loads(Variable.get("V3_TENANT_IDS", default_var="{}"))
        sf = SnowflakeHook(snowflake_conn_id="snowflake_default")

        specs = []
        for facility, cfg in FACILITIES.items():
            raw_schema = f"{facility.upper()}_V3_RAW"
            try:
                rows = sf.get_records(f"""
                    SELECT DISTINCT source_table
                    FROM   "{SF_DB}"."{raw_schema}"."EVENTS_RAW"
                    WHERE  source_table IS NOT NULL
                      AND  source_table != ''
                    ORDER  BY source_table
                """)
            except Exception as e:
                log.warning("Skipping %s – cannot query EVENTS_RAW: %s", facility, e)
                continue

            tenant_id = tenant_override.get(facility, cfg["tenant_id"])
            for (source_table,) in rows:
                specs.append({
                    "facility":     facility,
                    "raw_schema":   raw_schema,
                    "source_table": source_table,
                    "tenant_id":    tenant_id,
                })

        log.info("Found %d (facility, table) pairs across all V3_RAW schemas", len(specs))
        return specs

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def flatten_to_v3_ready(specs: list) -> None:
        """
        For each (facility, source_table):

        1. Discover column names by sampling OBJECT_KEYS from individual
           records inside the VARIANT array payload.
        2. Cross-reference V2_RAW INFORMATION_SCHEMA for better type hints.
        3. Ensure the V3_READY target table exists with those column types.
        4. MERGE deduplicated rows (keyed on tenant_id + id when present)
           from V3_RAW into V3_READY.

        Records inside each payload are accessed with a double LATERAL FLATTEN:
          LATERAL FLATTEN(payload)        → individual record objects
          LATERAL FLATTEN(OBJECT_KEYS(…)) → key names for column discovery
        """
        sf   = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = sf.get_conn()
        conn.autocommit(True)
        cur  = conn.cursor()

        # ── Pre-load V2_RAW column types as type hints ──────────────────
        v2_types: dict[str, dict[str, str]] = {}
        try:
            v2_rows = sf.get_records(f"""
                SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
                FROM   "{SF_DB}".INFORMATION_SCHEMA.COLUMNS
                WHERE  TABLE_SCHEMA = 'V2_RAW'
                ORDER  BY TABLE_NAME, ORDINAL_POSITION
            """)
            for tbl, col, dtype in v2_rows:
                v2_types.setdefault(tbl.lower(), {})[col.lower()] = dtype
            log.info("Loaded V2_RAW type hints for %d tables", len(v2_types))
        except Exception as e:
            log.warning("V2_RAW type hints unavailable – using heuristic inference: %s", e)

        failed = []

        for spec in specs:
            facility     = spec["facility"]
            raw_schema   = spec["raw_schema"]
            source_table = spec["source_table"]
            tenant_id    = spec["tenant_id"]

            q_raw = f'"{SF_DB}"."{raw_schema}"."EVENTS_RAW"'
            q_dst = f'"{SF_DB}"."{DST_SCHEMA}"."{source_table}"'

            try:
                # ── 1. Discover payload column names ────────────────────
                col_rows = sf.get_records(f"""
                    SELECT DISTINCT k.value::VARCHAR AS col_name
                    FROM (
                        SELECT payload
                        FROM   {q_raw}
                        WHERE  source_table = '{source_table}'
                          AND  IS_ARRAY(payload)
                        LIMIT 2000
                    ) AS sample,
                    LATERAL FLATTEN(input => sample.payload)          AS rec,
                    LATERAL FLATTEN(input => OBJECT_KEYS(rec.value))  AS k
                    WHERE IS_OBJECT(rec.value)
                    ORDER BY col_name
                """)
                col_names = [r[0] for r in col_rows
                             if r[0] and r[0].lower() not in _SKIP_COLS]

                if not col_names:
                    log.warning("No columns found for %s/%s – skipping", facility, source_table)
                    continue

                # ── 2. Build typed column definitions ───────────────────
                tbl_v2 = v2_types.get(source_table.lower(), {})
                has_id = "id" in [c.lower() for c in col_names]

                col_defs     = ['"tenant_id" VARCHAR NOT NULL']
                select_exprs = [f"'{tenant_id}'::VARCHAR AS \"tenant_id\""]

                for col in col_names:
                    sf_type     = _sf_type(col, tbl_v2.get(col.lower()))
                    null_clause = "NOT NULL" if col.lower() == "id" else "NULL"
                    col_defs.append(f'"{col.lower()}" {sf_type} {null_clause}')
                    select_exprs.append(
                        f'rec.value:"{col}"::{sf_type} AS "{col.lower()}"'
                    )

                col_defs.append('"_ingested_at" TIMESTAMP_TZ NULL')
                select_exprs.append('e.ingested_at AS "_ingested_at"')

                all_cols = (
                    ["tenant_id"]
                    + [c.lower() for c in col_names]
                    + ["_ingested_at"]
                )

                # ── 3. Ensure target table exists ────────────────────────
                pk_expr = ('"tenant_id", "id"' if has_id else '"tenant_id"')
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {q_dst} (
                        {', '.join(col_defs)},
                        PRIMARY KEY ({pk_expr})
                    )
                """)

                # ── 4. Build deduplicating source SELECT ─────────────────
                select_str = ", ".join(select_exprs)
                qualify_clause = ""
                if has_id:
                    qualify_clause = """
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY "tenant_id", rec.value:"id"::VARCHAR
                            ORDER BY e.ingested_at DESC
                        ) = 1
                    """

                source_sql = f"""
                    SELECT {select_str}
                    FROM   {q_raw} e,
                           LATERAL FLATTEN(
                               input => IFF(IS_ARRAY(e.payload), e.payload,
                                            ARRAY_CONSTRUCT(e.payload))
                           ) AS rec
                    WHERE  e.source_table = '{source_table}'
                      AND  IS_OBJECT(rec.value)
                      AND  rec.value:"id" IS NOT NULL
                    {qualify_clause}
                """

                # ── 5. MERGE into V3_READY ───────────────────────────────
                if has_id:
                    update_set = ", ".join(
                        f't."{c}" = s."{c}"'
                        for c in all_cols if c not in ("tenant_id", "id")
                    )
                    insert_cols = ", ".join(f'"{c}"' for c in all_cols)
                    insert_vals = ", ".join(f's."{c}"' for c in all_cols)
                    cur.execute(f"""
                        MERGE INTO {q_dst} AS t
                        USING ({source_sql}) AS s
                          ON  t."tenant_id" = s."tenant_id"
                         AND  t."id"        = s."id"
                        WHEN MATCHED THEN
                            UPDATE SET {update_set}
                        WHEN NOT MATCHED THEN
                            INSERT ({insert_cols}) VALUES ({insert_vals})
                    """)
                else:
                    # No stable id: delete this tenant's rows and reload
                    cur.execute(f'DELETE FROM {q_dst} WHERE "tenant_id" = \'{tenant_id}\'')
                    insert_cols = ", ".join(f'"{c}"' for c in all_cols)
                    cur.execute(f"INSERT INTO {q_dst} ({insert_cols}) {source_sql}")

                log.info(
                    "Merged %s/%s → %s.%s",
                    facility, source_table, DST_SCHEMA, source_table,
                )

            except Exception as e:
                log.error("Failed %s/%s: %s", facility, source_table, e, exc_info=True)
                failed.append(f"{facility}/{source_table}")

        conn.close()
        if failed:
            raise RuntimeError(f"Failed specs: {failed}")

    schema = ensure_v3_ready_schema()
    specs  = list_raw_table_specs()
    schema >> specs
    flatten_to_v3_ready(specs)
