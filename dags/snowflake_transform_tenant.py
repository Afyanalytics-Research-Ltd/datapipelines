from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import json
import logging

log = logging.getLogger(__name__)

SF_DB     = "HOSPITALS"
SRC_SCHEMA = "V2_RAW"
DST_SCHEMA = "V3_READY"

default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}


with DAG(
    dag_id="snowflake_transform_tenant",
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_tasks=4,
    tags=["migration", "snowflake", "transform", "multi-tenant"],
) as dag:

    @task
    def ensure_schema() -> None:
        sf = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = sf.get_conn()
        conn.autocommit(True)
        conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{DST_SCHEMA}")
        conn.close()

    @task
    def list_source_tables() -> list:
        sf = SnowflakeHook(snowflake_conn_id="snowflake_default")
        rows = sf.get_records(f"""
            SELECT TABLE_NAME
            FROM {SF_DB}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{SRC_SCHEMA}'
              AND TABLE_TYPE   = 'BASE TABLE'
              AND SUBSTR(TABLE_NAME, 1, 1) != '_'
            ORDER BY TABLE_NAME
        """)
        return [r[0] for r in rows]

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def transform_tables(table_names: list) -> None:
        """
        For each V2_RAW table:
          - Rename columns using V2_TO_V3_FIELD_MAPPING Airflow Variable
          - Inject tenant_id from MIGRATION_TENANT_ID Airflow Variable
          - Write result to V3_READY as CREATE OR REPLACE TABLE ... AS SELECT

        V2_TO_V3_FIELD_MAPPING format (JSON):
          {
            "patients":   { "fname": "first_name", "lname": "last_name" },
            "encounters": { "enc_date": "encounter_date" }
          }
        Tables absent from the mapping get a passthrough (all columns kept as-is).
        """
        sf = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = sf.get_conn()
        conn.autocommit(True)
        cur = conn.cursor()

        raw_mapping = Variable.get("V2_TO_V3_FIELD_MAPPING", default_var="{}")
        field_map: dict = json.loads(raw_mapping)
        tenant_id = Variable.get("MIGRATION_TENANT_ID")

        failed = []

        for table in table_names:
            try:
                rows = sf.get_records(f"""
                    SELECT COLUMN_NAME
                    FROM {SF_DB}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{SRC_SCHEMA}'
                      AND TABLE_NAME   = '{table}'
                    ORDER BY ORDINAL_POSITION
                """)
                src_cols = [r[0] for r in rows]

                # Per-table mapping (case-insensitive lookup)
                col_map: dict = {
                    k.upper(): v
                    for k, v in field_map.get(table, field_map.get(table.lower(), {})).items()
                }

                select_exprs = []
                for col in src_cols:
                    if col.upper() == "_INGESTED_AT":
                        continue
                    alias = col_map.get(col.upper(), col.lower())
                    if alias.upper() != col.upper():
                        select_exprs.append(f'"{col}" AS "{alias}"')
                    else:
                        select_exprs.append(f'"{col}"')

                # tenant_id first so it's always column 1 in V3_READY
                select_exprs.insert(0, f"'{tenant_id}' AS \"tenant_id\"")
                select_exprs.append("CURRENT_TIMESTAMP() AS \"_migrated_at\"")

                cur.execute(f"""
                    CREATE OR REPLACE TABLE "{SF_DB}"."{DST_SCHEMA}"."{table}" AS
                    SELECT {", ".join(select_exprs)}
                    FROM   "{SF_DB}"."{SRC_SCHEMA}"."{table}"
                """)
                log.info("Transformed %s.%s → %s.%s", SRC_SCHEMA, table, DST_SCHEMA, table)

            except Exception as e:
                log.error("Failed transform for %s: %s", table, e, exc_info=True)
                failed.append(table)

        conn.close()
        if failed:
            raise RuntimeError(f"Failed transforms: {failed}")

    _schema = ensure_schema()
    tables  = list_source_tables()
    _schema >> tables
    transform_tables(tables)
