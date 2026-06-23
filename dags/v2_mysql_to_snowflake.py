from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
import logging

log = logging.getLogger(__name__)

SF_DB = "HOSPITALS"
SF_SCHEMA = "V2_RAW"
CHUNK_SIZE = 50_000
SKIP_TABLES = {"flyway_schema_history", "schemamigrations", "spring_session", "spring_session_attributes"}

default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}


def map_mysql_to_snowflake(mysql_type: str) -> str:
    t = str(mysql_type).lower()
    if "bigint" in t:                       return "NUMBER"
    if "int" in t:                          return "NUMBER"
    if "decimal" in t:                      return "NUMBER"
    if "float" in t or "double" in t:       return "FLOAT"
    if "datetime" in t or "timestamp" in t: return "TIMESTAMP_NTZ"
    if "date" in t:                         return "DATE"
    if "tinyint(1)" in t or "bool" in t:    return "BOOLEAN"
    if "varchar" in t or "text" in t:       return "STRING"
    return "STRING"


with DAG(
    dag_id="v2_mysql_to_snowflake",
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_tasks=4,
    tags=["migration", "v2", "mysql", "snowflake"],
) as dag:

    @task
    def ensure_schema() -> None:
        sf = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = sf.get_conn()
        conn.autocommit(True)
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_SCHEMA}")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS "{SF_DB}"."{SF_SCHEMA}"."_WATERMARKS" (
                table_name  STRING    NOT NULL,
                last_max_ts TIMESTAMP_NTZ,
                row_count   NUMBER,
                loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (table_name)
            )
        """)
        conn.close()

    @task
    def discover_tables() -> list:
        mysql = MySqlHook(mysql_conn_id="v2_mysql_conn")
        db_name = Variable.get("V2_MYSQL_DB")
        rows = mysql.get_records(f"""
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = '{db_name}'
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """)
        tables = {}
        for table_name, col_name, data_type, is_nullable, col_key in rows:
            if table_name.lower() in SKIP_TABLES:
                continue
            tables.setdefault(table_name, []).append({
                "field": col_name,
                "type": data_type,
                "null": is_nullable,
                "key": col_key,
            })
        return [{"table_name": t, "schema": cols} for t, cols in tables.items()]

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def load_watermarks() -> dict:
        sf = SnowflakeHook(snowflake_conn_id="snowflake_default")
        rows = sf.get_records(
            f'SELECT table_name, last_max_ts FROM "{SF_DB}"."{SF_SCHEMA}"."_WATERMARKS"'
        )
        return {r[0]: str(r[1]) for r in rows if r[1]}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def extract_and_load(tables: list, watermarks: dict) -> None:
        mysql = MySqlHook(mysql_conn_id="v2_mysql_conn")
        sf = SnowflakeHook(snowflake_conn_id="snowflake_default")
        db_name = Variable.get("V2_MYSQL_DB")
        mysql_engine = mysql.get_sqlalchemy_engine()

        conn = sf.get_conn()
        conn.autocommit(True)
        cur = conn.cursor()
        cur.execute(f"USE DATABASE {SF_DB}")
        cur.execute(f"USE SCHEMA {SF_SCHEMA}")

        failed = []

        for table_cfg in tables:
            table = table_cfg["table_name"]
            df_schema = pd.DataFrame(table_cfg["schema"])
            qualified  = f'"{SF_DB}"."{SF_SCHEMA}"."{table}"'
            staging    = f'"{SF_DB}"."{SF_SCHEMA}"."{table}_staging"'
            staging_uq = f'"{table}_staging"'

            try:
                col_defs_parts, pk_cols, ts_col = [], [], None

                for _, row in df_schema.iterrows():
                    sf_type  = map_mysql_to_snowflake(row["type"])
                    nullable = "" if row["null"] == "YES" else "NOT NULL"
                    col_defs_parts.append(f'"{row["field"]}" {sf_type} {nullable}')
                    if str(row.get("key", "")).upper() == "PRI":
                        pk_cols.append(row["field"])
                    if sf_type == "TIMESTAMP_NTZ" and ts_col is None:
                        ts_col = row["field"]

                col_defs_parts.append('"_ingested_at" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()')
                col_defs = ", ".join(col_defs_parts)

                cur.execute(f"CREATE TABLE IF NOT EXISTS {qualified} ({col_defs})")
                cur.execute(f"CREATE OR REPLACE TABLE {staging} ({col_defs})")

                wm = watermarks.get(table)
                where = f"WHERE `{ts_col}` > '{wm}'" if (wm and ts_col) else ""
                sql = f"SELECT * FROM `{db_name}`.`{table}` {where}"

                total = 0
                for chunk in pd.read_sql(sql, mysql_engine, chunksize=CHUNK_SIZE):
                    chunk["_ingested_at"] = pd.Timestamp.utcnow()
                    chunk.columns = [c.upper() for c in chunk.columns]
                    path = f"/tmp/{table}_chunk.csv"
                    chunk.to_csv(path, index=False)
                    cur.execute(f"PUT file://{path} @%{staging_uq} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
                    cur.execute(f"""
                        COPY INTO {staging} FROM @%{staging_uq}
                        FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
                        ON_ERROR = 'CONTINUE' PURGE = TRUE
                    """)
                    total += len(chunk)

                log.info("Extracted %s: %d rows", table, total)

                if pk_cols:
                    all_cols = [r["field"] for _, r in df_schema.iterrows()] + ["_ingested_at"]
                    join_clause   = " AND ".join(f'tgt."{c}" = stg."{c}"' for c in pk_cols)
                    update_clause = ", ".join(f'tgt."{c}" = stg."{c}"' for c in all_cols)
                    insert_cols   = ", ".join(f'"{c}"' for c in all_cols)
                    insert_vals   = ", ".join(f'stg."{c}"' for c in all_cols)
                    cur.execute(f"""
                        MERGE INTO {qualified} tgt USING {staging} stg ON {join_clause}
                        WHEN MATCHED     THEN UPDATE SET {update_clause}
                        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
                    """)
                else:
                    cur.execute(f"TRUNCATE TABLE {qualified}")
                    cur.execute(f"INSERT INTO {qualified} SELECT * FROM {staging}")

                # Compute max timestamp before dropping staging
                if ts_col:
                    cur.execute(f'SELECT MAX("{ts_col.upper()}") FROM {staging}')
                    row_ts = cur.fetchone()[0]
                    new_ts_lit = f"'{row_ts}'" if row_ts else "NULL"
                else:
                    new_ts_lit = "NULL"

                cur.execute(f"""
                    MERGE INTO "{SF_DB}"."{SF_SCHEMA}"."_WATERMARKS" tgt
                    USING (SELECT '{table}' AS tn, {new_ts_lit}::TIMESTAMP_NTZ AS ts, {total} AS rc) src
                    ON tgt.table_name = src.tn
                    WHEN MATCHED     THEN UPDATE SET last_max_ts=src.ts, row_count=src.rc, loaded_at=CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (table_name, last_max_ts, row_count) VALUES (src.tn, src.ts, src.rc)
                """)
                cur.execute(f"DROP TABLE IF EXISTS {staging}")

            except Exception as e:
                log.error("Failed table %s: %s", table, e, exc_info=True)
                failed.append(table)
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {staging}")
                except Exception:
                    pass

        conn.close()
        if failed:
            raise RuntimeError(f"Failed tables: {failed}")

    _schema = ensure_schema()
    tables  = discover_tables()
    wms     = load_watermarks()

    _schema >> tables
    _schema >> wms
    extract_and_load(tables, wms)
