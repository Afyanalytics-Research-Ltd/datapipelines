from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
import json
import gspread
import logging

log = logging.getLogger(__name__)


def get_gsheet_client():
    # Store service account JSON in Airflow Variable GOOGLE_SA_JSON
    creds_dict = json.loads(Variable.get("GOOGLE_SA_JSON"))
    return gspread.service_account_from_dict(creds_dict)

def read_dictionary_sheet(spreadsheet_id: str, worksheet_name: str):
    gc = get_gsheet_client()
    ws = gc.open_by_key(spreadsheet_id).worksheet(worksheet_name)
    return ws.get_all_records()  # list[dict]


def map_mysql_to_snowflake(mysql_type):
    mysql_type = mysql_type.lower()

    if "bigint" in mysql_type:
        return "NUMBER"
    elif "int" in mysql_type:
        return "NUMBER"
    elif "decimal" in mysql_type:
        return "NUMBER"
    elif "float" in mysql_type or "double" in mysql_type:
        return "FLOAT"
    elif "varchar" in mysql_type or "text" in mysql_type:
        return "STRING"
    elif "datetime" in mysql_type or "timestamp" in mysql_type:
        return "TIMESTAMP_NTZ"
    elif "date" in mysql_type:
        return "DATE"
    elif "tinyint(1)" in mysql_type or "boolean" in mysql_type:
        return "BOOLEAN"
    else:
        return "STRING"



def generate_create_table_sql(table_name, df):
    columns = []


    for _, row in df.iterrows():
        col = row["field"]
        dtype = map_mysql_to_snowflake(row["type"])
        nullable = "" if row["null"] == "YES" else "NOT NULL"

        columns.append(f"{col} {dtype} {nullable}")

    columns.append("_ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()")

    return f"""
    CREATE OR REPLACE TABLE {table_name} (
        {', '.join(columns)}
    );
    """



def extract_table(mysql_hook, table_name, incremental_column=None):
    if incremental_column:
        query = f"""
        SELECT * FROM {table_name}
        WHERE {incremental_column} >= DATE_SUB(NOW(), INTERVAL 1 DAY)
        """
    else:
        query = f"SELECT * FROM {table_name}"

    df = mysql_hook.get_pandas_df(query)

    file_path = f"/tmp/{table_name}.csv"
    df.to_csv(file_path, index=False)

    return file_path

SPREADSHEET_ID = Variable.get("IGNITE_SHEET_ID")
WORKSHEET_NAME = Variable.get("WORKSHEET", default_var="Sheet1")

SF_DB = "HOSPITALS"
SF_SHARED_SCHEMA = "TENRI"
CHUNK_SIZE = 50_000
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

# =========================
# GOOGLE SHEETS
# =========================
def get_gsheet_client():
    creds_dict = json.loads(Variable.get("GOOGLE_SA_JSON"))
    return gspread.service_account_from_dict(creds_dict)

def read_dictionary_sheet():
    gc = get_gsheet_client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
    return ws.get_all_records()

# =========================
# TYPE MAPPING
# =========================
def map_mysql_to_snowflake(mysql_type):
    mysql_type = str(mysql_type).lower()

    if "bigint" in mysql_type:
        return "NUMBER"
    elif "int" in mysql_type:
        return "NUMBER"
    elif "decimal" in mysql_type:
        return "NUMBER"
    elif "float" in mysql_type or "double" in mysql_type:
        return "FLOAT"
    elif "varchar" in mysql_type or "text" in mysql_type:
        return "STRING"
    elif "datetime" in mysql_type or "timestamp" in mysql_type:
        return "TIMESTAMP_NTZ"
    elif "date" in mysql_type:
        return "DATE"
    elif "tinyint(1)" in mysql_type or "boolean" in mysql_type:
        return "BOOLEAN"
    else:
        return "STRING"

# =========================
# DAG
# =========================
with DAG(
    dag_id="gsheet_mysql_to_snowflake_dynamic",
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_tasks=4,
    tags=["dynamic", "mysql", "snowflake"]
) as dag:

    # =========================
    # 1. LOAD SCHEMA FROM GSHEET
    # =========================
    @task
    def load_schema():
        records = read_dictionary_sheet()
        df = pd.DataFrame(records)

        # Clean columns
        df.columns = df.columns.str.strip().str.lower()
        df = df.fillna("")

        return df.to_dict("records")

    # =========================
    # 2. GROUP INTO TABLES
    # =========================
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def group_tables(schema_records):
        df = pd.DataFrame(schema_records)

        grouped = df.groupby("table")

        tables = []
        for table_name, group in grouped:
            tables.append({
                "table_name": table_name,
                "schema": group.to_dict("records")
            })

        return tables

    # =========================
    # 3. PROCESS ALL TABLES SEQUENTIALLY
    # =========================
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def process_all_tables(tables):
        mysql = MySqlHook(mysql_conn_id="mysql_conn")
        snowflake = SnowflakeHook(snowflake_conn_id="snowflake_default")
        sf_conn = snowflake.get_conn()
        sf_conn.autocommit(True)
        sf_cursor = sf_conn.cursor()
        mysql_engine = mysql.get_sqlalchemy_engine()

        # Set Snowflake session context so @% stage refs work with unqualified table names
        sf_cursor.execute(f"USE DATABASE {SF_DB}")
        sf_cursor.execute(f"USE SCHEMA {SF_SHARED_SCHEMA}")

        failed_tables = []

        for table_config in tables:
            table = table_config["table_name"]
            qualified_table = f'"{SF_DB}"."{SF_SHARED_SCHEMA}"."{table}"'
            staging_table = f'"{SF_DB}"."{SF_SHARED_SCHEMA}"."{table}_staging"'
            staging_table_unqualified = f'"{table}_staging"'

            try:
                # --- BUILD COLUMN DEFINITIONS ---
                df_schema = pd.DataFrame(table_config["schema"])
                columns = []
                pk_columns = []
                for _, row in df_schema.iterrows():
                    col = row["field"]
                    dtype = map_mysql_to_snowflake(row["type"])
                    nullable = "" if row["null"] == "YES" else "NOT NULL"
                    columns.append(f'"{col}" {dtype} {nullable}')
                    if str(row.get("key", "")).upper() == "PRI":
                        pk_columns.append(col)
                columns.append('"_ingested_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP()')
                col_defs = ', '.join(columns)

                # --- ENSURE TARGET TABLE EXISTS (don't drop it) ---
                sf_cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {qualified_table} (
                        {col_defs}
                    )
                """)
                log.info("Ensured table '%s' exists", qualified_table)

                # --- CREATE STAGING TABLE ---
                sf_cursor.execute(f"""
                    CREATE OR REPLACE TABLE {staging_table} (
                        {col_defs}
                    )
                """)

                # --- EXTRACT IN CHUNKS & LOAD TO STAGING ---
                total_rows = 0

                for chunk in pd.read_sql(f"SELECT * FROM `{table}`", mysql_engine, chunksize=CHUNK_SIZE):
                    chunk["_ingested_at"] = pd.Timestamp.utcnow()
                    chunk.columns = [c.upper() for c in chunk.columns]

                    file_path = f"/tmp/{table}_chunk.csv"
                    chunk.to_csv(file_path, index=False)

                    # @% stage only works with unqualified table name when session context is set
                    sf_cursor.execute(f"PUT file://{file_path} @%{staging_table_unqualified} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
                    sf_cursor.execute(f"""
                        COPY INTO {staging_table}
                        FROM @%{staging_table_unqualified}
                        FILE_FORMAT = (
                            TYPE = CSV
                            SKIP_HEADER = 1
                            FIELD_OPTIONALLY_ENCLOSED_BY='"'
                        )
                        ON_ERROR = 'CONTINUE'
                        PURGE = TRUE
                    """)
                    total_rows += len(chunk)

                log.info("Extracted '%s': %d rows total", table, total_rows)

                # --- MERGE OR TRUNCATE+INSERT ---
                if pk_columns:
                    join_clause = " AND ".join(f'tgt."{c}" = stg."{c}"' for c in pk_columns)
                    all_cols = [row["field"] for _, row in df_schema.iterrows()] + ["_ingested_at"]
                    update_clause = ", ".join(f'tgt."{c}" = stg."{c}"' for c in all_cols)
                    insert_cols = ", ".join(f'"{c}"' for c in all_cols)
                    insert_vals = ", ".join(f'stg."{c}"' for c in all_cols)

                    sf_cursor.execute(f"""
                        MERGE INTO {qualified_table} tgt
                        USING {staging_table} stg
                        ON {join_clause}
                        WHEN MATCHED THEN UPDATE SET {update_clause}
                        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
                    """)
                    log.info("Merged '%s': %d rows", qualified_table, total_rows)
                else:
                    sf_cursor.execute(f"TRUNCATE TABLE {qualified_table}")
                    sf_cursor.execute(f"INSERT INTO {qualified_table} SELECT * FROM {staging_table}")
                    log.info("Truncate+inserted '%s': %d rows", qualified_table, total_rows)

                sf_cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")

            except Exception as e:
                log.error("Failed to process table '%s': %s", table, str(e), exc_info=True)
                failed_tables.append(table)
                # clean up staging if it exists before moving on
                try:
                    sf_cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
                except Exception:
                    pass

        if failed_tables:
            raise RuntimeError(f"The following tables failed to load: {failed_tables}")

    # =========================
    # DAG FLOW
    # =========================
    schema = load_schema()
    tables = group_tables(schema)
    process_all_tables(tables)