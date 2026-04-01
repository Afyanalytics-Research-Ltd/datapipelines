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
WORKSHEET_NAME = Variable.get("IGNITE_SHEET_WORKSHEET", default_var="Sheet1")

SF_DB = "HOSPITALS" 
SF_SHARED_SCHEMA = "SHARED"
SNOWFLAKE_STAGE = f"{SF_DB}.{SF_SHARED_SCHEMA}.DB_BUCKET"
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
    max_active_tasks=20,
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
    @task
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
    # 3. CREATE TABLE IN SNOWFLAKE
    # =========================
    @task(trigger_rule=TriggerRule.ALL_DONE)  
    def create_table(table_config):
        snowflake = SnowflakeHook(snowflake_conn_id="snowflake_default")

        df = pd.DataFrame(table_config["schema"])

        columns = []
        for _, row in df.iterrows():
            col = row["field"]
            dtype = map_mysql_to_snowflake(row["type"])
            nullable = "" if row["null"] == "YES" else "NOT NULL"

            columns.append(f"{col} {dtype} {nullable}")

        # metadata column
        columns.append("_ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()")

        create_sql = f"""
        CREATE OR REPLACE TABLE {table_config['table_name']} (
            {', '.join(columns)}
        );
        """

        conn = snowflake.get_conn()
        conn.cursor().execute(create_sql)

        return table_config

    # =========================
    # 4. EXTRACT FROM MYSQL
    # =========================
    @task(trigger_rule=TriggerRule.ALL_DONE)  
    def extract(table_config):
        mysql = MySqlHook(mysql_conn_id="mysql_conn")

        table = table_config["table_name"]

        query = f"SELECT * FROM {table}"

        df = mysql.get_pandas_df(query)

        file_path = f"/tmp/{table}.csv"
        df.to_csv(file_path, index=False)

        table_config["file_path"] = file_path

        return table_config

    # =========================
    # 5. LOAD INTO SNOWFLAKE
    # =========================
    @task(trigger_rule=TriggerRule.ALL_DONE)  
    def load(table_config):
        snowflake = SnowflakeHook(snowflake_conn_id="snowflake_conn")

        table = table_config["table_name"]
        file_path = table_config["file_path"]

        conn = snowflake.get_conn()
        cursor = conn.cursor()

        # upload file
        cursor.execute(f"PUT file://{file_path} @{SNOWFLAKE_STAGE} AUTO_COMPRESS=TRUE")

        # load data
        cursor.execute(f"""
            COPY INTO {table}
            FROM @{SNOWFLAKE_STAGE}
            FILE_FORMAT = (
                TYPE = CSV
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY='"'
            )
        """)

    # =========================
    # DAG FLOW
    # =========================
    schema = load_schema()
    tables = group_tables(schema)

    created = create_table.expand(table_config=tables)
    extracted = extract.expand(table_config=created)
    load.expand(table_config=extracted)