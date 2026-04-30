from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import json
import os

from airflow.models import Variable
import gspread

# snowflake deps
import snowflake.connector
from cryptography.hazmat.primitives import serialization


# ----------------------------
# SNOWFLAKE CLIENT (FIXED)
# ----------------------------
class SnowflakeClient:

    def __init__(self):
        private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip()

        with open(private_key_path, "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=None,
            )

        private_key = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        self.conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER").strip(),
            account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(),
            database=os.getenv("SNOWFLAKE_DATABASE").strip(),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW").strip(),
            role=os.getenv("SNOWFLAKE_ROLE").strip(),
            private_key=private_key,
        )

    def execute(self, sql: str):
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()

    def query(self, sql: str):
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetch_pandas_all()
        finally:
            cursor.close()


# ----------------------------
# CONFIG
# ----------------------------
SPREADSHEET_ID = Variable.get("WINGSPAN_SHEET_ID")
WORKSHEET_NAME = "Results"
SNOWFLAKE_TABLE = "RAW.GSHEET_RAW"

# ----------------------------
# GOOGLE SHEETS CLIENT
# ----------------------------
def get_gsheet_client():
    creds_dict = json.loads(Variable.get("GOOGLE_SA_JSON"))
    return gspread.service_account_from_dict(creds_dict)

def read_dictionary_sheet(spreadsheet_id: str, worksheet_name: str):
    gc = get_gsheet_client()
    ws = gc.open_by_key(spreadsheet_id).worksheet(worksheet_name)
    data = ws.get_all_records()
    return [
        {k.strip().lower().replace(" ", "_"): v for k, v in row.items()}
        for row in data
    ]

# ----------------------------
# DATA CLEANING & TYPE CONVERSION
# ----------------------------
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and convert data types to prevent Snowflake errors"""
    
    # Print initial column info for debugging
    print("Initial columns:", df.columns.tolist())
    print("Initial dtypes:\n", df.dtypes)
    
    # Handle numeric columns - critical for TOTAL_BILL issue
    numeric_cols = [
        'total_bill', 'claim_total', 'los', 'coverage_limit_kes', 
        'annual_premium', 'claim_frequency', 'avg_claim_amount',
        'total_claims_cost', 'loss_ratio', 'profit_margin', 
        'risk_score', 'fraud_probability', 'churn_probability',
        'county_cost_index', 'nhif_usage', 'telemedicine_usage',
        'mpesa_usage', 'family_size', 'deductible_pct'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            # Convert to numeric, coerce errors to NaN, then fill with 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            print(f"Cleaned {col}: min={df[col].min()}, max={df[col].max()}")
    
    # Handle date columns
    date_cols = ['adm_date', 'dis_date', 'claim_date', 'batch_date']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d')
    
    # Handle age_or_dob - could be mixed types
    if 'age_or_dob' in df.columns:
        df['age_or_dob'] = pd.to_numeric(df['age_or_dob'], errors='coerce').fillna(df['age_or_dob'])
    
    # Convert all remaining to string for safety
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).replace('nan', '')
    
    print("Final dtypes:\n", df.dtypes)
    print("Sample bad values check - total_bill contains '9500':")
    print(df['total_bill'].astype(str).str.contains('9500', na=False).sum())
    
    return df

# ----------------------------
# EXTRACT
# ----------------------------
def extract_from_gsheet(**context):
    data = read_dictionary_sheet(SPREADSHEET_ID, WORKSHEET_NAME)
    df = pd.DataFrame(data)
    
    # Clean data immediately after extraction
    df = clean_dataframe(df)
    
    # Save cleaned CSV
    file_path = "/tmp/gsheet_clean.csv"
    df.to_csv(file_path, index=False, na_rep='')
    
    # Save stage file path for loading
    context["ti"].xcom_push(key="file_path", value=file_path)
    context["ti"].xcom_push(key="row_count", value=len(df))
    
    print(f"Extracted {len(df)} rows to {file_path}")

# ----------------------------
# LOAD WITH COPY INTO (PRODUCTION READY)
# ----------------------------
def load_to_snowflake(**context):
    # pass
    client = SnowflakeClient()
    file_path = context["ti"].xcom_pull(task_ids="extract_gsheet", key="file_path")
    
    if not file_path or not os.path.exists(file_path):
        raise ValueError(f"Clean CSV file not found at {file_path}")
    
    row_count = context["ti"].xcom_pull(task_ids="extract_gsheet", key="row_count")
    print(f"Loading {row_count} rows from {file_path}")
    
    # 1. Create file format for robust CSV handling
    file_format_sql = """
    CREATE OR REPLACE FILE FORMAT gsheet_csv_format
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null', 'NaN', 'nan')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
    """
    client.execute(file_format_sql)
    
    # 2. Create temporary stage
    stage_sql = """
        CREATE OR REPLACE TEMPORARY STAGE gsheet_temp_stage
        FILE_FORMAT = gsheet_csv_format
    """
    client.execute(stage_sql)
    
    # 3. Upload file to stage
    upload_sql = f"""
        PUT file://{file_path} @gsheet_temp_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE
    """
    client.execute(upload_sql)
    
    # 4. Truncate table before loading fresh data
    truncate_sql = f"TRUNCATE TABLE {SNOWFLAKE_TABLE}"
    client.execute(truncate_sql)
    
    # 5. COPY INTO with error handling
    copy_sql = f"""
        COPY INTO {SNOWFLAKE_TABLE}
        FROM @gsheet_temp_stage
        FILE_FORMAT = (FORMAT_NAME = 'gsheet_csv_format')
        ON_ERROR = 'CONTINUE'
        PURGE = TRUE
    """
    client.execute(copy_sql)
    
    # 6. Verify load
    verify_sql = f"SELECT COUNT(*) as loaded_rows FROM {SNOWFLAKE_TABLE}"
    result = client.query(verify_sql)
    loaded_count = result['LOADED_ROWS'][0]
    
    print(f"Successfully loaded {loaded_count} rows into {SNOWFLAKE_TABLE}")
    
    # Cleanup
    client.execute("REMOVE @gsheet_temp_stage")
    
    # Remove local file
    try:
        os.remove(file_path)
    except:
        pass

# ----------------------------
# DAG DEFINITION
# ----------------------------
with DAG(
    dag_id="gsheet_to_snowflake",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["gsheet", "snowflake", "etl"],
    max_active_runs=1,
) as dag:

    extract = PythonOperator(
        task_id="extract_gsheet",
        python_callable=extract_from_gsheet,
        do_xcom_push=True
    )

    load = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    extract >> load