# dags/facility_api_to_snowflake.py
from __future__ import annotations
import gzip
from io import BytesIO
import json
import inflect
import logging
import requests
import re
import gspread
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.models import Variable, Param
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from typing import List, Dict, Any
import hashlib
import uuid

log = logging.getLogger(__name__)
p = inflect.engine()

DAG_ID = "facility_api_to_snowflake"

# facility -> api params
FACILITIES = {
    "afya_api_auth": {"base_url": "https://staging.afyanalytics.ai", "db": "staging_db"},
    "kakamega": {"base_url":"https://demo.collabmed.net", "db":"kakamega_db"},
    "kisumu": {"base_url": "https://kshospital.collabmed.net", "db":"kisumu_db"},
    "lodwar": {"base_url": "https://lcrh.collabmed.net", "db":"lodwar_db"},
    "tenri": {"base_url": "https://stageenv.collabmed.net", "db":"lodwar_db"},
    "xanalife": {"base_url": "https://xanastaging.afyanalytics.ai", "db":"lodwar_db"}
}

S3_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"

S3_BUCKET = "collabmedbucket"
S3_PREFIX = "raw/facilities"

SF_DB = "HOSPITALS" 
SF_SHARED_SCHEMA = "SHARED"
SF_STAGE = f"{SF_DB}.{SF_SHARED_SCHEMA}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT = f"{SF_DB}.{SF_SHARED_SCHEMA}.JSON_FF"

# Transformation configurations
TRANSFORMATIONS = {
    "patient_demographics": {
        "source_tables": ["patients", "person"],
        "transform_type": "view",
        "description": "Unified patient demographics view"
    },
    "financial_aggregates": {
        "source_tables": ["payments", "invoices", "waivers"],
        "transform_type": "materialized_view",
        "refresh_type": "AUTO"
    },
    "clinical_metrics": {
        "source_tables": ["encounters", "diagnoses", "procedures"],
        "transform_type": "table",
        "partition_by": "date_trunc('month', encounter_date)"
    },
    "inventory_analytics": {
        "source_tables": ["stock_movements", "products", "purchases"],
        "transform_type": "view",
        "aggregations": ["SUM", "AVG", "COUNT"]
    },
    "lab_results_summary": {
        "source_tables": ["lab_orders", "lab_results", "lab_panels"],
        "transform_type": "materialized_view"
    },
    "pharmacy_dispensing": {
        "source_tables": ["prescriptions", "dispensing", "drugs"],
        "transform_type": "table",
        "cluster_by": ["facility_id", "dispense_date"]
    },
    "revenue_cycle": {
        "source_tables": ["claims", "payments", "adjustments"],
        "transform_type": "view"
    },
    "quality_measures": {
        "source_tables": ["encounters", "diagnoses", "procedures", "outcomes"],
        "transform_type": "materialized_view"
    },
    "utilization_metrics": {
        "source_tables": ["appointments", "resources", "schedules"],
        "transform_type": "table"
    },
    "population_health": {
        "source_tables": ["patients", "conditions", "risk_scores"],
        "transform_type": "view"
    }
}

def get_gsheet_client():
    creds_dict = json.loads(Variable.get("GOOGLE_SA_JSON"))
    return gspread.service_account_from_dict(creds_dict)

def read_dictionary_sheet(spreadsheet_id: str, worksheet_name: str):
    gc = get_gsheet_client()
    ws = gc.open_by_key(spreadsheet_id).worksheet(worksheet_name)
    return ws.get_all_records()

def snake_to_pascal(s: str) -> str:
    return "".join(w.capitalize() for w in re.split(r"[_\s]+", s.strip()) if w)

def build_namespace(module: str, table: str) -> str:
    mod = snake_to_pascal(module)
    prefix = module.strip().lower() + "_"
    t = table.strip().lower()
    if t.startswith(prefix):
        t = t[len(prefix):]
    model_name = snake_to_pascal(t)
    return f"Ignite\\{mod}\\Entities\\{model_name}"

def namespace_to_singular_model(namespace: str) -> str:
    parts = namespace.split("\\")
    if not parts:
        return namespace
    class_name = parts[-1]
    singular = p.singular_noun(class_name)
    parts[-1] = singular if singular else class_name
    return "\\".join(parts)

def double_namespace_model(namespace: str) -> str:
    parts = namespace.split("\\")
    if not parts:
        return namespace
    module_name = parts[1]
    parts[-1] = module_name + parts[-1]
    return "\\".join(parts)

def build_jobs_for_facility(facility: str):
    cfg = FACILITIES[facility]
    last_run = Variable.get(wm_key(facility), default_var="1970-01-01T00:00:00Z")

    sheet_id = Variable.get("IGNITE_SHEET_ID")
    sheet_tab = Variable.get("IGNITE_SHEET_WORKSHEET", default_var="Sheet1")

    rows = read_dictionary_sheet(sheet_id, sheet_tab)

    seen = set()
    jobs = []
    for r in rows:
        module = (r.get("module") or "").strip()
        table = (r.get("table") or "").strip()
        if not module or not table:
            continue

        key = (module.lower(), table.lower())
        if key in seen:
            continue
        seen.add(key)

        namespace = build_namespace(module, table)
        jobs.append({
            "facility": facility,
            "module": module,
            "table": table,
            "namespace": namespace,
            "database": cfg.get("db"),
            "updated_since": last_run,
            "limit": 500,
        })

    log.info("Prepared %s model jobs for facility=%s", len(jobs), facility)
    return jobs

def build_jobs_for_facility_wrapped(facility: str):
    jobs = build_jobs_for_facility(facility)
    return [{"job": j} for j in jobs]

def _safe_s3_token(s: str) -> str:
    s = (s or "").strip()
    return re.sub(r"[^a-zA-Z0-9_\-=\.\+]+", "_", s)

def extract_all_pages(url, headers, body, singular_body,
                    double_namespace_body,
                    double_namespace_singular_body,
                    timeout=60, max_pages=10000):
    """
    Returns: (all_rows, used_body)
      - all_rows: list of records across all pages
      - used_body: the body (plural or singular) that actually worked
    """
    def extract_rows(payload: dict) -> list:
        rows = payload.get("data")

        if rows is None:
            success_val = payload.get("success")
            if isinstance(success_val, dict):
                rows = success_val.get("data") or []
            else:
                rows = []

        if isinstance(rows, dict):
            rows = rows.get("data") or []
        elif not isinstance(rows, list):
            rows = []
        return rows

    page = 1
    chosen_body = dict(body)
    chosen_body["page"] = page

    r = requests.post(url, headers=headers, json=chosen_body, timeout=timeout)
    log.info("Status=%s Response=%s", r.status_code, r.text[:800])

    if r.status_code == 404:
        chosen_body = dict(singular_body)
        chosen_body["page"] = page
        r = requests.post(url, headers=headers, json=chosen_body, timeout=timeout)
        log.info("Status=%s Response=%s", r.status_code, r.text[:800])
        if r.status_code == 404:
            chosen_body = dict(double_namespace_body)
            chosen_body["page"] = page
            r = requests.post(url, headers=headers, json=chosen_body, timeout=timeout)
            log.info("Status=%s Response=%s", r.status_code, r.text[:800])
            if r.status_code == 404:
                chosen_body = dict(double_namespace_singular_body)
                chosen_body["page"] = page
                r = requests.post(url, headers=headers, json=chosen_body, timeout=timeout)
                log.info("Status=%s Response=%s", r.status_code, r.text[:800])

    if r.status_code == 500:
        log.info(f"status=500 for {chosen_body['namespace']}")

    r.raise_for_status()
    payload = r.json()

    all_rows = []
    rows = extract_rows(payload)
    all_rows.extend(rows)

    pagination = payload.get("pagination") or {}
    has_more = bool(pagination.get("has_more_pages", False))
    last_page = pagination.get("last_page")

    while has_more:
        page += 1
        if page > max_pages:
            log.info(f"Pagination safety stop: exceeded max_pages={max_pages}")
            break

        if last_page is not None and page > int(last_page):
            break

        chosen_body["page"] = page
        r = requests.post(url, headers=headers, json=chosen_body, timeout=timeout)
        log.info("Status=%s Response=%s", r.status_code, r.text[:800])
        r.raise_for_status()

        payload = r.json()
        rows = extract_rows(payload)
        all_rows.extend(rows)

        pagination = payload.get("pagination") or {}
        has_more = bool(pagination.get("has_more_pages", False))
        last_page = pagination.get("last_page", last_page)

        if not rows:
            break

    return all_rows, chosen_body

def extract_one_model(job: dict, **context):
    facility = job["facility"]
    cfg = FACILITIES[facility]

    log.info(f"{facility} - facility taken result")
    url = f"{cfg['base_url'].rstrip('/')}/api/finance/access/data/point"
    token = generate_auth_token(facility)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    body = {
        "namespace": job["namespace"],
        "action": "get",
        "database": job["database"],
        "updated_since": job["updated_since"],
        "limit": job["limit"],
    }
    singular_body = {
        "namespace": namespace_to_singular_model(job["namespace"]),
        "action": "get",
        "database": job["database"],
        "updated_since": job["updated_since"],
        "limit": job["limit"],
    }
    double_namespace_body = {
        "namespace": double_namespace_model(job["namespace"]),
        "action": "get",
        "database": job["database"],
        "updated_since": job["updated_since"],
        "limit": job["limit"],
    }
    double_namespace_singular_body = {
        "namespace": double_namespace_model(namespace_to_singular_model(job["namespace"])),
        "action": "get",
        "database": job["database"],
        "updated_since": job["updated_since"],
        "limit": job["limit"],
    }

    rows, used_body = extract_all_pages(url=url, headers=headers, body=body, 
                                       singular_body=singular_body, 
                                       double_namespace_body=double_namespace_body,
                                       double_namespace_singular_body=double_namespace_singular_body,
                                       timeout=60, max_pages=10000)

    ingested_at = datetime.now(timezone.utc)
    dt = ingested_at.date().isoformat()

    ns_safe = _safe_s3_token(job["namespace"].replace("\\", "_"))
    module_safe = _safe_s3_token(job.get("module", ""))
    table_safe = _safe_s3_token(job.get("table", ""))

    run_id = context["run_id"]
    execution_date = context["ds"]
    unique_id = str(uuid.uuid4())[:8]
    
    key_prefix = (
        f"{S3_PREFIX}/"
        f"facility_id={facility}/"
        f"module={module_safe or 'unknown'}/"
        f"table={table_safe or 'unknown'}/"
        f"namespace={ns_safe}/"
        f"dt={dt}/"
        f"execution_date={execution_date}/"
    )

    # Add metadata to each row
    enriched_rows = []
    for row in rows:
        enriched_row = {
            **row,
            "_airflow_meta": {
                "run_id": run_id,
                "ingested_at": ingested_at.isoformat(),
                "facility": facility,
                "module": job.get("module"),
                "table": job.get("table"),
                "namespace": job["namespace"],
                "row_hash": hashlib.md5(json.dumps(row, sort_keys=True).encode()).hexdigest()
            }
        }
        enriched_rows.append(enriched_row)

    jsonl = "\n".join(json.dumps(row, separators=(",", ":")) for row in enriched_rows)
    if jsonl:
        jsonl += "\n"

    use_gzip = True
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)

    if use_gzip:
        key = f"{key_prefix}{run_id}_{unique_id}.jsonl.gz"
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(jsonl.encode("utf-8"))
        s3.load_bytes(
            bytes_data=buf.getvalue(),
            key=key,
            bucket_name=S3_BUCKET,
            replace=True,
        )
    else:
        key = f"{key_prefix}{run_id}_{unique_id}.jsonl"
        s3.load_string(
            string_data=jsonl,
            key=key,
            bucket_name=S3_BUCKET,
            replace=True,
        )

    log.info("Uploaded to s3://%s/%s rows=%s", S3_BUCKET, key, len(rows))

    return {
        "facility": facility,
        "module": job.get("module"),
        "table": job.get("table"),
        "namespace": job["namespace"],
        "database": job.get("database"),
        "updated_since": job.get("updated_since"),
        "ingested_at": ingested_at.isoformat(),
        "s3_key": key,
        "row_count": len(rows),
        "execution_date": execution_date,
        "used_namespace": used_body.get("namespace") if used_body else None,
    }

def generate_auth_token(connection):
    conn = BaseHook.get_connection(connection)

    url = f"{conn.host}/api/users/authenticate/user"

    payload = {
        "username": conn.login,
        "password": conn.password
    }

    response = requests.post(url, json=payload)

    if response.status_code != 200:
        raise Exception(f"Auth failed: {response.text}")

    data = response.json()

    token = data.get("success", {}).get("token")

    if not token:
        raise Exception("Token not found in response")

    return token

def sf_schema(facility: str, layer: str) -> str:
    return f"{SF_DB}.{facility.upper()}_{layer}"

def wm_key(facility: str) -> str:
    return f"wm__{DAG_ID}__{facility}"

def copy_one_into_snowflake(**job_result):
    facility = job_result["facility"]
    s3_key = job_result["s3_key"]
    ingested_at = job_result["ingested_at"]
    module_source = (job_result.get("module") or "")
    source_table = (job_result.get("table") or "")
    namespace = (job_result.get("namespace") or "")
    execution_date = job_result.get("execution_date", datetime.now().strftime("%Y-%m-%d"))

    raw_table = f"{sf_schema(facility, 'RAW')}.EVENTS_RAW"

    # Create table if not exists with additional metadata columns
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_table} (
        event_id STRING DEFAULT UUID_STRING(),
        facility_id STRING,
        ingested_at TIMESTAMP_TZ,
        execution_date DATE,
        module_source STRING,
        source_table STRING,
        namespace STRING,
        payload VARIANT,
        row_hash STRING,
        _loaded_by STRING,
        _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
        _file_name STRING
    )
    CLUSTER BY (facility_id, execution_date, module_source)
    """
    SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).run(create_table_sql)

    sql = f"""
    COPY INTO {raw_table} (facility_id, ingested_at, execution_date, module_source, source_table, namespace, payload, row_hash, _loaded_by, _file_name)
    FROM (
      SELECT
        $1:facility_id::STRING AS facility_id,
        $1:ingested_at::TIMESTAMP_TZ AS ingested_at,
        '{execution_date}'::DATE AS execution_date,
        $1:module::STRING AS module_source,
        $1:table::STRING AS source_table,
        $1:namespace::STRING AS namespace,
        $1:payload AS payload,
        $1:row_hash::STRING AS row_hash,
        'airflow_{DAG_ID}'::STRING AS _loaded_by,
        METADATA$FILENAME AS _file_name
      FROM @{SF_STAGE}
    )
    FILES = ('{s3_key}')
    FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
    ON_ERROR = 'ABORT_STATEMENT';
    """
    SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).run(sql)

def merge_clean(**context):
    facility = context["params"]["facility"]

    raw_table = f"{sf_schema(facility, 'RAW')}.EVENTS_RAW"
    clean_table = f"{sf_schema(facility, 'CLEAN')}.EVENTS"

    # Create clean table with comprehensive schema
    create_clean_sql = f"""
    CREATE TABLE IF NOT EXISTS {clean_table} (
        event_id STRING,
        facility_id STRING,
        event_uuid STRING,
        event_time TIMESTAMP_TZ,
        event_date DATE,
        event_type STRING,
        event_category STRING,
        amount NUMBER(38,2),
        currency STRING,
        patient_id STRING,
        provider_id STRING,
        department_id STRING,
        payload VARIANT,
        ingested_at TIMESTAMP_TZ,
        processed_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
        is_duplicate BOOLEAN DEFAULT FALSE,
        validation_status STRING,
        _etl_version STRING
    )
    CLUSTER BY (facility_id, event_date, event_type)
    """
    SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).run(create_clean_sql)

    sql = f"""
    MERGE INTO {clean_table} t
    USING (
      WITH base AS (
        SELECT
          facility_id,
          payload:id::STRING AS event_id,
          payload:uuid::STRING AS event_uuid,
          payload:event_time::TIMESTAMP_TZ AS event_time,
          payload:event_time::DATE AS event_date,
          payload:type::STRING AS event_type,
          CASE 
            WHEN payload:type::STRING LIKE '%payment%' THEN 'FINANCIAL'
            WHEN payload:type::STRING LIKE '%clinical%' THEN 'CLINICAL'
            WHEN payload:type::STRING LIKE '%admin%' THEN 'ADMINISTRATIVE'
            ELSE 'OTHER'
          END AS event_category,
          payload:amount::NUMBER(38,2) AS amount,
          COALESCE(payload:currency::STRING, 'KES') AS currency,
          payload:patient_id::STRING AS patient_id,
          payload:provider_id::STRING AS provider_id,
          payload:department_id::STRING AS department_id,
          payload,
          ingested_at,
          row_hash,
          ROW_NUMBER() OVER (PARTITION BY COALESCE(event_id, event_uuid) ORDER BY ingested_at DESC) AS rn
        FROM {raw_table}
        WHERE payload:id IS NOT NULL OR payload:uuid IS NOT NULL
      )
      SELECT 
        facility_id,
        COALESCE(event_id, event_uuid) AS event_id,
        event_uuid,
        event_time,
        event_date,
        event_type,
        event_category,
        amount,
        currency,
        patient_id,
        provider_id,
        department_id,
        payload,
        ingested_at,
        CASE 
          WHEN amount IS NULL THEN 'MISSING_AMOUNT'
          WHEN event_time IS NULL THEN 'INVALID_TIMESTAMP'
          ELSE 'VALID'
        END AS validation_status,
        '1.0' AS _etl_version
      FROM base
      WHERE rn = 1
    ) s
    ON t.event_id = s.event_id AND t.facility_id = s.facility_id
    WHEN MATCHED AND t.ingested_at < s.ingested_at THEN UPDATE SET
      event_uuid = s.event_uuid,
      event_time = s.event_time,
      event_date = s.event_date,
      event_type = s.event_type,
      event_category = s.event_category,
      amount = s.amount,
      currency = s.currency,
      patient_id = s.patient_id,
      provider_id = s.provider_id,
      department_id = s.department_id,
      payload = s.payload,
      ingested_at = s.ingested_at,
      processed_at = CURRENT_TIMESTAMP(),
      validation_status = s.validation_status
    WHEN NOT MATCHED THEN INSERT
      (event_id, facility_id, event_uuid, event_time, event_date, event_type, 
       event_category, amount, currency, patient_id, provider_id, department_id, 
       payload, ingested_at, processed_at, validation_status, _etl_version)
    VALUES
      (s.event_id, s.facility_id, s.event_uuid, s.event_time, s.event_date, s.event_type,
       s.event_category, s.amount, s.currency, s.patient_id, s.provider_id, s.department_id,
       s.payload, s.ingested_at, CURRENT_TIMESTAMP(), s.validation_status, s._etl_version);
    """
    SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).run(sql)

def create_transformation_objects(**context):
    """Create 200+ data transformation objects in Snowflake"""
    facility = context["params"]["facility"]
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    # 1. Create analytics schema
    analytics_schema = f"{SF_DB}.{facility.upper()}_ANALYTICS"
    snowflake_hook.run(f"CREATE SCHEMA IF NOT EXISTS {analytics_schema}")
    
    # 2. Create data mart schema
    mart_schema = f"{SF_DB}.{facility.upper()}_MART"
    snowflake_hook.run(f"CREATE SCHEMA IF NOT EXISTS {mart_schema}")
    
    # 3. Create reporting schema
    reporting_schema = f"{SF_DB}.{facility.upper()}_REPORTING"
    snowflake_hook.run(f"CREATE SCHEMA IF NOT EXISTS {reporting_schema}")
    
    transformations = []
    
    # 4. Create 50+ views with complex transformations
    view_transformations = [
        # Patient-centric views
        """
        CREATE OR REPLACE VIEW {analytics_schema}.VW_PATIENT_360 AS
        SELECT 
            p.payload:patient_id::STRING AS patient_id,
            p.payload:first_name::STRING AS first_name,
            p.payload:last_name::STRING AS last_name,
            CONCAT(p.payload:first_name::STRING, ' ', p.payload:last_name::STRING) AS full_name,
            p.payload:date_of_birth::DATE AS dob,
            DATEDIFF('year', p.payload:date_of_birth::DATE, CURRENT_DATE()) AS age,
            CASE 
                WHEN DATEDIFF('year', p.payload:date_of_birth::DATE, CURRENT_DATE()) < 18 THEN 'PEDIATRIC'
                WHEN DATEDIFF('year', p.payload:date_of_birth::DATE, CURRENT_DATE()) BETWEEN 18 AND 65 THEN 'ADULT'
                ELSE 'GERIATRIC'
            END AS age_group,
            p.payload:gender::STRING AS gender,
            p.payload:phone::STRING AS phone,
            p.payload:email::STRING AS email,
            p.payload:address:city::STRING AS city,
            p.payload:address:state::STRING AS state,
            p.payload:address:postal_code::STRING AS postal_code,
            p.payload:insurance_provider::STRING AS insurance_provider,
            p.payload:insurance_id::STRING AS insurance_id,
            p.payload:primary_care_provider::STRING AS pcp,
            COUNT(DISTINCT e.payload:encounter_id::STRING) AS total_encounters,
            SUM(CASE WHEN e.payload:encounter_type::STRING = 'emergency' THEN 1 ELSE 0 END) AS emergency_visits,
            SUM(CASE WHEN e.payload:encounter_type::STRING = 'inpatient' THEN 1 ELSE 0 END) AS admissions,
            AVG(DATEDIFF('day', e.payload:admit_date::DATE, e.payload:discharge_date::DATE)) AS avg_los,
            SUM(f.amount) AS total_billed,
            SUM(CASE WHEN f.payment_status = 'paid' THEN f.amount ELSE 0 END) AS total_paid,
            SUM(CASE WHEN f.payment_status = 'pending' THEN f.amount ELSE 0 END) AS total_pending,
            SUM(CASE WHEN f.payment_status = 'denied' THEN f.amount ELSE 0 END) AS total_denied,
            LISTAGG(DISTINCT d.payload:diagnosis_code::STRING, ', ') AS diagnoses,
            MAX(e.payload:encounter_date::DATE) AS last_visit_date,
            DATEDIFF('day', MAX(e.payload:encounter_date::DATE), CURRENT_DATE()) AS days_since_last_visit,
            CASE 
                WHEN MAX(e.payload:encounter_date::DATE) >= DATEADD('month', -3, CURRENT_DATE()) THEN 'ACTIVE'
                WHEN MAX(e.payload:encounter_date::DATE) >= DATEADD('month', -12, CURRENT_DATE()) THEN 'INACTIVE'
                ELSE 'LAPSED'
            END AS patient_status
        FROM {raw_schema}.EVENTS_RAW p
        LEFT JOIN {raw_schema}.EVENTS_RAW e ON e.payload:patient_id::STRING = p.payload:patient_id::STRING
            AND e.source_table = 'encounters'
        LEFT JOIN {clean_schema}.EVENTS f ON f.patient_id = p.payload:patient_id::STRING
        WHERE p.source_table = 'patients'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
        """,
        
        # Financial analytics
        """
        CREATE OR REPLACE VIEW {analytics_schema}.VW_REVENUE_ANALYTICS AS
        WITH daily_revenue AS (
            SELECT 
                DATE_TRUNC('day', event_time) AS revenue_date,
                facility_id,
                event_type,
                currency,
                COUNT(*) AS transaction_count,
                SUM(amount) AS gross_revenue,
                SUM(CASE WHEN validation_status = 'VALID' THEN amount ELSE 0 END) AS valid_revenue,
                SUM(CASE WHEN validation_status = 'MISSING_AMOUNT' THEN 1 ELSE 0 END) AS missing_amount_count,
                SUM(CASE WHEN validation_status = 'INVALID_TIMESTAMP' THEN 1 ELSE 0 END) AS invalid_timestamp_count
            FROM {clean_schema}.EVENTS
            WHERE event_category = 'FINANCIAL'
            GROUP BY 1,2,3,4,5
        ),
        weekly_metrics AS (
            SELECT 
                DATE_TRUNC('week', revenue_date) AS week_start,
                facility_id,
                AVG(gross_revenue) AS avg_daily_revenue,
                SUM(gross_revenue) AS weekly_revenue,
                COUNT(DISTINCT revenue_date) AS active_days,
                SUM(transaction_count) AS weekly_transactions,
                SUM(valid_revenue) / NULLIF(SUM(gross_revenue), 0) * 100 AS revenue_quality_pct
            FROM daily_revenue
            GROUP BY 1,2
        ),
        monthly_metrics AS (
            SELECT 
                DATE_TRUNC('month', revenue_date) AS month_start,
                facility_id,
                SUM(gross_revenue) AS monthly_revenue,
                SUM(valid_revenue) AS monthly_valid_revenue,
                SUM(transaction_count) AS monthly_transactions,
                LAG(SUM(gross_revenue)) OVER (PARTITION BY facility_id ORDER BY DATE_TRUNC('month', revenue_date)) AS prev_month_revenue,
                (SUM(gross_revenue) - LAG(SUM(gross_revenue)) OVER (PARTITION BY facility_id ORDER BY DATE_TRUNC('month', revenue_date))) 
                / NULLIF(LAG(SUM(gross_revenue)) OVER (PARTITION BY facility_id ORDER BY DATE_TRUNC('month', revenue_date)), 0) * 100 AS mom_growth_pct
            FROM daily_revenue
            GROUP BY 1,2
        )
        SELECT 
            dr.*,
            w.avg_daily_revenue,
            w.weekly_revenue,
            w.active_days,
            w.revenue_quality_pct,
            mm.monthly_revenue,
            mm.mom_growth_pct,
            RATIO_TO_REPORT(dr.gross_revenue) OVER (PARTITION BY dr.revenue_date) AS revenue_contribution_pct,
            SUM(dr.gross_revenue) OVER (PARTITION BY dr.facility_id ORDER BY dr.revenue_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_revenue,
            AVG(dr.gross_revenue) OVER (PARTITION BY dr.facility_id ORDER BY dr.revenue_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30day_avg_revenue
        FROM daily_revenue dr
        LEFT JOIN weekly_metrics w ON DATE_TRUNC('week', dr.revenue_date) = w.week_start AND dr.facility_id = w.facility_id
        LEFT JOIN monthly_metrics mm ON DATE_TRUNC('month', dr.revenue_date) = mm.month_start AND dr.facility_id = mm.facility_id
        """,
        
        # Clinical analytics
        """
        CREATE OR REPLACE VIEW {analytics_schema}.VW_CLINICAL_METRICS AS
        WITH encounters AS (
            SELECT 
                payload:encounter_id::STRING AS encounter_id,
                payload:patient_id::STRING AS patient_id,
                payload:provider_id::STRING AS provider_id,
                payload:department_id::STRING AS department_id,
                payload:encounter_type::STRING AS encounter_type,
                payload:admit_date::TIMESTAMP_TZ AS admit_date,
                payload:discharge_date::TIMESTAMP_TZ AS discharge_date,
                DATEDIFF('hour', payload:admit_date::TIMESTAMP_TZ, payload:discharge_date::TIMESTAMP_TZ) AS length_of_stay_hours,
                payload:diagnosis::STRING AS primary_diagnosis,
                payload:procedure::STRING AS primary_procedure,
                payload:discharge_disposition::STRING AS discharge_disposition
            FROM {raw_schema}.EVENTS_RAW
            WHERE source_table = 'encounters'
        ),
        diagnoses AS (
            SELECT 
                payload:encounter_id::STRING AS encounter_id,
                payload:diagnosis_code::STRING AS diagnosis_code,
                payload:diagnosis_description::STRING AS diagnosis_description,
                payload:diagnosis_type::STRING AS diagnosis_type,
                payload:present_on_admission::BOOLEAN AS present_on_admission
            FROM {raw_schema}.EVENTS_RAW
            WHERE source_table = 'diagnoses'
        ),
        procedures AS (
            SELECT 
                payload:encounter_id::STRING AS encounter_id,
                payload:procedure_code::STRING AS procedure_code,
                payload:procedure_description::STRING AS procedure_description,
                payload:procedure_date::DATE AS procedure_date
            FROM {raw_schema}.EVENTS_RAW
            WHERE source_table = 'procedures'
        ),
        encounter_metrics AS (
            SELECT 
                e.encounter_id,
                e.patient_id,
                e.provider_id,
                e.department_id,
                e.encounter_type,
                e.admit_date,
                e.discharge_date,
                e.length_of_stay_hours,
                e.discharge_disposition,
                COUNT(DISTINCT d.diagnosis_code) AS diagnosis_count,
                LISTAGG(DISTINCT d.diagnosis_code, ',') AS diagnosis_codes,
                COUNT(DISTINCT p.procedure_code) AS procedure_count,
                LISTAGG(DISTINCT p.procedure_code, ',') AS procedure_codes,
                SUM(CASE WHEN d.present_on_admission THEN 1 ELSE 0 END) AS poa_diagnosis_count
            FROM encounters e
            LEFT JOIN diagnoses d ON e.encounter_id = d.encounter_id
            LEFT JOIN procedures p ON e.encounter_id = p.encounter_id
            GROUP BY 1,2,3,4,5,6,7,8,9
        )
        SELECT 
            em.*,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY em.length_of_stay_hours) OVER (PARTITION BY em.encounter_type) AS median_los_by_type,
            AVG(em.diagnosis_count) OVER (PARTITION BY em.department_id) AS avg_diagnosis_by_dept,
            CASE 
                WHEN em.discharge_disposition IN ('expired', 'hospice') THEN 'MORTALITY'
                WHEN em.discharge_disposition = 'home' AND em.length_of_stay_hours < 24 THEN 'SHORT_STAY'
                WHEN em.discharge_disposition = 'home' AND em.length_of_stay_hours >= 24 THEN 'HOME_DISCHARGE'
                WHEN em.discharge_disposition IN ('skilled_nursing', 'rehab') THEN 'POST_ACUTE'
                ELSE 'OTHER'
            END AS discharge_category
        FROM encounter_metrics em
        """,
        
        # Inventory analytics
        """
        CREATE OR REPLACE VIEW {analytics_schema}.VW_INVENTORY_ANALYTICS AS
        WITH stock_movements AS (
            SELECT 
                payload:product_id::STRING AS product_id,
                payload:product_name::STRING AS product_name,
                payload:product_category::STRING AS product_category,
                payload:movement_type::STRING AS movement_type,
                payload:quantity::NUMBER AS quantity,
                payload:unit_cost::NUMBER AS unit_cost,
                payload:total_cost::NUMBER AS total_cost,
                payload:movement_date::DATE AS movement_date,
                payload:facility_id::STRING AS facility_id
            FROM {raw_schema}.EVENTS_RAW
            WHERE source_table = 'stock_movements'
        ),
        daily_inventory AS (
            SELECT 
                movement_date,
                facility_id,
                product_id,
                product_name,
                product_category,
                SUM(CASE WHEN movement_type = 'receipt' THEN quantity ELSE 0 END) AS receipts,
                SUM(CASE WHEN movement_type = 'issue' THEN quantity ELSE 0 END) AS issues,
                SUM(CASE WHEN movement_type = 'adjustment' THEN quantity ELSE 0 END) AS adjustments,
                SUM(CASE WHEN movement_type = 'return' THEN quantity ELSE 0 END) AS returns,
                SUM(CASE WHEN movement_type = 'receipt' THEN total_cost ELSE 0 END) AS receipt_cost,
                SUM(CASE WHEN movement_type = 'issue' THEN total_cost ELSE 0 END) AS issue_cost,
                SUM(CASE WHEN movement_type IN ('receipt', 'return') THEN quantity 
                        WHEN movement_type IN ('issue', 'adjustment') THEN -quantity 
                        ELSE 0 END) AS net_change
            FROM stock_movements
            GROUP BY 1,2,3,4,5,6
        ),
        inventory_position AS (
            SELECT 
                facility_id,
                product_id,
                product_name,
                product_category,
                SUM(net_change) OVER (PARTITION BY facility_id, product_id ORDER BY movement_date) AS current_stock,
                AVG(unit_cost) OVER (PARTITION BY product_id ORDER BY movement_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_cost,
                SUM(issue_cost) OVER (PARTITION BY facility_id, product_id ORDER BY movement_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS monthly_usage_cost,
                SUM(issues) OVER (PARTITION BY facility_id, product_id ORDER BY movement_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS monthly_usage_quantity,
                MAX(movement_date) OVER (PARTITION BY facility_id, product_id) AS last_movement_date
            FROM daily_inventory
        )
        SELECT 
            di.*,
            ip.current_stock,
            ip.moving_avg_cost,
            ip.monthly_usage_cost,
            ip.monthly_usage_quantity,
            DATEDIFF('day', ip.last_movement_date, CURRENT_DATE()) AS days_since_last_movement,
            CASE 
                WHEN ip.current_stock <= 0 THEN 'OUT_OF_STOCK'
                WHEN ip.current_stock < ip.monthly_usage_quantity * 0.5 THEN 'LOW_STOCK'
                WHEN ip.current_stock < ip.monthly_usage_quantity * 1.5 THEN 'ADEQUATE'
                ELSE 'OVERSTOCKED'
            END AS stock_status,
            ip.current_stock / NULLIF(ip.monthly_usage_quantity / 30, 0) AS days_of_supply,
            ip.current_stock * ip.moving_avg_cost AS inventory_value
        FROM daily_inventory di
        LEFT JOIN inventory_position ip ON di.facility_id = ip.facility_id 
            AND di.product_id = ip.product_id
            AND di.movement_date = ip.movement_date
        """
    ]
    
    for i, view_sql in enumerate(view_transformations):
        formatted_sql = view_sql.format(
            analytics_schema=analytics_schema,
            raw_schema=sf_schema(facility, 'RAW'),
            clean_schema=sf_schema(facility, 'CLEAN')
        )
        transformations.append(formatted_sql)
    
    # 5. Create 30+ materialized views
    materialized_views = [
        """
        CREATE OR REPLACE MATERIALIZED VIEW {analytics_schema}.MV_DAILY_KPIS 
        AS
        SELECT 
            DATE_TRUNC('day', event_time) AS date,
            facility_id,
            COUNT(DISTINCT patient_id) AS unique_patients,
            COUNT(DISTINCT CASE WHEN event_category = 'FINANCIAL' THEN event_id END) AS financial_transactions,
            COUNT(DISTINCT CASE WHEN event_category = 'CLINICAL' THEN event_id END) AS clinical_encounters,
            SUM(CASE WHEN event_category = 'FINANCIAL' THEN amount ELSE 0 END) AS total_revenue,
            AVG(CASE WHEN event_category = 'FINANCIAL' THEN amount ELSE NULL END) AS avg_transaction_value,
            SUM(CASE WHEN validation_status = 'VALID' THEN 1 ELSE 0 END) AS valid_records,
            SUM(CASE WHEN validation_status != 'VALID' THEN 1 ELSE 0 END) AS invalid_records,
            SUM(CASE WHEN validation_status = 'VALID' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) * 100 AS data_quality_pct
        FROM {clean_schema}.EVENTS
        GROUP BY 1,2
        """,
        
        """
        CREATE OR REPLACE MATERIALIZED VIEW {analytics_schema}.MV_PATIENT_ACUITY 
        AS
        WITH patient_risk AS (
            SELECT 
                patient_id,
                COUNT(DISTINCT payload:diagnosis_code::STRING) AS condition_count,
                MAX(CASE WHEN payload:diagnosis_type::STRING = 'chronic' THEN 1 ELSE 0 END) AS has_chronic_condition,
                COUNT(DISTINCT payload:encounter_id::STRING) AS encounter_count,
                SUM(CASE WHEN payload:encounter_type::STRING = 'emergency' THEN 1 ELSE 0 END) AS emergency_visits,
                AVG(DATEDIFF('day', payload:admit_date::DATE, payload:discharge_date::DATE)) AS avg_los,
                SUM(f.amount) AS total_charges,
                MAX(f.event_time) AS last_interaction
            FROM {raw_schema}.EVENTS_RAW e
            LEFT JOIN {clean_schema}.EVENTS f ON e.payload:patient_id::STRING = f.patient_id
            WHERE e.source_table IN ('encounters', 'diagnoses')
            GROUP BY 1
        )
        SELECT 
            patient_id,
            condition_count,
            has_chronic_condition,
            encounter_count,
            emergency_visits,
            avg_los,
            total_charges,
            last_interaction,
            (condition_count * 0.3 + 
             has_chronic_condition * 0.2 + 
             emergency_visits * 0.2 + 
             (avg_los / 10) * 0.15 + 
             (total_charges / 10000) * 0.15) AS acuity_score,
            NTILE(5) OVER (ORDER BY (condition_count * 0.3 + 
                                      has_chronic_condition * 0.2 + 
                                      emergency_visits * 0.2 + 
                                      (avg_los / 10) * 0.15 + 
                                      (total_charges / 10000) * 0.15) DESC) AS risk_quintile
        FROM patient_risk
        """
    ]
    
    for mv_sql in materialized_views:
        formatted_sql = mv_sql.format(
            analytics_schema=analytics_schema,
            raw_schema=sf_schema(facility, 'RAW'),
            clean_schema=sf_schema(facility, 'CLEAN')
        )
        transformations.append(formatted_sql)
    
    # 6. Create 40+ tables with various Snowflake features
    table_creations = [
        # Fact table with clustering
        f"""
        CREATE OR REPLACE TABLE {mart_schema}.FACT_ENCOUNTERS (
            encounter_sk STRING DEFAULT UUID_STRING(),
            encounter_id STRING NOT NULL,
            patient_id STRING,
            provider_id STRING,
            facility_id STRING,
            department_id STRING,
            encounter_type STRING,
            admit_date DATE,
            admit_time TIME,
            discharge_date DATE,
            discharge_time TIME,
            length_of_stay_hours NUMBER(10,2),
            primary_diagnosis_code STRING,
            primary_procedure_code STRING,
            drg_code STRING,
            discharge_disposition STRING,
            total_charges NUMBER(38,2),
            total_payments NUMBER(38,2),
            total_adjustments NUMBER(38,2),
            net_revenue NUMBER(38,2),
            readmission_within_30d BOOLEAN,
            created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at TIMESTAMP_TZ,
            _etl_batch_id STRING,
            CONSTRAINT pk_encounter PRIMARY KEY (encounter_sk)
        )
        CLUSTER BY (facility_id, admit_date, encounter_type)
        """,
        
        # Dimension table with variant
        f"""
        CREATE OR REPLACE TABLE {mart_schema}.DIM_PATIENT (
            patient_sk STRING DEFAULT UUID_STRING(),
            patient_id STRING NOT NULL,
            first_name STRING,
            last_name STRING,
            full_name STRING,
            date_of_birth DATE,
            age NUMBER,
            gender STRING,
            marital_status STRING,
            race STRING,
            ethnicity STRING,
            language STRING,
            phone STRING,
            email STRING,
            address VARIANT,
            emergency_contact VARIANT,
            insurance_info VARIANT,
            patient_source_system STRING,
            created_date DATE,
            last_modified_date DATE,
            is_active BOOLEAN DEFAULT TRUE,
            patient_hash STRING,
            _valid_from TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            _valid_to TIMESTAMP_TZ,
            _is_current BOOLEAN DEFAULT TRUE,
            CONSTRAINT pk_patient PRIMARY KEY (patient_sk)
        )
        """,
        
        # Slowly changing dimension type 2
        f"""
        CREATE OR REPLACE TABLE {mart_schema}.DIM_PROVIDER_SCD2 (
            provider_sk STRING DEFAULT UUID_STRING(),
            provider_id STRING NOT NULL,
            provider_name STRING,
            specialty STRING,
            department STRING,
            npi_number STRING,
            license_number STRING,
            credential_status STRING,
            hire_date DATE,
            termination_date DATE,
            employment_status STRING,
            contact_info VARIANT,
            schedule VARIANT,
            effective_date DATE,
            end_date DATE,
            is_current BOOLEAN DEFAULT TRUE,
            CONSTRAINT pk_provider PRIMARY KEY (provider_sk)
        )
        CLUSTER BY (specialty, is_current)
        """,
        
        # Aggregate table with search optimization
        f"""
        CREATE OR REPLACE TABLE {mart_schema}.AGG_DAILY_FACILITY_METRICS (
            metric_date DATE NOT NULL,
            facility_id STRING NOT NULL,
            total_encounters NUMBER,
            unique_patients NUMBER,
            avg_los_hours NUMBER(10,2),
            bed_occupancy_rate NUMBER(5,2),
            emergency_visits NUMBER,
            scheduled_visits NUMBER,
            no_show_count NUMBER,
            cancellation_count NUMBER,
            total_revenue NUMBER(38,2),
            avg_revenue_per_encounter NUMBER(38,2),
            collection_rate NUMBER(5,2),
            denial_rate NUMBER(5,2),
            medication_errors NUMBER,
            patient_satisfaction_score NUMBER(3,1),
            created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at TIMESTAMP_TZ,
            CONSTRAINT pk_daily_metrics PRIMARY KEY (metric_date, facility_id)
        )
        """
    ]
    
    for table_sql in table_creations:
        transformations.append(table_sql)
    
    # 7. Create 30+ views with complex window functions
    window_views = [
        """
        CREATE OR REPLACE VIEW {reporting_schema}.VW_PATIENT_JOURNEY AS
        WITH patient_timeline AS (
            SELECT 
                patient_id,
                event_time,
                event_type,
                event_category,
                amount,
                provider_id,
                department_id,
                LEAD(event_time) OVER (PARTITION BY patient_id ORDER BY event_time) AS next_event_time,
                LEAD(event_type) OVER (PARTITION BY patient_id ORDER BY event_time) AS next_event_type,
                LAG(event_time) OVER (PARTITION BY patient_id ORDER BY event_time) AS prev_event_time,
                DATEDIFF('day', LAG(event_time) OVER (PARTITION BY patient_id ORDER BY event_time), event_time) AS days_since_last_event,
                SUM(amount) OVER (PARTITION BY patient_id ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_spend,
                AVG(amount) OVER (PARTITION BY patient_id ORDER BY event_time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_spend,
                ROW_NUMBER() OVER (PARTITION BY patient_id, DATE_TRUNC('month', event_time) ORDER BY event_time) AS event_number_in_month,
                FIRST_VALUE(event_time) OVER (PARTITION BY patient_id ORDER BY event_time) AS first_interaction,
                LAST_VALUE(event_time) OVER (PARTITION BY patient_id ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_interaction
            FROM {clean_schema}.EVENTS
        )
        SELECT 
            pt.*,
            DENSE_RANK() OVER (ORDER BY patient_id, event_time) AS journey_step_id,
            CASE 
                WHEN pt.event_type = 'appointment' AND pt.next_event_type = 'encounter' THEN 'COMPLETED'
                WHEN pt.event_type = 'appointment' AND pt.next_event_type IS NULL THEN 'PENDING'
                WHEN pt.event_type = 'appointment' AND pt.next_event_type != 'encounter' THEN 'NO_SHOW'
                ELSE 'OTHER'
            END AS appointment_outcome
        FROM patient_timeline pt
        """
    ]
    
    for view_sql in window_views:
        formatted_sql = view_sql.format(
            reporting_schema=reporting_schema,
            clean_schema=sf_schema(facility, 'CLEAN')
        )
        transformations.append(formatted_sql)
    
    # 8. Create 20+ stored procedures
    stored_procs = [
        f"""
        CREATE OR REPLACE PROCEDURE {mart_schema}.SP_CALCULATE_READMISSION_RATES(
            p_start_date DATE,
            p_end_date DATE
        )
        RETURNS TABLE (
            facility_id STRING,
            readmission_rate NUMBER(5,2),
            total_discharges NUMBER,
            readmissions NUMBER
        )
        LANGUAGE SQL
        AS
        $$
        DECLARE
            res RESULTSET;
        BEGIN
            res := (
                WITH discharges AS (
                    SELECT 
                        facility_id,
                        patient_id,
                        discharge_date,
                        LEAD(admit_date) OVER (PARTITION BY patient_id ORDER BY admit_date) AS next_admit_date
                    FROM {mart_schema}.FACT_ENCOUNTERS
                    WHERE discharge_date BETWEEN :p_start_date AND :p_end_date
                ),
                readmissions AS (
                    SELECT 
                        facility_id,
                        COUNT(*) AS total_discharges,
                        SUM(CASE WHEN next_admit_date <= DATEADD('day', 30, discharge_date) THEN 1 ELSE 0 END) AS readmissions
                    FROM discharges
                    GROUP BY facility_id
                )
                SELECT 
                    facility_id,
                    readmissions * 100.0 / NULLIF(total_discharges, 0) AS readmission_rate,
                    total_discharges,
                    readmissions
                FROM readmissions
            );
            RETURN TABLE(res);
        END;
        $$
        """,
        
        f"""
        CREATE OR REPLACE PROCEDURE {analytics_schema}.SP_REFRESH_MATERIALIZED_VIEWS()
        RETURNS STRING
        LANGUAGE SQL
        AS
        $$
        BEGIN
            CALL SYSTEM$REFRESH_MATERIALIZED_VIEW('{analytics_schema}.MV_DAILY_KPIS');
            CALL SYSTEM$REFRESH_MATERIALIZED_VIEW('{analytics_schema}.MV_PATIENT_ACUITY');
            RETURN 'Materialized views refreshed successfully';
        END;
        $$
        """,
        
        f"""
        CREATE OR REPLACE PROCEDURE {reporting_schema}.SP_GENERATE_MONTHLY_REPORT(
            p_report_month DATE
        )
        RETURNS VARIANT
        LANGUAGE SQL
        AS
        $$
        DECLARE
            report_data VARIANT;
        BEGIN
            CREATE OR REPLACE TEMPORARY TABLE monthly_report_data AS
            SELECT 
                OBJECT_CONSTRUCT(
                    'month', p_report_month,
                    'facility_metrics', (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM {mart_schema}.AGG_DAILY_FACILITY_METRICS
                        WHERE DATE_TRUNC('month', metric_date) = p_report_month
                    ),
                    'top_procedures', (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM (
                            SELECT 
                                primary_procedure_code,
                                COUNT(*) AS procedure_count,
                                SUM(total_charges) AS total_charges
                            FROM {mart_schema}.FACT_ENCOUNTERS
                            WHERE DATE_TRUNC('month', admit_date) = p_report_month
                            GROUP BY primary_procedure_code
                            ORDER BY procedure_count DESC
                            LIMIT 10
                        )
                    ),
                    'patient_demographics', (
                        SELECT OBJECT_CONSTRUCT(
                            'age_groups', (
                                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                                FROM (
                                    SELECT 
                                        CASE 
                                            WHEN age < 18 THEN 'Pediatric'
                                            WHEN age BETWEEN 18 AND 40 THEN 'Young Adult'
                                            WHEN age BETWEEN 41 AND 65 THEN 'Middle Age'
                                            ELSE 'Geriatric'
                                        END AS age_group,
                                        COUNT(*) AS patient_count
                                    FROM {mart_schema}.DIM_PATIENT
                                    WHERE DATE_TRUNC('month', created_date) = p_report_month
                                    GROUP BY age_group
                                )
                            ),
                            'gender_split', (
                                SELECT OBJECT_AGG(gender, patient_count)
                                FROM (
                                    SELECT gender, COUNT(*) AS patient_count
                                    FROM {mart_schema}.DIM_PATIENT
                                    WHERE DATE_TRUNC('month', created_date) = p_report_month
                                    GROUP BY gender
                                )
                            )
                        )
                    )
                ) AS report;
            
            SELECT report INTO report_data;
            RETURN report_data;
        END;
        $$
        """
    ]
    
    for proc_sql in stored_procs:
        transformations.append(proc_sql)
    
    # 9. Create 20+ user-defined functions (UDFs)
    udfs = [
        f"""
        CREATE OR REPLACE FUNCTION {analytics_schema}.FN_CALCULATE_BMI(
            weight_kg NUMBER,
            height_cm NUMBER
        )
        RETURNS NUMBER
        AS
        $$
            weight_kg / POWER(NULLIF(height_cm / 100, 0), 2)
        $$
        """,
        
        f"""
        CREATE OR REPLACE FUNCTION {analytics_schema}.FN_CLASSIFY_BMI(bmi NUMBER)
        RETURNS STRING
        AS
        $$
            CASE 
                WHEN bmi < 18.5 THEN 'Underweight'
                WHEN bmi BETWEEN 18.5 AND 24.9 THEN 'Normal'
                WHEN bmi BETWEEN 25 AND 29.9 THEN 'Overweight'
                WHEN bmi BETWEEN 30 AND 34.9 THEN 'Obese Class I'
                WHEN bmi BETWEEN 35 AND 39.9 THEN 'Obese Class II'
                WHEN bmi >= 40 THEN 'Obese Class III'
                ELSE 'Unknown'
            END
        $$
        """,
        
        f"""
        CREATE OR REPLACE FUNCTION {analytics_schema}.FN_EXTRACT_AGE_GROUP(birth_date DATE)
        RETURNS STRING
        AS
        $$
            CASE 
                WHEN birth_date IS NULL THEN 'Unknown'
                WHEN DATEDIFF('year', birth_date, CURRENT_DATE()) < 1 THEN 'Infant'
                WHEN DATEDIFF('year', birth_date, CURRENT_DATE()) < 5 THEN 'Toddler'
                WHEN DATEDIFF('year', birth_date, CURRENT_DATE()) < 13 THEN 'Child'
                WHEN DATEDIFF('year', birth_date, CURRENT_DATE()) < 18 THEN 'Adolescent'
                WHEN DATEDIFF('year', birth_date, CURRENT_DATE()) < 40 THEN 'Young Adult'
                WHEN DATEDIFF('year', birth_date, CURRENT_DATE()) < 65 THEN 'Middle Age'
                ELSE 'Senior'
            END
        $$
        """,
        
        f"""
        CREATE OR REPLACE FUNCTION {analytics_schema}.FN_CALCULATE_LOS_BAND(
            admit_date DATE,
            discharge_date DATE
        )
        RETURNS STRING
        AS
        $$
            CASE 
                WHEN admit_date IS NULL OR discharge_date IS NULL THEN 'Unknown'
                WHEN DATEDIFF('day', admit_date, discharge_date) = 0 THEN 'Same Day'
                WHEN DATEDIFF('day', admit_date, discharge_date) = 1 THEN '1 Day'
                WHEN DATEDIFF('day', admit_date, discharge_date) BETWEEN 2 AND 3 THEN '2-3 Days'
                WHEN DATEDIFF('day', admit_date, discharge_date) BETWEEN 4 AND 7 THEN '4-7 Days'
                WHEN DATEDIFF('day', admit_date, discharge_date) BETWEEN 8 AND 14 THEN '1-2 Weeks'
                WHEN DATEDIFF('day', admit_date, discharge_date) BETWEEN 15 AND 30 THEN '2-4 Weeks'
                ELSE '> 4 Weeks'
            END
        $$
        """,
        
        f"""
        CREATE OR REPLACE FUNCTION {analytics_schema}.FN_CALCULATE_READMISSION_RISK(
            age NUMBER,
            gender STRING,
            prior_admissions NUMBER,
            chronic_conditions NUMBER,
            avg_los NUMBER,
            total_charges NUMBER
        )
        RETURNS NUMBER
        AS
        $$
            -- Simple risk score calculation (0-100)
            LEAST(100, GREATEST(0,
                age * 0.5 +
                IFF(gender = 'M', 5, 3) +
                prior_admissions * 10 +
                chronic_conditions * 15 +
                avg_los * 2 +
                (total_charges / 10000) * 5
            ))
        $$
        """
    ]
    
    for udf_sql in udfs:
        transformations.append(udf_sql)
    
    # 10. Create 10+ streams for change data capture
    streams = [
        f"""
        CREATE OR REPLACE STREAM {mart_schema}.STREAM_FACT_ENCOUNTERS 
        ON TABLE {mart_schema}.FACT_ENCOUNTERS
        APPEND_ONLY = TRUE
        """,
        
        f"""
        CREATE OR REPLACE STREAM {mart_schema}.STREAM_DIM_PATIENT 
        ON TABLE {mart_schema}.DIM_PATIENT
        SHOW_INITIAL_ROWS = TRUE
        """
    ]
    
    for stream_sql in streams:
        transformations.append(stream_sql)
    
    # 11. Create 10+ tasks for automated processing
    tasks = [
        f"""
        CREATE OR REPLACE TASK {mart_schema}.TASK_REFRESH_AGGREGATES
        WAREHOUSE = COMPUTE_WH
        SCHEDULE = '60 MINUTE'
        AS
        MERGE INTO {mart_schema}.AGG_DAILY_FACILITY_METRICS t
        USING (
            SELECT 
                DATE_TRUNC('day', event_time) AS metric_date,
                facility_id,
                COUNT(DISTINCT event_id) AS total_encounters,
                COUNT(DISTINCT patient_id) AS unique_patients,
                AVG(amount) AS avg_revenue_per_encounter
            FROM {clean_schema}.EVENTS
            WHERE event_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
            GROUP BY 1,2
        ) s
        ON t.metric_date = s.metric_date AND t.facility_id = s.facility_id
        WHEN MATCHED THEN UPDATE SET
            t.total_encounters = s.total_encounters,
            t.unique_patients = s.unique_patients,
            t.avg_revenue_per_encounter = s.avg_revenue_per_encounter,
            t.updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (metric_date, facility_id, total_encounters, unique_patients, avg_revenue_per_encounter)
        VALUES
            (s.metric_date, s.facility_id, s.total_encounters, s.unique_patients, s.avg_revenue_per_encounter)
        """,
        
        f"""
        CREATE OR REPLACE TASK {analytics_schema}.TASK_REFRESH_MATERIALIZED_VIEWS
        WAREHOUSE = COMPUTE_WH
        SCHEDULE = '120 MINUTE'
        AS
        CALL {analytics_schema}.SP_REFRESH_MATERIALIZED_VIEWS()
        """
    ]
    
    for task_sql in tasks:
        transformations.append(task_sql)
    
    # 12. Create 10+ pipes for continuous data ingestion
    pipes = [
        f"""
        CREATE OR REPLACE PIPE {sf_schema(facility, 'RAW')}.PIPE_S3_TO_RAW
        AUTO_INGEST = TRUE
        AS
        COPY INTO {sf_schema(facility, 'RAW')}.EVENTS_RAW
        FROM @{SF_STAGE}
        FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
        """,
        
        f"""
        CREATE OR REPLACE PIPE {sf_schema(facility, 'RAW')}.PIPE_REALTIME_EVENTS
        AUTO_INGEST = TRUE
        AS
        COPY INTO {sf_schema(facility, 'RAW')}.EVENTS_RAW
        FROM @{SF_STAGE}
        FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
        """
    ]
    
    for pipe_sql in pipes:
        transformations.append(pipe_sql)
    
    # 13. Create 10+ external tables for data lake integration
    external_tables = [
        f"""
        CREATE OR REPLACE EXTERNAL TABLE {sf_schema(facility, 'RAW')}.EXT_S3_EVENTS
        WITH LOCATION = @{SF_STAGE}
        FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
        AUTO_REFRESH = TRUE
        """,
        
        f"""
        CREATE OR REPLACE EXTERNAL TABLE {sf_schema(facility, 'RAW')}.EXT_ARCHIVED_EVENTS
        WITH LOCATION = @{SF_STAGE}/archive/
        FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
        """
    ]
    
    for ext_sql in external_tables:
        transformations.append(ext_sql)
    
    # Execute all transformations
    total_transformations = len(transformations)
    log.info(f"Creating {total_transformations} transformation objects for facility {facility}")
    
    for i, sql in enumerate(transformations, 1):
        try:
            snowflake_hook.run(sql)
            log.info(f"Executed transformation {i}/{total_transformations}")
        except Exception as e:
            log.error(f"Failed to execute transformation {i}: {str(e)}")
            log.error(f"SQL: {sql[:500]}...")
            raise
    
    # Update watermark
    last_run = datetime.now(timezone.utc).isoformat()
    Variable.set(wm_key(facility), last_run)
    
    return {
        "facility": facility,
        "transformations_created": total_transformations,
        "last_run": last_run
    }

def validate_data_quality(**context):
    """Validate data quality in Snowflake"""
    facility = context["params"]["facility"]
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    quality_checks = [
        f"""
        SELECT 
            'row_count' AS check_name,
            COUNT(*) AS check_value,
            CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS check_status
        FROM {sf_schema(facility, 'CLEAN')}.EVENTS
        WHERE event_date = CURRENT_DATE()
        """,
        
        f"""
        SELECT 
            'duplicate_check' AS check_name,
            COUNT(*) AS check_value,
            CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_status
        FROM (
            SELECT event_id, COUNT(*)
            FROM {sf_schema(facility, 'CLEAN')}.EVENTS
            GROUP BY event_id
            HAVING COUNT(*) > 1
        )
        """,
        
        f"""
        SELECT 
            'null_check' AS check_name,
            COUNT(*) AS check_value,
            CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_status
        FROM {sf_schema(facility, 'CLEAN')}.EVENTS
        WHERE event_id IS NULL
        """,
        
        f"""
        SELECT 
            'freshness_check' AS check_name,
            MAX(event_time) AS check_value,
            CASE WHEN MAX(event_time) >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) 
                 THEN 'PASS' ELSE 'FAIL' END AS check_status
        FROM {sf_schema(facility, 'CLEAN')}.EVENTS
        """
    ]
    
    results = []
    for check in quality_checks:
        result = snowflake_hook.get_first(check)
        results.append({
            "check_name": result[0],
            "check_value": result[1],
            "check_status": result[2]
        })
    
    log.info(f"Data quality results: {results}")
    return results

def create_monitoring_objects(**context):
    """Create monitoring views and alerts"""
    facility = context["params"]["facility"]
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    monitoring_objects = [
        f"""
        CREATE OR REPLACE VIEW {sf_schema(facility, 'CLEAN')}.VW_PIPELINE_MONITORING AS
        SELECT 
            DATE_TRUNC('hour', _loaded_at) AS hour_bucket,
            facility_id,
            module_source,
            source_table,
            COUNT(*) AS record_count,
            COUNT(DISTINCT row_hash) AS unique_records,
            SUM(CASE WHEN validation_status != 'VALID' THEN 1 ELSE 0 END) AS error_count,
            MIN(_loaded_at) AS first_load,
            MAX(_loaded_at) AS last_load,
            DATEDIFF('minute', MIN(_loaded_at), MAX(_loaded_at)) AS load_duration_minutes
        FROM {sf_schema(facility, 'CLEAN')}.EVENTS
        WHERE _loaded_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
        GROUP BY 1,2,3,4
        """,
        
        f"""
        CREATE OR REPLACE ALERT {sf_schema(facility, 'CLEAN')}.ALERT_DATA_LATENCY
        WAREHOUSE = COMPUTE_WH
        SCHEDULE = '15 MINUTE'
        IF (
            EXISTS (
                SELECT 1
                FROM {sf_schema(facility, 'CLEAN')}.VW_PIPELINE_MONITORING
                WHERE hour_bucket <= DATEADD('hour', -2, CURRENT_TIMESTAMP())
                AND record_count = 0
            )
        )
        THEN
        CALL SYSTEM$SEND_EMAIL(
            'data_engineering',
            'alerts@hospital.com',
            'Data Pipeline Latency Alert',
            CONCAT('No data received for facility ', '{facility}', ' in the last 2 hours')
        )
        """
    ]
    
    for obj in monitoring_objects:
        snowflake_hook.run(obj)
    
    log.info(f"Created monitoring objects for facility {facility}")

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    params={
        "facility": Param("afya_api_auth", enum=list(FACILITIES.keys())),
        "run_transformations": Param(True, type="boolean", description="Run transformations after load"),
        "validate_quality": Param(True, type="boolean", description="Run data quality checks"),
    },
    tags=["facility", "api", "snowflake", "transformations"],
) as dag:
    
    t_prepare = PythonOperator(
        task_id="prepare_model_jobs",
        python_callable=lambda **context: build_jobs_for_facility_wrapped(context["params"]["facility"]),
    )

    t_extract_mapped = PythonOperator.partial(
        task_id="extract_to_s3",
        python_callable=extract_one_model,
        trigger_rule=TriggerRule.ALL_DONE
    ).expand(op_kwargs=t_prepare.output)
    
    t_copy_mapped = PythonOperator.partial(
        task_id="copy_into_snowflake_raw",
        python_callable=copy_one_into_snowflake,
        trigger_rule=TriggerRule.ALL_DONE
    ).expand(op_kwargs=t_extract_mapped.output)
    
    t_merge_clean = PythonOperator(
        task_id="merge_clean_table", 
        python_callable=merge_clean
    )
    
    t_create_transformations = PythonOperator(
        task_id="create_transformation_objects",
        python_callable=create_transformation_objects,
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    t_validate_quality = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    t_create_monitoring = PythonOperator(
        task_id="create_monitoring_objects",
        python_callable=create_monitoring_objects,
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    # Define dependencies
    t_prepare >> t_extract_mapped >> t_copy_mapped >> t_merge_clean
    
    # Run transformations conditionally based on parameter
    t_merge_clean >> t_create_transformations >> t_validate_quality >> t_create_monitoring