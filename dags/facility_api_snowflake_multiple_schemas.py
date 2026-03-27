# dags/facility_api_to_snowflake.py
from __future__ import annotations
import gzip
from io import BytesIO
import json
import time
import inflect
import logging
import requests
from requests.exceptions import Timeout, ConnectionError, HTTPError
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
log = logging.getLogger(__name__)
p = inflect.engine()

DAG_ID = "facility_api_to_snowflake"

# facility -> api params
FACILITIES = {
    "afya_api_auth": {"base_url": "https://staging.afyanalytics.ai", "db": "staging_db"},
    "kakamega": {"base_url":"https://demo.collabmed.net", "db":"kakamega_db"},
    "kisumu": {"base_url": "https://kshospital.collabmed.net", "db":"kisumu_db"},
    "lodwar": {"base_url": "https://lcrh.collabmed.net", "db":"lodwar_db"},
    "tenri": {"base_url": "https://stageenv.collabmed.net", "db":"tenri_db"},
    "xanalife": {"base_url": "https://xanalife.afyanalytics.ai/", "db":"xanalife_db"}
}

S3_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"

S3_BUCKET = "collabmedbucket"
S3_PREFIX = "raw/facilities"  # s3://bucket/raw/facilities/facility_id=.../dt=.../*.jsonl

SF_DB = "HOSPITALS" 
SF_SHARED_SCHEMA = "SHARED"
SF_STAGE = f"{SF_DB}.{SF_SHARED_SCHEMA}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT = f"{SF_DB}.{SF_SHARED_SCHEMA}.JSON_FF"

def get_gsheet_client():
    # Store service account JSON in Airflow Variable GOOGLE_SA_JSON
    creds_dict = json.loads(Variable.get("GOOGLE_SA_JSON"))
    return gspread.service_account_from_dict(creds_dict)

def read_dictionary_sheet(spreadsheet_id: str, worksheet_name: str):
    gc = get_gsheet_client()
    ws = gc.open_by_key(spreadsheet_id).worksheet(worksheet_name)
    return ws.get_all_records()  # list[dict]

def snake_to_pascal(s: str) -> str:
    return "".join(w.capitalize() for w in re.split(r"[_\s]+", s.strip()) if w)

def build_namespace(module: str, table: str) -> str:
    # Example: module=Core, table=core_approvals -> Ignite\Core\Entities\Approvals
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
    jobs = build_jobs_for_facility(facility)  # your existing function returning list[dict]
    return [{"job": j} for j in jobs]

def _safe_s3_token(s: str) -> str:
    """
    Make a string safe for S3 key path segments (Snowflake FILES= also prefers simple paths).
    Keeps alnum, dash, underscore, equals, dot. Everything else -> underscore.
    """
    s = (s or "").strip()
    return re.sub(r"[^a-zA-Z0-9_\-=\.\+]+", "_", s)


def post_with_retry_and_fallback(
    url,
    headers,
    bodies,  # list of candidate bodies in fallback order
    timeout=60,
    max_retries=6,
    default_retry_wait=10,
    backoff_factor=2,
    base_delay=0.5,  # small polite delay after successful requests
):
    """
    Tries multiple request bodies in order.
    Handles:
      - 404 by moving to next fallback body
      - 429 by waiting and retrying same body
      - 5xx by retrying same body
      - Timeout / ConnectionError by retrying same body

    Returns:
      (response, used_body)

    Raises:
      Exception if all fallback bodies return 404
      or the final retryable error exhausts retries.
    """

    for body_index, base_body in enumerate(bodies):
        attempt = 0
        wait_time = default_retry_wait

        while True:
            attempt += 1

            try:
                r = requests.post(
                    url=url,
                    headers=headers,
                    json=base_body,
                    timeout=timeout,
                )

                log.info(
                    "BodyIndex=%s Attempt=%s Status=%s Response=%s",
                    body_index,
                    attempt,
                    r.status_code,
                    r.text[:800],
                )

                # 404 => try next fallback body
                if r.status_code == 404:
                    log.warning(
                        "404 for namespace=%s; trying next fallback body",
                        base_body.get("namespace"),
                    )
                    break

                # 429 => respect retry_after_seconds and retry same body
                if r.status_code == 429:
                    retry_after = default_retry_wait
                    try:
                        retry_after = int(
                            r.json().get("retry_after_seconds", default_retry_wait)
                        )
                    except Exception:
                        pass

                    if attempt >= max_retries:
                        r.raise_for_status()

                    log.warning(
                        "429 rate limited for namespace=%s. Sleeping %s seconds before retry (%s/%s)",
                        base_body.get("namespace"),
                        retry_after,
                        attempt,
                        max_retries,
                    )
                    time.sleep(retry_after)
                    continue

                # 5xx => retry same body
                if r.status_code in {500, 502, 503, 504}:
                    log.warning(
                        "Server error %s for namespace=%s",
                        r.status_code,
                        base_body.get("namespace"),
                    )

                    if attempt >= max_retries:
                        r.raise_for_status()

                    time.sleep(wait_time)
                    wait_time *= backoff_factor
                    continue

                r.raise_for_status()

                if base_delay > 0:
                    time.sleep(base_delay)

                return r, base_body

            except (Timeout, ConnectionError) as e:
                if attempt >= max_retries:
                    raise

                log.warning(
                    "Network error for namespace=%s: %s. Sleeping %s seconds before retry (%s/%s)",
                    base_body.get("namespace"),
                    str(e),
                    wait_time,
                    attempt,
                    max_retries,
                )
                time.sleep(wait_time)
                wait_time *= backoff_factor

            except HTTPError:
                raise

    raise Exception("All fallback request bodies returned 404")


def extract_all_pages(
    url,
    headers,
    body,
    singular_body,
    double_namespace_body,
    double_namespace_singular_body,
    timeout=60,
    max_pages=10000,
    max_retries=6,
    default_retry_wait=10,
    backoff_factor=2,
    base_delay=0.5,
):
    """
    Returns:
      (all_rows, used_body)
        - all_rows: list of records across all pages
        - used_body: the request body shape that actually worked
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

    candidate_bodies = [
        {**body, "page": page},
        {**singular_body, "page": page},
        {**double_namespace_body, "page": page},
        {**double_namespace_singular_body, "page": page},
    ]

    # First page with fallback sequence built in
    r, chosen_body = post_with_retry_and_fallback(
        url=url,
        headers=headers,
        bodies=candidate_bodies,
        timeout=timeout,
        max_retries=max_retries,
        default_retry_wait=default_retry_wait,
        backoff_factor=backoff_factor,
        base_delay=base_delay,
    )

    payload = r.json()
    all_rows = extract_rows(payload)

    pagination = payload.get("pagination") or {}
    has_more = bool(pagination.get("has_more_pages", False))
    last_page = pagination.get("last_page")

    # Remaining pages use the chosen body only
    while has_more:
        page += 1

        if page > max_pages:
            log.info("Pagination safety stop: exceeded max_pages=%s", max_pages)
            break

        if last_page is not None and page > int(last_page):
            break

        next_body = {**chosen_body, "page": page}

        r, chosen_body = post_with_retry_and_fallback(
            url=url,
            headers=headers,
            bodies=[next_body],
            timeout=timeout,
            max_retries=max_retries,
            default_retry_wait=default_retry_wait,
            backoff_factor=backoff_factor,
            base_delay=base_delay,
        )

        payload = r.json()
        rows = extract_rows(payload)
        all_rows.extend(rows)

        pagination = payload.get("pagination") or {}
        has_more = bool(pagination.get("has_more_pages", False))
        last_page = pagination.get("last_page", last_page)

        # extra guard in case pagination lies
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
    singular_body =  {
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
    

    rows = extract_all_pages(url=url,headers=headers, body=body, 
                             singular_body=singular_body, 
                             double_namespace_body=double_namespace_body,
                             double_namespace_singular_body=double_namespace_singular_body,
                             timeout=60, max_pages=10000)

    
    # ✅ S3 key per model namespace
    ingested_at = datetime.now(timezone.utc)
    dt = ingested_at.date().isoformat()

    # Safe key segments
    ns_safe = _safe_s3_token(job["namespace"].replace("\\", "_"))
    module_safe = _safe_s3_token(job.get("module", ""))
    table_safe = _safe_s3_token(job.get("table", ""))

    # Recommended: include module/table/namespace partitions for easy Snowflake COPY + lineage
    # Example:
    # raw/facilities/facility_id=afya_api_auth/module=Finance/table=waivers/namespace=Ignite_Finance_Entities_Waiver/dt=2026-02-25/run_id=...jsonl.gz
    run_id = context["run_id"]
    key_prefix = (
        f"{S3_PREFIX}/"
        f"facility_id={facility}/"
        f"module={module_safe or 'unknown'}/"
        f"table={table_safe or 'unknown'}/"
        f"namespace={ns_safe}/"
        f"dt={dt}/"
    )

    # Serialize to JSONL
    jsonl = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows)
    if jsonl:
        jsonl += "\n"

    # ---- Choose ONE: plain JSONL OR gzip JSONL ----
    use_gzip = True

    s3 = S3Hook(aws_conn_id=S3_CONN_ID)

    if use_gzip:
        key = f"{key_prefix}{run_id}.jsonl.gz"
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
        key = f"{key_prefix}{run_id}.jsonl"
        s3.load_string(
            string_data=jsonl,
            key=key,
            bucket_name=S3_BUCKET,
            replace=True,
        )

    log.info("Uploaded to s3://%s/%s rows=%s", S3_BUCKET, key, len(rows))

    # ✅ Return everything Snowflake needs later
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

    # 🔥 Extract properly from nested structure
    token = data.get("success", {}).get("token")

    if not token:
        raise Exception("Token not found in response")

    return token

def sf_schema(facility: str, layer: str) -> str:
    # layer = "RAW" or "CLEAN"
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

    raw_table = f"{sf_schema(facility, 'RAW')}.EVENTS_RAW"

    sql = f"""
    COPY INTO {raw_table} (facility_id, ingested_at, module_source, source_table, namespace, payload)
    FROM (
      SELECT
        '{facility}'::STRING AS facility_id,
        '{ingested_at}'::TIMESTAMP_TZ AS ingested_at,
        '{module_source}'::STRING AS module_source,
        '{source_table}'::STRING AS source_table,
        '{namespace}'::STRING AS namespace,
        PARSE_JSON($1) AS payload
      FROM @{SF_STAGE}
    )
    FILES = ('{s3_key}')
    FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
    ON_ERROR = 'CONTINUE';
    """
    SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).run(sql)

def merge_clean(**context):
    facility = context["params"]["facility"]

    raw_table = f"{sf_schema(facility, 'RAW')}.EVENTS_RAW"
    clean_table = f"{sf_schema(facility, 'CLEAN')}.EVENTS"

    sql = f"""
    MERGE INTO {clean_table} AS t
    USING (
        SELECT
            facility_id,
            f.value:id::STRING            AS event_id,
            f.value:event_time::TIMESTAMP AS event_time,
            f.value:type::STRING          AS event_type,
            f.value:amount::NUMBER        AS amount,
            f.value                       AS payload,
            ingested_at
        FROM {raw_table} AS r,
            LATERAL FLATTEN(input => r.payload) AS f
        WHERE f.value:id IS NOT NULL
        AND NULLIF(TRIM(f.value:id::STRING), '') IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY f.value:id::STRING
            ORDER BY ingested_at DESC
        ) = 1
    ) AS s
    ON t.event_id = s.event_id
    WHEN MATCHED THEN UPDATE SET
        event_time  = s.event_time,
        event_type  = s.event_type,
        amount      = s.amount,
        payload     = s.payload,
        ingested_at = s.ingested_at
    WHEN NOT MATCHED THEN INSERT (
        event_id,
        event_time,
        event_type,
        amount,
        payload,
        ingested_at
    )
    VALUES (
        s.event_id,
        s.event_time,
        s.event_type,
        s.amount,
        s.payload,
        s.ingested_at
    );
    """
    SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).run(sql)    

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    params={
        "facility": Param("afya_api_auth", enum=list(FACILITIES.keys())),
    },
    tags=["facility", "api", "snowflake"],
) as dag:
    
    t_prepare = PythonOperator(
        task_id="prepare_model_jobs",
        python_callable=lambda **context: build_jobs_for_facility_wrapped(context["params"]["facility"]),
    )

    t_extract_mapped = PythonOperator.partial(
        task_id="extract_to_s3", #loop here to maap out each table
        python_callable=extract_one_model,
        trigger_rule=TriggerRule.ALL_DONE
    ).expand(op_kwargs=t_prepare.output)
    t_copy_mapped = PythonOperator.partial(
        task_id="copy_into_snowflake_raw",
        python_callable=copy_one_into_snowflake,
        trigger_rule=TriggerRule.ALL_DONE
    ).expand(op_kwargs=t_extract_mapped.output)
    t3 = PythonOperator(task_id="merge_clean_table", python_callable=merge_clean)

    t_prepare >> t_extract_mapped >> t_copy_mapped >> t3