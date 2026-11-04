# dags/docs_to_events_raw.py
"""
docs_to_events_raw — bridge {facility}_DOCS.EXTRACTED_FORMS → {facility}_RAW.EVENTS_RAW

Writes two V3-shaped EVENTS_RAW rows per extracted record:
  source_table='patients', module_source='reception-service'
  source_table='visits',   module_source='reception-service'

Requires 'facility' in DAG conf (e.g. {"facility": "siaya"}).
Derives source/destination schemas from that key — no code changes per new facility.

snowflake_to_v3_migration.discover_tables() reads module_source from EVENTS_RAW
and routes reception-service rows through v3_passthrough automatically.
v3_passthrough handles: name splitting (last_name → first+last), patient_no
generation (MIG-{FACILITY}-{id}), visit_no generation, FK resolution, clinic fill.

Zero changes to v3_passthrough, snowflake_to_v3_migration, or v2_to_v3_api_migration.

Run order: gdrive_landingai_extract → THIS DAG → snowflake_flatten_clean → snowflake_to_v3_migration
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipelines_common import use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "docs_to_events_raw"
MODULE_SOURCE = "reception-service"


def _require_facility(facility: str) -> None:
    if not facility or not facility.replace('-', '').replace('_', '').isalnum():
        raise AirflowFailException(
            "docs_to_events_raw requires 'facility' in DAG conf "
            "(e.g. {\"facility\": \"siaya\"}). Got: " + repr(facility)
        )


def run_setup(cur, facility: str, dst: str) -> None:
    """Create source and destination schemas and EVENTS_RAW table if they do not exist."""
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS HOSPITALS.{facility.upper()}_DOCS")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS HOSPITALS.{facility.upper()}_RAW")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {dst} (
            facility_id    VARCHAR,
            ingested_at    TIMESTAMP_TZ,
            module_source  VARCHAR,
            source_table   VARCHAR,
            namespace      VARCHAR,
            payload        VARIANT
        )
    """)


def check_null_names(cur, src: str) -> int:
    """Return count of rows with NULL PATIENT_NAME and log a warning if any.

    NULL names → v3_passthrough holds the patient back (first_name required);
    the visit is also held back until its patient_id resolves.
    """
    row = cur.execute(f"SELECT COUNT(*) FROM {src} WHERE PATIENT_NAME IS NULL").fetchone()
    count = int(row[0]) if row else 0
    if count:
        log.warning(
            "%d row(s) in %s have NULL PATIENT_NAME. Those patient records will be "
            "held back by v3_passthrough (first_name NOT NULL). Their visit rows will "
            "also be held back until the patient lands in V3.",
            count, src,
        )
    return count


def run_validate(cur, src: str, facility: str) -> int:
    """Return total row count; raise AirflowFailException if EXTRACTED_FORMS is empty."""
    try:
        row = cur.execute(f"SELECT COUNT(*) FROM {src}").fetchone()
    except Exception as e:
        msg = str(e)
        if "does not exist" in msg or "not authorized" in msg:
            raise AirflowFailException(
                f"{src} does not exist or is not accessible. "
                f"Step 1 (gdrive_landingai_extract) must run successfully before this step. "
                f"Check: (1) step 1 completed without error, (2) its conf included "
                f"push_to_snowflake=true, snowflake_schema={facility.upper()}_DOCS, snowflake_table=EXTRACTED_FORMS."
            ) from e
        raise
    count = int(row[0]) if row else 0
    if not count:
        raise AirflowFailException(
            f"{src} is empty. gdrive_landingai_extract must run first and write records here. "
            f"If it ran but this table is still empty, check: (1) the GDRIVE_SOURCE_FOLDER_ID "
            f"Airflow Variable points at the correct {facility} PDF folder, and (2) the DAG conf "
            f"passed snowflake_schema={facility.upper()}_DOCS and snowflake_table=EXTRACTED_FORMS."
        )
    check_null_names(cur, src)
    log.info("%s: %d row(s) to process", src, count)
    return count


def run_write(cur, src: str, dst: str, facility: str) -> dict:
    """DELETE stale rows then INSERT patient + visit rows atomically. Returns row counts."""
    delete_sql = f"""
        DELETE FROM {dst}
        WHERE facility_id = '{facility}'
          AND module_source = '{MODULE_SOURCE}'
          AND source_table IN ('patients', 'visits')
          AND payload:file_id::STRING IN (SELECT FILE_ID FROM {src})
    """
    # Full name goes in last_name; v3_passthrough._patient_names() splits it → first + last.
    patient_insert_sql = f"""
        INSERT INTO {dst} (facility_id, ingested_at, module_source, source_table, namespace, payload)
        SELECT
            '{facility}',
            CURRENT_TIMESTAMP(),
            '{MODULE_SOURCE}',
            'patients',
            NULL,
            OBJECT_CONSTRUCT(
                'id',            RECORD_KEY,
                'file_id',       FILE_ID,
                'last_name',     PATIENT_NAME,
                'patient_no',    COALESCE(CAMP_FILE_NUMBER, PATIENT_ID_FILE_NO),
                'date_of_birth', DATE_OF_BIRTH,
                'age',           TRY_CAST(SPLIT_PART(AGE, ' ', 1) AS INTEGER),
                'gender',        SEX,
                'phone',         PHONE_NUMBER,
                'address',       ADDRESS
            )
        FROM {src}
    """
    # patient_id = RECORD_KEY links to the patient row above.
    # v3_passthrough holds this row back until the patient is confirmed in V3 (HOLD_IF_MISSING).
    visit_insert_sql = f"""
        INSERT INTO {dst} (facility_id, ingested_at, module_source, source_table, namespace, payload)
        SELECT
            '{facility}',
            CURRENT_TIMESTAMP(),
            '{MODULE_SOURCE}',
            'visits',
            NULL,
            OBJECT_CONSTRUCT(
                'id',         RECORD_KEY || '_v',
                'file_id',    FILE_ID,
                'patient_id', RECORD_KEY,
                'date',       VISIT_DATE,
                'notes',      COALESCE(NURSES_TRIAGE_NOTES, HISTORY_OF_PRESENT_ILLNESS, DOCTORS_NOTES)
            )
        FROM {src}
    """
    cur.execute("BEGIN")
    try:
        cur.execute(delete_sql)
        deleted = cur.rowcount
        cur.execute(patient_insert_sql)
        patients = cur.rowcount
        cur.execute(visit_insert_sql)
        visits = cur.rowcount
        if patients == 0 or visits == 0:
            cur.execute("ROLLBACK")
            raise AirflowFailException(
                f"INSERT produced 0 rows: patients={patients}, visits={visits}. "
                f"{src} had rows but none transferred — check column mapping between "
                f"EXTRACTED_FORMS and {dst}."
            )
        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise
    log.info(
        "Deleted %d stale row(s); inserted %d patient + %d visit row(s) into %s",
        deleted, patients, visits, dst,
    )
    return {"deleted": deleted, "patients": patients, "visits": visits}


def run_verify(cur, dst: str, facility: str) -> dict:
    """Confirm patients and visits rows landed in EVENTS_RAW with the right module_source.

    Raises if either table is missing — that would mean the INSERT succeeded
    but the rows are unreadable by downstream steps.
    If this passes, the data is correctly staged for snowflake_flatten_clean and
    snowflake_to_v3_migration. If those steps succeed but records don't appear in V3,
    the next thing to check is that the V3 gateway catalog has 'reception/patients'
    and 'reception/visits' registered — that is outside this pipeline's control.
    """
    verify_sql = f"""
        SELECT source_table, COUNT(*) AS n
        FROM {dst}
        WHERE facility_id = '{facility}'
          AND module_source = '{MODULE_SOURCE}'
          AND source_table IN ('patients', 'visits')
        GROUP BY source_table
    """
    rows = cur.execute(verify_sql).fetchall()
    counts = {r[0]: int(r[1]) for r in rows}
    missing = [t for t in ("patients", "visits") if counts.get(t, 0) == 0]
    if missing:
        raise AirflowFailException(
            f"{dst} has no rows for source_table={missing} with "
            f"module_source='{MODULE_SOURCE}' after the write step. "
            f"The INSERT reported success but rows are not visible — "
            f"check Snowflake session isolation or transaction commit."
        )
    log.info(
        "%s: verified %d patient row(s) and %d visit row(s) staged "
        "with module_source='%s' for snowflake_to_v3_migration.",
        dst, counts["patients"], counts["visits"], MODULE_SOURCE,
    )
    return counts


@dag(
    dag_id=DAG_ID,
    description="Bridge: {facility}_DOCS.EXTRACTED_FORMS → {facility}_RAW.EVENTS_RAW (facility from conf)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["snowflake", "documents", "migration"],
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=2)},
    params={"facility": Param("", type="string", title="Facility key", description='e.g. "siaya"')},
)
def docs_to_events_raw():

    @task
    def validate() -> int:
        ctx = get_current_context()
        facility = ctx['dag_run'].conf.get('facility', '').strip().lower()
        _require_facility(facility)
        src = f"HOSPITALS.{facility.upper()}_DOCS.EXTRACTED_FORMS"
        dst = f"HOSPITALS.{facility.upper()}_RAW.EVENTS_RAW"
        use_pipelines_dir()
        import flatten_jsons_schemas as fl
        conn = fl._snowflake_connect()
        try:
            cur = conn.cursor()
            run_setup(cur, facility, dst)
            return run_validate(cur, src, facility)
        finally:
            conn.close()

    @task
    def write(row_count: int) -> dict:
        ctx = get_current_context()
        facility = ctx['dag_run'].conf.get('facility', '').strip().lower()
        _require_facility(facility)
        src = f"HOSPITALS.{facility.upper()}_DOCS.EXTRACTED_FORMS"
        dst = f"HOSPITALS.{facility.upper()}_RAW.EVENTS_RAW"
        use_pipelines_dir()
        import flatten_jsons_schemas as fl
        conn = fl._snowflake_connect()
        try:
            cur = conn.cursor()
            run_setup(cur, facility, dst)
            return run_write(cur, src, dst, facility)
        finally:
            conn.close()

    @task
    def verify(counts: dict) -> None:
        ctx = get_current_context()
        facility = ctx['dag_run'].conf.get('facility', '').strip().lower()
        _require_facility(facility)
        dst = f"HOSPITALS.{facility.upper()}_RAW.EVENTS_RAW"
        use_pipelines_dir()
        import flatten_jsons_schemas as fl
        conn = fl._snowflake_connect()
        try:
            run_verify(conn.cursor(), dst, facility)
        finally:
            conn.close()

    verify(write(validate()))


docs_to_events_raw()
