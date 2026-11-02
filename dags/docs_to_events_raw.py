# dags/docs_to_events_raw.py
"""
docs_to_events_raw — bridge SIAYA_DOCS.EXTRACTED_FORMS → SIAYA_RAW.EVENTS_RAW

Writes two V3-shaped EVENTS_RAW rows per extracted record:
  source_table='patients', module_source='reception-service'
  source_table='visits',   module_source='reception-service'

snowflake_to_v3_migration.discover_tables() reads module_source from EVENTS_RAW
and routes reception-service rows through v3_passthrough automatically.
v3_passthrough handles: name splitting (last_name → first+last), patient_no
generation (MIG-SIAYA-{id}), visit_no generation, FK resolution, clinic fill.

Zero changes to v3_passthrough, snowflake_to_v3_migration, or v2_to_v3_api_migration.

Run order: gdrive_landingai_extract → THIS DAG → snowflake_flatten_clean → snowflake_to_v3_migration
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, task

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipelines_common import use_pipelines_dir

log = logging.getLogger(__name__)

DAG_ID = "docs_to_events_raw"
FACILITY = "siaya"
MODULE_SOURCE = "reception-service"
SRC = "HOSPITALS.SIAYA_DOCS.EXTRACTED_FORMS"
DST = "HOSPITALS.SIAYA_RAW.EVENTS_RAW"

# Delete previously bridged rows for whatever files are currently in EXTRACTED_FORMS.
# file_id is embedded in both patient and visit payloads, so one DELETE covers both.
DELETE_SQL = f"""
    DELETE FROM {DST}
    WHERE facility_id = '{FACILITY}'
      AND module_source = '{MODULE_SOURCE}'
      AND source_table IN ('patients', 'visits')
      AND payload:file_id::STRING IN (SELECT FILE_ID FROM {SRC})
"""

# Full name goes in last_name; v3_passthrough._patient_names() splits it → first + last.
# patient_no auto-filled as MIG-SIAYA-{{id}} if both file number columns are NULL.
PATIENT_INSERT_SQL = f"""
    INSERT INTO {DST} (facility_id, ingested_at, module_source, source_table, namespace, payload)
    SELECT
        '{FACILITY}',
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
            'age',           AGE,
            'sex',           SEX,
            'phone',         PHONE_NUMBER,
            'address',       ADDRESS
        )
    FROM {SRC}
"""

# patient_id = RECORD_KEY links to the patient row above.
# v3_passthrough holds this row back until the patient is confirmed in V3 (HOLD_IF_MISSING).
# visit_no auto-filled as SIAYA-org{org_id}-VST-{id}.
# clinic auto-filled from facility_id.
VISIT_INSERT_SQL = f"""
    INSERT INTO {DST} (facility_id, ingested_at, module_source, source_table, namespace, payload)
    SELECT
        '{FACILITY}',
        CURRENT_TIMESTAMP(),
        '{MODULE_SOURCE}',
        'visits',
        NULL,
        OBJECT_CONSTRUCT(
            'id',         RECORD_KEY || '_v',
            'file_id',    FILE_ID,
            'patient_id', RECORD_KEY,
            'date',       VISIT_DATE,
            'notes',      NURSES_TRIAGE_NOTES
        )
    FROM {SRC}
"""


def check_null_names(cur) -> int:
    """Return count of rows with NULL PATIENT_NAME and log a warning if any.

    NULL names → v3_passthrough holds the patient back (first_name required);
    the visit is also held back until its patient_id resolves.
    """
    row = cur.execute(f"SELECT COUNT(*) FROM {SRC} WHERE PATIENT_NAME IS NULL").fetchone()
    count = int(row[0]) if row else 0
    if count:
        log.warning(
            "%d row(s) in %s have NULL PATIENT_NAME. Those patient records will be "
            "held back by v3_passthrough (first_name NOT NULL). Their visit rows will "
            "also be held back until the patient lands in V3.",
            count, SRC,
        )
    return count


def run_validate(cur) -> int:
    """Return total row count; raise AirflowFailException if EXTRACTED_FORMS is empty."""
    row = cur.execute(f"SELECT COUNT(*) FROM {SRC}").fetchone()
    count = int(row[0]) if row else 0
    if not count:
        raise AirflowFailException(f"{SRC} is empty — run gdrive_landingai_extract first.")
    check_null_names(cur)
    log.info("%s: %d row(s) to process", SRC, count)
    return count


def run_write(cur) -> dict:
    """DELETE stale rows then INSERT patient + visit rows. Returns row counts."""
    cur.execute(DELETE_SQL)
    deleted = cur.rowcount
    cur.execute(PATIENT_INSERT_SQL)
    patients = cur.rowcount
    cur.execute(VISIT_INSERT_SQL)
    visits = cur.rowcount
    log.info(
        "Deleted %d stale row(s); inserted %d patient + %d visit row(s) into %s",
        deleted, patients, visits, DST,
    )
    return {"deleted": deleted, "patients": patients, "visits": visits}


@dag(
    dag_id=DAG_ID,
    description="Bridge: SIAYA_DOCS.EXTRACTED_FORMS → SIAYA_RAW.EVENTS_RAW (V3-shaped patient + visit rows)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["snowflake", "documents", "migration"],
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=2)},
)
def docs_to_events_raw():

    @task
    def validate() -> int:
        use_pipelines_dir()
        import flatten_jsons_schemas as fl
        conn = fl._snowflake_connect()
        try:
            return run_validate(conn.cursor())
        finally:
            conn.close()

    @task
    def write(row_count: int) -> dict:
        use_pipelines_dir()
        import flatten_jsons_schemas as fl
        conn = fl._snowflake_connect()
        try:
            return run_write(conn.cursor())
        finally:
            conn.close()

    write(validate())


docs_to_events_raw()
