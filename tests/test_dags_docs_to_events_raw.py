"""
dags/docs_to_events_raw.py unit tests.

Tests cover the module-level helpers (check_null_names, run_setup, run_validate,
run_write, run_verify, _require_facility) via a fake cursor — no Snowflake connection required.

Integration tests (real Snowflake) live on the server.
"""
from __future__ import annotations

import pytest
from airflow.exceptions import AirflowFailException

from tests.helpers import load_dag_module

dag = load_dag_module("docs_to_events_raw")

FACILITY = "siaya"
SRC = "HOSPITALS.SIAYA_DOCS.EXTRACTED_FORMS"
DST = "HOSPITALS.SIAYA_RAW.EVENTS_RAW"


# ---------------------------------------------------------------------------
# Fake cursor
# ---------------------------------------------------------------------------

class FakeCursor:
    """Records every SQL string passed to execute(); supports configurable
    fetchone() results keyed by a substring of the SQL."""

    def __init__(self, fetchone_map: dict | None = None, rowcount: int = 5,
                 fail_on: str | None = None):
        self._fetchone_map = fetchone_map or {}
        self.rowcount = rowcount
        self.fail_on = fail_on
        self.executed: list[str] = []

    def execute(self, sql, params=None):
        self.executed.append(sql.strip())
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError(f"Simulated failure on: {self.fail_on}")
        return self

    def fetchone(self):
        for key, value in self._fetchone_map.items():
            if any(key in s for s in self.executed[-1:]):
                return value
        return (0,)


# ---------------------------------------------------------------------------
# _require_facility
# ---------------------------------------------------------------------------

def test_require_facility_raises_on_empty_string():
    with pytest.raises(AirflowFailException, match="requires 'facility'"):
        dag._require_facility('')


def test_require_facility_raises_on_whitespace():
    with pytest.raises(AirflowFailException, match="requires 'facility'"):
        dag._require_facility('   ')


def test_require_facility_passes_for_valid_key():
    dag._require_facility('siaya')
    dag._require_facility('kisumu-v3')
    dag._require_facility('facility_name_01')


# ---------------------------------------------------------------------------
# check_null_names
# ---------------------------------------------------------------------------

def test_check_null_names_returns_zero_and_no_warning(caplog):
    cur = FakeCursor(fetchone_map={"PATIENT_NAME IS NULL": (0,)})
    with caplog.at_level("WARNING"):
        result = dag.check_null_names(cur, SRC)
    assert result == 0
    assert "PATIENT_NAME" not in caplog.text


def test_check_null_names_logs_warning_when_nulls_present(caplog):
    cur = FakeCursor(fetchone_map={"PATIENT_NAME IS NULL": (3,)})
    with caplog.at_level("WARNING"):
        result = dag.check_null_names(cur, SRC)
    assert result == 3
    assert "3 row(s)" in caplog.text
    assert "held back" in caplog.text


# ---------------------------------------------------------------------------
# run_setup
# ---------------------------------------------------------------------------

def test_run_setup_creates_both_schemas_and_events_raw_table():
    cur = FakeCursor()
    dag.run_setup(cur, FACILITY, DST)
    assert any("SIAYA_DOCS" in s for s in cur.executed)
    assert any("SIAYA_RAW" in s for s in cur.executed)
    assert any("EVENTS_RAW" in s and "CREATE TABLE" in s for s in cur.executed)
    assert all("IF NOT EXISTS" in s for s in cur.executed)


def test_run_setup_uses_facility_key_for_schema_names():
    cur = FakeCursor()
    dag.run_setup(cur, 'kisumu_v3', 'HOSPITALS.KISUMU_V3_RAW.EVENTS_RAW')
    assert any("KISUMU_V3_DOCS" in s for s in cur.executed)
    assert any("KISUMU_V3_RAW" in s for s in cur.executed)


# ---------------------------------------------------------------------------
# run_validate
# ---------------------------------------------------------------------------

def test_run_validate_raises_when_empty():
    cur = FakeCursor(fetchone_map={"COUNT(*)": (0,)})
    with pytest.raises(Exception, match="GDRIVE_SOURCE_FOLDER_ID"):
        dag.run_validate(cur, SRC, FACILITY)


def test_run_validate_raises_when_table_does_not_exist():
    class MissingTableCursor(FakeCursor):
        def execute(self, sql, params=None):
            if "COUNT(*)" in sql:
                raise RuntimeError("SQL compilation error: Object 'HOSPITALS.SIAYA_DOCS.EXTRACTED_FORMS' does not exist or not authorized")
            return super().execute(sql, params)
    with pytest.raises(Exception, match="does not exist"):
        dag.run_validate(MissingTableCursor(), SRC, FACILITY)


def test_run_validate_returns_count():
    cur = FakeCursor(fetchone_map={"PATIENT_NAME IS NULL": (0,), "COUNT(*)": (10,)})
    assert dag.run_validate(cur, SRC, FACILITY) == 10


def test_run_validate_calls_check_null_names(monkeypatch):
    calls = []
    monkeypatch.setattr(dag, "check_null_names", lambda cur, src: calls.append(True) or 0)
    cur = FakeCursor(fetchone_map={"COUNT(*)": (4,)})
    dag.run_validate(cur, SRC, FACILITY)
    assert calls == [True]


# ---------------------------------------------------------------------------
# run_write — SQL order and content
# ---------------------------------------------------------------------------

def test_run_write_returns_counts():
    cur = FakeCursor(rowcount=7)
    result = dag.run_write(cur, SRC, DST, FACILITY)
    assert result == {"deleted": 7, "patients": 7, "visits": 7}


def test_run_write_delete_before_inserts():
    cur = FakeCursor()
    dag.run_write(cur, SRC, DST, FACILITY)
    # BEGIN, DELETE, patient INSERT, visit INSERT, COMMIT
    assert cur.executed[0] == "BEGIN"
    assert "DELETE" in cur.executed[1]
    assert "'patients'" in cur.executed[2]
    assert "'visits'" in cur.executed[3]
    assert cur.executed[4] == "COMMIT"


def test_run_write_rolls_back_on_failure():
    cur = FakeCursor(fail_on="NURSES_TRIAGE_NOTES")  # unique to visit INSERT
    with pytest.raises(RuntimeError):
        dag.run_write(cur, SRC, DST, FACILITY)
    assert "ROLLBACK" in cur.executed
    assert "COMMIT" not in cur.executed


def test_run_write_delete_filters_by_file_id():
    cur = FakeCursor()
    dag.run_write(cur, SRC, DST, FACILITY)
    delete_sql = cur.executed[1]   # executed[0] = BEGIN
    assert "payload:file_id::STRING" in delete_sql
    assert "module_source = 'reception-service'" in delete_sql
    assert "source_table IN ('patients', 'visits')" in delete_sql


def test_run_write_patient_payload_fields():
    cur = FakeCursor()
    dag.run_write(cur, SRC, DST, FACILITY)
    patient_sql = cur.executed[2]  # BEGIN, DELETE, patient INSERT
    for field in ("'id'", "'file_id'", "'last_name'", "'patient_no'", "'date_of_birth'",
                  "'age'", "'gender'", "'phone'", "'address'"):
        assert field in patient_sql, f"Missing field {field} in patient INSERT"
    assert "RECORD_KEY" in patient_sql
    assert "PATIENT_NAME" in patient_sql
    assert "COALESCE(CAMP_FILE_NUMBER, PATIENT_ID_FILE_NO)" in patient_sql
    assert "TRY_CAST(SPLIT_PART(AGE" in patient_sql


def test_run_write_visit_payload_fields():
    cur = FakeCursor()
    dag.run_write(cur, SRC, DST, FACILITY)
    visit_sql = cur.executed[3]    # BEGIN, DELETE, patient INSERT, visit INSERT
    for field in ("'id'", "'file_id'", "'patient_id'", "'date'", "'notes'"):
        assert field in visit_sql, f"Missing field {field} in visit INSERT"
    assert "RECORD_KEY || '_v'" in visit_sql
    assert "RECORD_KEY" in visit_sql
    assert "COALESCE(NURSES_TRIAGE_NOTES, HISTORY_OF_PRESENT_ILLNESS, DOCTORS_NOTES)" in visit_sql


def test_visit_id_distinct_from_patient_id():
    cur = FakeCursor()
    dag.run_write(cur, SRC, DST, FACILITY)
    patient_sql = cur.executed[2]   # after BEGIN, DELETE
    visit_sql = cur.executed[3]
    assert "RECORD_KEY || '_v'" not in patient_sql
    assert "RECORD_KEY || '_v'" in visit_sql


def test_run_write_module_source_is_reception_service():
    cur = FakeCursor()
    dag.run_write(cur, SRC, DST, FACILITY)
    for sql in cur.executed[2:4]:  # patient INSERT + visit INSERT
        assert "'reception-service'" in sql


def test_run_write_raises_when_insert_produces_zero_rows():
    cur = FakeCursor(rowcount=0)
    with pytest.raises(Exception, match="0 rows"):
        dag.run_write(cur, SRC, DST, FACILITY)
    assert "ROLLBACK" in cur.executed
    assert "COMMIT" not in cur.executed


def test_run_write_raises_and_rolls_back_on_partial_failure():
    """visits INSERT produces 0 rows while patients succeeds — must ROLLBACK, not COMMIT."""
    counts = iter([5, 5, 0])  # deleted=5, patients=5, visits=0

    class PartialCursor(FakeCursor):
        def __init__(self):
            self._fetchone_map = {}
            self.fail_on = None
            self.executed = []

        @property
        def rowcount(self):
            return next(counts, 0)

    cur = PartialCursor()
    with pytest.raises(Exception, match="0 rows"):
        dag.run_write(cur, SRC, DST, FACILITY)
    assert "ROLLBACK" in cur.executed
    assert "COMMIT" not in cur.executed


def test_run_write_uses_facility_in_sql():
    cur = FakeCursor()
    dag.run_write(cur, SRC, DST, FACILITY)
    for sql in cur.executed[2:4]:
        assert f"'{FACILITY}'" in sql


# ---------------------------------------------------------------------------
# run_verify
# ---------------------------------------------------------------------------

class FakeVerifyCursor(FakeCursor):
    """Returns configurable per-source_table counts from the verify GROUP BY."""

    def __init__(self, patient_count: int = 5, visit_count: int = 5):
        super().__init__()
        self._results = [("patients", patient_count), ("visits", visit_count)]

    def fetchall(self):
        return [(r[0], r[1]) for r in self._results if r[1] > 0]


def test_run_verify_passes_when_both_tables_present():
    cur = FakeVerifyCursor(patient_count=3, visit_count=3)
    result = dag.run_verify(cur, DST, FACILITY)
    assert result["patients"] == 3
    assert result["visits"] == 3


def test_run_verify_raises_when_patients_missing():
    cur = FakeVerifyCursor(patient_count=0, visit_count=4)
    with pytest.raises(Exception, match="patients"):
        dag.run_verify(cur, DST, FACILITY)


def test_run_verify_raises_when_visits_missing():
    cur = FakeVerifyCursor(patient_count=4, visit_count=0)
    with pytest.raises(Exception, match="visits"):
        dag.run_verify(cur, DST, FACILITY)


def test_run_verify_sql_filters_by_facility_and_module_source():
    cur = FakeVerifyCursor()
    dag.run_verify(cur, DST, FACILITY)
    verify_sql = cur.executed[0]
    assert f"facility_id = '{FACILITY}'" in verify_sql
    assert f"module_source = '{dag.MODULE_SOURCE}'" in verify_sql
    assert "source_table IN ('patients', 'visits')" in verify_sql


# ---------------------------------------------------------------------------
# DAG constants sanity check
# ---------------------------------------------------------------------------

def test_dag_id():
    assert dag.DAG_ID == "docs_to_events_raw"


def test_module_source_is_reception_service():
    assert dag.MODULE_SOURCE == "reception-service"
