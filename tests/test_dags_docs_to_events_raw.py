"""
dags/docs_to_events_raw.py unit tests.

Tests cover the three module-level helpers (check_null_names, run_validate,
run_write) via a fake cursor — no Snowflake connection required.

Integration tests (real Snowflake) live in the server; the integration checklist
is in docs/feature-21-spec.md under Task 1 → Tests.
"""
from __future__ import annotations

import pytest

from tests.helpers import load_dag_module

dag = load_dag_module("docs_to_events_raw")


# ---------------------------------------------------------------------------
# Fake cursor
# ---------------------------------------------------------------------------

class FakeCursor:
    """Records every SQL string passed to execute(); supports configurable
    fetchone() results keyed by a substring of the SQL."""

    def __init__(self, fetchone_map: dict | None = None, rowcount: int = 5):
        self._fetchone_map = fetchone_map or {}
        self.rowcount = rowcount
        self.executed: list[str] = []

    def execute(self, sql, params=None):
        self.executed.append(sql.strip())
        return self

    def fetchone(self):
        for key, value in self._fetchone_map.items():
            if any(key in s for s in self.executed[-1:]):
                return value
        return (0,)


# ---------------------------------------------------------------------------
# check_null_names
# ---------------------------------------------------------------------------

def test_check_null_names_returns_zero_and_no_warning(caplog):
    cur = FakeCursor(fetchone_map={"PATIENT_NAME IS NULL": (0,)})
    with caplog.at_level("WARNING"):
        result = dag.check_null_names(cur)
    assert result == 0
    assert "PATIENT_NAME" not in caplog.text


def test_check_null_names_logs_warning_when_nulls_present(caplog):
    cur = FakeCursor(fetchone_map={"PATIENT_NAME IS NULL": (3,)})
    with caplog.at_level("WARNING"):
        result = dag.check_null_names(cur)
    assert result == 3
    assert "3 row(s)" in caplog.text
    assert "held back" in caplog.text


# ---------------------------------------------------------------------------
# run_validate
# ---------------------------------------------------------------------------

def test_run_validate_raises_when_empty():
    cur = FakeCursor(fetchone_map={"COUNT(*)" : (0,)})
    with pytest.raises(Exception, match="empty"):
        dag.run_validate(cur)


def test_run_validate_returns_count():
    cur = FakeCursor(fetchone_map={"COUNT(*)": (10,), "PATIENT_NAME IS NULL": (0,)})
    assert dag.run_validate(cur) == 10


def test_run_validate_calls_check_null_names(monkeypatch):
    calls = []
    monkeypatch.setattr(dag, "check_null_names", lambda cur: calls.append(True) or 0)
    cur = FakeCursor(fetchone_map={"COUNT(*)": (4,)})
    dag.run_validate(cur)
    assert calls == [True]


# ---------------------------------------------------------------------------
# run_write — SQL order and content
# ---------------------------------------------------------------------------

def test_run_write_returns_counts():
    cur = FakeCursor(rowcount=7)
    result = dag.run_write(cur)
    assert result == {"deleted": 7, "patients": 7, "visits": 7}


def test_run_write_delete_before_inserts():
    cur = FakeCursor()
    dag.run_write(cur)
    assert len(cur.executed) == 3
    assert "DELETE" in cur.executed[0]
    assert "source_table = 'patients'" in cur.executed[1] or "'patients'" in cur.executed[1]
    assert "source_table = 'visits'" in cur.executed[2] or "'visits'" in cur.executed[2]


def test_run_write_delete_filters_by_file_id():
    cur = FakeCursor()
    dag.run_write(cur)
    delete_sql = cur.executed[0]
    assert "payload:file_id::STRING" in delete_sql
    assert "module_source = 'reception-service'" in delete_sql
    assert "source_table IN ('patients', 'visits')" in delete_sql


def test_run_write_patient_payload_fields():
    cur = FakeCursor()
    dag.run_write(cur)
    patient_sql = cur.executed[1]
    for field in ("'id'", "'file_id'", "'last_name'", "'patient_no'", "'date_of_birth'",
                  "'age'", "'sex'", "'phone'", "'address'"):
        assert field in patient_sql, f"Missing field {field} in patient INSERT"
    assert "RECORD_KEY" in patient_sql
    assert "PATIENT_NAME" in patient_sql
    assert "COALESCE(CAMP_FILE_NUMBER, PATIENT_ID_FILE_NO)" in patient_sql


def test_run_write_visit_payload_fields():
    cur = FakeCursor()
    dag.run_write(cur)
    visit_sql = cur.executed[2]
    for field in ("'id'", "'file_id'", "'patient_id'", "'date'", "'notes'"):
        assert field in visit_sql, f"Missing field {field} in visit INSERT"
    assert "RECORD_KEY || '_v'" in visit_sql   # visit id is distinct from patient id
    assert "RECORD_KEY" in visit_sql            # patient_id links back to patient row


def test_visit_id_distinct_from_patient_id():
    cur = FakeCursor()
    dag.run_write(cur)
    patient_sql = cur.executed[1]
    visit_sql = cur.executed[2]
    # Patient uses bare RECORD_KEY; visit appends '_v'
    assert "'id',            RECORD_KEY," in patient_sql or "'id',\n" in patient_sql or "RECORD_KEY," in patient_sql
    assert "RECORD_KEY || '_v'" in visit_sql


def test_run_write_module_source_is_reception_service():
    cur = FakeCursor()
    dag.run_write(cur)
    for sql in cur.executed[1:]:   # both INSERTs
        assert "'reception-service'" in sql


# ---------------------------------------------------------------------------
# SQL constants sanity checks (catch accidental edits)
# ---------------------------------------------------------------------------

def test_sql_constants_reference_correct_tables():
    assert "HOSPITALS.SIAYA_DOCS.EXTRACTED_FORMS" in dag.SRC
    assert "HOSPITALS.SIAYA_RAW.EVENTS_RAW" in dag.DST
    assert dag.FACILITY == "siaya"
    assert dag.MODULE_SOURCE == "reception-service"


def test_dag_id():
    assert dag.DAG_ID == "docs_to_events_raw"
