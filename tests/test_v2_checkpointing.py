# tests/test_v2_checkpointing.py
import json
import os
from pathlib import Path

import pytest

# Import after env is set, or call use_state_dir in each test
import v2_to_v3_api_migration as m


@pytest.fixture
def facility_state(tmp_path, monkeypatch):
    monkeypatch.setenv("MIGRATION_STATE_ROOT", str(tmp_path))
    monkeypatch.setenv("MIGRATION_ENV", "local")
    m.use_state_dir("testfac")
    if hasattr(m, "_sync_path_globals"):
        m._sync_path_globals()
    m._inserted_ids.clear()
    m._record_progress_unflushed = False
    if hasattr(m, "_visit_patient_map"):
        m._visit_patient_map.clear()
        m._visit_patient_dirty = 0
    if hasattr(m, "_visit_admission_map"):
        m._visit_admission_map.clear()
        m._visit_admission_dirty = 0

    # Prefer migration_state paths; fall back to module globals
    if hasattr(m, "get_paths"):
        return m.get_paths()
    from migration_state import get_paths
    return get_paths()


def test_atomic_write_creates_valid_file(facility_state):
    path = facility_state.root / "sample.json"
    m._atomic_write(path, json.dumps({"a": 1}))
    assert json.loads(path.read_text()) == {"a": 1}


def test_mark_and_flush_record_progress(facility_state):
    m._mark_record_inserted("testfac|Patients", 101)
    # force flush
    with m._record_progress_lock:
        m._flush_record_progress()
    data = json.loads(facility_state.record_progress.read_text())
    assert 101 in data.get("testfac|Patients", []) or "101" in {
        str(x) for x in data.get("testfac|Patients", [])
    }


def test_load_progress_ignores_run_id_mismatch(facility_state):
    facility_state.progress.write_text(json.dumps({
        "run_id": "old-run",
        "completed": ["testfac|Patients"],
    }))
    got = m._load_progress("new-run")
    assert "testfac|Patients" in got


def test_load_watermarks_corrupt_moves_bak(facility_state):
    facility_state.watermark.write_text("{not-json")
    out = m._load_watermarks()
    assert out == {}
    baks = list(facility_state.watermark.parent.glob(".migration_watermarks.json.bak.*"))
    assert baks, "corrupt file should be renamed to .bak.*"


def test_save_watermarks_merge(facility_state):
    m._save_watermarks({"a|all": "2020-01-01T00:00:00Z"})
    m._save_watermarks({"b|all": "2021-01-01T00:00:00Z"})
    data = m._load_watermarks()
    assert "a|all" in data and "b|all" in data


def test_flush_visit_maps(facility_state):
    m._record_visit_patient(10, 20)
    m._visit_patient_dirty = 1  # ensure dirty if record already flushed by counter
    m._flush_visit_maps()
    raw = json.loads(facility_state.visit_patient.read_text())
    assert raw.get("10") == 20 or raw.get(10) == 20