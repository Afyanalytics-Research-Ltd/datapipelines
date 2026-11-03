
import json

import v2_to_v3_api_migration as pipeline


def test_completed_job_checkpoint_survives_restart(tmp_path, monkeypatch):
    """A completed job remains recorded when a new run starts."""

    progress_file = tmp_path / ".migration_progress.json"
    monkeypatch.setattr(pipeline, "PROGRESS_FILE", progress_file)
    pipeline._completed_jobs.clear()

    pipeline._mark_done("run-1", "facility_a|patients")

    # Simulate a fresh run reading the saved checkpoint.
    completed = pipeline._load_progress("run-2")

    assert "facility_a|patients" in completed

    saved = json.loads(progress_file.read_text())
    assert "facility_a|patients" in saved["completed"]


def test_corrupt_checkpoint_is_backed_up(tmp_path, monkeypatch):
    """A corrupt checkpoint is preserved instead of silently reused."""

    progress_file = tmp_path / ".migration_progress.json"
    progress_file.write_text("{invalid JSON", encoding="utf-8")
    monkeypatch.setattr(pipeline, "PROGRESS_FILE", progress_file)

    completed = pipeline._load_progress("run-2")

    assert completed == set()
    assert not progress_file.exists()
    assert list(tmp_path.glob(".migration_progress.json.bak.*"))


def test_checkpoint_write_produces_valid_json(tmp_path):
    """Atomic state writes should leave a complete, readable JSON file."""

    checkpoint = tmp_path / ".migration_progress.json"

    pipeline._atomic_write(
        checkpoint,
        json.dumps({
            "run_id": "run-1",
            "completed": ["facility_a|patients"],
        }),
    )

    data = json.loads(checkpoint.read_text(encoding="utf-8"))

    assert data["run_id"] == "run-1"
    assert data["completed"] == ["facility_a|patients"]
