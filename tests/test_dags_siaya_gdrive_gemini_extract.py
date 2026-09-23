"""
Deep coverage for dags/siaya_gdrive_gemini_extract.py.

Focus areas (see the module docstring's "NOTE ON DEDUP / RESUME"):
  - DAG task-graph shape: list_new_files -> extract_and_persist (mapped) -> summarize_run.
  - list_new_files: lists whatever Drive says is in GDRIVE_SOURCE_FOLDER_ID and
    does no additional local dedup -- the folder listing itself IS the queue.
  - extract_and_persist_one_file: download -> upload to Gemini -> one
    structured-extraction call -> persist every record to S3 -> only THEN
    move the file to GDRIVE_DEST_FOLDER_ID. A failure at any step must leave
    the file in the source folder (no move call) so it's retried whole next run.
  - SIAYA_PATIENT_SCHEMA_JSON / WRAPPED_SCHEMA validity, and an informational
    diff against the repo-root schema.json.
  - summarize_run aggregation.
  - call_with_retry / upload_pdf_to_gemini / extract_all_records_via_gemini
    retry-and-backoff behavior.

All Google Drive / Gemini clients are mocked; no network calls are made.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from tests.airflow_stub import AirflowVariableNotFound, TriggerRule
from tests.helpers import REPO_ROOT, load_dag_module

MODULE = load_dag_module("siaya_gdrive_gemini_extract")


@pytest.fixture(autouse=True)
def mock_sleep(monkeypatch):
    """None of these tests should actually block on time.sleep -- the module
    sleeps between Gemini retries (up to MAX_RETRIES * a few seconds each)
    and while polling for Gemini file PROCESSING -> ACTIVE. Replace it with a
    no-op Mock so tests stay fast; individual tests can still inspect
    call args to assert backoff behaviour."""
    m = mock.Mock()
    monkeypatch.setattr(MODULE.time, "sleep", m)
    return m


def _record(**overrides):
    """A minimal-but-schema-shaped fake extracted patient record."""
    base = {k: None for k in MODULE.SIAYA_PATIENT_SCHEMA["required"] if k not in (
        "chief_complaints", "provisional_diagnosis",
        "lab_imaging_investigations_ordered", "treatment_plan_prescriptions",
        "lab_results",
    )}
    for array_field in (
        "chief_complaints", "provisional_diagnosis",
        "lab_imaging_investigations_ordered", "treatment_plan_prescriptions",
        "lab_results",
    ):
        base[array_field] = []
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# DAG structural shape
# ---------------------------------------------------------------------------
class TestDagShape:
    def test_dag_id_schedule_tags_catchup(self):
        dag = MODULE.dag
        assert dag.dag_id == "siaya_gdrive_gemini_extract"
        assert dag.schedule == "@hourly"
        assert dag.catchup is False
        assert set(dag.tags) == {"siaya", "gdrive", "gemini", "extract"}
        assert dag.default_args.get("retries") == 3
        assert dag.default_args.get("retry_delay") is not None

    def test_task_chain_shape(self):
        dag = MODULE.dag
        assert dag.task_ids == ["list_new_files", "extract_and_persist", "summarize_run"]

        t_list = dag.get_task("list_new_files")
        t_extract = dag.get_task("extract_and_persist")
        t_summary = dag.get_task("summarize_run")

        assert t_list.python_callable is MODULE.list_new_files
        assert t_extract.python_callable is MODULE.extract_and_persist_one_file
        assert t_summary.python_callable is MODULE.summarize_run

        assert t_list.is_mapped is False
        assert t_extract.is_mapped is True
        assert t_summary.is_mapped is False

        assert t_list.downstream_task_ids == {"extract_and_persist"}
        assert t_extract.upstream_task_ids == {"list_new_files"}
        assert t_extract.downstream_task_ids == {"summarize_run"}
        assert t_summary.upstream_task_ids == {"extract_and_persist"}

        # Dynamic mapping is wired from list_new_files' XCom output.
        assert t_extract.mapped_kwargs is not None
        assert t_extract.mapped_kwargs["op_kwargs"].task is t_list

        # ALL_DONE on the extraction fan-out + summary so one failed file
        # doesn't skip processing/summarizing the rest of the batch.
        assert t_extract.trigger_rule == TriggerRule.ALL_DONE
        assert t_summary.trigger_rule == TriggerRule.ALL_DONE


# ---------------------------------------------------------------------------
# list_source_files (raw Drive pagination helper)
# ---------------------------------------------------------------------------
class TestListSourceFiles:
    def test_builds_expected_query_and_single_page(self):
        service = mock.MagicMock()
        service.files.return_value.list.return_value.execute.return_value = {
            "files": [
                {"id": "f1", "name": "a.pdf", "parents": ["SRC"], "mimeType": "application/pdf"},
                {"id": "f2", "name": "b.pdf", "parents": ["SRC"], "mimeType": "application/pdf"},
            ]
        }

        files = MODULE.list_source_files(service, "SRC_FOLDER_ID")

        assert [f["id"] for f in files] == ["f1", "f2"]
        kwargs = service.files.return_value.list.call_args.kwargs
        assert kwargs["q"] == (
            "'SRC_FOLDER_ID' in parents and trashed = false and mimeType = 'application/pdf'"
        )
        assert kwargs["pageSize"] == 1000
        assert kwargs["pageToken"] is None

    def test_paginates_until_no_next_page_token(self):
        service = mock.MagicMock()
        page1 = {
            "files": [{"id": "f1", "name": "a.pdf", "mimeType": "application/pdf"}],
            "nextPageToken": "TOK2",
        }
        page2 = {"files": [{"id": "f2", "name": "b.pdf", "mimeType": "application/pdf"}]}
        service.files.return_value.list.return_value.execute.side_effect = [page1, page2]

        files = MODULE.list_source_files(service, "SRC")

        assert [f["id"] for f in files] == ["f1", "f2"]
        calls = service.files.return_value.list.call_args_list
        assert len(calls) == 2
        assert calls[0].kwargs["pageToken"] is None
        assert calls[1].kwargs["pageToken"] == "TOK2"

    def test_empty_folder_returns_empty_list(self):
        service = mock.MagicMock()
        service.files.return_value.list.return_value.execute.return_value = {"files": []}
        assert MODULE.list_source_files(service, "SRC") == []


# ---------------------------------------------------------------------------
# list_new_files task callable
# ---------------------------------------------------------------------------
class TestListNewFiles:
    def test_returns_job_dicts_shaped_for_expand(self, set_variables):
        set_variables(GDRIVE_SOURCE_FOLDER_ID="SRC1")
        fake_service = object()
        drive_files = [
            {"id": "f1", "name": "a.pdf", "mimeType": "application/pdf"},
            {"id": "f2", "name": "b.pdf", "mimeType": "application/pdf"},
        ]
        with mock.patch.object(MODULE, "get_drive_service", return_value=fake_service), \
             mock.patch.object(MODULE, "list_source_files", return_value=drive_files) as m_list:
            jobs = MODULE.list_new_files()

        m_list.assert_called_once_with(fake_service, "SRC1")
        assert jobs == [
            {"file_id": "f1", "file_name": "a.pdf", "mime_type": "application/pdf"},
            {"file_id": "f2", "file_name": "b.pdf", "mime_type": "application/pdf"},
        ]

    def test_empty_source_folder_yields_no_jobs(self, set_variables):
        set_variables(GDRIVE_SOURCE_FOLDER_ID="SRC1")
        with mock.patch.object(MODULE, "get_drive_service", return_value=object()), \
             mock.patch.object(MODULE, "list_source_files", return_value=[]):
            assert MODULE.list_new_files() == []

    def test_missing_variable_raises(self):
        # No GDRIVE_SOURCE_FOLDER_ID registered -> the stub's loud failure,
        # not a silent default.
        with pytest.raises(AirflowVariableNotFound):
            MODULE.list_new_files()

    def test_dedup_relies_solely_on_drive_folder_membership(self, set_variables):
        """DESIGN CHECK (see module docstring's NOTE ON DEDUP / RESUME): the
        only "already processed" guard is that a finished file has been
        physically moved out of the source folder -- list_new_files applies
        no additional local/Variable-based filter on top of whatever Drive
        reports as currently in that folder. Demonstrate that by showing a
        duplicate id in the mocked listing passes straight through unfiltered
        (i.e. there is no defensive de-duplication in this function, so if
        Drive's own listing ever returned a file twice -- or two concurrent
        DAG runs both saw the same file before either finished moving it out
        -- it would be listed/processed twice here). This is not something to
        fix per the task's constraints; it's flagged in the report."""
        set_variables(GDRIVE_SOURCE_FOLDER_ID="SRC1")
        duplicated = [
            {"id": "f1", "name": "a.pdf", "mimeType": "application/pdf"},
            {"id": "f1", "name": "a.pdf", "mimeType": "application/pdf"},
        ]
        with mock.patch.object(MODULE, "get_drive_service", return_value=object()), \
             mock.patch.object(MODULE, "list_source_files", return_value=duplicated):
            jobs = MODULE.list_new_files()
        assert len(jobs) == 2  # no de-dup guard beyond Drive folder membership itself


# ---------------------------------------------------------------------------
# extract_and_persist_one_file
# ---------------------------------------------------------------------------
class _FakeGeminiFile(SimpleNamespace):
    pass


def _patch_pipeline(
    *,
    generate_content_return=None,
    generate_content_side_effect=None,
    upload_return=None,
    download_side_effect=None,
    s3_load_bytes_side_effect=None,
):
    """Shared scaffolding: patches get_drive_service/get_genai_client/
    download_file/move_file/S3Hook on MODULE and returns the mocks plus a
    shared `events` list that call-order-sensitive tests can inspect (each
    persisted record appends "s3", each move appends "move")."""
    events: list[str] = []

    fake_service = SimpleNamespace(name="fake-drive-service")
    fake_file_obj = upload_return or _FakeGeminiFile(name="files/abc123", state="ACTIVE")

    fake_client = mock.MagicMock()
    fake_client.files.upload.return_value = fake_file_obj
    if generate_content_side_effect is not None:
        fake_client.models.generate_content.side_effect = generate_content_side_effect
    else:
        fake_client.models.generate_content.return_value = generate_content_return

    m_get_drive = mock.patch.object(MODULE, "get_drive_service", return_value=fake_service)
    m_get_genai = mock.patch.object(MODULE, "get_genai_client", return_value=fake_client)

    m_download = mock.patch.object(MODULE, "download_file", side_effect=download_side_effect)

    m_move = mock.patch.object(MODULE, "move_file", side_effect=lambda *a, **k: events.append("move"))

    fake_s3_instance = mock.MagicMock()

    def _load_bytes(*args, **kwargs):
        if s3_load_bytes_side_effect is not None:
            result = s3_load_bytes_side_effect(*args, **kwargs)
            if isinstance(result, Exception):
                events.append("s3-error")
                raise result
        events.append("s3")

    fake_s3_instance.load_bytes.side_effect = _load_bytes
    m_s3hook = mock.patch.object(MODULE, "S3Hook", return_value=fake_s3_instance)

    return {
        "events": events,
        "fake_service": fake_service,
        "fake_client": fake_client,
        "fake_s3_instance": fake_s3_instance,
        "patches": [m_get_drive, m_get_genai, m_download, m_move, m_s3hook],
    }


class TestExtractAndPersistOneFile:
    def test_success_persists_then_moves_in_order(self, set_variables):
        set_variables(GDRIVE_DEST_FOLDER_ID="DEST1")
        records = [_record(patient_name="Alice"), _record(patient_name="Bob")]
        scaffold = _patch_pipeline(
            generate_content_return=SimpleNamespace(text=json.dumps({"records": records})),
        )
        with scaffold["patches"][0], scaffold["patches"][1], scaffold["patches"][2], \
             scaffold["patches"][3], scaffold["patches"][4]:
            result = MODULE.extract_and_persist_one_file(
                file_id="fid1", file_name="camp1.pdf", mime_type="application/pdf", ds="2026-09-22",
            )

        assert result["ok"] is True
        assert result["file_id"] == "fid1"
        assert result["file_name"] == "camp1.pdf"
        assert result["record_count"] == 2
        assert result["s3_keys"] == [
            "raw/siaya_gdrive_gemini_extract/dt=2026-09-22/file_id=fid1/camp1_record01.json",
            "raw/siaya_gdrive_gemini_extract/dt=2026-09-22/file_id=fid1/camp1_record02.json",
        ]

        # Persistence (both records) happened strictly before the move.
        assert scaffold["events"] == ["s3", "s3", "move"]

        s3_calls = scaffold["fake_s3_instance"].load_bytes.call_args_list
        assert len(s3_calls) == 2
        assert s3_calls[0].kwargs["key"] == result["s3_keys"][0]
        assert s3_calls[0].kwargs["bucket_name"] == MODULE.S3_BUCKET == "collabmedbucket"
        assert s3_calls[0].kwargs["replace"] is True
        assert json.loads(s3_calls[0].kwargs["bytes_data"].decode("utf-8")) == records[0]
        assert json.loads(s3_calls[1].kwargs["bytes_data"].decode("utf-8")) == records[1]

        # Gemini Files API cleanup happened (best-effort delete in `finally`).
        scaffold["fake_client"].files.delete.assert_called_once_with(name="files/abc123")

    def test_unsupported_mime_type_is_skipped_without_touching_drive_or_gemini(self):
        with mock.patch.object(MODULE, "get_drive_service") as m_drive, \
             mock.patch.object(MODULE, "get_genai_client") as m_genai, \
             mock.patch.object(MODULE, "move_file") as m_move, \
             mock.patch.object(MODULE, "S3Hook") as m_s3:
            result = MODULE.extract_and_persist_one_file(
                file_id="fid1", file_name="notapdf.docx", mime_type="application/msword",
            )

        assert result == {
            "ok": False, "file_id": "fid1", "file_name": "notapdf.docx",
            "error": "Unsupported mimeType: application/msword",
        }
        m_drive.assert_not_called()
        m_genai.assert_not_called()
        m_move.assert_not_called()
        m_s3.assert_not_called()

    def test_gemini_extraction_exception_leaves_file_in_source_and_reports_failure(self, set_variables):
        """A persistent Gemini error (e.g. rate limiting) exhausts
        call_with_retry's MAX_RETRIES and must surface as ok=False with the
        file NOT moved (so it's retried whole next scheduled run) and NOT
        silently swallowed (the error text is preserved in the result)."""
        set_variables(GDRIVE_DEST_FOLDER_ID="DEST1")
        scaffold = _patch_pipeline(
            generate_content_side_effect=RuntimeError("rate limited: 429"),
        )
        with scaffold["patches"][0], scaffold["patches"][1], scaffold["patches"][2], \
             scaffold["patches"][3], scaffold["patches"][4]:
            result = MODULE.extract_and_persist_one_file(
                file_id="fid1", file_name="camp1.pdf", mime_type="application/pdf", ds="2026-09-22",
            )

        assert result["ok"] is False
        assert result["file_id"] == "fid1"
        assert "rate limited: 429" in result["error"]
        assert "s3_keys" not in result
        assert "move" not in scaffold["events"]
        assert "s3" not in scaffold["events"]
        # Gemini Files API cleanup still runs even on failure.
        scaffold["fake_client"].files.delete.assert_called_once_with(name="files/abc123")
        # Retried MAX_RETRIES times with growing backoff, no real sleeping.
        assert MODULE.time.sleep.call_count == MODULE.MAX_RETRIES

    def test_malformed_json_response_does_not_move_file(self, set_variables):
        set_variables(GDRIVE_DEST_FOLDER_ID="DEST1")
        scaffold = _patch_pipeline(
            generate_content_return=SimpleNamespace(text="not valid json{{{"),
        )
        with scaffold["patches"][0], scaffold["patches"][1], scaffold["patches"][2], \
             scaffold["patches"][3], scaffold["patches"][4]:
            result = MODULE.extract_and_persist_one_file(
                file_id="fid1", file_name="camp1.pdf", mime_type="application/pdf", ds="2026-09-22",
            )

        assert result["ok"] is False
        assert "move" not in scaffold["events"]
        assert "s3" not in scaffold["events"]

    def test_empty_records_list_is_treated_as_failure_and_not_moved(self, set_variables):
        set_variables(GDRIVE_DEST_FOLDER_ID="DEST1")
        scaffold = _patch_pipeline(
            generate_content_return=SimpleNamespace(text=json.dumps({"records": []})),
        )
        with scaffold["patches"][0], scaffold["patches"][1], scaffold["patches"][2], \
             scaffold["patches"][3], scaffold["patches"][4]:
            result = MODULE.extract_and_persist_one_file(
                file_id="fid1", file_name="camp1.pdf", mime_type="application/pdf", ds="2026-09-22",
            )

        assert result["ok"] is False
        assert "Gemini returned no records" in result["error"]
        assert "move" not in scaffold["events"]

    def test_s3_persist_failure_partway_through_still_blocks_the_move(self, set_variables):
        """If S3 persistence fails on a later record after an earlier one in
        the same file already succeeded, the file must still not be moved
        (whole-file retry semantics -- confirms a file is never moved before
        every one of its records is durably persisted)."""
        set_variables(GDRIVE_DEST_FOLDER_ID="DEST1")
        records = [_record(patient_name="Alice"), _record(patient_name="Bob")]
        call_count = {"n": 0}

        def _s3_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                return RuntimeError("S3 unavailable")
            return None

        scaffold = _patch_pipeline(
            generate_content_return=SimpleNamespace(text=json.dumps({"records": records})),
            s3_load_bytes_side_effect=_s3_side_effect,
        )
        with scaffold["patches"][0], scaffold["patches"][1], scaffold["patches"][2], \
             scaffold["patches"][3], scaffold["patches"][4]:
            result = MODULE.extract_and_persist_one_file(
                file_id="fid1", file_name="camp1.pdf", mime_type="application/pdf", ds="2026-09-22",
            )

        assert result["ok"] is False
        assert "S3 unavailable" in result["error"]
        assert "move" not in scaffold["events"]
        assert scaffold["events"] == ["s3", "s3-error"]

    def test_download_failure_does_not_call_genai_extraction_or_move(self, set_variables):
        set_variables(GDRIVE_DEST_FOLDER_ID="DEST1")
        scaffold = _patch_pipeline(download_side_effect=OSError("disk full"))
        with scaffold["patches"][0], scaffold["patches"][1], scaffold["patches"][2], \
             scaffold["patches"][3], scaffold["patches"][4]:
            result = MODULE.extract_and_persist_one_file(
                file_id="fid1", file_name="camp1.pdf", mime_type="application/pdf", ds="2026-09-22",
            )

        assert result["ok"] is False
        assert "disk full" in result["error"]
        scaffold["fake_client"].models.generate_content.assert_not_called()
        assert "move" not in scaffold["events"]
        # gemini_file was never set (upload never reached) -> no delete call.
        scaffold["fake_client"].files.delete.assert_not_called()

    def test_client_construction_failure_propagates_uncaught(self, set_variables):
        """DESIGN NOTE: get_drive_service()/get_genai_client() are called
        BEFORE the function's try/except block, so a failure constructing
        either client (e.g. bad credentials) is NOT caught here and is NOT
        turned into an {"ok": False, ...} result -- it propagates out of
        extract_and_persist_one_file entirely. In real Airflow this fails the
        mapped task instance outright (subject to default_args retries=3),
        rather than being recorded as a per-file failure that summarize_run
        can count. The file is still never moved (so it's still safe to
        retry), but this is worth knowing: such failures won't show up in
        summarize_run's failure tally the way a Gemini/S3 failure would."""
        set_variables(GDRIVE_DEST_FOLDER_ID="DEST1")
        with mock.patch.object(MODULE, "get_drive_service", side_effect=RuntimeError("bad creds")), \
             mock.patch.object(MODULE, "move_file") as m_move:
            with pytest.raises(RuntimeError, match="bad creds"):
                MODULE.extract_and_persist_one_file(
                    file_id="fid1", file_name="camp1.pdf", mime_type="application/pdf", ds="2026-09-22",
                )
        m_move.assert_not_called()

    def test_ds_falls_back_to_context_or_utcnow_when_absent(self, set_variables):
        set_variables(GDRIVE_DEST_FOLDER_ID="DEST1")
        records = [_record(patient_name="Alice")]
        scaffold = _patch_pipeline(
            generate_content_return=SimpleNamespace(text=json.dumps({"records": records})),
        )
        with scaffold["patches"][0], scaffold["patches"][1], scaffold["patches"][2], \
             scaffold["patches"][3], scaffold["patches"][4]:
            result = MODULE.extract_and_persist_one_file(
                file_id="fid1", file_name="camp1.pdf", mime_type="application/pdf",
            )  # no ds= kwarg and no **context with "ds"

        assert result["ok"] is True
        # Should have used today's UTC date rather than crashing.
        import datetime as _dt
        expected_prefix = f"raw/siaya_gdrive_gemini_extract/dt={_dt.datetime.utcnow().date().isoformat()}/"
        assert result["s3_keys"][0].startswith(expected_prefix)


# ---------------------------------------------------------------------------
# persist_record_to_s3
# ---------------------------------------------------------------------------
class TestPersistRecordToS3:
    def test_key_template_and_payload(self):
        fake_instance = mock.MagicMock()
        with mock.patch.object(MODULE, "S3Hook", return_value=fake_instance) as m_cls:
            key = MODULE.persist_record_to_s3(
                {"patient_name": "Alice"}, file_id="FID", file_name="Some File.pdf", idx=3, ds="2026-01-05",
            )

        assert key == "raw/siaya_gdrive_gemini_extract/dt=2026-01-05/file_id=FID/Some File_record03.json"
        m_cls.assert_called_once_with(aws_conn_id=MODULE.S3_CONN_ID)
        fake_instance.load_bytes.assert_called_once()
        call_kwargs = fake_instance.load_bytes.call_args.kwargs
        assert call_kwargs["key"] == key
        assert call_kwargs["bucket_name"] == "collabmedbucket"
        assert call_kwargs["replace"] is True
        assert json.loads(call_kwargs["bytes_data"].decode("utf-8")) == {"patient_name": "Alice"}


# ---------------------------------------------------------------------------
# Schema fidelity
# ---------------------------------------------------------------------------
class TestSchema:
    def test_schema_json_string_parses(self):
        parsed = json.loads(MODULE.SIAYA_PATIENT_SCHEMA_JSON)
        assert parsed == MODULE.SIAYA_PATIENT_SCHEMA

    def test_schema_has_expected_json_schema_shape(self):
        schema = MODULE.SIAYA_PATIENT_SCHEMA
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert schema["type"] == "object"
        assert schema["additionalProperties"] is False
        assert isinstance(schema["properties"], dict) and schema["properties"]
        assert isinstance(schema["required"], list) and schema["required"]
        # Every required field must actually be a declared property (no typos
        # / no property renamed without updating `required`, and vice versa).
        assert set(schema["required"]) <= set(schema["properties"].keys())
        # Spot-check a plain string-or-null field and an array field's shape.
        assert schema["properties"]["patient_name"]["type"] == ["string", "null"]
        assert schema["properties"]["chief_complaints"]["type"] == "array"
        assert schema["properties"]["chief_complaints"]["items"] == {"type": "string"}
        expected_array_fields = {
            "chief_complaints", "provisional_diagnosis",
            "lab_imaging_investigations_ordered", "treatment_plan_prescriptions",
            "lab_results",
        }
        actual_array_fields = {
            name for name, spec in schema["properties"].items() if spec.get("type") == "array"
        }
        assert actual_array_fields == expected_array_fields

    def test_build_records_schema_wraps_patient_schema_in_records_array(self):
        wrapped = MODULE.build_records_schema(MODULE.SIAYA_PATIENT_SCHEMA)
        assert wrapped["type"] == "object"
        assert wrapped["required"] == ["records"]
        records_spec = wrapped["properties"]["records"]
        assert records_spec["type"] == "array"
        item_schema = records_spec["items"]
        assert item_schema["type"] == "object"
        assert item_schema["properties"] == MODULE.SIAYA_PATIENT_SCHEMA["properties"]
        assert item_schema["required"] == MODULE.SIAYA_PATIENT_SCHEMA["required"]

    def test_module_level_wrapped_schema_matches_builder_output(self):
        assert MODULE.WRAPPED_SCHEMA == MODULE.build_records_schema(MODULE.SIAYA_PATIENT_SCHEMA)

    def test_informational_diff_against_repo_root_schema_json(self, capsys):
        """The module docstring claims SIAYA_PATIENT_SCHEMA_JSON is kept
        "byte-for-byte identical" (in fields/required, not necessarily
        whitespace) to the repo-root schema.json. This makes that claim
        testable/repeatable. Per the task spec this check is INFORMATIONAL
        ONLY (printed, not asserted) since the repo-root file isn't deployed
        and could legitimately drift or be removed later."""
        root_schema_path = REPO_ROOT / "schema.json"
        if not root_schema_path.exists():
            pytest.skip("repo-root schema.json no longer exists")

        root_schema = json.loads(root_schema_path.read_text(encoding="utf-8"))
        dag_props = set(MODULE.SIAYA_PATIENT_SCHEMA["properties"].keys())
        root_props = set(root_schema.get("properties", {}).keys())
        dag_required = set(MODULE.SIAYA_PATIENT_SCHEMA["required"])
        root_required = set(root_schema.get("required", []))

        prop_diff = dag_props.symmetric_difference(root_props)
        required_diff = dag_required.symmetric_difference(root_required)

        if prop_diff or required_diff:
            print(
                "INFORMATIONAL schema drift between dags/siaya_gdrive_gemini_extract.py's "
                f"inlined schema and repo-root schema.json -- properties diff: {prop_diff}, "
                f"required diff: {required_diff}"
            )
        else:
            print("Schema fidelity check: properties/required keys match repo-root schema.json exactly.")
        # Deliberately no assertion here -- see docstring above.


# ---------------------------------------------------------------------------
# summarize_run
# ---------------------------------------------------------------------------
class TestSummarizeRun:
    def test_aggregates_mixed_results(self, caplog):
        results = [
            {"ok": True, "file_id": "f1", "file_name": "a.pdf", "record_count": 3},
            {"ok": True, "file_id": "f2", "file_name": "b.pdf", "record_count": 2},
            {"ok": False, "file_id": "f3", "file_name": "c.pdf", "error": "boom"},
        ]
        with caplog.at_level(logging.INFO, logger=MODULE.log.name):
            assert MODULE.summarize_run(results) is None
        assert "2/3 file(s) fully extracted" in caplog.text
        assert "5 patient record(s)" in caplog.text
        assert "c.pdf" in caplog.text
        assert "boom" in caplog.text

    def test_empty_list_does_not_crash(self, caplog):
        with caplog.at_level(logging.INFO, logger=MODULE.log.name):
            assert MODULE.summarize_run([]) is None
        assert "0/0 file(s) fully extracted" in caplog.text

    def test_none_results_does_not_crash(self, caplog):
        with caplog.at_level(logging.INFO, logger=MODULE.log.name):
            assert MODULE.summarize_run(None) is None
        assert "0/0 file(s) fully extracted" in caplog.text

    def test_all_failed_reports_zero_success_and_no_crash_on_missing_record_count(self, caplog):
        results = [{"ok": False, "file_id": "f1", "file_name": "a.pdf", "error": "e1"}]
        with caplog.at_level(logging.INFO, logger=MODULE.log.name):
            MODULE.summarize_run(results)
        assert "0/1 file(s) fully extracted" in caplog.text
        assert "0 patient record(s)" in caplog.text

    def test_all_succeeded_no_failure_section_logged(self, caplog):
        results = [{"ok": True, "file_id": "f1", "file_name": "a.pdf", "record_count": 1}]
        with caplog.at_level(logging.INFO, logger=MODULE.log.name):
            MODULE.summarize_run(results)
        assert "Failures" not in caplog.text


# ---------------------------------------------------------------------------
# Retry/backoff
# ---------------------------------------------------------------------------
class TestCallWithRetry:
    def test_succeeds_first_try_no_sleep(self, mock_sleep):
        fn = mock.Mock(return_value="ok")
        assert MODULE.call_with_retry(fn, "a", b=1) == "ok"
        fn.assert_called_once_with("a", b=1)
        mock_sleep.assert_not_called()

    def test_succeeds_after_transient_failures(self, mock_sleep):
        fn = mock.Mock(side_effect=[RuntimeError("transient1"), RuntimeError("transient2"), "ok"])
        result = MODULE.call_with_retry(fn, max_retries=5)
        assert result == "ok"
        assert fn.call_count == 3
        # Backoff grows linearly with attempt number (RETRY_BACKOFF_SECONDS * attempt).
        assert [c.args[0] for c in mock_sleep.call_args_list] == [
            MODULE.RETRY_BACKOFF_SECONDS * 1, MODULE.RETRY_BACKOFF_SECONDS * 2,
        ]

    def test_exhausts_retries_and_raises_runtimeerror_with_last_exception(self, mock_sleep):
        fn = mock.Mock(side_effect=RuntimeError("always fails"))
        with pytest.raises(RuntimeError) as exc_info:
            MODULE.call_with_retry(fn, max_retries=3)
        assert fn.call_count == 3
        assert "All 3 attempts failed" in str(exc_info.value)
        assert "always fails" in str(exc_info.value)

    def test_defaults_to_module_max_retries(self, mock_sleep):
        fn = mock.Mock(side_effect=RuntimeError("nope"))
        with pytest.raises(RuntimeError):
            MODULE.call_with_retry(fn)
        assert fn.call_count == MODULE.MAX_RETRIES


class TestUploadPdfToGemini:
    def test_returns_immediately_active_file(self, mock_sleep):
        client = mock.MagicMock()
        active_file = _FakeGeminiFile(name="files/x", state="ACTIVE")
        client.files.upload.return_value = active_file
        result = MODULE.upload_pdf_to_gemini(client, Path("/tmp/whatever.pdf"))
        assert result is active_file
        client.files.get.assert_not_called()
        mock_sleep.assert_not_called()

    def test_polls_through_processing_until_active(self, mock_sleep):
        client = mock.MagicMock()
        client.files.upload.return_value = _FakeGeminiFile(name="files/x", state="PROCESSING")
        client.files.get.side_effect = [
            _FakeGeminiFile(name="files/x", state="PROCESSING"),
            _FakeGeminiFile(name="files/x", state="ACTIVE"),
        ]
        result = MODULE.upload_pdf_to_gemini(client, Path("/tmp/whatever.pdf"))
        assert result.state == "ACTIVE"
        assert client.files.get.call_count == 2
        assert mock_sleep.call_count == 2

    def test_failed_processing_state_raises(self, mock_sleep):
        client = mock.MagicMock()
        client.files.upload.return_value = _FakeGeminiFile(name="files/x", state="FAILED")
        with pytest.raises(RuntimeError, match="Gemini file processing failed"):
            MODULE.upload_pdf_to_gemini(client, Path("/tmp/whatever.pdf"))


class TestExtractAllRecordsViaGemini:
    def test_success_returns_records_list(self, mock_sleep):
        client = mock.MagicMock()
        records = [_record(patient_name="A")]
        client.models.generate_content.return_value = SimpleNamespace(
            text=json.dumps({"records": records})
        )
        result = MODULE.extract_all_records_via_gemini(client, object())
        assert result == records

    def test_malformed_json_retries_then_raises(self, mock_sleep):
        client = mock.MagicMock()
        client.models.generate_content.return_value = SimpleNamespace(text="{{{not json")
        with pytest.raises(RuntimeError, match="All .* attempts failed"):
            MODULE.extract_all_records_via_gemini(client, object())
        assert client.models.generate_content.call_count == MODULE.MAX_RETRIES

    def test_empty_records_retries_then_raises(self, mock_sleep):
        client = mock.MagicMock()
        client.models.generate_content.return_value = SimpleNamespace(
            text=json.dumps({"records": []})
        )
        with pytest.raises(RuntimeError, match="All .* attempts failed"):
            MODULE.extract_all_records_via_gemini(client, object())

    def test_non_list_records_field_retries_then_raises(self, mock_sleep):
        client = mock.MagicMock()
        client.models.generate_content.return_value = SimpleNamespace(
            text=json.dumps({"records": "not-a-list"})
        )
        with pytest.raises(RuntimeError, match="All .* attempts failed"):
            MODULE.extract_all_records_via_gemini(client, object())

    def test_transient_error_then_success(self, mock_sleep):
        client = mock.MagicMock()
        records = [_record(patient_name="A")]
        client.models.generate_content.side_effect = [
            RuntimeError("503 transient"),
            SimpleNamespace(text=json.dumps({"records": records})),
        ]
        result = MODULE.extract_all_records_via_gemini(client, object())
        assert result == records
        assert client.models.generate_content.call_count == 2
