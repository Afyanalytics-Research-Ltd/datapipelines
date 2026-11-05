"""
dags/document_pipeline.py (gdrive_landingai_extract): the run params --
schemas inline or from a file, the splitting / file-selection / output knobs
-- and that a run honours them end to end. LandingAI and Google Drive are
faked; no network.
"""
from __future__ import annotations

import csv
import json
import re

import pytest

from tests.helpers import DAGS_DIR, load_dag_module

dp = load_dag_module("document_pipeline")

SCHEMA = {"type": "object", "properties": {"patient_name": {"type": ["string", "null"]}}}
OTHER_SCHEMA = {"type": "object", "properties": {"diagnosis": {"type": ["string", "null"]}}}
CUSTOM_BOUNDARY = {
    "type": "object",
    "properties": {**dp.BOUNDARY_SCHEMA["properties"], "ward": {"type": ["string", "null"]}},
}

PAGES = {
    "1": "# Outpatient Clinical Consultation Form\nPatient Demographics\n| Patient Name | John Otieno |",
    "2": "Progress notes, continued.",
    "3": "# Outpatient Clinical Consultation Form\nPatient Demographics\n| Patient Name | Mary Akinyi |",
    "4": "Lab results.",
}


# --------------------------------------------------------------------------
# load_json_param
# --------------------------------------------------------------------------
@pytest.fixture
def schema_file(tmp_path):
    path = tmp_path / "schema.json"
    path.write_text(json.dumps(SCHEMA), encoding="utf-8")
    return path


def test_blob_as_dict():
    assert dp.load_json_param(SCHEMA, None, "schema") == SCHEMA


def test_blob_as_json_text():
    assert dp.load_json_param(json.dumps(SCHEMA), None, "schema") == SCHEMA


def test_blob_as_double_encoded_json_text():
    assert dp.load_json_param(json.dumps(json.dumps(SCHEMA)), None, "schema") == SCHEMA


def test_blob_wins_over_path(schema_file):
    assert dp.load_json_param(OTHER_SCHEMA, str(schema_file), "schema") == OTHER_SCHEMA


def test_path_used_when_blob_empty(schema_file):
    for empty in (None, "", "   ", {}):
        assert dp.load_json_param(empty, str(schema_file), "schema") == SCHEMA


def test_plain_text_blob_is_read_as_a_path(schema_file):
    assert dp.load_json_param(str(schema_file), None, "schema") == SCHEMA


def test_relative_path_resolves_against_repo_root(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)  # not the repo: falls through to the repo root
    loaded = dp.load_json_param(None, "dags/jsons/schema.json", "schema")
    assert loaded == json.loads((DAGS_DIR / "jsons" / "schema.json").read_text(encoding="utf-8"))


def test_default_when_nothing_given():
    assert dp.load_json_param(None, "", "boundary_schema", default=dp.BOUNDARY_SCHEMA) is dp.BOUNDARY_SCHEMA


def test_nothing_given_and_no_default_raises():
    with pytest.raises(ValueError, match="schema: give it inline"):
        dp.load_json_param(None, None, "schema")


def test_missing_file_raises(tmp_path):
    with pytest.raises(ValueError, match="file not found"):
        dp.load_json_param(None, str(tmp_path / "nope.json"), "schema")


def test_bad_json_text_raises():
    with pytest.raises(ValueError, match="schema: not valid JSON"):
        dp.load_json_param('{"type": "object",', None, "schema")


def test_bad_json_file_raises(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{oops", encoding="utf-8")
    with pytest.raises(ValueError, match="is not valid JSON"):
        dp.load_json_param(None, str(path), "schema")


@pytest.mark.parametrize("value", [[1, 2], {"type": "object"}, {"properties": []}])
def test_non_schema_json_raises(value):
    with pytest.raises(ValueError, match="'properties' map"):
        dp.load_json_param(value, None, "schema")


# --------------------------------------------------------------------------
# config_from_params
# --------------------------------------------------------------------------
def test_empty_params_are_the_old_defaults():
    cfg = dp.config_from_params({})
    d = dp.DEFAULT_CONFIG
    assert cfg.schema == json.loads((DAGS_DIR / "jsons" / "patient_docs_outpatient_schema.json").read_text(encoding="utf-8"))
    assert cfg.boundary_schema is dp.BOUNDARY_SCHEMA
    for name in ("record_start_markers", "record_start_min_matches", "max_pages_per_record",
                 "boundary_score_threshold", "page_parse_workers", "output_dir", "state_dir",
                 "parse_model", "extract_model", "move_processed", "save_json"):
        assert getattr(cfg, name) == getattr(d, name), name


def test_blank_values_fall_back_to_defaults():
    blank = {"schema": None, "schema_path": "", "boundary_schema": "", "boundary_schema_path": None,
             "file_ids": [], "record_start_markers": [], "output_dir": ""}
    cfg = dp.config_from_params(blank)
    assert cfg.boundary_schema is dp.BOUNDARY_SCHEMA
    assert cfg.output_dir == dp.DEFAULT_CONFIG.output_dir
    assert cfg.record_start_markers == dp.DEFAULT_CONFIG.record_start_markers


def test_overrides(tmp_path):
    cfg = dp.config_from_params({
        "schema": SCHEMA,
        "boundary_schema": json.dumps(CUSTOM_BOUNDARY),
        "file_ids": "a1, b2\nc3",
        "name_contains": "  Siaya ",
        "max_files": "2",
        "move_processed": "false",
        "save_json": False,
        "record_start_markers": "Admission Form, Patient Details",
        "record_start_min_matches": 1,
        "max_pages_per_record": 4,
        "boundary_score_threshold": 3,
        "boundary_only_when_ambiguous": "0",
        "page_parse_workers": 0,  # floored to 1
        "extract_model": "extract-20250930",
        "state_dir": str(tmp_path / "st"),
    })
    assert cfg.schema == SCHEMA
    assert cfg.boundary_schema == CUSTOM_BOUNDARY
    assert cfg.file_ids == ["a1", "b2", "c3"]
    assert cfg.name_contains == "Siaya"
    assert cfg.max_files == 2
    assert cfg.move_processed is False and cfg.save_json is False
    assert cfg.record_start_markers == ["admission form", "patient details"]
    assert cfg.record_start_min_matches == 1
    assert cfg.max_pages_per_record == 4
    assert cfg.boundary_score_threshold == 3
    assert cfg.boundary_only_when_ambiguous is False
    assert cfg.page_parse_workers == 1
    assert cfg.extract_model == "extract-20250930"
    assert cfg.state_dir == str(tmp_path / "st")


def test_min_matches_clamped_to_marker_count():
    cfg = dp.config_from_params({"schema": SCHEMA, "record_start_markers": ["only one"],
                                 "record_start_min_matches": 5})
    assert cfg.record_start_min_matches == 1


def test_boundary_schema_without_splitter_fields_warns(caplog):
    with caplog.at_level("WARNING", logger="landingai_extract"):
        dp.config_from_params({"schema": SCHEMA, "boundary_schema": SCHEMA})
    assert "is_first_page_of_record" in caplog.text


def test_bad_schema_param_fails_fast():
    with pytest.raises(ValueError, match="schema"):
        dp.config_from_params({"schema": "{broken"})


def test_schema_hashes_track_content():
    a = dp.config_from_params({"schema": SCHEMA})
    b = dp.config_from_params({"schema": json.dumps(SCHEMA)})
    c = dp.config_from_params({"schema": OTHER_SCHEMA, "boundary_schema": CUSTOM_BOUNDARY})
    assert a.schema_hash == b.schema_hash != c.schema_hash
    assert a.boundary_schema_hash == dp.DEFAULT_BOUNDARY_SCHEMA_HASH != c.boundary_schema_hash


# --------------------------------------------------------------------------
# splitting knobs reach the splitter
# --------------------------------------------------------------------------
def _pages(texts):
    return [{"page": i, "markdown": t} for i, t in enumerate(texts, start=1)]


def test_custom_markers_drive_boundaries():
    pages = _pages(["ADMISSION FORM page", "notes", "ADMISSION FORM page", "notes"])
    assert [r["page_numbers"] for r in dp.group_pages_into_records(pages, {})] == [[1, 2, 3, 4]]
    cfg = dp.PipelineConfig(record_start_markers=["admission form"], record_start_min_matches=1)
    assert [r["page_numbers"] for r in dp.group_pages_into_records(pages, {}, cfg)] == [[1, 2], [3, 4]]


def test_max_pages_per_record_param():
    pages = _pages(["Page 1 of 4", "Page 2 of 4", "Page 3 of 4", "Page 4 of 4"])
    assert len(dp.group_pages_into_records(pages, {})) == 1
    cfg = dp.PipelineConfig(max_pages_per_record=2)
    recs = dp.group_pages_into_records(pages, {}, cfg)
    assert [r["page_numbers"] for r in recs] == [[1], [2], [3], [4]]


def test_threshold_param():
    meta = {"2": {"is_first_page_of_record": True}}
    pages = _pages(["intro", "something", "more"])
    assert [r["page_numbers"] for r in dp.group_pages_into_records(pages, meta)] == [[1], [2, 3]]
    cfg = dp.PipelineConfig(boundary_score_threshold=3)
    assert [r["page_numbers"] for r in dp.group_pages_into_records(pages, meta, cfg)] == [[1, 2, 3]]


def test_classify_every_page_when_not_only_ambiguous():
    md = PAGES["1"]
    assert dp.needs_classification(md) is False
    assert dp.needs_classification(md, dp.PipelineConfig(boundary_only_when_ambiguous=False)) is True


# --------------------------------------------------------------------------
# select_files
# --------------------------------------------------------------------------
class FakeDrive:
    def __init__(self, files):
        self.by_id = {f["id"]: f for f in files}
        self.moves = []
        self.listed_folders = []

    def files(self):
        return self

    def get(self, fileId, fields=None):
        return _Exec(self.by_id[fileId])

    def list(self, q, fields, pageSize, pageToken):
        self.listed_folders.append(q.split("'")[1])
        return _Exec({"files": list(self.by_id.values())})


class _Exec:
    def __init__(self, value):
        self.value = value

    def execute(self):
        return self.value


DRIVE_FILES = [
    {"id": f"id{n}", "name": name, "mimeType": "application/pdf", "parents": ["src"]}
    for n, name in enumerate(["siaya_b.pdf", "kisumu.pdf", "Siaya_a.pdf", "siaya_c.pdf"])
]


def test_select_files_filters_sorts_and_caps():
    drive = FakeDrive(DRIVE_FILES)
    cfg = dp.PipelineConfig(source_folder_id="folderX", name_contains="siaya", max_files=2)
    assert [f["name"] for f in dp.select_files(drive, cfg)] == ["Siaya_a.pdf", "siaya_b.pdf"]
    assert drive.listed_folders == ["folderX"]


def test_select_files_by_id_skips_listing():
    drive = FakeDrive(DRIVE_FILES)
    picked = dp.select_files(drive, dp.PipelineConfig(file_ids=["id1"]))
    assert [f["name"] for f in picked] == ["kisumu.pdf"] and drive.listed_folders == []


def test_select_files_falls_back_to_variable(monkeypatch):
    monkeypatch.setenv("GDRIVE_SOURCE_FOLDER_ID", "fromVariable")
    drive = FakeDrive(DRIVE_FILES)
    dp.select_files(drive, dp.PipelineConfig())
    assert drive.listed_folders == ["fromVariable"]


# --------------------------------------------------------------------------
# process_one_file end to end (cached pages, fake LandingAI)
# --------------------------------------------------------------------------
class FakeLandingAI:
    """Stands in for requests.post to ade/extract and records every call."""

    def __init__(self):
        self.calls = []

    def __call__(self, url, headers=None, files=None, data=None, timeout=None):
        schema = json.loads(data["schema"])
        md = files["markdown"][1]
        self.calls.append({"schema": schema, "model": data["model"], "markdown": md})
        if "is_first_page_of_record" in schema["properties"]:
            extraction = {"is_first_page_of_record": False}
        else:
            name = "Mary Akinyi" if "Mary" in md else "John Otieno"
            extraction = {key: name for key in schema["properties"]}
        return _Resp({"extraction": extraction})

    def boundary_calls(self):
        return [c for c in self.calls if "is_first_page_of_record" in c["schema"]["properties"]]

    def record_calls(self):
        return [c for c in self.calls if "is_first_page_of_record" not in c["schema"]["properties"]]


class _Resp:
    ok = True
    status_code = 200

    def __init__(self, body):
        self.body = body
        self.text = json.dumps(body)

    def json(self):
        return self.body


class MoveRecorder:
    def __init__(self):
        self.moves = []

    def __call__(self, service, file_id, dest):
        self.moves.append((file_id, dest))


@pytest.fixture
def run_env(tmp_path, monkeypatch):
    monkeypatch.setenv("VA_API_KEY", "test-key")
    api = FakeLandingAI()
    monkeypatch.setattr(dp.requests, "post", api)
    mover = MoveRecorder()
    monkeypatch.setattr(dp, "move_file", mover)
    monkeypatch.setattr(dp, "download_file", lambda *a, **k: pytest.fail("pages are cached"))

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "f1.json").write_text(json.dumps({
        "file_id": "f1", "filename": "batch.pdf", "total_pages": 4, "file_done": False,
        "pages": PAGES, "page_errors": {}, "page_meta": {}, "records": {},
    }), encoding="utf-8")

    def make_cfg(**params):
        base = {"schema": SCHEMA, "state_dir": str(state_dir),
                "output_dir": str(tmp_path / "out"), "json_output_dir": str(tmp_path / "out" / "json")}
        return dp.config_from_params({**base, **params})

    return {"api": api, "mover": mover, "state_dir": state_dir, "tmp": tmp_path, "cfg": make_cfg}


FILE_META = {"id": "f1", "name": "batch.pdf", "mimeType": "application/pdf"}


def _state(env):
    return json.loads((env["state_dir"] / "f1.json").read_text(encoding="utf-8"))


def _csv_rows(env):
    with open(env["tmp"] / "out" / "results.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_run_uses_the_param_schemas_and_dirs(run_env):
    cfg = run_env["cfg"](boundary_schema=CUSTOM_BOUNDARY, extract_model="m-x")
    results = dp.process_one_file(None, FILE_META, "doneFolder", cfg)

    assert sorted((r["page_range"], r["ok"]) for r in results) == [("1-2", True), ("3-4", True)]
    api = run_env["api"]
    assert [c["schema"] for c in api.record_calls()] == [SCHEMA, SCHEMA]
    assert len(api.boundary_calls()) == 2  # pages 2 and 4 are ambiguous
    assert all(c["schema"] == CUSTOM_BOUNDARY for c in api.boundary_calls())
    assert {c["model"] for c in api.calls} == {"m-x"}

    rows = _csv_rows(run_env)
    assert sorted(r["patient_label"] for r in rows) == ["John Otieno", "Mary Akinyi"]
    assert len(list((run_env["tmp"] / "out" / "json").glob("batch_record*.json"))) == 2

    state = _state(run_env)
    assert state["file_done"] is True
    assert state["schema_hash"] == cfg.schema_hash
    assert state["boundary_schema_hash"] == cfg.boundary_schema_hash
    assert run_env["mover"].moves == [("f1", "doneFolder")]


def test_rerun_with_same_params_does_nothing(run_env):
    cfg = run_env["cfg"]()
    dp.process_one_file(None, FILE_META, None, cfg)
    calls = len(run_env["api"].calls)
    assert dp.process_one_file(None, FILE_META, None, cfg) == []
    assert len(run_env["api"].calls) == calls


def test_no_dest_folder_leaves_file_in_place(run_env):
    dp.process_one_file(None, FILE_META, None, run_env["cfg"](save_json=False))
    assert run_env["mover"].moves == []
    assert not (run_env["tmp"] / "out" / "json").exists()
    assert all(r["json_path"] == "" for r in _csv_rows(run_env))


def test_new_schema_re_extracts_a_done_file(run_env):
    dp.process_one_file(None, FILE_META, None, run_env["cfg"]())
    api = run_env["api"]
    api.calls.clear()

    results = dp.process_one_file(None, FILE_META, None, run_env["cfg"](schema=OTHER_SCHEMA))
    assert len(results) == 2
    assert [c["schema"] for c in api.record_calls()] == [OTHER_SCHEMA, OTHER_SCHEMA]
    assert api.boundary_calls() == []  # same boundary schema: page_meta reused
    assert _state(run_env)["schema_hash"] == dp.json_hash(OTHER_SCHEMA)


def test_new_boundary_schema_reclassifies_pages(run_env):
    dp.process_one_file(None, FILE_META, None, run_env["cfg"]())
    api = run_env["api"]
    api.calls.clear()

    # same extraction schema, so only a reset file_done makes it run again
    state = _state(run_env)
    state["file_done"] = False
    (run_env["state_dir"] / "f1.json").write_text(json.dumps(state), encoding="utf-8")

    dp.process_one_file(None, FILE_META, None, run_env["cfg"](boundary_schema=CUSTOM_BOUNDARY))
    assert len(api.boundary_calls()) == 2
    assert api.record_calls() == []  # records unchanged and done under this schema


def test_legacy_state_without_hashes_is_treated_as_current(run_env):
    """State written before this change has no hashes: the default schemas
    must not re-bill anything."""
    cfg = run_env["cfg"]()
    dp.process_one_file(None, FILE_META, None, cfg)
    state = _state(run_env)
    state.pop("schema_hash")
    state.pop("boundary_schema_hash")
    state["file_done"] = False
    for rec in state["records"].values():
        rec.pop("schema_hash")
    (run_env["state_dir"] / "f1.json").write_text(json.dumps(state), encoding="utf-8")
    run_env["api"].calls.clear()

    dp.process_one_file(None, FILE_META, None, cfg)
    assert run_env["api"].calls == []


def test_regrouped_record_is_not_skipped(run_env):
    """A record index that now covers different pages is extracted again."""
    dp.process_one_file(None, FILE_META, None, run_env["cfg"]())
    state = _state(run_env)
    state["file_done"] = False
    (run_env["state_dir"] / "f1.json").write_text(json.dumps(state), encoding="utf-8")
    run_env["api"].calls.clear()

    dp.process_one_file(None, FILE_META, None, run_env["cfg"](max_pages_per_record=1))
    assert len(run_env["api"].record_calls()) == 4


# --------------------------------------------------------------------------
# Snowflake load
# --------------------------------------------------------------------------
def test_sf_identifier():
    assert dp.sf_identifier("Simple neonatal note extraction") == "SIMPLE_NEONATAL_NOTE_EXTRACTION"
    assert dp.sf_identifier("Birth weight (kg)") == "BIRTH_WEIGHT_KG"
    assert dp.sf_identifier("1st visit") == "_1ST_VISIT"
    assert dp.sf_identifier("  ") == ""


@pytest.mark.parametrize("prop, expected", [
    ({"type": "string"}, "VARCHAR"),
    ({"type": ["string", "null"]}, "VARCHAR"),
    ({"type": "integer"}, "NUMBER(38,0)"),
    ({"type": ["number", "null"]}, "FLOAT"),
    ({"type": "boolean"}, "BOOLEAN"),
    ({"type": "array", "items": {"type": "string"}}, "VARIANT"),
    ({"type": "object"}, "VARIANT"),
    ({"type": ["string", "integer"]}, "VARIANT"),
    ({"enum": ["M", "F"]}, "VARCHAR"),
    ({"anyOf": [{"type": "string"}]}, "VARIANT"),
])
def test_sf_type_for(prop, expected):
    assert dp.sf_type_for(prop) == expected


def test_schema_columns_handle_collisions():
    schema = {"properties": {
        "status": {"type": "string"}, "Birth weight": {"type": "number"},
        "birth-weight": {"type": "string"}, "diagnoses": {"type": "array"},
    }}
    assert dp.schema_columns(schema) == [
        ("F_STATUS", "status", "VARCHAR"),
        ("BIRTH_WEIGHT", "Birth weight", "FLOAT"),
        ("BIRTH_WEIGHT_2", "birth-weight", "VARCHAR"),
        ("DIAGNOSES", "diagnoses", "VARIANT"),
    ]


def test_real_schema_file_maps_to_columns():
    schema = json.loads((DAGS_DIR / "jsons" / "schema.json").read_text(encoding="utf-8"))
    cols = {c: t for c, _, t in dp.schema_columns(schema)}
    assert cols["BABY_NAME"] == "VARCHAR" and cols["DIAGNOSES"] == "VARIANT"
    assert len(cols) == len(schema["properties"])


@pytest.mark.parametrize("filename", [
    "siaya_outpatient_schema.json",
    "patient_docs_outpatient_schema.json",
])
def test_outpatient_schema_files_are_valid_and_complete(filename):
    """Both outpatient schema files must be valid JSON, loadable, and contain the
    clinical fields docs_to_events_raw.py reads from EXTRACTED_FORMS columns."""
    schema = json.loads((DAGS_DIR / "jsons" / filename).read_text(encoding="utf-8"))
    required_fields = {
        "patient_name", "age", "sex", "blood_pressure", "heart_rate",
        "temperature", "weight", "height", "body_mass_index",
        "chief_complaints", "history_of_present_illness",
        "general_appearance", "doctors_notes",
    }
    missing = required_fields - set(schema.get("properties", {}).keys())
    assert not missing, f"{filename} missing fields: {missing}"
    assert schema.get("type") == "object"


def test_snowflake_target_defaults(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "hospitals")
    titled = dp.PipelineConfig(schema={"title": "Simple neonatal note extraction", "properties": {}})
    assert titled.snowflake_target == ("HOSPITALS", "SIMPLE_NEONATAL_NOTE_EXTRACTION", "EXTRACTED_RECORDS")
    untitled = dp.PipelineConfig(schema=SCHEMA)
    assert untitled.snowflake_target == ("HOSPITALS", "LANDINGAI_EXTRACT", "EXTRACTED_RECORDS")
    named = dp.config_from_params({"schema": SCHEMA, "snowflake_database": "analytics",
                                   "snowflake_schema": "my-extracts", "snowflake_table": "neo notes"})
    assert named.snowflake_target == ("ANALYTICS", "MY_EXTRACTS", "NEO_NOTES")


def test_snowflake_target_needs_a_database(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_DATABASE", raising=False)
    cfg = dp.PipelineConfig(schema=SCHEMA)
    with pytest.raises(ValueError, match="snowflake_database"):
        cfg.snowflake_target
    assert "not configured" in cfg.describe()
    assert "snowflake=off" in dp.PipelineConfig(schema=SCHEMA, push_to_snowflake=False).describe()


def test_push_to_snowflake_param():
    assert dp.config_from_params({"schema": SCHEMA}).push_to_snowflake is dp.PUSH_TO_SNOWFLAKE
    assert dp.config_from_params({"schema": SCHEMA, "push_to_snowflake": "false"}).push_to_snowflake is False


def test_sf_cast_uses_the_column_type():
    assert dp.sf_cast("v", "TEXT").startswith("IFF(IS_NULL_VALUE(v), NULL")
    assert dp.sf_cast("v", "NUMBER", 2).startswith("TRY_TO_NUMBER(")
    assert dp.sf_cast("v", "NUMBER", 2).endswith(", 38, 2)")
    assert dp.sf_cast("v", "FLOAT").startswith("TRY_TO_DOUBLE(")
    assert dp.sf_cast("v", "BOOLEAN").startswith("IFF(IS_BOOLEAN(v)")
    assert dp.sf_cast("v", "DATE").startswith("TRY_TO_DATE(")
    assert dp.sf_cast("v", "TIMESTAMP_NTZ").startswith("TRY_TO_TIMESTAMP(")
    assert dp.sf_cast("v", "VARIANT") == "IFF(IS_NULL_VALUE(v), NULL, v)"
    assert dp.sf_cast("v", "ARRAY") == "IFF(IS_ARRAY(v), v::ARRAY, NULL)"


def test_batches_split_by_size():
    assert dp._batches(["aa", "bb", "cc"], 4) == [["aa", "bb"], ["cc"]]
    assert dp._batches(["toolong"], 3) == [["toolong"]]
    assert dp._batches([], 10) == []


def test_snowflake_docs_current_records_only(run_env):
    cfg = run_env["cfg"]()
    dp.process_one_file(None, FILE_META, None, cfg)
    state = _state(run_env)
    state["records"]["9"] = {**state["records"]["1"]}          # left over from an older grouping
    state["records"]["2"]["schema_hash"] = "someotherschema"   # extracted under another schema
    (run_env["state_dir"] / "f1.json").write_text(json.dumps(state), encoding="utf-8")

    docs = dp.snowflake_docs(cfg, ["f1", "not-processed"])
    assert [d["record_key"] for d in docs] == ["f1:1"]
    doc = docs[0]
    assert doc["filename"] == "batch.pdf" and doc["page_range"] == "1-2"
    assert doc["extraction"]["patient_name"] == "John Otieno"
    assert doc["schema_hash"] == cfg.schema_hash


class FakeSnowflake:
    """INFORMATION_SCHEMA + statements, enough to drive load_to_snowflake."""

    SF_TYPES = {"VARCHAR": ("TEXT", None), "NUMBER(38,0)": ("NUMBER", 0), "FLOAT": ("FLOAT", None),
                "BOOLEAN": ("BOOLEAN", None), "VARIANT": ("VARIANT", None),
                "TIMESTAMP_NTZ": ("TIMESTAMP_NTZ", None)}

    def __init__(self, schemas=(), tables=None):
        self.schemas = set(schemas)
        self.tables = {k: dict(v) for k, v in (tables or {}).items()}  # (schema, table) -> {col: (type, scale)}
        self.sql = []
        self.staged = []
        self.committed = False
        self.rolled_back = False
        self.fail_on = None

    def fetchall(self, sql, params=None):
        self.sql.append(sql)
        if "SCHEMATA" in sql:
            return [(1,)] if params[0] in self.schemas else []
        if "COLUMNS" in sql:
            cols = self.tables.get(tuple(params), {})
            return [(c, t, s) for c, (t, s) in cols.items()]
        raise AssertionError(sql)

    def execute(self, sql, params=None):
        self.sql.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("boom")
        m = re.match(r"CREATE SCHEMA IF NOT EXISTS \w+\.(\w+)", sql)
        if m:
            self.schemas.add(m.group(1))
        m = re.match(r"CREATE TABLE IF NOT EXISTS \w+\.(\w+)\.(\w+) \((.*)\)$", sql)
        if m:
            cols = {}
            for part in m.group(3).split(", "):
                col, typ = part.split(" ", 1)
                cols[col] = self.SF_TYPES[typ]
            self.tables[(m.group(1), m.group(2))] = cols
        m = re.match(r"ALTER TABLE \w+\.(\w+)\.(\w+) ADD COLUMN (\w+) (.+)$", sql)
        if m:
            self.tables[(m.group(1), m.group(2))][m.group(3)] = self.SF_TYPES[m.group(4)]
        if sql.startswith("DELETE"):
            return 3
        if sql.startswith("INSERT INTO") and "SELECT" in sql:
            return len(self.staged)
        return 0

    def executemany(self, sql, rows):
        self.sql.append(sql)
        self.staged.extend(json.loads(r[0]) for r in rows)
        return len(rows)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


@pytest.fixture
def sf_cfg(run_env, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "HOSPITALS")
    titled = {**SCHEMA, "title": "Neo notes",
              "properties": {**SCHEMA["properties"], "weight": {"type": "number"}}}
    cfg = run_env["cfg"](schema=titled)
    dp.process_one_file(None, FILE_META, None, cfg)
    return cfg


def test_load_creates_schema_and_table_from_the_json_schema(sf_cfg):
    sf = FakeSnowflake()
    out = dp.load_to_snowflake(sf_cfg, ["f1"], sf=sf)

    assert out == {"table": "HOSPITALS.NEO_NOTES.EXTRACTED_RECORDS", "rows": 2, "replaced": 3, "files": 1}
    assert "CREATE SCHEMA IF NOT EXISTS HOSPITALS.NEO_NOTES" in sf.sql
    cols = sf.tables[("NEO_NOTES", "EXTRACTED_RECORDS")]
    assert list(cols)[: len(dp.SF_META_COLUMNS)] == list(dp.SF_META_COLUMNS)
    assert cols["PATIENT_NAME"] == ("TEXT", None) and cols["WEIGHT"] == ("FLOAT", None)
    assert [d["record_key"] for d in sf.staged] == ["f1:1", "f1:2"]
    insert = next(s for s in sf.sql if s.startswith("INSERT INTO HOSPITALS.NEO_NOTES.EXTRACTED_RECORDS"))
    assert "GET(d:extraction, 'patient_name')" in insert and "TRY_TO_DOUBLE" in insert
    delete = next(s for s in sf.sql if s.startswith("DELETE"))
    assert sf.sql.index("BEGIN") < sf.sql.index(delete) < sf.sql.index(insert)
    assert sf.committed and not sf.rolled_back


def test_load_uses_an_existing_schema_and_table(sf_cfg):
    existing = {c: FakeSnowflake.SF_TYPES[t] for c, t in dp.SF_META_COLUMNS.items()}
    existing["PATIENT_NAME"] = ("TEXT", None)
    existing["WEIGHT"] = ("NUMBER", 1)       # their type, not ours: cast to it
    existing["THEIR_OWN"] = ("TEXT", None)   # left alone
    sf = FakeSnowflake(schemas={"NEO_NOTES"}, tables={("NEO_NOTES", "EXTRACTED_RECORDS"): existing})
    dp.load_to_snowflake(sf_cfg, ["f1"], sf=sf)

    assert not any(s.startswith(("CREATE SCHEMA", "CREATE TABLE", "ALTER TABLE")) for s in sf.sql)
    insert = next(s for s in sf.sql if s.startswith("INSERT INTO HOSPITALS.NEO_NOTES.EXTRACTED_RECORDS"))
    assert "TRY_TO_NUMBER(" in insert and ", 38, 1)" in insert
    assert "THEIR_OWN" not in insert


def test_load_adds_only_missing_columns(sf_cfg):
    existing = {"FILE_ID": ("TEXT", None), "PATIENT_NAME": ("TEXT", None)}
    sf = FakeSnowflake(schemas={"NEO_NOTES"}, tables={("NEO_NOTES", "EXTRACTED_RECORDS"): existing})
    dp.load_to_snowflake(sf_cfg, ["f1"], sf=sf)
    added = [s.split(" ADD COLUMN ")[1].split()[0] for s in sf.sql if s.startswith("ALTER TABLE")]
    assert "FILE_ID" not in added and "PATIENT_NAME" not in added
    assert "WEIGHT" in added and "RECORD_KEY" in added and "EXTRACTION" in added


def test_load_rolls_back_on_failure(sf_cfg):
    sf = FakeSnowflake()
    sf.fail_on = "INSERT INTO HOSPITALS.NEO_NOTES.EXTRACTED_RECORDS"
    with pytest.raises(RuntimeError, match="boom"):
        dp.load_to_snowflake(sf_cfg, ["f1"], sf=sf)
    assert sf.rolled_back and not sf.committed


def test_load_with_nothing_extracted_touches_nothing(run_env, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "HOSPITALS")
    sf = FakeSnowflake()
    out = dp.load_to_snowflake(run_env["cfg"](), ["f1"], sf=sf)
    assert out["rows"] == 0 and sf.sql == []
