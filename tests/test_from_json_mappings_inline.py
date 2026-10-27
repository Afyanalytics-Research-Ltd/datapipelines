"""
from_json_mappings_to_snowflake: the mappings can be a JSON FILE or the JSON ITSELF
(text, bytes, or an already-parsed object) — in the loader, the CLI and the DAG.

Nothing here reaches the network or Snowflake: the extraction client is a fake, and the
Snowflake/S3 helpers the module imports are stubbed when their packages aren't installed.
"""
from __future__ import annotations

import copy
import importlib
import io
import json
import sys
import threading
import types
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent


# ─── import the pipeline module (stub its Snowflake/S3 plumbing if the env lacks it) ──────────

def _import_fj():
    try:
        import facility_to_snowflake_fast_resume  # noqa: F401
    except Exception:
        stub = types.ModuleType("facility_to_snowflake_fast_resume")
        stub.S3_BUCKET, stub.SF_DB, stub.SF_FILE_FORMAT, stub.SF_STAGE = "bucket", "AFYA_DB", "FF", "STAGE"

        class SnowflakeClient:                      # never used: every test runs dry
            def __init__(self, *a, **k):
                raise AssertionError("Snowflake must not be touched in these tests")
        stub.SnowflakeClient = SnowflakeClient
        stub._dumps_bytes = lambda obj: json.dumps(obj).encode()
        stub._s3_client = lambda: None
        stub._strip_secrets = lambda row: dict(row)
        sys.modules["facility_to_snowflake_fast_resume"] = stub
    return importlib.import_module("from_json_mappings_to_snowflake")


fj = _import_fj()

REAL_EXPORTS = [p for p in (REPO / "silverwood_data_mappings.json",
                            REPO / "dags" / "jsons" / "silverwood_data_mappings.json",
                            REPO / "dags" / "jsons" / "wema_medical_data_mappings.json") if p.is_file()]


# ─── fixtures ──────────────────────────────────────────────────────────────────────────────

def minimal_doc() -> dict:
    """A small but complete mappings export: two source tables, one split into two V3 tables by
    a "where", nested rule objects (map / steps / lookup), a JSON-text rule and a transform."""
    return {
        "connection": {"id": 35, "name": "Test Clinic", "exported_at": "2026-10-09T05:55:51+00:00"},
        "tables": [
            {"source table": "person", "v3 model": "App\\Models\\Reception\\Patient",
             "v3 table": "reception_patients", "v3 service": "reception-service"},
            {"source table": "services", "v3 model": "App\\Models\\Inventory\\Product",
             "v3 table": "inventory_products", "where": {"column": "kind", "in": ["Drug"]}},
            {"source table": "services", "v3 model": "App\\Models\\Evaluation\\Procedure",
             "v3 table": "evaluation_procedures", "where": {"column": "kind", "in": ["Procedure", "Lab"]}},
        ],
        "fields": [
            {"table": "person", "v3_table": "reception_patients", "model": "App\\Models\\Reception\\Patient",
             "column": "fname", "field": "first_name", "transform": "trim"},
            {"table": "person", "v3_table": "reception_patients", "model": "App\\Models\\Reception\\Patient",
             "column": "sex", "field": "gender", "rule": {"map": {"M": "male", "F": "female"}, "default": "other"}},
            {"table": "person", "v3_table": "reception_patients", "model": "App\\Models\\Reception\\Patient",
             "column": "full", "field": "last_name", "rule": json.dumps({"part": "rest"})},
            {"table": "person", "v3_table": "reception_patients", "model": "App\\Models\\Reception\\Patient",
             "column": "nested", "field": "note", "rule": {"steps": [{"op": "uppercase"}]}},
            {"table": "services", "v3_table": "inventory_products", "model": "App\\Models\\Inventory\\Product",
             "column": "label", "field": "name"},
            {"table": "services", "v3_table": "evaluation_procedures", "model": "App\\Models\\Evaluation\\Procedure",
             "column": "label", "field": "name", "transform": "lowercase"},
        ],
    }


PERSON_ROWS = [
    {"id": 1, "fname": "  Achieng ", "sex": "F", "full": "Achieng Mary Otieno", "nested": "jambo"},
    {"id": 2, "fname": "Wanjiru", "sex": "X", "full": "Wanjiru", "nested": None},
]
SERVICE_ROWS = [
    {"id": 10, "kind": "Drug", "label": "Paracetamol"},
    {"id": 11, "kind": "Lab", "label": "Full Blood Count"},
    {"id": 12, "kind": "Other", "label": "Parking"},
]


class FakeClient:
    """Stands in for ExtractionClient: no HTTP."""
    instances: list = []

    def __init__(self, connection_id, *, per_page=500, page_workers=4, **_):
        self.connection_id = connection_id
        FakeClient.instances.append(self)

    def connection(self):
        return {"name": "Test Clinic"}

    def fetch_all(self, table):
        return copy.deepcopy({"person": PERSON_ROWS, "services": SERVICE_ROWS}.get(table, []))


@pytest.fixture
def doc():
    return minimal_doc()


@pytest.fixture
def doc_file(tmp_path, doc):
    p = tmp_path / "test_data_mappings.json"
    p.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return p


@pytest.fixture
def fake_client(monkeypatch):
    FakeClient.instances = []
    monkeypatch.setattr(fj, "ExtractionClient", FakeClient)
    return FakeClient


def model_summary(spec):
    return [(m.source_table, m.namespace, m.table, m.service, m.where, m.fields) for m in spec.models]


# ─── looks_like_json ───────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("text", ['{"a": 1}', "  {", "\n\t{", "\ufeff{", "[1]", '"x.json"', "  [", '\ufeff "x"'])
def test_looks_like_json_true(text):
    assert fj.looks_like_json(text)


@pytest.mark.parametrize("text", ["silverwood_data_mappings.json", "/opt/x.json", "C:\\x.json", "", "   ",
                                  "null", "123", "true", "./{weird}.json", "~/m.json"])
def test_looks_like_json_false(text):
    assert not fj.looks_like_json(text)


# ─── every input form gives the same mappings ──────────────────────────────────────────────

def test_file_text_dict_bytes_all_equal(doc, doc_file):
    by_path = fj.Mappings.load(doc_file)
    variants = {
        "str path": str(doc_file),
        "compact text": json.dumps(doc, separators=(",", ":")),
        "pretty text": json.dumps(doc, indent=4),
        "crlf text": json.dumps(doc, indent=2).replace("\n", "\r\n"),
        "padded text": "\n\n   " + json.dumps(doc) + "  \n\t",
        "bom text": "\ufeff" + json.dumps(doc),
        "ascii-escaped text": json.dumps(doc, ensure_ascii=True),
        "dict": doc,
        "bytes": json.dumps(doc).encode("utf-8"),
        "bom bytes": "\ufeff".encode("utf-8") + json.dumps(doc).encode("utf-8"),
        "bytearray": bytearray(json.dumps(doc).encode("utf-8")),
        "double-encoded text": json.dumps(json.dumps(doc)),
        "quoted path": json.dumps(str(doc_file)),
    }
    for name, source in variants.items():
        spec = fj.Mappings.load(source)
        assert spec.connection_id == 35, name
        assert spec.exported_name == "Test Clinic", name
        assert model_summary(spec) == model_summary(by_path), name


def test_labels(doc, doc_file):
    assert fj.Mappings.load(doc_file).label == doc_file.name
    assert fj.Mappings.load(str(doc_file)).label == doc_file.name
    assert fj.Mappings.load(json.dumps(doc)).label == fj.INLINE_LABEL
    assert fj.Mappings.load(doc).label == fj.INLINE_LABEL


def test_dict_input_is_not_mutated(doc):
    before = copy.deepcopy(doc)
    spec = fj.Mappings.load(doc)
    for m in spec.models:
        for r in PERSON_ROWS + SERVICE_ROWS:
            if m.accepts(r):
                m.to_v3(r)
    assert doc == before


def test_user_example_nested_json_parses_but_is_not_a_mappings_export():
    text = '{"key":"value","nested_key":{"key":"value"}}'
    assert fj.parse_json_text(text) == {"key": "value", "nested_key": {"key": "value"}}
    with pytest.raises(fj.MappingsError, match='"connection" must be an object'):
        fj.Mappings.load(text)
    with pytest.raises(fj.MappingsError, match='"connection" must be an object'):
        fj.Mappings.load({"key": "value", "nested_key": {"key": "value"}})


def test_deeply_nested_values_survive(doc):
    doc["tables"][0]["extra"] = {"a": {"b": {"c": [1, {"d": None}]}}}
    doc["connection"]["meta"] = {"x": [{"y": {"z": "ünïcødé 🙂"}}]}
    spec = fj.Mappings.load(json.dumps(doc, ensure_ascii=False))
    assert spec.connection_id == 35 and len(spec.models) == 3


def test_unicode_and_escapes(doc):
    doc["connection"]["name"] = "Kliniki ya Afya – Kisumu ✓"
    doc["fields"][0]["field"] = "first_name"
    for text in (json.dumps(doc, ensure_ascii=False), json.dumps(doc, ensure_ascii=True)):
        assert fj.Mappings.load(text).exported_name == "Kliniki ya Afya – Kisumu ✓"


def test_connection_id_as_numeric_string(doc):
    doc["connection"]["id"] = "35"
    assert fj.Mappings.load(json.dumps(doc)).connection_id == 35


def test_fields_key_optional(doc):
    del doc["fields"]
    spec = fj.Mappings.load(doc)
    assert all(m.fields == [] for m in spec.models)


def test_large_inline_document(doc):
    base = doc["fields"][0]
    doc["fields"] += [{**base, "column": f"c{i}", "field": f"f{i}"} for i in range(5000)]
    spec = fj.Mappings.load(json.dumps(doc))
    patient = next(m for m in spec.models if m.table == "reception_patients")
    assert len(patient.fields) == 5004


# ─── the mapping itself behaves the same from inline JSON ──────────────────────────────────

def test_rules_from_inline_json_are_applied(doc):
    spec = fj.Mappings.load(json.dumps(doc))
    patient = next(m for m in spec.models if m.table == "reception_patients")
    a, b = (patient.to_v3(r) for r in PERSON_ROWS)
    assert a == {"id": 1, "first_name": "Achieng", "gender": "female", "last_name": "Mary Otieno", "note": "JAMBO"}
    assert b["gender"] == "other" and b["last_name"] is None and b["note"] is None


def test_where_split_from_inline_json(doc):
    spec = fj.Mappings.load(json.dumps(doc))
    prod = next(m for m in spec.models if m.table == "inventory_products")
    proc = next(m for m in spec.models if m.table == "evaluation_procedures")
    assert [r["id"] for r in SERVICE_ROWS if prod.accepts(r)] == [10]
    assert [r["id"] for r in SERVICE_ROWS if proc.accepts(r)] == [11]
    assert proc.to_v3(SERVICE_ROWS[1]) == {"id": 11, "name": "full blood count"}


# ─── real exports from the repo: file vs text vs dict ──────────────────────────────────────

@pytest.mark.parametrize("path", REAL_EXPORTS, ids=lambda p: str(p.relative_to(REPO)))
def test_real_exports_identical_from_file_text_and_dict(path):
    raw = path.read_text(encoding="utf-8-sig")
    by_file = fj.Mappings.load(path)
    for source in (raw, json.loads(raw), raw.encode("utf-8"), json.dumps(json.loads(raw))):
        spec = fj.Mappings.load(source)
        assert spec.connection_id == by_file.connection_id
        assert spec.exported_name == by_file.exported_name
        assert model_summary(spec) == model_summary(by_file)
    assert by_file.models, "real export has no tables?"


# ─── failures: clear MappingsError, never a bare KeyError / JSONDecodeError ────────────────

@pytest.mark.parametrize("text, message", [
    ('{"connection": {"id": 35}, "tables": [}', r"inline mappings JSON is not valid JSON: .*line 1, column"),
    ('{"connection": {"id": 35},}', r"not valid JSON"),
    ("{'connection': {'id': 35}}", r"not valid JSON"),                 # a Python dict repr
    ('{"connection": {"id": 35} // comment\n}', r"not valid JSON"),
    ('{"connection": {"id": 35}', r"not valid JSON"),                  # truncated
    ('[{"source table": "x"}]', r"expected a JSON object .* got list"),
    ("[]", r"got list"),
    ('"  "', r"the JSON string is empty"),
    ('{}', r'"connection" must be an object'),
    ('{"connection": 35, "tables": []}', r'"connection" must be an object'),
    ('{"connection": {}, "tables": []}', r'"connection" must be an object with an "id"'),
    ('{"connection": {"id": "abc"}, "tables": [{"source table": "a", "v3 model": "M"}]}', r'"id" must be a number'),
    ('{"connection": {"id": true}, "tables": [{"source table": "a", "v3 model": "M"}]}', r'"id" must be a number'),
    ('{"connection": {"id": null}, "tables": [{"source table": "a", "v3 model": "M"}]}', r'"id" must be a number'),
    ('{"connection": {"id": 1}}', r'"tables" must be a non-empty list'),
    ('{"connection": {"id": 1}, "tables": []}', r'"tables" must be a non-empty list'),
    ('{"connection": {"id": 1}, "tables": {"a": 1}}', r'"tables" must be a non-empty list'),
    ('{"connection": {"id": 1}, "tables": ["person"]}', r"tables\[0\] must be an object"),
    ('{"connection": {"id": 1}, "tables": [{"v3 model": "M"}]}', r'tables\[0\] needs a "source table"'),
    ('{"connection": {"id": 1}, "tables": [{"source table": "a", "v3 model": "  "}]}', r'tables\[0\] needs a "v3 model"'),
    ('{"connection": {"id": 1}, "tables": [{"source table": "a", "v3 model": "M"}], "fields": {}}', r'"fields" must be a list'),
    ('{"connection": {"id": 1}, "tables": [{"source table": "a", "v3 model": "M"}], "fields": [1]}', r"fields\[0\] must be an object"),
    ('{"connection": {"id": 1}, "tables": [{"source table": "a", "v3 model": "M"}], "fields": [{"table": "a", "model": "M"}]}',
     r'fields\[0\] needs "table", "field"'),
    ('{"connection": {"id": 1}, "tables": [{"source table": "a", "v3 model": "M"}], "fields": [{"field": "x", "model": "M"}]}',
     r'fields\[0\] needs "table"'),
    ('{"connection": {"id": 1}, "tables": [{"source table": "a", "v3 model": "M"}], "fields": [{"table": "a", "field": "x"}]}',
     r'"v3_table" or "model"'),
])
def test_inline_errors(text, message):
    with pytest.raises(fj.MappingsError, match=message):
        fj.Mappings.load(text)


@pytest.mark.parametrize("source, message", [
    ("", "No mappings given"),
    ("   \n\t ", "No mappings given"),
    ("\ufeff", "No mappings given"),
    (b"\xff\xfe\x00{", "not UTF-8"),
    (12345, "not int"),
    (None, "not NoneType"),
    (3.5, "not float"),
    ("null", "Mappings file not found"),            # valid JSON, but not JSON-looking: read as a file name
    ("does_not_exist.json", "Mappings file not found"),
])
def test_other_input_errors(source, message):
    with pytest.raises(fj.MappingsError, match=message):
        fj.Mappings.load(source)


def test_file_errors(tmp_path, doc):
    with pytest.raises(fj.MappingsError, match="Mappings file not found"):
        fj.Mappings.load(tmp_path / "missing.json")
    with pytest.raises(fj.MappingsError, match="is a directory"):
        fj.Mappings.load(tmp_path)
    bad = tmp_path / "bad.json"
    bad.write_text('{"connection": {"id": 1},\n "tables": [,]}', encoding="utf-8")
    with pytest.raises(fj.MappingsError, match=r"Mappings file bad.json is not valid JSON: .*line 2"):
        fj.Mappings.load(bad)
    latin = tmp_path / "latin.json"
    latin.write_bytes(json.dumps(doc).replace("Test Clinic", "Clinic é").encode("latin-1"))
    with pytest.raises(fj.MappingsError, match="not UTF-8"):
        fj.Mappings.load(latin)
    shape = tmp_path / "shape.json"
    shape.write_text("[]", encoding="utf-8")
    with pytest.raises(fj.MappingsError, match="shape.json: expected a JSON object"):
        fj.Mappings.load(shape)


def test_file_with_bom_spaces_and_unicode_name(tmp_path, doc):
    p = tmp_path / "dir with spaces" / "mäppings ✓.json"
    p.parent.mkdir()
    p.write_bytes("\ufeff".encode("utf-8") + json.dumps(doc).encode("utf-8"))
    assert fj.Mappings.load(p).connection_id == 35
    assert fj.Mappings.load(str(p)).label == "mäppings ✓.json"
    assert fj.Mappings.load(json.dumps(str(p))).connection_id == 35


def test_home_relative_path(tmp_path, doc, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    (tmp_path / "m.json").write_text(json.dumps(doc), encoding="utf-8")
    assert fj.Mappings.load("~/m.json").connection_id == 35


def test_duplicate_v3_table_names_label(doc, doc_file):
    doc["tables"].append(dict(doc["tables"][0]))
    with pytest.raises(fj.MappingsError, match=r"inline mappings JSON: several entries map to the same V3 table"):
        fj.Mappings.load(json.dumps(doc))
    doc_file.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(fj.MappingsError, match=r"test_data_mappings.json: several entries"):
        fj.Mappings.load(doc_file)


def test_mappings_error_is_a_value_error():
    assert issubclass(fj.MappingsError, ValueError)


# ─── run(): the whole extract → map flow from inline JSON (dry run, fake client) ───────────

def test_run_inline_dict_text_and_file_give_same_results(doc, doc_file, fake_client):
    results = {}
    for name, source in (("file", doc_file), ("text", json.dumps(doc)), ("dict", doc)):
        out = fj.run(source, dry_run=True, workers=2)
        results[name] = sorted((r.source_table, r.status, r.source_rows, sorted(r.v3.items())) for r in out)
    assert results["file"] == results["text"] == results["dict"]
    by_table = {t: v3 for t, _, _, v3 in results["dict"]}
    assert dict(by_table["services"])["inventory_products"]["rows"] == 1
    assert dict(by_table["services"])["evaluation_procedures"]["rows"] == 1
    assert dict(by_table["person"])["reception_patients"]["rows"] == 2


def test_run_tables_filter_and_unknown_table_label(doc, fake_client):
    out = fj.run(json.dumps(doc), tables=["person"], dry_run=True, workers=1)
    assert [r.source_table for r in out] == ["person"]
    with pytest.raises(SystemExit, match=r"Not in inline mappings JSON: \['nope'\]"):
        fj.run(doc, tables=["nope"], dry_run=True)


def test_run_bad_inline_raises_mappings_error(fake_client):
    with pytest.raises(fj.MappingsError):
        fj.run('{"connection": ', dry_run=True)


# ─── CLI ───────────────────────────────────────────────────────────────────────────────────

def _cli(monkeypatch, *argv, stdin: str | None = None):
    monkeypatch.setattr(sys, "argv", ["from_json_mappings_to_snowflake.py", *argv])
    if stdin is not None:
        monkeypatch.setattr(sys, "stdin", io.StringIO(stdin))
    fj.main()


def test_cli_list_from_file_text_and_stdin(doc, doc_file, monkeypatch, capsys):
    outs = []
    for argv, stdin in ((("--mappings", str(doc_file)), None),
                        (("--mappings", json.dumps(doc)), None),
                        (("--mappings", json.dumps(doc, indent=2)), None),
                        (("--mappings", "-"), json.dumps(doc)),
                        (("--mappings", " - "), "\ufeff" + json.dumps(doc, indent=2))):
        _cli(monkeypatch, *argv, "--list", stdin=stdin)
        out = capsys.readouterr().out
        assert "connection 35 (export name 'Test Clinic')" in out
        outs.append(out.split("\n", 1)[1])                 # body without the label line
    assert all(o == outs[0] for o in outs)
    assert "person" in outs[0] and "inventory_products" in outs[0]


def test_cli_error_message_not_traceback(monkeypatch):
    with pytest.raises(SystemExit) as e:
        _cli(monkeypatch, "--mappings", '{"connection": {"id": 35}, "tables": [}', "--list")
    assert str(e.value).startswith("error: inline mappings JSON is not valid JSON")
    with pytest.raises(SystemExit) as e:
        _cli(monkeypatch, "--mappings", "missing_file.json", "--list")
    assert str(e.value).startswith("error: Mappings file not found")
    with pytest.raises(SystemExit) as e:
        _cli(monkeypatch, "--mappings", "-", "--list", stdin="")
    assert "No mappings given" in str(e.value)


def test_cli_dry_run_inline(doc, monkeypatch, fake_client, capsys):
    with pytest.raises(SystemExit) as e:
        _cli(monkeypatch, "--mappings", json.dumps(doc), "--dry-run", "--workers", "1")
    assert e.value.code == 0
    assert "reception_patients" in capsys.readouterr().out


def test_cli_default_is_the_repo_file():
    assert fj.DEFAULT_MAPPINGS.name == "silverwood_data_mappings.json"


# ─── DAG ───────────────────────────────────────────────────────────────────────────────────

class _Param:
    def __init__(self, default=None, **kwargs):
        self.default, self.kwargs = default, kwargs


class _Task:
    """@task / @task(...) stand-in: calling it inside the DAG body only wires, never runs."""
    registry: dict = {}

    def __init__(self, fn):
        self.function = fn
        _Task.registry[fn.__name__] = fn

    def __call__(self, *a, **k):
        return ("xcom", self.function.__name__)

    def expand(self, **k):
        return ("mapped", self.function.__name__)


def _task(fn=None, **_kwargs):
    return _Task(fn) if callable(fn) else _Task


_CTX: dict = {}


@pytest.fixture
def dag_mod(tmp_path, monkeypatch, fake_client):
    """The real DAG file, loaded against a minimal airflow.sdk stand-in."""
    sdk = types.ModuleType("airflow.sdk")
    sdk.Param = _Param
    sdk.task = _task
    sdk.get_current_context = lambda: _CTX
    sdk.dag = lambda **kw: (lambda fn: (lambda: (_DAG.update(kw), fn())))
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    exc = sys.modules["airflow.exceptions"]

    class AirflowFailException(Exception):
        pass
    monkeypatch.setattr(exc, "AirflowFailException", getattr(exc, "AirflowFailException", AirflowFailException),
                        raising=False)
    from tests.helpers import load_dag_module
    _Task.registry = {}
    mod = load_dag_module("from_json_mappings_to_snowflake_dag", force_reload=True)
    monkeypatch.setattr(mod, "PIPELINES_DIR", tmp_path)
    monkeypatch.setattr(mod, "use_pipelines_dir", lambda *a, **k: None)
    mod.FAIL = exc.AirflowFailException
    mod.TASKS = dict(_Task.registry)
    return mod


_DAG: dict = {}


def test_dag_param_accepts_string_and_object(dag_mod):
    p = _DAG["params"]["mappings"]
    assert p.default == "silverwood_data_mappings.json"
    assert p.kwargs["type"] == ["string", "object"]


def test_dag_file_name_and_absolute_path(dag_mod, tmp_path, doc):
    (tmp_path / "m.json").write_text(json.dumps(doc), encoding="utf-8")
    assert dag_mod._mappings_path("m.json") == tmp_path / "m.json"
    assert dag_mod._mappings_path("  m.json  ") == tmp_path / "m.json"
    assert dag_mod._mappings_path(str(tmp_path / "m.json")) == tmp_path / "m.json"
    assert dag_mod._mappings_path(json.dumps("m.json")) == tmp_path / "m.json"      # quoted file name
    with pytest.raises(dag_mod.FAIL, match="Mappings file not found"):
        dag_mod._mappings_path("nope.json")


def test_dag_inline_text_and_object_saved_once_by_content(dag_mod, tmp_path, doc):
    a = dag_mod._mappings_path(json.dumps(doc))
    b = dag_mod._mappings_path(doc)
    c = dag_mod._mappings_path(json.dumps(doc, indent=3))
    d = dag_mod._mappings_path(json.dumps(json.dumps(doc)))
    reordered = json.loads(json.dumps(doc), object_pairs_hook=lambda kv: dict(reversed(kv)))
    e = dag_mod._mappings_path(reordered)
    assert a == b == c == d == e
    assert a.parent == tmp_path / "output" / "inline_mappings" and a.suffix == ".json"
    assert json.loads(a.read_text(encoding="utf-8")) == doc
    assert list(a.parent.iterdir()) == [a]                      # nothing left behind (no .tmp)
    doc["connection"]["id"] = 36
    assert dag_mod._mappings_path(doc) != a


def test_dag_inline_errors_fail_the_task(dag_mod, tmp_path):
    for bad, msg in (('{"connection": ', "not valid JSON"), ("[]", "got list"), ({"x": 1}, '"connection"'),
                     ([1, 2], "got list"), (42, "got int"), ("", "got ''"), ("   ", "got '   '"),
                     (None, "got NoneType"), ('{"key":"value","nested_key":{"key":"value"}}', '"connection"')):
        with pytest.raises(dag_mod.FAIL, match=msg):
            dag_mod._mappings_path(bad)
    assert not (tmp_path / "output").exists()                   # nothing saved for bad input


@pytest.mark.parametrize("attempt", range(25))               # a race: repeat it so a lucky run can't pass
def test_dag_concurrent_saves_are_safe(dag_mod, doc, attempt):
    out, errors = [], []
    start = threading.Barrier(16)

    def go():
        try:
            start.wait()                                        # all 16 hit the save at the same moment
            out.append(dag_mod._mappings_path(json.dumps(doc)))
        except Exception as e:
            errors.append(e)
    threads = [threading.Thread(target=go) for _ in range(16)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors, errors
    assert len(set(out)) == 1
    assert json.loads(out[0].read_text(encoding="utf-8")) == doc
    assert [p.name for p in out[0].parent.iterdir()] == [out[0].name]     # no stray .tmp files


def _run_dag(dag_mod, mappings, tables=()):
    _CTX.clear()
    _CTX["params"] = {"mappings": mappings, "tables": list(tables), "dry_run": True}
    items = dag_mod.TASKS["plan"]()
    results = [dag_mod.TASKS["extract_load"](item) for item in items]
    dag_mod.TASKS["report"](results)
    return items, results


def test_dag_end_to_end_inline_object_text_and_file(dag_mod, tmp_path, doc):
    (tmp_path / "m.json").write_text(json.dumps(doc), encoding="utf-8")
    runs = {name: _run_dag(dag_mod, src) for name, src in
            (("file", "m.json"), ("text", json.dumps(doc)), ("object", doc))}
    for name, (items, results) in runs.items():
        assert sorted(i["source_table"] for i in items) == ["person", "services"], name
        assert all(i["name"] == "Test Clinic" and i["dry_run"] for i in items), name
        assert all(Path(i["mappings"]).is_file() for i in items), name
        json.dumps(items)                                       # XCom-serialisable
        assert all(len(json.dumps(i)) < 1000 for i in items), "mappings travel as a path, not inline"
        by = {r["source_table"]: r for r in results}
        assert by["person"]["v3"]["reception_patients"]["rows"] == 2, name
        assert by["services"]["v3"]["inventory_products"]["rows"] == 1, name
        assert by["services"]["v3"]["evaluation_procedures"]["rows"] == 1, name
    strip = lambda rs: sorted((r["source_table"], json.dumps(r["v3"], sort_keys=True)) for r in rs)
    assert strip(runs["file"][1]) == strip(runs["text"][1]) == strip(runs["object"][1])


def test_dag_plan_tables_filter_and_unknown(dag_mod, doc):
    items, _ = _run_dag(dag_mod, json.dumps(doc), tables=["services"])
    assert [i["source_table"] for i in items] == ["services"]
    with pytest.raises(dag_mod.FAIL, match=r"Not in .*\.json: \['nope'\]"):
        _run_dag(dag_mod, doc, tables=["nope"])


def test_dag_plan_bad_file_fails_cleanly(dag_mod, tmp_path):
    (tmp_path / "broken.json").write_text('{"connection": {"id": 1}, "tables": [', encoding="utf-8")
    with pytest.raises(dag_mod.FAIL, match="Mappings file broken.json is not valid JSON"):
        _run_dag(dag_mod, "broken.json")
    (tmp_path / "shape.json").write_text('{"connection": {"id": 1}}', encoding="utf-8")
    with pytest.raises(dag_mod.FAIL, match='shape.json: "tables" must be a non-empty list'):
        _run_dag(dag_mod, "shape.json")


@pytest.mark.parametrize("path", REAL_EXPORTS, ids=lambda p: str(p.relative_to(REPO)))
def test_dag_real_export_inline_matches_file(dag_mod, tmp_path, path):
    raw = path.read_text(encoding="utf-8-sig")
    saved = dag_mod._mappings_path(raw)
    assert model_summary(fj.Mappings.load(saved)) == model_summary(fj.Mappings.load(path))
    assert dag_mod._mappings_path(json.loads(raw)) == saved
