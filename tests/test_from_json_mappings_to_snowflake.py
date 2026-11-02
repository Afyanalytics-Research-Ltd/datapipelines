"""
Tests for from_json_mappings_to_snowflake.py — focused on the dict-input
path added in feature/lap-migration-api-fix.

Coverage:
  C1.2-a  Mappings.load(dict) happy path
  C1.2-b  Mappings.load(dict) with duplicate V3 tables — readable error, not TypeError

Validation messages are read_mappings()'s (MappingsError, a ValueError) since
the merge with main's inline-mappings support.
  C1.2-c  slug() is identity on Str::slug-equivalent output ([a-z0-9_] input)
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

# from_json_mappings_to_snowflake.py lives in the repo root, not in dags/
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import from_json_mappings_to_snowflake as fj


# ---------------------------------------------------------------------------
# Minimal mapping dict that mirrors what MappingExport::data() produces and
# MigrationFlow::mapping() sends — connection.name is set to the airflow_key.
# ---------------------------------------------------------------------------
def _mapping_dict(*, connection_name="kisumu_v3", tables=None) -> dict:
    """Build the minimal mapping structure LAP sends as conf.mappings."""
    if tables is None:
        tables = [
            {
                "v3 model":   "App\\Models\\Reception\\Patient",
                "v3 table":   "reception_patient",
                "v3 service": "reception-service",
                "source table": "patients",
            }
        ]
    return {
        "connection": {"id": 42, "name": connection_name},
        "tables": tables,
        "fields": [
            {
                "model":    "App\\Models\\Reception\\Patient",
                "v3_table": "reception_patient",
                "table":    "patients",
                "column":   "patient_no",
                "field":    "ref_number",
            }
        ],
    }


# ---------------------------------------------------------------------------
# C1.2-a  Happy path: Mappings.load(dict) loads correctly
# ---------------------------------------------------------------------------
class TestMappingsLoadDict:
    def test_connection_id(self):
        spec = fj.Mappings.load(_mapping_dict())
        assert spec.connection_id == 42

    def test_exported_name_is_airflow_key(self):
        # MigrationFlow sets connection.name to the airflow_key before sending.
        spec = fj.Mappings.load(_mapping_dict(connection_name="kisumu_v3"))
        assert spec.exported_name == "kisumu_v3"

    def test_source_tables(self):
        spec = fj.Mappings.load(_mapping_dict())
        assert spec.source_tables == ["patients"]

    def test_model_count(self):
        spec = fj.Mappings.load(_mapping_dict())
        assert len(spec.models) == 1

    def test_model_fields_loaded(self):
        spec = fj.Mappings.load(_mapping_dict())
        model = spec.models[0]
        assert model.source_table == "patients"
        assert model.v3_table == "reception_patient"
        assert len(model.fields) == 1
        assert model.fields[0]["field"] == "ref_number"

    def test_file_path_still_works(self, tmp_path):
        """Existing file-path behaviour is not broken."""
        import json
        p = tmp_path / "test_mapping.json"
        p.write_text(json.dumps(_mapping_dict()))
        spec = fj.Mappings.load(p)
        assert spec.connection_id == 42
        assert spec.exported_name == "kisumu_v3"


# ---------------------------------------------------------------------------
# C1.2-a2  Validation: malformed dict raises ValueError with a clear message
# ---------------------------------------------------------------------------
class TestMappingsLoadDictValidation:
    def test_missing_tables_key(self):
        doc = {"connection": {"id": 1, "name": "k"}, "fields": []}
        with pytest.raises(ValueError, match='"tables" must be a non-empty list'):
            fj.Mappings.load(doc)

    def test_missing_connection_key(self):
        doc = {"tables": [], "fields": []}
        with pytest.raises(ValueError, match='"connection" must be an object with an "id"'):
            fj.Mappings.load(doc)

    def test_fields_not_a_list(self):
        doc = _mapping_dict()
        doc["fields"] = {"table": "patients"}
        with pytest.raises(ValueError, match='"fields" must be a list'):
            fj.Mappings.load(doc)

    def test_connection_id_missing(self):
        doc = {"connection": {"name": "k"}, "tables": [], "fields": []}
        with pytest.raises(ValueError, match='"connection" must be an object with an "id"'):
            fj.Mappings.load(doc)

    def test_connection_id_non_numeric(self):
        doc = {"connection": {"id": "abc", "name": "k"}, "tables": [], "fields": []}
        with pytest.raises(ValueError, match='connection "id" must be a number'):
            fj.Mappings.load(doc)

    def test_empty_tables_raises(self):
        doc = {"connection": {"id": 1, "name": "k"}, "tables": [], "fields": []}
        with pytest.raises(ValueError, match='"tables" must be a non-empty list'):
            fj.Mappings.load(doc)

    def test_table_entry_missing_source_table(self):
        doc = _mapping_dict()
        doc["tables"][0].pop("source table")
        with pytest.raises(ValueError, match=r'tables\[0\] needs a "source table"'):
            fj.Mappings.load(doc)

    def test_table_entry_missing_v3_model(self):
        doc = _mapping_dict()
        doc["tables"][0].pop("v3 model")
        with pytest.raises(ValueError, match=r'tables\[0\] needs a "v3 model"'):
            fj.Mappings.load(doc)

    def test_field_missing_table_key(self):
        doc = _mapping_dict()
        doc["fields"][0].pop("table")
        with pytest.raises(ValueError, match=r'fields\[0\] needs "table", "field" and "v3_table" or "model"'):
            fj.Mappings.load(doc)

    def test_field_missing_model_and_v3_table(self):
        doc = _mapping_dict()
        # fixture has both 'model' and 'v3_table' — must remove both to trigger the check
        doc["fields"][0].pop("model")
        doc["fields"][0].pop("v3_table")
        with pytest.raises(ValueError, match=r'fields\[0\] needs "table", "field" and "v3_table" or "model"'):
            fj.Mappings.load(doc)

    def test_error_message_names_inline(self):
        """Every validation error on a dict input must say it was inline, not name a file path."""
        doc = {"connection": {"id": 1, "name": "k"}, "fields": []}
        with pytest.raises(ValueError) as exc_info:
            fj.Mappings.load(doc)
        assert fj.INLINE_LABEL in str(exc_info.value)

    def test_error_message_names_file(self, tmp_path):
        """Validation errors on a file input must name the file."""
        import json
        bad = {"connection": {"id": 1, "name": "k"}, "fields": []}
        p = tmp_path / "bad_mapping.json"
        p.write_text(json.dumps(bad))
        with pytest.raises(ValueError) as exc_info:
            fj.Mappings.load(p)
        assert "bad_mapping.json" in str(exc_info.value)


# ---------------------------------------------------------------------------
# C1.2-b  Duplicate V3 tables: MappingsError with readable message, not TypeError
# ---------------------------------------------------------------------------
class TestMappingsLoadDictDupes:
    def test_dupes_raise_mappings_error(self):
        doc = _mapping_dict(
            tables=[
                {"v3 model": "App\\Models\\Reception\\Patient", "v3 table": "reception_patient", "source table": "patients"},
                {"v3 model": "App\\Models\\Reception\\Patient", "v3 table": "reception_patient", "source table": "visits"},
            ]
        )
        with pytest.raises(fj.MappingsError):
            fj.Mappings.load(doc)

    def test_dupes_message_contains_table_name(self):
        doc = _mapping_dict(
            tables=[
                {"v3 model": "App\\Models\\Reception\\Patient", "v3 table": "reception_patient", "source table": "patients"},
                {"v3 model": "App\\Models\\Reception\\Patient", "v3 table": "reception_patient", "source table": "visits"},
            ]
        )
        with pytest.raises(fj.MappingsError) as exc_info:
            fj.Mappings.load(doc)
        msg = str(exc_info.value)
        assert "reception_patient" in msg

    def test_dupes_message_says_inline_not_path(self):
        """Must not crash with TypeError from Path(dict) — must say it was inline."""
        doc = _mapping_dict(
            tables=[
                {"v3 model": "App\\Models\\Reception\\Patient", "v3 table": "reception_patient", "source table": "patients"},
                {"v3 model": "App\\Models\\Reception\\Patient", "v3 table": "reception_patient", "source table": "visits"},
            ]
        )
        with pytest.raises(fj.MappingsError) as exc_info:
            fj.Mappings.load(doc)
        msg = str(exc_info.value)
        assert fj.INLINE_LABEL in msg


# ---------------------------------------------------------------------------
# C1.2-c  slug() identity: Python slug() is a no-op on PHP Str::slug output.
#
# PHP Str::slug(x, '_') produces [a-z0-9_]+ — lowercase alphanumeric and
# underscores only. Python slug() must be identity on that character set so
# that the schema name key() returns in PHP and the name Warehouse() derives
# in Python are the same string (both upper-cased to the same schema).
# ---------------------------------------------------------------------------
class TestSlugIdentity:
    @pytest.mark.parametrize("value", [
        "kisumu_v3",
        "silverwood",
        "special_cases_b_test",
        "connection_42",
        "org250",
        "kisumu_general_hospital",
        "a",
        "abc_123_xyz",
    ])
    def test_slug_is_identity_on_php_slug_output(self, value):
        """slug(x) == x for any x in [a-z0-9_]+ — the output space of PHP Str::slug(x,'_')."""
        assert re.match(r'^[a-z0-9_]+$', value), f"test value {value!r} is not in [a-z0-9_]+ — fix the test"
        assert fj.slug(value) == value
