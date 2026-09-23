"""
Deep-coverage tests for dags/flatten_jsons_schemas.py.

This DAG is a downstream RAW->CLEAN flattening step: it discovers JSON field
shapes in a Snowflake EVENTS_RAW table (via TYPEOF()/LATERAL FLATTEN
queries), resolves conflicting per-field types, optionally expands
OBJECT-typed fields up to two levels deep (gated on a >=20% fill rate), and
builds a CREATE OR REPLACE VIEW statement that TRY_CASTs each discovered
JSON path.

Almost everything here is pure-Python (dicts/lists/strings in, SQL strings
or plain data out); the handful of functions that talk to Snowflake take an
``sf`` object and only ever call ``sf.fetchall(sql, params)`` /
``sf.execute(sql, label=...)`` on it, so a small duck-typed FakeSnowflakeClient
(no real network/credentials) is enough to exercise them.
"""
from __future__ import annotations

from datetime import timedelta

import pytest

from tests.airflow_stub import TriggerRule, XComArgStub
from tests.helpers import load_dag_module

flatten = load_dag_module("flatten_jsons_schemas")


# ---------------------------------------------------------------------------
# Fake Snowflake client
# ---------------------------------------------------------------------------
class FakeSnowflakeClient:
    """Duck-typed stand-in for flatten.SnowflakeClient. Responses to
    ``.fetchall(...)`` are served from a FIFO queue supplied up front, in
    the exact order the module under test is expected to call them — this
    keeps each test's queue a direct, readable trace of the calls the
    function under test is documented to make."""

    def __init__(self, fetchall_responses=None):
        self._fetchall_responses = list(fetchall_responses or [])
        self.fetchall_calls: list[tuple[str, dict | None]] = []
        self.execute_calls: list[tuple[str, str | None]] = []

    def fetchall(self, sql, params=None):
        self.fetchall_calls.append((sql, params))
        if not self._fetchall_responses:
            raise AssertionError(
                f"FakeSnowflakeClient.fetchall() called with no queued response "
                f"left (call #{len(self.fetchall_calls)}). sql={sql!r} params={params!r}"
            )
        return self._fetchall_responses.pop(0)

    def execute(self, sql, label=None):
        self.execute_calls.append((sql, label))
        return {"rowcount": 0, "sfqid": "FAKE_SFQID"}

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _patch_client(monkeypatch, fake):
    """Make ``SnowflakeClient()`` (no-arg construction, as used by every
    task callable in the module) return the given fake instance."""
    monkeypatch.setattr(flatten, "SnowflakeClient", lambda *a, **kw: fake)


# ===========================================================================
# DAG structural shape
# ===========================================================================
class TestDagStructure:
    def test_dag_id_schedule_tags(self):
        dag = flatten.dag
        assert dag.dag_id == "flatten_jsons_schemas"
        assert dag.schedule == "@daily"
        assert dag.catchup is False
        assert dag.tags == ["snowflake", "transform", "clean"]

    def test_default_args(self):
        dag = flatten.dag
        assert dag.default_args["retries"] == 3
        assert dag.default_args["retry_delay"] == timedelta(minutes=2)

    def test_task_chain_shape(self):
        dag = flatten.dag
        t_ensure = dag.task_dict["ensure_clean_schemas"]
        t_discover = dag.task_dict["discover_flatten_jobs"]
        t_flatten = dag.task_dict["flatten_table"]

        assert t_ensure.upstream_task_ids == set()
        assert t_ensure.downstream_task_ids == {"discover_flatten_jobs"}

        assert t_discover.upstream_task_ids == {"ensure_clean_schemas"}
        assert t_discover.downstream_task_ids == {"flatten_table"}

        assert t_flatten.upstream_task_ids == {"discover_flatten_jobs"}
        assert t_flatten.is_mapped is True
        assert t_flatten.trigger_rule == TriggerRule.ALL_DONE

        assert isinstance(t_flatten.mapped_kwargs["op_kwargs"], XComArgStub)
        assert t_flatten.mapped_kwargs["op_kwargs"].task is t_discover

    def test_exactly_three_tasks(self):
        assert {t.task_id for t in flatten.dag.tasks} == {
            "ensure_clean_schemas",
            "discover_flatten_jobs",
            "flatten_table",
        }


# ===========================================================================
# SCHEMA_PAIRS
# ===========================================================================
def test_schema_pairs_only_active_entry():
    assert flatten.SCHEMA_PAIRS == [("AFYA_API_AUTH_RAW", "AFYA_API_AUTH_CLEAN")]


@pytest.mark.parametrize(
    "facility", ["KISUMU", "KAKAMEGA", "LODWAR", "XANALIFE"]
)
def test_schema_pairs_commented_facilities_not_active(facility):
    assert not any(facility in raw for raw, _clean in flatten.SCHEMA_PAIRS)


# ===========================================================================
# resolve_type
# ===========================================================================
@pytest.mark.parametrize(
    "types, expected",
    [
        (["INTEGER"], "INTEGER"),
        (["VARCHAR"], "VARCHAR"),
        (["BOOLEAN"], "BOOLEAN"),
        (["OBJECT"], "OBJECT"),
        (["ARRAY"], "ARRAY"),
        (["DATE"], "DATE"),
        (["TIMESTAMP_NTZ"], "TIMESTAMP_NTZ"),
        # NULL_VALUE stripped out, single real type remains
        (["INTEGER", "NULL_VALUE"], "INTEGER"),
        (["NULL_VALUE", "VARCHAR"], "VARCHAR"),
        # all-null / empty -> defensive VARCHAR fallback
        (["NULL_VALUE", "NULL_VALUE"], "VARCHAR"),
        ([], "VARCHAR"),
        # mixed scalar + nested -> VARIANT
        (["OBJECT", "VARCHAR"], "VARIANT"),
        (["ARRAY", "INTEGER"], "VARIANT"),
        (["OBJECT", "ARRAY"], "VARIANT"),
        # numeric widening
        (["INTEGER", "DOUBLE"], "DOUBLE"),
        (["INTEGER", "DECIMAL"], "DECIMAL"),
        (["DECIMAL", "DOUBLE"], "DOUBLE"),
        (["INTEGER", "DECIMAL", "DOUBLE"], "DOUBLE"),
        # genuinely mixed scalars -> VARCHAR
        (["VARCHAR", "BOOLEAN"], "VARCHAR"),
        (["INTEGER", "VARCHAR"], "VARCHAR"),
        (["BOOLEAN", "INTEGER"], "VARCHAR"),
    ],
)
def test_resolve_type(types, expected):
    assert flatten.resolve_type(types) == expected


# ===========================================================================
# infer_type
# ===========================================================================
@pytest.mark.parametrize(
    "snowflake_type, expected",
    [
        ("INTEGER", "NUMBER"),
        ("DECIMAL", "NUMBER(18,2)"),
        ("DOUBLE", "FLOAT"),
        ("VARCHAR", "STRING"),
        ("TEXT", "STRING"),
        ("BOOLEAN", "BOOLEAN"),
        ("DATE", "DATE"),
        ("TIMESTAMP_NTZ", "TIMESTAMP"),
        ("TIMESTAMP_TZ", "TIMESTAMP"),
        ("TIMESTAMP_LTZ", "TIMESTAMP"),
        ("ARRAY", "ARRAY"),
        ("OBJECT", "OBJECT"),
        # unknown type -> safe STRING fallback
        ("VARIANT", "STRING"),
        ("SOMETHING_UNKNOWN", "STRING"),
        # case-insensitive lookup
        ("integer", "NUMBER"),
        ("Timestamp_Ntz", "TIMESTAMP"),
    ],
)
def test_infer_type(snowflake_type, expected):
    assert flatten.infer_type(snowflake_type) == expected


# ===========================================================================
# check_fill_rate
# ===========================================================================
class TestCheckFillRate:
    @pytest.mark.parametrize(
        "total, populated, expected_rate",
        [
            (100, 20, 0.20),   # exactly at the 20% boundary
            (100, 21, 0.21),   # just above
            (100, 19, 0.19),   # just below
            (100, 0, 0.0),     # 0% fill
            (100, 100, 1.0),   # 100% fill
            (0, 0, 0.0),       # no rows at all -> defensive 0.0, no div-by-zero
        ],
    )
    def test_rate_values(self, total, populated, expected_rate):
        fake = FakeSnowflakeClient(fetchall_responses=[[(total, populated)]])
        rate = flatten.check_fill_rate(fake, "AFYA_API_AUTH_RAW", "some_table", "field")
        assert rate == pytest.approx(expected_rate)

    @pytest.mark.parametrize(
        "total, populated",
        [(100, 20), (100, 21), (100, 19)],
    )
    def test_gate_decision_against_expand_objects_threshold(self, total, populated):
        """The gate used by expand_objects is `< 0.2` (strictly less-than):
        exactly 20% fill passes the gate (expands); anything below fails
        (stays VARIANT)."""
        rate = flatten.check_fill_rate(
            FakeSnowflakeClient(fetchall_responses=[[(total, populated)]]),
            "AFYA_API_AUTH_RAW", "t", "f",
        )
        gate_fails = rate < 0.2
        if populated == 19:
            assert gate_fails is True
        else:
            assert gate_fails is False

    def test_dotted_json_path_quoting_and_params(self):
        fake = FakeSnowflakeClient(fetchall_responses=[[(10, 5)]])
        flatten.check_fill_rate(fake, "AFYA_API_AUTH_RAW", "beds", "bed_type.type")
        sql, params = fake.fetchall_calls[0]
        assert params == {"table": "beds"}
        assert 'payload:"bed_type":"type"' in sql
        assert "AFYA_API_AUTH_RAW.EVENTS_RAW" in sql


# ===========================================================================
# get_source_tables / discover_fields / _discover_inner_fields
# ===========================================================================
class TestDiscoveryQueries:
    def test_get_source_tables(self):
        fake = FakeSnowflakeClient(
            fetchall_responses=[[("finance_invoices", 120), ("consumables", 45)]]
        )
        result = flatten.get_source_tables(fake, "AFYA_API_AUTH_RAW")
        assert result == ["finance_invoices", "consumables"]
        sql, params = fake.fetchall_calls[0]
        assert "AFYA_API_AUTH_RAW.EVENTS_RAW" in sql
        assert params is None

    def test_get_source_tables_empty_raw_schema(self):
        """A RAW schema with no rows for EVENTS_RAW / a schema that doesn't
        exist in RAW (query just yields zero rows) -> empty list, no error."""
        fake = FakeSnowflakeClient(fetchall_responses=[[]])
        assert flatten.get_source_tables(fake, "NO_SUCH_RAW") == []

    def test_discover_fields_conflicting_types_widen(self):
        fake = FakeSnowflakeClient(
            fetchall_responses=[[
                ("id", "INTEGER"),
                ("amount", "INTEGER"),
                ("amount", "DECIMAL"),
                ("bed-type", "VARCHAR"),
            ]]
        )
        result = flatten.discover_fields(fake, "AFYA_API_AUTH_RAW", "invoices")
        assert result == [
            ("id", "INTEGER"),
            ("amount", "DECIMAL"),   # widened INTEGER+DECIMAL -> DECIMAL
            ("bed_type", "VARCHAR"),  # hyphen normalized to underscore
        ]
        sql, params = fake.fetchall_calls[0]
        assert params == {"table": "invoices"}
        assert "LATERAL FLATTEN(input => payload)" in sql

    def test_discover_fields_zero_rows(self):
        """Table with zero discoverable fields (e.g. every payload row for
        this source_table is somehow non-object, or the table genuinely has
        no rows) -> empty field list, no error."""
        fake = FakeSnowflakeClient(fetchall_responses=[[]])
        assert flatten.discover_fields(fake, "AFYA_API_AUTH_RAW", "empty_table") == []

    def test_discover_inner_fields_dotted_path(self):
        fake = FakeSnowflakeClient(
            fetchall_responses=[[("id", "INTEGER"), ("name", "VARCHAR")]]
        )
        result = flatten._discover_inner_fields(
            fake, "AFYA_API_AUTH_RAW", "beds", "bed_type.type"
        )
        assert result == [("id", "INTEGER"), ("name", "VARCHAR")]
        sql, params = fake.fetchall_calls[0]
        assert params == {"table": "beds"}
        assert 'payload:"bed_type":"type"' in sql
        assert "LATERAL FLATTEN(input => payload:" in sql

    def test_discover_inner_fields_hyphenated_and_conflicting(self):
        fake = FakeSnowflakeClient(
            fetchall_responses=[[
                ("first-name", "VARCHAR"),
                ("count", "INTEGER"),
                ("count", "DOUBLE"),
            ]]
        )
        result = flatten._discover_inner_fields(fake, "AFYA_API_AUTH_RAW", "t", "obj")
        assert result == [
            ("first_name", "VARCHAR"),
            ("count", "DOUBLE"),
        ]


# ===========================================================================
# _add_field dedup
# ===========================================================================
class TestAddFieldDedup:
    def test_simple_collision_gets_trailing_underscore(self):
        result: list[tuple[str, str, str]] = []
        seen: set[str] = set()

        flatten._add_field(result, seen, "BED_ID", "bed_id", "INTEGER")
        flatten._add_field(result, seen, "BED_ID", "bed.id", "VARCHAR")

        assert result == [
            ("BED_ID", "bed_id", "INTEGER"),
            ("BED_ID_", "bed.id", "VARCHAR"),
        ]
        assert seen == {"BED_ID", "BED_ID_"}
        # neither field was silently dropped
        assert len(result) == 2

    def test_collision_is_case_insensitive(self):
        result: list[tuple[str, str, str]] = []
        seen: set[str] = set()

        flatten._add_field(result, seen, "bed_id", "x", "INTEGER")
        flatten._add_field(result, seen, "BED_ID", "y", "VARCHAR")

        assert [r[0] for r in result] == ["bed_id", "BED_ID_"]

    def test_triple_collision_keeps_disambiguating(self):
        result: list[tuple[str, str, str]] = []
        seen: set[str] = set()

        flatten._add_field(result, seen, "BED_ID", "a", "INTEGER")
        flatten._add_field(result, seen, "BED_ID", "b", "VARCHAR")
        flatten._add_field(result, seen, "BED_ID", "c", "BOOLEAN")

        assert [r[0] for r in result] == ["BED_ID", "BED_ID_", "BED_ID__"]
        assert len(result) == 3


# ===========================================================================
# expand_objects
# ===========================================================================
class TestExpandObjects:
    def test_zero_level_scalar_passthrough(self):
        fake = FakeSnowflakeClient(fetchall_responses=[])
        result = flatten.expand_objects(
            fake, "AFYA_API_AUTH_RAW", "t", [("id", "INTEGER"), ("name", "VARCHAR")]
        )
        assert result == [("id", "id", "INTEGER"), ("name", "name", "VARCHAR")]
        assert fake.fetchall_calls == []  # no queries needed for non-OBJECT fields

    def test_object_below_fill_threshold_stays_variant(self):
        fake = FakeSnowflakeClient(fetchall_responses=[[(10, 1)]])  # 10% fill
        result = flatten.expand_objects(
            fake, "AFYA_API_AUTH_RAW", "t", [("sparse_obj", "OBJECT")]
        )
        assert result == [("sparse_obj", "sparse_obj", "VARIANT")]

    def test_object_at_exact_threshold_expands(self):
        fake = FakeSnowflakeClient(
            fetchall_responses=[
                [(100, 20)],  # exactly 20% fill -> gate (`< 0.2`) does NOT trip
                [("a", "INTEGER")],  # inner fields
            ]
        )
        result = flatten.expand_objects(
            fake, "AFYA_API_AUTH_RAW", "t", [("obj", "OBJECT")]
        )
        assert result == [("obj_a", "obj.a", "INTEGER")]

    def test_full_two_level_expansion_and_mixed_fields(self):
        """id (scalar, 0 levels), sparse_obj (OBJECT, below threshold ->
        VARIANT), nested_obj (OBJECT, above threshold, with one scalar
        inner field and one 1-level-nested-OBJECT inner field that itself
        expands to two more leaf fields)."""
        fake = FakeSnowflakeClient(
            fetchall_responses=[
                [(10, 1)],                                 # sparse_obj fill rate: 10%
                [(10, 5)],                                 # nested_obj fill rate: 50%
                [("type", "OBJECT"), ("count", "INTEGER")],  # nested_obj inner fields
                [("id", "INTEGER"), ("label", "VARCHAR")],   # nested_obj.type inner (level 2)
            ]
        )
        top_level_fields = [
            ("id", "INTEGER"),
            ("sparse_obj", "OBJECT"),
            ("nested_obj", "OBJECT"),
        ]
        result = flatten.expand_objects(fake, "AFYA_API_AUTH_RAW", "t", top_level_fields)

        assert result == [
            ("id", "id", "INTEGER"),
            ("sparse_obj", "sparse_obj", "VARIANT"),
            ("nested_obj_type_id", "nested_obj.type.id", "INTEGER"),
            ("nested_obj_type_label", "nested_obj.type.label", "VARCHAR"),
            ("nested_obj_count", "nested_obj.count", "INTEGER"),
        ]
        assert fake.fetchall_calls == []  # all four responses consumed, none left over

    def test_level_two_object_field_is_not_expanded_a_third_level(self):
        """outer (OBJECT) -> mid (OBJECT, level 1) -> deep (OBJECT, level 2).
        Per the module's documented '2 levels deep' behavior, `deep` is
        added as a raw OBJECT column and NOT recursed into again (that
        would require a 4th fetchall call, which this test proves never
        happens since the fake's queue only has 3 responses and is fully
        drained without error)."""
        fake = FakeSnowflakeClient(
            fetchall_responses=[
                [(10, 10)],          # outer fill rate: 100%
                [("mid", "OBJECT")],  # outer inner fields (level 1)
                [("deep", "OBJECT")],  # outer.mid inner fields (level 2)
            ]
        )
        result = flatten.expand_objects(fake, "AFYA_API_AUTH_RAW", "t", [("outer", "OBJECT")])

        assert result == [("outer_mid_deep", "outer.mid.deep", "OBJECT")]
        assert len(fake.fetchall_calls) == 3
        assert fake._fetchall_responses == []  # nothing left unconsumed, nothing extra fetched

    def test_level_two_empty_inner_fields_falls_back_to_variant(self):
        fake = FakeSnowflakeClient(
            fetchall_responses=[
                [(10, 10)],           # fill rate: 100%
                [("type", "OBJECT")],  # level 1: one OBJECT field
                [],                    # level 2: no inner fields discovered
            ]
        )
        result = flatten.expand_objects(fake, "AFYA_API_AUTH_RAW", "t", [("obj", "OBJECT")])
        assert result == [("obj_type", "obj.type", "VARIANT")]

    def test_dedup_applied_across_expansion(self):
        """A top-level scalar field and an expanded nested field that would
        produce the same column name must not collide silently."""
        fake = FakeSnowflakeClient(
            fetchall_responses=[
                [(10, 10)],  # bed fill rate: 100%
                [("id", "INTEGER")],  # bed.id inner field -> would-be column BED_ID
            ]
        )
        top_level_fields = [
            ("BED_ID", "INTEGER"),  # pre-existing top-level column named BED_ID
            ("bed", "OBJECT"),
        ]
        result = flatten.expand_objects(fake, "AFYA_API_AUTH_RAW", "t", top_level_fields)
        assert result == [
            ("BED_ID", "BED_ID", "INTEGER"),
            ("bed_id_", "bed.id", "INTEGER"),  # disambiguated, not dropped
        ]


# ===========================================================================
# build_flatten_sql
# ===========================================================================
class TestBuildFlattenSql:
    def test_basic_fields_try_cast_and_view_shape(self):
        fields = [
            ("ID", "id", "INTEGER"),
            ("NAME", "name", "VARCHAR"),
            ("PAYLOAD_BLOB", "payload_blob", "OBJECT"),
        ]
        sql = flatten.build_flatten_sql("AFYA_API_AUTH_RAW", "AFYA_API_AUTH_CLEAN", "invoices", fields)

        assert "CREATE OR REPLACE VIEW AFYA_API_AUTH_CLEAN.invoices AS" in sql
        assert 'TRY_CAST(record:"id"::STRING AS NUMBER) AS "ID"' in sql
        assert 'TRY_CAST(record:"name"::STRING AS STRING) AS "NAME"' in sql
        # ARRAY/OBJECT/VARIANT fields are passed through raw, no TRY_CAST
        assert 'record:"payload_blob" AS "PAYLOAD_BLOB"' in sql
        assert 'TRY_CAST(record:"payload_blob"' not in sql
        assert "FROM AFYA_API_AUTH_RAW.EVENTS_RAW" in sql
        assert "WHERE source_table = 'invoices'" in sql
        assert "facility_id as source_schema" in sql

    def test_nested_json_path_dotted_cast(self):
        fields = [("BED_TYPE_NAME", "bed_type.name", "VARCHAR")]
        sql = flatten.build_flatten_sql("R", "C", "beds", fields)
        assert 'record:"bed_type":"name"::STRING AS STRING) AS "BED_TYPE_NAME"' in sql

    def test_reserved_word_field_name_is_quoted_safely(self):
        fields = [("GROUP", "group", "VARCHAR")]
        sql = flatten.build_flatten_sql("R", "C", "t", fields)
        assert '"GROUP"' in sql

    def test_field_name_with_space_is_quoted(self):
        fields = [("CONSULT TYPE", "consult type", "VARCHAR")]
        sql = flatten.build_flatten_sql("R", "C", "t", fields)
        assert '"CONSULT TYPE"' in sql
        assert '"consult type"' in sql  # the json_path part is quoted too

    def test_zero_discoverable_fields_produces_dangling_comma(self):
        """BUG (flagged, not fixed): build_flatten_sql does not guard
        against an empty expanded_fields list. select_list becomes "" and
        the template still emits the trailing comma after
        `facility_id as source_schema,`, producing syntactically invalid
        SQL (`... source_schema,\\n    \\nFROM deduped;`). A table that
        discovers zero fields (e.g. discover_fields returned [], or every
        top-level field was itself somehow dropped) would fail at
        CREATE OR REPLACE VIEW execution time in real Snowflake."""
        sql = flatten.build_flatten_sql("AFYA_API_AUTH_RAW", "AFYA_API_AUTH_CLEAN", "empty_table", [])
        assert "facility_id as source_schema,\n        \n    FROM deduped;" in sql

    def test_field_name_containing_double_quote_breaks_identifier_quoting(self):
        """BUG (flagged, not fixed): build_flatten_sql quotes column/path
        identifiers with plain f'"{...}"' and never escapes an embedded
        double-quote character. A JSON key discovered from a real payload
        that itself contains a literal `"` (a plausible, if unusual,
        input — discover_fields uses the raw JSON object key verbatim,
        only normalizing '-' to '_', never escaping '"') produces a
        malformed/injectable identifier instead of a properly escaped one
        (Snowflake's own escaping convention would double the embedded
        quote, i.e. `"a""b"`)."""
        fields = [('A"B', 'a"b', "VARCHAR")]
        sql = flatten.build_flatten_sql("R", "C", "t", fields)
        # Demonstrates the malformed output: the embedded quote is emitted
        # raw, breaking out of the intended identifier instead of being
        # escaped as `""`.
        assert '"A"B"' in sql
        assert '"A""B"' not in sql  # the properly-escaped form is absent


# ===========================================================================
# Task callables (ensure_clean_schemas / discover_flatten_jobs / flatten_table)
# ===========================================================================
class TestTaskCallables:
    def test_ensure_clean_schemas_creates_only_active_schema(self, monkeypatch):
        fake = FakeSnowflakeClient()
        _patch_client(monkeypatch, fake)

        flatten.ensure_clean_schemas()

        assert len(fake.execute_calls) == 1
        sql, label = fake.execute_calls[0]
        assert "CREATE SCHEMA IF NOT EXISTS AFYA_API_AUTH_CLEAN" in sql
        assert label == "schema:AFYA_API_AUTH_CLEAN"

    def test_discover_flatten_jobs_builds_one_job_per_table(self, monkeypatch):
        fake = FakeSnowflakeClient(
            fetchall_responses=[[("finance_invoices", 120), ("consumables", 45)]]
        )
        _patch_client(monkeypatch, fake)

        jobs = flatten.discover_flatten_jobs()

        assert jobs == [
            {"job": {"raw_schema": "AFYA_API_AUTH_RAW", "clean_schema": "AFYA_API_AUTH_CLEAN", "table": "finance_invoices"}},
            {"job": {"raw_schema": "AFYA_API_AUTH_RAW", "clean_schema": "AFYA_API_AUTH_CLEAN", "table": "consumables"}},
        ]

    def test_discover_flatten_jobs_empty_raw_schema_yields_no_jobs(self, monkeypatch):
        fake = FakeSnowflakeClient(fetchall_responses=[[]])
        _patch_client(monkeypatch, fake)

        jobs = flatten.discover_flatten_jobs()
        assert jobs == []

    def test_flatten_table_end_to_end_executes_create_view(self, monkeypatch):
        fake = FakeSnowflakeClient(
            fetchall_responses=[
                [("id", "INTEGER"), ("name", "VARCHAR")],  # discover_fields
                # no OBJECT-typed fields -> expand_objects issues no further queries
            ]
        )
        _patch_client(monkeypatch, fake)

        flatten.flatten_table({
            "raw_schema": "AFYA_API_AUTH_RAW",
            "clean_schema": "AFYA_API_AUTH_CLEAN",
            "table": "consumables",
        })

        assert len(fake.execute_calls) == 1
        sql, label = fake.execute_calls[0]
        assert "CREATE OR REPLACE VIEW AFYA_API_AUTH_CLEAN.consumables AS" in sql
        assert 'TRY_CAST(record:"id"::STRING AS NUMBER) AS "ID"' in sql
        assert label == "flatten:AFYA_API_AUTH_RAW.consumables"

    def test_flatten_table_zero_fields_table(self, monkeypatch):
        """A table present in RAW's source_table list but with zero
        discoverable fields (e.g. all its payload rows fail the IS_OBJECT
        filter some other way) still runs through to a CREATE VIEW call —
        it just hits the dangling-comma bug documented above."""
        fake = FakeSnowflakeClient(fetchall_responses=[[]])
        _patch_client(monkeypatch, fake)

        flatten.flatten_table({
            "raw_schema": "AFYA_API_AUTH_RAW",
            "clean_schema": "AFYA_API_AUTH_CLEAN",
            "table": "weird_table",
        })

        assert len(fake.execute_calls) == 1
        sql, _label = fake.execute_calls[0]
        assert "CREATE OR REPLACE VIEW AFYA_API_AUTH_CLEAN.weird_table AS" in sql
