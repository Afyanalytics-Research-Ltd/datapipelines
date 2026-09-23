"""
Deep coverage for dags/orthopedic_raw_to_clean.py: column sanitization
(_col/_alias/_key_sql/_dedup_cols), type-inference routing (_query_key_types,
_is_object_array, _discover_array_element_keys), the CTAS/INSERT SQL builders,
per-table flatten decision logic (_flatten_table), table discovery
(list_raw_tables/list_target_tables), the three Airflow Variables that drive
behavior (ORTHOPEDIC_CLEAN_SAMPLE_SIZE/_FULL_REFRESH/_DRY_RUN), and the DAG's
structural shape / failure-surfacing in summarize_results.

All Snowflake access goes through this module's own `SnowflakeClient`, which
opens a real `snowflake.connector.connect(...)` in `__init__`. Tests never
construct a real SnowflakeClient: functions that take an `sf` object
directly (`_flatten_table`, `_query_key_types`, `_is_object_array`,
`_discover_array_element_keys`, `_table_exists`, `_get_existing_columns`,
`list_raw_tables`) are given a lightweight `FakeSF` test double instead; the
three task callables that construct `SnowflakeClient()` themselves
(`ensure_clean_schema`, `list_target_tables`, `flatten_table_task`) have that
class patched via `mocker.patch.object(MODULE, "SnowflakeClient", ...)`.
"""
from __future__ import annotations

import pytest

from tests.helpers import DAGS_DIR, load_dag_module
from tests.airflow_stub import TriggerRule, XComArgStub

MODULE = load_dag_module("orthopedic_raw_to_clean")


# ─── Test double for SnowflakeClient ──────────────────────────────────────

class FakeSF:
    """
    Stands in for a SnowflakeClient instance. `.query(sql, label=...)`
    returns a canned response keyed by `label` (the production code labels
    every query distinctly enough — see module docstring — that matching on
    label alone is unambiguous and far more readable than matching on raw
    SQL text). Any label not pre-registered raises immediately with the SQL
    that was attempted, so a test's expectations about *which* queries a
    code path issues are enforced implicitly.
    """

    def __init__(self, responses: dict[str, object] | None = None):
        self.responses = dict(responses or {})
        self.query_calls: list[tuple[str | None, str]] = []
        self.execute_calls: list[tuple[str | None, str]] = []

    def query(self, sql, label=None):
        self.query_calls.append((label, sql))
        if label not in self.responses:
            raise AssertionError(
                f"FakeSF.query: no canned response registered for label={label!r}\nsql={sql}"
            )
        return self.responses[label]

    def execute(self, sql, label=None):
        self.execute_calls.append((label, sql))
        return {"rowcount": 0, "sfqid": "fake-sfqid"}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _fake_client_cm(fake_sf: FakeSF):
    """A SnowflakeClient()-shaped context manager wrapping a FakeSF."""

    class _CM:
        def __enter__(self):
            return fake_sf

        def __exit__(self, *exc):
            return False

    return _CM()


# ─── DAG structural shape ──────────────────────────────────────────────────

def test_dag_id_schedule_tags():
    dag = MODULE.dag
    assert dag.dag_id == "orthopedic_raw_to_clean"
    assert dag.schedule == "@daily"
    assert dag.catchup is False
    assert set(dag.tags) == {"orthopedic", "v1", "transform", "clean"}


def test_task_chain_ensure_schema_list_flatten_summarize():
    dag = MODULE.dag
    task_ids = set(dag.task_dict.keys())
    assert task_ids == {
        "ensure_clean_schema", "list_target_tables", "flatten_table", "summarize_results",
    }

    t_schema = dag.get_task("ensure_clean_schema")
    t_list = dag.get_task("list_target_tables")
    t_flatten = dag.get_task("flatten_table")
    t_summary = dag.get_task("summarize_results")

    assert t_schema.upstream_task_ids == set()
    assert t_schema.downstream_task_ids == {"list_target_tables"}

    assert t_list.upstream_task_ids == {"ensure_clean_schema"}
    assert t_list.downstream_task_ids == {"flatten_table"}

    assert t_flatten.upstream_task_ids == {"list_target_tables"}
    assert t_flatten.downstream_task_ids == {"summarize_results"}
    assert t_flatten.is_mapped is True
    op_kwargs = t_flatten.mapped_kwargs["op_kwargs"]
    assert isinstance(op_kwargs, XComArgStub)
    assert op_kwargs.task.task_id == "list_target_tables"

    assert t_summary.upstream_task_ids == {"flatten_table"}
    assert t_summary.downstream_task_ids == set()


def test_flatten_table_and_summarize_results_use_all_done_trigger_rule():
    dag = MODULE.dag
    assert dag.get_task("flatten_table").trigger_rule == TriggerRule.ALL_DONE
    assert dag.get_task("summarize_results").trigger_rule == TriggerRule.ALL_DONE
    # Sanity: the non-terminal, non-mapped tasks keep the (stub) default.
    assert dag.get_task("ensure_clean_schema").trigger_rule == TriggerRule.ALL_SUCCESS


# ─── summarize_results: failure surfacing ──────────────────────────────────

def test_summarize_results_raises_on_any_error_and_names_failed_tables():
    results = [
        {"table": "a", "status": "ok", "mode": "ctas", "columns": 3},
        {"table": "b", "status": "error", "error": "boom"},
        {"table": "c", "status": "skipped", "columns": 0},
        {"table": "d", "status": "dry_run", "columns": 2},
        {"table": "e", "status": "error", "error": "kaboom"},
    ]
    with pytest.raises(RuntimeError) as exc_info:
        MODULE.summarize_results(results=results)
    msg = str(exc_info.value)
    assert "2 table(s) failed to flatten" in msg
    assert "'b'" in msg
    assert "'e'" in msg


def test_summarize_results_does_not_raise_when_all_succeed():
    results = [
        {"table": "a", "status": "ok", "mode": "ctas", "columns": 3},
        {"table": "b", "status": "skipped", "columns": 0},
        {"table": "c", "status": "dry_run", "columns": 1},
    ]
    assert MODULE.summarize_results(results=results) is None


@pytest.mark.parametrize("results", [None, []])
def test_summarize_results_handles_empty_or_missing_results(results):
    assert MODULE.summarize_results(results=results) is None


# ─── _col / _alias / _key_sql sanitization ─────────────────────────────────

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Patient Name", "patient_name"),
        ("SELECT", "select"),
        ("order by", "order_by"),
        ("na!!me??", "na_me"),
        ("  leading and trailing  ", "leading_and_trailing"),
        ("100_something", "c_100_something"),
        ("___", "col"),
        ("", "col"),
        ("Foo-Bar_Baz", "foo_bar_baz"),
    ],
)
def test_col_sanitization(raw, expected):
    assert MODULE._col(raw) == expected


def test_alias_quotes_and_escapes_reserved_and_special_names():
    assert MODULE._alias("select") == '"select"'
    assert MODULE._alias("order_by") == '"order_by"'
    assert MODULE._alias('weird"name') == '"weird""name"'


def test_key_sql_escapes_single_quotes():
    assert MODULE._key_sql("normal") == "'normal'"
    assert MODULE._key_sql("o'brien") == "'o''brien'"
    assert MODULE._key_sql("it's a 'test'") == "'it''s a ''test'''"


def test_dedup_cols_deduplicates_colliding_sanitized_names():
    # "Foo Bar", "foo_bar", "FOO--BAR" all sanitize to the same "foo_bar".
    pairs = [
        ("Foo Bar", "TEXT"),
        ("foo_bar", "INTEGER"),
        ("FOO--BAR", "BOOLEAN"),
    ]
    result = MODULE._dedup_cols(pairs, set())
    assert [r[1] for r in result] == ["foo_bar", "foo_bar_1", "foo_bar_2"]
    assert result[0] == ("Foo Bar", "foo_bar", MODULE._TYPE_CAST["TEXT"])
    assert result[1] == ("foo_bar", "foo_bar_1", MODULE._TYPE_CAST["INTEGER"])
    assert result[2] == ("FOO--BAR", "foo_bar_2", MODULE._TYPE_CAST["BOOLEAN"])


def test_dedup_cols_respects_preseeded_seen_set():
    pairs = [("First Name", "TEXT")]
    seen = {"first_name"}
    result = MODULE._dedup_cols(pairs, seen)
    assert result == [("First Name", "first_name_1", MODULE._TYPE_CAST["TEXT"])]
    assert "first_name_1" in seen


def test_dedup_cols_unknown_type_falls_back_to_default_cast():
    result = MODULE._dedup_cols([("weird", "SOME_UNKNOWN_TYPE")], set())
    assert result == [("weird", "weird", MODULE._DEFAULT_CAST)]


# ─── _query_key_types / _is_object_array / _discover_array_element_keys ───

def test_query_key_types_parses_rows_into_tuples_and_limits_sample_size():
    fake_sf = FakeSF({
        "mylabel": [("first_name", "TEXT"), ("address", "OBJECT"), ("tags", "ARRAY")],
    })
    result = MODULE._query_key_types(
        "HOSPITALS.ORTHOPEDIC_RAW.TBL", "payload", fake_sf, sample_size=777, label="mylabel",
    )
    assert result == [("first_name", "TEXT"), ("address", "OBJECT"), ("tags", "ARRAY")]
    label, sql = fake_sf.query_calls[0]
    assert label == "mylabel"
    assert "LIMIT 777" in sql
    assert "HOSPITALS.ORTHOPEDIC_RAW.TBL" in sql


@pytest.mark.parametrize(
    "rows, expected",
    [
        ([(5, 5)], True),     # 100% objects
        ([(3, 5)], True),     # 60% objects, > 0.5 threshold
        ([(2, 5)], False),    # 40% objects
        ([(0, 5)], False),    # no objects at all
        ([], False),          # empty sample
        ([(0, 0)], False),    # zero total guards against div-by-zero
    ],
)
def test_is_object_array_classification(rows, expected):
    fake_sf = FakeSF({"chk_arr:items": rows})
    assert MODULE._is_object_array(
        "HOSPITALS.ORTHOPEDIC_RAW.TBL", "items", fake_sf, sample_size=2000,
    ) is expected


def test_discover_array_element_keys_returns_key_superset():
    # Represents the union computed server-side (by the LATERAL FLATTEN +
    # QUALIFY ROW_NUMBER SQL) across array elements with inconsistent keys,
    # e.g. element 1 = {sku, qty}, element 2 = {qty, notes} -> superset
    # {sku, qty, notes}. The Python layer just reshapes the returned rows.
    fake_sf = FakeSF({
        "arr_keys:items": [("notes", "TEXT"), ("qty", "INTEGER"), ("sku", "TEXT")],
    })
    result = MODULE._discover_array_element_keys(
        "HOSPITALS.ORTHOPEDIC_RAW.TBL", "items", fake_sf, sample_size=2000,
    )
    assert result == [("notes", "TEXT"), ("qty", "INTEGER"), ("sku", "TEXT")]
    label, sql = fake_sf.query_calls[0]
    assert label == "arr_keys:items"
    assert "LIMIT 2000" in sql


# ─── _table_exists / _get_existing_columns ─────────────────────────────────

@pytest.mark.parametrize("rows, expected", [([(1,)], True), ([(0,)], False)])
def test_table_exists(rows, expected):
    fake_sf = FakeSF({"exists:tbl": rows})
    assert MODULE._table_exists("tbl", MODULE.SF_CLEAN_SCHEMA, fake_sf) is expected


def test_get_existing_columns_lowercases_and_sets():
    fake_sf = FakeSF({"cols:tbl": [("FIRST_NAME",), ("address__city",)]})
    result = MODULE._get_existing_columns("tbl", MODULE.SF_CLEAN_SCHEMA, fake_sf)
    assert result == {"first_name", "address__city"}


# ─── list_raw_tables ────────────────────────────────────────────────────────

def test_list_raw_tables_queries_information_schema_and_lowercases():
    fake_sf = FakeSF({"list_raw_tables": [("PATIENTS",), ("Visits",)]})
    result = MODULE.list_raw_tables(fake_sf)
    assert result == ["patients", "visits"]
    label, sql = fake_sf.query_calls[0]
    assert "ORTHOPEDIC_RAW" in sql
    assert "BASE TABLE" in sql


# ─── CTAS / INSERT builders ─────────────────────────────────────────────────

def test_build_parent_ctas_includes_all_column_kinds_and_correct_fqns():
    scalar_cols = [("first_name", "first_name", "::VARCHAR")]
    object_cols = [("address", "address", [("city", "city", "::VARCHAR"), ("zip", "zip", "::NUMBER")])]
    array_cols = [("tags", "tags", "::VARIANT")]

    sql = MODULE._build_parent_ctas("tbl", scalar_cols, object_cols, array_cols)

    assert sql.startswith("CREATE OR REPLACE TABLE HOSPITALS.ORTHOPEDIC_CLEAN.TBL AS")
    assert "FROM HOSPITALS.ORTHOPEDIC_RAW.TBL;" in sql
    assert "payload['first_name']::VARCHAR AS \"first_name\"" in sql
    assert "payload['address']['city']::VARCHAR AS \"address__city\"" in sql
    assert "payload['address']['zip']::NUMBER AS \"address__zip\"" in sql
    assert "payload['tags']::VARIANT AS \"tags\"" in sql


def test_build_child_ctas_flattens_array_and_correct_fqns():
    element_cols = [("sku", "sku", "::VARCHAR"), ("qty", "qty", "::NUMBER")]
    sql = MODULE._build_child_ctas("tbl", "items", "tbl__items", element_cols)

    assert sql.startswith("CREATE OR REPLACE TABLE HOSPITALS.ORTHOPEDIC_CLEAN.TBL__ITEMS AS")
    assert "FROM HOSPITALS.ORTHOPEDIC_RAW.TBL p," in sql
    assert "LATERAL FLATTEN(input => p.payload['items']) f" in sql
    assert "f.value['sku']::VARCHAR AS \"sku\"" in sql
    assert "f.value['qty']::NUMBER AS \"qty\"" in sql
    assert "f.index AS \"_array_index\"" in sql
    assert "WHERE TYPEOF(p.payload['items']) = 'ARRAY';" in sql


def test_build_parent_insert_includes_all_columns_and_anti_join():
    scalar_cols = [("first_name", "first_name", "::VARCHAR")]
    object_cols = [("address", "address", [("city", "city", "::VARCHAR")])]
    array_cols = [("tags", "tags", "::VARIANT")]

    sql = MODULE._build_parent_insert("tbl", scalar_cols, object_cols, array_cols)

    assert sql.startswith("INSERT INTO HOSPITALS.ORTHOPEDIC_CLEAN.TBL")
    assert '"first_name"' in sql
    assert '"address__city"' in sql
    assert '"tags"' in sql
    assert "payload['first_name']::VARCHAR" in sql
    assert "payload['address']['city']::VARCHAR" in sql
    assert "payload['tags']::VARIANT" in sql
    assert "FROM HOSPITALS.ORTHOPEDIC_RAW.TBL" in sql
    assert "EXCEPT" in sql
    assert "WHERE _run_id IN (" in sql


def test_build_child_insert_includes_all_columns_and_anti_join():
    element_cols = [("sku", "sku", "::VARCHAR")]
    sql = MODULE._build_child_insert("tbl", "items", "tbl__items", element_cols)

    assert sql.startswith("INSERT INTO HOSPITALS.ORTHOPEDIC_CLEAN.TBL__ITEMS")
    assert '"sku"' in sql
    assert '"_array_index"' in sql
    assert "f.value['sku']::VARCHAR" in sql
    assert "LATERAL FLATTEN(input => p.payload['items']) f" in sql
    assert "EXCEPT" in sql
    assert "AND p._run_id IN (" in sql


def test_build_parent_ctas_zero_columns_is_a_known_sql_bug():
    """
    KNOWN BUG (flagged, not fixed — see task constraints): when a table
    contributes zero actual columns to the parent (e.g. its only discovered
    top-level key is an OBJECT with zero discovered sub-keys, so
    scalar_cols/object_cols/array_cols are all empty even though top_pairs
    was non-empty), _build_parent_ctas still emits a bare, trailing-comma
    "column" line before FROM:

        _ingested_at,

    FROM ...;

    That is not valid SQL (dangling comma / empty select-list item) and
    would fail if ever sent to Snowflake. _flatten_table only guards the
    *simpler* all-empty-payload case (top_pairs itself empty -> returns
    status="skipped" without generating SQL at all); it does NOT guard this
    "every discovered key produced zero columns" case. This test documents
    the current (buggy) output rather than asserting the SQL is valid.
    """
    sql = MODULE._build_parent_ctas("tbl", [], [], [])
    assert "_ingested_at,\n    \nFROM HOSPITALS.ORTHOPEDIC_RAW.TBL;" in sql


def test_build_child_ctas_zero_columns_is_the_same_known_sql_bug():
    sql = MODULE._build_child_ctas("tbl", "items", "tbl__items", [])
    assert "\"_array_index\",\n    \nFROM HOSPITALS.ORTHOPEDIC_RAW.TBL p," in sql


# ─── _flatten_table: end-to-end routing decisions ──────────────────────────

def _discovery_fixture_responses():
    """
    A synthetic payload with one of each kind of top-level key:
      first_name  -> scalar (TEXT)
      address     -> OBJECT with sub-keys city (TEXT), zip (INTEGER)
      tags        -> ARRAY of scalars (_is_object_array -> False)
      items       -> ARRAY of OBJECTs (_is_object_array -> True), elements
                     have keys sku (TEXT), qty (INTEGER)
    """
    return {
        "disc:tbl": [
            ("first_name", "TEXT"),
            ("address", "OBJECT"),
            ("tags", "ARRAY"),
            ("items", "ARRAY"),
        ],
        "obj:tbl.address": [("city", "TEXT"), ("zip", "INTEGER")],
        "chk_arr:tags": [(0, 5)],     # not an object array
        "chk_arr:items": [(5, 5)],    # an object array
        "arr_keys:items": [("sku", "TEXT"), ("qty", "INTEGER")],
    }


def test_flatten_table_first_run_ctas_routes_every_key_kind_correctly():
    fake_sf = FakeSF(_discovery_fixture_responses())

    result = MODULE._flatten_table(
        "tbl", fake_sf, sample_size=2000, dry_run=False, full_refresh=True,
    )

    assert result["status"] == "ok"
    assert result["mode"] == "ctas"
    # 1 scalar + 2 object sub-cols + 2 array cols (tags AND items — see note
    # below) = 5 parent columns.
    assert result["columns"] == 5
    assert result["children"] == ["tbl__items"]

    # full_refresh=True must short-circuit _table_exists entirely (no
    # "exists:*" label was registered in the fixture — if the code called
    # it anyway, FakeSF would have raised).
    assert not any(lbl and lbl.startswith("exists:") for lbl, _ in fake_sf.query_calls)

    assert len(fake_sf.execute_calls) == 2
    parent_label, parent_sql = fake_sf.execute_calls[0]
    child_label, child_sql = fake_sf.execute_calls[1]
    assert parent_label == "ctas:tbl"
    assert child_label == "ctas:tbl__items"

    assert parent_sql.startswith("CREATE OR REPLACE TABLE HOSPITALS.ORTHOPEDIC_CLEAN.TBL AS")
    assert "payload['first_name']::VARCHAR AS \"first_name\"" in parent_sql
    assert "payload['address']['city']::VARCHAR AS \"address__city\"" in parent_sql
    assert "payload['address']['zip']::NUMBER AS \"address__zip\"" in parent_sql
    assert "payload['tags']::VARIANT AS \"tags\"" in parent_sql
    # NOTE (discrepancy vs. module docstring, flagged in report): the
    # docstring says an ARRAY-of-OBJECT key becomes *only* a child table,
    # while ARRAY-of-scalars is "kept as a single VARIANT column on the
    # parent". The actual code unconditionally appends every ARRAY key
    # (object-array or not) to array_cols, so "items" (an object array)
    # *also* ends up as a VARIANT column on the parent, in addition to
    # getting its own child table below. This test documents that actual
    # behavior.
    assert "payload['items']::VARIANT AS \"items\"" in parent_sql

    assert child_sql.startswith("CREATE OR REPLACE TABLE HOSPITALS.ORTHOPEDIC_CLEAN.TBL__ITEMS AS")
    assert "LATERAL FLATTEN(input => p.payload['items']) f" in child_sql
    assert "f.value['sku']::VARCHAR AS \"sku\"" in child_sql
    assert "f.value['qty']::NUMBER AS \"qty\"" in child_sql


def test_flatten_table_incremental_insert_filters_to_existing_columns():
    responses = _discovery_fixture_responses()
    responses.update({
        "exists:tbl": [(1,)],          # parent clean table already exists
        # Only first_name and address__city already exist on the clean
        # table; address__zip, tags, and items are new payload keys that
        # must be dropped from this incremental run per the module's
        # documented "ignore new keys until full refresh" behavior.
        "cols:tbl": [("first_name",), ("address__city",)],
        "exists:tbl__items": [(0,)],   # child table does not exist yet
    })
    fake_sf = FakeSF(responses)

    result = MODULE._flatten_table(
        "tbl", fake_sf, sample_size=2000, dry_run=False, full_refresh=False,
    )

    assert result["status"] == "ok"
    assert result["mode"] == "insert"
    assert result["children"] == ["tbl__items"]

    parent_label, parent_sql = fake_sf.execute_calls[0]
    assert parent_label == "insert:tbl"
    assert parent_sql.startswith("INSERT INTO HOSPITALS.ORTHOPEDIC_CLEAN.TBL")
    assert '"first_name"' in parent_sql
    assert '"address__city"' in parent_sql
    assert '"address__zip"' not in parent_sql
    assert '"tags"' not in parent_sql
    assert '"items"' not in parent_sql
    assert "EXCEPT" in parent_sql

    # Child table didn't exist -> CTAS (not filtered), even though parent used INSERT.
    child_label, child_sql = fake_sf.execute_calls[1]
    assert child_label == "ctas:tbl__items"
    assert child_sql.startswith("CREATE OR REPLACE TABLE HOSPITALS.ORTHOPEDIC_CLEAN.TBL__ITEMS AS")


def test_flatten_table_incremental_insert_also_filters_existing_child_columns():
    responses = _discovery_fixture_responses()
    responses.update({
        "exists:tbl": [(1,)],
        "cols:tbl": [("first_name",), ("address__city",), ("address__zip",), ("tags",), ("items",)],
        "exists:tbl__items": [(1,)],           # child table already exists too
        "cols:tbl__items": [("sku",)],         # only sku already present; qty is new
    })
    fake_sf = FakeSF(responses)

    result = MODULE._flatten_table(
        "tbl", fake_sf, sample_size=2000, dry_run=False, full_refresh=False,
    )

    assert result["mode"] == "insert"
    child_label, child_sql = fake_sf.execute_calls[1]
    assert child_label == "insert:tbl__items"
    assert child_sql.startswith("INSERT INTO HOSPITALS.ORTHOPEDIC_CLEAN.TBL__ITEMS")
    assert '"sku"' in child_sql
    assert '"qty"' not in child_sql


def test_flatten_table_no_payload_keys_is_skipped_without_generating_sql():
    fake_sf = FakeSF({"disc:tbl": []})
    result = MODULE._flatten_table(
        "tbl", fake_sf, sample_size=2000, dry_run=False, full_refresh=True,
    )
    assert result == {"table": "tbl", "status": "skipped", "columns": 0}
    assert fake_sf.execute_calls == []


def test_flatten_table_dry_run_skips_execution_but_returns_result():
    fake_sf = FakeSF(_discovery_fixture_responses())
    result = MODULE._flatten_table(
        "tbl", fake_sf, sample_size=2000, dry_run=True, full_refresh=True,
    )
    assert result == {"table": "tbl", "status": "dry_run", "columns": 5}
    assert fake_sf.execute_calls == []


def test_flatten_table_sample_size_propagates_into_limit_clauses():
    fake_sf = FakeSF(_discovery_fixture_responses())
    MODULE._flatten_table(
        "tbl", fake_sf, sample_size=42, dry_run=True, full_refresh=True,
    )
    sql_by_label = {lbl: sql for lbl, sql in fake_sf.query_calls}
    assert "LIMIT 42" in sql_by_label["disc:tbl"]
    assert "LIMIT 42" in sql_by_label["obj:tbl.address"]
    assert "LIMIT 42" in sql_by_label["chk_arr:tags"]
    assert "LIMIT 42" in sql_by_label["chk_arr:items"]
    assert "LIMIT 42" in sql_by_label["arr_keys:items"]


# ─── ensure_clean_schema ───────────────────────────────────────────────────

def test_ensure_clean_schema_issues_create_schema_if_not_exists(mocker):
    fake_sf = FakeSF()
    mocker.patch.object(MODULE, "SnowflakeClient", return_value=_fake_client_cm(fake_sf))

    MODULE.ensure_clean_schema()

    assert len(fake_sf.execute_calls) == 1
    label, sql = fake_sf.execute_calls[0]
    assert label == "ensure_clean_schema"
    assert sql == "CREATE SCHEMA IF NOT EXISTS HOSPITALS.ORTHOPEDIC_CLEAN;"


# ─── list_target_tables ─────────────────────────────────────────────────────

def test_list_target_tables_defaults_to_all_raw_base_tables(mocker):
    fake_sf = FakeSF({"list_raw_tables": [("PATIENTS",), ("VISITS",)]})
    mocker.patch.object(MODULE, "SnowflakeClient", return_value=_fake_client_cm(fake_sf))

    result = MODULE.list_target_tables()

    assert result == [{"table": "patients"}, {"table": "visits"}]
    assert len(fake_sf.query_calls) == 1  # only the discovery query, no override path


def test_list_target_tables_honors_variable_override(mocker, set_variables):
    set_variables(ORTHOPEDIC_CLEAN_TABLES="TableA, TableB ,tableC")
    fake_sf = FakeSF()  # no responses registered: override path must not query at all
    mocker.patch.object(MODULE, "SnowflakeClient", return_value=_fake_client_cm(fake_sf))

    result = MODULE.list_target_tables()

    assert result == [{"table": "tablea"}, {"table": "tableb"}, {"table": "tablec"}]
    assert fake_sf.query_calls == []


def test_list_target_tables_empty_override_falls_back_to_discovery(mocker, set_variables):
    set_variables(ORTHOPEDIC_CLEAN_TABLES="   ")
    fake_sf = FakeSF({"list_raw_tables": [("PATIENTS",)]})
    mocker.patch.object(MODULE, "SnowflakeClient", return_value=_fake_client_cm(fake_sf))

    result = MODULE.list_target_tables()

    assert result == [{"table": "patients"}]
    assert len(fake_sf.query_calls) == 1


def test_list_target_tables_returns_empty_list_when_nothing_found(mocker):
    fake_sf = FakeSF({"list_raw_tables": []})
    mocker.patch.object(MODULE, "SnowflakeClient", return_value=_fake_client_cm(fake_sf))

    assert MODULE.list_target_tables() == []


# ─── flatten_table_task: Variable-driven behavior ──────────────────────────

def test_flatten_table_task_default_sample_size_is_2000(mocker):
    fake_sf = FakeSF(_discovery_fixture_responses())
    fake_sf.responses["exists:tbl"] = [(0,)]
    mocker.patch.object(MODULE, "SnowflakeClient", return_value=_fake_client_cm(fake_sf))

    result = MODULE.flatten_table_task("tbl")

    assert result["status"] == "ok"
    label, sql = fake_sf.query_calls[0]
    assert "LIMIT 2000" in sql


def test_flatten_table_task_sample_size_variable_changes_limit(mocker, set_variables):
    set_variables(ORTHOPEDIC_CLEAN_SAMPLE_SIZE="150")
    fake_sf = FakeSF(_discovery_fixture_responses())
    fake_sf.responses["exists:tbl"] = [(0,)]
    mocker.patch.object(MODULE, "SnowflakeClient", return_value=_fake_client_cm(fake_sf))

    MODULE.flatten_table_task("tbl")

    label, sql = fake_sf.query_calls[0]
    assert "LIMIT 150" in sql


def test_flatten_table_task_full_refresh_variable_forces_ctas(mocker, set_variables):
    set_variables(ORTHOPEDIC_CLEAN_FULL_REFRESH="true")
    # No "exists:tbl" registered: full_refresh=True must short-circuit
    # _table_exists entirely, or FakeSF would raise on the missing label.
    fake_sf = FakeSF(_discovery_fixture_responses())
    mocker.patch.object(MODULE, "SnowflakeClient", return_value=_fake_client_cm(fake_sf))

    result = MODULE.flatten_table_task("tbl")

    assert result["status"] == "ok"
    assert result["mode"] == "ctas"


def test_flatten_table_task_full_refresh_false_and_parent_exists_uses_insert(mocker, set_variables):
    set_variables(ORTHOPEDIC_CLEAN_FULL_REFRESH="false")
    responses = _discovery_fixture_responses()
    responses.update({
        "exists:tbl": [(1,)],
        "cols:tbl": [("first_name",), ("address__city",), ("address__zip",), ("tags",), ("items",)],
        "exists:tbl__items": [(0,)],
    })
    fake_sf = FakeSF(responses)
    mocker.patch.object(MODULE, "SnowflakeClient", return_value=_fake_client_cm(fake_sf))

    result = MODULE.flatten_table_task("tbl")

    assert result["status"] == "ok"
    assert result["mode"] == "insert"


def test_flatten_table_task_dry_run_variable_skips_execution(mocker, set_variables):
    set_variables(ORTHOPEDIC_CLEAN_DRY_RUN="true")
    fake_sf = FakeSF(_discovery_fixture_responses())
    fake_sf.responses["exists:tbl"] = [(0,)]
    mocker.patch.object(MODULE, "SnowflakeClient", return_value=_fake_client_cm(fake_sf))

    result = MODULE.flatten_table_task("tbl")

    assert result["status"] == "dry_run"
    assert result["columns"] == 5
    assert fake_sf.execute_calls == []


def test_flatten_table_task_dry_run_false_by_default_executes(mocker):
    fake_sf = FakeSF(_discovery_fixture_responses())
    fake_sf.responses["exists:tbl"] = [(0,)]
    mocker.patch.object(MODULE, "SnowflakeClient", return_value=_fake_client_cm(fake_sf))

    result = MODULE.flatten_table_task("tbl")

    assert result["status"] == "ok"
    assert len(fake_sf.execute_calls) >= 1


def test_flatten_table_task_catches_exceptions_and_returns_error_status(mocker):
    # No responses registered at all -> the first sf.query() call inside
    # _flatten_table raises AssertionError, which flatten_table_task must
    # catch and translate into a per-table error result rather than letting
    # it propagate (that's how one bad table doesn't take down the whole
    # mapped task fan-out before summarize_results gets a chance to run).
    fake_sf = FakeSF({})
    mocker.patch.object(MODULE, "SnowflakeClient", return_value=_fake_client_cm(fake_sf))

    result = MODULE.flatten_table_task("brokentable")

    assert result["table"] == "brokentable"
    assert result["status"] == "error"
    assert "error" in result


# ─── _dominant_type_query dead-code confirmation ───────────────────────────

def test_dominant_type_query_was_actually_dropped_as_dead_code():
    """
    The build agent that converted the standalone script reported dropping
    `_dominant_type_query` as unused/dead code during the DAG conversion.
    Confirm that: it must not exist on the converted module, and the name
    must not appear anywhere in dags/orthopedic_raw_to_clean.py's source
    (i.e. no residual call site or definition was left behind).
    """
    assert not hasattr(MODULE, "_dominant_type_query")
    source = (DAGS_DIR / "orthopedic_raw_to_clean.py").read_text(encoding="utf-8")
    assert "_dominant_type_query" not in source
