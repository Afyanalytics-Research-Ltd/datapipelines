"""
Deep coverage for dags/orthopedic_v2_raw_pipeline.py — the 19-namespace v2
orthopedic ingestion DAG (gateway connection_id=20, facility_id=47), landing
into HOSPITALS.ORTHOPEDIC_RAW_V2. Distinct from dags/orthopedic_api_to_snowflake.py
(v1, 26 namespaces, connection_id=16), which is only read here as a read-only
sanity comparison, never modified or itself under test.

Per this DAG's own docstring, ORTHOPEDIC_RAW_V2 is a deliberately NOT
de-identified bronze/raw layer (no anonymize / anonymize_fields /
row_transform on any of its 19 models) — de-identification is deferred to
the separate orthopedic_v2_clean_pipeline. That governance invariant is
explicitly asserted below.
"""
from __future__ import annotations

import gzip
import json
from unittest.mock import MagicMock

import pytest
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

from tests.helpers import load_dag_module

MODULE_NAME = "orthopedic_v2_raw_pipeline"


@pytest.fixture
def module():
    return load_dag_module(MODULE_NAME)


class FakeResponse:
    def __init__(self, status_code, json_data=None, text=""):
        self.status_code = status_code
        self._json = json_data if json_data is not None else {}
        self.text = text
        self.ok = 200 <= status_code < 400

    def json(self):
        return self._json


def paged_side_effect(page_responses, login_token="tok-1", login_suffix="/auth/login"):
    """requests.post side_effect keyed on URL suffix + the request's "page"
    field, so it's safe under ThreadPoolExecutor concurrency where call
    order across pages is not deterministic."""

    def _fn(url, headers=None, json=None, timeout=None, **kwargs):
        if url.endswith(login_suffix):
            return FakeResponse(200, {"token": login_token})
        page = (json or {}).get("page")
        resp = page_responses[page]
        if isinstance(resp, Exception):
            raise resp
        return resp

    return _fn


# ── DAG structural shape ─────────────────────────────────────────────────
def test_dag_id_schedule_tags(module):
    dag = module.dag
    assert dag.dag_id == "orthopedic_v2_raw_pipeline"
    assert dag.schedule == "@daily"
    assert dag.catchup is False
    assert "v2" in dag.tags
    assert "orthopedic" in dag.tags


def test_dag_id_distinct_from_v1(module):
    v1 = load_dag_module("orthopedic_api_to_snowflake")
    assert module.dag.dag_id != v1.dag.dag_id
    assert module.dag.dag_id == "orthopedic_v2_raw_pipeline"
    assert v1.dag.dag_id == "orthopedic_api_to_snowflake"


def test_task_chain_and_v2_naming(module):
    dag = module.dag
    expected_order = [
        "ensure_orthopedic_v2_raw_schema",
        "prepare_orthopedic_v2_jobs",
        "extract_orthopedic_v2_to_s3",
        "copy_into_orthopedic_v2_raw",
        "update_orthopedic_v2_watermarks",
    ]
    assert set(dag.task_ids) == set(expected_order)

    # every task_id carries a "v2" marker so it can never collide with v1's
    # task_ids even if both DAGs were ever merged into one file.
    for task_id in expected_order:
        assert "v2" in task_id, f"{task_id!r} is missing the v2 marker"

    for upstream_id, downstream_id in zip(expected_order, expected_order[1:]):
        upstream = dag.get_task(upstream_id)
        downstream = dag.get_task(downstream_id)
        assert downstream_id in upstream.downstream_task_ids
        assert upstream_id in downstream.upstream_task_ids


def test_extract_and_copy_tasks_are_dynamically_mapped(module):
    dag = module.dag
    t_extract = dag.get_task("extract_orthopedic_v2_to_s3")
    t_copy = dag.get_task("copy_into_orthopedic_v2_raw")
    t_ensure = dag.get_task("ensure_orthopedic_v2_raw_schema")
    t_prepare = dag.get_task("prepare_orthopedic_v2_jobs")
    t_watermark = dag.get_task("update_orthopedic_v2_watermarks")

    assert t_extract.is_mapped is True
    assert t_copy.is_mapped is True
    assert t_ensure.is_mapped is False
    assert t_prepare.is_mapped is False
    assert t_watermark.is_mapped is False

    assert t_extract.mapped_kwargs is not None and "op_kwargs" in t_extract.mapped_kwargs
    assert t_copy.mapped_kwargs is not None and "op_kwargs" in t_copy.mapped_kwargs


# ── Model registry (19 namespaces, no de-identification) ────────────────
def test_models_list_has_exactly_19_entries(module):
    assert len(module.MODELS) == 19


def test_every_model_has_namespace_and_table(module):
    for m in module.MODELS:
        assert m.get("namespace"), f"model missing namespace: {m}"
        assert m.get("table"), f"model missing table: {m}"
        assert isinstance(m["namespace"], str)
        assert isinstance(m["table"], str)


def test_model_tables_are_unique(module):
    tables = [m["table"] for m in module.MODELS]
    assert len(tables) == len(set(tables))


def test_no_model_has_anonymize_or_row_transform_config(module):
    """Governance invariant from the module docstring: ORTHOPEDIC_RAW_V2 is
    a full-fidelity bronze layer. A failure here means someone accidentally
    started (or stopped) redacting at ingestion time against the documented
    design — flag, don't silently "fix"."""
    offending = [
        m["table"]
        for m in module.MODELS
        if "anonymize" in m or "anonymize_fields" in m or "row_transform" in m
    ]
    assert offending == [], (
        f"models unexpectedly carry anonymize/row_transform config: {offending} "
        "— contradicts the RAW_V2 'not de-identified at this stage' governance note"
    )
    for m in module.MODELS:
        assert set(m.keys()) == {"namespace", "table"}


# ── v1 vs v2 read-only sanity comparison ─────────────────────────────────
def test_v1_vs_v2_namespace_counts_and_ids_match_docstring_claims():
    v1 = load_dag_module("orthopedic_api_to_snowflake")
    v2 = load_dag_module(MODULE_NAME)

    assert len(v1.MODELS) == 26
    assert len(v2.MODELS) == 19

    assert v1._get_afya_connection_id() == 16
    assert int(v2.__dict__.get("Variable").get("ORTHOPEDIC_V2_GATEWAY_CONNECTION_ID", default_var="20")) == 20

    assert v1._get_afya_facility_id() == 47
    assert int(v2.__dict__.get("Variable").get("ORTHOPEDIC_V2_FACILITY_ID", default_var="47")) == 47

    assert v1.dag.dag_id != v2.dag.dag_id


# ── Variables: connection_id / facility_id defaults + overrides ─────────
def test_prepare_jobs_uses_default_connection_and_facility(module):
    jobs = module.prepare_all_jobs()
    assert len(jobs) == 19
    for entry in jobs:
        job = entry["job"]
        assert job["gateway_connection_id"] == 20
        assert job["facility_id"] == 47


def test_prepare_jobs_honors_variable_overrides(module, set_variables):
    set_variables(ORTHOPEDIC_V2_GATEWAY_CONNECTION_ID="99", ORTHOPEDIC_V2_FACILITY_ID="5")
    jobs = module.prepare_all_jobs()
    for entry in jobs:
        job = entry["job"]
        assert job["gateway_connection_id"] == 99
        assert job["facility_id"] == 5


def test_prepare_jobs_default_watermark_and_registered_watermark(module, set_variables):
    set_variables(**{module._wm_key("admnotes"): "2026-01-01T00:00:00Z"})
    jobs = module.prepare_all_jobs()
    by_table = {e["job"]["table"]: e["job"] for e in jobs}
    assert by_table["admnotes"]["updated_since"] == "2026-01-01T00:00:00Z"
    # a model with no registered watermark falls back to the epoch default.
    assert by_table["cadex"]["updated_since"] == "1970-01-01T00:00:00Z"


def test_prepare_jobs_includes_all_namespaces_and_tables(module):
    jobs = module.prepare_all_jobs()
    got = {(e["job"]["namespace"], e["job"]["table"]) for e in jobs}
    expected = {(m["namespace"], m["table"]) for m in module.MODELS}
    assert got == expected


# ── small helpers ─────────────────────────────────────────────────────────
def test_wm_key_format(module):
    assert module._wm_key("admnotes") == "orthopedic_v2__orthopedic_v2_raw_pipeline__admnotes"


def test_table_fqn_format(module):
    assert module._table_fqn("admnotes") == "HOSPITALS.ORTHOPEDIC_RAW_V2.ADMNOTES"


def test_safe_sanitizes_unsafe_characters(module):
    assert module._safe("App\\Models\\Admonotes") == "App_Models_Admonotes"
    assert module._safe("  weird value!! ") == "weird_value_"
    assert module._safe(None) == ""


# ── nested pagination parsing ────────────────────────────────────────────
def test_extract_rows_and_pagination_flat_shape(module):
    payload = {"data": [{"id": 1}, {"id": 2}], "pagination": {"last_page": 4}}
    rows, pagination = module._extract_rows_and_pagination(payload)
    assert rows == [{"id": 1}, {"id": 2}]
    assert pagination == {"last_page": 4}


def test_extract_rows_and_pagination_nested_shape(module):
    payload = {"data": {"data": [{"id": 1}, {"id": 2}], "current_page": 1, "last_page": 3}}
    rows, pagination = module._extract_rows_and_pagination(payload)
    assert rows == [{"id": 1}, {"id": 2}]
    assert pagination["last_page"] == 3
    assert pagination["current_page"] == 1


def test_extract_rows_and_pagination_nested_shape_with_meta(module):
    payload = {
        "data": {"data": [{"id": 9}], "current_page": 2},
        "meta": {"last_page": 5},
    }
    rows, pagination = module._extract_rows_and_pagination(payload)
    assert rows == [{"id": 9}]
    # top-level meta is merged in *after* the nested dict, so it wins.
    assert pagination["last_page"] == 5
    assert pagination["current_page"] == 2


def test_extract_rows_and_pagination_missing_data_returns_empty(module):
    rows, pagination = module._extract_rows_and_pagination({})
    assert rows == []
    assert pagination == {}


def test_parse_last_page_variants(module):
    assert module._parse_last_page({"last_page": 7}) == 7
    assert module._parse_last_page({"total_pages": "3"}) == 3
    assert module._parse_last_page({"pageCount": 2}) == 2


def test_parse_last_page_missing_or_invalid(module):
    assert module._parse_last_page({}) is None
    assert module._parse_last_page({"last_page": "not-a-number"}) is None
    assert module._parse_last_page({"last_page": 0}) is None


# ── gateway auth (_login) ────────────────────────────────────────────────
def test_login_success(module, monkeypatch):
    mock_post = MagicMock(return_value=FakeResponse(200, {"token": "abc123"}))
    monkeypatch.setattr(module.requests, "post", mock_post)
    token = module._login("https://afyapi.afyaanalytics.ai/api", "user", "pw")
    assert token == "abc123"
    mock_post.assert_called_once()
    assert mock_post.call_args.args[0] == "https://afyapi.afyaanalytics.ai/api/auth/login"


def test_login_failure_bad_status_raises(module, monkeypatch):
    mock_post = MagicMock(return_value=FakeResponse(401, {}, text="bad creds"))
    monkeypatch.setattr(module.requests, "post", mock_post)
    with pytest.raises(RuntimeError, match="Login failed"):
        module._login("https://afyapi.afyaanalytics.ai/api", "user", "pw")


def test_login_missing_token_raises(module, monkeypatch):
    mock_post = MagicMock(return_value=FakeResponse(200, {}))
    monkeypatch.setattr(module.requests, "post", mock_post)
    with pytest.raises(RuntimeError, match="no 'token' field"):
        module._login("https://afyapi.afyaanalytics.ai/api", "user", "pw")


# ── gateway retry matrix (_gateway_request) ──────────────────────────────
def _call_gateway(module, side_effect, monkeypatch, **overrides):
    monkeypatch.setattr(module.requests, "post", MagicMock(side_effect=side_effect))
    monkeypatch.setattr(module.time, "sleep", MagicMock())
    token_box = ["tok-0"]
    kwargs = dict(
        base_url="https://afyapi.afyaanalytics.ai/api",
        gateway_connection_id=20,
        facility_id=47,
        namespace=r"App\Models\Admonotes",
        page=1,
        per_page=950,
        updated_since=None,
        token_box=token_box,
        creds=("user", "pw"),
    )
    kwargs.update(overrides)
    return module._gateway_request(**kwargs), token_box


def test_gateway_request_401_refreshes_token_and_retries(module, monkeypatch):
    calls = [
        FakeResponse(401),
        FakeResponse(200, {"token": "fresh-token"}),
        FakeResponse(200, {"data": [{"id": 1}]}),
    ]
    result, token_box = _call_gateway(module, calls, monkeypatch)
    assert result == {"data": [{"id": 1}]}
    assert token_box[0] == "fresh-token"


def test_gateway_request_401_exhausts_retries(module, monkeypatch):
    calls = [FakeResponse(401), FakeResponse(200, {"token": "t2"}), FakeResponse(401)]
    with pytest.raises(RuntimeError, match="401 after 2 token refresh"):
        _call_gateway(module, calls, monkeypatch, max_retries=2)


def test_gateway_request_429_respects_retry_after(module, monkeypatch):
    calls = [
        FakeResponse(429, {"retry_after_seconds": 7}),
        FakeResponse(200, {"data": []}),
    ]
    sleep_mock = MagicMock()
    monkeypatch.setattr(module.requests, "post", MagicMock(side_effect=calls))
    monkeypatch.setattr(module.time, "sleep", sleep_mock)
    result, _ = _call_gateway(module, calls, monkeypatch)
    # re-patch sleep after helper re-set it, so assert against the second call
    assert result == {"data": []}
    module.time.sleep.assert_any_call(7)


def test_gateway_request_429_exhausts_retries(module, monkeypatch):
    calls = [
        FakeResponse(429, {"retry_after_seconds": 1}),
        FakeResponse(429, {"retry_after_seconds": 1}),
    ]
    with pytest.raises(RuntimeError, match="429 after 2 retries"):
        _call_gateway(module, calls, monkeypatch, max_retries=2)


def test_gateway_request_5xx_backoff_then_success(module, monkeypatch):
    calls = [FakeResponse(503), FakeResponse(200, {"data": [{"id": 1}]})]
    result, _ = _call_gateway(module, calls, monkeypatch, default_wait=10, backoff=2)
    assert result == {"data": [{"id": 1}]}
    module.time.sleep.assert_any_call(10)


def test_gateway_request_5xx_exhausts_retries(module, monkeypatch):
    calls = [FakeResponse(500), FakeResponse(502)]
    with pytest.raises(RuntimeError, match="500 after 2 retries|502 after 2 retries"):
        _call_gateway(module, calls, monkeypatch, max_retries=2)


def test_gateway_request_404_raises_without_retry(module, monkeypatch):
    mock_post = MagicMock(return_value=FakeResponse(404, {}, text="not registered"))
    monkeypatch.setattr(module.requests, "post", mock_post)
    monkeypatch.setattr(module.time, "sleep", MagicMock())
    with pytest.raises(RuntimeError, match="404"):
        module._gateway_request(
            "https://afyapi.afyaanalytics.ai/api", 20, 47, r"App\Models\Admonotes", 1, 950,
            None, ["tok"], ("user", "pw"),
        )
    assert mock_post.call_count == 1


def test_gateway_request_422_raises_without_retry(module, monkeypatch):
    mock_post = MagicMock(return_value=FakeResponse(422, {}, text="bad body"))
    monkeypatch.setattr(module.requests, "post", mock_post)
    monkeypatch.setattr(module.time, "sleep", MagicMock())
    with pytest.raises(RuntimeError, match="422"):
        module._gateway_request(
            "https://afyapi.afyaanalytics.ai/api", 20, 47, r"App\Models\Admonotes", 1, 950,
            None, ["tok"], ("user", "pw"),
        )
    assert mock_post.call_count == 1


@pytest.mark.parametrize("exc_cls", [Timeout, ConnectionError, ChunkedEncodingError])
def test_gateway_request_network_error_retries_then_succeeds(module, monkeypatch, exc_cls):
    calls = [exc_cls("boom"), FakeResponse(200, {"data": [{"id": 1}]})]
    result, _ = _call_gateway(module, calls, monkeypatch)
    assert result == {"data": [{"id": 1}]}


# ── extract_one_model: pagination accumulation + S3 upload ──────────────
def _job(**overrides):
    job = {
        "namespace": r"App\Models\Admonotes",
        "table": "admnotes",
        "gateway_connection_id": 20,
        "facility_id": 47,
        "updated_since": "1970-01-01T00:00:00Z",
        "per_page": 950,
        "page_workers": 2,
    }
    job.update(overrides)
    return job


def test_extract_one_model_nested_pagination_accumulates_all_rows(module, monkeypatch, register_connection):
    register_connection(
        module.API_CONN_ID,
        host="https://afyapi.afyaanalytics.ai/api",
        login="user",
        password="pw",
    )
    page_responses = {
        1: FakeResponse(200, {"data": {"data": [{"id": 1}, {"id": 2}], "current_page": 1, "last_page": 3}}),
        2: FakeResponse(200, {"data": {"data": [{"id": 3}, {"id": 4}], "current_page": 2, "last_page": 3}}),
        3: FakeResponse(200, {"data": {"data": [{"id": 5}, {"id": 6}], "current_page": 3, "last_page": 3}}),
    }
    monkeypatch.setattr(module.requests, "post", MagicMock(side_effect=paged_side_effect(page_responses)))

    mock_s3_cls = MagicMock()
    mock_s3_instance = mock_s3_cls.return_value
    monkeypatch.setattr(module, "S3Hook", mock_s3_cls)

    result = module.extract_one_model(_job(), run_id="manual_run_1")

    assert result["row_count"] == 6
    assert result["table"] == "admnotes"
    assert result["s3_key"] is not None
    assert "model=admnotes" in result["s3_key"]

    mock_s3_cls.assert_called_once_with(aws_conn_id=module.S3_CONN_ID)
    assert mock_s3_instance.load_bytes.call_count == 1
    upload_kwargs = mock_s3_instance.load_bytes.call_args.kwargs
    assert upload_kwargs["bucket_name"] == module.S3_BUCKET
    assert upload_kwargs["key"] == result["s3_key"]

    raw = gzip.decompress(upload_kwargs["bytes_data"])
    lines = [json.loads(line) for line in raw.splitlines() if line]
    assert {row["id"] for row in lines} == {1, 2, 3, 4, 5, 6}


def test_extract_one_model_zero_rows_skips_s3_upload(module, monkeypatch, register_connection):
    register_connection(
        module.API_CONN_ID,
        host="https://afyapi.afyaanalytics.ai/api",
        login="user",
        password="pw",
    )
    page_responses = {1: FakeResponse(200, {"data": []})}
    monkeypatch.setattr(module.requests, "post", MagicMock(side_effect=paged_side_effect(page_responses)))

    mock_s3_cls = MagicMock()
    monkeypatch.setattr(module, "S3Hook", mock_s3_cls)

    result = module.extract_one_model(_job(), run_id="manual_run_2")

    assert result["row_count"] == 0
    assert result["s3_key"] is None
    mock_s3_cls.assert_not_called()


def test_extract_one_model_sequential_exhaustion_when_last_page_unknown(module, monkeypatch, register_connection):
    register_connection(
        module.API_CONN_ID,
        host="https://afyapi.afyaanalytics.ai/api",
        login="user",
        password="pw",
    )
    page_responses = {
        1: FakeResponse(200, {"data": [{"id": i} for i in range(1, 11)], "pagination": {}}),
        2: FakeResponse(200, {"data": [{"id": 11}, {"id": 12}], "pagination": {"has_more_pages": False}}),
    }
    monkeypatch.setattr(module.requests, "post", MagicMock(side_effect=paged_side_effect(page_responses)))

    mock_s3_cls = MagicMock()
    monkeypatch.setattr(module, "S3Hook", mock_s3_cls)

    result = module.extract_one_model(_job(page_workers=1), run_id="manual_run_3")

    assert result["row_count"] == 12
    mock_s3_cls.return_value.load_bytes.assert_called_once()


# ── ensure_orthopedic_v2_raw_schema ───────────────────────────────────────
def _patched_snowflake_client(module, monkeypatch):
    mock_cls = MagicMock()
    mock_instance = mock_cls.return_value
    mock_instance.__enter__.return_value = mock_instance
    mock_instance.__exit__.return_value = False
    monkeypatch.setattr(module, "SnowflakeClient", mock_cls)
    return mock_cls, mock_instance


def test_ensure_schema_creates_schema_and_19_tables(module, monkeypatch):
    mock_cls, mock_instance = _patched_snowflake_client(module, monkeypatch)

    module.ensure_orthopedic_v2_raw_schema()

    mock_cls.assert_called_once_with(schema_=module.SF_RAW_SCHEMA)
    # 1 CREATE SCHEMA + 19 CREATE TABLE statements
    assert mock_instance.execute.call_count == 1 + len(module.MODELS)

    all_sql = " ".join(c.args[0] for c in mock_instance.execute.call_args_list)
    assert "CREATE SCHEMA IF NOT EXISTS HOSPITALS.ORTHOPEDIC_RAW_V2" in all_sql
    assert "CREATE TABLE IF NOT EXISTS HOSPITALS.ORTHOPEDIC_RAW_V2.ADMNOTES" in all_sql
    assert "_run_id" in all_sql and "_namespace" in all_sql and "_ingested_at" in all_sql and "payload" in all_sql


# ── copy_into_orthopedic_v2_raw ───────────────────────────────────────────
def test_copy_into_raw_executes_copy_sql(module, monkeypatch):
    mock_cls, mock_instance = _patched_snowflake_client(module, monkeypatch)

    result = module.copy_into_orthopedic_v2_raw(
        table="admnotes",
        s3_key="raw/orthopedic_v2/model=admnotes/dt=2026-09-22/run1.jsonl.gz",
        namespace=r"App\Models\Admonotes",
        ingested_at="2026-09-22T00:00:00+00:00",
    )

    assert result == {"table": "admnotes", "status": "ok"}
    mock_cls.assert_called_once_with(schema_=module.SF_RAW_SCHEMA)
    mock_instance.execute.assert_called_once()
    sql = mock_instance.execute.call_args.args[0]
    assert "COPY INTO HOSPITALS.ORTHOPEDIC_RAW_V2.ADMNOTES" in sql
    assert "FILES = ('raw/orthopedic_v2/model=admnotes/dt=2026-09-22/run1.jsonl.gz')" in sql
    assert f"FILE_FORMAT = (FORMAT_NAME = {module.SF_FILE_FORMAT})" in sql


def test_copy_into_raw_skips_when_no_s3_key(module, monkeypatch):
    mock_cls, mock_instance = _patched_snowflake_client(module, monkeypatch)

    result = module.copy_into_orthopedic_v2_raw(table="admnotes", s3_key=None, namespace="ns", ingested_at="ts")

    assert result == {"table": "admnotes", "status": "empty"}
    mock_cls.assert_not_called()


# ── update_watermarks: per-model success/failure gating ─────────────────
def test_update_watermarks_advances_only_successful_models(module, set_variables):
    fake_ti = MagicMock()
    fake_ti.xcom_pull.return_value = [
        {"table": "admnotes", "status": "ok"},
        {"table": "cadex", "status": "error"},
        None,
    ]

    module.update_watermarks(ti=fake_ti)

    fake_ti.xcom_pull.assert_called_once_with(task_ids="copy_into_orthopedic_v2_raw")
    assert module.Variable.get(module._wm_key("admnotes"), default_var=None) is not None
    assert module.Variable.get(module._wm_key("cadex"), default_var=None) is None


def test_update_watermarks_empty_status_also_advances(module):
    fake_ti = MagicMock()
    fake_ti.xcom_pull.return_value = [{"table": "history", "status": "empty"}]

    module.update_watermarks(ti=fake_ti)

    assert module.Variable.get(module._wm_key("history"), default_var=None) is not None


def test_update_watermarks_no_results_sets_nothing(module):
    fake_ti = MagicMock()
    fake_ti.xcom_pull.return_value = None

    module.update_watermarks(ti=fake_ti)  # must not raise

    for m in module.MODELS:
        assert module.Variable.get(module._wm_key(m["table"]), default_var=None) is None
