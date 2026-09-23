"""
Deep coverage for dags/orthopedic_api_to_snowflake.py (26-namespace v1
orthopedic ingestion DAG): PII redaction (_redact_orderitementries_pii /
_redact_supplier_contacts), the anonymize=False exceptions on the 26-entry
MODELS registry, gateway auth/pagination/retry logic, row_transform XCom
plumbing, and watermark-advance behavior (including verifying the claimed
"per-model gate" against the actual implementation).

All external I/O (requests, Snowflake, S3, BaseHook) is mocked — no real
network/credentials are touched.
"""
from __future__ import annotations

import gzip
import json
from unittest import mock

import pytest
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

from tests.airflow_stub import TriggerRule
from tests.helpers import load_dag_module

MODULE_NAME = "orthopedic_api_to_snowflake"


@pytest.fixture
def module():
    return load_dag_module(MODULE_NAME)


class FakeResponse:
    """Minimal stand-in for requests.Response."""

    def __init__(self, status_code: int, json_data=None, text: str = ""):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text
        self.ok = 200 <= status_code < 400

    def json(self):
        if self._json_data is None:
            raise ValueError("response has no JSON body")
        return self._json_data

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(f"HTTP {self.status_code}")


# ===========================================================================
# DAG structural shape
# ===========================================================================
class TestDagShape:
    def test_dag_id_schedule_tags(self, module):
        dag = module.dag
        assert dag.dag_id == "orthopedic_api_to_snowflake"
        assert dag.schedule == "@daily"
        assert dag.catchup is False
        assert set(dag.tags) == {"orthopedic", "v1", "api", "snowflake", "ingest"}

    def test_task_ids_present(self, module):
        dag = module.dag
        expected = {
            "ensure_orthopedic_raw_schema",
            "prepare_all_jobs",
            "extract_to_s3",
            "copy_into_orthopedic_raw",
            "update_watermarks",
        }
        assert expected == set(dag.task_ids)

    def test_task_chain_order(self, module):
        dag = module.dag
        t_ensure = dag.get_task("ensure_orthopedic_raw_schema")
        t_prepare = dag.get_task("prepare_all_jobs")
        t_extract = dag.get_task("extract_to_s3")
        t_copy = dag.get_task("copy_into_orthopedic_raw")
        t_watermark = dag.get_task("update_watermarks")

        assert t_prepare.upstream_task_ids == {"ensure_orthopedic_raw_schema"}
        assert t_extract.upstream_task_ids == {"prepare_all_jobs"}
        assert t_copy.upstream_task_ids == {"extract_to_s3"}
        assert t_watermark.upstream_task_ids == {"copy_into_orthopedic_raw"}

        assert t_ensure.downstream_task_ids == {"prepare_all_jobs"}
        assert t_prepare.downstream_task_ids == {"extract_to_s3"}
        assert t_extract.downstream_task_ids == {"copy_into_orthopedic_raw"}
        assert t_copy.downstream_task_ids == {"update_watermarks"}

    def test_extract_and_copy_are_mapped(self, module):
        dag = module.dag
        assert dag.get_task("extract_to_s3").is_mapped is True
        assert dag.get_task("copy_into_orthopedic_raw").is_mapped is True
        assert dag.get_task("ensure_orthopedic_raw_schema").is_mapped is False
        assert dag.get_task("prepare_all_jobs").is_mapped is False

    def test_update_watermarks_trigger_rule_is_all_done(self, module):
        dag = module.dag
        assert dag.get_task("update_watermarks").trigger_rule == TriggerRule.ALL_DONE
        # The mapped extract/copy tasks also use ALL_DONE per the house
        # template so one failing model doesn't block siblings from running.
        assert dag.get_task("extract_to_s3").trigger_rule == TriggerRule.ALL_DONE
        assert dag.get_task("copy_into_orthopedic_raw").trigger_rule == TriggerRule.ALL_DONE


# ===========================================================================
# PII redaction: _redact_orderitementries_pii
# ===========================================================================
class TestRedactOrderItemEntriesPii:
    def test_textarea_type_case_insensitive_substring_redacted(self, module):
        rows = [
            {"fields": [
                {"type": "Textarea", "name": "notes", "value": ["some clinical note"]},
                {"type": "IncrementalTextarea", "name": "history", "value": ["more notes"]},
                {"type": "textarea", "name": "comments", "value": ["lowercase variant"]},
            ]}
        ]
        out = module._redact_orderitementries_pii(rows)
        for f in out[0]["fields"]:
            assert f["value"] == ["[REDACTED]"]

    def test_surgeon_anaesthetist_name_exact_match_redacted_regardless_of_type(self, module):
        rows = [
            {"fields": [
                {"type": "SearchableFromPrevious", "name": "surgeon", "value": ["DR MARANYA"]},
                {"type": "SearchableFromPrevious", "name": "anaesthetist", "value": ["DR WANJALA"]},
            ]}
        ]
        out = module._redact_orderitementries_pii(rows)
        assert out[0]["fields"][0]["value"] == ["[REDACTED]"]
        assert out[0]["fields"][1]["value"] == ["[REDACTED]"]

    def test_name_match_is_case_insensitive_and_strips_whitespace(self, module):
        rows = [{"fields": [
            {"type": "text", "name": "  Surgeon  ", "value": ["DR X"]},
            {"type": "text", "name": "ANAESTHETIST", "value": ["DR Y"]},
        ]}]
        out = module._redact_orderitementries_pii(rows)
        assert out[0]["fields"][0]["value"] == ["[REDACTED]"]
        assert out[0]["fields"][1]["value"] == ["[REDACTED]"]

    def test_non_pii_fields_untouched(self, module):
        rows = [{"fields": [
            {"type": "text", "name": "diagnosis", "value": ["fracture"]},
            {"type": "select", "name": "priority", "value": ["high"]},
        ]}]
        out = module._redact_orderitementries_pii(rows)
        assert out[0]["fields"][0]["value"] == ["fracture"]
        assert out[0]["fields"][1]["value"] == ["high"]

    def test_name_substring_that_is_a_common_word_is_not_redacted(self, module):
        """Boundary case: the name check is an *exact* match against
        {"surgeon", "anaesthetist"}, not a substring match. A field named
        "surgeonNotes" (a plausible real form-builder field name) must NOT
        be redacted purely by name — only if its type also contains
        'textarea' would it qualify."""
        rows = [{"fields": [
            {"type": "text", "name": "surgeonNotes", "value": ["not actually the surgeon field"]},
            {"type": "text", "name": "anaesthetist_id", "value": ["42"]},
        ]}]
        out = module._redact_orderitementries_pii(rows)
        assert out[0]["fields"][0]["value"] == ["not actually the surgeon field"]
        assert out[0]["fields"][1]["value"] == ["42"]

    def test_missing_fields_key_does_not_crash(self, module):
        rows = [{"id": 1}]
        out = module._redact_orderitementries_pii(rows)
        assert out == [{"id": 1}]

    def test_fields_present_but_none_does_not_crash(self, module):
        rows = [{"id": 1, "fields": None}]
        out = module._redact_orderitementries_pii(rows)
        assert out[0]["fields"] is None

    def test_fields_not_a_list_malformed_does_not_crash(self, module):
        rows = [{"id": 1, "fields": "not-a-list"}]
        out = module._redact_orderitementries_pii(rows)
        assert out[0]["fields"] == "not-a-list"

    def test_field_item_not_a_dict_is_skipped_without_crash(self, module):
        rows = [{"fields": ["oops-a-string", {"type": "textarea", "name": "x", "value": ["y"]}]}]
        out = module._redact_orderitementries_pii(rows)
        assert out[0]["fields"][0] == "oops-a-string"
        assert out[0]["fields"][1]["value"] == ["[REDACTED]"]

    def test_field_with_missing_type_and_name_not_redacted(self, module):
        rows = [{"fields": [{"value": ["untouched"]}]}]
        out = module._redact_orderitementries_pii(rows)
        assert out[0]["fields"][0]["value"] == ["untouched"]

    def test_field_value_was_none_gets_overwritten_to_redacted_marker(self, module):
        rows = [{"fields": [{"type": "textarea", "name": "notes", "value": None}]}]
        out = module._redact_orderitementries_pii(rows)
        assert out[0]["fields"][0]["value"] == ["[REDACTED]"]

    def test_empty_rows_list_returns_empty_list(self, module):
        assert module._redact_orderitementries_pii([]) == []


# ===========================================================================
# PII redaction: _redact_supplier_contacts
# ===========================================================================
class TestRedactSupplierContacts:
    def test_multi_element_emails_array_fully_redacted(self, module):
        rows = [{"emails": ["a@x.com", "b@x.com", "c@x.com"]}]
        out = module._redact_supplier_contacts(rows)
        assert out[0]["emails"] == ["[REDACTED]"]
        assert "a@x.com" not in out[0]["emails"]
        assert "b@x.com" not in out[0]["emails"]
        assert "c@x.com" not in out[0]["emails"]

    def test_multi_element_phones_array_fully_redacted(self, module):
        rows = [{"phones": ["0700111222", "0700333444"]}]
        out = module._redact_supplier_contacts(rows)
        assert out[0]["phones"] == ["[REDACTED]"]

    def test_empty_arrays_left_as_is(self, module):
        rows = [{"emails": [], "phones": []}]
        out = module._redact_supplier_contacts(rows)
        assert out[0]["emails"] == []
        assert out[0]["phones"] == []

    def test_malformed_non_list_contact_fields_do_not_crash(self, module):
        """Risk flag (see test docstring / final report): if emails/phones
        ever arrives as a non-list (e.g. a bare string) the isinstance guard
        means it is silently left UNREDACTED rather than masked. This test
        documents the actual (current) behavior; it is not asserting this
        is desirable."""
        rows = [{"emails": "single@x.com", "phones": {"mobile": "0700"}}]
        out = module._redact_supplier_contacts(rows)
        # Documents current behavior: non-list contact fields pass through
        # untouched instead of being redacted.
        assert out[0]["emails"] == "single@x.com"
        assert out[0]["phones"] == {"mobile": "0700"}

    def test_missing_emails_and_phones_keys_do_not_crash(self, module):
        rows = [{"id": 1, "name": "Acme Supplies"}]
        out = module._redact_supplier_contacts(rows)
        assert out == [{"id": 1, "name": "Acme Supplies"}]

    def test_empty_rows_list_returns_empty_list(self, module):
        assert module._redact_supplier_contacts([]) == []


# ===========================================================================
# MODELS registry: anonymize=False exceptions
# ===========================================================================
class TestModelsAnonymizeFlags:
    ANONYMIZE_FALSE_TABLES = {"singleorderitems", "saleitems", "diagnoses2"}

    def test_registry_has_26_models(self, module):
        assert len(module.MODELS) == 26

    def test_exact_three_tables_have_anonymize_disabled(self, module):
        disabled = {m["table"] for m in module.MODELS if m.get("anonymize") is False}
        assert disabled == self.ANONYMIZE_FALSE_TABLES, (
            "The set of anonymize=False model overrides changed. If this is "
            "intentional, update ANONYMIZE_FALSE_TABLES in this test; "
            "otherwise this is a compliance-relevant regression."
        )

    def test_every_other_model_has_no_anonymize_false_override(self, module):
        for m in module.MODELS:
            if m["table"] in self.ANONYMIZE_FALSE_TABLES:
                assert m.get("anonymize") is False
            else:
                assert m.get("anonymize") is not False, (
                    f"model {m['table']!r} unexpectedly has anonymize=False"
                )

    def test_table_names_are_unique(self, module):
        tables = [m["table"] for m in module.MODELS]
        assert len(tables) == len(set(tables))

    def test_namespaces_are_unique(self, module):
        namespaces = [m["namespace"] for m in module.MODELS]
        assert len(namespaces) == len(set(namespaces))


# ===========================================================================
# row_transform -> _ROW_TRANSFORMS string-key XCom plumbing
# ===========================================================================
class TestRowTransformLookup:
    def test_orderitementries_row_transform_resolves_to_redact_function(self, module):
        model = next(m for m in module.MODELS if m["table"] == "orderitementries")
        assert model["row_transform"] == "orderitementries_pii"
        assert module._ROW_TRANSFORMS[model["row_transform"]] is module._redact_orderitementries_pii

    def test_supplier_row_transform_resolves_to_redact_function(self, module):
        model = next(m for m in module.MODELS if m["table"] == "suppliers")
        assert model["row_transform"] == "supplier_contacts"
        assert module._ROW_TRANSFORMS[model["row_transform"]] is module._redact_supplier_contacts

    def test_models_without_row_transform_have_none(self, module):
        no_transform_tables = {
            m["table"] for m in module.MODELS if m["table"] not in {"orderitementries", "suppliers"}
        }
        for m in module.MODELS:
            if m["table"] in no_transform_tables:
                assert m.get("row_transform") is None

    def test_prepare_all_jobs_carries_row_transform_string_key(self, module, set_variables):
        jobs = module.prepare_all_jobs()
        by_table = {j["job"]["table"]: j["job"] for j in jobs}
        assert by_table["orderitementries"]["row_transform"] == "orderitementries_pii"
        assert by_table["suppliers"]["row_transform"] == "supplier_contacts"
        assert by_table["orders"]["row_transform"] is None


# ===========================================================================
# prepare_all_jobs (watermark plumbing into jobs)
# ===========================================================================
class TestPrepareAllJobs:
    def test_default_watermark_when_none_set(self, module):
        jobs = module.prepare_all_jobs()
        assert len(jobs) == 26
        by_table = {j["job"]["table"]: j["job"] for j in jobs}
        assert by_table["orders"]["updated_since"] == "1970-01-01T00:00:00Z"

    def test_existing_watermark_variable_is_used(self, module, set_variables):
        set_variables(**{module._wm_key("orders"): "2024-05-01T00:00:00Z"})
        jobs = module.prepare_all_jobs()
        by_table = {j["job"]["table"]: j["job"] for j in jobs}
        assert by_table["orders"]["updated_since"] == "2024-05-01T00:00:00Z"
        # Unrelated model's watermark is untouched/default.
        assert by_table["shifts"]["updated_since"] == "1970-01-01T00:00:00Z"

    def test_anonymize_false_and_anonymize_fields_carried_through(self, module):
        jobs = module.prepare_all_jobs()
        by_table = {j["job"]["table"]: j["job"] for j in jobs}
        assert by_table["singleorderitems"]["anonymize"] is False
        assert by_table["patients2"]["anonymize_fields"] == [
            "name", "phone", "nokName", "nokPhone", "email",
        ]


# ===========================================================================
# _afya_login
# ===========================================================================
class TestAfyaLogin:
    def _register(self, register_connection, **overrides):
        kwargs = dict(host="https://afyapi.example.com/api", login="svc_user", password="svc_pass")
        kwargs.update(overrides)
        return register_connection("orthopedic_api_auth", **kwargs)

    def test_success_returns_token_and_base_url(self, module, register_connection):
        self._register(register_connection)
        with mock.patch.object(module.requests, "post", return_value=FakeResponse(200, json_data={"token": "abc123"})) as mock_post:
            token, base_url = module._afya_login()
        assert token == "abc123"
        assert base_url == "https://afyapi.example.com/api"
        mock_post.assert_called_once()
        _, kwargs = mock_post.call_args
        assert kwargs["json"] == {"username": "svc_user", "password": "svc_pass"}

    def test_missing_credentials_raises(self, module, register_connection):
        self._register(register_connection, login=None, password=None)
        with pytest.raises(RuntimeError, match="missing login/password"):
            module._afya_login()

    def test_401_raises_runtime_error(self, module, register_connection):
        self._register(register_connection)
        with mock.patch.object(module.requests, "post", return_value=FakeResponse(401, text="bad creds")):
            with pytest.raises(RuntimeError, match="401"):
                module._afya_login()

    def test_non_json_body_raises(self, module, register_connection):
        self._register(register_connection)
        with mock.patch.object(module.requests, "post", return_value=FakeResponse(200, json_data=None, text="<html/>")):
            with pytest.raises(RuntimeError, match="non-JSON"):
                module._afya_login()

    def test_missing_token_field_raises(self, module, register_connection):
        self._register(register_connection)
        with mock.patch.object(module.requests, "post", return_value=FakeResponse(200, json_data={"foo": "bar"})):
            with pytest.raises(RuntimeError, match="no 'token' field"):
                module._afya_login()


# ===========================================================================
# _gateway_request retry/backoff behavior
# ===========================================================================
class TestGatewayRequestRetries:
    def _call(self, module, **overrides):
        kwargs = dict(
            base_url="https://afyapi.example.com/api",
            token_ref={"token": "t0"},
            namespace=r"App\Models\Order",
            page=1,
            per_page=100,
            updated_since=None,
            connection_id=16,
            facility_id=47,
            anonymize=None,
            anonymize_fields=None,
            anonymize_skip=None,
            max_retries=6,
            default_wait=10,
            backoff=2,
        )
        kwargs.update(overrides)
        return module._gateway_request(**kwargs)

    def test_401_refreshes_token_and_retries_once(self, module):
        responses = [FakeResponse(401, text="unauthorized"), FakeResponse(200, json_data={"data": [{"id": 1}]})]
        with mock.patch.object(module.requests, "post", side_effect=responses) as mock_post, \
             mock.patch.object(module, "_afya_login", return_value=("newtoken", "https://afyapi.example.com/api")) as mock_login, \
             mock.patch.object(module.time, "sleep") as mock_sleep:
            result = self._call(module, max_retries=6)
        assert result == {"data": [{"id": 1}]}
        assert mock_post.call_count == 2
        assert mock_login.call_count == 1
        second_headers = mock_post.call_args_list[1].kwargs["headers"]
        assert second_headers["Authorization"] == "Bearer newtoken"
        mock_sleep.assert_not_called()  # 401 path doesn't sleep, just refreshes+retries

    def test_401_exhausts_max_retries_raises(self, module):
        responses = [FakeResponse(401)] * 3
        with mock.patch.object(module.requests, "post", side_effect=responses) as mock_post, \
             mock.patch.object(module, "_afya_login", return_value=("t", "u")) as mock_login:
            with pytest.raises(RuntimeError, match="401 Unauthorized"):
                self._call(module, max_retries=3)
        assert mock_post.call_count == 3
        assert mock_login.call_count == 2

    def test_429_respects_retry_after_seconds(self, module):
        responses = [FakeResponse(429, json_data={"retry_after_seconds": 7}), FakeResponse(200, json_data={"data": []})]
        with mock.patch.object(module.requests, "post", side_effect=responses) as mock_post, \
             mock.patch.object(module.time, "sleep") as mock_sleep:
            result = self._call(module, default_wait=10, max_retries=6)
        assert result == {"data": []}
        mock_sleep.assert_called_once_with(7)
        assert mock_post.call_count == 2

    def test_429_falls_back_to_default_wait_when_body_unparseable(self, module):
        responses = [FakeResponse(429, text="rate limited"), FakeResponse(200, json_data={"data": []})]
        with mock.patch.object(module.requests, "post", side_effect=responses), \
             mock.patch.object(module.time, "sleep") as mock_sleep:
            self._call(module, default_wait=15, max_retries=6)
        mock_sleep.assert_called_once_with(15)

    def test_429_exhausts_max_retries_raises(self, module):
        responses = [FakeResponse(429, json_data={"retry_after_seconds": 1})] * 3
        with mock.patch.object(module.requests, "post", side_effect=responses) as mock_post, \
             mock.patch.object(module.time, "sleep"):
            with pytest.raises(RuntimeError, match="429 rate-limited"):
                self._call(module, max_retries=3)
        assert mock_post.call_count == 3

    def test_5xx_backs_off_and_retries_up_to_max(self, module):
        responses = [FakeResponse(500), FakeResponse(502), FakeResponse(200, json_data={"data": []})]
        with mock.patch.object(module.requests, "post", side_effect=responses) as mock_post, \
             mock.patch.object(module.time, "sleep") as mock_sleep:
            result = self._call(module, default_wait=10, backoff=2, max_retries=5)
        assert result == {"data": []}
        assert mock_post.call_count == 3
        assert mock_sleep.call_args_list == [mock.call(10), mock.call(20)]

    def test_5xx_exhausts_max_retries_raises(self, module):
        responses = [FakeResponse(503)] * 3
        with mock.patch.object(module.requests, "post", side_effect=responses) as mock_post, \
             mock.patch.object(module.time, "sleep") as mock_sleep:
            with pytest.raises(RuntimeError, match="503 server error"):
                self._call(module, default_wait=10, backoff=2, max_retries=3)
        assert mock_post.call_count == 3
        assert mock_sleep.call_count == 2

    def test_404_raises_immediately_no_retry(self, module):
        with mock.patch.object(module.requests, "post", return_value=FakeResponse(404, text="not found")) as mock_post, \
             mock.patch.object(module.time, "sleep") as mock_sleep:
            with pytest.raises(RuntimeError, match="404 Not Found"):
                self._call(module, max_retries=6)
        assert mock_post.call_count == 1
        mock_sleep.assert_not_called()

    def test_422_raises_immediately_no_retry(self, module):
        with mock.patch.object(module.requests, "post", return_value=FakeResponse(422, text="bad request")) as mock_post, \
             mock.patch.object(module.time, "sleep") as mock_sleep:
            with pytest.raises(RuntimeError, match="422 Unprocessable"):
                self._call(module, max_retries=6)
        assert mock_post.call_count == 1
        mock_sleep.assert_not_called()

    def test_network_error_retries_then_succeeds(self, module):
        with mock.patch.object(module.requests, "post", side_effect=[Timeout("timed out"), FakeResponse(200, json_data={"data": []})]) as mock_post, \
             mock.patch.object(module.time, "sleep") as mock_sleep:
            result = self._call(module, default_wait=5, backoff=2, max_retries=6)
        assert result == {"data": []}
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(5)

    def test_network_error_exhausts_max_retries_raises(self, module):
        with mock.patch.object(module.requests, "post", side_effect=ConnectionError("boom")) as mock_post, \
             mock.patch.object(module.time, "sleep"):
            with pytest.raises(RuntimeError, match="Network error after 2 retries"):
                self._call(module, default_wait=1, backoff=2, max_retries=2)
        assert mock_post.call_count == 2

    def test_chunked_encoding_error_retried_like_network_error(self, module):
        with mock.patch.object(module.requests, "post", side_effect=[ChunkedEncodingError("chunked"), FakeResponse(200, json_data={"data": []})]), \
             mock.patch.object(module.time, "sleep") as mock_sleep:
            result = self._call(module, default_wait=2, max_retries=6)
        assert result == {"data": []}
        mock_sleep.assert_called_once_with(2)

    def test_mutually_exclusive_anonymize_options_raise_value_error(self, module):
        with pytest.raises(ValueError, match="mutually exclusive"):
            self._call(module, anonymize_fields=["name"], anonymize_skip=["id"])


# ===========================================================================
# fetch_all_pages pagination
# ===========================================================================
class TestFetchAllPages:
    def _call(self, module, fake_gateway, **overrides):
        kwargs = dict(
            base_url="https://afyapi.example.com/api",
            token_ref={"token": "t0"},
            namespace=r"App\Models\Order",
            per_page=100,
            updated_since=None,
            connection_id=16,
            facility_id=47,
            anonymize=None,
            anonymize_fields=None,
            anonymize_skip=None,
            page_workers=2,
        )
        kwargs.update(overrides)
        with mock.patch.object(module, "_gateway_request", side_effect=fake_gateway) as mock_gw:
            result = module.fetch_all_pages(**kwargs)
        return result, mock_gw

    def test_empty_first_page_returns_empty_and_stops(self, module):
        def fake_gateway(*args, **kwargs):
            return {"data": []}

        result, mock_gw = self._call(module, fake_gateway)
        assert result == []
        assert mock_gw.call_count == 1

    def test_multi_page_fan_out_with_known_last_page(self, module):
        def fake_gateway(base_url, token_ref, namespace, page, *rest, **kwargs):
            if page == 1:
                return {"data": [{"id": 1}], "pagination": {"last_page": 3}}
            return {"data": [{"id": page}]}

        result, mock_gw = self._call(module, fake_gateway)
        ids = sorted(r["id"] for r in result)
        assert ids == [1, 2, 3]
        assert mock_gw.call_count == 3

    def test_single_page_when_last_page_is_1(self, module):
        def fake_gateway(base_url, token_ref, namespace, page, *rest, **kwargs):
            return {"data": [{"id": 1}], "pagination": {"last_page": 1}}

        result, mock_gw = self._call(module, fake_gateway)
        assert result == [{"id": 1}]
        assert mock_gw.call_count == 1

    def test_sequential_exhaustion_when_no_last_page(self, module):
        def fake_gateway(base_url, token_ref, namespace, page, *rest, **kwargs):
            if page <= 3:
                return {"data": [{"id": page}], "pagination": {}}
            return {"data": []}

        result, mock_gw = self._call(module, fake_gateway)
        ids = sorted(r["id"] for r in result)
        assert ids == [1, 2, 3]
        assert mock_gw.call_count == 4  # pages 1,2,3, then empty page 4 stops it

    def test_sequential_exhaustion_stops_on_has_more_pages_false(self, module):
        def fake_gateway(base_url, token_ref, namespace, page, *rest, **kwargs):
            if page == 1:
                return {"data": [{"id": 1}], "pagination": {"hasMorePages": True}}
            return {"data": [{"id": 2}], "pagination": {"hasMorePages": False}}

        result, mock_gw = self._call(module, fake_gateway)
        ids = sorted(r["id"] for r in result)
        assert ids == [1, 2]
        assert mock_gw.call_count == 2


# ===========================================================================
# extract_one_model
# ===========================================================================
class TestExtractOneModel:
    def _job(self, **overrides):
        job = {
            "table": "orderitementries",
            "namespace": r"App\Models\OrderItemEntry",
            "anonymize": None,
            "anonymize_fields": None,
            "anonymize_skip": None,
            "row_transform": "orderitementries_pii",
            "updated_since": "1970-01-01T00:00:00Z",
        }
        job.update(overrides)
        return job

    def test_success_uploads_gzip_jsonl_with_redaction_applied(self, module):
        rows = [{
            "id": 1,
            "fields": [{"type": "textarea", "name": "notes", "value": ["secret clinical note"]}],
        }]
        s3_mock = mock.MagicMock()
        with mock.patch.object(module, "_afya_login", return_value=("tok", "https://afyapi.example.com/api")), \
             mock.patch.object(module, "fetch_all_pages", return_value=rows), \
             mock.patch.object(module, "S3Hook", return_value=s3_mock) as s3_cls:
            result = module.extract_one_model(self._job(), run_id="run123")

        assert result["table"] == "orderitementries"
        assert result["row_count"] == 1
        assert result["s3_key"] is not None
        assert "model=orderitementries" in result["s3_key"]
        assert result["s3_key"].endswith("run123.jsonl.gz")

        s3_cls.assert_called_once_with(aws_conn_id=module.S3_CONN_ID)
        load_call = s3_mock.load_bytes.call_args
        gz_bytes = load_call.kwargs["bytes_data"]
        assert load_call.kwargs["bucket_name"] == module.S3_BUCKET
        assert load_call.kwargs["key"] == result["s3_key"]

        decompressed = gzip.decompress(gz_bytes).decode("utf-8")
        lines = [json.loads(l) for l in decompressed.splitlines() if l]
        assert len(lines) == 1
        # PII redaction must actually have been applied before upload.
        assert lines[0]["fields"][0]["value"] == ["[REDACTED]"]

    def test_empty_rows_skips_upload(self, module):
        with mock.patch.object(module, "_afya_login", return_value=("tok", "https://afyapi.example.com/api")), \
             mock.patch.object(module, "fetch_all_pages", return_value=[]), \
             mock.patch.object(module, "S3Hook") as s3_cls:
            result = module.extract_one_model(self._job(), run_id="run123")

        assert result["s3_key"] is None
        assert result["row_count"] == 0
        s3_cls.assert_not_called()

    def test_no_row_transform_leaves_rows_unmodified(self, module):
        rows = [{"id": 1, "name": "Order #1"}]
        s3_mock = mock.MagicMock()
        with mock.patch.object(module, "_afya_login", return_value=("tok", "https://afyapi.example.com/api")), \
             mock.patch.object(module, "fetch_all_pages", return_value=rows), \
             mock.patch.object(module, "S3Hook", return_value=s3_mock):
            result = module.extract_one_model(
                self._job(table="orders", namespace=r"App\Models\Order", row_transform=None),
                run_id="run456",
            )
        gz_bytes = s3_mock.load_bytes.call_args.kwargs["bytes_data"]
        decompressed = gzip.decompress(gz_bytes).decode("utf-8")
        lines = [json.loads(l) for l in decompressed.splitlines() if l]
        assert lines == rows
        assert result["row_count"] == 1

    def test_supplier_contacts_transform_redacts_all_array_elements(self, module):
        rows = [{"id": 1, "emails": ["a@x.com", "b@x.com"], "phones": ["0700", "0711"]}]
        s3_mock = mock.MagicMock()
        with mock.patch.object(module, "_afya_login", return_value=("tok", "https://afyapi.example.com/api")), \
             mock.patch.object(module, "fetch_all_pages", return_value=rows), \
             mock.patch.object(module, "S3Hook", return_value=s3_mock):
            module.extract_one_model(
                self._job(table="suppliers", namespace=r"App\Models\Supplier", row_transform="supplier_contacts"),
                run_id="run789",
            )
        gz_bytes = s3_mock.load_bytes.call_args.kwargs["bytes_data"]
        decompressed = gzip.decompress(gz_bytes).decode("utf-8")
        line = json.loads(decompressed.splitlines()[0])
        assert line["emails"] == ["[REDACTED]"]
        assert line["phones"] == ["[REDACTED]"]


# ===========================================================================
# copy_into_orthopedic_raw
# ===========================================================================
class TestCopyIntoOrthopedicRaw:
    def test_builds_expected_copy_into_sql(self, module):
        sf_instance = mock.MagicMock()
        sf_cm = mock.MagicMock()
        sf_cm.__enter__.return_value = sf_instance
        sf_cm.__exit__.return_value = False
        with mock.patch.object(module, "SnowflakeClient", return_value=sf_cm) as sf_cls:
            module.copy_into_orthopedic_raw(
                table="orders",
                namespace=r"App\Models\Order",
                s3_key="raw/orthopedic/model=orders/dt=2026-09-22/run1.jsonl.gz",
                ingested_at="2026-09-22T00:00:00+00:00",
                run_id="run1",
            )
        sf_cls.assert_called_once_with(schema_=module.SF_RAW_SCHEMA)
        assert sf_instance.execute.call_count == 1
        sql = sf_instance.execute.call_args.args[0]
        assert "HOSPITALS.ORTHOPEDIC_RAW.ORDERS" in sql
        assert "FILES = ('raw/orthopedic/model=orders/dt=2026-09-22/run1.jsonl.gz')" in sql

    def test_no_s3_key_skips_copy(self, module):
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            module.copy_into_orthopedic_raw(
                table="orders", namespace=r"App\Models\Order",
                s3_key=None, ingested_at="2026-09-22T00:00:00+00:00", run_id="run1",
            )
        sf_cls.assert_not_called()

    def test_escapes_single_quotes_in_namespace_and_run_id(self, module):
        sf_instance = mock.MagicMock()
        sf_cm = mock.MagicMock()
        sf_cm.__enter__.return_value = sf_instance
        sf_cm.__exit__.return_value = False
        with mock.patch.object(module, "SnowflakeClient", return_value=sf_cm):
            module.copy_into_orthopedic_raw(
                table="orders", namespace="App\\Models\\O'rder",
                s3_key="k.jsonl.gz", ingested_at="2026-09-22T00:00:00+00:00",
                run_id="run'1",
            )
        sql = sf_instance.execute.call_args.args[0]
        assert "O\\'rder" in sql
        assert "run\\'1" in sql


# ===========================================================================
# ensure_orthopedic_raw_schema
# ===========================================================================
class TestEnsureSchema:
    def test_creates_schema_and_one_table_per_model(self, module):
        sf_instance = mock.MagicMock()
        sf_cm = mock.MagicMock()
        sf_cm.__enter__.return_value = sf_instance
        sf_cm.__exit__.return_value = False
        with mock.patch.object(module, "SnowflakeClient", return_value=sf_cm) as sf_cls:
            module.ensure_orthopedic_raw_schema()
        sf_cls.assert_called_once_with(schema_=module.SF_RAW_SCHEMA)
        # 1 CREATE SCHEMA + 26 CREATE TABLE statements.
        assert sf_instance.execute.call_count == 1 + len(module.MODELS)


# ===========================================================================
# Watermark behavior — verifying the CLAIMED "per-model gate" against the
# ACTUAL implementation. See final report: this is the highest-risk finding.
# ===========================================================================
class TestUpdateWatermarks:
    def test_all_configured_models_watermark_advances(self, module, set_variables):
        old = "2020-01-01T00:00:00Z"
        set_variables(**{
            module._wm_key("orders"): old,
            module._wm_key("orderitementries"): old,
        })
        module.update_watermarks()
        from tests.airflow_stub import Variable
        assert Variable.get(module._wm_key("orders")) != old
        assert Variable.get(module._wm_key("orderitementries")) != old

    def test_update_watermarks_is_NOT_actually_gated_per_model_on_copy_success(self, module, set_variables):
        """
        DESIGN-TRADEOFF CLAIM UNDER TEST:
        The task brief for this DAG states the watermark-advance behavior
        is "per-model": each model's watermark should advance based on
        *that model's own* copy success, not gated on sibling failures.

        ACTUAL BEHAVIOR (verified here): `update_watermarks()` takes no
        per-model success/failure signal at all — it has no access to any
        XCom result from `copy_into_orthopedic_raw`. It simply loops over
        the static `MODELS` registry and unconditionally calls
        `Variable.set(...)` for every single model, every time it runs
        (and it always runs, since its trigger_rule is ALL_DONE).

        This test simulates "model B's extraction failed" (an exception
        raised out of extract_one_model) and then calls
        update_watermarks() exactly as the DAG would (since ALL_DONE means
        it runs regardless of upstream mapped-task failures) — and shows
        BOTH model A's and model B's watermarks advance identically. There
        is no per-model gating in the code as written; watermarks advance
        unconditionally for the entire registry on every run where this
        task executes. See the final report for why this matters
        (a failed/partial extraction's data window is silently never
        retried on the next run).
        """
        old = "2020-01-01T00:00:00Z"
        set_variables(**{
            module._wm_key("orders"): old,       # "model A" - simulate success
            module._wm_key("shifts"): old,        # "model B" - simulate failure
        })

        # Simulate model B's extract task failing (as it would under
        # ALL_DONE, the DAG proceeds to update_watermarks anyway).
        with mock.patch.object(module, "fetch_all_pages", side_effect=RuntimeError("gateway 500")):
            with pytest.raises(RuntimeError):
                module.extract_one_model(
                    {"table": "shifts", "namespace": r"App\Models\Shift", "anonymize": None,
                     "anonymize_fields": None, "anonymize_skip": None, "row_transform": None,
                     "updated_since": old},
                    run_id="run1",
                )

        # update_watermarks has no knowledge of the failure above; it is
        # called unconditionally per the DAG's ALL_DONE trigger_rule.
        module.update_watermarks()

        from tests.airflow_stub import Variable
        orders_wm = Variable.get(module._wm_key("orders"))
        shifts_wm = Variable.get(module._wm_key("shifts"))
        assert orders_wm != old
        assert shifts_wm != old
        # Both watermarks advance to (effectively) the same instant,
        # regardless of which model actually succeeded.
        assert orders_wm == shifts_wm


# ===========================================================================
# Small helper coverage: _wm_key, _get_afya_connection_id/_facility_id, _safe
# ===========================================================================
class TestSmallHelpers:
    def test_wm_key_format(self, module):
        assert module._wm_key("orders") == "orthopedic__orthopedic_api_to_snowflake__orders"

    def test_afya_connection_id_default(self, module):
        assert module._get_afya_connection_id() == 16

    def test_afya_connection_id_override(self, module, set_variables):
        set_variables(ORTHOPEDIC_AFYA_CONNECTION_ID="99")
        assert module._get_afya_connection_id() == 99

    def test_afya_facility_id_default(self, module):
        assert module._get_afya_facility_id() == 47

    def test_afya_facility_id_override(self, module, set_variables):
        set_variables(ORTHOPEDIC_AFYA_FACILITY_ID="123")
        assert module._get_afya_facility_id() == 123

    def test_safe_strips_unsafe_characters(self, module):
        assert module._safe("run/with spaces!") == "run_with_spaces_"
