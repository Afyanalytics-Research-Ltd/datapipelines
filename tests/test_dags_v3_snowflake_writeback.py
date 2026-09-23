"""
Deep coverage for dags/v3_snowflake_writeback.py — Snowflake V3_READY.<service>
-> V3 gateway APIs writeback.

The single highest-value property this DAG must never regress is the
``_dirty`` clearing contract documented in the module docstring and in
``_apply_result``:

    ok               -> clear _dirty (confirmed success)
    failed_permanent -> clear _dirty (confirmed validation failure — retrying
                         without fixing the data would just fail again)
    failed_transient -> _dirty stays TRUE (next hourly run must retry)

and the flag may only ever be cleared *after* ``writeback_to_api`` has
actually POSTed the row and recorded a real API response — never
speculatively. ``TestApplyResultDirtyFlagSafety`` and
``TestClearDirtyFlagSequencing`` below are written to catch any regression
of that contract as directly as possible.

All external I/O (Snowflake, the V3 gateway HTTP calls, ``time.sleep``) is
mocked — no real network access or sleeping happens in this file.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
from requests.exceptions import ConnectionError as ReqConnectionError
from requests.exceptions import HTTPError, Timeout

from tests.airflow_stub import TriggerRule
from tests.helpers import load_dag_module

MODULE_NAME = "v3_snowflake_writeback"


@pytest.fixture
def module():
    return load_dag_module(MODULE_NAME)


def _resp(status_code=200, json_data=None, text="", content=b"1", json_raises=False):
    r = mock.Mock()
    r.status_code = status_code
    r.ok = 200 <= status_code < 300
    r.text = text
    r.content = content
    if json_raises:
        r.json.side_effect = ValueError("invalid json")
    else:
        r.json.return_value = {} if json_data is None else json_data
    return r


# ─────────────────────────────────────────────────────────────────────────
# DAG structural shape
# ─────────────────────────────────────────────────────────────────────────
class TestDagStructure:
    def test_dag_id_schedule_catchup_tags(self, module):
        dag = module.dag
        assert dag.dag_id == "v3_snowflake_writeback"
        assert dag.schedule == "@hourly"
        assert dag.catchup is False
        assert dag.tags == ["v3", "writeback", "snowflake", "api"]
        assert dag.max_active_tasks == 8

    def test_default_args_retries(self, module):
        dag = module.dag
        assert dag.default_args.get("retries") == 3
        assert dag.default_args.get("retry_delay") == timedelta(minutes=2)

    def test_task_ids(self, module):
        dag = module.dag
        assert set(dag.task_ids) == {
            "ensure_writeback_schema", "find_dirty_rows", "writeback_to_api",
            "clear_dirty_flag", "record_writeback_run",
        }

    def test_dependency_chain(self, module):
        t = module.dag.task_dict
        assert t["ensure_writeback_schema"].upstream_task_ids == set()
        assert t["ensure_writeback_schema"].downstream_task_ids == {"find_dirty_rows"}
        assert t["find_dirty_rows"].upstream_task_ids == {"ensure_writeback_schema"}
        assert t["find_dirty_rows"].downstream_task_ids == {"writeback_to_api"}
        assert t["writeback_to_api"].upstream_task_ids == {"find_dirty_rows"}
        assert t["writeback_to_api"].downstream_task_ids == {"clear_dirty_flag"}
        assert t["clear_dirty_flag"].upstream_task_ids == {"writeback_to_api"}
        assert t["clear_dirty_flag"].downstream_task_ids == {"record_writeback_run"}
        assert t["record_writeback_run"].upstream_task_ids == {"clear_dirty_flag"}
        assert t["record_writeback_run"].downstream_task_ids == set()

    def test_writeback_to_api_is_mapped_over_find_dirty_rows_output(self, module):
        t = module.dag.task_dict
        t_writeback = t["writeback_to_api"]
        assert t_writeback.is_mapped is True
        assert t_writeback.mapped_kwargs is not None
        assert t_writeback.mapped_kwargs["op_kwargs"].task is t["find_dirty_rows"]

    def test_clear_dirty_flag_is_mapped_over_writeback_to_api_output(self, module):
        t = module.dag.task_dict
        t_clear = t["clear_dirty_flag"]
        assert t_clear.is_mapped is True
        assert t_clear.mapped_kwargs is not None
        assert t_clear.mapped_kwargs["op_kwargs"].task is t["writeback_to_api"]

    def test_record_writeback_run_is_not_mapped(self, module):
        t_record = module.dag.task_dict["record_writeback_run"]
        assert t_record.is_mapped is False

    def test_trigger_rules(self, module):
        t = module.dag.task_dict
        assert t["ensure_writeback_schema"].trigger_rule == TriggerRule.ALL_SUCCESS
        assert t["find_dirty_rows"].trigger_rule == TriggerRule.ALL_SUCCESS
        assert t["writeback_to_api"].trigger_rule == TriggerRule.ALL_DONE
        assert t["clear_dirty_flag"].trigger_rule == TriggerRule.ALL_DONE
        assert t["record_writeback_run"].trigger_rule == TriggerRule.ALL_DONE


# ─────────────────────────────────────────────────────────────────────────
# _active_services
# ─────────────────────────────────────────────────────────────────────────
class TestActiveServices:
    def test_default_is_all_services(self, module, set_variables):
        set_variables(V3_WRITEBACK_SERVICES="")
        assert module._active_services() == list(module.V3_SERVICES.keys())

    def test_restricted_to_override(self, module, set_variables):
        set_variables(V3_WRITEBACK_SERVICES="core, finance")
        assert module._active_services() == ["core", "finance"]

    def test_unknown_service_raises(self, module, set_variables):
        set_variables(V3_WRITEBACK_SERVICES="core,bogus")
        with pytest.raises(ValueError, match="Unknown V3_WRITEBACK_SERVICES"):
            module._active_services()


# ─────────────────────────────────────────────────────────────────────────
# _apply_result — the _dirty clearing safety contract (HIGHEST PRIORITY)
# ─────────────────────────────────────────────────────────────────────────
class TestApplyResultDirtyFlagSafety:
    """_apply_result is the *only* place `_dirty` is ever written back to
    Snowflake. These tests assert, by inspecting the literal SQL text sent
    to the (mocked) SnowflakeClient, exactly what happens to `_dirty` for
    each of the three possible row outcomes. `test_failed_transient_*` is
    the most important test in this file: a regression here would mean a
    row that failed for a transient reason (network blip, 5xx, exhausted
    429 backoff) silently stops being retried."""

    def _sql(self, module, **kwargs):
        sf = mock.Mock()
        defaults = dict(
            sf=sf, service="core", model="patient", record_id="rec-1",
            status="ok", new_id=None, error=None, run_id="run-abc",
        )
        defaults.update(kwargs)
        module._apply_result(**defaults)
        assert sf.execute.call_count == 1
        return sf.execute.call_args.args[0]

    # ---- status == "ok" --------------------------------------------------
    def test_ok_clears_dirty(self, module):
        sql = self._sql(module, status="ok", new_id="new-42", error=None)
        assert re.search(r"_dirty\s*=\s*FALSE", sql)
        assert not re.search(r"_dirty\s*=\s*TRUE", sql)

    def test_ok_stamps_success_metadata(self, module):
        sql = self._sql(module, status="ok", new_id="new-42", error=None, run_id="run-abc")
        assert re.search(r"_writeback_at\s*=\s*CURRENT_TIMESTAMP\(\)", sql)
        assert re.search(r"_writeback_run_id\s*=\s*'run-abc'", sql)
        assert re.search(r"_writeback_error\s*=\s*NULL", sql)
        assert re.search(r"_writeback_action\s*=\s*'update'", sql)

    def test_ok_adopts_new_record_id(self, module):
        sql = self._sql(module, status="ok", new_id="server-generated-99", record_id="placeholder-1")
        assert re.search(r"_record_id\s*=\s*'server-generated-99'", sql)
        # the WHERE clause must still target the *old* record_id — that's
        # the row we actually POSTed, before the server assigned a new id.
        assert "WHERE _model = 'patient' AND _record_id = 'placeholder-1'" in sql

    # ---- status == "failed_permanent" ------------------------------------
    def test_failed_permanent_also_clears_dirty(self, module):
        """A confirmed validation failure (400/404/409/422/500) will never
        succeed on blind retry, so it's safe — and correct — to clear
        _dirty here too; the docstring says re-flag after fixing the data."""
        sql = self._sql(module, status="failed_permanent", new_id=None,
                         error="422 unprocessable")
        assert re.search(r"_dirty\s*=\s*FALSE", sql)
        assert not re.search(r"_dirty\s*=\s*TRUE", sql)

    def test_failed_permanent_keeps_error_message(self, module):
        sql = self._sql(module, status="failed_permanent", error="422 unprocessable",
                         run_id="run-xyz")
        assert re.search(r"_writeback_error\s*=\s*'422 unprocessable'", sql)
        assert re.search(r"_writeback_run_id\s*=\s*'run-xyz'", sql)

    def test_failed_permanent_does_not_touch_writeback_at(self, module):
        """Documented behavior, not a bug: _writeback_at means "set on
        successful writeback" per the module docstring, so the failure
        branch's SQL never assigns it."""
        sql = self._sql(module, status="failed_permanent", error="boom")
        assert "_writeback_at" not in sql

    # ---- status == "failed_transient" — MUST NEVER CLEAR DIRTY -----------
    def test_failed_transient_leaves_dirty_true(self, module):
        sql = self._sql(module, status="failed_transient", new_id=None,
                         error="502 — retries exhausted")
        assert re.search(r"_dirty\s*=\s*TRUE", sql)
        assert not re.search(r"_dirty\s*=\s*FALSE", sql)

    def test_failed_transient_still_records_error_and_run_id(self, module):
        sql = self._sql(module, status="failed_transient",
                         error="network error: Connection refused", run_id="run-777")
        assert re.search(r"_writeback_error\s*=\s*'network error: Connection refused'", sql)
        assert re.search(r"_writeback_run_id\s*=\s*'run-777'", sql)

    def test_failed_transient_does_not_touch_writeback_at(self, module):
        sql = self._sql(module, status="failed_transient", error="timeout")
        assert "_writeback_at" not in sql

    @pytest.mark.parametrize("record_id,error", [
        ("rec-1", "timeout"),
        ("rec-2", "429 — retries exhausted"),
        ("O'Brien-rec", "network error: peer reset O'Brien's connection"),
        ("rec-4", None),
    ])
    def test_failed_transient_never_clears_dirty_across_many_inputs(self, module, record_id, error):
        """Regression guard: sweep record_id/error combinations (including
        SQL-quote-shaped ones) and assert _dirty is set TRUE — never FALSE
        — in every single one. This is the row-safety property the whole
        DAG exists to protect."""
        sql = self._sql(module, record_id=record_id, status="failed_transient", error=error)
        assert re.search(r"_dirty\s*=\s*TRUE", sql)
        assert not re.search(r"_dirty\s*=\s*FALSE", sql)

    def test_unknown_status_defaults_to_keeping_dirty_true(self, module):
        """Fail-safe: the branch is `"FALSE" if status == "failed_permanent"
        else "TRUE"` — any status string other than "ok"/"failed_permanent"
        (including a typo/unexpected value) defaults to leaving _dirty TRUE
        rather than clearing it, which is the safe direction to fail in."""
        sql = self._sql(module, status="some_unexpected_status", error="?")
        assert re.search(r"_dirty\s*=\s*TRUE", sql)

    def test_where_clause_always_targets_model_and_record_id(self, module):
        for status in ("ok", "failed_permanent", "failed_transient"):
            sql = self._sql(module, status=status, record_id="rec-9", model="invoice",
                             new_id="rec-9" if status == "ok" else None)
            assert "WHERE _model = 'invoice' AND _record_id = 'rec-9'" in sql

    def test_error_message_and_record_id_are_sql_escaped(self, module):
        sql = self._sql(module, status="failed_transient", record_id="rec-1",
                         error="bad input: O'Reilly's \"quote\"")
        assert "O''Reilly''s" in sql


# ─────────────────────────────────────────────────────────────────────────
# _write_row — HTTP status classification / retry & backoff behavior
# ─────────────────────────────────────────────────────────────────────────
class TestWriteRowStatusClassification:
    """Every test here patches module._session()/_get_token and
    module.time.sleep so nothing actually waits or hits the network."""

    def _call(self, module, session, **kwargs):
        defaults = dict(
            service="core", model="patient", action="update", record_id="rec-1",
            payload={"name": "Bob"}, gw_meta={}, facility_id=6, organization_id=1,
            update_action="update",
        )
        defaults.update(kwargs)
        with mock.patch.object(module, "_session", return_value=session), \
             mock.patch.object(module, "_get_token", return_value="TESTTOKEN"), \
             mock.patch.object(module, "_invalidate_token"), \
             mock.patch("time.sleep") as sleep_mock:
            result = module._write_row(**defaults)
        return result, sleep_mock

    def test_200_success(self, module):
        session = mock.Mock()
        session.post.return_value = _resp(200, {"id": "99"})
        (status, new_id, err), sleep_mock = self._call(module, session)
        assert (status, new_id, err) == ("ok", "99", None)
        assert session.post.call_count == 1
        sleep_mock.assert_not_called()

    def test_201_success_nested_data_id(self, module):
        session = mock.Mock()
        session.post.return_value = _resp(201, {"data": {"id": "77"}})
        (status, new_id, err), _ = self._call(module, session)
        assert (status, new_id) == ("ok", "77")

    def test_success_falls_back_to_record_id_when_no_id_in_response(self, module):
        session = mock.Mock()
        session.post.return_value = _resp(200, {})
        (status, new_id, err), _ = self._call(module, session, record_id="rec-1")
        assert (status, new_id) == ("ok", "rec-1")

    def test_401_exhausts_retries(self, module):
        session = mock.Mock()
        session.post.return_value = _resp(401)
        (status, new_id, err), sleep_mock = self._call(module, session)
        assert status == "failed_transient"
        assert "401" in err
        assert session.post.call_count == 5
        sleep_mock.assert_not_called()  # 401 refreshes token, no sleep

    def test_401_then_success_refreshes_token_and_retries(self, module):
        session = mock.Mock()
        session.post.side_effect = [_resp(401), _resp(200, {"id": "7"})]
        (status, new_id, err), _ = self._call(module, session)
        assert (status, new_id) == ("ok", "7")
        assert session.post.call_count == 2

    def test_429_exhausts_retries_with_retry_after(self, module):
        session = mock.Mock()
        session.post.return_value = _resp(429, {"retry_after_seconds": 3})
        (status, new_id, err), sleep_mock = self._call(module, session)
        assert status == "failed_transient"
        assert "429" in err
        assert session.post.call_count == 5
        assert sleep_mock.call_args_list == [mock.call(3)] * 4

    def test_429_then_success(self, module):
        session = mock.Mock()
        session.post.side_effect = [_resp(429, {"retry_after_seconds": 2}), _resp(200, {"id": "5"})]
        (status, new_id, err), sleep_mock = self._call(module, session)
        assert (status, new_id) == ("ok", "5")
        assert sleep_mock.call_args_list == [mock.call(2)]

    def test_429_missing_retry_after_uses_init_wait_default(self, module):
        session = mock.Mock()
        session.post.side_effect = [_resp(429, json_raises=True), _resp(200, {"id": "5"})]
        (status, new_id, err), sleep_mock = self._call(module, session)
        assert status == "ok"
        assert sleep_mock.call_args_list == [mock.call(5)]  # init_wait default

    @pytest.mark.parametrize("code", [400, 404, 409, 422, 500])
    def test_permanent_failure_codes_no_retry(self, module, code):
        session = mock.Mock()
        session.post.return_value = _resp(code, {"error": f"bad request {code}"})
        (status, new_id, err), sleep_mock = self._call(module, session)
        assert status == "failed_permanent"
        assert new_id is None
        assert str(code) not in err or f"bad request {code}" in err  # error body captured
        assert session.post.call_count == 1  # no retry loop at all
        sleep_mock.assert_not_called()

    @pytest.mark.parametrize("code", [400, 404, 409, 422, 500])
    def test_permanent_failure_captures_raw_text_when_body_not_json(self, module, code):
        session = mock.Mock()
        session.post.return_value = _resp(code, json_raises=True, text="not json at all")
        (status, new_id, err), _ = self._call(module, session)
        assert status == "failed_permanent"
        assert "not json at all" in err

    @pytest.mark.parametrize("code", [502, 503, 504])
    def test_5xx_transient_codes_exhaust_retries_with_exponential_backoff(self, module, code):
        session = mock.Mock()
        session.post.return_value = _resp(code)
        (status, new_id, err), sleep_mock = self._call(module, session)
        assert status == "failed_transient"
        assert str(code) in err
        assert session.post.call_count == 5
        assert sleep_mock.call_args_list == [mock.call(5), mock.call(10), mock.call(20), mock.call(40)]

    @pytest.mark.parametrize("code", [502, 503, 504])
    def test_5xx_transient_codes_succeed_on_retry(self, module, code):
        session = mock.Mock()
        session.post.side_effect = [_resp(code), _resp(200, {"id": "1"})]
        (status, new_id, err), sleep_mock = self._call(module, session)
        assert (status, new_id) == ("ok", "1")
        assert sleep_mock.call_args_list == [mock.call(5)]

    @pytest.mark.parametrize("exc_cls", [ReqConnectionError, Timeout])
    def test_network_errors_exhaust_retries_then_failed_transient(self, module, exc_cls):
        session = mock.Mock()
        session.post.side_effect = exc_cls("boom")
        (status, new_id, err), sleep_mock = self._call(module, session)
        assert status == "failed_transient"
        assert "network error" in err
        assert session.post.call_count == 5
        assert sleep_mock.call_args_list == [mock.call(5), mock.call(10), mock.call(20), mock.call(40)]

    @pytest.mark.parametrize("exc_cls", [ReqConnectionError, Timeout])
    def test_network_error_then_success(self, module, exc_cls):
        session = mock.Mock()
        session.post.side_effect = [exc_cls("boom"), _resp(200, {"id": "3"})]
        (status, new_id, err), sleep_mock = self._call(module, session)
        assert (status, new_id) == ("ok", "3")
        assert sleep_mock.call_args_list == [mock.call(5)]

    def test_http_error_raised_directly_is_failed_permanent_single_attempt(self, module):
        session = mock.Mock()
        session.post.side_effect = HTTPError("weird transport-level http error")
        (status, new_id, err), sleep_mock = self._call(module, session)
        assert status == "failed_permanent"
        assert session.post.call_count == 1
        sleep_mock.assert_not_called()

    def test_other_non_ok_status_is_failed_permanent(self, module):
        """Any status code not explicitly classified (e.g. 418) falls
        through the `if not r.ok` catch-all as failed_permanent."""
        session = mock.Mock()
        session.post.return_value = _resp(418, text="I'm a teapot")
        (status, new_id, err), _ = self._call(module, session)
        assert status == "failed_permanent"
        assert "418" in err


# ─────────────────────────────────────────────────────────────────────────
# Gateway payload shape
# ─────────────────────────────────────────────────────────────────────────
class TestGatewayPayload:
    def test_update_action_body_and_headers(self, module):
        session = mock.Mock()
        session.post.return_value = _resp(200, {"id": "rec-1"})
        with mock.patch.object(module, "_session", return_value=session), \
             mock.patch.object(module, "_get_token", return_value="TESTTOKEN"), \
             mock.patch("time.sleep"):
            module._write_row(
                service="core", model="patient", action="update", record_id="rec-1",
                payload={"name": "Bob"}, gw_meta={"facility": True}, facility_id=6,
                organization_id=1, update_action="update",
            )
        _, kwargs = session.post.call_args
        body = kwargs["json"]
        assert body["action"] == "update"
        assert body["model"] == "patient"
        assert body["id"] == "rec-1"
        assert body["data"]["id"] == "rec-1"
        assert body["data"]["name"] == "Bob"
        assert body["destination_tenant_id"] == 1

        headers = kwargs["headers"]
        assert headers["Authorization"] == "Bearer TESTTOKEN"
        assert headers["X-Tenant-Id"] == "1"
        assert headers["X-Facility-Id"] == "6"

    def test_insert_action_strips_id_from_data(self, module):
        session = mock.Mock()
        session.post.return_value = _resp(200, {"id": "new-1"})
        with mock.patch.object(module, "_session", return_value=session), \
             mock.patch.object(module, "_get_token", return_value="TESTTOKEN"), \
             mock.patch("time.sleep"):
            module._write_row(
                service="core", model="patient", action="insert", record_id="placeholder",
                payload={"id": "placeholder", "name": "Bob"}, gw_meta={}, facility_id=6,
                organization_id=1, update_action="update",
            )
        body = session.post.call_args.kwargs["json"]
        assert body["action"] == "insert"
        assert "id" not in body["data"]
        assert "id" not in body  # top-level "id" only added for update

    def test_no_organization_id_omits_tenant_fields(self, module):
        session = mock.Mock()
        session.post.return_value = _resp(200, {"id": "rec-1"})
        with mock.patch.object(module, "_session", return_value=session), \
             mock.patch.object(module, "_get_token", return_value="TESTTOKEN"), \
             mock.patch("time.sleep"):
            module._write_row(
                service="core", model="patient", action="update", record_id="rec-1",
                payload={"name": "Bob"}, gw_meta={"facility": True}, facility_id=6,
                organization_id=None, update_action="update",
            )
        body = session.post.call_args.kwargs["json"]
        headers = session.post.call_args.kwargs["headers"]
        assert "destination_tenant_id" not in body
        assert "X-Tenant-Id" not in headers

    def test_no_facility_meta_omits_facility_header(self, module):
        session = mock.Mock()
        session.post.return_value = _resp(200, {"id": "rec-1"})
        with mock.patch.object(module, "_session", return_value=session), \
             mock.patch.object(module, "_get_token", return_value="TESTTOKEN"), \
             mock.patch("time.sleep"):
            module._write_row(
                service="core", model="patient", action="update", record_id="rec-1",
                payload={"name": "Bob"}, gw_meta={}, facility_id=6,
                organization_id=1, update_action="update",
            )
        headers = session.post.call_args.kwargs["headers"]
        assert "X-Facility-Id" not in headers

    def test_end_to_end_payload_uses_variables_via_writeback_to_api(
        self, module, set_variables,
    ):
        """Full task-callable path: Variables -> writeback_to_api ->
        _write_row -> actual outgoing POST body/headers."""
        set_variables(
            V3_WRITEBACK_SERVICES="core", V3_WRITEBACK_MODELS="patient",
            V3_WRITEBACK_FACILITY_ID="6", V3_WRITEBACK_ORGANIZATION_ID="1",
            V3_WRITEBACK_BATCH_SIZE="200", V3_WRITEBACK_UPDATE_ACTION="update",
        )
        rows = [{"_RECORD_ID": "rec-1", "_WRITEBACK_ACTION": "update",
                 "PAYLOAD": json.dumps({"name": "Bob"})}]
        session = mock.Mock()
        session.post.return_value = _resp(200, {"id": "rec-1"})

        with mock.patch.object(module, "SnowflakeClient") as sf_cls, \
             mock.patch.object(module, "discover_gateway_meta",
                                return_value={"patient": {"facility": True}}), \
             mock.patch.object(module, "_session", return_value=session), \
             mock.patch.object(module, "_get_token", return_value="TESTTOKEN"), \
             mock.patch("time.sleep"):
            sf_cls.return_value.__enter__.return_value.query.return_value = rows
            result = module.writeback_to_api({"service": "core", "model": "patient"}, run_id="run1")

        assert result["results"][0]["status"] == "ok"
        body = session.post.call_args.kwargs["json"]
        headers = session.post.call_args.kwargs["headers"]
        assert body["action"] == "update"
        assert body["model"] == "patient"
        assert body["data"]["name"] == "Bob"
        assert body["destination_tenant_id"] == 1
        assert headers["X-Tenant-Id"] == "1"
        assert headers["X-Facility-Id"] == "6"


# ─────────────────────────────────────────────────────────────────────────
# clear_dirty_flag sequencing — never clear _dirty without an attempted call
# ─────────────────────────────────────────────────────────────────────────
class TestClearDirtyFlagSequencing:
    def test_clear_dirty_flag_never_calls_write_row_or_http_itself(self, module):
        """clear_dirty_flag only ever consumes the `results` list handed to
        it by writeback_to_api (via job_result) — it must never itself
        reach out to _write_row / the V3 gateway / _session. Proven here by
        making those calls raise if touched."""
        job_result = {
            "service": "core", "model": "patient",
            "results": [{
                "run_id": "run1", "service": "core", "model": "patient",
                "record_id": "rec-1", "action": "update",
                "status": "ok", "new_id": "rec-1", "error": None,
            }],
        }
        with mock.patch.object(module, "SnowflakeClient") as sf_cls, \
             mock.patch.object(module, "_write_row",
                                side_effect=AssertionError("clear_dirty_flag must not call _write_row")), \
             mock.patch.object(module, "_session",
                                side_effect=AssertionError("clear_dirty_flag must not open an HTTP session")):
            sf = sf_cls.return_value.__enter__.return_value
            summary = module.clear_dirty_flag(job_result=job_result, run_id="run1")

        assert summary == {"service": "core", "model": "patient", "total": 1, "succeeded": 1, "failed": 0}
        # one _apply_result UPDATE + one audit INSERT
        assert sf.execute.call_count == 2

    def test_apply_result_call_count_matches_attempted_rows_exactly(self, module):
        """Every row in `results` gets exactly one _apply_result UPDATE —
        no more, no fewer — so _dirty is only ever touched for rows that
        writeback_to_api actually attempted."""
        results = [
            {"run_id": "r", "service": "core", "model": "patient", "record_id": f"rec-{i}",
             "action": "update", "status": s, "new_id": (f"rec-{i}" if s == "ok" else None),
             "error": (None if s == "ok" else "err")}
            for i, s in enumerate(["ok", "failed_permanent", "failed_transient", "ok"])
        ]
        job_result = {"service": "core", "model": "patient", "results": results}
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf = sf_cls.return_value.__enter__.return_value
            summary = module.clear_dirty_flag(job_result=job_result, run_id="r")

        # 4 _apply_result UPDATEs + 1 audit INSERT = 5
        assert sf.execute.call_count == 5
        assert summary == {"service": "core", "model": "patient", "total": 4, "succeeded": 2, "failed": 2}

    def test_no_rows_attempted_means_no_apply_result_calls(self, module):
        job_result = {"service": "core", "model": "patient", "results": []}
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf = sf_cls.return_value.__enter__.return_value
            summary = module.clear_dirty_flag(job_result=job_result, run_id="r")
        assert sf.execute.call_count == 0  # no UPDATEs, no audit insert either
        assert summary == {"service": "core", "model": "patient", "total": 0, "succeeded": 0, "failed": 0}

    def test_end_to_end_row_attempted_before_dirty_cleared_or_kept(self, module):
        """The strongest version of the sequencing guarantee: run the real
        writeback_to_api (which calls the real _write_row against a mocked
        HTTP session) to produce job_result, then feed that exact
        job_result into the real clear_dirty_flag, and assert the SQL for
        each row matches the status that _write_row actually produced for
        it — proving there is no path from "row never POSTed" to "_dirty
        cleared"."""
        rows = [
            {"_RECORD_ID": "rec-ok", "_WRITEBACK_ACTION": "update", "PAYLOAD": json.dumps({"a": 1})},
            {"_RECORD_ID": "rec-fail", "_WRITEBACK_ACTION": "update", "PAYLOAD": json.dumps({"a": 2})},
        ]
        session = mock.Mock()
        session.post.side_effect = [
            _resp(200, {"id": "rec-ok"}),       # first row succeeds
            _resp(503), _resp(503), _resp(503), _resp(503), _resp(503),  # second row exhausts retries
        ]

        with mock.patch.object(module, "SnowflakeClient") as sf_cls, \
             mock.patch.object(module, "discover_gateway_meta", return_value={}), \
             mock.patch.object(module, "_session", return_value=session), \
             mock.patch.object(module, "_get_token", return_value="TESTTOKEN"), \
             mock.patch("time.sleep"):
            sf_cls.return_value.__enter__.return_value.query.return_value = rows
            job_result = module.writeback_to_api({"service": "core", "model": "patient"}, run_id="run1")

        assert session.post.call_count == 6  # 1 (ok) + 5 (exhausted retries)
        statuses = {r["record_id"]: r["status"] for r in job_result["results"]}
        assert statuses == {"rec-ok": "ok", "rec-fail": "failed_transient"}

        with mock.patch.object(module, "SnowflakeClient") as sf_cls2:
            sf2 = sf_cls2.return_value.__enter__.return_value
            summary = module.clear_dirty_flag(job_result=job_result, run_id="run1")

        assert summary == {"service": "core", "model": "patient", "total": 2, "succeeded": 1, "failed": 1}
        apply_sqls = [c.args[0] for c in sf2.execute.call_args_list[:2]]
        ok_sql = next(s for s in apply_sqls if "rec-ok" in s)
        fail_sql = next(s for s in apply_sqls if "rec-fail" in s)
        assert re.search(r"_dirty\s*=\s*FALSE", ok_sql)
        assert re.search(r"_dirty\s*=\s*TRUE", fail_sql)
        assert not re.search(r"_dirty\s*=\s*FALSE", fail_sql)


# ─────────────────────────────────────────────────────────────────────────
# find_dirty_rows
# ─────────────────────────────────────────────────────────────────────────
class TestFindDirtyRows:
    def _fake_query(self, results_by_service):
        def query(sql, label=None):
            for svc, rows in results_by_service.items():
                if f"V3_READY.{svc.upper()}" in sql:
                    return rows
            return []
        return query

    def test_produces_expected_service_model_jobs(self, module, set_variables):
        set_variables(V3_WRITEBACK_SERVICES="core,finance")
        fake_data = {
            "core": [{"_MODEL": "patient", "CNT": 5}, {"_MODEL": "invoice", "CNT": 2}],
            "finance": [{"_MODEL": "ledger", "CNT": 3}],
        }
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_cls.return_value.__enter__.return_value.query.side_effect = self._fake_query(fake_data)
            jobs = module.find_dirty_rows()

        assert jobs == [
            {"service": "core", "model": "patient", "count": 5},
            {"service": "core", "model": "invoice", "count": 2},
            {"service": "finance", "model": "ledger", "count": 3},
        ]

    def test_model_with_zero_dirty_rows_is_absent_from_results(self, module, set_variables):
        """Because the SQL is `WHERE _dirty = TRUE GROUP BY _model`, a model
        with no dirty rows never appears in Snowflake's result set at all
        (simulated here by simply not including it in the fake query
        response) — and find_dirty_rows must not invent a job for it."""
        set_variables(V3_WRITEBACK_SERVICES="core")
        fake_data = {"core": [{"_MODEL": "patient", "CNT": 5}]}  # "invoice" has 0 dirty rows
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_cls.return_value.__enter__.return_value.query.side_effect = self._fake_query(fake_data)
            jobs = module.find_dirty_rows()
        assert [j["model"] for j in jobs] == ["patient"]
        assert "invoice" not in [j["model"] for j in jobs]

    def test_only_models_filter_restricts_jobs(self, module, set_variables):
        set_variables(V3_WRITEBACK_SERVICES="core,finance", V3_WRITEBACK_MODELS="patient,ledger")
        fake_data = {
            "core": [{"_MODEL": "patient", "CNT": 5}, {"_MODEL": "invoice", "CNT": 2}],
            "finance": [{"_MODEL": "ledger", "CNT": 3}],
        }
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_cls.return_value.__enter__.return_value.query.side_effect = self._fake_query(fake_data)
            jobs = module.find_dirty_rows()
        assert {j["model"] for j in jobs} == {"patient", "ledger"}

    def test_no_dirty_rows_anywhere_returns_empty_list(self, module, set_variables):
        set_variables(V3_WRITEBACK_SERVICES="core,finance")
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_cls.return_value.__enter__.return_value.query.return_value = []
            jobs = module.find_dirty_rows()
        assert jobs == []

    def test_service_query_exception_is_skipped_not_fatal(self, module, set_variables):
        set_variables(V3_WRITEBACK_SERVICES="core,finance")

        def query(sql, label=None):
            if "V3_READY.CORE" in sql:
                raise RuntimeError("table does not exist")
            if "V3_READY.FINANCE" in sql:
                return [{"_MODEL": "ledger", "CNT": 1}]
            return []

        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf_cls.return_value.__enter__.return_value.query.side_effect = query
            jobs = module.find_dirty_rows()
        assert jobs == [{"service": "finance", "model": "ledger", "count": 1}]


# ─────────────────────────────────────────────────────────────────────────
# record_writeback_run
# ─────────────────────────────────────────────────────────────────────────
class TestRecordWritebackRun:
    def _ctx(self, **overrides):
        ctx = dict(run_id="run1", data_interval_start=datetime(2026, 9, 22, tzinfo=timezone.utc),
                   logical_date=None)
        ctx.update(overrides)
        return ctx

    def test_all_succeeded_status_success_no_raise(self, module):
        summaries = [
            {"service": "core", "model": "patient", "total": 5, "succeeded": 5, "failed": 0},
            {"service": "core", "model": "invoice", "total": 2, "succeeded": 2, "failed": 0},
        ]
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf = sf_cls.return_value.__enter__.return_value
            module.record_writeback_run(summaries=summaries, **self._ctx())
        sql = sf.execute.call_args.args[0]
        assert "'success'" in sql
        assert ", 7, 7, 0, 7" in sql  # total_jobs, succeeded, failed, total_rows

    def test_partial_failure_raises_runtime_error(self, module):
        summaries = [
            {"service": "core", "model": "patient", "total": 5, "succeeded": 3, "failed": 2},
        ]
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf = sf_cls.return_value.__enter__.return_value
            with pytest.raises(RuntimeError, match=r"2 row\(s\) failed writeback"):
                module.record_writeback_run(summaries=summaries, **self._ctx())
        # the audit row must still have been written before raising
        sql = sf.execute.call_args.args[0]
        assert "'partial'" in sql

    def test_all_failed_status_failed_and_raises(self, module):
        summaries = [
            {"service": "core", "model": "patient", "total": 5, "succeeded": 0, "failed": 5},
        ]
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf = sf_cls.return_value.__enter__.return_value
            with pytest.raises(RuntimeError):
                module.record_writeback_run(summaries=summaries, **self._ctx())
        sql = sf.execute.call_args.args[0]
        assert "'failed'" in sql

    def test_no_failures_does_not_raise(self, module):
        summaries = [{"service": "core", "model": "patient", "total": 3, "succeeded": 3, "failed": 0}]
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            module.record_writeback_run(summaries=summaries, **self._ctx())  # should not raise

    def test_empty_summaries_list_no_crash_no_snowflake_call(self, module):
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            module.record_writeback_run(summaries=[], **self._ctx())  # should not raise
        sf_cls.assert_not_called()

    def test_none_summaries_treated_as_empty(self, module):
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            module.record_writeback_run(summaries=None, **self._ctx())  # should not raise
        sf_cls.assert_not_called()

    def test_falls_back_to_logical_date_when_no_data_interval_start(self, module):
        summaries = [{"service": "core", "model": "patient", "total": 1, "succeeded": 1, "failed": 0}]
        logical = datetime(2026, 9, 20, 5, tzinfo=timezone.utc)
        with mock.patch.object(module, "SnowflakeClient") as sf_cls:
            sf = sf_cls.return_value.__enter__.return_value
            module.record_writeback_run(summaries=summaries, run_id="run1",
                                         data_interval_start=None, logical_date=logical)
        sql = sf.execute.call_args.args[0]
        assert "2026-09-20" in sql
