"""
Coverage for dags/api_v2_incremental_to_snowflake.py (V2 / Ignite incremental).

The properties that must not regress:

  * Namespaces match facility_api_to_snowflake, and page 1 tries all four
    spellings in the same order before calling a model absent.
  * A model absent under every spelling is NOT_FOUND and its watermark never
    moves; a facility where EVERY model is NOT_FOUND fails the run.
  * One JSON object per JSONL line (flatten_jsons_schemas needs
    IS_OBJECT(payload)), COPYed into {FACILITY}_RAW with module_source.
  * Unwatermarked units seed from MAX(ingested_at) already in RAW.
  * report_failures filters on DAG_ID (run_ids collide with the V3 DAG).

All external I/O (Snowflake, HTTP, S3, time.sleep) is mocked.
"""
from __future__ import annotations

import gzip
import json
from datetime import datetime
from unittest import mock

import pytest

from tests.airflow_stub import TriggerRule
from tests.helpers import load_dag_module

MODULE_NAME = "api_v2_incremental_to_snowflake"
NS = "Ignite\\Core\\Entities\\Approvals"


@pytest.fixture
def module():
    return load_dag_module(MODULE_NAME)


def _resp(status_code=200, json_data=None):
    r = mock.Mock()
    r.status_code = status_code
    r.text = ""
    r.json.return_value = {} if json_data is None else json_data
    return r


def _job(module, facility="kisumu", namespace=NS):
    return {
        "facility": facility, "module": "Core", "table": "core_approvals",
        "namespace": namespace, "source_key": module._source_key(facility, namespace),
        "database": module.FACILITIES[facility]["db"],
        "window_start": "2026-09-29T09:45:00", "window_end": "2026-09-29T10:58:00",
        "updated_since": "2026-09-29T09:45:00Z", "limit": 500,
    }


@pytest.fixture
def no_io(module):
    with mock.patch.object(module.time, "sleep"), \
         mock.patch.object(module, "_auth_token", return_value="tok"), \
         mock.patch.object(module, "S3Hook") as s3:
        yield s3


@pytest.fixture
def sf(module):
    client = mock.MagicMock()
    client.__enter__.return_value = client
    client.execute.return_value = {"rows": [], "rowcount": 0, "sfqid": "q"}
    with mock.patch.object(module, "SnowflakeClient", return_value=client):
        yield client


def _uploaded_lines(s3):
    data = s3.return_value.load_bytes.call_args.kwargs["bytes_data"]
    return gzip.decompress(data).decode().splitlines()


# ─────────────────────────────────────────────────────────────────────────
# DAG shape
# ─────────────────────────────────────────────────────────────────────────
def test_dag_shape(module):
    d = module.dag
    assert d.dag_id == "api_v2_incremental_to_snowflake"
    assert "v2" in d.tags
    assert d.task_dict["report_failures"].trigger_rule == TriggerRule.ALL_DONE


# ─────────────────────────────────────────────────────────────────────────
# Namespaces
# ─────────────────────────────────────────────────────────────────────────
def test_namespace_matches_facility_api_to_snowflake(module):
    assert module._build_namespace("Core", "core_approvals") == NS
    assert module._build_namespace("Finance", "waivers") == "Ignite\\Finance\\Entities\\Waivers"


def test_candidate_order(module):
    assert module._namespace_candidates(NS) == [
        "Ignite\\Core\\Entities\\Approvals",
        "Ignite\\Core\\Entities\\Approval",
        "Ignite\\Core\\Entities\\CoreApprovals",
        "Ignite\\Core\\Entities\\CoreApproval",
    ]


# ─────────────────────────────────────────────────────────────────────────
# extract_one_unit
# ─────────────────────────────────────────────────────────────────────────
def test_falls_back_through_spellings_and_lands_objects(module, no_io):
    rows = [{"id": 1}, {"id": 2}]
    with mock.patch.object(module.requests, "post", side_effect=[
        _resp(404), _resp(404),
        _resp(200, {"data": rows, "pagination": {"has_more_pages": False}}),
    ]) as post:
        out = module.extract_one_unit(_job(module), run_id="r1")

    assert out["status"] == "EXTRACTED" and out["row_count"] == 2
    assert post.call_args.kwargs["json"]["namespace"] == "Ignite\\Core\\Entities\\CoreApprovals"
    assert post.call_args.args[0] == "https://kshospital.collabmed.net/api/finance/access/data/point"
    assert [json.loads(line) for line in _uploaded_lines(no_io)] == rows
    assert out["namespace"] == NS  # RAW keeps the canonical namespace


def test_all_404_is_not_found(module, no_io):
    with mock.patch.object(module.requests, "post", return_value=_resp(404)) as post:
        out = module.extract_one_unit(_job(module), run_id="r1")
    assert out["status"] == "NOT_FOUND"
    assert post.call_count == 4


def test_later_pages_reuse_resolved_spelling(module, no_io):
    with mock.patch.object(module.requests, "post", side_effect=[
        _resp(404),
        _resp(200, {"data": [{"id": 1}], "pagination": {"has_more_pages": True}}),
        _resp(200, {"data": [{"id": 2}], "pagination": {"has_more_pages": False}}),
    ]) as post:
        out = module.extract_one_unit(_job(module), run_id="r1")
    assert out["row_count"] == 2
    sent = [(c.kwargs["json"]["namespace"], c.kwargs["json"]["page"]) for c in post.call_args_list]
    assert sent[1:] == [("Ignite\\Core\\Entities\\Approval", 1),
                        ("Ignite\\Core\\Entities\\Approval", 2)]


def test_server_error_is_failed_not_not_found(module, no_io):
    with mock.patch.object(module.requests, "post", return_value=_resp(503)):
        out = module.extract_one_unit(_job(module), run_id="r1")
    assert out["status"] == "FAILED"


# ─────────────────────────────────────────────────────────────────────────
# prepare_jobs seeding
# ─────────────────────────────────────────────────────────────────────────
def test_unwatermarked_unit_seeds_from_raw(module, sf, set_variables):
    set_variables(IGNITE_SHEET_ID="s")
    now = datetime(2026, 9, 29, 12, 0, 0)
    landed = datetime(2026, 9, 29, 11, 0, 0)

    def execute(sql, label=None, params=None):
        if label == "bootstrap:kisumu":
            return {"rows": [(NS, landed)]}
        return {"rows": []}
    sf.execute.side_effect = execute

    with mock.patch.object(module, "FACILITIES", {"kisumu": module.FACILITIES["kisumu"]}), \
         mock.patch.object(module, "_read_sheet",
                           return_value=[{"module": "Core", "table": "core_approvals"}]), \
         mock.patch.object(module, "_utcnow", return_value=now):
        jobs = module.prepare_jobs()

    assert len(jobs) == 1
    assert jobs[0]["job"]["window_start"] == "2026-09-29T10:45:00"  # landed - LOOKBACK


# ─────────────────────────────────────────────────────────────────────────
# copy_and_advance / report_failures
# ─────────────────────────────────────────────────────────────────────────
def test_not_found_never_advances_watermark(module, sf):
    unit = {**_job(module), "status": "NOT_FOUND", "run_id": "r1", "row_count": 0,
            "s3_key": None}
    with mock.patch.object(module, "_advance_watermark") as adv:
        assert module.copy_and_advance(**unit)["status"] == "NOT_FOUND"
    adv.assert_not_called()


def test_copy_targets_raw_and_module_source(module, sf):
    unit = {**_job(module), "status": "EXTRACTED", "run_id": "r1", "row_count": 2,
            "s3_key": "raw/facilities_incremental/x.jsonl.gz",
            "ingested_at": "2026-09-29T11:00:00+00:00"}
    with mock.patch.object(module, "_advance_watermark") as adv:
        assert module.copy_and_advance(**unit)["status"] == "COPIED"
    adv.assert_called_once()
    copy_sql = sf.execute.call_args_list[0].args[0]
    assert "HOSPITALS.KISUMU_RAW.EVENTS_RAW" in copy_sql
    assert "module_source" in copy_sql


def test_report_filters_on_dag_id(module, sf):
    module.report_failures(run_id="scheduled__x")
    for c in sf.execute.call_args_list:
        assert "DAG_ID = %(dag)s" in c.args[0]
        assert c.kwargs["params"]["dag"] == "api_v2_incremental_to_snowflake"


def test_report_fails_when_every_namespace_404s(module, sf):
    sf.execute.side_effect = [{"rows": []}, {"rows": [("kisumu", 60, 60)]}]
    with pytest.raises(RuntimeError):
        module.report_failures(run_id="r1")


def test_report_passes_with_partial_not_found(module, sf):
    sf.execute.side_effect = [{"rows": []}, {"rows": [("kisumu", 5, 60)]}]
    module.report_failures(run_id="r1")
