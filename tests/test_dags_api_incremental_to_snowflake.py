"""
Coverage for dags/api_incremental_to_snowflake.py (V3 incremental).

Pins the two fixes made alongside adding the V2 sibling DAG:

  * copy_and_advance must not crash writing the run log. `unit` already
    carries "status", so passing both **unit and status= raised TypeError --
    after the COPY and watermark advance had committed, so the Airflow
    retry re-COPYed the file.
  * report_failures filters on DAG_ID: the V2 DAG shares the run log and the
    @hourly schedule, so scheduled run_ids are identical across the two.

All external I/O (Snowflake) is mocked.
"""
from __future__ import annotations

from unittest import mock

import pytest

from tests.helpers import load_dag_module

MODULE_NAME = "api_incremental_to_snowflake"


@pytest.fixture
def module():
    return load_dag_module(MODULE_NAME)


@pytest.fixture
def sf(module):
    client = mock.MagicMock()
    client.__enter__.return_value = client
    client.execute.return_value = {"rows": [], "rowcount": 0, "sfqid": "q"}
    with mock.patch.object(module, "SnowflakeClient", return_value=client):
        yield client


def _unit(module, status, **extra):
    ns = "core.approvals"
    return {
        "facility": "collabmed", "module": "Core", "table": "approvals", "namespace": ns,
        "source_key": module._source_key("collabmed", ns),
        "window_start": "2026-09-29T09:45:00", "window_end": "2026-09-29T10:58:00",
        "run_id": "r1", "status": status, "row_count": 0, "s3_key": None,
        "ingested_at": "2026-09-29T11:00:00+00:00", **extra,
    }


def _run_log_statuses(sf):
    return [c.kwargs["params"]["status"] for c in sf.execute.call_args_list
            if c.kwargs.get("label") == "run_log"]


def test_dag_id_is_unchanged(module):
    # Part of every watermark SOURCE_KEY; renaming it re-seeds every unit.
    assert module.dag.dag_id == "api_incremental_to_snowflake"


@pytest.mark.parametrize("status", ["FAILED", "EMPTY"])
def test_non_copy_paths_log_run(module, sf, status):
    out = module.copy_and_advance(**_unit(module, status, error="boom"))
    assert out["status"] == status
    assert _run_log_statuses(sf) == [status]


def test_copied_path_logs_run_once_and_advances(module, sf):
    unit = _unit(module, "EXTRACTED", row_count=2,
                 s3_key="raw/v3_facilities_incremental/x.jsonl.gz")
    with mock.patch.object(module, "_advance_watermark") as adv:
        assert module.copy_and_advance(**unit)["status"] == "COPIED"
    adv.assert_called_once()
    assert _run_log_statuses(sf) == ["COPIED"]


def test_report_filters_on_dag_id(module, sf):
    module.report_failures(run_id="scheduled__x")
    c = sf.execute.call_args
    assert "DAG_ID = %(dag)s" in c.args[0]
    assert c.kwargs["params"] == {"run_id": "scheduled__x", "dag": module.DAG_ID}
