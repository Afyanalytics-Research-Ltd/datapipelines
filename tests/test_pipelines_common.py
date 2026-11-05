"""
dags/pipelines_common.py unit tests — focused on lap_callback.

lap_callback is a fire-and-forget HTTP call: it must never raise, must POST the
right payload, and must do nothing when LAP_CALLBACK_URL is not set.
"""
from __future__ import annotations

import json
import os
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from tests.helpers import load_dag_module

pc = load_dag_module("pipelines_common")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_context(dag_id: str, run_id: str, state: str) -> dict:
    dag_run = MagicMock()
    dag_run.dag_id = dag_id
    dag_run.run_id = run_id
    dag_run.state = state
    return {"dag_run": dag_run}


# ---------------------------------------------------------------------------
# lap_callback — no-ops
# ---------------------------------------------------------------------------

def test_lap_callback_does_nothing_when_url_not_set(monkeypatch):
    """No LAP_CALLBACK_URL → function returns without making any network call."""
    monkeypatch.delenv("LAP_CALLBACK_URL", raising=False)
    ctx = _make_context("gdrive_landingai_extract", "lap_docs1_abc_step1", "success")
    with patch("urllib.request.urlopen") as mock_open:
        pc.lap_callback(ctx)
    mock_open.assert_not_called()


def test_lap_callback_does_nothing_when_dag_run_missing():
    """Missing dag_run in context → early return, no crash."""
    with patch("urllib.request.urlopen") as mock_open:
        pc.lap_callback({})
    mock_open.assert_not_called()


# ---------------------------------------------------------------------------
# lap_callback — happy path
# ---------------------------------------------------------------------------

def test_lap_callback_posts_correct_payload(monkeypatch):
    monkeypatch.setenv("LAP_CALLBACK_URL", "https://afyapi.test/api/airflow/dag-callback")
    monkeypatch.setenv("LAP_CALLBACK_SECRET", "my-secret")
    ctx = _make_context("docs_to_events_raw", "lap_docs5_xyz_step2", "success")

    captured: list = []

    def fake_urlopen(req, timeout):
        captured.append(req)
        return MagicMock(__enter__=lambda s: s, __exit__=MagicMock(return_value=False))

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        pc.lap_callback(ctx)

    assert len(captured) == 1
    req = captured[0]
    body = json.loads(req.data.decode())
    assert body == {"dag_id": "docs_to_events_raw", "run_id": "lap_docs5_xyz_step2", "state": "success"}
    assert req.get_header("X-lap-secret") == "my-secret"
    assert req.get_header("Content-type") == "application/json"


def test_lap_callback_posts_failed_state(monkeypatch):
    monkeypatch.setenv("LAP_CALLBACK_URL", "https://afyapi.test/api/airflow/dag-callback")
    monkeypatch.delenv("LAP_CALLBACK_SECRET", raising=False)
    ctx = _make_context("snowflake_flatten_clean", "lap_docs3_def_step3", "failed")

    captured: list = []

    def fake_urlopen(req, timeout):
        captured.append(req)
        return MagicMock(__enter__=lambda s: s, __exit__=MagicMock(return_value=False))

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        pc.lap_callback(ctx)

    body = json.loads(captured[0].data.decode())
    assert body["state"] == "failed"


# ---------------------------------------------------------------------------
# lap_callback — fire-and-forget: never raises
# ---------------------------------------------------------------------------

def test_lap_callback_swallows_network_error_and_logs_warning(monkeypatch, caplog):
    """Network failure must not raise AND must emit a WARNING with the URL and cause so Airflow logs are actionable."""
    monkeypatch.setenv("LAP_CALLBACK_URL", "https://afyapi.test/api/airflow/dag-callback")
    ctx = _make_context("gdrive_landingai_extract", "lap_docs1_abc_step1", "success")

    import logging
    with caplog.at_level(logging.WARNING):
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("connection refused")):
            pc.lap_callback(ctx)  # must not raise

    assert any("afyapi.test" in r.message for r in caplog.records), "warning must include the URL"
    assert any("connection refused" in r.message for r in caplog.records), "warning must include the cause"


def test_lap_callback_swallows_http_error(monkeypatch):
    monkeypatch.setenv("LAP_CALLBACK_URL", "https://afyapi.test/api/airflow/dag-callback")
    ctx = _make_context("gdrive_landingai_extract", "lap_docs1_abc_step1", "success")

    with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
        "url", 401, "Unauthorized", {}, None
    )):
        pc.lap_callback(ctx)  # must not raise


def test_lap_callback_swallows_timeout(monkeypatch):
    monkeypatch.setenv("LAP_CALLBACK_URL", "https://afyapi.test/api/airflow/dag-callback")
    ctx = _make_context("gdrive_landingai_extract", "lap_docs1_abc_step1", "success")

    with patch("urllib.request.urlopen", side_effect=TimeoutError("timed out")):
        pc.lap_callback(ctx)  # must not raise
