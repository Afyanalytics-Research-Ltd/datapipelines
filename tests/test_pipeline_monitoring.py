from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

import pipeline_monitoring as monitoring
from pipeline_monitoring import (
    MonitoringError,
    PipelineMonitor,
    QualityResult,
    airflow_failure_callback,
    check_duplicates,
    check_metric_change,
    check_null_rate,
    check_referential_integrity,
    check_row_count,
    check_schema,
)


class FakeSentry:
    """Stand-in sentry_sdk module that records captured events and flushes."""

    def __init__(self, *, initialized: bool) -> None:
        self.initialized = initialized
        self.events: list[dict[str, Any]] = []
        self.flushes = 0
        self.fail_capture = False
        self._tags: dict[str, str] = {}

    def module(self) -> ModuleType:
        fake = self
        sentry_sdk = ModuleType("sentry_sdk")

        class Scope:
            def __enter__(self) -> Scope:
                fake._tags = {}
                return self

            def __exit__(self, *args: Any) -> None:
                return None

            def set_tag(self, key: str, value: str) -> None:
                fake._tags[key] = value

            def set_context(self, key: str, value: Any) -> None:
                return None

        def record(kind: str, payload: Any) -> None:
            if fake.fail_capture:
                raise ConnectionError("Sentry unavailable")
            fake.events.append({"kind": kind, "payload": payload, "tags": dict(fake._tags)})

        def flush(timeout: float | None = None) -> None:
            fake.flushes += 1

        sentry_sdk.is_initialized = lambda: fake.initialized  # type: ignore[attr-defined]
        sentry_sdk.new_scope = Scope  # type: ignore[attr-defined]
        sentry_sdk.capture_message = (  # type: ignore[attr-defined]
            lambda message, *, level: record("message", message)
        )
        sentry_sdk.capture_exception = lambda error: record("exception", error)  # type: ignore[attr-defined]
        sentry_sdk.flush = flush  # type: ignore[attr-defined]
        return sentry_sdk


@pytest.fixture(autouse=True)
def uninitialized_sentry(monkeypatch: Any) -> FakeSentry:
    """Keep ordinary unit tests local: the process has not initialized Sentry."""
    fake = FakeSentry(initialized=False)
    monkeypatch.setitem(sys.modules, "sentry_sdk", fake.module())
    return fake


@pytest.fixture
def initialized_sentry(monkeypatch: Any) -> FakeSentry:
    """Simulate a process whose Sentry SDK was initialized at startup."""
    fake = FakeSentry(initialized=True)
    monkeypatch.setitem(sys.modules, "sentry_sdk", fake.module())
    return fake


def test_context_manager_logs_lifecycle_events_and_redacts_secrets(
    caplog: Any,
) -> None:
    """A run emits structured lifecycle records with sensitive values redacted."""
    caplog.set_level(logging.DEBUG, logger=monitoring.LOGGER.name)
    monitor = PipelineMonitor("unit-test", run_id="run-1")

    with monitor:
        monitor.metric("rows", 4, unit="records")
        monitor.emit(
            "metadata",
            message="request token=private-value failed",
            data={"api_key": "hidden", "safe": "visible"},
            occurred_at=datetime(2026, 10, 23, 12, tzinfo=timezone.utc),
        )

    records = [
        record
        for record in caplog.records
        if hasattr(record, "pipeline_monitor_event_type")
    ]
    assert [record.pipeline_monitor_event_type for record in records] == [
        "run_started",
        "metric",
        "metadata",
        "run_finished",
    ]
    metadata = records[2]
    assert "token=[REDACTED]" in metadata.getMessage()
    assert metadata.pipeline_monitor_data == {
        "api_key": "[REDACTED]",
        "safe": "visible",
    }
    assert metadata.pipeline_monitor_occurred_at == "2026-10-23T12:00:00.000+00:00"
    assert records[-1].pipeline_monitor_status == "succeeded"


def test_failed_quality_check_logs_error_with_searchable_attributes(
    caplog: Any,
) -> None:
    """Failed checks are error-level records with pipeline metadata for Sentry."""
    caplog.set_level(logging.INFO, logger=monitoring.LOGGER.name)
    monitor = PipelineMonitor("quality-pipeline", run_id="run-2", facility="clinic_a")

    monitor.quality(
        QualityResult(
            "row_count",
            False,
            0,
            {"minimum": 1},
            message="No rows loaded",
        ),
        stage="load",
    )

    record = caplog.records[-1]
    assert record.levelno == logging.ERROR
    assert record.pipeline_monitor_pipeline == "quality-pipeline"
    assert record.pipeline_monitor_run_id == "run-2"
    assert record.pipeline_monitor_facility == "clinic_a"
    assert record.pipeline_monitor_event_type == "quality_check"
    assert record.pipeline_monitor_status == "failed"
    assert record.pipeline_monitor_stage == "load"
    assert record.pipeline_monitor_data["check"] == "row_count"


def test_heartbeat_is_debug_level(caplog: Any) -> None:
    """Heartbeat events can be enabled explicitly without flooding INFO logs."""
    caplog.set_level(logging.DEBUG, logger=monitoring.LOGGER.name)
    PipelineMonitor("heartbeat-pipeline").heartbeat(stage="extract")

    assert caplog.records[-1].levelno == logging.DEBUG
    assert caplog.records[-1].pipeline_monitor_event_type == "heartbeat"


def test_context_manager_logs_exception_and_does_not_swallow_it(caplog: Any) -> None:
    """Pipeline exceptions keep their traceback and propagate to the caller."""
    caplog.set_level(logging.ERROR, logger=monitoring.LOGGER.name)

    with pytest.raises(RuntimeError, match="load failed"):
        with PipelineMonitor("failed-pipeline"):
            raise RuntimeError("load failed")

    failure = next(
        record for record in caplog.records
        if getattr(record, "pipeline_monitor_event_type", None) == "run_finished"
    )
    assert failure.pipeline_monitor_status == "failed"
    assert failure.exc_info is not None
    assert failure.exc_info[0] is RuntimeError


def test_explicit_finish_can_only_be_called_once() -> None:
    """A monitor records a single terminal outcome."""
    monitor = PipelineMonitor("finish-pipeline")
    monitor.finish("skipped")

    with pytest.raises(RuntimeError, match="already recorded"):
        monitor.finish("succeeded")


def test_quality_helpers_cover_bounds_schema_duplicates_and_relationships() -> None:
    """Built-in data checks return expected pass/fail results."""
    assert check_row_count("rows", 3, minimum=3).passed
    assert not check_row_count("rows", 2, minimum=3).passed
    assert check_null_rate(
        "nulls", null_count=1, total_count=100, max_rate=0.01
    ).passed
    assert not check_duplicates("duplicates", 1).passed
    assert check_metric_change(
        "row_count_drift",
        actual=90,
        baseline=100,
        max_change_fraction=0.15,
    ).passed
    assert not check_metric_change(
        "row_count_drift",
        actual=70,
        baseline=100,
        max_change_fraction=0.15,
    ).passed
    assert check_schema(
        "schema", expected_columns=["ID"], actual_columns=["id"]
    ).passed
    assert not check_schema(
        "schema", expected_columns=["id"], actual_columns=["id", "extra"]
    ).passed
    assert check_referential_integrity(
        "foreign_keys", orphan_count=0, child_count=20
    ).passed


def test_check_metric_change_handles_zero_baseline_and_rejects_invalid_values() -> None:
    """Metric drift calculations keep their zero-baseline and validation rules."""
    assert check_metric_change(
        "zero_baseline", actual=0, baseline=0, max_change_fraction=0
    ).passed
    assert not check_metric_change(
        "zero_baseline", actual=1, baseline=0, max_change_fraction=0
    ).passed
    with pytest.raises(ValueError, match="finite"):
        check_metric_change(
            "non_finite", actual=float("inf"), baseline=1, max_change_fraction=0.1
        )


def test_module_never_initializes_sentry_itself() -> None:
    """Sentry setup belongs to service startup (airflow_local_settings.py)."""
    assert not hasattr(monitoring, "configure_sentry")


def test_error_events_go_to_initialized_sentry_with_tags(
    initialized_sentry: FakeSentry,
) -> None:
    """ERROR-level events become tagged Sentry issues and are flushed at once."""
    monitor = PipelineMonitor(
        "quality-pipeline",
        run_id="run-5",
        facility="clinic_a",
        tags={"monitor_type": "data_quality", "unset": None},
    )

    monitor.metric("rows", 4)
    monitor.stage("extract", status="succeeded")
    monitor.quality(QualityResult("row_count", False, 0, {"minimum": 1}), stage="load")

    assert len(initialized_sentry.events) == 1
    event = initialized_sentry.events[0]
    assert event["kind"] == "message"
    assert event["payload"] == "Pipeline quality-pipeline quality_check status=failed: row_count"
    assert event["tags"] == {
        "pipeline": "quality-pipeline",
        "run_id": "run-5",
        "facility": "clinic_a",
        "event_type": "quality_check",
        "status": "failed",
        "stage": "load",
        "monitor_type": "data_quality",
    }
    assert initialized_sentry.flushes == 1


def test_error_event_with_exception_is_captured_as_exception(
    initialized_sentry: FakeSentry,
) -> None:
    """Exceptions keep their traceback in Sentry instead of a plain message."""
    error = RuntimeError("load failed")

    PipelineMonitor("failed-pipeline").failure("Load failed", exception=error)

    assert initialized_sentry.events[0]["kind"] == "exception"
    assert initialized_sentry.events[0]["payload"] is error


def test_error_events_are_only_logged_without_initialized_sentry(
    uninitialized_sentry: FakeSentry, caplog: Any
) -> None:
    """Local runs without Sentry still log every event."""
    caplog.set_level(logging.ERROR, logger=monitoring.LOGGER.name)

    PipelineMonitor("local-pipeline").failure("Load failed")

    assert uninitialized_sentry.events == []
    assert caplog.records[-1].pipeline_monitor_event_type == "failure"


def test_sentry_send_failure_can_be_strict_or_fail_open(
    initialized_sentry: FakeSentry, caplog: Any
) -> None:
    """A Sentry outage raises only when monitoring is configured as strict."""
    initialized_sentry.fail_capture = True

    PipelineMonitor("fail-open-pipeline").failure("Load failed")
    assert "Could not send the event to Sentry" in caplog.text

    with pytest.raises(MonitoringError, match="send the event to Sentry"):
        PipelineMonitor("strict-pipeline", fail_open=False).failure("Load failed")


def test_sentry_cron_checkin_tracks_start_and_success(
    monkeypatch: Any, initialized_sentry: FakeSentry,
) -> None:
    """Configured scheduled runs send Sentry Cron start and success check-ins."""
    checkins: list[dict[str, Any]] = []
    crons = ModuleType("sentry_sdk.crons")
    crons.capture_checkin = (  # type: ignore[attr-defined]
        lambda **kwargs: checkins.append(kwargs) or "check-in-1"
    )
    consts = ModuleType("sentry_sdk.crons.consts")
    consts.MonitorStatus = SimpleNamespace(  # type: ignore[attr-defined]
        IN_PROGRESS="in_progress",
        OK="ok",
        ERROR="error",
    )
    monkeypatch.setitem(sys.modules, "sentry_sdk.crons", crons)
    monkeypatch.setitem(sys.modules, "sentry_sdk.crons.consts", consts)

    monitor = PipelineMonitor(
        "hourly-pipeline",
        cron_monitor_config={
            "schedule": {"type": "interval", "value": 1, "unit": "hour"}
        },
    )
    monitor.start()
    monitor.finish("succeeded")

    assert checkins == [
        {
            "monitor_slug": "hourly-pipeline",
            "status": "in_progress",
            "monitor_config": {
                "schedule": {"type": "interval", "value": 1, "unit": "hour"}
            },
        },
        {
            "monitor_slug": "hourly-pipeline",
            "check_in_id": "check-in-1",
            "status": "ok",
        },
    ]
    assert initialized_sentry.flushes == 1


def test_sentry_cron_checkin_is_skipped_without_initialized_sentry(
    monkeypatch: Any,
) -> None:
    """No check-ins are attempted when the process has not set up Sentry."""
    crons = ModuleType("sentry_sdk.crons")
    crons.capture_checkin = lambda **kwargs: pytest.fail("unexpected check-in")  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "sentry_sdk.crons", crons)

    monitor = PipelineMonitor("hourly-pipeline", cron_monitor_config={})
    monitor.start()
    monitor.finish("succeeded")


def test_sentry_cron_checkin_marks_failed_run_as_error(
    monkeypatch: Any, initialized_sentry: FakeSentry,
) -> None:
    """Failed scheduled runs finish their check-in with Sentry error status."""
    checkins: list[dict[str, Any]] = []
    crons = ModuleType("sentry_sdk.crons")
    crons.capture_checkin = (  # type: ignore[attr-defined]
        lambda **kwargs: checkins.append(kwargs) or "check-in-2"
    )
    consts = ModuleType("sentry_sdk.crons.consts")
    consts.MonitorStatus = SimpleNamespace(  # type: ignore[attr-defined]
        IN_PROGRESS="in_progress",
        OK="ok",
        ERROR="error",
    )
    monkeypatch.setitem(sys.modules, "sentry_sdk.crons", crons)
    monkeypatch.setitem(sys.modules, "sentry_sdk.crons.consts", consts)

    monitor = PipelineMonitor("failing-cron", cron_monitor_config={})
    monitor.start()
    monitor.finish("failed", message="Task failed")

    assert checkins[-1]["status"] == "error"
    assert checkins[-1]["check_in_id"] == "check-in-2"


def test_airflow_failure_callback_logs_task_context_and_exception(
    caplog: Any,
) -> None:
    """The Airflow callback emits searchable task metadata and traceback."""
    caplog.set_level(logging.ERROR, logger=monitoring.LOGGER.name)
    task_instance = SimpleNamespace(
        task_id="load_rows", dag_id="callback-pipeline", run_id="airflow-run-1"
    )

    airflow_failure_callback(
        {
            "task_instance": task_instance,
            "run_id": "airflow-run-1",
            "params": {"facility": "clinic_a"},
            "exception": RuntimeError("load failed"),
        }
    )

    record = caplog.records[-1]
    assert record.pipeline_monitor_pipeline == "callback-pipeline"
    assert record.pipeline_monitor_run_id == "airflow-run-1"
    assert record.pipeline_monitor_facility == "clinic_a"
    assert record.pipeline_monitor_event_type == "failure"
    assert record.pipeline_monitor_stage == "load_rows"
    assert record.exc_info is not None
    assert record.exc_info[0] is RuntimeError
