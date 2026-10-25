from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from pipeline_monitoring import (
    MonitoringError,
    PipelineMonitor,
    PostgresEventStore,
    QualityResult,
    SLAPolicy,
    SQLiteEventStore,
    check_duplicates,
    check_metric_change,
    check_null_rate,
    check_referential_integrity,
    check_row_count,
    check_schema,
    default_event_store,
    run_watchdog,
)


class RecordingWebhook:
    """Capture webhook payloads without making network requests."""

    def __init__(self) -> None:
        """Initialize an empty payload list."""
        self.payloads: list[dict] = []

    def send(self, payload: dict) -> None:
        """Record one alert payload."""
        self.payloads.append(payload)


class FailOnceOnRecoveryWebhook(RecordingWebhook):
    """Fail one recovery delivery so incident-state retry can be verified."""

    def __init__(self) -> None:
        """Initialize the payload recorder and one-shot failure state."""
        super().__init__()
        self.failed_recovery = False

    def send(self, payload: dict) -> None:
        """Record each payload and fail the first recovery delivery."""
        self.payloads.append(payload)
        if payload["alert_type"] == "recovery" and not self.failed_recovery:
            self.failed_recovery = True
            raise MonitoringError("simulated webhook outage")


def test_default_event_store_selects_configured_database(
    monkeypatch: Any, tmp_path: Path
) -> None:
    """Configured PostgreSQL is preferred, with SQLite retained as local fallback."""
    monkeypatch.setenv(
        "PIPELINE_MONITOR_DATABASE_URL", "postgresql://user:password@postgres/airflow"
    )
    assert isinstance(default_event_store(), PostgresEventStore)

    monkeypatch.delenv("PIPELINE_MONITOR_DATABASE_URL")
    monkeypatch.setenv("PIPELINE_MONITOR_DB_PATH", str(tmp_path / "events.sqlite3"))
    assert isinstance(default_event_store(), SQLiteEventStore)


def test_context_manager_persists_events_and_redacts_secrets(tmp_path: Path) -> None:
    """A run records lifecycle events and redacts sensitive event fields."""
    store = SQLiteEventStore(tmp_path / "events.sqlite3")
    monitor = PipelineMonitor("unit-test", run_id="run-1", store=store)

    with monitor:
        monitor.metric("rows", 4, unit="records")
        monitor.emit(
            "metadata",
            message="request token=private-value failed",
            data={"api_key": "hidden", "safe": "visible"},
        )

    with closing(sqlite3.connect(store.path)) as connection, connection:
        rows = connection.execute(
            "SELECT event_type, status, message, data_json FROM pipeline_events "
            "ORDER BY occurred_at, rowid"
        ).fetchall()

    assert [row[0] for row in rows] == [
        "run_started",
        "metric",
        "metadata",
        "run_finished",
    ]
    assert rows[2][2] == "request token=[REDACTED] failed"
    assert json.loads(rows[2][3]) == {"api_key": "[REDACTED]", "safe": "visible"}
    assert rows[-1][1] == "succeeded"
    assert store.recent_metrics("unit-test", "rows") == [4.0]


def test_quality_alerts_are_deduplicated_and_recover(tmp_path: Path) -> None:
    """An active check alerts once and a later passing check reports recovery."""
    store = SQLiteEventStore(tmp_path / "events.sqlite3")
    webhook = RecordingWebhook()
    failed = QualityResult("row_count", False, 0, {"minimum": 1})

    for run_id in ("run-1", "run-2"):
        monitor = PipelineMonitor(
            "quality-pipeline", run_id=run_id, store=store, webhook=webhook
        )
        monitor.quality(failed)

    assert len(webhook.payloads) == 1
    assert webhook.payloads[0]["alert_type"] == "pipeline_incident"

    recovered = PipelineMonitor(
        "quality-pipeline", run_id="run-3", store=store, webhook=webhook
    )
    recovered.quality(QualityResult("row_count", True, 10, {"minimum": 1}))
    assert len(webhook.payloads) == 2
    assert webhook.payloads[1]["alert_type"] == "recovery"


def test_failed_recovery_notification_remains_retryable(tmp_path: Path) -> None:
    """A failed recovery delivery reopens the incident for another attempt."""
    store = SQLiteEventStore(tmp_path / "events.sqlite3")
    webhook = FailOnceOnRecoveryWebhook()
    failed = QualityResult("schema", False, {"missing": ["id"]}, {"columns": ["id"]})
    passed = QualityResult("schema", True, {"missing": [], "extra": []}, {"columns": ["id"]})

    PipelineMonitor("recovery-pipeline", store=store, webhook=webhook).quality(failed)
    monitor = PipelineMonitor("recovery-pipeline", store=store, webhook=webhook)
    monitor.quality(passed)
    monitor.quality(passed)

    assert [payload["alert_type"] for payload in webhook.payloads] == [
        "pipeline_incident",
        "recovery",
        "recovery",
    ]


def test_quality_helpers_cover_bounds_schema_duplicates_and_relationships() -> None:
    """Built-in data checks return the expected pass/fail results."""
    assert check_row_count("rows", 3, minimum=3).passed
    assert not check_row_count("rows", 2, minimum=3).passed
    assert check_null_rate(
        "nulls", null_count=1, total_count=100, max_rate=0.01
    ).passed
    assert check_duplicates("duplicates", 1).passed is False
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


def test_watchdog_reports_stale_and_fresh_signals(tmp_path: Path) -> None:
    """The watchdog identifies overdue heartbeats and leaves fresh runs healthy."""
    store = SQLiteEventStore(tmp_path / "events.sqlite3")
    webhook = RecordingWebhook()
    now = datetime(2026, 10, 23, 12, tzinfo=timezone.utc)
    stale = PipelineMonitor(
        "stale-pipeline",
        run_id="stale-run",
        store=store,
    )
    stale.emit("heartbeat", occurred_at=now - timedelta(minutes=20))
    fresh = PipelineMonitor("fresh-pipeline", run_id="fresh-run", store=store)
    fresh.emit("heartbeat", occurred_at=now - timedelta(minutes=2))

    results = run_watchdog(
        [
            SLAPolicy("stale-pipeline", max_silence_seconds=600),
            SLAPolicy("fresh-pipeline", max_silence_seconds=600),
            SLAPolicy(
                "not-started",
                max_silence_seconds=600,
                initial_due_at="2026-10-24T00:00:00Z",
            ),
        ],
        now=now,
        store=store,
        webhook=webhook,
    )

    assert [result.status for result in results] == ["breached", "ok", "not_due"]
    assert results[0].overdue_seconds == 600
    assert len(webhook.payloads) == 1
    assert webhook.payloads[0]["pipeline"] == "stale-pipeline"


def test_watchdog_detects_missed_initial_due_time(tmp_path: Path) -> None:
    """A configured first-run deadline raises an SLA breach without prior events."""
    store = SQLiteEventStore(tmp_path / "events.sqlite3")
    results = run_watchdog(
        [
            SLAPolicy(
                "never-started",
                max_silence_seconds=60,
                initial_due_at="2026-10-23T11:59:00Z",
            )
        ],
        now=datetime(2026, 10, 23, 12, tzinfo=timezone.utc),
        store=store,
    )
    assert results[0].status == "breached"
    assert results[0].overdue_seconds == 60


def test_airflow_failure_callback_records_task_metadata(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The Airflow callback records the failed task without requiring Airflow."""
    from pipeline_monitoring import airflow_failure_callback

    monkeypatch.setenv("PIPELINE_MONITOR_DB_PATH", str(tmp_path / "airflow.sqlite3"))
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

    with closing(sqlite3.connect(tmp_path / "airflow.sqlite3")) as connection:
        row = connection.execute(
            "SELECT pipeline, run_id, facility, event_type, stage, status "
            "FROM pipeline_events"
        ).fetchone()
    assert row == (
        "callback-pipeline",
        "airflow-run-1",
        "clinic_a",
        "failure",
        "load_rows",
        "failed",
    )
