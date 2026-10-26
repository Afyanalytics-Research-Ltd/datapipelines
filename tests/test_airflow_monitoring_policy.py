from __future__ import annotations

import asyncio
import importlib.util
import sys
from datetime import timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest


@pytest.fixture
def airflow_local_settings(monkeypatch: Any) -> Any:
    """Load Airflow local settings with the SDK deadline API stubbed."""
    class FakeDeadlineAlert:
        def __init__(self, *, reference: Any, interval: timedelta, callback: Any) -> None:
            self.reference = reference
            self.interval = interval
            self.callback = callback

    class FakeAsyncCallback:
        def __init__(self, callback_callable: Any) -> None:
            self.callback_callable = callback_callable

    deadline = ModuleType("airflow.sdk.definitions.deadline")
    deadline.AsyncCallback = FakeAsyncCallback  # type: ignore[attr-defined]
    deadline.DeadlineAlert = FakeDeadlineAlert  # type: ignore[attr-defined]
    deadline.DeadlineReference = SimpleNamespace(  # type: ignore[attr-defined]
        DAGRUN_QUEUED_AT="dagrun_queued_at"
    )
    airflow = ModuleType("airflow")
    sdk = ModuleType("airflow.sdk")
    definitions = ModuleType("airflow.sdk.definitions")
    airflow.__path__ = []  # type: ignore[attr-defined]
    sdk.__path__ = []  # type: ignore[attr-defined]
    definitions.__path__ = []  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "airflow", airflow)
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    monkeypatch.setitem(sys.modules, "airflow.sdk.definitions", definitions)
    monkeypatch.setitem(
        sys.modules, "airflow.sdk.definitions.deadline", deadline
    )

    settings_path = (
        Path(__file__).resolve().parents[1] / "config" / "airflow_local_settings.py"
    )
    spec = importlib.util.spec_from_file_location(
        "airflow_local_settings_under_test", settings_path
    )
    assert spec is not None and spec.loader is not None
    settings = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(settings)
    return settings


def test_dag_policy_sets_one_hour_deadline_and_preserves_override(
    airflow_local_settings: Any,
) -> None:
    dag = SimpleNamespace(deadline=None)
    airflow_local_settings.dag_policy(dag)

    assert dag.deadline.reference == "dagrun_queued_at"
    assert dag.deadline.interval == timedelta(hours=1)
    assert (
        dag.deadline.callback.callback_callable
        is airflow_local_settings.sentry_deadline_callback
    )

    existing_deadline = object()
    dag_with_override = SimpleNamespace(deadline=existing_deadline)
    airflow_local_settings.dag_policy(dag_with_override)
    assert dag_with_override.deadline is existing_deadline


def test_task_policy_preserves_existing_success_callbacks(
    airflow_local_settings: Any,
) -> None:
    existing_callback = lambda context: None
    task = SimpleNamespace(
        on_success_callback=existing_callback,
        downstream_task_ids=set(),
    )

    airflow_local_settings.task_policy(task)

    assert task.on_success_callback == [
        existing_callback,
        airflow_local_settings.pipeline_row_count_callback,
    ]

    upstream_task = SimpleNamespace(
        on_success_callback=None,
        downstream_task_ids={"load_rows"},
    )
    airflow_local_settings.task_policy(upstream_task)
    assert upstream_task.on_success_callback is None


def test_deadline_callback_sends_tagged_sentry_event(
    monkeypatch: Any,
    airflow_local_settings: Any,
) -> None:
    initialized: list[dict[str, Any]] = []
    events: list[tuple[str, str]] = []
    tags: dict[str, str] = {}

    class FakeScope:
        def __enter__(self) -> FakeScope:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def set_tag(self, key: str, value: str) -> None:
            tags[key] = value

    sentry_sdk = ModuleType("sentry_sdk")
    sentry_sdk.is_initialized = lambda: False  # type: ignore[attr-defined]
    sentry_sdk.init = lambda **kwargs: initialized.append(kwargs)  # type: ignore[attr-defined]
    sentry_sdk.new_scope = lambda: FakeScope()  # type: ignore[attr-defined]
    sentry_sdk.capture_message = (  # type: ignore[attr-defined]
        lambda message, *, level: events.append((message, level))
    )
    monkeypatch.setitem(sys.modules, "sentry_sdk", sentry_sdk)
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.invalid/1")

    dag_run = SimpleNamespace(dag_id="example-pipeline", run_id="run-1")
    asyncio.run(
        airflow_local_settings.sentry_deadline_callback(
            context={"dag_run": dag_run}
        )
    )

    assert initialized == [
        {
            "dsn": "https://public@example.invalid/1",
            "send_default_pii": False,
        }
    ]
    assert events == [
        ("Airflow DAG run exceeded its one-hour runtime deadline", "error")
    ]
    assert tags == {
        "monitor_type": "dag_deadline",
        "dag_id": "example-pipeline",
        "run_id": "run-1",
    }


def test_row_count_callback_reports_explicit_zero_and_skips_missing_values(
    monkeypatch: Any,
    airflow_local_settings: Any,
) -> None:
    initialized: list[dict[str, Any]] = []
    events: list[tuple[str, str]] = []
    tags: dict[str, str] = {}

    class FakeScope:
        def __enter__(self) -> FakeScope:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def set_tag(self, key: str, value: str) -> None:
            tags[key] = value

    sentry_sdk = ModuleType("sentry_sdk")
    sentry_sdk.is_initialized = lambda: False  # type: ignore[attr-defined]
    sentry_sdk.init = lambda **kwargs: initialized.append(kwargs)  # type: ignore[attr-defined]
    sentry_sdk.new_scope = lambda: FakeScope()  # type: ignore[attr-defined]
    sentry_sdk.capture_message = (  # type: ignore[attr-defined]
        lambda message, *, level: events.append((message, level))
    )
    monkeypatch.setitem(sys.modules, "sentry_sdk", sentry_sdk)
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.invalid/1")
    monkeypatch.setenv("PIPELINES_DIR", str(Path(__file__).resolve().parents[1]))

    task_instance = SimpleNamespace(
        dag_id="example-pipeline",
        task_id="load_rows",
        run_id="run-2",
        xcom_pull=lambda **kwargs: 0,
    )
    airflow_local_settings.pipeline_row_count_callback(
        {"task_instance": task_instance}
    )

    assert initialized == [
        {
            "dsn": "https://public@example.invalid/1",
            "send_default_pii": False,
        }
    ]
    assert events == [
        (
            "Data-quality check 'example-pipeline.load_rows.output_rows' failed: "
            "output row count is zero",
            "error",
        )
    ]
    assert tags == {
        "monitor_type": "data_quality",
        "dag_id": "example-pipeline",
        "task_id": "load_rows",
        "run_id": "run-2",
        "quality_check": "example-pipeline.load_rows.output_rows",
    }

    task_instance.xcom_pull = lambda **kwargs: None
    airflow_local_settings.pipeline_row_count_callback(
        {"task_instance": task_instance}
    )
    task_instance.xcom_pull = lambda **kwargs: 1
    airflow_local_settings.pipeline_row_count_callback(
        {"task_instance": task_instance}
    )
    assert len(events) == 1


def test_deadline_callback_requires_sentry_dsn(
    monkeypatch: Any,
    airflow_local_settings: Any,
) -> None:
    monkeypatch.delenv("SENTRY_DSN", raising=False)

    with pytest.raises(RuntimeError, match="SENTRY_DSN must be configured"):
        asyncio.run(airflow_local_settings.sentry_deadline_callback(context={}))
