from __future__ import annotations

import asyncio
import importlib.util
import logging
import sys
from datetime import timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


class FakeConf:
    """Stand-in for airflow.configuration.conf backed by a [sentry] dict."""

    def __init__(self, sentry: dict[str, str]) -> None:
        self.sentry = sentry

    def get(self, section: str, key: str, fallback: str = "") -> str:
        assert section == "sentry"
        return self.sentry.get(key, fallback)

    def getboolean(self, section: str, key: str, fallback: bool = False) -> bool:
        value = self.get(section, key, "")
        return value.lower() in {"1", "true", "yes", "on"} if value else fallback


class FakeSentry:
    """Records what the monitoring code sends to an initialized Sentry SDK."""

    def __init__(self, *, initialized: bool = True) -> None:
        self.initialized = initialized
        self.init_calls: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []
        self.flushes = 0
        self._tags: dict[str, str] = {}
        self._contexts: dict[str, Any] = {}

    def module(self) -> ModuleType:
        fake = self
        sentry_sdk = ModuleType("sentry_sdk")

        class Scope:
            def __enter__(self) -> Scope:
                fake._tags, fake._contexts = {}, {}
                return self

            def __exit__(self, *args: Any) -> None:
                return None

            def set_tag(self, key: str, value: str) -> None:
                fake._tags[key] = value

            def set_context(self, key: str, value: Any) -> None:
                fake._contexts[key] = value

        def init(**kwargs: Any) -> None:
            fake.init_calls.append(kwargs)
            fake.initialized = True

        def record(kind: str, payload: Any, **extra: Any) -> None:
            fake.events.append(
                {"kind": kind, "payload": payload, "tags": dict(fake._tags),
                 "contexts": dict(fake._contexts), **extra}
            )

        def flush(timeout: float | None = None) -> None:
            fake.flushes += 1

        sentry_sdk.is_initialized = lambda: fake.initialized  # type: ignore[attr-defined]
        sentry_sdk.init = init  # type: ignore[attr-defined]
        sentry_sdk.new_scope = Scope  # type: ignore[attr-defined]
        sentry_sdk.capture_message = (  # type: ignore[attr-defined]
            lambda message, *, level: record("message", message, level=level)
        )
        sentry_sdk.capture_exception = (  # type: ignore[attr-defined]
            lambda error: record("exception", error)
        )
        sentry_sdk.flush = flush  # type: ignore[attr-defined]
        return sentry_sdk


@pytest.fixture
def install_sentry(monkeypatch: Any) -> Any:
    """Install a FakeSentry as sentry_sdk (with its logging integration)."""
    def install(*, initialized: bool = True) -> FakeSentry:
        fake = FakeSentry(initialized=initialized)
        integrations = ModuleType("sentry_sdk.integrations")
        integrations.__path__ = []  # type: ignore[attr-defined]
        logging_integration = ModuleType("sentry_sdk.integrations.logging")

        class FakeLoggingIntegration:
            def __init__(self, **kwargs: Any) -> None:
                self.options = kwargs

        logging_integration.LoggingIntegration = FakeLoggingIntegration  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "sentry_sdk", fake.module())
        monkeypatch.setitem(sys.modules, "sentry_sdk.integrations", integrations)
        monkeypatch.setitem(
            sys.modules, "sentry_sdk.integrations.logging", logging_integration
        )
        return fake

    return install


@pytest.fixture
def load_settings(monkeypatch: Any) -> Any:
    """Load Airflow local settings with conf and the deadline API stubbed."""
    class FakeDeadlineAlert:
        def __init__(self, *, reference: Any, interval: timedelta, callback: Any) -> None:
            self.reference = reference
            self.interval = interval
            self.callback = callback

    class FakeAsyncCallback:
        def __init__(self, callback_callable: Any) -> None:
            self.callback_callable = callback_callable

    monkeypatch.setenv("PIPELINES_DIR", str(REPO_ROOT))
    monkeypatch.delenv("SENTRY_DSN", raising=False)

    def load(sentry: dict[str, str] | None = None) -> Any:
        deadline = ModuleType("airflow.sdk.definitions.deadline")
        deadline.AsyncCallback = FakeAsyncCallback  # type: ignore[attr-defined]
        deadline.DeadlineAlert = FakeDeadlineAlert  # type: ignore[attr-defined]
        deadline.DeadlineReference = SimpleNamespace(  # type: ignore[attr-defined]
            DAGRUN_QUEUED_AT="dagrun_queued_at"
        )
        configuration = ModuleType("airflow.configuration")
        configuration.conf = FakeConf(sentry or {})  # type: ignore[attr-defined]
        airflow = ModuleType("airflow")
        sdk = ModuleType("airflow.sdk")
        definitions = ModuleType("airflow.sdk.definitions")
        airflow.__path__ = []  # type: ignore[attr-defined]
        sdk.__path__ = []  # type: ignore[attr-defined]
        definitions.__path__ = []  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "airflow", airflow)
        monkeypatch.setitem(sys.modules, "airflow.configuration", configuration)
        monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
        monkeypatch.setitem(sys.modules, "airflow.sdk.definitions", definitions)
        monkeypatch.setitem(
            sys.modules, "airflow.sdk.definitions.deadline", deadline
        )

        spec = importlib.util.spec_from_file_location(
            "airflow_local_settings_under_test",
            REPO_ROOT / "config" / "airflow_local_settings.py",
        )
        assert spec is not None and spec.loader is not None
        settings = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(settings)
        return settings

    return load


def test_service_startup_initializes_sentry_from_airflow_config(
    install_sentry: Any, load_settings: Any
) -> None:
    sentry = install_sentry(initialized=False)

    load_settings(
        {
            "sentry_on": "true",
            "sentry_dsn": "https://public@example.invalid/1",
            "environment": "production",
        }
    )

    assert len(sentry.init_calls) == 1
    options = sentry.init_calls[0]
    assert options["dsn"] == "https://public@example.invalid/1"
    assert options["environment"] == "production"
    assert options["release"] is None
    assert options["send_default_pii"] is False
    assert options["auto_enabling_integrations"] is False
    assert options["integrations"][0].options == {"level": None, "event_level": None}


def test_service_startup_falls_back_to_sentry_dsn_env(
    monkeypatch: Any, install_sentry: Any, load_settings: Any
) -> None:
    sentry = install_sentry(initialized=False)
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.invalid/2")

    load_settings({"sentry_on": "true"})

    assert sentry.init_calls[0]["dsn"] == "https://public@example.invalid/2"


@pytest.mark.parametrize(
    "sentry_config",
    [{}, {"sentry_on": "false", "sentry_dsn": "https://public@example.invalid/1"},
     {"sentry_on": "true"}],
    ids=["sentry-off-by-default", "sentry-off", "no-dsn"],
)
def test_service_startup_skips_sentry_when_off_or_without_dsn(
    install_sentry: Any, load_settings: Any, sentry_config: dict[str, str]
) -> None:
    sentry = install_sentry(initialized=False)

    settings = load_settings(sentry_config)

    assert sentry.init_calls == []
    assert settings.configure_sentry() is False


def test_service_startup_keeps_an_existing_sentry_client(
    install_sentry: Any, load_settings: Any
) -> None:
    sentry = install_sentry(initialized=True)

    settings = load_settings(
        {"sentry_on": "true", "sentry_dsn": "https://public@example.invalid/1"}
    )

    assert sentry.init_calls == []
    assert settings.configure_sentry() is True


def test_service_startup_survives_failed_sentry_initialization(
    install_sentry: Any, load_settings: Any, caplog: Any
) -> None:
    sentry = install_sentry(initialized=False)

    def failing_init(**kwargs: Any) -> None:
        raise ValueError("bad DSN")

    sys.modules["sentry_sdk"].init = failing_init  # type: ignore[attr-defined]

    settings = load_settings({"sentry_on": "true", "sentry_dsn": "not-a-dsn"})

    assert sentry.initialized is False
    assert settings.configure_sentry() is False
    assert "Could not initialize Sentry" in caplog.text


def test_dag_policy_sets_one_hour_deadline_and_preserves_override(
    load_settings: Any,
) -> None:
    settings = load_settings()
    dag = SimpleNamespace(deadline=None)
    settings.dag_policy(dag)

    assert dag.deadline.reference == "dagrun_queued_at"
    assert dag.deadline.interval == timedelta(hours=1)
    assert dag.deadline.callback.callback_callable is settings.sentry_deadline_callback

    existing_deadline = object()
    dag_with_override = SimpleNamespace(deadline=existing_deadline)
    settings.dag_policy(dag_with_override)
    assert dag_with_override.deadline is existing_deadline


def test_task_policy_attaches_monitoring_and_preserves_existing_callbacks(
    load_settings: Any,
) -> None:
    settings = load_settings()
    existing_success = lambda context: None
    existing_failure = lambda context: None
    terminal = SimpleNamespace(
        on_success_callback=existing_success,
        on_failure_callback=[existing_failure],
        downstream_task_ids=set(),
    )

    settings.task_policy(terminal)
    settings.task_policy(terminal)  # applying twice must not duplicate

    assert terminal.on_success_callback == [
        existing_success,
        settings.pipeline_row_count_callback,
    ]
    assert terminal.on_failure_callback == [
        existing_failure,
        settings.pipeline_failure_callback,
    ]

    upstream = SimpleNamespace(
        on_success_callback=None,
        on_failure_callback=None,
        downstream_task_ids={"load_rows"},
    )
    settings.task_policy(upstream)
    assert upstream.on_success_callback is None
    assert upstream.on_failure_callback is settings.pipeline_failure_callback


def test_failure_callback_sends_tagged_exception_to_sentry(
    install_sentry: Any, load_settings: Any
) -> None:
    sentry = install_sentry()
    settings = load_settings()
    error = RuntimeError("load failed")
    task_instance = SimpleNamespace(
        dag_id="example-pipeline", task_id="load_rows", run_id="run-3", try_number=2
    )

    settings.pipeline_failure_callback(
        {
            "task_instance": task_instance,
            "params": {"facility": "kisumu"},
            "exception": error,
        }
    )

    assert len(sentry.events) == 1
    event = sentry.events[0]
    assert event["kind"] == "exception" and event["payload"] is error
    assert event["tags"] == {
        "pipeline": "example-pipeline",
        "run_id": "run-3",
        "facility": "kisumu",
        "event_type": "failure",
        "status": "failed",
        "stage": "load_rows",
        "monitor_type": "task_failure",
        "dag_id": "example-pipeline",
        "task_id": "load_rows",
        "try_number": "2",
    }
    assert sentry.flushes == 1


def test_deadline_callback_sends_tagged_sentry_event(
    install_sentry: Any, load_settings: Any
) -> None:
    sentry = install_sentry()
    settings = load_settings()

    dag_run = SimpleNamespace(dag_id="example-pipeline", run_id="run-1")
    asyncio.run(settings.sentry_deadline_callback(context={"dag_run": dag_run}))

    assert len(sentry.events) == 1
    event = sentry.events[0]
    assert event["kind"] == "message" and event["level"] == "error"
    assert event["payload"] == (
        "Pipeline example-pipeline deadline_missed status=breached: "
        "DAG run exceeded its one-hour runtime deadline"
    )
    assert event["tags"]["monitor_type"] == "dag_deadline"
    assert event["tags"]["dag_id"] == "example-pipeline"
    assert event["tags"]["run_id"] == "run-1"
    assert event["contexts"]["pipeline_monitor"]["data"] == {"deadline_seconds": 3600.0}
    assert sentry.flushes == 1


def test_row_count_callback_reports_explicit_zero_and_skips_missing_values(
    install_sentry: Any, load_settings: Any
) -> None:
    sentry = install_sentry()
    settings = load_settings()
    task_instance = SimpleNamespace(
        dag_id="example-pipeline",
        task_id="load_rows",
        run_id="run-2",
        xcom_pull=lambda **kwargs: 0,
    )

    settings.pipeline_row_count_callback({"task_instance": task_instance})

    assert len(sentry.events) == 1
    event = sentry.events[0]
    assert event["kind"] == "message" and event["level"] == "error"
    assert event["payload"] == (
        "Pipeline example-pipeline quality_check status=failed: "
        "output row count is zero"
    )
    assert event["tags"]["monitor_type"] == "data_quality"
    assert event["tags"]["task_id"] == "load_rows"
    assert event["tags"]["run_id"] == "run-2"
    assert event["tags"]["quality_check"] == "example-pipeline.load_rows.output_rows"
    assert event["contexts"]["pipeline_monitor"]["data"]["observed"] == 0

    task_instance.xcom_pull = lambda **kwargs: None
    settings.pipeline_row_count_callback({"task_instance": task_instance})
    task_instance.xcom_pull = lambda **kwargs: 1
    settings.pipeline_row_count_callback({"task_instance": task_instance})
    assert len(sentry.events) == 1

    task_instance.xcom_pull = lambda **kwargs: "0"
    with pytest.raises(TypeError, match="must be integers"):
        settings.pipeline_row_count_callback({"task_instance": task_instance})


def test_callbacks_only_log_when_sentry_is_not_initialized(
    install_sentry: Any, load_settings: Any, caplog: Any
) -> None:
    sentry = install_sentry(initialized=False)
    settings = load_settings()
    caplog.set_level(logging.ERROR, logger="pipeline_monitoring")

    asyncio.run(
        settings.sentry_deadline_callback(
            context={"dag_run": SimpleNamespace(dag_id="example-pipeline", run_id="run-4")}
        )
    )

    assert sentry.events == []
    assert caplog.records[-1].pipeline_monitor_event_type == "deadline_missed"
