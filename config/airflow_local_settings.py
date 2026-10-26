from __future__ import annotations

import os
import sys
import threading
from datetime import timedelta
from typing import Any, Mapping

from airflow.sdk.definitions.deadline import (
    AsyncCallback,
    DeadlineAlert,
    DeadlineReference,
)

_DEADLINE_SENTRY_INITIALIZED = False
_DEADLINE_SENTRY_LOCK = threading.Lock()


def dag_policy(dag: Any) -> None:
    """Set a one-hour run deadline unless the DAG defines its own deadline."""
    if dag.deadline is None:
        dag.deadline = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(hours=1),
            callback=AsyncCallback(sentry_deadline_callback),
        )


def task_policy(task: Any) -> None:
    """Check explicit row-count XComs after successful terminal tasks."""
    if getattr(task, "downstream_task_ids", set()):
        return

    existing_callback = task.on_success_callback
    if existing_callback is None:
        task.on_success_callback = pipeline_row_count_callback
    elif isinstance(existing_callback, list):
        if pipeline_row_count_callback not in existing_callback:
            task.on_success_callback = [*existing_callback, pipeline_row_count_callback]
    elif existing_callback is not pipeline_row_count_callback:
        task.on_success_callback = [existing_callback, pipeline_row_count_callback]


def _initialize_sentry() -> Any:
    """Initialize Sentry when a monitoring callback runs outside a task."""
    global _DEADLINE_SENTRY_INITIALIZED

    dsn = os.getenv("SENTRY_DSN", "").strip()
    if not dsn and not _DEADLINE_SENTRY_INITIALIZED:
        raise RuntimeError(
            "SENTRY_DSN must be configured to report pipeline monitoring alerts"
        )

    import sentry_sdk

    with _DEADLINE_SENTRY_LOCK:
        if _DEADLINE_SENTRY_INITIALIZED or sentry_sdk.is_initialized():
            _DEADLINE_SENTRY_INITIALIZED = True
            return sentry_sdk

        sentry_sdk.init(dsn=dsn, send_default_pii=False)
        _DEADLINE_SENTRY_INITIALIZED = True
        return sentry_sdk


def pipeline_row_count_callback(context: Mapping[str, Any]) -> None:
    """Alert when a task explicitly reports zero output rows in XCom."""
    task_instance = context.get("task_instance") or context.get("ti")
    if task_instance is None:
        raise ValueError("Task success context does not contain a task instance")

    row_count = task_instance.xcom_pull(
        task_ids=task_instance.task_id,
        key="pipeline_row_count",
    )
    if row_count is None:
        return
    if isinstance(row_count, bool) or not isinstance(row_count, int):
        raise TypeError("pipeline_row_count XCom values must be integers")

    pipelines_dir = os.getenv("PIPELINES_DIR")
    if pipelines_dir and pipelines_dir not in sys.path:
        sys.path.insert(0, pipelines_dir)
    from pipeline_monitoring import check_row_count

    result = check_row_count(
        f"{task_instance.dag_id}.{task_instance.task_id}.output_rows",
        row_count,
        minimum=1,
    )
    if result.passed:
        return

    sentry_sdk = _initialize_sentry()
    with sentry_sdk.new_scope() as scope:
        scope.set_tag("monitor_type", "data_quality")
        scope.set_tag("dag_id", str(task_instance.dag_id))
        scope.set_tag("task_id", str(task_instance.task_id))
        scope.set_tag("run_id", str(task_instance.run_id))
        scope.set_tag("quality_check", result.name)
        sentry_sdk.capture_message(
            f"Data-quality check {result.name!r} failed: output row count is zero",
            level="error",
        )


async def sentry_deadline_callback(**kwargs: Any) -> None:
    """Send a Sentry issue when a DAG run exceeds the default runtime."""
    context = kwargs.get("context") or {}
    dag_run = context.get("dag_run") if isinstance(context, Mapping) else None
    dag_id = getattr(dag_run, "dag_id", None) or "airflow-unknown"
    run_id = getattr(dag_run, "run_id", None)
    sentry_sdk = _initialize_sentry()

    with sentry_sdk.new_scope() as scope:
        scope.set_tag("monitor_type", "dag_deadline")
        scope.set_tag("dag_id", str(dag_id))
        if run_id is not None:
            scope.set_tag("run_id", str(run_id))
        sentry_sdk.capture_message(
            "Airflow DAG run exceeded its one-hour runtime deadline",
            level="error",
        )
