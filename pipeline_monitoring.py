"""Opt-in pipeline logging and quality checks with Sentry integration.

Operational events are emitted through Python's standard logging framework.
When ``SENTRY_DSN`` is configured, the Sentry SDK forwards those logs and
ERROR-level events without storing them in a local or Airflow database.
"""
from __future__ import annotations

import logging
import math
import os
import re
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence


LOGGER = logging.getLogger("pipeline_monitoring")
_REDACTED = "[REDACTED]"
_SENSITIVE_KEY = re.compile(
    r"(password|passwd|secret|token|authorization|api[_-]?key|private[_-]?key)",
    re.IGNORECASE,
)
_SENSITIVE_TEXT = re.compile(
    r"(?i)\b(password|passwd|secret|token|authorization|api[_-]?key|private[_-]?key)"
    r"(\s*[:=]\s*)[^\s,;]+"
)
_SENTRY_INITIALIZED = False
_SENTRY_INITIALIZATION_LOCK = threading.Lock()


class MonitoringError(RuntimeError):
    """Raised when Sentry monitoring setup fails in strict mode."""


@dataclass(frozen=True)
class QualityResult:
    """Result of one data-quality or SLA check.

    Attributes:
        name: Stable, human-readable check name.
        passed: Whether the observed value satisfies the check.
        observed: JSON-safe measured value.
        expected: JSON-safe expectation or threshold.
        severity: Alert severity (for example ``critical``, ``high``, or ``low``).
        message: Optional brief explanation; do not put patient or row data here.
    """

    name: str
    passed: bool
    observed: Any
    expected: Any
    severity: str = "high"
    message: str = ""

    def __post_init__(self) -> None:
        """Reject empty check identifiers or severity labels."""
        if not self.name.strip():
            raise ValueError("QualityResult.name must not be blank")
        if not self.severity.strip():
            raise ValueError("QualityResult.severity must not be blank")


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


def _timestamp(value: datetime | None = None) -> str:
    """Format a datetime as an ISO-8601 UTC timestamp."""
    dt = value or _utc_now()
    if dt.tzinfo is None:
        raise ValueError("Timestamps must include a timezone")
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def _safe_value(value: Any, *, key: str = "") -> Any:
    """Redact secret-like mapping values and convert data to JSON-safe shapes."""
    if _SENSITIVE_KEY.search(key):
        return _REDACTED
    if isinstance(value, Mapping):
        return {str(k): _safe_value(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, datetime):
        return _timestamp(value)
    return str(value)


def _safe_message(message: str) -> str:
    """Redact common inline secret assignments from event and alert messages."""
    return _SENSITIVE_TEXT.sub(r"\1\2" + _REDACTED, message)[:2000]


def configure_sentry(
    dsn: str | None = None,
    *,
    environment: str | None = None,
    release: str | None = None,
) -> bool:
    """Initialize Sentry log capture once; return false when no DSN is configured.

    Sentry's logging integration sends INFO-and-higher records as searchable
    Sentry Logs and ERROR-and-higher records as Sentry issue events.
    """
    global _SENTRY_INITIALIZED

    sentry_dsn = (dsn or os.getenv("SENTRY_DSN", "")).strip()
    if not sentry_dsn:
        return False

    with _SENTRY_INITIALIZATION_LOCK:
        if _SENTRY_INITIALIZED:
            return True
        airflow_sentry_enabled = os.getenv(
            "AIRFLOW__SENTRY__SENTRY_ON", ""
        ).strip().lower() in {"1", "true", "yes", "on"}
        if airflow_sentry_enabled:
            # Airflow initializes its Sentry client around task execution.
            _SENTRY_INITIALIZED = True
            return True
        try:
            import sentry_sdk
            from sentry_sdk.integrations.logging import LoggingIntegration
        except ImportError as error:
            raise MonitoringError(
                "SENTRY_DSN is configured but sentry-sdk is not installed"
            ) from error

        try:
            sentry_sdk.init(
                dsn=sentry_dsn,
                environment=environment or os.getenv("SENTRY_ENVIRONMENT"),
                release=release or os.getenv("SENTRY_RELEASE"),
                send_default_pii=False,
                integrations=[
                    LoggingIntegration(
                        level=logging.INFO,
                        event_level=logging.ERROR,
                        capture_sentry_logs=True,
                        sentry_logs_level=logging.INFO,
                    )
                ],
            )
        except Exception as error:
            raise MonitoringError(
                f"Could not initialize Sentry ({type(error).__name__})"
            ) from error
        _SENTRY_INITIALIZED = True
    return True


@dataclass
class PipelineMonitor:
    """Emit structured pipeline events to standard logging and optionally Sentry.

    Monitoring is fail-open by default. Set ``fail_open=False`` to raise
    :class:`MonitoringError` if Sentry cannot be initialized.
    """

    pipeline: str
    run_id: str | None = None
    facility: str | None = None
    fail_open: bool = True
    cron_monitor_config: Mapping[str, Any] | None = None
    _finished: bool = field(default=False, init=False, repr=False)
    _check_in_id: str | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Resolve defaults and validate monitor identity."""
        self.pipeline = self.pipeline.strip()
        if not self.pipeline:
            raise ValueError("pipeline must not be blank")
        self.run_id = self.run_id or str(uuid.uuid4())
        try:
            configure_sentry()
        except MonitoringError:
            if not self.fail_open:
                raise
            LOGGER.exception("Could not initialize Sentry for pipeline monitoring")

    def emit(
        self,
        event_type: str,
        *,
        status: str = "info",
        stage: str | None = None,
        message: str = "",
        data: Mapping[str, Any] | None = None,
        occurred_at: datetime | None = None,
        exception: BaseException | None = None,
    ) -> dict[str, Any]:
        """Log a structured event; Sentry forwards it when configured."""
        if not event_type.strip():
            raise ValueError("event_type must not be blank")

        safe_data = _safe_value(data or {})
        event = {
            "event_id": str(uuid.uuid4()),
            "occurred_at": _timestamp(occurred_at),
            "pipeline": self.pipeline,
            "run_id": str(self.run_id),
            "facility": self.facility,
            "event_type": event_type,
            "status": status,
            "stage": stage,
            "message": _safe_message(message),
            "data": safe_data,
        }
        level = (
            logging.DEBUG
            if event_type == "heartbeat"
            else logging.ERROR
            if status in {"failed", "breached"}
            else logging.INFO
        )
        log_message = (
            f"Pipeline {self.pipeline} {event_type} status={status}"
            + (f": {event['message']}" if event["message"] else "")
        )
        LOGGER.log(
            level,
            log_message,
            # When sending log records as Sentry logs, any fields provided in 
            # the extra dictionary are automatically promoted to top-level 
            # attributes on the log entry. This makes them searchable and 
            # filterable in the Sentry UI.
            extra={
                "pipeline_monitor_event_id": event["event_id"],
                "pipeline_monitor_pipeline": self.pipeline,
                "pipeline_monitor_run_id": str(self.run_id),
                "pipeline_monitor_facility": self.facility,
                "pipeline_monitor_event_type": event_type,
                "pipeline_monitor_status": status,
                "pipeline_monitor_stage": stage,
                "pipeline_monitor_occurred_at": event["occurred_at"],
                "pipeline_monitor_data": safe_data,
            },
            exc_info=(
                (type(exception), exception, exception.__traceback__)
                if exception is not None
                else None
            ),
        )
        return event

    def start(self, *, metadata: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Record a pipeline run start."""
        event = self.emit("run_started", status="running", data=metadata)
        if self.cron_monitor_config is not None:
            self._start_cron_checkin()
        return event

    def _start_cron_checkin(self) -> None:
        """Start a Sentry Cron check-in when this run has a monitor configuration."""
        try:
            if not configure_sentry():
                return
        except MonitoringError as error:
            self._handle_sentry_error("initialize Sentry for Cron monitoring", error)
            return
        try:
            from sentry_sdk.crons import capture_checkin
            from sentry_sdk.crons.consts import MonitorStatus

            self._check_in_id = capture_checkin(
                monitor_slug=self.pipeline,
                status=MonitorStatus.IN_PROGRESS,
                monitor_config=dict(self.cron_monitor_config or {}),
            )
        except Exception as error:
            self._handle_sentry_error("start Sentry Cron check-in", error)

    def _finish_cron_checkin(self, status: str) -> None:
        """Finish the Sentry Cron check-in for this run, if one was started."""
        if self._check_in_id is None:
            return
        try:
            from sentry_sdk.crons import capture_checkin
            from sentry_sdk.crons.consts import MonitorStatus

            capture_checkin(
                monitor_slug=self.pipeline,
                check_in_id=self._check_in_id,
                status=(
                    MonitorStatus.OK if status in {"succeeded", "skipped"}
                    else MonitorStatus.ERROR
                ),
            )
        except Exception as error:
            self._handle_sentry_error("finish Sentry Cron check-in", error)

    def _handle_sentry_error(self, operation: str, error: Exception) -> None:
        """Log Sentry SDK failures or raise when strict monitoring is requested."""
        if not self.fail_open:
            raise MonitoringError(
                f"Could not {operation} ({type(error).__name__})"
            ) from error
        LOGGER.exception("Could not %s", operation, exc_info=error)

    def heartbeat(
        self, *, stage: str | None = None, metadata: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        """Record a debug-level heartbeat without filling the Sentry log stream."""
        return self.emit("heartbeat", status="running", stage=stage, data=metadata)

    def stage(
        self,
        name: str,
        *,
        status: str,
        message: str = "",
        metrics: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Record a stage transition; failed stages become Sentry error events."""
        if status not in {"running", "succeeded", "failed", "skipped"}:
            raise ValueError("Stage status must be running, succeeded, failed, or skipped")
        return self.emit(
            "stage",
            status=status,
            stage=name,
            message=message,
            data=metrics,
        )

    def metric(
        self,
        name: str,
        value: int | float,
        *,
        unit: str | None = None,
        stage: str | None = None,
    ) -> dict[str, Any]:
        """Log a numeric operational metric such as extracted row count."""
        if not name.strip():
            raise ValueError("Metric name must not be blank")
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError("Metric value must be a finite number")
        return self.emit(
            "metric",
            stage=stage,
            data={"name": name, "value": value, "unit": unit},
        )

    def quality(self, result: QualityResult, *, stage: str | None = None) -> dict[str, Any]:
        """Log a quality result; failed checks are sent as Sentry error events."""
        return self.emit(
            "quality_check",
            status="passed" if result.passed else "failed",
            stage=stage,
            message=result.message or result.name,
            data={
                "check": result.name,
                "observed": result.observed,
                "expected": result.expected,
                "severity": result.severity,
            },
        )

    def failure(
        self,
        message: str,
        *,
        stage: str | None = None,
        severity: str = "high",
        data: Mapping[str, Any] | None = None,
        exception: BaseException | None = None,
    ) -> dict[str, Any]:
        """Log a failed pipeline operation, including a traceback when provided."""
        failure_data = dict(data or {})
        failure_data["severity"] = severity
        return self.emit(
            "failure",
            status="failed",
            stage=stage,
            message=message,
            data=failure_data,
            exception=exception,
        )

    def finish(
        self,
        status: str,
        *,
        message: str = "",
        summary: Mapping[str, Any] | None = None,
        exception: BaseException | None = None,
    ) -> dict[str, Any]:
        """Record run completion once without suppressing pipeline errors."""
        if status not in {"succeeded", "failed", "skipped"}:
            raise ValueError("Run status must be succeeded, failed, or skipped")
        if self._finished:
            raise RuntimeError("This monitor has already recorded run completion")
        event = self.emit(
            "run_finished",
            status=status,
            message=message,
            data=summary,
            exception=exception,
        )
        self._finished = True
        self._finish_cron_checkin(status)
        return event

    def __enter__(self) -> PipelineMonitor:
        """Start monitoring when used as a context manager."""
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any,
    ) -> bool:
        """Record the run outcome on context exit without swallowing exceptions."""
        if not self._finished:
            if exc_value is None:
                self.finish("succeeded")
            else:
                self.finish(
                    "failed",
                    message=f"{exc_type.__name__ if exc_type else 'Error'}: {exc_value}",
                    exception=exc_value,
                )
        return False


def check_row_count(
    name: str,
    actual: int,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
    severity: str = "high",
) -> QualityResult:
    """Check that a row count is within optional inclusive bounds."""
    if actual < 0:
        raise ValueError("actual row count cannot be negative")
    if minimum is not None and minimum < 0:
        raise ValueError("minimum cannot be negative")
    if maximum is not None and maximum < 0:
        raise ValueError("maximum cannot be negative")
    passed = (minimum is None or actual >= minimum) and (
        maximum is None or actual <= maximum
    )
    return QualityResult(
        name=name,
        passed=passed,
        observed=actual,
        expected={"minimum": minimum, "maximum": maximum},
        severity=severity,
    )


def check_null_rate(
    name: str,
    *,
    null_count: int,
    total_count: int,
    max_rate: float,
    severity: str = "high",
) -> QualityResult:
    """Check that a null fraction does not exceed the configured threshold."""
    if total_count < 0 or null_count < 0 or null_count > total_count:
        raise ValueError("Null and total counts must satisfy 0 <= null_count <= total_count")
    if not 0 <= max_rate <= 1:
        raise ValueError("max_rate must be between 0 and 1")
    actual_rate = null_count / total_count if total_count else 0.0
    return QualityResult(
        name=name,
        passed=actual_rate <= max_rate,
        observed={"null_count": null_count, "total_count": total_count, "rate": actual_rate},
        expected={"max_rate": max_rate},
        severity=severity,
    )


def check_duplicates(
    name: str, duplicate_count: int, *, max_duplicates: int = 0, severity: str = "high"
) -> QualityResult:
    """Check the number of duplicate keys against an allowed maximum."""
    if duplicate_count < 0 or max_duplicates < 0:
        raise ValueError("Duplicate counts cannot be negative")
    return QualityResult(
        name=name,
        passed=duplicate_count <= max_duplicates,
        observed=duplicate_count,
        expected={"max_duplicates": max_duplicates},
        severity=severity,
    )


def check_metric_change(
    name: str,
    *,
    actual: int | float,
    baseline: int | float,
    max_change_fraction: float,
    severity: str = "high",
) -> QualityResult:
    """Check absolute relative metric change against a caller-supplied baseline.

    If the baseline is zero, zero-to-zero passes and any non-zero value fails.
    Historical baselines must be supplied by the caller; this module does not
    persist or query metric history.
    """
    if isinstance(actual, bool) or not isinstance(actual, (int, float)):
        raise ValueError("actual must be numeric")
    if isinstance(baseline, bool) or not isinstance(baseline, (int, float)):
        raise ValueError("baseline must be numeric")
    if not math.isfinite(actual) or not math.isfinite(baseline):
        raise ValueError("actual and baseline must be finite")
    if not math.isfinite(max_change_fraction) or max_change_fraction < 0:
        raise ValueError("max_change_fraction must be finite and non-negative")
    change_fraction = (
        abs(actual - baseline) / abs(baseline)
        if baseline != 0
        else (0.0 if actual == 0 else None)
    )
    passed = (
        actual == 0
        if change_fraction is None
        else change_fraction <= max_change_fraction
    )
    return QualityResult(
        name=name,
        passed=passed,
        observed={"actual": actual, "baseline": baseline, "change_fraction": change_fraction},
        expected={"max_change_fraction": max_change_fraction},
        severity=severity,
    )


def check_schema(
    name: str,
    *,
    expected_columns: Sequence[str],
    actual_columns: Sequence[str],
    allow_extra: bool = False,
    severity: str = "high",
) -> QualityResult:
    """Compare case-insensitive column names and report missing/extra fields."""
    expected = {column.casefold() for column in expected_columns}
    actual = {column.casefold() for column in actual_columns}
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    return QualityResult(
        name=name,
        passed=not missing and (allow_extra or not extra),
        observed={"missing": missing, "extra": extra},
        expected={"columns": sorted(expected), "allow_extra": allow_extra},
        severity=severity,
    )


def check_referential_integrity(
    name: str,
    *,
    orphan_count: int,
    child_count: int,
    max_orphan_rate: float = 0.0,
    severity: str = "critical",
) -> QualityResult:
    """Check orphan references against a maximum orphan fraction."""
    if child_count < 0 or orphan_count < 0 or orphan_count > child_count:
        raise ValueError("Counts must satisfy 0 <= orphan_count <= child_count")
    if not 0 <= max_orphan_rate <= 1:
        raise ValueError("max_orphan_rate must be between 0 and 1")
    actual_rate = orphan_count / child_count if child_count else 0.0
    return QualityResult(
        name=name,
        passed=actual_rate <= max_orphan_rate,
        observed={
            "orphan_count": orphan_count,
            "child_count": child_count,
            "rate": actual_rate,
        },
        expected={"max_orphan_rate": max_orphan_rate},
        severity=severity,
    )


# Future central reconciliation hook. Keep disabled until DAGs publish
# comparable per-target counts and explicitly configured quality thresholds.
#
# def report_reconciliation(
#     monitor: PipelineMonitor,
#     *,
#     target: str,
#     expected_target_rows: int,
#     loaded_target_rows: int,
#     duplicate_count: int | None = None,
#     max_duplicates: int = 0,
#     null_count: int | None = None,
#     total_count: int | None = None,
#     max_null_rate: float | None = None,
#     expected_columns: Sequence[str] | None = None,
#     actual_columns: Sequence[str] | None = None,
#     allow_extra_columns: bool = False,
#     actual_metric: int | float | None = None,
#     baseline_metric: int | float | None = None,
#     max_change_fraction: float | None = None,
#     orphan_count: int | None = None,
#     child_count: int | None = None,
#     max_orphan_rate: float = 0.0,
# ) -> None:
#     checks = [
#         check_row_count(
#             f"{target}.loaded_rows",
#             loaded_target_rows,
#             minimum=expected_target_rows,
#             maximum=expected_target_rows,
#         )
#     ]
#     if duplicate_count is not None:
#         checks.append(
#             check_duplicates(
#                 f"{target}.duplicates",
#                 duplicate_count,
#                 max_duplicates=max_duplicates,
#             )
#         )
#     if null_count is not None or total_count is not None or max_null_rate is not None:
#         if null_count is None or total_count is None or max_null_rate is None:
#             raise ValueError("Null checks require counts and an explicit maximum rate")
#         checks.append(
#             check_null_rate(
#                 f"{target}.null_rate",
#                 null_count=null_count,
#                 total_count=total_count,
#                 max_rate=max_null_rate,
#             )
#         )
#     if expected_columns is not None or actual_columns is not None:
#         if expected_columns is None or actual_columns is None:
#             raise ValueError("Schema checks require expected and actual columns")
#         checks.append(
#             check_schema(
#                 f"{target}.schema",
#                 expected_columns=expected_columns,
#                 actual_columns=actual_columns,
#                 allow_extra=allow_extra_columns,
#             )
#         )
#     metric_values = (actual_metric, baseline_metric, max_change_fraction)
#     if any(value is not None for value in metric_values):
#         if any(value is None for value in metric_values):
#             raise ValueError("Metric checks require actual, baseline, and threshold")
#         checks.append(
#             check_metric_change(
#                 f"{target}.metric_change",
#                 actual=actual_metric,
#                 baseline=baseline_metric,
#                 max_change_fraction=max_change_fraction,
#             )
#         )
#     if orphan_count is not None or child_count is not None:
#         if orphan_count is None or child_count is None:
#             raise ValueError("Referential checks require orphan and child counts")
#         checks.append(
#             check_referential_integrity(
#                 f"{target}.referential_integrity",
#                 orphan_count=orphan_count,
#                 child_count=child_count,
#                 max_orphan_rate=max_orphan_rate,
#             )
#         )
#     for result in checks:
#         monitor.quality(result, stage="reconcile")


def airflow_failure_callback(context: Mapping[str, Any]) -> None:
    """Send failed Airflow task metadata and traceback to Sentry through logging.

    Configure a DAG or task with ``on_failure_callback=airflow_failure_callback``.
    Only operational metadata is read; task arguments and XCom payloads are not
    copied into monitoring events.
    """
    dag = context.get("dag")
    task_instance = context.get("task_instance") or context.get("ti")
    dag_id = getattr(dag, "dag_id", None) or getattr(task_instance, "dag_id", None)
    task_id = getattr(task_instance, "task_id", None)
    run_id = context.get("run_id") or getattr(task_instance, "run_id", None)
    params = context.get("params") or {}
    facility = params.get("facility") if isinstance(params, Mapping) else None
    exception = context.get("exception")
    pipeline = str(dag_id or "airflow-unknown")
    monitor = PipelineMonitor(
        pipeline=pipeline,
        run_id=str(run_id or uuid.uuid4()),
        facility=str(facility) if facility is not None else None,
    )
    message = (
        f"{type(exception).__name__}: {exception}"
        if isinstance(exception, BaseException)
        else "Airflow task failed"
    )
    monitor.failure(
        message,
        stage=str(task_id) if task_id else None,
        exception=exception if isinstance(exception, BaseException) else None,
    )


configure_sentry()
