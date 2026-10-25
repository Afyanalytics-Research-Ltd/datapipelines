"""Reusable, opt-in pipeline monitoring helpers for Airflow DAGs and scripts.

Events use PostgreSQL when ``PIPELINE_MONITOR_DATABASE_URL`` is configured and
otherwise default to a local SQLite database. Pipelines opt in by importing
:class:`PipelineMonitor`; this module never patches or discovers pipelines.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence


LOGGER = logging.getLogger("pipeline_monitoring")
DEFAULT_REPEAT_ALERT_SECONDS = 60 * 60
_REDACTED = "[REDACTED]"
_SENSITIVE_KEY = re.compile(
    r"(password|passwd|secret|token|authorization|api[_-]?key|private[_-]?key)",
    re.IGNORECASE,
)
_SENSITIVE_TEXT = re.compile(
    r"(?i)\b(password|passwd|secret|token|authorization|api[_-]?key|private[_-]?key)"
    r"(\s*[:=]\s*)[^\s,;]+"
)


class MonitoringError(RuntimeError):
    """Raised when monitoring persistence or alert delivery fails in strict mode."""


@dataclass(frozen=True)
class QualityResult:
    """Result of one data-quality or SLA check.

    Attributes:
        name: Stable, human-readable check name used for incident deduplication.
        passed: Whether the observed value satisfies the check.
        observed: JSON-serializable measured value.
        expected: JSON-serializable expectation or threshold.
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


@dataclass(frozen=True)
class SLAPolicy:
    """Expected heartbeat frequency used by the independent SLA watchdog.

    ``initial_due_at`` is only needed when a pipeline has not emitted any
    monitoring events yet. Use an ISO-8601 timestamp including a timezone.
    """

    pipeline: str
    max_silence_seconds: int
    facility: str | None = None
    initial_due_at: str | None = None
    severity: str = "high"

    def __post_init__(self) -> None:
        """Reject invalid policy values before the watchdog evaluates them."""
        if not self.pipeline.strip():
            raise ValueError("SLAPolicy.pipeline must not be blank")
        if self.max_silence_seconds <= 0:
            raise ValueError("SLAPolicy.max_silence_seconds must be positive")
        if self.initial_due_at:
            _parse_timestamp(self.initial_due_at)


@dataclass(frozen=True)
class WatchdogResult:
    """Outcome for one pipeline's heartbeat/SLA evaluation."""

    pipeline: str
    facility: str | None
    status: str
    last_signal_at: str | None
    overdue_seconds: int | None


class EventStore(Protocol):
    """Persistence interface used by monitors and the SLA watchdog."""

    def append(self, event: Mapping[str, Any]) -> None: ...

    def recent_metrics(
        self,
        pipeline: str,
        metric_name: str,
        *,
        facility: str | None = None,
        limit: int = 30,
    ) -> list[float]: ...

    def claim_alert(
        self, fingerprint: str, *, repeat_after_seconds: int = DEFAULT_REPEAT_ALERT_SECONDS
    ) -> bool: ...

    def resolve_alert(self, fingerprint: str) -> bool: ...

    def retry_alert(self, fingerprint: str, *, reopen: bool = False) -> None: ...

    def latest_signal(
        self, pipeline: str, facility: str | None = None
    ) -> str | None: ...


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


def _timestamp(value: datetime | None = None) -> str:
    """Format a datetime as an ISO-8601 UTC timestamp."""
    dt = value or _utc_now()
    if dt.tzinfo is None:
        raise ValueError("Timestamps must include a timezone")
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def _parse_timestamp(value: str) -> datetime:
    """Parse a timezone-aware ISO-8601 timestamp and normalize it to UTC."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp must include a timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


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


def _json(value: Any) -> str:
    """Serialize a value to compact, secret-redacted JSON."""
    return json.dumps(
        _safe_value(value), separators=(",", ":"), sort_keys=True, allow_nan=False
    )


def _safe_message(message: str) -> str:
    """Redact common inline secret assignments from event and alert messages."""
    return _SENSITIVE_TEXT.sub(r"\1\2" + _REDACTED, message)[:2000]


def _event_fingerprint(
    pipeline: str, facility: str | None, kind: str, name: str
) -> str:
    """Build a stable incident key without embedding a run-specific identifier."""
    return "|".join((pipeline, facility or "-", kind, name))


class SQLiteEventStore:
    """Append-only local event store plus deduplicated incident state.

    Use a database path on a persistent volume shared by pipeline workers and
    the watchdog if those components run in separate containers or processes.
    """

    def __init__(self, path: str | Path | None = None) -> None:
        """Create the store wrapper without opening the database."""
        configured = path or os.getenv("PIPELINE_MONITOR_DB_PATH")
        self.path = Path(configured).expanduser() if configured else (
            Path.home() / ".local" / "state" / "datapipelines-monitoring" / "events.sqlite3"
        )

    def _connect(self) -> sqlite3.Connection:
        """Open and initialize a SQLite connection with safe concurrency settings."""
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        connection = sqlite3.connect(self.path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 15000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS pipeline_events (
                event_id TEXT PRIMARY KEY,
                occurred_at TEXT NOT NULL,
                pipeline TEXT NOT NULL,
                run_id TEXT NOT NULL,
                facility TEXT,
                event_type TEXT NOT NULL,
                status TEXT NOT NULL,
                stage TEXT,
                message TEXT NOT NULL,
                data_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS pipeline_events_latest
                ON pipeline_events (pipeline, facility, occurred_at);
            CREATE TABLE IF NOT EXISTS pipeline_incidents (
                fingerprint TEXT PRIMARY KEY,
                active INTEGER NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_notified_at TEXT,
                resolved_at TEXT
            );
            CREATE TABLE IF NOT EXISTS pipeline_metrics (
                metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                occurred_at TEXT NOT NULL,
                pipeline TEXT NOT NULL,
                facility TEXT,
                metric_name TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT
            );
            CREATE INDEX IF NOT EXISTS pipeline_metrics_recent
                ON pipeline_metrics (pipeline, facility, metric_name, occurred_at);
            """
        )
        if os.name != "nt":
            self.path.chmod(0o600)
        return connection

    def append(self, event: Mapping[str, Any]) -> None:
        """Persist one event, raising on storage errors."""
        with closing(self._connect()) as connection, connection:
            connection.execute(
                """
                INSERT INTO pipeline_events (
                    event_id, occurred_at, pipeline, run_id, facility,
                    event_type, status, stage, message, data_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(event["event_id"]),
                    str(event["occurred_at"]),
                    str(event["pipeline"]),
                    str(event["run_id"]),
                    event.get("facility"),
                    str(event["event_type"]),
                    str(event["status"]),
                    event.get("stage"),
                    str(event.get("message", ""))[:2000],
                    _json(event.get("data", {})),
                ),
            )
            if event["event_type"] == "metric":
                metric = event.get("data", {})
                if (
                    not isinstance(metric, Mapping)
                    or not isinstance(metric.get("name"), str)
                    or isinstance(metric.get("value"), bool)
                    or not isinstance(metric.get("value"), (int, float))
                    or not math.isfinite(metric["value"])
                ):
                    raise ValueError("Metric events require a name and finite numeric value")
                connection.execute(
                    """
                    INSERT INTO pipeline_metrics (
                        event_id, occurred_at, pipeline, facility, metric_name, value, unit
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(event["event_id"]),
                        str(event["occurred_at"]),
                        str(event["pipeline"]),
                        event.get("facility"),
                        metric["name"],
                        float(metric["value"]),
                        metric.get("unit"),
                    ),
                )

    def recent_metrics(
        self,
        pipeline: str,
        metric_name: str,
        *,
        facility: str | None = None,
        limit: int = 30,
    ) -> list[float]:
        """Return recent metric values in chronological order for trend checks."""
        if limit < 1:
            raise ValueError("limit must be positive")
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT value
                FROM pipeline_metrics
                WHERE pipeline = ? AND metric_name = ?
                  AND ((facility IS NULL AND ? IS NULL) OR facility = ?)
                ORDER BY occurred_at DESC, metric_id DESC
                LIMIT ?
                """,
                (pipeline, metric_name, facility, facility, limit),
            ).fetchall()
        return [float(row["value"]) for row in reversed(rows)]

    def claim_alert(
        self, fingerprint: str, *, repeat_after_seconds: int = DEFAULT_REPEAT_ALERT_SECONDS
    ) -> bool:
        """Claim a new or repeat alert; return false when it should be suppressed."""
        now = _timestamp()
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT active, last_notified_at FROM pipeline_incidents WHERE fingerprint = ?",
                (fingerprint,),
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO pipeline_incidents
                        (fingerprint, active, first_seen_at, last_notified_at)
                    VALUES (?, 1, ?, ?)
                    """,
                    (fingerprint, now, now),
                )
                return True
            if not row["active"]:
                connection.execute(
                    """
                    UPDATE pipeline_incidents
                    SET active = 1, first_seen_at = ?, last_notified_at = ?, resolved_at = NULL
                    WHERE fingerprint = ?
                    """,
                    (now, now, fingerprint),
                )
                return True
            last_notified = row["last_notified_at"]
            if last_notified is None or (
                _parse_timestamp(now) - _parse_timestamp(last_notified)
            ).total_seconds() >= repeat_after_seconds:
                connection.execute(
                    "UPDATE pipeline_incidents SET last_notified_at = ? WHERE fingerprint = ?",
                    (now, fingerprint),
                )
                return True
            return False

    def resolve_alert(self, fingerprint: str) -> bool:
        """Resolve an active incident and return whether a recovery was recorded."""
        now = _timestamp()
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT active FROM pipeline_incidents WHERE fingerprint = ?",
                (fingerprint,),
            ).fetchone()
            if row is None or not row["active"]:
                return False
            connection.execute(
                """
                UPDATE pipeline_incidents
                SET active = 0, resolved_at = ?
                WHERE fingerprint = ?
                """,
                (now, fingerprint),
            )
            return True

    def retry_alert(self, fingerprint: str, *, reopen: bool = False) -> None:
        """Retry alert delivery promptly, reopening a recovery if it was undelivered."""
        with closing(self._connect()) as connection, connection:
            if reopen:
                connection.execute(
                    """
                    UPDATE pipeline_incidents
                    SET active = 1, last_notified_at = NULL, resolved_at = NULL
                    WHERE fingerprint = ?
                    """,
                    (fingerprint,),
                )
            else:
                connection.execute(
                    """
                    UPDATE pipeline_incidents
                    SET last_notified_at = NULL
                    WHERE fingerprint = ? AND active = 1
                    """,
                    (fingerprint,),
                )

    def latest_signal(
        self, pipeline: str, facility: str | None = None
    ) -> str | None:
        """Return the latest run-start, heartbeat, or completion timestamp."""
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT MAX(occurred_at) AS last_signal
                FROM pipeline_events
                WHERE pipeline = ?
                  AND event_type IN ('run_started', 'heartbeat', 'run_finished', 'failure')
                  AND ((facility IS NULL AND ? IS NULL) OR facility = ?)
                """,
                (pipeline, facility, facility),
            ).fetchone()
        return str(row["last_signal"]) if row and row["last_signal"] else None


class PostgresEventStore:
    """PostgreSQL event store using a schema separate from Airflow metadata."""

    schema = "pipeline_monitoring"
    _initialized_dsns: set[str] = set()
    _initialization_lock = threading.Lock()

    def __init__(self, dsn: str | None = None) -> None:
        """Create the store wrapper without opening a database connection."""
        self.dsn = dsn or os.getenv("PIPELINE_MONITOR_DATABASE_URL")
        if not self.dsn:
            raise ValueError("A PostgreSQL DSN is required")

    def _connect(self) -> Any:
        """Open and initialize a PostgreSQL connection."""
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as error:
            raise RuntimeError(
                "PostgreSQL monitoring requires psycopg; install project requirements"
            ) from error

        connection = psycopg.connect(self.dsn, row_factory=dict_row)
        try:
            with self._initialization_lock:
                if self.dsn not in self._initialized_dsns:
                    connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtext(%s))",
                        (f"{self.schema}_schema",),
                    )
                    self._initialize_schema(connection)
                    connection.commit()
                    self._initialized_dsns.add(self.dsn)
            connection.execute(f"SET search_path TO {self.schema}")
            connection.commit()
        except Exception:
            connection.close()
            raise
        return connection

    @classmethod
    def _initialize_schema(cls, connection: Any) -> None:
        """Create monitoring tables once, serializing startup across workers."""
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {cls.schema}")
        connection.execute(f"SET search_path TO {cls.schema}")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_events (
                event_id TEXT PRIMARY KEY,
                occurred_at TEXT NOT NULL,
                pipeline TEXT NOT NULL,
                run_id TEXT NOT NULL,
                facility TEXT,
                event_type TEXT NOT NULL,
                status TEXT NOT NULL,
                stage TEXT,
                message TEXT NOT NULL,
                data_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS pipeline_events_latest
                ON pipeline_events (pipeline, facility, occurred_at)
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_incidents (
                fingerprint TEXT PRIMARY KEY,
                active BOOLEAN NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_notified_at TEXT,
                resolved_at TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_metrics (
                metric_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                occurred_at TEXT NOT NULL,
                pipeline TEXT NOT NULL,
                facility TEXT,
                metric_name TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                unit TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS pipeline_metrics_recent
                ON pipeline_metrics (pipeline, facility, metric_name, occurred_at)
            """
        )

    def append(self, event: Mapping[str, Any]) -> None:
        """Persist one event, raising on storage errors."""
        with closing(self._connect()) as connection, connection:
            connection.execute(
                """
                INSERT INTO pipeline_events (
                    event_id, occurred_at, pipeline, run_id, facility,
                    event_type, status, stage, message, data_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(event["event_id"]),
                    str(event["occurred_at"]),
                    str(event["pipeline"]),
                    str(event["run_id"]),
                    event.get("facility"),
                    str(event["event_type"]),
                    str(event["status"]),
                    event.get("stage"),
                    str(event.get("message", ""))[:2000],
                    _json(event.get("data", {})),
                ),
            )
            if event["event_type"] == "metric":
                metric = event.get("data", {})
                if (
                    not isinstance(metric, Mapping)
                    or not isinstance(metric.get("name"), str)
                    or isinstance(metric.get("value"), bool)
                    or not isinstance(metric.get("value"), (int, float))
                    or not math.isfinite(metric["value"])
                ):
                    raise ValueError("Metric events require a name and finite numeric value")
                connection.execute(
                    """
                    INSERT INTO pipeline_metrics (
                        event_id, occurred_at, pipeline, facility, metric_name, value, unit
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(event["event_id"]),
                        str(event["occurred_at"]),
                        str(event["pipeline"]),
                        event.get("facility"),
                        metric["name"],
                        float(metric["value"]),
                        metric.get("unit"),
                    ),
                )

    def recent_metrics(
        self,
        pipeline: str,
        metric_name: str,
        *,
        facility: str | None = None,
        limit: int = 30,
    ) -> list[float]:
        """Return recent metric values in chronological order for trend checks."""
        if limit < 1:
            raise ValueError("limit must be positive")
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT value
                FROM pipeline_metrics
                WHERE pipeline = %s AND metric_name = %s
                  AND facility IS NOT DISTINCT FROM %s
                ORDER BY occurred_at DESC, metric_id DESC
                LIMIT %s
                """,
                (pipeline, metric_name, facility, limit),
            ).fetchall()
        return [float(row["value"]) for row in reversed(rows)]

    def claim_alert(
        self, fingerprint: str, *, repeat_after_seconds: int = DEFAULT_REPEAT_ALERT_SECONDS
    ) -> bool:
        """Claim a new or repeat alert atomically across PostgreSQL clients."""
        now = _timestamp()
        with closing(self._connect()) as connection, connection:
            inserted = connection.execute(
                """
                INSERT INTO pipeline_incidents
                    (fingerprint, active, first_seen_at, last_notified_at)
                VALUES (%s, TRUE, %s, %s)
                ON CONFLICT (fingerprint) DO NOTHING
                """,
                (fingerprint, now, now),
            )
            if inserted.rowcount:
                return True
            row = connection.execute(
                """
                SELECT active, last_notified_at
                FROM pipeline_incidents
                WHERE fingerprint = %s
                FOR UPDATE
                """,
                (fingerprint,),
            ).fetchone()
            if row is None:
                raise RuntimeError("Incident row disappeared while claiming an alert")
            if not row["active"]:
                connection.execute(
                    """
                    UPDATE pipeline_incidents
                    SET active = TRUE, first_seen_at = %s, last_notified_at = %s,
                        resolved_at = NULL
                    WHERE fingerprint = %s
                    """,
                    (now, now, fingerprint),
                )
                return True
            last_notified = row["last_notified_at"]
            if last_notified is None or (
                _parse_timestamp(now) - _parse_timestamp(last_notified)
            ).total_seconds() >= repeat_after_seconds:
                connection.execute(
                    "UPDATE pipeline_incidents SET last_notified_at = %s "
                    "WHERE fingerprint = %s",
                    (now, fingerprint),
                )
                return True
            return False

    def resolve_alert(self, fingerprint: str) -> bool:
        """Resolve an active incident and return whether a recovery was recorded."""
        now = _timestamp()
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                """
                SELECT active FROM pipeline_incidents
                WHERE fingerprint = %s
                FOR UPDATE
                """,
                (fingerprint,),
            ).fetchone()
            if row is None or not row["active"]:
                return False
            connection.execute(
                """
                UPDATE pipeline_incidents
                SET active = FALSE, resolved_at = %s
                WHERE fingerprint = %s
                """,
                (now, fingerprint),
            )
            return True

    def retry_alert(self, fingerprint: str, *, reopen: bool = False) -> None:
        """Retry alert delivery promptly, reopening an undelivered recovery."""
        with closing(self._connect()) as connection, connection:
            if reopen:
                connection.execute(
                    """
                    UPDATE pipeline_incidents
                    SET active = TRUE, last_notified_at = NULL, resolved_at = NULL
                    WHERE fingerprint = %s
                    """,
                    (fingerprint,),
                )
            else:
                connection.execute(
                    """
                    UPDATE pipeline_incidents
                    SET last_notified_at = NULL
                    WHERE fingerprint = %s AND active = TRUE
                    """,
                    (fingerprint,),
                )

    def latest_signal(
        self, pipeline: str, facility: str | None = None
    ) -> str | None:
        """Return the latest run-start, heartbeat, or completion timestamp."""
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT MAX(occurred_at) AS last_signal
                FROM pipeline_events
                WHERE pipeline = %s
                  AND event_type IN ('run_started', 'heartbeat', 'run_finished', 'failure')
                  AND facility IS NOT DISTINCT FROM %s
                """,
                (pipeline, facility),
            ).fetchone()
        return str(row["last_signal"]) if row and row["last_signal"] else None


def default_event_store() -> EventStore:
    """Use PostgreSQL when configured, otherwise retain the local SQLite default."""
    if os.getenv("PIPELINE_MONITOR_DATABASE_URL"):
        return PostgresEventStore()
    return SQLiteEventStore()


class WebhookNotifier:
    """Send JSON alerts to a generic HTTP webhook without third-party packages."""

    def __init__(self, url: str, *, timeout_seconds: float = 10.0, attempts: int = 3) -> None:
        """Validate and retain webhook transport settings."""
        if not url.startswith(("https://", "http://")):
            raise ValueError("Webhook URL must use HTTP or HTTPS")
        if timeout_seconds <= 0 or attempts < 1:
            raise ValueError("Webhook timeout and attempt count must be positive")
        self.url = url
        self.timeout_seconds = timeout_seconds
        self.attempts = attempts

    def send(self, payload: Mapping[str, Any]) -> None:
        """POST one JSON payload, retrying transient HTTP/network failures."""
        body = _json(payload).encode("utf-8")
        request = urllib.request.Request(
            self.url,
            data=body,
            headers={"Content-Type": "application/json", "User-Agent": "pipeline-monitoring/1"},
            method="POST",
        )
        for attempt in range(1, self.attempts + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    if not 200 <= response.status < 300:
                        raise MonitoringError(f"Alert webhook returned HTTP {response.status}")
                return
            except urllib.error.HTTPError as error:
                if error.code < 500 and error.code not in {408, 425, 429}:
                    raise MonitoringError(
                        f"Alert webhook rejected the request with HTTP {error.code}"
                    ) from error
                if attempt == self.attempts:
                    raise MonitoringError(
                        f"Alert webhook failed after {attempt} attempts "
                        f"(HTTP {error.code})"
                    ) from error
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                if attempt == self.attempts:
                    raise MonitoringError(
                        f"Alert webhook delivery failed after {attempt} attempts "
                        f"({type(error).__name__})"
                    ) from error
            time.sleep(min(2 ** (attempt - 1), 8))


@dataclass
class PipelineMonitor:
    """Opt-in run/event recorder with quality checks and alert deduplication.

    By default, monitoring failures are logged explicitly but do not replace
    the pipeline's own result. Set ``fail_open=False`` to raise
    :class:`MonitoringError` when persistence or notification fails.
    """

    pipeline: str
    run_id: str | None = None
    facility: str | None = None
    store: EventStore | None = None
    webhook: WebhookNotifier | None = None
    fail_open: bool = True
    repeat_alert_seconds: int = DEFAULT_REPEAT_ALERT_SECONDS
    _finished: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        """Resolve defaults and validate monitor identity."""
        self.pipeline = self.pipeline.strip()
        if not self.pipeline:
            raise ValueError("pipeline must not be blank")
        self.run_id = self.run_id or str(uuid.uuid4())
        self.store = self.store or default_event_store()
        if self.webhook is None:
            webhook_url = os.getenv("PIPELINE_MONITOR_WEBHOOK_URL", "").strip()
            if webhook_url:
                self.webhook = WebhookNotifier(webhook_url)
        if self.repeat_alert_seconds < 0:
            raise ValueError("repeat_alert_seconds cannot be negative")

    def _persist(self, event: Mapping[str, Any]) -> bool:
        """Write an event and honor the configured monitoring failure policy."""
        try:
            store = self.store
            if store is None:
                raise RuntimeError("PipelineMonitor store was not initialized")
            store.append(event)
            return True
        except Exception as error:
            if not self.fail_open:
                raise MonitoringError(
                    f"Could not persist monitoring event ({type(error).__name__})"
                ) from error
            LOGGER.exception("Could not persist monitoring event for %s", self.pipeline)
            return False

    def _alert(
        self,
        *,
        fingerprint: str,
        event: Mapping[str, Any],
        severity: str,
        recovery: bool = False,
    ) -> None:
        """Send a deduplicated alert or recovery notification."""
        if self.store is None:
            raise RuntimeError("PipelineMonitor store was not initialized")
        store = self.store
        try:
            should_send = (
                store.resolve_alert(fingerprint)
                if recovery
                else store.claim_alert(
                    fingerprint, repeat_after_seconds=self.repeat_alert_seconds
                )
            )
        except Exception as error:
            if not self.fail_open:
                raise MonitoringError(
                    f"Could not update incident state ({type(error).__name__})"
                ) from error
            LOGGER.exception("Could not update incident state for %s", fingerprint)
            should_send = not recovery
        if not should_send:
            return
        alert_payload = {
            "alert_type": "recovery" if recovery else "pipeline_incident",
            "severity": severity,
            "fingerprint": fingerprint,
            "pipeline": self.pipeline,
            "run_id": self.run_id,
            "facility": self.facility,
            "occurred_at": event["occurred_at"],
            "event_type": event["event_type"],
            "status": event["status"],
            "stage": event.get("stage"),
            "message": _safe_message(str(event.get("message", ""))),
            "data": event.get("data", {}),
        }
        if self.webhook is None:
            LOGGER.warning(
                "Monitoring alert has no webhook destination: %s (%s)",
                fingerprint,
                alert_payload["message"],
            )
            try:
                store.retry_alert(fingerprint, reopen=recovery)
            except Exception:
                LOGGER.exception("Could not leave alert %s eligible for later delivery", fingerprint)
            return
        try:
            self.webhook.send(alert_payload)
        except MonitoringError:
            if self.fail_open:
                LOGGER.exception("Could not deliver monitoring alert %s", fingerprint)
            try:
                store.retry_alert(fingerprint, reopen=recovery)
            except Exception as error:
                if not self.fail_open:
                    raise MonitoringError(
                        f"Could not restore incident state after delivery failure "
                        f"({type(error).__name__})"
                    ) from error
                LOGGER.exception("Could not make alert %s eligible for retry", fingerprint)
            if not self.fail_open:
                raise

    def emit(
        self,
        event_type: str,
        *,
        status: str = "info",
        stage: str | None = None,
        message: str = "",
        data: Mapping[str, Any] | None = None,
        occurred_at: datetime | None = None,
    ) -> dict[str, Any]:
        """Persist a structured event and return its JSON-safe representation."""
        if not event_type.strip():
            raise ValueError("event_type must not be blank")
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
            "data": _safe_value(data or {}),
        }
        self._persist(event)
        return event

    def start(self, *, metadata: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Record a pipeline run start."""
        return self.emit("run_started", status="running", data=metadata)

    def heartbeat(
        self, *, stage: str | None = None, metadata: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        """Record liveness while a long-running pipeline is still active."""
        return self.emit("heartbeat", status="running", stage=stage, data=metadata)

    def stage(
        self,
        name: str,
        *,
        status: str,
        message: str = "",
        metrics: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Record a stage transition and alert when the stage has failed."""
        if status not in {"running", "succeeded", "failed", "skipped"}:
            raise ValueError("Stage status must be running, succeeded, failed, or skipped")
        event = self.emit(
            "stage",
            status=status,
            stage=name,
            message=message,
            data=metrics,
        )
        fingerprint = _event_fingerprint(self.pipeline, self.facility, "stage", name)
        if status == "failed":
            self._alert(fingerprint=fingerprint, event=event, severity="high")
        elif status == "succeeded":
            self._alert(fingerprint=fingerprint, event=event, severity="high", recovery=True)
        return event

    def metric(
        self, name: str, value: int | float, *, unit: str | None = None, stage: str | None = None
    ) -> dict[str, Any]:
        """Record a numeric operational metric such as extracted row count."""
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
        """Record a quality result, alert on failure, and notify on recovery."""
        event = self.emit(
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
        fingerprint = _event_fingerprint(
            self.pipeline, self.facility, "quality", result.name
        )
        self._alert(
            fingerprint=fingerprint,
            event=event,
            severity=result.severity,
            recovery=result.passed,
        )
        return event

    def failure(
        self,
        message: str,
        *,
        stage: str | None = None,
        severity: str = "high",
        data: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Record a terminal failure and emit a deduplicated alert."""
        event = self.emit(
            "failure", status="failed", stage=stage, message=message, data=data
        )
        fingerprint = _event_fingerprint(
            self.pipeline, self.facility, "failure", stage or "pipeline"
        )
        self._alert(fingerprint=fingerprint, event=event, severity=severity)
        return event

    def finish(
        self,
        status: str,
        *,
        message: str = "",
        summary: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Record run completion, alert on failure, and prevent double finish."""
        if status not in {"succeeded", "failed", "skipped"}:
            raise ValueError("Run status must be succeeded, failed, or skipped")
        if self._finished:
            raise RuntimeError("This monitor has already recorded run completion")
        event = self.emit(
            "run_finished",
            status=status,
            message=message,
            data=summary,
        )
        self._finished = True
        fingerprint = _event_fingerprint(
            self.pipeline, self.facility, "failure", "pipeline"
        )
        if status == "failed":
            self._alert(fingerprint=fingerprint, event=event, severity="critical")
        elif status == "succeeded":
            self._alert(
                fingerprint=fingerprint, event=event, severity="critical", recovery=True
            )
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
        """Record run outcome on context exit without suppressing pipeline errors."""
        if not self._finished:
            if exc_value is None:
                self.finish("succeeded")
            else:
                self.finish(
                    "failed",
                    message=f"{exc_type.__name__ if exc_type else 'Error'}: {exc_value}",
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
    """Check absolute relative metric change against a historical baseline.

    If the baseline is zero, zero-to-zero passes and any non-zero value fails;
    callers should establish a suitable baseline before enabling this check.
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


def _build_monitor(
    pipeline: str,
    *,
    facility: str | None,
    store: EventStore,
    webhook: WebhookNotifier | None,
) -> PipelineMonitor:
    """Create a fail-open monitor for the watchdog."""
    return PipelineMonitor(
        pipeline=pipeline,
        run_id=f"watchdog-{uuid.uuid4()}",
        facility=facility,
        store=store,
        webhook=webhook,
    )


def run_watchdog(
    policies: Sequence[SLAPolicy],
    *,
    now: datetime | None = None,
    store: EventStore | None = None,
    webhook: WebhookNotifier | None = None,
) -> list[WatchdogResult]:
    """Evaluate expected pipeline heartbeats, emit events, and alert on breaches.

    Policies without previous events are ignored until ``initial_due_at`` is
    configured and has passed. A result with ``status='breached'`` is an SLA
    violation; callers can use it to fail an Airflow watchdog task if desired.
    """
    current = now or _utc_now()
    if current.tzinfo is None:
        raise ValueError("now must include a timezone")
    current = current.astimezone(timezone.utc)
    event_store = store or default_event_store()
    notifier = webhook
    if notifier is None:
        webhook_url = os.getenv("PIPELINE_MONITOR_WEBHOOK_URL", "").strip()
        notifier = WebhookNotifier(webhook_url) if webhook_url else None
    results: list[WatchdogResult] = []

    for policy in policies:
        monitor = _build_monitor(
            policy.pipeline, facility=policy.facility, store=event_store, webhook=notifier
        )
        last_signal = event_store.latest_signal(policy.pipeline, policy.facility)
        breach = False
        overdue_seconds: int | None = None
        if last_signal is not None:
            elapsed = (current - _parse_timestamp(last_signal)).total_seconds()
            overdue_seconds = max(0, int(elapsed - policy.max_silence_seconds))
            breach = elapsed > policy.max_silence_seconds
        elif policy.initial_due_at is not None:
            due_at = _parse_timestamp(policy.initial_due_at)
            if current > due_at:
                breach = True
                overdue_seconds = int((current - due_at).total_seconds())

        result = WatchdogResult(
            pipeline=policy.pipeline,
            facility=policy.facility,
            status="breached" if breach else ("ok" if last_signal else "not_due"),
            last_signal_at=last_signal,
            overdue_seconds=overdue_seconds,
        )
        results.append(result)
        event = monitor.emit(
            "sla_check",
            status=result.status,
            stage="watchdog",
            message=(
                f"No signal for {overdue_seconds} seconds beyond the expected interval"
                if breach
                else (
                    "Heartbeat is within the configured SLA"
                    if result.status == "ok"
                    else "No heartbeat check is due yet"
                )
            ),
            data={
                "max_silence_seconds": policy.max_silence_seconds,
                "last_signal_at": last_signal,
                "overdue_seconds": overdue_seconds,
            },
            occurred_at=current,
        )
        fingerprint = _event_fingerprint(
            policy.pipeline, policy.facility, "sla", "heartbeat"
        )
        if breach or result.status == "ok":
            monitor._alert(
                fingerprint=fingerprint,
                event=event,
                severity=policy.severity,
                recovery=not breach,
            )
    return results


def airflow_failure_callback(context: Mapping[str, Any]) -> None:
    """Record an Airflow task failure using the standard callback signature.

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
        if exception is not None
        else "Airflow task failed"
    )
    monitor.failure(message, stage=str(task_id) if task_id else None)


def _load_policies(path: Path) -> list[SLAPolicy]:
    """Load SLA policy objects from a JSON file."""
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, list):
        raise ValueError("The SLA policy file must contain a JSON array")
    policies: list[SLAPolicy] = []
    for index, item in enumerate(document):
        if not isinstance(item, dict):
            raise ValueError(f"SLA policy {index} must be a JSON object")
        policies.append(SLAPolicy(**item))
    return policies


def main(argv: Sequence[str] | None = None) -> int:
    """Run the independent SLA watchdog CLI and return its process exit code."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    watchdog_parser = subparsers.add_parser(
        "watchdog", help="Evaluate pipeline heartbeat policies once"
    )
    watchdog_parser.add_argument(
        "--policies", type=Path, required=True, help="JSON file containing SLA policies"
    )
    watchdog_parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help=(
            "SQLite event database path (default: PostgreSQL from "
            "PIPELINE_MONITOR_DATABASE_URL, otherwise local SQLite)"
        ),
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if args.command == "watchdog":
        policies = _load_policies(args.policies)
        store = SQLiteEventStore(args.db) if args.db else default_event_store()
        results = run_watchdog(policies, store=store)
        for result in results:
            print(
                f"{result.status:8} pipeline={result.pipeline} facility={result.facility or '-'} "
                f"last_signal={result.last_signal_at or '-'} "
                f"overdue_seconds={result.overdue_seconds if result.overdue_seconds is not None else '-'}"
            )
        return 1 if any(result.status == "breached" for result in results) else 0
    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
