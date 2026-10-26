# Sentry pipeline monitoring

`pipeline_monitoring.py` is an opt-in helper for pipeline scripts and Airflow
DAGs. It emits structured events through Python's standard `logging` module.
When Sentry is configured, the Sentry Python SDK forwards these logs directly
to Sentry; monitoring events are not written to a local or Airflow database.

The module does not discover or instrument every existing pipeline by itself.
Import it from the pipeline or callback code that should emit monitoring
events.

## Setup

Install the Sentry SDK in the same Python environment used by Airflow workers:

```bash
pip install "sentry-sdk>=2.35,<3"
```

Set a Sentry project DSN in the Airflow task environment. For Docker Compose,
the senior engineer should add this to the shared `x-airflow-common`
`environment` block in both `docker-compose.yaml` and
`docker-compose.dev.yaml`:

```yaml
SENTRY_DSN: ${SENTRY_DSN:-}
SENTRY_ENVIRONMENT: ${SENTRY_ENVIRONMENT:-development}
```

Provide the actual DSN through the deployment environment or a secret manager;
do not commit it to the repository. `SENTRY_ENVIRONMENT` and
`SENTRY_RELEASE` are optional. Sentry initialization disables default PII
collection.

After adding the SDK dependency, rebuild the Airflow image. After adding the
environment entries, pass `SENTRY_DSN` through the environment or a
non-committed `.env` file before starting Compose.

When `pipeline_monitoring` is imported with `SENTRY_DSN` set, it initializes
the Sentry Python SDK's logging integration. To forward general Airflow
Python logs in an Airflow process, the senior engineer can add this call to
`config/airflow_local_settings.py` (ensure the repository root is on
`PYTHONPATH`):

```python
from pipeline_monitoring import configure_sentry

configure_sentry()
```

This startup-hook change is optional for pipelines that import
`pipeline_monitoring` themselves; it is needed to initialize Sentry for
general Airflow logs that are emitted before an instrumented task imports the
module.

See the official Sentry documentation for
[Python logging](https://docs.sentry.io/platforms/python/integrations/logging/)
and [Cron monitoring](https://docs.sentry.io/platforms/python/crons/).

## What is sent

The Sentry logging integration is configured to:

- Send `INFO` and higher log records to Sentry Logs.
- Create Sentry issue events for `ERROR` and higher records.
- Capture `INFO` and higher breadcrumbs on error events.
- Exclude default PII collection.

Pipeline log records include searchable attributes for pipeline, run ID,
facility, event type, status, stage, timestamp, and structured event data.
Heartbeats are `DEBUG` records so they do not fill the default `INFO` Sentry
log stream. If debug logging is enabled, they can be emitted locally.

The helper redacts values under keys resembling `password`, `secret`, `token`,
`authorization`, `api_key`, and `private_key`, and masks common inline secret
assignments in messages. Redaction is a safeguard, not a guarantee: **never
include patient data, credentials, full source rows, or other sensitive
information in messages or event metadata.**

Without `SENTRY_DSN`, events are still emitted through normal Python logging,
but are not sent to Sentry. Sentry initialization is fail-open by default;
set `fail_open=False` on `PipelineMonitor` to raise `MonitoringError` when
SDK initialization fails.

## Instrument a pipeline

Use the context manager for a whole run. It logs run start and completion,
records an exception with its traceback when one escapes, and does not swallow
pipeline exceptions:

```python
from pipeline_monitoring import PipelineMonitor, check_row_count

with PipelineMonitor(
    pipeline="nightly_patient_ingest",
    run_id="airflow-run-2026-10-23T02:00:00Z",
    facility="clinic_a",
) as monitor:
    monitor.stage("extract", status="running")
    rows = extract_rows()
    monitor.metric("source_rows", len(rows), stage="extract")
    monitor.stage("extract", status="succeeded", metrics={"rows": len(rows)})

    result = check_row_count("source_rows_present", len(rows), minimum=1)
    monitor.quality(result, stage="extract")
    if not result.passed:
        raise RuntimeError("Source row count did not meet the expected minimum")

    monitor.stage("load", status="running")
    load_rows(rows)
    monitor.stage("load", status="succeeded", metrics={"rows_loaded": len(rows)})
```

Event methods include:

- `start(metadata=...)` and `finish(status, summary=...)` for run lifecycle.
- `stage(name, status=..., message=..., metrics=...)` for stage transitions.
- `heartbeat(stage=..., metadata=...)` for debug-level liveness messages.
- `metric(name, value, unit=..., stage=...)` for numeric operational values.
- `quality(result, stage=...)` for data-quality outcomes.
- `failure(message, stage=..., exception=...)` for explicit errors.

Failed stages, failed quality checks, failed runs, and calls to `failure()`
are logged at `ERROR` level. Pass an exception to `failure()` to include its
traceback.

## Airflow failure callback

Add the callback to a DAG or task that should report task failures:

```python
from pipeline_monitoring import airflow_failure_callback

with DAG(
    dag_id="my_pipeline",
    # Other DAG arguments...
    on_failure_callback=airflow_failure_callback,
) as dag:
    ...
```

The callback logs the DAG ID, task ID, run ID, optional
`params["facility"]`, and exception traceback. It does not copy task arguments
or XCom payloads into the log.

## Sentry Cron monitoring

Sentry Cron monitoring can alert on missed scheduled runs and runs that exceed
the configured maximum runtime. Opt in per scheduled pipeline by passing a
Sentry monitor configuration to `PipelineMonitor`:

```python
with PipelineMonitor(
    pipeline="hourly_invoice_sync",
    cron_monitor_config={
        "schedule": {"type": "crontab", "value": "0 * * * *"},
        "timezone": "UTC",
        "checkin_margin": 5,
        "max_runtime": 30,
    },
) as monitor:
    run_invoice_sync()
```

The pipeline name is used as the Sentry monitor slug. The helper sends an
`IN_PROGRESS` check-in at `start()` and completes it as `OK` or `ERROR` at
`finish()`. The same slug must always refer to the same scheduled job and
schedule. For an existing Sentry Cron monitor, the configuration may be
omitted if Sentry already knows its schedule; a monitor configuration creates
or updates the monitor from code. Cron check-ins are only sent when the Sentry
SDK is initialized with a DSN.

## Data-quality checks

The helper includes checks for row counts, null rates, duplicate counts,
metric changes, schemas, and referential integrity. Each returns an immutable
`QualityResult`; pass it to `monitor.quality(result)` to log it and send failed
checks to Sentry as error events:

```python
monitor.quality(check_row_count("target_rows", actual, minimum=expected_min))
monitor.quality(
    check_null_rate(
        "patient_id_null_rate",
        null_count=missing_patient_ids,
        total_count=rows_checked,
        max_rate=0.005,
    )
)
monitor.quality(check_duplicates("duplicate_visit_ids", duplicate_count))
monitor.quality(
    check_schema(
        "warehouse_schema",
        expected_columns=["id", "visit_id", "created_at"],
        actual_columns=warehouse_columns,
        allow_extra=False,
    )
)
monitor.quality(
    check_referential_integrity(
        "orphan_visit_references",
        orphan_count=orphan_rows,
        child_count=rows_checked,
        max_orphan_rate=0,
    )
)
```

`check_metric_change` accepts the current value and a baseline supplied by
the caller. The helper does not retain metric history; use an existing
warehouse, metrics service, or other approved source to calculate baselines.

## Limits and deployment notes

Sentry receives events as the application logs them; this module does not
persist a second copy or replay events after an outage. Sentry retention,
project quotas, alert rules, and issue grouping are managed in Sentry. Configure
Sentry alerts for error events and Cron monitor failures in the Sentry project.

Sentry logging integration captures records only in processes where the SDK
has been initialized. Importing this helper from a DAG makes it available to
that code, but does not automatically instrument every unrelated task or
Airflow component. Initialize it from Airflow's shared startup/logging
configuration if centralized capture of general Airflow logs is required.

The helper does not monitor pipelines that do not import it, and it cannot
infer data-quality expectations. Add instrumentation and explicit checks to
the pipeline code whose behavior you want monitored.
