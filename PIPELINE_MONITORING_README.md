# Reusable pipeline monitoring

`pipeline_monitoring.py` is an **opt-in helper module** for new or intentionally
instrumented pipeline code. It is independent of the existing pipelines and
DAGs: importing it does not patch, discover, or change any existing task.

Its features include:

- Structured run, stage, metric, heartbeat, quality-check, and SLA events.
- A PostgreSQL event and incident store for Airflow deployments, with a local
  SQLite fallback for development outside Compose.
- Deduplicated incident alerts and recovery notifications.
- A generic JSON webhook notifier with bounded retries.
- Reusable row-count, null-rate, duplicate, schema, and referential-integrity
  checks, plus relative metric-change checks against historical values.
- An independent heartbeat/SLA watchdog that can be run as a CLI command or
  from a scheduled Airflow task.
- An Airflow-compatible task failure callback.

## Where files live and importing

Keep `pipeline_monitoring.py` in the repository root. In the current Airflow
deployment, the repository is mounted at `/opt/airflow/pipelines`; that root
is on `sys.path` for pipeline scripts that use `pipelines_common.use_pipelines_dir`.
For a DAG that does not use that helper, put the repository root on `sys.path`
using the project’s normal deployment/import convention before importing:

```python
from pipeline_monitoring import (
    PipelineMonitor,
    airflow_failure_callback,
    check_duplicates,
    check_metric_change,
    check_null_rate,
    check_referential_integrity,
    check_row_count,
    check_schema,
)
```

The import itself does not open a database or make a network request. Those
operations occur when you create a monitor and record events.

## Configuration

| Setting | Required | Description |
| --- | --- | --- |
| `PIPELINE_MONITOR_DATABASE_URL` | No | PostgreSQL connection URL. When set, the helper uses the `pipeline_monitoring` schema on that server (separate from Airflow's metadata tables). Compose points this at its PostgreSQL service by default. |
| `PIPELINE_MONITOR_DB_PATH` | No | SQLite file path used when no PostgreSQL URL is set. Defaults to `~/.local/state/datapipelines-monitoring/events.sqlite3`. |
| `PIPELINE_MONITOR_WEBHOOK_URL` | No | HTTP(S) endpoint that accepts the JSON alert payload. Keep credentials in the webhook provider’s supported secret mechanism; do not put secrets in source code or policy files. |

The database stores operational metadata, not source rows. The helper redacts
values under keys resembling `password`, `secret`, `token`, `authorization`,
`api_key`, and `private_key`; nevertheless, **do not send patient data,
credentials, row payloads, or other sensitive data as event messages or
metadata**.

By default, monitoring is **fail-open**: if persistence or alert delivery
fails, the helper emits an explicit log error and does not replace the
pipeline’s result. Use `fail_open=False` when the caller explicitly wants a
monitoring failure to raise `MonitoringError`. Webhook delivery is retried up
to three times for transient errors. Alerts with the same pipeline, facility,
and issue are deduplicated; the default repeat notification interval is one
hour. A passing check or successful stage/run sends a recovery notification
for its corresponding active incident.

If no webhook is configured, events are still stored and alert-worthy
conditions are logged as warnings. Configure a real operations destination
before relying on notifications.

## Instrument a script or task

Use the context manager for a whole run. It writes a start event and marks the
run successful when the block returns normally, or failed when an exception
leaves the block. Exceptions are **not swallowed**.

```python
from pipeline_monitoring import PipelineMonitor, check_row_count

with PipelineMonitor(
    pipeline="nightly_patient_ingest",
    run_id="airflow-run-2026-10-23T02:00:00Z",
    facility="clinic_a",
) as monitor:
    monitor.stage("extract", status="running")
    rows = extract_rows() #This function is a placeholder
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

Call `monitor.heartbeat(stage="load")` periodically inside a long-running
operation if that operation may exceed the configured SLA interval. A heartbeat
means the pipeline is alive; it does not assert the stage has succeeded.

You can also create a monitor without a context manager:

```python
monitor = PipelineMonitor("orders_to_warehouse", facility="clinic_a")
monitor.start(metadata={"trigger": "hourly"})
monitor.stage("copy", status="failed", message="Warehouse COPY failed")
monitor.finish("failed", message="Copy stage failed", summary={"rows": 0})
```

`finish` accepts `succeeded`, `failed`, or `skipped` and may be called only
once on a monitor instance. A failed stage/run produces an alert; a later
successful stage/run with the same incident key produces a recovery.

### Airflow failure callback

Add the callback to a new or intentionally updated DAG/task. This does not
change retry, trigger, or task execution behavior:

```python
from pipeline_monitoring import airflow_failure_callback

with DAG(
    dag_id="my_new_pipeline",
    # Other DAG arguments...
    on_failure_callback=airflow_failure_callback,
) as dag:
    ...
```

The callback reads DAG ID, task ID, run ID, an optional `params["facility"]`,
and the exception summary. It does not serialize task arguments or XCom values.

## Data-quality checks

Each check returns an immutable `QualityResult`; pass it to
`monitor.quality(result)` to persist it and trigger an alert/recovery:

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

The caller obtains counts and schema using the pipeline’s existing source,
warehouse, or API clients. The module intentionally does not connect to
Snowflake, source databases, or V3 on behalf of the pipeline.

### Compare metrics with recent history

`monitor.metric(...)` also stores numeric values in a metric history table.
Before recording the current run’s metric, retrieve a baseline and apply a
pipeline-specific tolerance:

```python
from statistics import median
from pipeline_monitoring import default_event_store, check_metric_change

history = default_event_store().recent_metrics(
    "nightly_patient_ingest",
    "source_rows",
    facility="clinic_a",
    limit=30,
)
if len(history) >= 5:
    monitor.quality(
        check_metric_change(
            "source_row_count_drift",
            actual=current_row_count,
            baseline=median(history),
            max_change_fraction=0.40,
        ),
        stage="extract",
    )
monitor.metric("source_rows", current_row_count, unit="records", stage="extract")
```

The change check measures absolute relative deviation. A zero baseline passes
only when the current value is also zero; establish a non-zero history before
using relative thresholds for datasets that may be new or sparse.

## Independent SLA watchdog

`run_watchdog` checks the most recent `run_started`, `heartbeat`,
`run_finished`, or `failure` event for each pipeline/facility. Run it from a
separate scheduled Airflow DAG or an external scheduler. That separation lets
the watchdog report a pipeline that has stopped starting or sending
heartbeats.

Create a JSON policy file, for example `config/pipeline_sla_policies.json`:

```json
[
  {
    "pipeline": "nightly_patient_ingest",
    "facility": "clinic_a",
    "max_silence_seconds": 5400,
    "initial_due_at": "2026-10-24T01:30:00Z",
    "severity": "critical"
  },
  {
    "pipeline": "hourly_invoice_sync",
    "max_silence_seconds": 5400
  }
]
```

`initial_due_at` is optional. If there are no events and it is omitted, the
watchdog reports `not_due` rather than raising a false alarm. If configured,
it starts alerting after that first expected signal time. For events already
being recorded, `max_silence_seconds` is measured from the most recent signal.
Run heartbeats often enough that normal long-running jobs do not breach the
configured interval.

Run the watchdog once:

```bash
python pipeline_monitoring.py watchdog \
  --policies config/pipeline_sla_policies.json
```

In Compose, the watchdog uses PostgreSQL automatically. Outside Compose,
configure `PIPELINE_MONITOR_DATABASE_URL` to use PostgreSQL or set
`PIPELINE_MONITOR_DB_PATH` to choose a SQLite file:

```bash
python pipeline_monitoring.py watchdog \
  --policies config/pipeline_sla_policies.json
```

The command prints one result per policy and exits `1` if any policy is
`breached`, otherwise `0`. This makes it suitable for an Airflow
`BashOperator`/`PythonOperator` or a system scheduler. When used in Airflow, a
non-zero command status fails the watchdog task; use that DAG’s normal
`on_failure_callback` if you also want Airflow-level alerting.

The CLI arguments are:

- `watchdog`: evaluate the supplied policies once.
- `--policies PATH` (required): JSON array of `SLAPolicy` objects.
- `--db PATH` (optional): explicitly use a SQLite database path, overriding
  the PostgreSQL URL if one is configured.

## Event store and operational deployment

PostgreSQL stores monitoring data in the `pipeline_monitoring` schema, separate
from Airflow's metadata tables. It contains:

- `pipeline_events`: append-only event history.
- `pipeline_incidents`: active/resolved state used to deduplicate alerts.
- `pipeline_metrics`: numeric values from `monitor.metric(...)`, queryable
  through `default_event_store().recent_metrics(...)`.

### Deployment changes required

The following deployment-file changes are required for PostgreSQL monitoring.
They must be applied to **both** `docker-compose.yaml` and
`docker-compose.dev.yaml`:

1. Under the shared `x-airflow-common` configuration, enable the Dockerfile
   build so the Airflow image installs the Python driver from `requirements.txt`:

   ```yaml
   image: ${AIRFLOW_IMAGE_NAME:-customairflow:latest}
   build: .
   ```

   In `docker-compose.yaml` and `docker-compose.dev.yaml`, replace the existing
   `# build: .` line with `build: .`.

2. Add the monitoring database URL under the shared `environment` block, next
   to the existing Airflow database settings. This gives Airflow services the
   same monitoring database configuration:

   ```yaml
   PIPELINE_MONITOR_DATABASE_URL: "${PIPELINE_MONITOR_DATABASE_URL:-postgresql://airflow:airflow@postgres:5432/airflow}"
   ```

3. Add the PostgreSQL driver to `requirements.txt`:

   ```text
   psycopg[binary]>=3.2,<4
   ```

   Rebuild the Airflow image after applying these changes:

   ```bash
   docker compose up --build
   ```

The URL above matches the credentials in the repository's local Compose
PostgreSQL service. Override `PIPELINE_MONITOR_DATABASE_URL` for other
environments and use their approved secret-management mechanism rather than
committing production credentials. The configured PostgreSQL user must be
allowed to create the `pipeline_monitoring` schema and its tables. Outside
Compose, if no PostgreSQL URL is set, the helper retains its SQLite fallback
for local use.

The webhook payload is a JSON object containing alert type, severity,
fingerprint, pipeline, run ID, facility, event time/type, stage, message, and
redacted data. Adapt the webhook receiver to route critical incidents to the
on-call system and lower-severity quality alerts to the appropriate operations
channel.

## Rollout and limitations

1. Configure the PostgreSQL URL and an operations webhook secret.
2. Deploy one scheduled watchdog with policies for a pilot pipeline.
3. Import the helper into a new or explicitly approved pipeline and emit
   lifecycle events, heartbeats, metrics, and quality results.
4. Test success, failure, alert deduplication, recovery, stale heartbeat, and
   webhook/database outage behavior before expanding adoption.
5. Set pipeline-specific quality thresholds from known data behavior; do not
   use a single universal threshold for every dataset.

This module cannot monitor a pipeline that has not opted in, cannot observe a
pipeline before its first event unless `initial_due_at` is set, and cannot
detect Airflow scheduler/worker outages by itself. Monitor Airflow health and
the watchdog’s own execution from an independent operations system.
