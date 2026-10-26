# Sentry pipeline monitoring

`pipeline_monitoring.py` emits structured pipeline events through Python's
standard `logging` module. Airflow's Sentry integration captures Airflow task
exceptions; the helper adds pipeline lifecycle, quality, and scheduled-run
monitoring where pipelines explicitly use it. Events are not written to a
local or Airflow database.

The module does not discover or instrument every existing pipeline by itself.
Import it from the pipeline or callback code that should emit monitoring
events.

## Setup

The Airflow image already installs the Sentry SDK through `requirements.txt`.
The shared Airflow Compose environment enables Airflow's built-in Sentry
integration in the API server, scheduler, DAG processor, workers, and
triggerer. Set `SENTRY_DSN` in the Compose project `.env` file or deployment
environment before starting the services:

```dotenv
SENTRY_DSN=https://public@example.invalid/1
```

The DSN is a secret: provide it through the deployment environment or a secret
manager and do not commit it. `AIRFLOW__SENTRY__SENTRY_ON` defaults to `true`
in both Compose files; set it to `false` to disable Sentry. Without a DSN,
Sentry has nowhere to send events. Recreate/restart the Airflow services after
changing these settings.

Airflow initializes Sentry in the task execution path and captures task
exceptions with DAG/task tags and run context. When this integration is
enabled, `pipeline_monitoring.configure_sentry()` defers to Airflow's SDK
client instead of initializing a second client. Outside Airflow, the helper
still initializes Sentry itself when `SENTRY_DSN` is set.

See the official Sentry documentation for
[Python logging](https://docs.sentry.io/platforms/python/integrations/logging/)
and [Cron monitoring](https://docs.sentry.io/platforms/python/crons/).

## What is sent

Airflow captures task exceptions as Sentry issue events and adds Airflow
context, including DAG ID, task ID, operator, and data interval. The Airflow
Sentry integration also adds breadcrumbs for completed tasks in the same DAG
run.

The Airflow cluster policy in `config/airflow_local_settings.py` assigns a
one-hour deadline from the time each DAG run is queued, unless that DAG
already defines its own deadline. A missed deadline sends a tagged Sentry
error event from the triggerer. Airflow 3.1.7 marks Deadline Alerts as
experimental.

The same cluster policy checks successful terminal tasks for the XCom key
`pipeline_row_count`. If a terminal task explicitly publishes a count of
zero, it sends a tagged data-quality Sentry event; tasks that do not publish
this key are skipped. For example, a task whose output is required to be
nonempty should publish:

```python
context["ti"].xcom_push(key="pipeline_row_count", value=row_count)
```

Only publish the key where zero rows indicate degradation. Incremental
pipelines may legitimately process zero new rows and should not publish this
key for those runs. No current DAG uses this standard key, so row-count
alerting becomes active as the relevant tasks adopt this contract.

`PipelineMonitor` emits structured Python log records for lifecycle events,
metrics, and data-quality checks. Failed stages and checks are logged at
`ERROR` level. Heartbeats are `DEBUG` records. Which log records are forwarded
depends on the Sentry logging integration enabled by the deployed Airflow and
SDK versions; this is distinct from Airflow's guaranteed task-exception
capture.

The helper redacts values under keys resembling `password`, `secret`, `token`,
`authorization`, `api_key`, and `private_key`, and masks common inline secret
assignments in messages. Redaction is a safeguard, not a guarantee: **never
include patient data, credentials, full source rows, or other sensitive
information in messages or event metadata.**

Without `SENTRY_DSN`, events are still emitted through normal Python logging,
but are not sent to Sentry. Sentry initialization is fail-open by default
outside Airflow; set `fail_open=False` on `PipelineMonitor` to raise
`MonitoringError` when SDK initialization fails.

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
the configured maximum runtime. Enable it for a scheduled pipeline by passing
its expected schedule and runtime limit to `PipelineMonitor`:

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

## Alerting and coverage

Configure Sentry issue-alert rules to notify the operations team for Airflow
task exceptions and failed pipeline/data-quality events. Configure Sentry
Cron alert rules for missed or overlong runs. Notification destinations and
thresholds (for example Slack or email) are Sentry project settings and must
be set up in the Sentry project.

Airflow exception capture applies to every Airflow task without adding
callbacks to individual DAGs. SLA/Cron monitoring still needs an explicit
Sentry Cron configuration for missed scheduled runs. A one-hour DAG-run
deadline is applied by default as described above. The row-count check is
active only when a task explicitly publishes `pipeline_row_count`; null-rate,
duplicate, metric-drift, schema, and referential-integrity checks need
pipeline-specific inputs and thresholds.

## Central data reconciliation: current limits and a uniform contract

Central execution of data-quality checks is possible, but central monitoring
cannot reliably calculate every check from the Airflow task context alone.
The repository's DAGs are heterogeneous: they transfer JSON, spreadsheets,
and database records using different operators and libraries; some return
counts, some return status dictionaries, and others return nothing. Some
tasks are reports, cleanup, watermark updates, or triggers rather than data
loads. The meaning of a count also varies: it may describe rows fetched,
mapped, copied, merged, or merely attempted.

Source and target counts are not automatically expected to match. A pipeline
may filter records, map one source into multiple target tables, deduplicate,
skip unchanged data, or legitimately process no new records. A global rule
such as “target count equals source count” or “zero rows is a failure” would
therefore create false alerts. Likewise, null limits, duplicate rules,
required columns, referential-integrity expectations, and acceptable metric
drift are data-contract decisions; generic code cannot infer them safely.

The existing helpers in `pipeline_monitoring.py` evaluate values passed in by
the pipeline; they do not independently inspect arbitrary source files or
connect to each pipeline's source and target systems. Airflow's built-in
Sentry integration reports task exceptions, not transfer reconciliation.
Currently, the global row-count callback only reads the optional
`pipeline_row_count` XCom from a successful terminal task. No current DAG
publishes that key, so this callback does not yet produce row-count alerts.

### Suggested DAG output contract

To make reconciliation centrally reusable, each data-producing DAG should
publish one structured `pipeline_reconciliation` XCom after its target write.
It should contain one result per target table/model, not just one aggregate
count for an entire run. A conceptual payload is:

```json
{
  "targets": [
    {
      "target": "reception_patient",
      "expected_target_rows": 120,
      "loaded_target_rows": 120,
      "duplicate_count": 0,
      "null_count": 3,
      "total_count": 120,
      "max_null_rate": 0.05,
      "expected_columns": ["id", "patient_number"],
      "actual_columns": ["id", "patient_number"],
      "allow_extra_columns": false
    }
  ]
}
```

The contract should follow these rules:

1. **Measure at comparable points.** `expected_target_rows` is the count after
   the DAG's own filters, mapping, and transformations. `loaded_target_rows`
   is the count confirmed at the target, scoped to this run and target. Do not
   compare raw source rows directly with target rows when transformations
   change cardinality.
2. **Scope counts to one run.** Include a stable Airflow `dag_id`, `run_id`,
   and target identifier in the reconciliation record. The target-side query
   must identify only rows written by that run, using a run ID, batch ID, or
   equivalent load metadata. A timestamp alone may not uniquely identify a
   run. If a target cannot identify run-scoped rows, report the loader's
   confirmed copy result and clearly label it as a load count rather than a
   target verification.
3. **Make expectations explicit.** Publish only checks that apply and
   include their thresholds: duplicate count and allowed maximum; null and
   total counts with maximum rate; expected and actual columns with whether
   extra columns are allowed; metric actual value, baseline, and allowed
   change; orphan count, child count, and allowed orphan rate. Missing checks
   should be treated as “not configured,” not silently assumed to pass or
   fail.
4. **Represent valid empty runs.** A DAG must be able to declare that zero is
   expected or allowed for a target, such as an incremental run with no
   changes. A zero count should only alert when that target's declared
   contract requires data.
5. **Keep payloads operational and safe.** Publish counts, schema names,
   check results, and identifiers needed for diagnosis—not source rows,
   patient data, credentials, or full spreadsheet/JSON contents.

For mapped pipelines such as `from_json_mappings_to_snowflake.py`, this
contract is especially practical: the code already knows the records expected
for each mapped V3 table after its mapping and filters. It would still need
to capture the Snowflake COPY result and, where feasible, verify the target
count for that specific run. Its raw table currently has no run-ID column, so
reliable target-side verification would require adding run-scoped load
metadata. The source count alone is not a substitute for the expected count
of each mapped target.

### Central rollout

Once DAGs publish the agreed contract, the global Airflow task policy can
read `pipeline_reconciliation` from the final data task's XCom and pass each
configured check to the existing `check_row_count`, `check_duplicates`,
`check_null_rate`, `check_metric_change`, `check_schema`, and
`check_referential_integrity` helpers. Failed `QualityResult` values can then
be sent through `PipelineMonitor.quality()` and captured by Sentry, with tags
for DAG, run, target, and check. Central policy handles consistent
evaluation and alerting; DAG tasks remain responsible for measuring their
source/target data and supplying meaningful expectations.

The commented-out `report_reconciliation` template near the quality helpers
in `pipeline_monitoring.py` illustrates this future step. It is intentionally
inactive: uncomment/adapt it only after DAGs publish the required comparable
counts and per-check expectations. Until then, keep the existing safe
behavior—Airflow exceptions and deadlines are monitored centrally, and absent
quality metrics are not guessed.

Roll this out incrementally: define the contract, pilot it on a pipeline with
reliable mapped and loaded counts, run checks in report-only mode, compare
results with operators' expectations, then enable Sentry alerts for verified
rules. Add other DAGs as their transfer stages can provide equivalent
measurements. This avoids changing every DAG at once or raising alerts from
incomparable counts.

## Limits and deployment notes

Sentry receives events as the application logs them; this module does not
persist a second copy or replay events after an outage. Sentry retention,
project quotas, alert rules, and issue grouping are managed in Sentry.

Airflow's built-in integration captures task exceptions, not every log record.
The helper does not automatically create domain-specific quality checks or
SLA thresholds. Add instrumentation and explicit checks to the pipeline code
whose behavior you want monitored.
