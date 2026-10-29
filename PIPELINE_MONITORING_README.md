# Sentry pipeline monitoring

Every Airflow pipeline in this repository is monitored centrally. No DAG
imports anything or sets any callback to take part. Three alerts go to Sentry:

| What | Raised when | Sentry tag `monitor_type` |
|---|---|---|
| Pipeline failure | any task in any DAG fails (with traceback) | `task_failure` |
| SLA breach | a DAG run is still running one hour after it was queued | `dag_deadline` |
| Data-quality degradation | a successful terminal task reports zero output rows | `data_quality` |

Events are not written to a local or Airflow database.

## How it works

Two files do the work:

- **`config/airflow_local_settings.py`** is Airflow's local-settings module.
  Airflow imports it automatically in every service (api-server, scheduler,
  dag-processor, worker, triggerer) when the service starts, because
  `config/` is mounted at `/opt/airflow/config`. When imported it:
  1. **Initializes Sentry once for that service** from Airflow's `[sentry]`
     settings (see Setup). This is the only place Sentry is configured.
  2. Registers **cluster policies** that Airflow applies to every DAG and task
     each time a DAG file is parsed, including on the worker just before a
     task runs:
     - `task_policy` adds `pipeline_failure_callback` to every task's
       `on_failure_callback`, and `pipeline_row_count_callback` to the
       `on_success_callback` of every terminal task (no downstream tasks).
       Callbacks a DAG already defines are kept; the monitoring callback is
       appended.
     - `dag_policy` gives every DAG a one-hour Deadline Alert, measured from
       when the run is queued, unless the DAG defines its own `deadline`.
- **`pipeline_monitoring.py`** (repo root, mounted at `PIPELINES_DIR`) builds
  the events. `PipelineMonitor` logs every event through Python logging and
  sends ERROR-level events (failed tasks, failed checks, breached deadlines)
  to Sentry as tagged issues. It never initializes Sentry itself. If the
  process has no initialized Sentry client, as in local CLI runs or tests,
  events are only logged.

Where each callback runs:

| Callback | Runs in | Sentry client from |
|---|---|---|
| `pipeline_failure_callback` | worker (task process) | worker startup |
| `pipeline_row_count_callback` | worker (task process) | worker startup |
| `sentry_deadline_callback` | triggerer | triggerer startup |

### Why not Airflow's own `[sentry]` integration

Airflow 3.1.7 still ships `airflow/sentry.py`, but it only wraps the old
Airflow 2 task path. Airflow 3 runs tasks through the Task SDK supervisor,
which never loads it. So setting `sentry_on = true` alone does **not** report
failed tasks. `airflow_local_settings.py` reads the same `[sentry]` settings and
initializes the SDK itself. Leave `[sentry]` options such as `before_send` or
`transport` unset: they are not applied.

## Setup

1. Rebuild the Airflow image (`docker compose build`) so it includes
   `sentry-sdk` from `requirements.txt`. Images built before the dependency
   was added do not have it. In that case each service logs
   `sentry_on is set but sentry-sdk is not installed` at startup, and events
   are only logged.
2. Set the DSN in the Compose project `.env` file or the deployment
   environment:

   ```dotenv
   SENTRY_DSN=https://public@example.invalid/1
   # optional
   SENTRY_ENVIRONMENT=production
   SENTRY_RELEASE=datapipelines@fd99a39
   ```

   The DSN is a secret: provide it through the deployment environment or a
   secret manager and do not commit it.
3. Recreate the Airflow services (`docker compose up -d`). Settings are read
   only at service startup.

Configuration, all read at service startup:

| Setting | Default | Effect |
|---|---|---|
| `AIRFLOW__SENTRY__SENTRY_ON` | `true` in both Compose files | `false` turns all Sentry reporting off |
| `SENTRY_DSN` (or `AIRFLOW__SENTRY__SENTRY_DSN`) | empty | Without a DSN, reporting is off and a warning is logged at startup |
| `SENTRY_ENVIRONMENT` / `AIRFLOW__SENTRY__ENVIRONMENT` | `production` | Sentry environment |
| `SENTRY_RELEASE` / `AIRFLOW__SENTRY__RELEASE` | unset | Sentry release |

A Sentry misconfiguration never prevents Airflow from starting. Sentry setup
only logs its problems, and `pipeline_monitoring` is imported only when a
callback runs.

To confirm it is active, check a service's startup log. A warning containing
`[sentry]` or `Could not initialize Sentry` means alerts are off. A task that
raises an exception should appear in Sentry tagged `monitor_type:task_failure`.

## What is sent

Each issue is tagged with `pipeline` (the DAG ID), `run_id`, `event_type`,
`status` and `stage` (the task ID), plus `facility` when the run has a
`facility` param, and the `monitor_type` tags below:

- **`task_failure`**: also `dag_id`, `task_id` and `try_number`. The task's
  exception is captured with its traceback. A task with retries alerts on each
  failed try, and `try_number` tells the tries apart.
- **`dag_deadline`**: also `dag_id`. The message reads `DAG run exceeded its
  one-hour runtime deadline`. Airflow 3.1.7 marks Deadline Alerts as
  experimental.
- **`data_quality`**: also `dag_id`, `task_id` and `quality_check`. Raised only
  when a terminal task explicitly publishes a zero row count (see below).

Each event also carries a `pipeline_monitor` context with the event ID,
timestamp and the event's (redacted) data. Events are flushed as soon as they
are captured, because Airflow's task process exits without running the SDK's
normal shutdown flush.

Only these monitoring events are sent. Ordinary log lines are neither Sentry
issues nor breadcrumbs, because pipeline logs can contain patient rows, and
the SDK's auto-enabled framework integrations are off. The SDK's default
integrations stay on, so an unhandled exception that crashes an Airflow
service process is also reported.

Values under keys resembling `password`, `secret`, `token`, `authorization`,
`api_key` and `private_key` are redacted, and common inline secret
assignments in messages are masked. Exception messages are sent as raised.
Redaction is a safeguard, not a guarantee: **never include patient data,
credentials, full source rows, or other sensitive information in messages,
exception text or event metadata.**

## Reporting row counts for the data-quality check

`pipeline_row_count_callback` reads the XCom key `pipeline_row_count` from a
successful terminal task. A task whose output must not be empty can publish:

```python
context["ti"].xcom_push(key="pipeline_row_count", value=row_count)
```

A value of `0` raises a `data_quality` issue. A missing key is skipped, and a
non-integer value fails the callback with `TypeError`. Only publish the key
where zero rows indicates degradation. Incremental pipelines may legitimately
process zero new rows and should not publish it for those runs. No current DAG
publishes this key, so row-count alerting becomes active as tasks adopt it.

## Changing the deadline for one DAG

Give the DAG its own `deadline` and the default one-hour alert is not added:

```python
from datetime import timedelta
from airflow.sdk.definitions.deadline import AsyncCallback, DeadlineAlert, DeadlineReference
from airflow_local_settings import sentry_deadline_callback

DAG(
    ...,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(hours=6),
        callback=AsyncCallback(sentry_deadline_callback),
    ),
)
```

The alert message still says "one-hour", so the issue title will not match a
custom interval.

## Optional: richer events from inside a pipeline

The central callbacks need no code in the pipelines. A pipeline that wants
stage-level events or extra quality checks can still use `PipelineMonitor`
directly. Its ERROR-level events reach Sentry through the client the service
already initialized:

```python
from pipeline_monitoring import PipelineMonitor, check_null_rate

monitor = PipelineMonitor(pipeline="nightly_patient_ingest", run_id=run_id, facility="clinic_a")
monitor.stage("extract", status="succeeded", metrics={"rows": len(rows)})
monitor.quality(
    check_null_rate("patient_id_null_rate", null_count=missing, total_count=len(rows), max_rate=0.005),
    stage="extract",
)
```

Event methods:

- `start(metadata=...)` and `finish(status, summary=...)` for run lifecycle.
  The monitor also works as a context manager. It records the outcome, keeps
  the traceback of an escaping exception, and does not swallow it.
- `stage(name, status=..., message=..., metrics=...)` for stage transitions.
- `heartbeat(stage=..., metadata=...)` for debug-level liveness messages.
- `metric(name, value, unit=..., stage=...)` for numeric operational values.
- `quality(result, stage=...)` for data-quality outcomes.
- `failure(message, stage=..., exception=...)` for explicit errors.
- `emit(event_type, status=..., ...)` for any other event. Status `failed` or
  `breached` makes it an ERROR.

Failed stages, failed quality checks, failed runs, `failure()` calls and
breached events are ERROR level and go to Sentry. Pass `tags={...}` to add
Sentry tags to every event from a monitor. Sending is fail-open by default.
Set `fail_open=False` to raise `MonitoringError` when an event cannot be sent.

The quality helpers each return an immutable `QualityResult`:
`check_row_count`, `check_null_rate`, `check_duplicates`,
`check_metric_change`, `check_schema` and `check_referential_integrity`.
`check_metric_change` compares against a baseline the caller supplies. The
module does not keep metric history.

### Sentry Cron check-ins

Passing `cron_monitor_config` to `PipelineMonitor` sends an `IN_PROGRESS`
check-in at `start()` and `OK` or `ERROR` at `finish()`, using the pipeline
name as the Sentry monitor slug. Sentry can then alert on missed or overlong
scheduled runs. The same slug must always refer to the same scheduled job.
Check-ins are only sent when the process has an initialized Sentry client.

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

## Alerting the operations team

Notification routing is configured in the Sentry project, not in code. Create
issue-alert rules that notify the operations team (for example by Slack or
email) for new and regressed issues, filtered by tag:

- `monitor_type:task_failure`: pipeline failures.
- `monitor_type:dag_deadline`: SLA breaches.
- `monitor_type:data_quality`: data-quality degradation.

Add Cron alert rules if any pipeline uses Sentry Cron check-ins.

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
connect to each pipeline's source and target systems. The central failure
callback reports task exceptions, not transfer reconciliation.
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

## Testing

```bash
# from a WSL shell at the repo root
.venv/bin/python -m pytest tests/test_pipeline_monitoring.py tests/test_airflow_monitoring_policy.py
```

The tests stub Airflow and the Sentry SDK. To check the policies against real
Airflow, run in the worker container:

```bash
docker compose exec airflow-worker airflow dags list-import-errors
```

## Limits and deployment notes

- Only DAG runs managed by Airflow are monitored. Running a repo-root script
  directly from the CLI bypasses the policies. Such a run reaches Sentry only
  if the script initializes the Sentry SDK itself and uses `PipelineMonitor`.
- Sentry receives events when they occur. This module does not keep a second
  copy or replay events after a Sentry outage. Retention, quotas, alert rules
  and issue grouping are managed in Sentry.
- Tasks that never start, such as `upstream_failed` or skipped tasks, run no
  failure callback. The failure of the task they depend on is what is
  reported.
- Domain-specific quality checks and per-DAG SLA thresholds are not inferred
  automatically. See the reconciliation contract above.
