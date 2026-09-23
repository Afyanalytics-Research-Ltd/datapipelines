# Tests

Unit/structural tests for the Airflow DAGs in `dags/`, focused on the 13
DAGs converted from standalone root scripts (see `tests/helpers.py::all_new_dag_module_names`).

## Why this looks the way it does

Real `apache-airflow` is only installed inside the project's Docker image
(`Dockerfile: FROM apache/airflow:3.1.7`) — it's not a dependency of
`requirements.txt` and doesn't install cleanly in a local dev virtualenv
(Unix-only assumptions, needs a live metadata DB, heavy/slow/version-fragile
otherwise). So these tests import each `dags/*.py` module against
`tests/airflow_stub.py`, a small in-repo stand-in for the handful of
Airflow symbols these DAGs actually use at import time (`DAG`,
`PythonOperator` incl. `.partial().expand()`, `BaseHook`, `Variable`,
`S3Hook`, `EC2Hook`, `TriggerRule`). See that file's docstring for the full
rationale.

**This is not a substitute for testing against real Airflow.** Before
deploying a DAG change, also run, inside the actual container:

```bash
docker compose run --rm airflow-worker airflow dags list-import-errors
```

## Running the tests

This repo's populated virtualenv is `env/` (created inside WSL — the
`.venv/` folder is a stale/incomplete one, don't use it).

```bash
# from a WSL shell, at the repo root:
source env/bin/activate
pip install -r requirements-dev.txt   # first time only
python -m pytest                      # runs everything under tests/
python -m pytest tests/test_dags_orthopedic_api_to_snowflake.py -v   # one file
```

From native Windows (this repo lives on a `\\wsl.localhost\...` UNC path),
run the same commands via `wsl.exe -e bash -c "cd /home/luther/datapipelines && source env/bin/activate && python -m pytest ..."`.

## Layout

- `airflow_stub.py` — the fake Airflow package. Don't mutate its
  class-level stores (`Variable._store`, `BaseHook._connections`) directly
  in a test — use the `set_variables` / `register_connection` fixtures from
  `conftest.py`, which reset state after every test.
- `helpers.py` — `load_dag_module("some_dag_filename")` imports
  `dags/some_dag_filename.py` without needing `dags/` to be a real Python
  package (it deliberately isn't, to match how Airflow's own DAG processor
  discovers it).
- `conftest.py` — installs the stub, adds the repo root to `sys.path`,
  provides `set_variables` / `register_connection` fixtures, and resets
  stub state between every test automatically.
- `test_dag_import_smoke.py` — repo-wide structural checks across all 13
  new DAGs at once (imports cleanly, one `dag` object, sane
  `default_args`/`catchup`/`tags`, task graph has no cycles/orphan edges,
  every `dag_id` is globally unique including against pre-existing
  deployed DAGs).
- `test_dags_<module_name>.py` — one file per converted DAG with deep
  coverage of that module's actual business logic (SQL builders,
  PII-redaction/field-mapping, pagination/retry/backoff behavior, watermark
  keys, idempotent MERGE/upsert semantics, ...), plus that DAG's specific
  task-graph shape. All external I/O (Snowflake, S3, HTTP APIs, Google
  APIs, LLM calls) is mocked — no test should need real credentials or
  network access.
