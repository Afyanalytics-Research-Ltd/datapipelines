"""
Root pytest fixtures shared by every test module.

Installs the ``airflow_stub`` (see airflow_stub.py's module docstring for
why: real Airflow only exists inside the Docker image, not in any local
dev environment for this repo) before anything else, then adds the repo
root to ``sys.path`` so ``tests.*`` imports resolve regardless of how
pytest was invoked.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests import airflow_stub  # noqa: E402

airflow_stub.install()

import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _isolated_airflow_stub_state():
    """
    Every test starts and ends with empty Variable/Connection registries.
    Without this, a Variable.set(...) or a registered Connection in one
    test would silently leak into the next test that happens to run after
    it, producing order-dependent failures/false-passes.
    """
    saved_vars = dict(airflow_stub.Variable._store)
    saved_conns = dict(airflow_stub.BaseHook._connections)
    airflow_stub.Variable._store.clear()
    airflow_stub.BaseHook._connections.clear()
    try:
        yield
    finally:
        airflow_stub.Variable._store.clear()
        airflow_stub.Variable._store.update(saved_vars)
        airflow_stub.BaseHook._connections.clear()
        airflow_stub.BaseHook._connections.update(saved_conns)


@pytest.fixture
def set_variables():
    """set_variables(KEY="value", OTHER_KEY="value2") registers Airflow
    Variables for the duration of one test."""

    def _set(**kv: str):
        for key, value in kv.items():
            airflow_stub.Variable.set(key, value)

    return _set


@pytest.fixture
def register_connection():
    """register_connection("conn_id", host=..., login=..., password=...)
    registers a fake Airflow Connection for the duration of one test."""

    def _register(conn_id: str, **kwargs) -> airflow_stub.Connection:
        conn = airflow_stub.Connection(conn_id=conn_id, **kwargs)
        airflow_stub.BaseHook._connections[conn_id] = conn
        return conn

    return _register
