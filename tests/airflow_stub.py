"""
A minimal, in-repo stand-in for the slice of the Apache Airflow API that
this repo's ``dags/*.py`` modules import at module-load time.

WHY THIS EXISTS
----------------
The real ``apache-airflow`` package is only installed inside the project's
Docker image (see Dockerfile: ``FROM apache/airflow:3.1.7``). It is
deliberately NOT a dependency of requirements.txt, it assumes a live
metadata database and a Unix host, and installing it into a local dev
virtualenv is slow, version-fragile, and not how this project runs it.

DAG modules, however, only need three things from Airflow at *import*
time: build a ``DAG`` object, construct some ``PythonOperator`` tasks
(including ``.partial().expand()`` dynamic task mapping), and wire
dependencies with ``>>``. None of that requires a scheduler or a database
— it's plain Python object construction. This module fakes just enough of
that surface (``airflow``, ``airflow.models.Variable``,
``airflow.hooks.base.BaseHook``, ``airflow.operators.python.PythonOperator``,
``airflow.providers.amazon.aws.hooks.s3.S3Hook``,
``airflow.providers.amazon.aws.hooks.ec2.EC2Hook``,
``airflow.utils.trigger_rule.TriggerRule``) so ``import dags.<module>``
succeeds and the resulting task graph is fully inspectable, while every
actual I/O call (Snowflake, S3, requests, Google APIs, ...) still has to be
mocked explicitly by each test, exactly as it would with real Airflow.

This stub is NOT a substitute for testing against real Airflow. Before
deploying a change, also run (inside the actual container):

    docker compose run --rm airflow-worker airflow dags list-import-errors

Tests should treat the classes below as test doubles: register fake
``Variable`` values / ``Connection`` objects via the ``conftest.py``
fixtures (``set_variables`` / ``register_connection``) rather than mutating
the class-level stores directly, and reach for ``unittest.mock.patch`` on
``S3Hook`` / ``EC2Hook`` / ``BaseHook`` wherever a test needs to assert
*how* a DAG module called them.
"""
from __future__ import annotations

import sys
import types
from enum import Enum
from typing import Any


class AirflowStubError(RuntimeError):
    """Raised when stubbed code is exercised without the test having set
    up the fake state (a registered Variable/Connection, or a mock) it
    needs — a loud failure is much more useful here than a silent no-op."""


# ---------------------------------------------------------------------------
# airflow.utils.trigger_rule
# ---------------------------------------------------------------------------
class TriggerRule(str, Enum):
    ALL_SUCCESS = "all_success"
    ALL_FAILED = "all_failed"
    ALL_DONE = "all_done"
    ONE_SUCCESS = "one_success"
    ONE_FAILED = "one_failed"
    NONE_FAILED = "none_failed"
    NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"
    NONE_SKIPPED = "none_skipped"
    ALWAYS = "always"


# ---------------------------------------------------------------------------
# Task graph primitives (airflow.operators.python, airflow.models.baseoperator)
# ---------------------------------------------------------------------------
_CURRENT_DAG_STACK: list["DAG"] = []


class XComArgStub:
    """Stand-in for the object returned by ``<task>.output``. Only used so
    ``.expand(op_kwargs=upstream.output)`` has something to wire a
    dependency edge from — it carries no data at import time."""

    def __init__(self, task: "BaseOperatorStub"):
        self.task = task

    def map(self, fn) -> "XComArgStub":
        """Stand-in for real Airflow's ``XComArg.map(callable)`` (a lazy
        per-item transform applied at task-mapping time). The transform
        itself is never actually applied here — tests exercise the
        transform function directly by unit-testing it (or the task
        callable) on its own — this only needs to keep resolving back to
        the same upstream task so dependency wiring in ``.expand(...)``
        still works."""
        return _MappedXComArgStub(self.task, fn)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"XComArgStub({self.task.task_id!r})"


class _MappedXComArgStub(XComArgStub):
    def __init__(self, task: "BaseOperatorStub", fn):
        super().__init__(task)
        self.fn = fn

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"MappedXComArgStub({self.task.task_id!r}, fn={self.fn!r})"


class BaseOperatorStub:
    """Records ``task_id`` / callable / dependency wiring for one task.
    Never executes ``python_callable`` itself — tests call the underlying
    function directly and mock its collaborators."""

    def __init__(
        self,
        *,
        task_id: str,
        python_callable=None,
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        is_mapped: bool = False,
        **kwargs: Any,
    ):
        self.task_id = task_id
        self.python_callable = python_callable
        self.trigger_rule = trigger_rule
        self.is_mapped = is_mapped
        self.mapped_kwargs: dict[str, Any] | None = None
        self.upstream_task_ids: set[str] = set()
        self.downstream_task_ids: set[str] = set()
        self.extra_kwargs = kwargs

        dag = _CURRENT_DAG_STACK[-1] if _CURRENT_DAG_STACK else None
        self.dag = dag
        if dag is not None:
            dag.add_task(self)

    @property
    def output(self) -> XComArgStub:
        return XComArgStub(self)

    @staticmethod
    def _iter_tasks(value):
        """Walk an op_kwargs/op_args-shaped structure and yield every
        BaseOperatorStub referenced via an XComArgStub inside it."""
        if isinstance(value, XComArgStub):
            yield value.task
        elif isinstance(value, dict):
            for v in value.values():
                yield from BaseOperatorStub._iter_tasks(v)
        elif isinstance(value, (list, tuple, set)):
            for v in value:
                yield from BaseOperatorStub._iter_tasks(v)

    def set_upstream(self, other):
        for other_task in self._resolve(other):
            self.upstream_task_ids.add(other_task.task_id)
            other_task.downstream_task_ids.add(self.task_id)

    def set_downstream(self, other):
        for other_task in self._resolve(other):
            other_task.upstream_task_ids.add(self.task_id)
            self.downstream_task_ids.add(other_task.task_id)

    @staticmethod
    def _resolve(value):
        if isinstance(value, (list, tuple, set)):
            for v in value:
                yield from BaseOperatorStub._resolve(v)
        elif isinstance(value, XComArgStub):
            yield value.task
        else:
            yield value

    def __rshift__(self, other):
        self.set_downstream(other)
        return other

    def __lshift__(self, other):
        self.set_upstream(other)
        return other

    def __rrshift__(self, other):
        # ``other >> self`` where other is a bare list of tasks
        self.set_upstream(other)
        return self

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        flag = "[mapped]" if self.is_mapped else ""
        return f"<Task {self.task_id}{flag}>"


class _PartialStub:
    """Result of ``PythonOperator.partial(...)``, awaiting ``.expand(...)``."""

    def __init__(self, cls, kwargs: dict[str, Any]):
        self._cls = cls
        self._kwargs = kwargs

    def expand(self, **expand_kwargs) -> BaseOperatorStub:
        kwargs = dict(self._kwargs)
        kwargs["is_mapped"] = True
        task = self._cls(**kwargs)
        task.mapped_kwargs = expand_kwargs
        for value in expand_kwargs.values():
            for upstream_task in BaseOperatorStub._iter_tasks(value):
                task.set_upstream(upstream_task)
        return task

    def expand_kwargs(self, xcomarg) -> BaseOperatorStub:
        # `.partial(...).expand_kwargs(upstream.output)` variant, seen in
        # some Airflow 2.x/3.x dynamic-mapping call sites.
        return self.expand(op_kwargs=xcomarg)


class PythonOperator(BaseOperatorStub):
    @classmethod
    def partial(cls, **kwargs) -> _PartialStub:
        return _PartialStub(cls, kwargs)


# ---------------------------------------------------------------------------
# airflow.models.Variable
# ---------------------------------------------------------------------------
_UNSET = object()


class AirflowVariableNotFound(KeyError):
    pass


class Variable:
    """Backed by a plain class-level dict. Tests should populate it via the
    ``set_variables`` fixture in conftest.py, which is reset after every
    test — do not rely on state leaking between tests."""

    _store: dict[str, str] = {}

    @classmethod
    def get(cls, key: str, default_var: Any = _UNSET, deserialize_json: bool = False):
        if key in cls._store:
            value = cls._store[key]
        elif default_var is not _UNSET:
            return default_var
        else:
            raise AirflowVariableNotFound(
                f"Variable {key!r} is not set and no default_var was given. "
                f"Register it in your test via the set_variables fixture."
            )
        if deserialize_json:
            import json

            return json.loads(value)
        return value

    @classmethod
    def set(cls, key: str, value: Any, serialize_json: bool = False) -> None:
        if serialize_json:
            import json

            value = json.dumps(value)
        cls._store[key] = value

    @classmethod
    def delete(cls, key: str) -> None:
        cls._store.pop(key, None)


# ---------------------------------------------------------------------------
# airflow.hooks.base.BaseHook / Connection
# ---------------------------------------------------------------------------
class Connection:
    def __init__(
        self,
        conn_id: str | None = None,
        conn_type: str | None = None,
        host: str | None = None,
        login: str | None = None,
        password: str | None = None,
        schema: str | None = None,
        port: int | None = None,
        extra: str | dict | None = None,
    ):
        self.conn_id = conn_id
        self.conn_type = conn_type
        self.host = host
        self.login = login
        self.password = password
        self.schema = schema
        self.port = port
        self._extra = extra

    @property
    def extra_dejson(self) -> dict:
        if isinstance(self._extra, dict):
            return self._extra
        if not self._extra:
            return {}
        import json

        try:
            return json.loads(self._extra)
        except (TypeError, ValueError):
            return {}

    def get_uri(self) -> str:
        return f"{self.conn_type or ''}://{self.login or ''}:***@{self.host or ''}"

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<Connection {self.conn_id!r} host={self.host!r}>"


class BaseHook:
    """Tests register fake connections via the ``register_connection``
    fixture in conftest.py; that fixture resets ``_connections`` after
    every test."""

    _connections: dict[str, Connection] = {}

    @classmethod
    def get_connection(cls, conn_id: str) -> Connection:
        try:
            return cls._connections[conn_id]
        except KeyError as exc:
            raise AirflowStubError(
                f"No stub Connection registered for conn_id={conn_id!r}. "
                f"Register one in your test via the register_connection fixture."
            ) from exc


# ---------------------------------------------------------------------------
# airflow.providers.amazon.aws.hooks.s3 / .ec2
# ---------------------------------------------------------------------------
class S3Hook:
    """Constructible stand-in for the real S3Hook. Tests that need to
    assert *what* was uploaded should ``unittest.mock.patch`` this class
    (or the ``S3Hook`` name inside the dag module under test) rather than
    relying on this stub's in-memory ``uploaded`` list, since production
    code constructs a fresh ``S3Hook(...)`` per call."""

    def __init__(self, aws_conn_id: str | None = None, **kwargs: Any):
        self.aws_conn_id = aws_conn_id
        self.uploaded: list[dict[str, Any]] = []

    def load_bytes(self, bytes_data, key, bucket_name=None, replace=True, **kwargs):
        self.uploaded.append(
            {"type": "bytes", "key": key, "bucket_name": bucket_name, "data": bytes_data}
        )

    def load_string(self, string_data, key, bucket_name=None, replace=True, **kwargs):
        self.uploaded.append(
            {"type": "string", "key": key, "bucket_name": bucket_name, "data": string_data}
        )

    def load_file(self, filename, key, bucket_name=None, replace=True, **kwargs):
        self.uploaded.append(
            {"type": "file", "key": key, "bucket_name": bucket_name, "filename": filename}
        )

    def get_credentials(self):
        return types.SimpleNamespace(access_key="TEST", secret_key="TEST", token=None)


class EC2Hook:
    """boto3-EC2-client access must be mocked explicitly by tests — there
    is no meaningful fake to return here."""

    def __init__(self, aws_conn_id: str | None = None, region_name: str | None = None, **kwargs):
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def get_conn(self):
        raise AirflowStubError(
            "EC2Hook.get_conn() was called without being mocked. "
            "Patch this in your test, e.g. "
            "mock.patch('dags_under_test.<module>.EC2Hook').return_value.get_conn.return_value = ..."
        )

    def get_client_type(self, client_type: str, region_name: str | None = None):
        raise AirflowStubError(
            "EC2Hook.get_client_type() was called without being mocked. Patch this in your test."
        )


# ---------------------------------------------------------------------------
# airflow.DAG
# ---------------------------------------------------------------------------
class DAG:
    def __init__(
        self,
        dag_id: str,
        *,
        start_date=None,
        schedule=None,
        schedule_interval=None,
        catchup: bool | None = None,
        default_args: dict | None = None,
        max_active_tasks: int | None = None,
        max_active_runs: int | None = None,
        max_active_tis_per_dag: int | None = None,
        tags: list[str] | None = None,
        params: dict | None = None,
        description: str | None = None,
        **kwargs: Any,
    ):
        self.dag_id = dag_id
        self.start_date = start_date
        # Airflow 3.x renamed schedule_interval -> schedule; accept either.
        self.schedule = schedule if schedule is not None else schedule_interval
        self.catchup = catchup
        self.default_args = default_args or {}
        self.max_active_tasks = max_active_tasks
        self.max_active_runs = max_active_runs
        self.max_active_tis_per_dag = max_active_tis_per_dag
        self.tags = list(tags or [])
        self.params = dict(params or {})
        self.description = description
        self.extra_kwargs = kwargs

        self.tasks: list[BaseOperatorStub] = []
        self.task_dict: dict[str, BaseOperatorStub] = {}

    def add_task(self, task: BaseOperatorStub) -> None:
        if task.task_id in self.task_dict:
            raise AirflowStubError(
                f"Task id {task.task_id!r} already exists in dag {self.dag_id!r}"
            )
        self.tasks.append(task)
        self.task_dict[task.task_id] = task

    def get_task(self, task_id: str) -> BaseOperatorStub:
        return self.task_dict[task_id]

    @property
    def task_ids(self) -> list[str]:
        return [t.task_id for t in self.tasks]

    def __enter__(self) -> "DAG":
        _CURRENT_DAG_STACK.append(self)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        popped = _CURRENT_DAG_STACK.pop()
        assert popped is self, "airflow_stub DAG context stack corrupted"
        return False

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<DAG {self.dag_id!r} tasks={len(self.tasks)}>"


# ---------------------------------------------------------------------------
# Module registration
# ---------------------------------------------------------------------------
def install() -> dict[str, types.ModuleType]:
    """Register the fake ``airflow`` package tree into ``sys.modules``.
    Always overwrites any previously-installed stub modules (idempotent to
    call more than once) — this test suite is written against the stub, not
    against a real Airflow install, by design (see module docstring)."""

    airflow_mod = types.ModuleType("airflow")
    airflow_mod.DAG = DAG

    models_mod = types.ModuleType("airflow.models")
    models_mod.Variable = Variable
    models_mod.DAG = DAG

    operators_mod = types.ModuleType("airflow.operators")
    operators_python_mod = types.ModuleType("airflow.operators.python")
    operators_python_mod.PythonOperator = PythonOperator

    hooks_mod = types.ModuleType("airflow.hooks")
    hooks_base_mod = types.ModuleType("airflow.hooks.base")
    hooks_base_mod.BaseHook = BaseHook
    hooks_base_mod.Connection = Connection

    providers_mod = types.ModuleType("airflow.providers")
    providers_amazon_mod = types.ModuleType("airflow.providers.amazon")
    providers_amazon_aws_mod = types.ModuleType("airflow.providers.amazon.aws")
    providers_amazon_aws_hooks_mod = types.ModuleType("airflow.providers.amazon.aws.hooks")
    s3_hook_mod = types.ModuleType("airflow.providers.amazon.aws.hooks.s3")
    s3_hook_mod.S3Hook = S3Hook
    ec2_hook_mod = types.ModuleType("airflow.providers.amazon.aws.hooks.ec2")
    ec2_hook_mod.EC2Hook = EC2Hook

    utils_mod = types.ModuleType("airflow.utils")
    trigger_rule_mod = types.ModuleType("airflow.utils.trigger_rule")
    trigger_rule_mod.TriggerRule = TriggerRule

    exceptions_mod = types.ModuleType("airflow.exceptions")
    exceptions_mod.AirflowException = RuntimeError
    exceptions_mod.AirflowNotFoundException = AirflowVariableNotFound
    exceptions_mod.AirflowSkipException = RuntimeError

    modules = {
        "airflow": airflow_mod,
        "airflow.models": models_mod,
        "airflow.operators": operators_mod,
        "airflow.operators.python": operators_python_mod,
        "airflow.hooks": hooks_mod,
        "airflow.hooks.base": hooks_base_mod,
        "airflow.providers": providers_mod,
        "airflow.providers.amazon": providers_amazon_mod,
        "airflow.providers.amazon.aws": providers_amazon_aws_mod,
        "airflow.providers.amazon.aws.hooks": providers_amazon_aws_hooks_mod,
        "airflow.providers.amazon.aws.hooks.s3": s3_hook_mod,
        "airflow.providers.amazon.aws.hooks.ec2": ec2_hook_mod,
        "airflow.utils": utils_mod,
        "airflow.utils.trigger_rule": trigger_rule_mod,
        "airflow.exceptions": exceptions_mod,
    }

    # wire parent -> child attributes so both `import airflow.models` and
    # `airflow.models.Variable` (post `import airflow`) resolve correctly.
    airflow_mod.models = models_mod
    airflow_mod.operators = operators_mod
    operators_mod.python = operators_python_mod
    airflow_mod.hooks = hooks_mod
    hooks_mod.base = hooks_base_mod
    airflow_mod.providers = providers_mod
    providers_mod.amazon = providers_amazon_mod
    providers_amazon_mod.aws = providers_amazon_aws_mod
    providers_amazon_aws_mod.hooks = providers_amazon_aws_hooks_mod
    providers_amazon_aws_hooks_mod.s3 = s3_hook_mod
    providers_amazon_aws_hooks_mod.ec2 = ec2_hook_mod
    airflow_mod.utils = utils_mod
    utils_mod.trigger_rule = trigger_rule_mod
    airflow_mod.exceptions = exceptions_mod

    sys.modules.update(modules)
    return modules
