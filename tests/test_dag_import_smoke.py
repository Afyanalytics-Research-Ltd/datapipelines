"""
Repo-wide structural smoke test for the 13 DAGs converted from standalone
root scripts in this session (see tests/helpers.py::all_new_dag_module_names).

This file intentionally does NOT test business logic — that's covered in
each dag's own tests/test_dags_<module>.py file. This only asserts that
every module (a) imports cleanly against the airflow_stub, (b) exposes
exactly one importable ``dag`` object with a sane, unique dag_id, and
(c) has a task graph that is internally consistent (every dependency edge
points at a real task, no cycles, no orphaned task_ids).
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.helpers import DAGS_DIR, all_new_dag_module_names, load_dag_module

MODULE_NAMES = all_new_dag_module_names()


@pytest.mark.parametrize("module_name", MODULE_NAMES)
def test_module_file_exists_and_parses(module_name):
    path = DAGS_DIR / f"{module_name}.py"
    assert path.exists(), f"expected dags/{module_name}.py to exist"
    ast.parse(path.read_text(encoding="utf-8"))


@pytest.mark.parametrize("module_name", MODULE_NAMES)
def test_module_imports_and_exposes_one_dag(module_name):
    module = load_dag_module(module_name)
    assert hasattr(module, "dag"), f"{module_name}.py must expose a module-level `dag` object"

    from tests.airflow_stub import DAG

    assert isinstance(module.dag, DAG)
    assert module.dag.dag_id, "dag_id must be a non-empty string"
    assert len(module.dag.tasks) >= 1, "a DAG with zero tasks is almost certainly a bug"


@pytest.mark.parametrize("module_name", MODULE_NAMES)
def test_dag_has_sane_default_args(module_name):
    module = load_dag_module(module_name)
    dag = module.dag
    # Every DAG in this repo follows a house convention of >=1 retry with a
    # real retry_delay so a transient Snowflake/API/S3 hiccup doesn't fail
    # a whole day's run outright.
    assert dag.default_args.get("retries", 0) >= 1, (
        f"{module_name}: expected default_args.retries >= 1 per house convention"
    )
    assert dag.default_args.get("retry_delay") is not None, (
        f"{module_name}: expected default_args.retry_delay to be set"
    )
    assert dag.catchup is False, f"{module_name}: expected catchup=False (these are not backfill DAGs)"
    assert dag.tags, f"{module_name}: expected a non-empty tags list"


@pytest.mark.parametrize("module_name", MODULE_NAMES)
def test_dag_task_graph_is_internally_consistent(module_name):
    module = load_dag_module(module_name)
    dag = module.dag
    task_ids = set(dag.task_dict.keys())

    for task in dag.tasks:
        for upstream_id in task.upstream_task_ids:
            assert upstream_id in task_ids, (
                f"{module_name}: task {task.task_id!r} references unknown "
                f"upstream {upstream_id!r}"
            )
        for downstream_id in task.downstream_task_ids:
            assert downstream_id in task_ids, (
                f"{module_name}: task {task.task_id!r} references unknown "
                f"downstream {downstream_id!r}"
            )

    # Cycle check (Kahn's algorithm) — a DAG with a cycle isn't a DAG.
    indegree = {tid: len(dag.task_dict[tid].upstream_task_ids) for tid in task_ids}
    queue = [tid for tid, deg in indegree.items() if deg == 0]
    visited = 0
    downstream_map = {tid: dag.task_dict[tid].downstream_task_ids for tid in task_ids}
    while queue:
        current = queue.pop()
        visited += 1
        for nxt in downstream_map[current]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)
    assert visited == len(task_ids), f"{module_name}: task graph contains a cycle"

    # Every DAG built by this session's conversion has at least one root
    # (no upstream) task — otherwise nothing would ever start.
    roots = [tid for tid, deg in indegree.items() if len(dag.task_dict[tid].upstream_task_ids) == 0]
    assert roots, f"{module_name}: task graph has no root task"


def test_all_dag_ids_are_globally_unique():
    seen: dict[str, str] = {}
    for module_name in MODULE_NAMES:
        module = load_dag_module(module_name)
        dag_id = module.dag.dag_id
        assert dag_id not in seen, (
            f"dag_id collision: {module_name}.py and {seen.get(dag_id)}.py "
            f"both use dag_id={dag_id!r}"
        )
        seen[dag_id] = module_name


def test_all_dag_ids_are_also_unique_against_pre_existing_dags():
    """Belt-and-braces: also diff against every dag_id already deployed in
    dags/*.py (outside the 13 new ones), by scanning for the
    `DAG_ID = "..."` / `dag_id="..."` convention used across this repo,
    without importing files this test suite doesn't stub for (e.g. ones
    using TaskFlow decorators or MySQL hooks not modelled here)."""
    new_ids = {load_dag_module(name).dag.dag_id for name in MODULE_NAMES}

    pre_existing_ids: set[str] = set()
    for path in DAGS_DIR.glob("*.py"):
        if path.stem in MODULE_NAMES:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
                if isinstance(target, ast.Name) and target.id == "DAG_ID":
                    if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                        pre_existing_ids.add(node.value.value)
            if isinstance(node, ast.keyword) and node.arg == "dag_id":
                if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                    pre_existing_ids.add(node.value.value)

    collisions = new_ids & pre_existing_ids
    assert not collisions, f"dag_id(s) collide with a pre-existing deployed DAG: {collisions}"
