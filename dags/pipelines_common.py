# dags/pipelines_common.py
"""
Shared plumbing for the DAGs that drive the repo-root pipeline scripts:

  v2_facility_to_snowflake   → facility_to_snowflake_fast_resume.py
  snowflake_flatten_clean    → flatten_jsons_schemas.py
  snowflake_to_v3_migration  → snowflake_to_v3_migration.py

These DAGs do NOT carry their own copy of the pipeline logic (unlike the
older facility_api_to_snowflake / flatten_jsons_schemas DAGs): the repo root
is mounted at PIPELINES_DIR (see docker-compose.yaml) and the scripts'
functions are called directly, so the CLI and Airflow always run the same
code and share the same state files (.watermarks.json, .migration_*.json).

Nothing heavy is imported at DAG-parse time. Facility keys and the
old-system-history table list are read out of the script source with `ast`
so the trigger form's dropdowns always match the scripts, without
importing gspread/pandas/boto3/snowflake into the DAG processor.
"""
from __future__ import annotations

import ast
import os
import sys
from functools import lru_cache
from pathlib import Path

PIPELINES_DIR = Path(os.getenv("PIPELINES_DIR", "/opt/airflow/pipelines"))
LOADER_SCRIPT = PIPELINES_DIR / "facility_to_snowflake_fast_resume.py"
PIPELINE_MODULES = ("facility_to_snowflake_fast_resume", "flatten_jsons_schemas",
                    "snowflake_to_v3_migration", "v2_to_v3_api_migration")

# Used only if the scripts aren't mounted, so the DAGs still parse and the
# import error surfaces when a task runs, not as a broken DAG.
_FALLBACK_FACILITIES = ["afya_api_auth", "kakamega", "kisumu", "kisumu_v3",
                        "lodwar", "tenri", "xanalife"]


@lru_cache(maxsize=None)
def _literal_assignments(path: Path) -> dict:
    """Top-level `NAME = <literal>` / `NAME: T = <literal>` values in a file."""
    out: dict = {}
    try:
        tree = ast.parse(path.read_text())
    except (OSError, SyntaxError):
        return out
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target, value = node.targets[0], node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            target, value = node.target, node.value
        else:
            continue
        if isinstance(target, ast.Name):
            try:
                out[target.id] = ast.literal_eval(value)
            except ValueError:
                pass
    return out


def facility_keys() -> list[str]:
    facilities = _literal_assignments(LOADER_SCRIPT).get("FACILITIES")
    return sorted(facilities) if facilities else _FALLBACK_FACILITIES


def old_system_history_tables() -> list[str]:
    return sorted(_literal_assignments(LOADER_SCRIPT).get("OLD_SYSTEM_HISTORY_TABLES") or {})


def use_pipelines_dir() -> None:
    """Call at the top of every task before importing a pipeline script.

    Puts the repo root on sys.path and makes it the working directory, so
    the .env's relative paths (SNOWFLAKE_PRIVATE_KEY_PATH=config/rsa_key.p8,
    GOOGLE_SA_JSON_PATH=service_account.json) resolve the same way they do
    when the scripts are run from the repo. Each Airflow task runs in its
    own process, so the chdir doesn't leak into other tasks."""
    if not LOADER_SCRIPT.exists():
        raise RuntimeError(
            f"Pipeline scripts not found under {PIPELINES_DIR}. Mount the repo root "
            f"there (docker-compose.yaml) or set PIPELINES_DIR."
        )
    root = str(PIPELINES_DIR)
    if root in sys.path:
        sys.path.remove(root)
    sys.path.insert(0, root)   # ahead of dags/, which has same-named DAG files
    # Drop any same-named module that was loaded from somewhere else (e.g.
    # dags/flatten_jsons_schemas.py, the older self-contained DAG).
    for name in PIPELINE_MODULES:
        mod = sys.modules.get(name)
        if mod is not None and not str(getattr(mod, "__file__", "")).startswith(root):
            del sys.modules[name]
    os.chdir(root)


def clean_list(values) -> list[str]:
    """Normalise a list param: accepts a list or a comma/newline string;
    trims, lower-cases, drops blanks and duplicates, keeps order."""
    if not values:
        return []
    if isinstance(values, str):
        values = values.replace("\n", ",").split(",")
    seen, out = set(), []
    for v in values:
        v = str(v).strip().lower()
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def raw_schema(facility: str) -> str:
    return f"{facility.upper()}_RAW"


def clean_schema(facility: str) -> str:
    return f"{facility.upper()}_CLEAN"


def int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, str(default))))
    except ValueError:
        return default
