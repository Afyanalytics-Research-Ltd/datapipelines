"""
Shared helpers for importing ``dags/*.py`` files under test.

The ``dags/`` folder is a flat Airflow dags folder (no ``__init__.py`` —
and it should stay that way, since we don't want to change how the real
Airflow DAG processor discovers it). To import one of those files as a
module in tests without turning ``dags/`` into a package, we load it
directly from its file path with ``importlib``.

Always import ``tests.airflow_stub`` (or rely on ``conftest.py``, which
does it automatically for every test) *before* calling ``load_dag_module``
— otherwise the real ``import airflow`` in the target file will fail with
``ModuleNotFoundError`` since Airflow isn't installed in this environment.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = REPO_ROOT / "dags"

_MODULE_NAMESPACE = "dags_under_test"


def load_dag_module(module_filename: str, *, force_reload: bool = False) -> ModuleType:
    """
    Import ``dags/<module_filename>.py`` as ``dags_under_test.<module_filename>``.

    Cached in ``sys.modules`` across calls within a test run unless
    ``force_reload=True``. DAG construction is deterministic and
    side-effect-free (it only builds an in-memory task graph against the
    airflow_stub), so the default caching is safe and keeps test suites
    fast — every test that needs a *fresh* copy of module-level mutable
    state (e.g. to isolate a module-level cache dict) should pass
    ``force_reload=True`` explicitly.
    """
    registered_name = f"{_MODULE_NAMESPACE}.{module_filename}"
    if not force_reload and registered_name in sys.modules:
        return sys.modules[registered_name]

    file_path = DAGS_DIR / f"{module_filename}.py"
    if not file_path.exists():
        raise FileNotFoundError(f"No such dag file: {file_path}")

    spec = importlib.util.spec_from_file_location(registered_name, file_path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise ImportError(f"Could not build an import spec for {file_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[registered_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(registered_name, None)
        raise
    return module


def all_new_dag_module_names() -> list[str]:
    """The 13 DAG modules converted from standalone root scripts in this
    session, used by the repo-wide smoke test. Kept as an explicit list
    (rather than a directory glob) so adding a new dags/*.py file doesn't
    silently start failing this particular smoke test until it's been
    reviewed for inclusion."""
    return [
        "competitor_pricing_pipeline",
        "ec2_ami_s3_backup_pipeline",
        "flatten_jsons_schemas",
        "siaya_gdrive_gemini_extract",
        "siaya_gdrive_landingai_extract",
        "orthopedic_api_to_snowflake",
        "orthopedic_raw_to_clean",
        "orthopedic_v2_raw_pipeline",
        "orthopedic_v2_clean_pipeline",
        "siaya_medical_report_analyzer",
        "siaya_v2_visits_to_snowflake",
        "siaya_v3_visits_to_snowflake",
        "v3_snowflake_writeback",
    ]
