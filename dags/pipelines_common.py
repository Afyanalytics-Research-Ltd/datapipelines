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
                    "snowflake_to_v3_migration", "v2_to_v3_api_migration",
                    "migrate_facility", "reingest")

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


# ─── CONFIG FROM AIRFLOW ─────────────────────────────────────────────────
# The scripts read plain env vars (from the repo's .env when run by hand).
# A deployed Airflow may instead hold them as Variables/Connections — the
# older DAGs here use Variable IGNITE_SHEET_ID / GOOGLE_SA_JSON, Connection
# aws_default and one Connection per facility (conn id = facility key). So
# before a script is imported, every setting it needs that isn't already in
# the environment is filled in from Airflow. Precedence:
#   process env  >  Airflow Variable / Connection  >  repo .env
# (the scripts' load_dotenv(override=False) only fills what's still unset).

_VARIABLE_KEYS = (
    "IGNITE_SHEET_ID", "IGNITE_SHEET_WORKSHEET", "GOOGLE_SA_JSON", "GOOGLE_SA_JSON_PATH",
    "SNOWFLAKE_USER", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA", "SNOWFLAKE_PRIVATE_KEY_PATH",
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION",
    "AFYA_USERNAME", "AFYA_PASSWORD", "CORE_APP_ID", "CORE_APP_SECRET",
    "MODEL_GATEWAY_MIGRATION_KEY",
)
_PATH_KEYS = ("SNOWFLAKE_PRIVATE_KEY_PATH", "GOOGLE_SA_JSON_PATH")


def _variable(key: str) -> str | None:
    try:
        from airflow.sdk import Variable
        value = Variable.get(key, default=None)
    except Exception:
        return None
    return str(value).strip() if value not in (None, "") else None


def _connection(conn_id: str):
    try:
        from airflow.sdk import BaseHook
        return BaseHook.get_connection(conn_id)
    except Exception:
        return None


def _set_if_missing(key: str, value, source: str, filled: list[str]) -> None:
    if value and not (os.environ.get(key) or "").strip():
        os.environ[key] = str(value)
        filled.append(f"{key}←{source}")


def load_airflow_config(facilities=()) -> None:
    filled: list[str] = []
    for key in _VARIABLE_KEYS:
        if not (os.environ.get(key) or "").strip():
            _set_if_missing(key, _variable(key), "variable", filled)
    # older DAGs call the worksheet Variable just WORKSHEET
    _set_if_missing("IGNITE_SHEET_WORKSHEET", _variable("WORKSHEET"), "variable WORKSHEET", filled)

    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        aws = _connection("aws_default")
        if aws is not None:
            _set_if_missing("AWS_ACCESS_KEY_ID", aws.login, "conn aws_default", filled)
            _set_if_missing("AWS_SECRET_ACCESS_KEY", aws.password, "conn aws_default", filled)
            region = (aws.extra_dejson or {}).get("region_name")
            _set_if_missing("AWS_REGION", region, "conn aws_default", filled)

    for facility in facilities:
        up = facility.upper()
        # V2 source credentials: FACILITY_<F>_* — Variable, else Connection <facility>
        for suffix in ("USERNAME", "PASSWORD"):
            _set_if_missing(f"FACILITY_{up}_{suffix}", _variable(f"FACILITY_{up}_{suffix}"),
                            "variable", filled)
        if not os.environ.get(f"FACILITY_{up}_USERNAME"):
            conn = _connection(facility)
            if conn is not None:
                _set_if_missing(f"FACILITY_{up}_USERNAME", conn.login, f"conn {facility}", filled)
                _set_if_missing(f"FACILITY_{up}_PASSWORD", conn.password, f"conn {facility}", filled)
        # V2 base URL / database: Variable, else Connection <facility> (host,
        # schema or Extra "db") — a new facility needs no code change
        for suffix in ("BASE_URL", "DB"):
            _set_if_missing(f"FACILITY_{up}_{suffix}", _variable(f"FACILITY_{up}_{suffix}"), "variable", filled)
        if not os.environ.get(f"FACILITY_{up}_BASE_URL") or not os.environ.get(f"FACILITY_{up}_DB"):
            v2conn = _connection(facility)
            if v2conn is not None:
                host = (v2conn.host or "").strip()
                if host and not host.startswith("http"):
                    host = "https://" + host
                _set_if_missing(f"FACILITY_{up}_BASE_URL", host, f"conn {facility}", filled)
                _set_if_missing(f"FACILITY_{up}_DB", v2conn.schema or (v2conn.extra_dejson or {}).get("db"),
                                f"conn {facility}", filled)
        # V3 destination account: AFYA_<F>_* — Variable, else Connection afya_v3_<facility>
        for suffix in ("USERNAME", "PASSWORD"):
            _set_if_missing(f"AFYA_{up}_{suffix}", _variable(f"AFYA_{up}_{suffix}"), "variable", filled)
        if not os.environ.get(f"AFYA_{up}_USERNAME"):
            conn = _connection(f"afya_v3_{facility}")
            if conn is not None:
                _set_if_missing(f"AFYA_{up}_USERNAME", conn.login, f"conn afya_v3_{facility}", filled)
                _set_if_missing(f"AFYA_{up}_PASSWORD", conn.password, f"conn afya_v3_{facility}", filled)
        # V3 org / facility ids: Variable, else the Connection's Extra
        # {"organization_id": 4, "facility_id": 4} — overrides FACILITY_V3_CONFIG
        conn = None
        for key in ("ORGANIZATION_ID", "FACILITY_ID"):
            _set_if_missing(f"AFYA_{up}_{key}", _variable(f"AFYA_{up}_{key}"), "variable", filled)
            if not os.environ.get(f"AFYA_{up}_{key}"):
                conn = conn or _connection(f"afya_v3_{facility}")
                extra = (conn.extra_dejson or {}) if conn is not None else {}
                _set_if_missing(f"AFYA_{up}_{key}", extra.get(key.lower()), f"conn afya_v3_{facility}", filled)
        # V3 URLs: Extra "url_template" ("https://{service}.afyaanalytics.ai/api/")
        # and/or "urls" {"core": "https://…/api/", …} — see v2v3._apply_v3_urls
        conn = conn or _connection(f"afya_v3_{facility}")
        extra = (conn.extra_dejson or {}) if conn is not None else {}
        _set_if_missing(f"AFYA_{up}_URL_TEMPLATE", extra.get("url_template"), f"conn afya_v3_{facility}", filled)
        for svc, url in (extra.get("urls") or {}).items():
            _set_if_missing(f"AFYA_{up}_{str(svc).upper()}_URL", url, f"conn afya_v3_{facility}", filled)

    if filled:
        import logging
        logging.getLogger(__name__).info("Config from Airflow: %s", ", ".join(filled))


def _absolutize_paths() -> None:
    """Make relative file-path settings absolute before the chdir: try the
    task's original working directory first (/opt/airflow, where the mounted
    config/ lives in the deployed stack), then the repo root (where the CLI
    resolves them). A Google SA path that exists nowhere is dropped when
    GOOGLE_SA_JSON is available, so the loader falls back to the raw JSON."""
    # AIRFLOW_HOME first: a task's cwd isn't guaranteed to be /opt/airflow,
    # and the deployed stack's real key lives in its mounted config/.
    bases: list[Path] = []
    for base in (Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")), Path.cwd(), PIPELINES_DIR):
        if base not in bases:
            bases.append(base)
    for key in _PATH_KEYS:
        value = (os.environ.get(key) or "").strip().strip("'\"")
        if not value or os.path.isabs(value):
            continue
        for base in bases:
            candidate = base / value
            if candidate.exists():
                os.environ[key] = str(candidate)
                break
        else:
            if key == "GOOGLE_SA_JSON_PATH" and os.environ.get("GOOGLE_SA_JSON"):
                os.environ.pop(key)


def _key_fingerprint(path: str) -> str:
    """SHA256 fingerprint of the private key's public half, in the format
    Snowflake shows as RSA_PUBLIC_KEY_FP in `DESC USER <user>`."""
    import base64
    import hashlib
    from cryptography.hazmat.primitives import serialization
    passphrase = (os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or "").encode() or None
    key = serialization.load_pem_private_key(Path(path).read_bytes(), password=passphrase)
    der = key.public_key().public_bytes(serialization.Encoding.DER,
                                        serialization.PublicFormat.SubjectPublicKeyInfo)
    return "SHA256:" + base64.b64encode(hashlib.sha256(der).digest()).decode()


def log_snowflake_identity() -> None:
    """Log which user/account/key a task will connect with, so a 'JWT token
    is invalid' can be checked against `DESC USER` (RSA_PUBLIC_KEY_FP)."""
    import logging
    logger = logging.getLogger(__name__)
    path = (os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or "").strip()
    try:
        fp = _key_fingerprint(path) if path else "-"
    except Exception as e:
        fp = f"unreadable ({type(e).__name__}: {e})"
    logger.info("Snowflake identity: user=%s account=%s key=%s fingerprint=%s",
                os.getenv("SNOWFLAKE_USER"), os.getenv("SNOWFLAKE_ACCOUNT"), path or "-", fp)


def use_pipelines_dir(facilities=()) -> None:
    """Call at the top of every task before importing a pipeline script.

    Fills missing settings from Airflow Variables/Connections (see above),
    puts the repo root on sys.path and makes it the working directory, so
    the .env's relative paths (SNOWFLAKE_PRIVATE_KEY_PATH=config/rsa_key.p8,
    GOOGLE_SA_JSON_PATH=service_account.json) resolve the same way they do
    when the scripts are run from the repo. Each Airflow task runs in its
    own process, so the chdir doesn't leak into other tasks."""
    if not LOADER_SCRIPT.exists():
        raise RuntimeError(
            f"Pipeline scripts not found under {PIPELINES_DIR}. Mount the repo root "
            f"there (docker-compose.yaml) or set PIPELINES_DIR."
        )
    load_airflow_config(facilities)
    # Load the repo .env now (the scripts would at import time, after the
    # chdir) so its relative paths go through the same lookup order below.
    try:
        from dotenv import load_dotenv
        load_dotenv(PIPELINES_DIR / ".env", override=False)
    except ImportError:
        pass
    _absolutize_paths()
    log_snowflake_identity()
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
    """Integer setting from the environment, else the repo .env (where the
    CLI gets it, e.g. PAGE_WORKERS=32), else `default` — so DAG defaults
    match what the scripts use when run by hand."""
    value = os.getenv(name)
    if value is None:
        try:
            from dotenv import dotenv_values
            value = dotenv_values(PIPELINES_DIR / ".env").get(name)
        except Exception:
            value = None
    try:
        return max(1, int(str(value).strip().strip("'\""))) if value is not None else default
    except ValueError:
        return default
