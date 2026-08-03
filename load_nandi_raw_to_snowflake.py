#!/usr/bin/env python3
"""
load_nandi_raw_to_snowflake.py — load the Nandi Kapsabet CSVs into a fresh
HOSPITALS.NANDI_RAW schema:

    2024_clinicians_workload_Jan-Aug2024.csv -> NANDI_RAW.CLINICIANS_WORKLOAD
    outpatient_register_cleaned.csv          -> NANDI_RAW.OUTPATIENT_REGISTER

Both tables are (re)created with CREATE OR REPLACE TABLE, then loaded via
write_pandas. Safe to rerun -- each run starts from a clean table.

USAGE
    python load_nandi_raw_to_snowflake.py
    python load_nandi_raw_to_snowflake.py --dry-run   # read/validate CSVs only

ENV VARS (.env)
    SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_WAREHOUSE / SNOWFLAKE_ROLE
    SNOWFLAKE_DATABASE (defaults to HOSPITALS)
    SNOWFLAKE_PRIVATE_KEY_PATH  (or SNOWFLAKE_PASSWORD)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

log = logging.getLogger("nandi_raw_load")
if not log.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S",
    ))
    log.addHandler(h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

TARGET_DB = os.getenv("SNOWFLAKE_DATABASE", "HOSPITALS").strip()
TARGET_SCHEMA = "NANDI_RAW"

HERE = Path(__file__).resolve().parent

CLINICIANS_CSV = HERE / "2024_clinicians_workload_Jan-Aug2024.csv"
CLINICIANS_TABLE = "CLINICIANS_WORKLOAD"
CLINICIANS_COLUMNS = [
    "NAME", "JAN_2024", "FEB_2024", "MAR_2024", "APR_2024",
    "MAY_2024", "JUN_2024", "JUL_2024", "AUG_2024",
]
CLINICIANS_DDL = f"""
    CREATE OR REPLACE TABLE {TARGET_DB}.{TARGET_SCHEMA}.{CLINICIANS_TABLE} (
        NAME     VARCHAR(100),
        JAN_2024 NUMBER,
        FEB_2024 NUMBER,
        MAR_2024 NUMBER,
        APR_2024 NUMBER,
        MAY_2024 NUMBER,
        JUN_2024 NUMBER,
        JUL_2024 NUMBER,
        AUG_2024 NUMBER
    );
"""

OUTPATIENT_CSV = HERE / "outpatient_register_cleaned.csv"
OUTPATIENT_TABLE = "OUTPATIENT_REGISTER"
OUTPATIENT_COLUMNS = ["PNO", "FULL_NAMES", "SEX", "AGE", "RESIDENCE", "ATT_DIAGNOSIS"]
OUTPATIENT_DDL = f"""
    CREATE OR REPLACE TABLE {TARGET_DB}.{TARGET_SCHEMA}.{OUTPATIENT_TABLE} (
        PNO           VARCHAR(20),
        FULL_NAMES    VARCHAR(200),
        SEX           VARCHAR(5),
        AGE           VARCHAR(20),
        RESIDENCE     VARCHAR(200),
        ATT_DIAGNOSIS VARCHAR(1000)
    );
"""


def _snowflake_connect():
    kwargs: dict[str, Any] = dict(
        user=os.getenv("SNOWFLAKE_USER", "").strip(),
        account=os.getenv("SNOWFLAKE_ACCOUNT", "").strip(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "").strip(),
        role=os.getenv("SNOWFLAKE_ROLE", "").strip() or None,
        database=TARGET_DB,
        schema=TARGET_SCHEMA,
    )
    pk_path = (os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or "").strip()
    pwd = (os.getenv("SNOWFLAKE_PASSWORD") or "").strip()
    if pk_path:
        kwargs["private_key_file"] = pk_path
    elif pwd:
        kwargs["password"] = pwd
    else:
        raise RuntimeError("Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD in .env")
    return snowflake.connector.connect(**kwargs)


def validate_clinicians_workload() -> int:
    df = pd.read_csv(CLINICIANS_CSV, dtype=str)
    df.columns = CLINICIANS_COLUMNS
    for col in CLINICIANS_COLUMNS[1:]:
        pd.to_numeric(df[col], errors="raise")
    log.info("validated %d rows from %s", len(df), CLINICIANS_CSV.name)
    return len(df)


def validate_outpatient_register() -> int:
    df = pd.read_csv(OUTPATIENT_CSV, dtype=str)
    df.columns = OUTPATIENT_COLUMNS
    log.info("validated %d rows from %s", len(df), OUTPATIENT_CSV.name)
    return len(df)


def _copy_into(cur, csv_path: Path, table: str) -> None:
    """PUT the local CSV to the table's stage and COPY INTO it.

    Both source CSVs already have their columns in the exact order the
    target tables declare, so no column mapping is needed beyond skipping
    the header row.
    """
    stage = f"@%{table}"
    cur.execute(f"REMOVE {stage};")
    cur.execute(f"PUT 'file://{csv_path.as_posix()}' {stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;")
    cur.execute(
        f"""
        COPY INTO {TARGET_DB}.{TARGET_SCHEMA}.{table}
        FROM {stage}
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            ESCAPE_UNENCLOSED_FIELD = NONE
        )
        ON_ERROR = ABORT_STATEMENT;
        """
    )
    cur.execute(f"REMOVE {stage};")


def run(dry_run: bool) -> None:
    validate_clinicians_workload()
    validate_outpatient_register()

    if dry_run:
        log.info("dry-run: skipping Snowflake writes")
        return

    conn = _snowflake_connect()
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_DB}.{TARGET_SCHEMA};")
        log.info("ensured schema %s.%s", TARGET_DB, TARGET_SCHEMA)

        for label, ddl, table, csv_path in (
            ("clinicians workload", CLINICIANS_DDL, CLINICIANS_TABLE, CLINICIANS_CSV),
            ("outpatient register", OUTPATIENT_DDL, OUTPATIENT_TABLE, OUTPATIENT_CSV),
        ):
            cur.execute(ddl)
            _copy_into(cur, csv_path, table)
            cur.execute(f"SELECT COUNT(*) FROM {TARGET_DB}.{TARGET_SCHEMA}.{table};")
            n_rows = cur.fetchone()[0]
            log.info(
                "%s: loaded %d row(s) into %s.%s.%s",
                label, n_rows, TARGET_DB, TARGET_SCHEMA, table,
            )
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="read/validate CSVs only, skip Snowflake")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
