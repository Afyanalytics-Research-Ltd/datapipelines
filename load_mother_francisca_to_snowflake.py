"""
One-off historical load: Kapsabet LimSoft (SQL Server) -> Snowflake.

Source: Kapsabett_LimSoft_DB_BK_2026_SEP21_9AM 1.BAK, a native SQL Server
backup (confirmed via `file`, NT Backup / MTF format — NOT MySQL despite
the original ask). It was restored into a temporary throwaway SQL Server
container so this script has something to read from:

    docker run -d --name mssql_restore -e ACCEPT_EULA=Y \
        -e MSSQL_SA_PASSWORD='TempRestore2026!Pass' -p 14330:1433 \
        mcr.microsoft.com/mssql/server:2022-latest
    docker cp "Kapsabett_LimSoft_DB_BK_2026_SEP21_9AM 1.BAK" \
        mssql_restore:/var/opt/mssql/backup/kapsabet.bak
    # RESTORE FILELISTONLY first to get logical file names, then:
    RESTORE DATABASE [MotherFrancisca]
    FROM DISK = N'/var/opt/mssql/backup/kapsabet.bak'
    WITH MOVE 'LimSoft_Data' TO '/var/opt/mssql/data/MotherFrancisca.mdf',
         MOVE 'LimSoft_Log'  TO '/var/opt/mssql/data/MotherFrancisca_log.ldf',
         REPLACE, STATS = 5;

This script connects to that container over pymssql, reads every dbo table
in the restored database wholesale, and writes it into Snowflake under
HOSPITALS.MOTHERFRANSICA_HISTORICAL_DATA — one Snowflake table per source
table, same name (uppercased), straight column-for-column mirror. This is
a historical archive dump, not an API ingest, so there's no RAW/CLEAN
VARIANT split like the rest of this repo's pipelines — tables are loaded
as native typed Snowflake tables via write_pandas(auto_create_table=True).

Run:
    python3 load_mother_francisca_to_snowflake.py

Afterwards the temporary SQL Server container can be torn down:
    docker rm -f mssql_restore
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import pandas as pd
import pymssql
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s · %(levelname)-7s · %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("mother_francisca_load")

MSSQL_HOST = "127.0.0.1"
MSSQL_PORT = 14330
MSSQL_USER = "sa"
MSSQL_PASSWORD = "TempRestore2026!Pass"
MSSQL_DATABASE = "MotherFrancisca"

SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "HOSPITALS")
SNOWFLAKE_SCHEMA = "MOTHERFRANSICA_HISTORICAL_DATA"


def _mssql_connect() -> pymssql.Connection:
    return pymssql.connect(
        server=MSSQL_HOST,
        port=MSSQL_PORT,
        user=MSSQL_USER,
        password=MSSQL_PASSWORD,
        database=MSSQL_DATABASE,
    )


def _snowflake_connect() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER").strip(),
        account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(),
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip(),
    )


def list_tables(mssql_conn: pymssql.Connection) -> list[tuple[str, int]]:
    cur = mssql_conn.cursor()
    cur.execute(
        """
        SELECT t.name AS table_name, p.rows AS row_count
        FROM sys.tables t
        JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
        WHERE t.schema_id = SCHEMA_ID('dbo')
        ORDER BY t.name
        """
    )
    return [(row[0], row[1]) for row in cur.fetchall()]


def load_table(mssql_conn: pymssql.Connection, sf_conn, table_name: str) -> int:
    df = pd.read_sql(f"SELECT * FROM [dbo].[{table_name}]", mssql_conn)
    target_table = table_name.strip().upper()

    # write_pandas needs object-dtype columns to not be a mix of types;
    # SQL Server NULLs already come through as None/NaN which it handles.
    success, _, nrows, _ = write_pandas(
        sf_conn,
        df,
        table_name=target_table,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        auto_create_table=True,
        overwrite=True,
        quote_identifiers=True,
    )
    if not success:
        raise RuntimeError(f"write_pandas reported failure for {target_table}")
    return nrows


def main() -> None:
    mssql_conn = _mssql_connect()
    sf_conn = _snowflake_connect()

    sf_conn.cursor().execute(
        f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}"
    )
    log.info("Target: %s.%s", SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA)

    tables = list_tables(mssql_conn)
    log.info("Found %d tables in %s", len(tables), MSSQL_DATABASE)

    loaded, failed = 0, []
    for table_name, row_count in tables:
        try:
            n = load_table(mssql_conn, sf_conn, table_name)
            log.info("  %-32s %8d rows -> %s", table_name.strip(), n, table_name.strip().upper())
            loaded += 1
        except Exception as e:
            log.error("  %-32s FAILED: %s", table_name.strip(), e)
            failed.append(table_name)

    log.info("Done — %d/%d tables loaded", loaded, len(tables))
    if failed:
        log.warning("Failed tables: %s", ", ".join(t.strip() for t in failed))

    mssql_conn.close()
    sf_conn.close()


if __name__ == "__main__":
    sys.exit(main())
