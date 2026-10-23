"""
One-off historical load: Gyno Solutions HMS (custom PHP / MariaDB, hms.gynosolutions.co.ke) -> Snowflake.

Source: the live database's backup, downloaded 2026-10-20 through the client's cPanel ("Download a MySQL
Database Backup", read-only on their side): gynosolutionsco_hms.sql.gz (55 tables, MariaDB 10.6.28).
Restored into a throwaway MariaDB container on the Airflow network so this script can read it:

    docker run -d --name gyno_mariadb --network <airflow network> \
        -e MARIADB_ROOT_PASSWORD=... -e MARIADB_DATABASE=gynosolutionsco_hms mariadb:10.6
    zcat gynosolutionsco_hms.sql.gz | docker exec -i gyno_mariadb mariadb -uroot -p... gynosolutionsco_hms

Every table is mirrored into HOSPITALS.GYNOSOLUTIONS_HISTORICAL_DATA under the same (uppercased) name, every
column but credentials (EXCLUDED_COLUMNS) as VARCHAR: the source has zero dates ('0000-00-00'), enums and mixed-type columns, so values are kept
as the database printed them and typed later, when mapped to V3. Each table is re-created on every run (a
re-run replaces it) and its row count checked against the source.

Run inside the Airflow worker (needs the MariaDB root password):
    GYNO_MARIADB_PASSWORD=... python load_gyno_to_snowflake.py [table ...]
Afterwards the container can be removed:  docker rm -f gyno_mariadb
"""
from __future__ import annotations

import logging
import os
import sys

import pandas as pd
import pymysql
from snowflake.connector.pandas_tools import write_pandas

logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("gyno_load")

MARIADB_HOST = os.getenv("GYNO_MARIADB_HOST", "gyno_mariadb")
MARIADB_DATABASE = "gynosolutionsco_hms"
SNOWFLAKE_DATABASE = "HOSPITALS"
SNOWFLAKE_SCHEMA = "GYNOSOLUTIONS_HISTORICAL_DATA"
CHUNK = 50_000
# credentials never leave the source (users.password, doctors.password hold login hashes)
EXCLUDED_COLUMNS = {"password"}


def text(v):
    """A source value as the text Snowflake stores (None stays NULL)."""
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="replace")
    return str(v)


def load_table(src, sf, table: str) -> tuple[int, int]:
    cur = src.cursor()
    cur.execute(f"SELECT COUNT(*) FROM `{table}`")
    expected = cur.fetchone()[0]
    cur.execute(f"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
                f"ORDER BY ORDINAL_POSITION", (MARIADB_DATABASE, table))
    cols = [r[0] for r in cur.fetchall() if r[0].lower() not in EXCLUDED_COLUMNS]
    target = table.upper()
    sf.cursor().execute(f'CREATE OR REPLACE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}."{target}" ('
                        + ", ".join(f'"{c.upper()}" VARCHAR' for c in cols) + ")")
    loaded = 0
    stream = src.cursor(pymysql.cursors.SSCursor)            # server-side: big tables aren't held in memory
    stream.execute(f"SELECT {', '.join(f'`{c}`' for c in cols)} FROM `{table}`")
    while True:
        rows = stream.fetchmany(CHUNK)
        if not rows:
            break
        df = pd.DataFrame([[text(v) for v in r] for r in rows], columns=[c.upper() for c in cols], dtype=object)
        ok, _, n, _ = write_pandas(sf, df, table_name=target, database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA,
                                   quote_identifiers=True, auto_create_table=False)
        if not ok:
            raise RuntimeError(f"write_pandas failed for {target}")
        loaded += n
    stream.close()
    return expected, loaded


def main(only: list[str]) -> int:
    from pipelines_common import use_pipelines_dir
    use_pipelines_dir([])
    import snowflake_to_v3_migration as s2v3
    src = pymysql.connect(host=MARIADB_HOST, user="root", password=os.environ["GYNO_MARIADB_PASSWORD"],
                          database=MARIADB_DATABASE, charset="utf8mb4")
    sf = s2v3._snowflake_connect()
    sf.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
    cur = src.cursor()
    cur.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' "
                "ORDER BY TABLE_NAME", (MARIADB_DATABASE,))
    tables = [r[0] for r in cur.fetchall() if not only or r[0] in only]
    log.info("Target %s.%s — %d table(s)", SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, len(tables))
    bad = []
    for t in tables:
        try:
            expected, loaded = load_table(src, sf, t)
            flag = "" if expected == loaded else f"  MISMATCH (source {expected})"
            log.info("  %-30s %9d rows%s", t, loaded, flag)
            if flag:
                bad.append(t)
        except Exception as e:
            log.error("  %-30s FAILED: %s", t, str(e)[:300])
            bad.append(t)
    log.info("Done — %d/%d tables loaded and counted", len(tables) - len(bad), len(tables))
    if bad:
        log.warning("Check: %s", ", ".join(bad))
    src.close()
    sf.close()
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
