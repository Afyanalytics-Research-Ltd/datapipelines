"""
One-off historical load: Eagle Nursing Home's Patcom HMS (custom MySQL 8, schema `patcom`) -> Snowflake.

Source: patcom_afya_backup.rar (Navicat dump of EAGLE_DB, 2026-10-26, 4.7 GB, 269 tables), restored into a
throwaway MySQL container on the Airflow network so this script can read it:

    docker run -d --name eagle_mysql --network datapipelines_default -e MYSQL_ROOT_PASSWORD=... \
        -e MYSQL_DATABASE=patcom mysql:8.0 --skip-log-bin --innodb-flush-log-at-trx-commit=0
    { echo "SET autocommit=0;"; cat patcom_afya_backup.sql; echo "COMMIT;"; } | docker exec -i eagle_mysql mysql -uroot -p... patcom

Every table is mirrored into HOSPITALS.EAGLE_NURSING_HOME_HISTORICAL_DATA under the same (uppercased) name, as
VARCHAR (zero dates, enums and mixed-type columns are kept as the database printed them and typed later, when
mapped to V3). Credentials never leave the source: login tables (AUTH_TABLES) are skipped and password / token /
secret columns dropped. Each table is re-created on every run and its row count checked against the source.

Run inside the Airflow worker:
    EAGLE_MYSQL_PASSWORD=... python load_eagle_to_snowflake.py [table ...]
Afterwards the container can be removed:  docker rm -f eagle_mysql
"""
from __future__ import annotations

import logging
import os
import re
import sys

import pandas as pd
import pymysql
from snowflake.connector.pandas_tools import write_pandas

sys.path.insert(0, "/opt/airflow/dags")          # pipelines_common
logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("eagle_load")

MYSQL_HOST = os.getenv("EAGLE_MYSQL_HOST", "eagle_mysql")
MYSQL_DATABASE = "patcom"
SNOWFLAKE_DATABASE = "HOSPITALS"
SNOWFLAKE_SCHEMA = "EAGLE_NURSING_HOME_HISTORICAL_DATA"
CHUNK = 100_000
AUTH_TABLES = {"passwordhistory", "passwordexpiry", "usertokens", "trusteddevices", "loginmfachallenges",
               "userloginlockout", "userloginemails"}
SECRET_COLUMN = re.compile(r"pass(word|wd)|token|secret|otp|hash", re.I)


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
    cur.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
                "ORDER BY ORDINAL_POSITION", (MYSQL_DATABASE, table))
    cols = [r[0] for r in cur.fetchall() if not SECRET_COLUMN.search(r[0])]
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
    src = pymysql.connect(host=MYSQL_HOST, user="root", password=os.environ["EAGLE_MYSQL_PASSWORD"],
                          database=MYSQL_DATABASE, charset="utf8mb4")
    sf = s2v3._snowflake_connect()
    sf.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
    cur = src.cursor()
    cur.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' "
                "ORDER BY TABLE_NAME", (MYSQL_DATABASE,))
    tables = [r[0] for r in cur.fetchall() if r[0] not in AUTH_TABLES and (not only or r[0] in only)]
    log.info("Target %s.%s — %d table(s)", SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, len(tables))
    bad = []
    for t in tables:
        try:
            expected, loaded = load_table(src, sf, t)
            flag = "" if expected == loaded else f"  MISMATCH (source {expected})"
            log.info("  %-36s %9d rows%s", t, loaded, flag)
            if flag:
                bad.append(t)
        except Exception as e:
            log.error("  %-36s FAILED: %s", t, str(e)[:300])
            bad.append(t)
    log.info("Done — %d/%d tables loaded and counted", len(tables) - len(bad), len(tables))
    if bad:
        log.warning("Check: %s", ", ".join(bad))
    src.close()
    sf.close()
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
