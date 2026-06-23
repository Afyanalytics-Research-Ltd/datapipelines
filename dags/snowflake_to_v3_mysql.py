from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import json
import logging

log = logging.getLogger(__name__)

SF_DB      = "HOSPITALS"
SF_SCHEMA  = "V3_READY"
CHUNK_SIZE = 10_000

SF_TO_MYSQL: dict = {
    "NUMBER":        "BIGINT",
    "FLOAT":         "DOUBLE",
    "BOOLEAN":       "TINYINT(1)",
    "TIMESTAMP_NTZ": "DATETIME",
    "TIMESTAMP_LTZ": "DATETIME",
    "TIMESTAMP_TZ":  "DATETIME",
    "DATE":          "DATE",
    "STRING":        "TEXT",
    "TEXT":          "TEXT",
    "VARIANT":       "JSON",
}

default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}


def sf_type_to_mysql(sf_type: str) -> str:
    for k, v in SF_TO_MYSQL.items():
        if k in sf_type.upper():
            return v
    return "TEXT"


with DAG(
    dag_id="snowflake_to_v3_mysql",
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_tasks=4,
    tags=["migration", "v3", "mysql", "multi-tenant"],
) as dag:

    @task
    def list_ready_tables() -> list:
        """List all tables in V3_READY with their column definitions."""
        sf = SnowflakeHook(snowflake_conn_id="snowflake_default")
        rows = sf.get_records(f"""
            SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE,
                   c.CHARACTER_MAXIMUM_LENGTH, c.ORDINAL_POSITION
            FROM {SF_DB}.INFORMATION_SCHEMA.COLUMNS c
            JOIN {SF_DB}.INFORMATION_SCHEMA.TABLES  t
              ON  c.TABLE_SCHEMA = t.TABLE_SCHEMA
             AND  c.TABLE_NAME   = t.TABLE_NAME
            WHERE c.TABLE_SCHEMA = '{SF_SCHEMA}'
              AND t.TABLE_TYPE   = 'BASE TABLE'
            ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
        """)
        tables = {}
        for table_name, col_name, data_type, is_nullable, char_len, _ in rows:
            tables.setdefault(table_name, []).append({
                "column":   col_name,
                "type":     data_type,
                "nullable": is_nullable,
                "char_len": char_len,
            })
        return [{"table": t, "columns": cols} for t, cols in tables.items()]

    @task
    def create_v3_mysql_tables(table_defs: list) -> None:
        """
        Create tables in V3 MySQL with:
          - tenant_id VARCHAR(64) NOT NULL  (always column 1)
          - all columns from V3_READY
          - composite PRIMARY KEY (tenant_id, <original_pks>)
          - index on tenant_id for fast per-tenant scans

        Set V3_PK_COLUMNS Airflow Variable to declare original PKs per table:
          { "patients": ["patient_id"], "encounters": ["encounter_id"] }
        Tables with no entry get tenant_id as the sole PK.
        """
        mysql   = MySqlHook(mysql_conn_id="v3_mysql_conn")
        pk_map: dict = json.loads(Variable.get("V3_PK_COLUMNS", default_var="{}"))
        conn = mysql.get_conn()
        cur  = conn.cursor()

        for tbl in table_defs:
            table = tbl["table"]
            cols  = tbl["columns"]

            col_defs    = []
            col_names   = [c["column"].lower() for c in cols]
            has_tenant  = "tenant_id" in col_names

            if not has_tenant:
                col_defs.append("`tenant_id` VARCHAR(64) NOT NULL")

            for c in cols:
                sf_type    = c["type"]
                mysql_type = sf_type_to_mysql(sf_type)
                # Use VARCHAR for short string columns instead of TEXT for indexability
                if mysql_type == "TEXT" and c["char_len"] and int(c["char_len"]) <= 255:
                    mysql_type = f"VARCHAR({c['char_len']})"
                null_clause = "NULL" if c["nullable"] == "YES" else "NOT NULL"
                col_defs.append(f"`{c['column'].lower()}` {mysql_type} {null_clause}")

            original_pks = pk_map.get(table, pk_map.get(table.lower(), []))
            pk_parts     = ["`tenant_id`"] + [f"`{c}`" for c in original_pks]
            col_defs.append(f"PRIMARY KEY ({', '.join(pk_parts)})")

            if original_pks:
                col_defs.append(f"INDEX `idx_tenant_id` (`tenant_id`)")

            create_sql = (
                f"CREATE TABLE IF NOT EXISTS `{table}` "
                f"({', '.join(col_defs)}) "
                f"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
            )
            cur.execute(create_sql)
            log.info("Ensured V3 MySQL table: %s (PKs: %s)", table, pk_parts)

        conn.commit()
        conn.close()

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def load_tables(table_defs: list) -> None:
        """
        Stream rows from Snowflake V3_READY → V3 MySQL using LIMIT/OFFSET pagination.
        Uses INSERT ... ON DUPLICATE KEY UPDATE for idempotent upserts.
        """
        sf    = SnowflakeHook(snowflake_conn_id="snowflake_default")
        mysql = MySqlHook(mysql_conn_id="v3_mysql_conn")

        sf_conn  = sf.get_conn()
        sf_cur   = sf_conn.cursor()
        my_conn  = mysql.get_conn()
        my_cur   = my_conn.cursor()

        failed = []

        for tbl in table_defs:
            table = tbl["table"]
            try:
                offset = 0
                total  = 0

                while True:
                    sf_cur.execute(f"""
                        SELECT * FROM "{SF_DB}"."{SF_SCHEMA}"."{table}"
                        ORDER BY 1
                        LIMIT {CHUNK_SIZE} OFFSET {offset}
                    """)
                    rows = sf_cur.fetchall()
                    if not rows:
                        break

                    col_names   = [desc[0].lower() for desc in sf_cur.description]
                    placeholders = ", ".join(["%s"] * len(col_names))
                    update_set   = ", ".join(
                        f"`{c}` = VALUES(`{c}`)"
                        for c in col_names
                        if c not in ("tenant_id",)
                    )
                    insert_sql = (
                        f"INSERT INTO `{table}` "
                        f"({', '.join(f'`{c}`' for c in col_names)}) "
                        f"VALUES ({placeholders}) "
                        f"ON DUPLICATE KEY UPDATE {update_set}"
                    )
                    my_cur.executemany(insert_sql, [tuple(r) for r in rows])
                    my_conn.commit()

                    total  += len(rows)
                    offset += CHUNK_SIZE
                    if len(rows) < CHUNK_SIZE:
                        break

                log.info("Loaded %s: %d rows → V3 MySQL", table, total)

            except Exception as e:
                log.error("Failed loading %s: %s", table, e, exc_info=True)
                my_conn.rollback()
                failed.append(table)

        sf_conn.close()
        my_conn.close()

        if failed:
            raise RuntimeError(f"Failed tables: {failed}")

    tables = list_ready_tables()
    create_v3_mysql_tables(tables) >> load_tables(tables)
