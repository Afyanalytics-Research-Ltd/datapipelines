from __future__ import annotations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator

DAG_ID = "dataengineeringtest_dag"
HTTP_CONN_ID = "example_com"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["test", "http"],
) as dag:

    hit_example_com = HttpOperator(
        task_id="hit_example_com",
        http_conn_id=HTTP_CONN_ID,
        endpoint="",
        method="GET",
        log_response=True,
    )
