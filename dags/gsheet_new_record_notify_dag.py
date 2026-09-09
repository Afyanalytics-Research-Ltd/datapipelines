#!/usr/bin/env python3
"""
gsheet_new_record_poc_dag.py

PROOF OF CONCEPT -- minimal version. Polls a Google Sheet on a short
interval, detects new rows by comparing row count to the last run, and
sends a real email via smtplib when it finds any. Once you're happy
with the behavior, fold the relevant pieces into the fuller
gsheet_new_record_notify_dag.py (EmailOperator version, daily
schedule, richer HTML body) for the main project.

AUTH (same pattern as sheet_to_snowflake.py)
  Airflow Variable `GOOGLE_SA_JSON` -- service account JSON, already
  set up if your other Sheets pipelines are working.

REQUIRED AIRFLOW VARIABLES
  GOOGLE_SA_JSON                 (should already exist)
  GSHEET_POC_SPREADSHEET_ID      spreadsheet ID from the sheet's URL
  GSHEET_POC_WORKSHEET_NAME      default "Sheet1"
  GSHEET_POC_EMAIL_TO            where the notification goes
  SMTP_HOST                      e.g. smtp.gmail.com
  SMTP_PORT                      e.g. 587
  SMTP_USER                      the sending email address
  SMTP_PASSWORD                  app password (Gmail requires this, not your real password)

Set them with:
  airflow variables set GSHEET_POC_SPREADSHEET_ID "..."
  airflow variables set GSHEET_POC_WORKSHEET_NAME "Sheet1"
  airflow variables set GSHEET_POC_EMAIL_TO "you@example.com"
  airflow variables set SMTP_HOST "smtp.gmail.com"
  airflow variables set SMTP_PORT "587"
  airflow variables set SMTP_USER "your-notifier@gmail.com"
  airflow variables set SMTP_PASSWORD "your-app-password"

STATE
  Airflow Variable GSHEET_POC_LAST_ROW_COUNT, updated automatically.
  Delete it to reset (next run will treat all current rows as "new").

SCHEDULE
  Runs every minute for fast POC testing. Change schedule_interval
  before this goes anywhere near production.
"""

from __future__ import annotations

import json
import logging
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText

import gspread
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

LAST_ROW_COUNT_VAR = "GSHEET_POC_LAST_ROW_COUNT"


def get_gsheet_client():
    creds_dict = json.loads(Variable.get("GOOGLE_SA_JSON"))
    return gspread.service_account_from_dict(creds_dict)


def check_and_notify(**context) -> None:
    spreadsheet_id = Variable.get("GSHEET_POC_SPREADSHEET_ID")
    worksheet_name = Variable.get("GSHEET_POC_WORKSHEET_NAME", default_var="Sheet1")
    email_to = Variable.get("GSHEET_POC_EMAIL_TO")

    gc = get_gsheet_client()
    ws = gc.open_by_key(spreadsheet_id).worksheet(worksheet_name)
    all_values = ws.get_all_values()

    header = all_values[0] if all_values else []
    data_rows = all_values[1:] if all_values else []

    last_row_count = int(Variable.get(LAST_ROW_COUNT_VAR, default_var=0))
    current_row_count = len(data_rows)

    if current_row_count <= last_row_count:
        log.info("No new rows (%s -> %s).", last_row_count, current_row_count)
        return

    new_rows = data_rows[last_row_count:]
    log.info("Found %d new row(s). Sending email.", len(new_rows))

    lines = [", ".join(row) for row in new_rows]
    body = (
        f"{len(new_rows)} new record(s) arrived in '{worksheet_name}'.\n\n"
        f"Columns: {', '.join(header)}\n\n"
        + "\n".join(lines)
        + f"\n\nSheet: https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
    )

    send_email(
        to_addr=email_to,
        subject=f"[POC] {len(new_rows)} new record(s) in Google Sheet",
        body=body,
    )

    Variable.set(LAST_ROW_COUNT_VAR, current_row_count)


def send_email(to_addr: str, subject: str, body: str) -> None:
    smtp_host = Variable.get("SMTP_HOST")
    smtp_port = int(Variable.get("SMTP_PORT", default_var="587"))
    smtp_user = Variable.get("SMTP_USER")
    smtp_password = Variable.get("SMTP_PASSWORD")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_addr

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, [to_addr], msg.as_string())

    log.info("Email sent to %s", to_addr)


with DAG(
    dag_id="gsheet_new_record_poc_dag",
    description="POC: detects new rows in a Google Sheet and emails a notification.",
    default_args=default_args,
    schedule_interval="* * * * *",  # every minute -- POC only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["poc", "google-sheets", "email"],
) as dag:

    check_and_notify_task = PythonOperator(
        task_id="check_and_notify",
        python_callable=check_and_notify,
    )


