#!/usr/bin/env python3
"""
gsheet_new_record_notify_dag.py

Standalone watcher: senses when new records land in a Google Sheet (the
back end for Kobo Collect / Google Forms / ODK field submissions) and
emails a reviewer so they can verify the data *before* it is sent to the
warehouse.

This DAG is deliberately self-contained -- its own Airflow Variables, its
own credentials, its own state, no imports from the rest of the project.
Every Variable/env var it reads is prefixed GSHEET_WATCH_ / GSHEET_LOAD_ and
is exclusive to this file -- none of them are also read by any other DAG or
script in this repo (even the Google service-account JSON and Snowflake/AWS
credentials get their own dedicated copies below, rather than sharing the
GOOGLE_SA_JSON / SNOWFLAKE_* / AWS_* names other pipelines use). That's
deliberate: this file can be deployed, rotated or torn out on its own
without checking what else in the project might be reading the same
Variable.

HOW IT WORKS
  A single @task.sensor polls the sheet on a short interval in
  ``reschedule`` mode (the worker slot is freed between pokes). "New" is
  decided by a *timestamp water mark*, not by row count -- edits,
  deletions and re-ordering all break a row count, a monotonically
  increasing submission timestamp does not.

  Optional equality filters (GSHEET_WATCH_FILTERS) let you scope which
  rows count as notify-worthy, e.g. only rows whose "Status" is
  "Submitted". The water mark still advances past everything so filtered
  rows are not re-evaluated forever.

  When at least one new (and filter-matching) row is found, the sensor
  completes, hands the rows to ``notify_reviewer`` via XCom, the email
  goes out, and only then is the water mark advanced -- a failed send
  re-notifies on the next run instead of silently dropping records.

  First run initialises the water mark to the current max timestamp and
  sends nothing (so you are not blasted with the whole sheet history).
  Delete GSHEET_WATCH_LAST_TS to re-initialise.

REQUIRED AIRFLOW VARIABLES
  GSHEET_WATCH_SA_JSON        service-account JSON, dedicated to this pipeline
                               (can hold the same value as GOOGLE_SA_JSON if
                               you're reusing the same service account, or a
                               separate one -- this file never reads
                               GOOGLE_SA_JSON itself)
  GSHEET_WATCH_SPREADSHEET_ID spreadsheet ID from the sheet URL
  GSHEET_WATCH_EMAIL_TO       reviewer address(es), comma-separated
  GSHEET_WATCH_SMTP_HOST / _SMTP_PORT / _SMTP_USER / _SMTP_PASSWORD
                             dedicated SMTP credentials (does not read the
                             shared SMTP_* Variables other pipelines may use)

OPTIONAL AIRFLOW VARIABLES
  GSHEET_WATCH_WORKSHEET     tab name           (default "Form Responses 1")
  GSHEET_WATCH_TS_COLUMN     timestamp header   (default "Timestamp")
  GSHEET_WATCH_FILTERS       JSON object of {column: value} equality filters
                             (default {} -- no filtering)

STATE (managed automatically)
  GSHEET_WATCH_LAST_TS       ISO-8601 timestamp water mark

--------------------------------------------------------------------------

SECOND DAG IN THIS FILE: gsheet_verified_to_warehouse

Once a reviewer has checked the digest email above, they mark a row as
approved by typing a value (default "YES") into a "Verified" column in the
sheet. This second, independent @continuous DAG watches for rows that are
Verified but not yet loaded, uploads them to S3 (the data lake, as CSV) and
COPY INTOs them into the warehouse -- then writes a "Loaded At" timestamp
back into the sheet so that row is never re-loaded.

It is a second @dag in this same file rather than more tasks bolted onto
gsheet_new_record_notify because the two loops must be independent: sensing
new records must keep running on its own 30s cycle even while a previous
batch is still awaiting human review, which could take hours.

A row's load status is tracked by writing directly into the sheet (the
"Loaded At" column), not by a timestamp water mark -- verification doesn't
happen in submission order, so a water mark could permanently skip a row
that gets verified late.

REQUIRED (sheet columns, in addition to Timestamp)
  Verified    reviewer types GSHEET_LOAD_VERIFIED_VALUE (default "YES") here
  Loaded At   left blank by you; the DAG fills this in once a row is loaded

REQUIRED (env, read via os.getenv -- dedicated names, prefixed GSHEET_LOAD_
so this file never reads the shared AWS_*/SNOWFLAKE_* env vars other
pipelines in this repo use; set them to the same values as those if you're
reusing the same AWS/Snowflake accounts, or point them at separate ones)
  GSHEET_LOAD_AWS_ACCESS_KEY_ID / _AWS_SECRET_ACCESS_KEY / _AWS_REGION
  GSHEET_LOAD_SNOWFLAKE_ACCOUNT / _SNOWFLAKE_USER / _SNOWFLAKE_WAREHOUSE
  GSHEET_LOAD_SNOWFLAKE_DATABASE
  GSHEET_LOAD_SNOWFLAKE_PRIVATE_KEY_PATH   PEM key-pair private key, no passphrase

OPTIONAL AIRFLOW VARIABLES
  GSHEET_LOAD_SPREADSHEET_ID / GSHEET_LOAD_WORKSHEET
                             (falls back to GSHEET_WATCH_SPREADSHEET_ID / _WORKSHEET)
  GSHEET_LOAD_TS_COLUMN      (falls back to GSHEET_WATCH_TS_COLUMN, default "Timestamp")
  GSHEET_LOAD_VERIFIED_COLUMN   (default "Verified")
  GSHEET_LOAD_VERIFIED_VALUE    (default "YES", case-insensitive)
  GSHEET_LOAD_LOADED_COLUMN     (default "Loaded At")
  GSHEET_LOAD_SOURCE_COLUMNS    JSON array of sheet columns to export
                                 (default ["Name", "Message", "Email"])

OPTIONAL (env)
  GSHEET_LOAD_S3_BUCKET      (default "collabmedbucket")
  GSHEET_LOAD_S3_PREFIX      (default "raw/gsheet_verified")
  GSHEET_LOAD_SNOWFLAKE_SCHEMA        (default "RAW")
  GSHEET_LOAD_SNOWFLAKE_ROLE
  GSHEET_LOAD_SNOWFLAKE_TABLE        (default "RAW.GSHEET_VERIFIED_RECORDS")
  GSHEET_LOAD_SNOWFLAKE_STAGE        (default "RAW.GSHEET_VERIFIED_STAGE")
  GSHEET_LOAD_SNOWFLAKE_FILE_FORMAT  (default "RAW.GSHEET_VERIFIED_CSV_FF")

  The stage/file format/table above must exist in Snowflake before this DAG
  runs -- see the CREATE STAGE / CREATE FILE FORMAT / CREATE TABLE DDL handed
  to you separately (scoped to this pipeline, does not touch any existing
  JSON-based stage).
"""

from __future__ import annotations

import csv
import email.utils
import html
import io
import json
import logging
import os
import smtplib
import uuid
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3
import gspread
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dateutil import parser as dateutil_parser

from airflow.exceptions import AirflowException

try:  # Airflow 3.x
    from airflow.sdk import PokeReturnValue, Variable, dag, task
except ImportError:  # older layout
    from airflow.decorators import dag, task
    from airflow.models import Variable
    from airflow.sensors.base import PokeReturnValue

log = logging.getLogger(__name__)

WATERMARK_VAR = "GSHEET_WATCH_LAST_TS"
DEFAULT_WORKSHEET = "Form Responses 1"
DEFAULT_TS_COLUMN = "Timestamp"

DEFAULT_VERIFIED_COLUMN = "Verified"
DEFAULT_VERIFIED_VALUE = "YES"
DEFAULT_LOADED_COLUMN = "Loaded At"
DEFAULT_SOURCE_COLUMNS = ["Name", "Message", "Email"]
DEFAULT_S3_BUCKET = "collab-med-bucket"
DEFAULT_S3_PREFIX = "raw/gsheet_verified"
DEFAULT_SF_TABLE = "RAW.GSHEET_VERIFIED_RECORDS"
DEFAULT_SF_STAGE = "RAW.GSHEET_VERIFIED_STAGE"
DEFAULT_SF_FILE_FORMAT = "RAW.GSHEET_VERIFIED_CSV_FF"

# Google Forms writes the timestamp in the spreadsheet's locale. Try a few
# explicit shapes, then fall back to a lenient parse.
_TS_FORMATS = (
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
)


# --------------------------------------------------------------------------- #
# config
# --------------------------------------------------------------------------- #
def _raw_var(name: str):
    """Variable.get across the airflow.sdk (default=) and airflow.models (default_var=) APIs."""
    try:
        return Variable.get(name, default=None)
    except TypeError:
        return Variable.get(name, default_var=None)


def _var(name: str, *fallbacks: str, default=None, required: bool = False):
    for candidate in (name, *fallbacks):
        value = _raw_var(candidate)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    if required:
        raise AirflowException(f"Airflow Variable '{name}' is required but not set.")
    return default


def load_config() -> dict:
    raw_filters = _var("GSHEET_WATCH_FILTERS", default="{}")
    try:
        filters = json.loads(raw_filters) or {}
        if not isinstance(filters, dict):
            raise ValueError
    except ValueError:
        raise AirflowException("GSHEET_WATCH_FILTERS must be a JSON object, e.g. {\"Status\": \"Submitted\"}.")

    return {
        "sa_json": _var("GSHEET_WATCH_SA_JSON", required=True),
        "spreadsheet_id": _var("GSHEET_WATCH_SPREADSHEET_ID", required=True),
        "worksheet": _var("GSHEET_WATCH_WORKSHEET", default=DEFAULT_WORKSHEET),
        "ts_column": _var("GSHEET_WATCH_TS_COLUMN", default=DEFAULT_TS_COLUMN),
        "filters": {str(k): str(v) for k, v in filters.items()},
        "email_to": _var("GSHEET_WATCH_EMAIL_TO", required=True),
        "smtp_host": _var("GSHEET_WATCH_SMTP_HOST", required=True),
        "smtp_port": int(_var("GSHEET_WATCH_SMTP_PORT", default="587")),
        "smtp_user": _var("GSHEET_WATCH_SMTP_USER", required=True),
        "smtp_password": _var("GSHEET_WATCH_SMTP_PASSWORD", required=True),
    }


def load_warehouse_config() -> dict:
    raw_source_cols = _var("GSHEET_LOAD_SOURCE_COLUMNS", default=json.dumps(DEFAULT_SOURCE_COLUMNS))
    try:
        source_columns = json.loads(raw_source_cols)
        if not isinstance(source_columns, list) or not all(isinstance(c, str) for c in source_columns):
            raise ValueError
    except ValueError:
        raise AirflowException("GSHEET_LOAD_SOURCE_COLUMNS must be a JSON array of column names.")

    return {
        "sa_json": _var("GSHEET_WATCH_SA_JSON", required=True),
        "spreadsheet_id": _var("GSHEET_LOAD_SPREADSHEET_ID", "GSHEET_WATCH_SPREADSHEET_ID", required=True),
        "worksheet": _var("GSHEET_LOAD_WORKSHEET", "GSHEET_WATCH_WORKSHEET", default=DEFAULT_WORKSHEET),
        "ts_column": _var("GSHEET_LOAD_TS_COLUMN", "GSHEET_WATCH_TS_COLUMN", default=DEFAULT_TS_COLUMN),
        "verified_column": _var("GSHEET_LOAD_VERIFIED_COLUMN", default=DEFAULT_VERIFIED_COLUMN),
        "verified_value": _var("GSHEET_LOAD_VERIFIED_VALUE", default=DEFAULT_VERIFIED_VALUE),
        "loaded_column": _var("GSHEET_LOAD_LOADED_COLUMN", default=DEFAULT_LOADED_COLUMN),
        "source_columns": source_columns,
        "s3_bucket": os.getenv("GSHEET_LOAD_S3_BUCKET", DEFAULT_S3_BUCKET),
        "s3_prefix": os.getenv("GSHEET_LOAD_S3_PREFIX", DEFAULT_S3_PREFIX).strip("/"),
        "aws_access_key_id": os.getenv("GSHEET_LOAD_AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("GSHEET_LOAD_AWS_SECRET_ACCESS_KEY"),
        "aws_region": os.getenv("GSHEET_LOAD_AWS_REGION", "us-east-1"),
        "sf_account": os.getenv("GSHEET_LOAD_SNOWFLAKE_ACCOUNT"),
        "sf_user": os.getenv("GSHEET_LOAD_SNOWFLAKE_USER"),
        "sf_warehouse": os.getenv("GSHEET_LOAD_SNOWFLAKE_WAREHOUSE"),
        "sf_database": os.getenv("GSHEET_LOAD_SNOWFLAKE_DATABASE"),
        "sf_schema": os.getenv("GSHEET_LOAD_SNOWFLAKE_SCHEMA", "RAW"),
        "sf_role": os.getenv("GSHEET_LOAD_SNOWFLAKE_ROLE"),
        "sf_private_key_path": os.getenv("GSHEET_LOAD_SNOWFLAKE_PRIVATE_KEY_PATH"),
        "sf_table": os.getenv("GSHEET_LOAD_SNOWFLAKE_TABLE", DEFAULT_SF_TABLE),
        "sf_stage": os.getenv("GSHEET_LOAD_SNOWFLAKE_STAGE", DEFAULT_SF_STAGE),
        "sf_file_format": os.getenv("GSHEET_LOAD_SNOWFLAKE_FILE_FORMAT", DEFAULT_SF_FILE_FORMAT),
    }


# --------------------------------------------------------------------------- #
# sheet access
# --------------------------------------------------------------------------- #
def fetch_rows(cfg: dict) -> tuple[list[str], list[dict]]:
    """Return (header, rows-as-dicts). Short rows are padded to header width."""
    gc = gspread.service_account_from_dict(json.loads(cfg["sa_json"]))
    ws = gc.open_by_key(cfg["spreadsheet_id"]).worksheet(cfg["worksheet"])
    values = ws.get_all_values()
    if not values:
        return [], []
    header = [h.strip() for h in values[0]]
    width = len(header)
    rows = [dict(zip(header, (r + [""] * width)[:width])) for r in values[1:]]
    return header, rows


def apply_filters(rows: list[dict], filters: dict) -> list[dict]:
    if not filters:
        return rows
    return [
        row for row in rows
        if all(str(row.get(col, "")).strip() == val.strip() for col, val in filters.items())
    ]


def parse_ts(raw: str) -> datetime | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    for fmt in _TS_FORMATS:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    try:
        return dateutil_parser.parse(raw, dayfirst=False).replace(tzinfo=None)
    except (ValueError, OverflowError):
        return None


def detect_new_rows(header: list[str], rows: list[dict], ts_column: str,
                    watermark: datetime | None) -> tuple[list[dict], datetime | None]:
    if ts_column not in header:
        raise AirflowException(
            f"Timestamp column {ts_column!r} not in sheet headers: {header}"
        )

    parsed: list[tuple[datetime, dict]] = []
    unparseable = 0
    for row in rows:
        ts = parse_ts(row.get(ts_column, ""))
        if ts is None:
            unparseable += 1
        else:
            parsed.append((ts, row))

    if rows and not parsed:
        raise AirflowException(
            f"No parseable timestamp in column {ts_column!r} across {len(rows)} row(s); "
            "check GSHEET_WATCH_TS_COLUMN."
        )
    if unparseable:
        log.warning("Skipped %d row(s) with an unparseable %r.", unparseable, ts_column)

    max_seen = max((ts for ts, _ in parsed), default=None)
    if watermark is None:
        return [], max_seen
    new_rows = [r for ts, r in sorted(parsed, key=lambda p: p[0]) if ts > watermark]
    return new_rows, max_seen


# --------------------------------------------------------------------------- #
# Email
# --------------------------------------------------------------------------- #
def _html_digest(records: list[dict], header: list[str], worksheet: str,
                 spreadsheet_id: str) -> str:
    cols = header or list(records[0].keys())
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    body = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(r.get(c, '')))}</td>" for c in cols) + "</tr>"
        for r in records
    )
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
    return f"""\
<html><body style="font-family:Arial,Helvetica,sans-serif;color:#222;">
<p><strong>{len(records)}</strong> new record(s) landed in
<strong>{html.escape(worksheet)}</strong> and need verification before they
are sent to the warehouse.</p>
<table border="1" cellpadding="6" cellspacing="0"
       style="border-collapse:collapse;font-size:13px;">
<thead style="background:#f0f0f0;"><tr>{head}</tr></thead>
<tbody>{body}</tbody>
</table>
<p>Review in the source sheet: <a href="{url}">{url}</a></p>
<p style="color:#888;font-size:12px;">Sent by the
<code>gsheet_new_record_notify</code> Airflow DAG.</p>
</body></html>"""


def send_email(cfg: dict, subject: str, text_body: str, html_body: str) -> None:
    recipients = [a.strip() for a in cfg["email_to"].split(",") if a.strip()]
    if not recipients:
        raise AirflowException("GSHEET_WATCH_EMAIL_TO has no valid address.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = cfg["smtp_user"]
    msg["To"] = ", ".join(recipients)
    msg["Date"] = email.utils.formatdate(localtime=True)
    msg["Message-ID"] = email.utils.make_msgid(domain=cfg["smtp_host"])
    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    port = cfg["smtp_port"]
    server = (smtplib.SMTP_SSL if port == 465 else smtplib.SMTP)(
        cfg["smtp_host"], port, timeout=30
    )
    try:
        if port != 465:
            server.starttls()
        server.login(cfg["smtp_user"], cfg["smtp_password"])
        server.sendmail(cfg["smtp_user"], recipients, msg.as_string())
    finally:
        server.quit()
    log.info("Verification email sent to %s", ", ".join(recipients))


# --------------------------------------------------------------------------- #
# warehouse load: S3 (data lake) -> Snowflake COPY INTO
# --------------------------------------------------------------------------- #
def build_csv(rows: list[dict], columns: list[str]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([row.get(col, "") for col in columns])
    return buf.getvalue().encode("utf-8")


def upload_to_s3(cfg: dict, key: str, body: bytes) -> None:
    if not cfg["aws_access_key_id"] or not cfg["aws_secret_access_key"]:
        raise AirflowException("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are not set.")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=cfg["aws_access_key_id"],
        aws_secret_access_key=cfg["aws_secret_access_key"],
        region_name=cfg["aws_region"],
    )
    s3.put_object(Bucket=cfg["s3_bucket"], Key=key, Body=body, ContentType="text/csv")


def _snowflake_connect(cfg: dict):
    key_path = cfg["sf_private_key_path"]
    if not key_path:
        raise AirflowException("SNOWFLAKE_PRIVATE_KEY_PATH is not set.")
    try:
        with open(key_path, "rb") as f:
            p_key = serialization.load_pem_private_key(f.read(), password=None)
    except FileNotFoundError:
        raise AirflowException(
            f"Snowflake private key not found at {key_path!r}. Place the key-pair "
            "PEM file there (mounted into the containers via the config/ volume)."
        )
    private_key_der = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        user=cfg["sf_user"],
        account=cfg["sf_account"],
        warehouse=cfg["sf_warehouse"],
        database=cfg["sf_database"],
        schema=cfg["sf_schema"],
        role=cfg["sf_role"],
        private_key=private_key_der,
    )


def copy_into_snowflake(cfg: dict, stage_sub_path: str, spreadsheet_id: str, worksheet: str) -> int:
    target_cols = [c.lower().replace(" ", "_") for c in cfg["source_columns"]]
    target_cols += ["submitted_at", "verified_by_flag", "source_spreadsheet_id", "source_worksheet"]
    select_list = ", ".join(f"${i + 1}" for i in range(len(cfg["source_columns"]) + 2))

    sql = f"""
        COPY INTO {cfg['sf_table']} ({', '.join(target_cols)})
        FROM (
            SELECT {select_list}, '{spreadsheet_id}', '{worksheet}'
            FROM @{cfg['sf_stage']}/{stage_sub_path}
        )
        FILE_FORMAT = (FORMAT_NAME = {cfg['sf_file_format']})
    """

    conn = _snowflake_connect(cfg)
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            colnames = [d[0].lower() for d in cur.description]
            result = cur.fetchall()
        finally:
            cur.close()
    finally:
        conn.close()

    if not result:
        raise AirflowException(f"COPY INTO {cfg['sf_table']} returned no result rows.")
    rows_loaded = sum(r[colnames.index("rows_loaded")] for r in result)
    if rows_loaded == 0:
        raise AirflowException(f"COPY INTO {cfg['sf_table']} loaded 0 rows. Result: {result}")
    log.info("COPY INTO %s result: %s", cfg["sf_table"], result)
    return rows_loaded


def mark_rows_loaded(cfg: dict, header: list[str], row_numbers: list[int]) -> None:
    col_idx = header.index(cfg["loaded_column"]) + 1  # gspread columns are 1-based
    gc = gspread.service_account_from_dict(json.loads(cfg["sa_json"]))
    ws = gc.open_by_key(cfg["spreadsheet_id"]).worksheet(cfg["worksheet"])
    stamp = datetime.now().isoformat(timespec="seconds")
    ws.batch_update([
        {"range": gspread.utils.rowcol_to_a1(row_num, col_idx), "values": [[stamp]]}
        for row_num in row_numbers
    ])


# --------------------------------------------------------------------------- #
# DAG
# --------------------------------------------------------------------------- #
@dag(
    dag_id="gsheet_new_record_notify",
    description="Sense new Google Sheet records by timestamp and email a reviewer to verify them.",
    schedule="@continuous",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["google-sheets", "email", "field-data", "verification"],
)
def gsheet_new_record_notify():

    @task.sensor(poke_interval=30, timeout=60 * 60 * 6, mode="reschedule", soft_fail=True)
    def wait_for_new_records() -> PokeReturnValue:
        cfg = load_config()
        raw_wm = _raw_var(WATERMARK_VAR)
        watermark = datetime.fromisoformat(raw_wm) if raw_wm else None

        header, rows = fetch_rows(cfg)
        if not header:
            log.info("Worksheet %r is empty; still watching.", cfg["worksheet"])
            return PokeReturnValue(is_done=False)

        rows = apply_filters(rows, cfg["filters"])
        new_rows, max_seen = detect_new_rows(header, rows, cfg["ts_column"], watermark)

        if watermark is None:
            init = (max_seen or datetime.now()).isoformat()
            Variable.set(WATERMARK_VAR, init)
            log.info("Initialised %s to %s; no email on first run.", WATERMARK_VAR, init)
            return PokeReturnValue(is_done=False)

        if not new_rows:
            log.info("No rows newer than %s.", watermark.isoformat())
            return PokeReturnValue(is_done=False)

        log.info("Found %d new row(s) since %s.", len(new_rows), watermark.isoformat())
        return PokeReturnValue(
            is_done=True,
            xcom_value={
                "rows": new_rows,
                "header": header,
                "worksheet": cfg["worksheet"],
                "spreadsheet_id": cfg["spreadsheet_id"],
                "new_watermark": (max_seen or watermark).isoformat(),
            },
        )

    @task
    def notify_reviewer(payload: dict) -> None:
        cfg = load_config()
        records = payload["rows"]
        header = payload["header"]
        worksheet = payload["worksheet"]
        spreadsheet_id = payload["spreadsheet_id"]

        text_body = (
            f"{len(records)} new record(s) in '{worksheet}' need verification before "
            f"they are sent to the warehouse.\n\n"
            + "\n".join(", ".join(f"{k}={v}" for k, v in r.items()) for r in records)
            + f"\n\nSheet: https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        )
        send_email(
            cfg,
            subject=f"[Verify] {len(records)} new field record(s) in '{worksheet}'",
            text_body=text_body,
            html_body=_html_digest(records, header, worksheet, spreadsheet_id),
        )

        # Advance the water mark only after a successful send.
        Variable.set(WATERMARK_VAR, payload["new_watermark"])
        log.info("Advanced %s to %s.", WATERMARK_VAR, payload["new_watermark"])

    notify_reviewer(wait_for_new_records())


gsheet_new_record_notify()


@dag(
    dag_id="gsheet_verified_to_warehouse",
    description="Upload reviewer-verified Google Sheet records to S3 and COPY them into the warehouse.",
    schedule="@continuous",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["google-sheets", "s3", "snowflake", "warehouse", "field-data"],
)
def gsheet_verified_to_warehouse():

    @task.sensor(poke_interval=30, timeout=60 * 60 * 6, mode="reschedule", soft_fail=True)
    def wait_for_verified_records() -> PokeReturnValue:
        cfg = load_warehouse_config()
        header, rows = fetch_rows(cfg)
        if not header:
            log.info("Worksheet %r is empty; still watching.", cfg["worksheet"])
            return PokeReturnValue(is_done=False)

        required_cols = [*cfg["source_columns"], cfg["ts_column"], cfg["verified_column"], cfg["loaded_column"]]
        missing = [c for c in required_cols if c not in header]
        if missing:
            raise AirflowException(f"Column(s) {missing} not in sheet headers: {header}")

        candidates = [
            (row_num, row)
            for row_num, row in zip(range(2, 2 + len(rows)), rows)
            if row.get(cfg["verified_column"], "").strip().casefold() == cfg["verified_value"].strip().casefold()
            and not row.get(cfg["loaded_column"], "").strip()
        ]
        if not candidates:
            log.info("No verified-but-unloaded rows.")
            return PokeReturnValue(is_done=False)

        log.info("Found %d verified row(s) ready to load.", len(candidates))
        return PokeReturnValue(
            is_done=True,
            xcom_value={
                "row_numbers": [rn for rn, _ in candidates],
                "rows": [row for _, row in candidates],
                "header": header,
                "worksheet": cfg["worksheet"],
                "spreadsheet_id": cfg["spreadsheet_id"],
            },
        )

    @task
    def upload_and_load(payload: dict) -> None:
        cfg = load_warehouse_config()
        rows = payload["rows"]
        row_numbers = payload["row_numbers"]
        columns = [*cfg["source_columns"], cfg["ts_column"], cfg["verified_column"]]

        csv_bytes = build_csv(rows, columns)
        sub_path = (
            f"spreadsheet_id={payload['spreadsheet_id']}/worksheet={payload['worksheet']}/"
            f"dt={datetime.now().strftime('%Y-%m-%d')}/{uuid.uuid4().hex}.csv"
        )
        key = f"{cfg['s3_prefix']}/{sub_path}"
        upload_to_s3(cfg, key, csv_bytes)
        log.info("Uploaded %d row(s) to s3://%s/%s", len(rows), cfg["s3_bucket"], key)

        rows_loaded = copy_into_snowflake(cfg, sub_path, payload["spreadsheet_id"], payload["worksheet"])
        log.info("COPY INTO %s loaded %d row(s).", cfg["sf_table"], rows_loaded)

        mark_rows_loaded(cfg, payload["header"], row_numbers)
        log.info("Marked %d row(s) as loaded (sheet rows: %s).", len(row_numbers), row_numbers)

    upload_and_load(wait_for_verified_records())


gsheet_verified_to_warehouse()
