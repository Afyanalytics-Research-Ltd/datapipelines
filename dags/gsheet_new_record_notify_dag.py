#!/usr/bin/env python3
"""
gsheet_new_record_notify_dag.py

Standalone watcher: senses when new records land in a Google Sheet (the
back end for Kobo Collect / Google Forms / ODK field submissions) and
emails a reviewer so they can verify the data *before* it is sent to the
warehouse.

This DAG is deliberately self-contained -- its own Airflow Variables, its
own state, no imports from the rest of the project. Get it working on its
own first; wire the warehouse load in later as a separate DAG.

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
  GSHEET_WATCH_SA_JSON        service-account JSON (falls back to GOOGLE_SA_JSON)
  GSHEET_WATCH_SPREADSHEET_ID spreadsheet ID from the sheet URL
  GSHEET_WATCH_EMAIL_TO       reviewer address(es), comma-separated
  SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASSWORD
                             (or GSHEET_WATCH_SMTP_* to override just this DAG)

OPTIONAL AIRFLOW VARIABLES
  GSHEET_WATCH_WORKSHEET     tab name           (default "Form Responses 1")
  GSHEET_WATCH_TS_COLUMN     timestamp header   (default "Timestamp")
  GSHEET_WATCH_FILTERS       JSON object of {column: value} equality filters
                             (default {} -- no filtering)

STATE (managed automatically)
  GSHEET_WATCH_LAST_TS       ISO-8601 timestamp water mark
"""

from __future__ import annotations

import email.utils
import html
import json
import logging
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import gspread
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
        "sa_json": _var("GSHEET_WATCH_SA_JSON", "GOOGLE_SA_JSON", required=True),
        "spreadsheet_id": _var("GSHEET_WATCH_SPREADSHEET_ID", required=True),
        "worksheet": _var("GSHEET_WATCH_WORKSHEET", default=DEFAULT_WORKSHEET),
        "ts_column": _var("GSHEET_WATCH_TS_COLUMN", default=DEFAULT_TS_COLUMN),
        "filters": {str(k): str(v) for k, v in filters.items()},
        "email_to": _var("GSHEET_WATCH_EMAIL_TO", required=True),
        "smtp_host": _var("GSHEET_WATCH_SMTP_HOST", "SMTP_HOST", required=True),
        "smtp_port": int(_var("GSHEET_WATCH_SMTP_PORT", "SMTP_PORT", default="587")),
        "smtp_user": _var("GSHEET_WATCH_SMTP_USER", "SMTP_USER", required=True),
        "smtp_password": _var("GSHEET_WATCH_SMTP_PASSWORD", "SMTP_PASSWORD", required=True),
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
