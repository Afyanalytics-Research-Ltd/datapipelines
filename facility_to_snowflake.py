#!/usr/bin/env python3
"""
facility_api_to_snowflake.py  —  standalone (no Airflow) pipeline runner.

  1. Reads the data-dictionary Google Sheet to know which (module, table)
     pairs to extract.
  2. For each pair, calls the facility's /api/finance/access/data/point
     endpoint (with namespace + singular/double-namespace fallbacks),
     paginating until done.
  3. Writes each model's payload as gzipped JSONL to S3.
  4. COPYs each S3 file into the facility's RAW.EVENTS_RAW table.
  5. MERGEs RAW.EVENTS_RAW → CLEAN.EVENTS.

USAGE
  python facility_api_to_snowflake.py --facility kisumu
  python facility_api_to_snowflake.py --facility xanalife --since 2025-09-01
  python facility_api_to_snowflake.py --facility xanalife --skip-merge
  python facility_api_to_snowflake.py --facility xanalife --only-tables sales,patients
  python facility_api_to_snowflake.py --facility xanalife --dry-run         # extract only, no S3/Snowflake

ENV VARS  (put them in a `.env` file next to this script — auto-loaded)
  # Snowflake (key-pair auth)
  SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_PRIVATE_KEY_PATH

  # AWS  (or use the default boto3 credential chain — ~/.aws/credentials)
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

  # Google service account for the data-dictionary sheet
  GOOGLE_SA_JSON_PATH=/abs/path/to/service-account.json
  IGNITE_SHEET_ID=...
  IGNITE_SHEET_WORKSHEET=Sheet1

  # Facility API credentials  (per facility you want to run)
  FACILITY_KISUMU_USERNAME=...,    FACILITY_KISUMU_PASSWORD=...
  FACILITY_XANALIFE_USERNAME=...,  FACILITY_XANALIFE_PASSWORD=...
  ...
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import os
import re
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import boto3
import gspread
import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, HTTPError, Timeout

# Load .env from same dir as this script (override env so .env wins on conflicts)
load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

# ─── LOGGING ─────────────────────────────────────────────────────────────

log = logging.getLogger("facility_pipeline")
if not log.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(name)s · %(message)s",
        datefmt="%H:%M:%S",
    ))
    log.addHandler(h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ─── CONFIG ──────────────────────────────────────────────────────────────

PIPELINE_NAME = "facility_api_to_snowflake"

FACILITIES = {
    "afya_api_auth": {"base_url": "https://staging.afyanalytics.ai", "db": "staging_db"},
    "kakamega":      {"base_url": "https://demo.collabmed.net",      "db": "kakamega_db"},
    "kisumu":        {"base_url": "https://kshospital.collabmed.net","db": "kisumu_db"},
    "lodwar":        {"base_url": "https://lcrh.collabmed.net",      "db": "lodwar_db"},
    "tenri":         {"base_url": "https://stageenv.collabmed.net",  "db": "tenri_db"},
    "xanalife":      {"base_url": "https://xanalife.afyanalytics.ai/", "db": "xanalife_db"},
}

S3_BUCKET = "collabmedbucket"
S3_PREFIX = "raw/facilities"

SF_DB            = "HOSPITALS"
SF_SHARED_SCHEMA = "SHARED"
SF_STAGE         = f"{SF_DB}.{SF_SHARED_SCHEMA}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT   = f"{SF_DB}.{SF_SHARED_SCHEMA}.JSON_FF"

WATERMARK_FILE = Path(__file__).resolve().parent / ".watermarks.json"

# ─── INFLECT (lazy, avoids slow import at module load) ───────────────────

_inflect_engine = None
def _get_inflect():
    global _inflect_engine
    if _inflect_engine is None:
        import inflect
        _inflect_engine = inflect.engine()
    return _inflect_engine

# ─── WATERMARKS  (local file replacing Airflow Variables) ────────────────

def load_watermarks() -> dict:
    if WATERMARK_FILE.exists():
        try:
            return json.loads(WATERMARK_FILE.read_text())
        except Exception as e:
            log.warning("Could not parse %s: %s — starting fresh", WATERMARK_FILE, e)
    return {}

def get_watermark(facility: str, default: str = "1970-01-01T00:00:00Z") -> str:
    return load_watermarks().get(facility, default)

def set_watermark(facility: str, ts_iso: str) -> None:
    wm = load_watermarks()
    wm[facility] = ts_iso
    WATERMARK_FILE.write_text(json.dumps(wm, indent=2, sort_keys=True))
    log.info("Watermark for %s set → %s", facility, ts_iso)

# ─── SNOWFLAKE CLIENT ────────────────────────────────────────────────────

class SnowflakeClient:
    """Read (`query`) + write (`execute`) with structured logging.
    Authenticates via key-pair using SNOWFLAKE_* env vars."""

    def __init__(self, schema_: str | None = None):
        with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip(), "rb") as key:
            key.read()  # presence check; the connector reads the path itself

        self._conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER").strip(),
            account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(),
            database=os.getenv("SNOWFLAKE_DATABASE").strip(),
            schema=schema_ or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC").strip(),
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip(),
        )

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    @contextmanager
    def _cursor(self):
        cur = self._conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def query(self, sql: str, label: str | None = None) -> pd.DataFrame:
        label = label or f"q:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-26s SELECT    | %s…", label, " ".join(sql.split())[:140])
        t0 = time.perf_counter()
        try:
            with self._cursor() as cur:
                cur.execute(sql)
                df = cur.fetch_pandas_all()
            elapsed = time.perf_counter() - t0
            from decimal import Decimal
            for col in df.columns:
                if df[col].dtype == "object":
                    nn = df[col].dropna()
                    if len(nn) and isinstance(nn.iloc[0], Decimal):
                        df[col] = df[col].astype(float)
            log.info("✓ %-26s done      | %s rows · %d cols · %.2fs",
                     label, f"{len(df):,}", df.shape[1], elapsed)
            return df
        except Exception as e:
            log.exception("✗ %-26s SELECT failed | %.2fs · %s",
                          label, time.perf_counter() - t0, e)
            raise

    def execute(self, sql: str, label: str | None = None) -> dict:
        label = label or f"x:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-26s WRITE     | %s…", label, " ".join(sql.split())[:140])
        t0 = time.perf_counter()
        try:
            with self._cursor() as cur:
                cur.execute(sql)
                rowcount = cur.rowcount
                sfqid = cur.sfqid
            elapsed = time.perf_counter() - t0
            log.info("✓ %-26s done      | rowcount=%s · sfqid=%s · %.2fs",
                     label, rowcount, sfqid, elapsed)
            return {"rowcount": rowcount, "sfqid": sfqid, "elapsed_s": elapsed}
        except Exception as e:
            log.exception("✗ %-26s WRITE failed | %.2fs · %s",
                          label, time.perf_counter() - t0, e)
            raise

    def __enter__(self): return self
    def __exit__(self, *a): self.close()

# ─── GOOGLE SHEET  (data dictionary) ─────────────────────────────────────

def get_gsheet_client():
    sa_path = os.getenv("GOOGLE_SA_JSON_PATH")
    if sa_path:
        return gspread.service_account(filename=sa_path)
    sa_json = os.getenv("GOOGLE_SA_JSON")
    if sa_json:
        return gspread.service_account_from_dict(json.loads(sa_json))
    raise RuntimeError("Set GOOGLE_SA_JSON_PATH (file path) or GOOGLE_SA_JSON (raw JSON)")

def read_dictionary_sheet(spreadsheet_id: str, worksheet_name: str):
    gc = get_gsheet_client()
    ws = gc.open_by_key(spreadsheet_id).worksheet(worksheet_name)
    return ws.get_all_records()

# ─── NAMESPACE BUILDERS ──────────────────────────────────────────────────

def snake_to_pascal(s: str) -> str:
    return "".join(w.capitalize() for w in re.split(r"[_\s]+", s.strip()) if w)

def build_namespace(module: str, table: str) -> str:
    mod = snake_to_pascal(module)
    prefix = module.strip().lower() + "_"
    t = table.strip().lower()
    if t.startswith(prefix):
        t = t[len(prefix):]
    return f"Ignite\\{mod}\\Entities\\{snake_to_pascal(t)}"

def namespace_to_singular_model(namespace: str) -> str:
    parts = namespace.split("\\")
    if not parts:
        return namespace
    class_name = parts[-1]
    singular = _get_inflect().singular_noun(class_name)
    parts[-1] = singular if singular else class_name
    return "\\".join(parts)

def double_namespace_model(namespace: str) -> str:
    parts = namespace.split("\\")
    if not parts:
        return namespace
    parts[-1] = parts[1] + parts[-1]
    return "\\".join(parts)

def _safe_s3_token(s: str) -> str:
    s = (s or "").strip()
    return re.sub(r"[^a-zA-Z0-9_\-=\.\+]+", "_", s)

# ─── JOB BUILDER ─────────────────────────────────────────────────────────

def build_jobs_for_facility(facility: str, since: str | None = None,
                             only_tables: set[str] | None = None) -> list[dict]:
    cfg = FACILITIES[facility]
    last_run = since or get_watermark(facility)

    sheet_id  = os.environ["IGNITE_SHEET_ID"]
    sheet_tab = os.getenv("IGNITE_SHEET_WORKSHEET", "Sheet1")
    rows = read_dictionary_sheet(sheet_id, sheet_tab)

    seen, jobs = set(), []
    for r in rows:
        module = (r.get("module") or "").strip()
        table  = (r.get("table") or "").strip()
        if not module or not table:
            continue
        if only_tables and table.lower() not in only_tables:
            continue

        key = (module.lower(), table.lower())
        if key in seen:
            continue
        seen.add(key)

        jobs.append({
            "facility":      facility,
            "module":        module,
            "table":         table,
            "namespace":     build_namespace(module, table),
            "database":      cfg.get("db"),
            "updated_since": last_run,
            "limit":         500,
        })

    log.info("Prepared %d model jobs for facility=%s (since %s)",
             len(jobs), facility, last_run)
    return jobs

# ─── AUTH  (replaces Airflow's BaseHook.get_connection) ──────────────────

def generate_auth_token(facility: str) -> str:
    cfg = FACILITIES[facility]
    upper = facility.upper()
    user = os.getenv(f"FACILITY_{upper}_USERNAME")
    pwd  = os.getenv(f"FACILITY_{upper}_PASSWORD")
    if not user or not pwd:
        raise RuntimeError(
            f"Missing FACILITY_{upper}_USERNAME / FACILITY_{upper}_PASSWORD env vars"
        )

    url = f"{cfg['base_url'].rstrip('/')}/api/users/authenticate/user"
    r = requests.post(url, json={"username": user, "password": pwd}, timeout=30)
    if r.status_code != 200:
        raise Exception(f"Auth failed for {facility}: {r.status_code} · {r.text}")
    token = (r.json().get("success") or {}).get("token")
    if not token:
        raise Exception(f"Token not found in response for {facility}")
    return token

# ─── HTTP RETRY + FALLBACK ───────────────────────────────────────────────

def post_with_retry_and_fallback(
    url, headers, bodies, *, timeout=60, max_retries=6,
    default_retry_wait=10, backoff_factor=2, base_delay=0.5,
):
    for body_index, base_body in enumerate(bodies):
        attempt, wait_time = 0, default_retry_wait
        while True:
            attempt += 1
            try:
                r = requests.post(url=url, headers=headers, json=base_body, timeout=timeout)
                log.info("BodyIndex=%s Attempt=%s Status=%s Resp=%.300s",
                         body_index, attempt, r.status_code, r.text)

                if r.status_code == 404:
                    log.warning("404 for ns=%s — trying next fallback", base_body.get("namespace"))
                    break

                if r.status_code == 429:
                    retry_after = default_retry_wait
                    try:
                        retry_after = int(r.json().get("retry_after_seconds", default_retry_wait))
                    except Exception:
                        pass
                    if attempt >= max_retries:
                        r.raise_for_status()
                    log.warning("429 ns=%s · sleeping %ss (%s/%s)",
                                base_body.get("namespace"), retry_after, attempt, max_retries)
                    time.sleep(retry_after)
                    continue

                if r.status_code in {500, 502, 503, 504}:
                    if attempt >= max_retries:
                        r.raise_for_status()
                    time.sleep(wait_time)
                    wait_time *= backoff_factor
                    continue

                r.raise_for_status()
                if base_delay > 0:
                    time.sleep(base_delay)
                return r, base_body

            except (Timeout, ConnectionError) as e:
                if attempt >= max_retries:
                    raise
                log.warning("Network error ns=%s · %s · sleeping %ss (%s/%s)",
                            base_body.get("namespace"), e, wait_time, attempt, max_retries)
                time.sleep(wait_time)
                wait_time *= backoff_factor
            except HTTPError:
                raise

    raise Exception("All fallback request bodies returned 404")

def extract_all_pages(url, headers, body, singular_body,
                      double_namespace_body, double_namespace_singular_body,
                      *, timeout=60, max_pages=10000, max_retries=6,
                      default_retry_wait=10, backoff_factor=2, base_delay=0.5):

    def extract_rows(payload: dict) -> list:
        rows = payload.get("data")
        if rows is None:
            sv = payload.get("success")
            rows = sv.get("data") or [] if isinstance(sv, dict) else []
        if isinstance(rows, dict):
            rows = rows.get("data") or []
        elif not isinstance(rows, list):
            rows = []
        return rows

    page = 1
    candidate_bodies = [
        {**body, "page": page},
        {**singular_body, "page": page},
        {**double_namespace_body, "page": page},
        {**double_namespace_singular_body, "page": page},
    ]

    r, chosen_body = post_with_retry_and_fallback(
        url=url, headers=headers, bodies=candidate_bodies,
        timeout=timeout, max_retries=max_retries,
        default_retry_wait=default_retry_wait,
        backoff_factor=backoff_factor, base_delay=base_delay,
    )
    payload  = r.json()
    all_rows = extract_rows(payload)

    pagination = payload.get("pagination") or {}
    has_more   = bool(pagination.get("has_more_pages", False))
    last_page  = pagination.get("last_page")

    while has_more:
        page += 1
        if page > max_pages:
            log.info("Pagination safety stop (max_pages=%s)", max_pages)
            break
        if last_page is not None and page > int(last_page):
            break

        next_body = {**chosen_body, "page": page}
        r, chosen_body = post_with_retry_and_fallback(
            url=url, headers=headers, bodies=[next_body],
            timeout=timeout, max_retries=max_retries,
            default_retry_wait=default_retry_wait,
            backoff_factor=backoff_factor, base_delay=base_delay,
        )
        payload  = r.json()
        rows = extract_rows(payload)
        all_rows.extend(rows)
        pagination = payload.get("pagination") or {}
        has_more   = bool(pagination.get("has_more_pages", False))
        last_page  = pagination.get("last_page", last_page)
        if not rows:
            break

    return all_rows, chosen_body

# ─── PIPELINE STEPS ──────────────────────────────────────────────────────

def sf_schema(facility: str, layer: str) -> str:
    return f"{SF_DB}.{facility.upper()}_{layer}"

def extract_one_model(job: dict, run_id: str, dry_run: bool = False) -> dict | None:
    facility = job["facility"]
    cfg      = FACILITIES[facility]

    url   = f"{cfg['base_url'].rstrip('/')}/api/finance/access/data/point"
    token = generate_auth_token(facility)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    body = {
        "namespace": job["namespace"], "action": "get",
        "database": job["database"], "updated_since": job["updated_since"],
        "limit": job["limit"],
    }
    singular_body                  = {**body, "namespace": namespace_to_singular_model(job["namespace"])}
    double_namespace_body          = {**body, "namespace": double_namespace_model(job["namespace"])}
    double_namespace_singular_body = {**body, "namespace": double_namespace_model(namespace_to_singular_model(job["namespace"]))}

    rows, _ = extract_all_pages(
        url=url, headers=headers, body=body,
        singular_body=singular_body,
        double_namespace_body=double_namespace_body,
        double_namespace_singular_body=double_namespace_singular_body,
        timeout=60, max_pages=10000,
    )

    ingested_at = datetime.now(timezone.utc)
    dt = ingested_at.date().isoformat()
    ns_safe     = _safe_s3_token(job["namespace"].replace("\\", "_"))
    module_safe = _safe_s3_token(job.get("module", ""))
    table_safe  = _safe_s3_token(job.get("table", ""))

    key_prefix = (
        f"{S3_PREFIX}/"
        f"facility_id={facility}/"
        f"module={module_safe or 'unknown'}/"
        f"table={table_safe or 'unknown'}/"
        f"namespace={ns_safe}/"
        f"dt={dt}/"
    )

    jsonl = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows)
    if jsonl:
        jsonl += "\n"

    key = f"{key_prefix}{run_id}.jsonl.gz"

    if dry_run:
        log.info("DRY-RUN ✓ %-22s %s rows (would upload to s3://%s/%s)",
                 job["table"], len(rows), S3_BUCKET, key)
        return None

    ak = os.getenv("AWS_ACCESS_KEY_ID")
    sk = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not (ak and sk):
        raise RuntimeError(
            "AWS credentials missing — set AWS_ACCESS_KEY_ID + "
            "AWS_SECRET_ACCESS_KEY in your .env (and AWS_REGION)."
        )
    s3 = boto3.client(
        "s3",
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(jsonl.encode("utf-8"))
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    log.info("Uploaded to s3://%s/%s rows=%s", S3_BUCKET, key, len(rows))

    return {
        "facility":      facility,
        "module":        job.get("module"),
        "table":         job.get("table"),
        "namespace":     job["namespace"],
        "database":      job.get("database"),
        "updated_since": job.get("updated_since"),
        "ingested_at":   ingested_at.isoformat(),
        "s3_key":        key,
        "row_count":     len(rows),
    }

def copy_into_snowflake(job_result: dict) -> None:
    facility      = job_result["facility"]
    s3_key        = job_result["s3_key"]
    ingested_at   = job_result["ingested_at"]
    module_source = job_result.get("module") or ""
    source_table  = job_result.get("table") or ""
    namespace     = job_result.get("namespace") or ""

    raw_table = f"{sf_schema(facility, 'RAW')}.EVENTS_RAW"
    sql = f"""
    COPY INTO {raw_table} (facility_id, ingested_at, module_source, source_table, namespace, payload)
    FROM (
      SELECT
        '{facility}'::STRING        AS facility_id,
        '{ingested_at}'::TIMESTAMP_TZ AS ingested_at,
        '{module_source}'::STRING   AS module_source,
        '{source_table}'::STRING    AS source_table,
        '{namespace}'::STRING       AS namespace,
        PARSE_JSON($1)              AS payload
      FROM @{SF_STAGE}
    )
    FILES = ('{s3_key}')
    FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
    ON_ERROR = 'CONTINUE';
    """
    with SnowflakeClient(schema_=sf_schema(facility, "RAW")) as sf:
        sf.execute(sql, label=f"copy:{facility}:{source_table or 'events'}")

def merge_clean(facility: str) -> None:
    raw_table   = f"{sf_schema(facility, 'RAW')}.EVENTS_RAW"
    clean_table = f"{sf_schema(facility, 'CLEAN')}.EVENTS"
    sql = f"""
    MERGE INTO {clean_table} AS t
    USING (
        SELECT
            facility_id,
            f.value:id::STRING            AS event_id,
            f.value:event_time::TIMESTAMP AS event_time,
            f.value:type::STRING          AS event_type,
            f.value:amount::NUMBER        AS amount,
            f.value                       AS payload,
            ingested_at
        FROM {raw_table} AS r,
             LATERAL FLATTEN(input => r.payload) AS f
        WHERE f.value:id IS NOT NULL
          AND NULLIF(TRIM(f.value:id::STRING), '') IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY f.value:id::STRING ORDER BY ingested_at DESC
        ) = 1
    ) AS s
    ON t.event_id = s.event_id
    WHEN MATCHED THEN UPDATE SET
        event_time  = s.event_time,
        event_type  = s.event_type,
        amount      = s.amount,
        payload     = s.payload,
        ingested_at = s.ingested_at
    WHEN NOT MATCHED THEN INSERT (event_id, event_time, event_type, amount, payload, ingested_at)
    VALUES (s.event_id, s.event_time, s.event_type, s.amount, s.payload, s.ingested_at);
    """
    with SnowflakeClient(schema_=sf_schema(facility, "RAW")) as sf:
        sf.execute(sql, label=f"merge_clean:{facility}")

# ─── ORCHESTRATOR ────────────────────────────────────────────────────────

def run_pipeline(facility: str, *, since: str | None = None,
                 only_tables: set[str] | None = None,
                 skip_merge: bool = False, dry_run: bool = False,
                 update_watermark: bool = True) -> None:

    if facility not in FACILITIES:
        raise ValueError(f"Unknown facility {facility!r}. Known: {list(FACILITIES)}")

    run_id = datetime.now(timezone.utc).strftime("manual__%Y-%m-%dT%H-%M-%SZ")
    log.info("══════ START %s · facility=%s · run_id=%s ══════",
             PIPELINE_NAME, facility, run_id)
    started_at = datetime.now(timezone.utc)

    # 1. Build jobs
    jobs = build_jobs_for_facility(facility, since=since, only_tables=only_tables)
    if not jobs:
        log.warning("No jobs to run.")
        return

    # 2. Extract → S3   3. Copy → Snowflake RAW  (sequential, with per-job try/except)
    successes, failures = [], []
    for i, job in enumerate(jobs, start=1):
        log.info("──[%d/%d] %s · %s", i, len(jobs), job["module"], job["table"])
        try:
            result = extract_one_model(job, run_id=run_id, dry_run=dry_run)
            if result is None:                # dry-run path
                continue
            if result["row_count"] == 0:
                log.info("    (no rows — skipping COPY)")
                successes.append(result)
                continue
            copy_into_snowflake(result)
            successes.append(result)
        except Exception as e:
            log.error("    ✗ failed: %s", e, exc_info=True)
            failures.append({"job": job, "error": str(e)})

    # 4. Merge into CLEAN
    if not dry_run and not skip_merge:
        try:
            merge_clean(facility)
        except Exception as e:
            log.error("merge_clean failed: %s", e, exc_info=True)
            failures.append({"job": "merge_clean", "error": str(e)})

    # 5. Update watermark only if everything succeeded
    if not dry_run and update_watermark and not failures:
        set_watermark(facility, started_at.isoformat().replace("+00:00", "Z"))

    log.info("══════ END   ✓ %d ok · ✗ %d failed · %s ══════",
             len(successes), len(failures), PIPELINE_NAME)
    if failures:
        log.warning("Failures (truncated):")
        for f in failures[:10]:
            log.warning("  · %s", f)
        sys.exit(1)

# ─── CLI ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--facility", required=True, choices=list(FACILITIES.keys()))
    ap.add_argument("--since",
                    help="ISO timestamp / date to extract from. "
                         "Overrides the local watermark file. "
                         "Example: 2025-09-01 or 2025-09-01T00:00:00Z")
    ap.add_argument("--only-tables",
                    help="Comma-separated list of table names to limit the run to "
                         "(matches the `table` column in the data dictionary).")
    ap.add_argument("--skip-merge", action="store_true",
                    help="Skip the CLEAN MERGE step (extract + COPY only).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Extract + log only — no S3 upload, no Snowflake writes.")
    ap.add_argument("--no-watermark-update", action="store_true",
                    help="Don't bump the local watermark even on a clean run.")
    args = ap.parse_args()

    only_tables = None
    if args.only_tables:
        only_tables = {t.strip().lower() for t in args.only_tables.split(",") if t.strip()}

    run_pipeline(
        facility=args.facility,
        since=args.since,
        only_tables=only_tables,
        skip_merge=args.skip_merge,
        dry_run=args.dry_run,
        update_watermark=not args.no_watermark_update,
    )


if __name__ == "__main__":
    main()