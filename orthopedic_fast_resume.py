#!/usr/bin/env python3
"""
orthopedic_fast_resume.py — High-speed resume for a partially extracted model.

Reads .orthopedic_progress.json to find already-uploaded S3 keys, fetches every
missing page concurrently, uploads each to S3 in the same worker thread, checkpoints
progress every --checkpoint-every pages, then runs ONE COPY INTO Snowflake at the end.

Stop the main pipeline first to avoid racing on the progress file.

USAGE
  python orthopedic_fast_resume.py --table orderitementries
  python orthopedic_fast_resume.py --table orderitementries --workers 80
  python orthopedic_fast_resume.py --table orderitementries --last-page 10000
  python orthopedic_fast_resume.py --table orderitementries --skip-snowflake
  python orthopedic_fast_resume.py --table orderitementries --dry-run
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
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import boto3
import requests
import requests.adapters
import snowflake.connector
from dotenv import load_dotenv
from requests.exceptions import ChunkedEncodingError, ConnectionError, HTTPError, Timeout

try:
    import orjson
    def _dumps_bytes(obj: object) -> bytes:
        return orjson.dumps(obj)
except ImportError:
    def _dumps_bytes(obj: object) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode()

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

# ─── LOGGING ─────────────────────────────────────────────────────────────────

log = logging.getLogger("orth_fast_resume")
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s",
        datefmt="%H:%M:%S",
    ))
    log.addHandler(_h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ─── CONFIG ──────────────────────────────────────────────────────────────────

AFYA_BASE_URL  = os.getenv("AFYA_EXTRACTION_BASE_URL", "https://afyapi.afyaanalytics.ai/api").rstrip("/")
LOGIN_URL      = f"{AFYA_BASE_URL}/auth/login"
GATEWAY_URL    = f"{AFYA_BASE_URL}/gateway"
CONNECTION_ID  = int(os.getenv("AFYA_EXTRACTION_CONNECTION_ID", "16"))
FACILITY_ID    = int(os.getenv("AFYA_EXTRACTION_FACILITY_ID", "47"))
TOKEN_TTL      = int(os.getenv("TOKEN_TTL_SECONDS", str(50 * 60)))

S3_BUCKET  = os.getenv("S3_BUCKET", "collabmedbucket")
S3_PREFIX  = "raw/orthopedic"

SF_DB          = os.getenv("SNOWFLAKE_DATABASE", "HOSPITALS")
SF_RAW_SCHEMA  = "ORTHOPEDIC_RAW"
SF_SHARED      = "SHARED"
SF_STAGE       = f"{SF_DB}.{SF_SHARED}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT = f"{SF_DB}.{SF_SHARED}.JSON_FF"

PROGRESS_FILE  = Path(__file__).resolve().parent / ".orthopedic_progress.json"

_TABLE_TO_NS: dict[str, str] = {
    "orderitementries":       r"App\Models\OrderItemEntry",
    "singleorderitems":       r"App\Models\SingleOrderItem",
    "orders":                 r"App\Models\Order",
    "ledgerentries":          r"App\Models\LedgerEntry",
    "statemententries":       r"App\Models\StatementEntry",
    "inventoryledgerentries": r"App\Models\InventoryLedgerEntry",
    "payments":               r"App\Models\Payment",
    "requests":               r"App\Models\Request",
    "queueentries":           r"App\Models\QueueEntry",
    "codings":                r"App\Models\Coding",
    "patientschemes":         r"App\Models\PatientScheme",
    "systemlogs":             r"App\Models\SystemLog",
    "errorlogs":              r"App\Models\ErrorLog",
    "patientplans":           r"App\Models\PatientPlan",
    "reorderlevels":          r"App\Models\ReorderLevel",
    "saleitems":              r"App\Models\SaleItem",
    "inventoryitems":         r"App\Models\InventoryItem",
    "purchaseorders":         r"App\Models\PurchaseOrder",
    "reports":                r"App\Models\Report",
    "shifts":                 r"App\Models\Shift",
    "suppliers":              r"App\Models\Supplier",
    "patientinvoices":        r"App\Models\PatientInvoice",
    "diagnoses2":             r"App\Models\Diagnosis2",
    "patients2":              r"App\Models\Patient2",
    "invoices2":              r"App\Models\Invoice2",
    "users2":                 r"App\Models\Users2",
}

_SF_FILES_LIMIT = 1_000

# ─── HTTP + AUTH ──────────────────────────────────────────────────────────────

_session_singleton: requests.Session | None = None
_session_lock  = threading.Lock()
_token_cache:  tuple[str, float] | None = None
_token_lock    = threading.Lock()

def _session() -> requests.Session:
    global _session_singleton
    if _session_singleton is None:
        with _session_lock:
            if _session_singleton is None:
                s = requests.Session()
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=128, pool_maxsize=128, max_retries=0,
                )
                s.mount("https://", adapter)
                s.mount("http://", adapter)
                _session_singleton = s
    return _session_singleton

def _login() -> str:
    username = os.getenv("AFYA_EXTRACTION_USERNAME", "")
    password = os.getenv("AFYA_EXTRACTION_PASSWORD", "")
    if not username or not password:
        raise RuntimeError("Set AFYA_EXTRACTION_USERNAME / AFYA_EXTRACTION_PASSWORD in .env")
    r = _session().post(LOGIN_URL, json={"username": username, "password": password}, timeout=30)
    r.raise_for_status()
    token = r.json().get("token")
    if not token:
        raise RuntimeError(f"Login response missing 'token' field: {r.text[:200]}")
    log.info("Authenticated OK")
    return token

def _get_token() -> str:
    global _token_cache
    with _token_lock:
        if _token_cache and (time.time() - _token_cache[1]) < TOKEN_TTL:
            return _token_cache[0]
        t = _login()
        _token_cache = (t, time.time())
        return t

# Semaphore that caps simultaneous in-flight requests regardless of worker count.
# Adjust via --concurrency at runtime; the default is set in fast_resume().
_request_sem: threading.Semaphore = threading.Semaphore(25)

def _gateway_request(namespace: str, page: int, per_page: int) -> dict:
    MAX_RETRIES = 10
    for attempt in range(1, MAX_RETRIES + 1):
        with _request_sem:
            try:
                r = _session().post(
                    GATEWAY_URL,
                    json={
                        "connection_id": CONNECTION_ID,
                        "namespace":     namespace,
                        "facility_id":   FACILITY_ID,
                        "page":          page,
                        "per_page":      per_page,
                    },
                    headers={"Authorization": f"Bearer {_get_token()}"},
                    timeout=60,
                )

                if r.status_code == 401:
                    global _token_cache
                    with _token_lock:
                        _token_cache = None
                    # retry immediately — don't count as a backoff attempt
                    continue

                if r.status_code == 429:
                    retry_after = int(r.headers.get("Retry-After", 0)) or (2 ** attempt)
                    log.warning(
                        "  page=%d  429 rate-limited — waiting %ds (attempt %d/%d)",
                        page, retry_after, attempt, MAX_RETRIES,
                    )
                    time.sleep(retry_after)
                    continue

                r.raise_for_status()
                return r.json()

            except (Timeout, ConnectionError, ChunkedEncodingError) as e:
                if attempt == MAX_RETRIES:
                    raise
                wait = min(2 ** attempt, 60)
                log.warning("  page=%d attempt=%d/%d retrying in %ds — %s",
                            page, attempt, MAX_RETRIES, wait, e)
                time.sleep(wait)

            except HTTPError as e:
                # Non-429 HTTP errors are not retried
                raise

    raise RuntimeError(f"Exhausted {MAX_RETRIES} retries on page {page}")

def _extract_rows_and_pagination(payload: dict) -> tuple[list, dict]:
    data = payload.get("data", payload)
    if isinstance(data, dict):
        rows = data.get("data", [])
        pag  = {k: v for k, v in data.items() if k != "data"}
    elif isinstance(data, list):
        rows = data
        pag  = payload
    else:
        rows, pag = [], payload
    return (rows if isinstance(rows, list) else []), pag

def _parse_last_page(pag: dict) -> int | None:
    for key in ("last_page", "total_pages", "pageCount"):
        v = pag.get(key)
        if v is not None:
            try:
                lp = int(v)
                return lp if lp > 0 else None
            except (TypeError, ValueError):
                pass
    return None

# ─── S3 ───────────────────────────────────────────────────────────────────────

_s3_singleton = None
_s3_lock      = threading.Lock()

def _s3():
    global _s3_singleton
    if _s3_singleton is None:
        with _s3_lock:
            if _s3_singleton is None:
                ak = os.getenv("AWS_ACCESS_KEY_ID", "")
                sk = os.getenv("AWS_SECRET_ACCESS_KEY", "")
                if not ak or not sk:
                    raise RuntimeError("Missing AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY")
                _s3_singleton = boto3.client(
                    "s3",
                    aws_access_key_id=ak,
                    aws_secret_access_key=sk,
                    region_name=os.getenv("AWS_REGION", "us-east-1"),
                )
    return _s3_singleton

def _safe(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-=\.+]+", "_", (s or "").strip())

def _upload_page(rows: list, table: str, run_id: str, page: int) -> str:
    dt  = datetime.now(timezone.utc).date().isoformat()
    key = f"{S3_PREFIX}/model={_safe(table)}/dt={dt}/{run_id}_p{page:04d}.jsonl.gz"
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(b"\n".join(_dumps_bytes(r) for r in rows) + b"\n")
    _s3().put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    return key

# ─── PROGRESS ─────────────────────────────────────────────────────────────────

_prog_lock = threading.Lock()

def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text())
        except Exception:
            pass
    return {"models": {}}

def _save_progress(data: dict) -> None:
    PROGRESS_FILE.write_text(json.dumps(data, indent=2))

# ─── SNOWFLAKE ────────────────────────────────────────────────────────────────

class SnowflakeClient:
    def __init__(self):
        user      = os.getenv("SNOWFLAKE_USER", "").strip()
        account   = os.getenv("SNOWFLAKE_ACCOUNT", "").strip()
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "").strip()
        database  = os.getenv("SNOWFLAKE_DATABASE", SF_DB).strip()
        key_path  = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()
        password  = os.getenv("SNOWFLAKE_PASSWORD", "").strip()
        for name, val in [("SNOWFLAKE_USER", user), ("SNOWFLAKE_ACCOUNT", account),
                          ("SNOWFLAKE_WAREHOUSE", warehouse), ("SNOWFLAKE_DATABASE", database)]:
            if not val:
                raise RuntimeError(f"Missing env var {name}")
        if not key_path and not password:
            raise RuntimeError("Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")
        kwargs: dict = dict(user=user, account=account, warehouse=warehouse,
                            database=database, schema=SF_RAW_SCHEMA)
        if key_path:
            if not Path(key_path).exists():
                raise RuntimeError(f"Private key not found: {key_path}")
            kwargs["private_key_file"] = key_path
        else:
            kwargs["password"] = password
        self._conn = snowflake.connector.connect(**kwargs)
        self._lock = threading.Lock()

    def close(self) -> None:
        if self._conn:
            try: self._conn.close()
            except Exception: pass
            self._conn = None

    @contextmanager
    def _cursor(self):
        cur = self._conn.cursor()
        try: yield cur
        finally: cur.close()

    def execute(self, sql: str, label: str = "") -> dict:
        label = label or f"x:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-45s | %s…", label, " ".join(sql.split())[:80])
        t0 = time.perf_counter()
        with self._lock, self._cursor() as cur:
            cur.execute(sql)
            rowcount, sfqid = cur.rowcount, cur.sfqid
        log.info("✓ %-45s rowcount=%s  %.2fs", label, rowcount, time.perf_counter() - t0)
        return {"rowcount": rowcount, "sfqid": sfqid}

    def __enter__(self): return self
    def __exit__(self, *_): self.close()

def copy_into_snowflake(table: str, namespace: str, run_id: str, s3_keys: list[str], sf: SnowflakeClient) -> None:
    if not s3_keys:
        return
    fqn     = f"{SF_DB}.{SF_RAW_SCHEMA}.{table.upper()}"
    ns_esc  = namespace.replace("'", "\\'")
    run_esc = run_id.replace("'", "\\'")
    chunks  = [s3_keys[i:i+_SF_FILES_LIMIT] for i in range(0, len(s3_keys), _SF_FILES_LIMIT)]
    for idx, chunk in enumerate(chunks, 1):
        files_sql = ", ".join(f"'{k}'" for k in chunk)
        label = f"copy:{table}({len(chunk)} files" + (f", batch {idx}/{len(chunks)})" if len(chunks) > 1 else ")")
        sf.execute(f"""
        COPY INTO {fqn} (_run_id, _namespace, _ingested_at, payload)
        FROM (
          SELECT '{run_esc}'::VARCHAR AS _run_id, '{ns_esc}'::VARCHAR AS _namespace,
                 CURRENT_TIMESTAMP::TIMESTAMP_TZ AS _ingested_at, PARSE_JSON($1) AS payload
          FROM @{SF_STAGE}
        )
        FILES = ({files_sql})
        FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
        ON_ERROR = 'CONTINUE';
        """, label=label)

# ─── CORE RESUME LOGIC ────────────────────────────────────────────────────────

def fast_resume(
    table: str,
    *,
    workers: int = 40,
    concurrency: int = 25,
    per_page: int = 500,
    last_page_override: int | None = None,
    checkpoint_every: int = 200,
    skip_snowflake: bool = False,
    dry_run: bool = False,
) -> None:
    namespace = _TABLE_TO_NS.get(table)
    if not namespace:
        raise ValueError(f"Unknown table '{table}'. Add it to _TABLE_TO_NS.")

    # Apply concurrency cap globally so all worker threads share the same limit
    global _request_sem
    _request_sem = threading.Semaphore(concurrency)

    # ── Load existing progress ────────────────────────────────────────────────
    progress   = _load_progress()
    mdata      = progress.setdefault("models", {}).setdefault(table, {})
    pages_done = set(mdata.get("pages_done", []))
    s3_keys:   dict[int, str] = {int(p): k for p, k in mdata.get("s3_keys", {}).items()}

    log.info("Table        : %s", table)
    log.info("Namespace    : %s", namespace)
    log.info("Workers      : %d  (concurrency cap: %d)", workers, concurrency)
    log.info("Already done : %d pages  (%d S3 keys)", len(pages_done), len(s3_keys))

    # ── Discover last_page ────────────────────────────────────────────────────
    if last_page_override:
        last_page = last_page_override
        log.info("Last page    : %d  (from --last-page)", last_page)
    else:
        log.info("Fetching page 1 to discover last_page …")
        payload1          = _gateway_request(namespace, 1, per_page)
        first_rows, pag1  = _extract_rows_and_pagination(payload1)
        last_page         = _parse_last_page(pag1)
        if not last_page:
            raise RuntimeError(
                "Could not determine last_page from page 1 response. "
                "Pass --last-page <N> to override."
            )
        log.info("Last page    : %d  (from API)", last_page)

        # If page 1 not done, store it now so we don't re-fetch
        if 1 not in pages_done and first_rows and not dry_run:
            run_id_p1 = datetime.now(timezone.utc).strftime("orth__resume_%Y-%m-%dT%H-%M-%SZ")
            key = _upload_page(first_rows, table, run_id_p1, 1)
            s3_keys[1] = key
            pages_done.add(1)

    remaining = sorted(p for p in range(1, last_page + 1) if p not in pages_done)
    total_missing = len(remaining)

    log.info("Missing pages: %d  (of %d total)", total_missing, last_page)

    if dry_run:
        log.info("DRY-RUN — would fetch %d pages with %d workers then COPY INTO Snowflake.",
                 total_missing, workers)
        return

    if not remaining:
        log.info("Nothing to fetch — all pages already uploaded.")
    else:
        # ── Fan-out fetch + upload ─────────────────────────────────────────────
        run_id      = datetime.now(timezone.utc).strftime("orth__resume_%Y-%m-%dT%H-%M-%SZ")
        done_count  = 0
        error_count = 0
        start_time  = time.perf_counter()
        counter_lock = threading.Lock()

        def _do_page(page: int) -> tuple[int, str | None]:
            rows, _ = _extract_rows_and_pagination(_gateway_request(namespace, page, per_page))
            if not rows:
                return page, None
            key = _upload_page(rows, table, run_id, page)
            return page, key

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_do_page, p): p for p in remaining}
            pending_checkpoint: dict[int, str] = {}

            for fut in as_completed(futures):
                page = futures[fut]
                try:
                    p, key = fut.result()
                    with counter_lock:
                        done_count += 1
                        if key:
                            s3_keys[p] = key
                            pages_done.add(p)
                            pending_checkpoint[p] = key

                        # Progress log every 100 pages
                        if done_count % 100 == 0 or done_count == total_missing:
                            elapsed = time.perf_counter() - start_time
                            rate = done_count / elapsed if elapsed > 0 else 0
                            eta  = (total_missing - done_count) / rate if rate > 0 else float("inf")
                            log.info(
                                "  progress  %d/%d pages  %.1f p/s  ETA %.0fs",
                                done_count, total_missing, rate, eta,
                            )

                        # Checkpoint to disk periodically
                        if len(pending_checkpoint) >= checkpoint_every:
                            mdata["pages_done"] = sorted(pages_done)
                            mdata["s3_keys"]    = {str(p): k for p, k in s3_keys.items()}
                            mdata["status"]     = "partial"
                            _save_progress(progress)
                            log.info("  checkpoint saved  (%d total pages done)", len(pages_done))
                            pending_checkpoint.clear()

                except Exception as e:
                    with counter_lock:
                        error_count += 1
                    log.error("  page=%d FAILED: %s", page, e)

        # Final checkpoint
        mdata["pages_done"] = sorted(pages_done)
        mdata["s3_keys"]    = {str(p): k for p, k in s3_keys.items()}
        mdata["status"]     = "partial"
        _save_progress(progress)

        elapsed = time.perf_counter() - start_time
        log.info(
            "Fetch complete  pages_fetched=%d  errors=%d  total_s3_keys=%d  elapsed=%.1fs",
            total_missing - error_count, error_count, len(s3_keys), elapsed,
        )

        if error_count:
            log.warning("%d pages failed — re-run the script to retry them.", error_count)

    # ── COPY INTO Snowflake ───────────────────────────────────────────────────
    if skip_snowflake:
        log.info("--skip-snowflake set — skipping COPY INTO.")
        return

    ordered_keys = [s3_keys[p] for p in sorted(s3_keys)]
    log.info("COPY INTO  table=%s  files=%d  batches=%d",
             table, len(ordered_keys), (len(ordered_keys) + _SF_FILES_LIMIT - 1) // _SF_FILES_LIMIT)

    run_id_sf = datetime.now(timezone.utc).strftime("orth__resume_%Y-%m-%dT%H-%M-%SZ")
    with SnowflakeClient() as sf:
        copy_into_snowflake(table, namespace, run_id_sf, ordered_keys, sf)

    # Mark done
    mdata["status"]     = "done"
    mdata["total_rows"] = len(ordered_keys) * per_page  # approximate
    _save_progress(progress)

    log.info("Done  table=%s  total_files=%d", table, len(ordered_keys))

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Fast-resume a partial orthopedic extraction.")
    parser.add_argument("--table",    required=True, help="Table name, e.g. orderitementries")
    parser.add_argument("--workers",     type=int, default=40,
                        help="Thread pool size (default: 40)")
    parser.add_argument("--concurrency", type=int, default=25,
                        help="Max simultaneous in-flight API requests (default: 25); tune down if you see 429s")
    parser.add_argument("--per-page", type=int, default=500, help="Rows per page (default: 500)")
    parser.add_argument("--last-page",type=int, default=None,help="Override last page number (skip page-1 API call)")
    parser.add_argument("--checkpoint-every", type=int, default=200,
                        help="Save progress file every N completed pages (default: 200)")
    parser.add_argument("--skip-snowflake", action="store_true",
                        help="Upload to S3 only — skip COPY INTO Snowflake")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would run without fetching or uploading")
    args = parser.parse_args()

    fast_resume(
        table=args.table.strip().lower(),
        workers=args.workers,
        concurrency=args.concurrency,
        per_page=args.per_page,
        last_page_override=args.last_page,
        checkpoint_every=args.checkpoint_every,
        skip_snowflake=args.skip_snowflake,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
