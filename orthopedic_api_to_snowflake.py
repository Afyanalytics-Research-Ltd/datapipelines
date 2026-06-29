#!/usr/bin/env python3
"""
orthopedic_api_to_snowflake.py — Afya Extraction API gateway → S3 → Snowflake ORTHOPEDIC_RAW

Extracts 26 model namespaces from the Afya Extraction platform
(connection_id=16, facility_id=47) into HOSPITALS.ORTHOPEDIC_RAW per-model tables.

Flow:
  1. POST https://afyapi.afyaanalytics.ai/api/auth/login  →  cache bearer token (50-min TTL).
  2. For each of the 26 configured namespaces, POST /api/gateway with
     connection_id, facility_id, namespace, page, and per_page=950.
  3. Paginate all pages concurrently (fan-out once last_page is known from page 1).
  4. Gzip-encode rows as JSONL → upload to S3.
  5. COPY INTO HOSPITALS.ORTHOPEDIC_RAW.<model_table> from the S3 object.
  6. Resume-aware: completed jobs are checkpointed in .orthopedic_progress.json;
     watermarks (.orthopedic_watermarks.json) only advance on a zero-failure run.

MISSING-DATA GUARANTEE
  a) Per-job checkpoint written immediately on success (_mark_done).
  b) Watermarks blocked from advancing if any job failed.
  c) --no-resume forces a complete re-run ignoring prior checkpoints.

USAGE
  python orthopedic_api_to_snowflake.py
  python orthopedic_api_to_snowflake.py --models patients,orders,invoices
  python orthopedic_api_to_snowflake.py --since 2025-01-01T00:00:00Z
  python orthopedic_api_to_snowflake.py --full-refresh
  python orthopedic_api_to_snowflake.py --dry-run
  python orthopedic_api_to_snowflake.py --no-resume
  python orthopedic_api_to_snowflake.py --workers 6 --page-workers 4

ENV VARS  (place in .env next to this script)
  # Afya Extraction credentials
  AFYA_EXTRACTION_USERNAME=admin
  AFYA_EXTRACTION_PASSWORD=Afya@extract26

  # Gateway config (defaults match current orthopedic facility)
  AFYA_EXTRACTION_BASE_URL=https://afyapi.afyaanalytics.ai/api
  AFYA_EXTRACTION_CONNECTION_ID=16
  AFYA_EXTRACTION_FACILITY_ID=47

  # Snowflake (key-pair auth preferred)
  SNOWFLAKE_USER=MBIRONGA
  SNOWFLAKE_ACCOUNT=UFLYZNZ-RA32706
  SNOWFLAKE_WAREHOUSE=COMPUTE_WH
  SNOWFLAKE_DATABASE=HOSPITALS
  SNOWFLAKE_PRIVATE_KEY_PATH=config/rsa_key.p8
  # OR: SNOWFLAKE_PASSWORD=<password>  (if not using key-pair)

  # AWS S3
  AWS_ACCESS_KEY_ID=...
  AWS_SECRET_ACCESS_KEY=...
  AWS_REGION=us-east-1

  # Tuning
  PIPELINE_WORKERS=6     # parallel model jobs (default 6)
  PAGE_WORKERS=4         # parallel pages within a job (default 4)
  PER_PAGE=950           # records per page (default 950)
  TOKEN_TTL_SECONDS=3000 # bearer token TTL in seconds (default 50 min)
  LOG_LEVEL=INFO
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
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

try:
    import orjson
    def _dumps_bytes(obj: object) -> bytes:
        return orjson.dumps(obj)
except ImportError:
    def _dumps_bytes(obj: object) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

# ─── LOGGING ─────────────────────────────────────────────────────────────────

log = logging.getLogger("orthopedic_pipeline")
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(name)s · %(message)s",
        datefmt="%H:%M:%S",
    ))
    log.addHandler(_h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ─── CONFIG ──────────────────────────────────────────────────────────────────

PIPELINE_NAME = "orthopedic_api_to_snowflake"

_base = os.getenv("AFYA_EXTRACTION_BASE_URL", "https://afyapi.afyaanalytics.ai/api").rstrip("/")
AFYA_BASE_URL     = _base
LOGIN_URL         = f"{AFYA_BASE_URL}/auth/login"
GATEWAY_URL       = f"{AFYA_BASE_URL}/gateway"
CONNECTION_ID     = int(os.getenv("AFYA_EXTRACTION_CONNECTION_ID", "16"))
FACILITY_ID       = int(os.getenv("AFYA_EXTRACTION_FACILITY_ID", "47"))

S3_BUCKET  = os.getenv("S3_BUCKET", "collabmedbucket")
S3_PREFIX  = "raw/orthopedic"

SF_DB         = os.getenv("SNOWFLAKE_DATABASE", "HOSPITALS")
SF_RAW_SCHEMA = "ORTHOPEDIC_RAW"
SF_SHARED     = "SHARED"
SF_STAGE      = f"{SF_DB}.{SF_SHARED}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT = f"{SF_DB}.{SF_SHARED}.JSON_FF"

WATERMARK_FILE = Path(__file__).resolve().parent / ".orthopedic_watermarks.json"
PROGRESS_FILE  = Path(__file__).resolve().parent / ".orthopedic_progress.json"

DEFAULT_PIPELINE_WORKERS = int(os.getenv("PIPELINE_WORKERS", "6"))
DEFAULT_PAGE_WORKERS     = int(os.getenv("PAGE_WORKERS", "16"))
DEFAULT_PER_PAGE         = int(os.getenv("PER_PAGE", "950"))
TOKEN_TTL_SECONDS        = int(os.getenv("TOKEN_TTL_SECONDS", str(50 * 60)))

# ─── MODEL REGISTRY ──────────────────────────────────────────────────────────
# Maps PHP namespace → Snowflake table name in ORTHOPEDIC_RAW.
# Table names match the .bson.gz filenames (without extension).

MODELS: list[dict] = [
    {"namespace": r"App\Models\OrderItemEntry",       "table": "orderitementries"},
    {"namespace": r"App\Models\SingleOrderItem",      "table": "singleorderitems"},
    {"namespace": r"App\Models\Order",                "table": "orders"},
    {"namespace": r"App\Models\LedgerEntry",          "table": "ledgerentries"},
    {"namespace": r"App\Models\StatementEntry",       "table": "statemententries"},
    {"namespace": r"App\Models\InventoryLedgerEntry", "table": "inventoryledgerentries"},
    {"namespace": r"App\Models\Payment",              "table": "payments"},
    {"namespace": r"App\Models\Request",              "table": "requests"},
    {"namespace": r"App\Models\QueueEntry",           "table": "queueentries"},
    {"namespace": r"App\Models\Coding",               "table": "codings"},
    {"namespace": r"App\Models\PatientScheme",        "table": "patientschemes"},
    {"namespace": r"App\Models\SystemLog",            "table": "systemlogs"},
    {"namespace": r"App\Models\ErrorLog",             "table": "errorlogs"},
    {"namespace": r"App\Models\PatientPlan",          "table": "patientplans"},
    {"namespace": r"App\Models\ReorderLevel",         "table": "reorderlevels"},
    {"namespace": r"App\Models\SaleItem",             "table": "saleitems"},
    {"namespace": r"App\Models\InventoryItem",        "table": "inventoryitems"},
    {"namespace": r"App\Models\PurchaseOrder",        "table": "purchaseorders"},
    {"namespace": r"App\Models\Report",               "table": "reports"},
    {"namespace": r"App\Models\Shift",                "table": "shifts"},
    {"namespace": r"App\Models\Supplier",             "table": "suppliers"},
    {"namespace": r"App\Models\PatientInvoice",       "table": "patientinvoices"},
    {"namespace": r"App\Models\Diagnosis2",           "table": "diagnoses2"},
    {"namespace": r"App\Models\Patient2",             "table": "patients2"},
    {"namespace": r"App\Models\Invoice2",             "table": "invoices2"},
    {"namespace": r"App\Models\Users2",               "table": "users2"},
]

# Quick lookup: table_name → namespace
_TABLE_TO_NS: dict[str, str] = {m["table"]: m["namespace"] for m in MODELS}

# ─── JSON FILE HELPERS ────────────────────────────────────────────────────────

def _load_json(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception as e:
            log.warning("Could not parse %s: %s", path, e)
    return {}

def _save_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True))

# ─── WATERMARKS ───────────────────────────────────────────────────────────────

_wm_lock = threading.Lock()

def _wm_key(table: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "_", table)

def get_watermark(table: str, default: str = "1970-01-01T00:00:00Z") -> str:
    return _load_json(WATERMARK_FILE).get(_wm_key(table), default)

def set_watermark(table: str, ts_iso: str) -> None:
    with _wm_lock:
        wm = _load_json(WATERMARK_FILE)
        wm[_wm_key(table)] = ts_iso
        _save_json(WATERMARK_FILE, wm)
    log.info("Watermark [%s] → %s", table, ts_iso)

# ─── PROGRESS / RESUME (page-level) ──────────────────────────────────────────
#
# Progress file layout:
# {
#   "models": {
#     "patients": {
#       "status":     "done" | "partial" | "empty",
#       "run_id":     "orth__2026-06-29T10-00-00Z",
#       "last_page":  8,
#       "pages_done": [1,2,3,4,5,6,7,8],
#       "s3_keys":    {"1": "raw/orthopedic/..._p0001.jsonl.gz", ...},
#       "total_rows": 4000,
#       "completed_at": "..."
#     }
#   }
# }
#
# On restart:
#   status="done"    → skip entirely
#   status="partial" → fetch only missing pages from the API, upload them to S3,
#                      then do ONE COPY INTO covering all s3_keys (old + new)

_progress_lock = threading.Lock()

def _get_model_progress(table: str) -> dict:
    return _load_json(PROGRESS_FILE).get("models", {}).get(table, {})

def _mark_page_done(
    table: str, page: int, s3_key: str, last_page: int | None, row_count: int
) -> None:
    with _progress_lock:
        prog = _load_json(PROGRESS_FILE)
        m    = prog.setdefault("models", {}).setdefault(table, {})
        m.setdefault("status", "partial")
        m.setdefault("pages_done", [])
        m.setdefault("s3_keys", {})
        m.setdefault("total_rows", 0)
        if page not in m["pages_done"]:
            m["pages_done"].append(page)
            m["pages_done"].sort()
        m["s3_keys"][str(page)] = s3_key
        m["total_rows"] = m.get("total_rows", 0) + row_count
        m["status"]     = "partial"
        if last_page is not None:
            m["last_page"] = last_page
        _save_json(PROGRESS_FILE, prog)

def _mark_model_done(table: str, run_id: str, total_rows: int) -> None:
    with _progress_lock:
        prog = _load_json(PROGRESS_FILE)
        m    = prog.setdefault("models", {}).setdefault(table, {})
        m["status"]       = "done"
        m["run_id"]       = run_id
        m["total_rows"]   = total_rows
        m["completed_at"] = datetime.now(timezone.utc).isoformat()
        _save_json(PROGRESS_FILE, prog)

def _reset_model(table: str) -> None:
    with _progress_lock:
        prog = _load_json(PROGRESS_FILE)
        prog.get("models", {}).pop(table, None)
        _save_json(PROGRESS_FILE, prog)

def _clear_all_progress() -> None:
    with _progress_lock:
        _save_json(PROGRESS_FILE, {})

def _done_tables() -> set[str]:
    models = _load_json(PROGRESS_FILE).get("models", {})
    return {t for t, v in models.items() if v.get("status") == "done"}

# ─── SNOWFLAKE CLIENT ─────────────────────────────────────────────────────────

class SnowflakeClient:
    def __init__(self):
        user      = os.getenv("SNOWFLAKE_USER", "").strip()
        account   = os.getenv("SNOWFLAKE_ACCOUNT", "").strip()
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "").strip()
        database  = os.getenv("SNOWFLAKE_DATABASE", SF_DB).strip()
        key_path  = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()
        password  = os.getenv("SNOWFLAKE_PASSWORD", "").strip()

        for name, val in [
            ("SNOWFLAKE_USER", user),
            ("SNOWFLAKE_ACCOUNT", account),
            ("SNOWFLAKE_WAREHOUSE", warehouse),
            ("SNOWFLAKE_DATABASE", database),
        ]:
            if not val:
                raise RuntimeError(f"Missing env var {name} — add it to your .env.")

        if not key_path and not password:
            raise RuntimeError(
                "Snowflake auth: set SNOWFLAKE_PRIVATE_KEY_PATH (key-pair) "
                "or SNOWFLAKE_PASSWORD in your .env."
            )

        kwargs: dict = dict(
            user=user,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=SF_RAW_SCHEMA,
        )
        if key_path:
            if not Path(key_path).exists():
                raise RuntimeError(
                    f"Snowflake private key not found: {key_path} — "
                    "check SNOWFLAKE_PRIVATE_KEY_PATH in your .env."
                )
            kwargs["private_key_file"] = key_path
        else:
            kwargs["password"] = password

        try:
            self._conn = snowflake.connector.connect(**kwargs)
        except Exception as e:
            raise RuntimeError(
                f"Snowflake connection failed — account={account} user={user}. "
                f"Cause: {e}"
            ) from e

        self._lock = threading.Lock()

    def close(self) -> None:
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

    def execute(self, sql: str, label: str | None = None) -> dict:
        label = label or f"x:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-35s | %s…", label, " ".join(sql.split())[:120])
        t0 = time.perf_counter()
        try:
            with self._lock, self._cursor() as cur:
                cur.execute(sql)
                rowcount, sfqid = cur.rowcount, cur.sfqid
            log.info("✓ %-35s rowcount=%s · %.2fs", label, rowcount, time.perf_counter() - t0)
            return {"rowcount": rowcount, "sfqid": sfqid}
        except Exception as e:
            log.exception("✗ %-35s FAILED · %s", label, e)
            raise

    def __enter__(self): return self
    def __exit__(self, *_): self.close()

# ─── HTTP SESSION + TOKEN CACHE ───────────────────────────────────────────────

_session_singleton: requests.Session | None = None
_session_lock  = threading.Lock()
_token_cache:    tuple[str, float] | None = None
_token_lock    = threading.Lock()

def _session() -> requests.Session:
    global _session_singleton
    if _session_singleton is None:
        with _session_lock:
            if _session_singleton is None:
                s = requests.Session()
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=32, pool_maxsize=32, max_retries=0,
                )
                s.mount("https://", adapter)
                s.mount("http://", adapter)
                _session_singleton = s
    return _session_singleton

def _get_token() -> str:
    global _token_cache
    with _token_lock:
        if _token_cache and (time.time() - _token_cache[1]) < TOKEN_TTL_SECONDS:
            return _token_cache[0]
        token = _login()
        _token_cache = (token, time.time())
        return token

def _invalidate_token() -> None:
    global _token_cache
    with _token_lock:
        _token_cache = None

def _login() -> str:
    username = (
        os.getenv("AFYA_EXTRACTION_USERNAME")
        or os.getenv("afya_extraction_username")
    )
    password = (
        os.getenv("AFYA_EXTRACTION_PASSWORD")
        or os.getenv("afya_extraction_password")
    )
    if not username:
        raise RuntimeError(
            "Missing AFYA_EXTRACTION_USERNAME env var — add it to your .env. "
            f"This authenticates against {LOGIN_URL}."
        )
    if not password:
        raise RuntimeError(
            f"Missing AFYA_EXTRACTION_PASSWORD env var for user '{username}' — "
            "add it to your .env."
        )

    log.info("Authenticating as %s → %s", username, LOGIN_URL)
    try:
        r = _session().post(
            LOGIN_URL,
            json={"username": username, "password": password},
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=30,
        )
    except Exception as e:
        raise RuntimeError(
            f"Network error reaching {LOGIN_URL} — check connectivity. Cause: {e}"
        ) from e

    if r.status_code == 401:
        raise RuntimeError(
            f"Login rejected (401) — wrong credentials for '{username}'. "
            f"Response: {r.text[:300]}"
        )
    if r.status_code != 200:
        raise RuntimeError(
            f"Login failed with status {r.status_code} from {LOGIN_URL}. "
            f"Response: {r.text[:300]}"
        )
    try:
        body = r.json()
    except Exception:
        raise RuntimeError(
            f"Login returned 200 but response is not valid JSON. "
            f"Raw: {r.text[:300]}"
        )

    # The Afya Extraction API returns {"token": "..."} (not "access_token")
    token = body.get("token")
    if not token:
        raise RuntimeError(
            f"Login succeeded (200) but no 'token' field in response. "
            f"Keys present: {list(body.keys())}. "
            f"Full response: {r.text[:500]}"
        )
    log.info("Authenticated as %s  (token length=%d)", username, len(token))
    return token

def _auth_headers() -> dict:
    return {
        "Authorization": f"Bearer {_get_token()}",
        "Accept":        "application/json",
        "Content-Type":  "application/json",
    }

# ─── GATEWAY PAGINATION ───────────────────────────────────────────────────────

def _extract_rows_and_pagination(payload: dict) -> tuple[list, dict]:
    """Return (rows, pagination) from a gateway response.

    The Afya Extraction gateway returns paginated results.  Handles nested
    shapes where data is {"data": {"data": [...], "current_page": 1, ...}}.
    """
    rows       = payload.get("data") or []
    pagination = payload.get("pagination") or payload.get("meta") or {}

    if isinstance(rows, dict):
        pagination = {**rows, **pagination}
        rows       = rows.get("data") or list(rows.values())

    return (rows if isinstance(rows, list) else []), pagination

def _gateway_request(
    namespace: str,
    page: int,
    per_page: int,
    updated_since: str | None = None,
    *,
    max_retries: int = 6,
    default_wait: int = 10,
    backoff: int = 2,
) -> dict:
    """POST /api/gateway for a single page with retry for 401/429/5xx/network."""
    body: dict = {
        "connection_id": CONNECTION_ID,
        "namespace":     namespace,
        "facility_id":   FACILITY_ID,
        "page":          page,
        "per_page":      per_page,
    }
    if updated_since and updated_since != "1970-01-01T00:00:00Z":
        body["updated_since"] = updated_since

    attempt, wait = 0, default_wait
    while True:
        attempt += 1
        try:
            r = _session().post(
                GATEWAY_URL,
                headers=_auth_headers(),
                json=body,
                timeout=120,
            )
            log.info(
                "· namespace=%-45s page=%-4s status=%s",
                namespace.split("\\")[-1], page, r.status_code,
            )

            if r.status_code == 401:
                _invalidate_token()
                if attempt >= max_retries:
                    raise RuntimeError(
                        f"Gateway 401 Unauthorized after {max_retries} token refreshes — "
                        f"namespace={namespace} page={page}. "
                        f"Response: {r.text[:300]}"
                    )
                log.warning(
                    "  401 — refreshing token and retrying (%d/%d)", attempt, max_retries
                )
                continue

            if r.status_code == 404:
                raise RuntimeError(
                    f"Gateway 404 Not Found — namespace={namespace} url={GATEWAY_URL}. "
                    f"The namespace may not be registered for connection_id={CONNECTION_ID}. "
                    f"Response: {r.text[:300]}"
                )

            if r.status_code == 422:
                raise RuntimeError(
                    f"Gateway 422 Unprocessable — namespace={namespace} page={page}. "
                    f"Request body: {body}. Response: {r.text[:300]}"
                )

            if r.status_code == 429:
                retry_after = default_wait
                try:
                    retry_after = int(r.json().get("retry_after_seconds", default_wait))
                except Exception:
                    pass
                if attempt >= max_retries:
                    raise RuntimeError(
                        f"Gateway 429 rate-limited after {max_retries} retries — "
                        f"namespace={namespace} page={page}. Response: {r.text[:200]}"
                    )
                log.warning(
                    "  429 rate-limited — sleeping %ds then retrying (%d/%d)",
                    retry_after, attempt, max_retries,
                )
                time.sleep(retry_after)
                continue

            if r.status_code in {500, 502, 503, 504}:
                if attempt >= max_retries:
                    raise RuntimeError(
                        f"Gateway {r.status_code} server error after {max_retries} retries — "
                        f"namespace={namespace} page={page}. Response: {r.text[:300]}"
                    )
                log.warning(
                    "  %s server error — sleeping %ds (%d/%d) — %s",
                    r.status_code, wait, attempt, max_retries, r.text[:150],
                )
                time.sleep(wait)
                wait = min(wait * backoff, 120)
                continue

            if not r.ok:
                raise RuntimeError(
                    f"Gateway unexpected status {r.status_code} — "
                    f"namespace={namespace} page={page}. Response: {r.text[:300]}"
                )

            try:
                return r.json()
            except Exception:
                raise RuntimeError(
                    f"Gateway returned 200 but response is not valid JSON — "
                    f"namespace={namespace} page={page}. Raw: {r.text[:300]}"
                )

        except (Timeout, ConnectionError, ChunkedEncodingError) as e:
            if attempt >= max_retries:
                raise RuntimeError(
                    f"Network error after {max_retries} retries — "
                    f"namespace={namespace} page={page} url={GATEWAY_URL}. Cause: {e}"
                ) from e
            log.warning(
                "  Network error page=%d — sleeping %ds (%d/%d): %s",
                page, wait, attempt, max_retries, e,
            )
            time.sleep(wait)
            wait = min(wait * backoff, 120)

def _parse_last_page(pagination: dict) -> int | None:
    """Extract the last page number from a pagination dict, or None if unavailable."""
    for key in ("last_page", "total_pages", "pageCount"):
        v = pagination.get(key)
        if v is not None:
            try:
                lp = int(v)
                return lp if lp > 0 else None
            except (TypeError, ValueError):
                pass
    return None



def fetch_all_pages(
    namespace: str,
    per_page: int,
    updated_since: str | None = None,
    *,
    max_pages: int = 10_000,
    page_workers: int = DEFAULT_PAGE_WORKERS,
) -> list:
    """Exhaust all pages for a namespace.

    Page 1 is fetched first to discover last_page.  If last_page > 1 the
    remaining pages are fetched concurrently (fan-out).  When the API does not
    include a last_page the fallback is sequential — pages are fetched until an
    empty response is returned.

    The row-count vs per_page heuristic is intentionally NOT used as a stop
    condition.  Some APIs enforce their own internal page cap (e.g. 500 rows)
    regardless of the requested per_page value, which would cause a false early
    exit on the very first page.
    """
    payload1         = _gateway_request(namespace, 1, per_page, updated_since)
    first_rows, pag1 = _extract_rows_and_pagination(payload1)

    if not first_rows:
        return []

    all_rows  = list(first_rows)
    last_page = _parse_last_page(pag1)
    ns_short  = namespace.split("\\")[-1]

    # Explicit single page
    if last_page is not None and last_page <= 1:
        log.info("  %-35s  page 1/1  rows=%d", ns_short, len(all_rows))
        return all_rows

    # Known page count — fan-out all remaining pages concurrently
    if last_page is not None:
        last_page = min(last_page, max_pages)
        pages     = list(range(2, last_page + 1))
        log.info(
            "  %-35s  last_page=%d — fanning out %d page(s)",
            ns_short, last_page, len(pages),
        )

        def _fetch(p: int) -> tuple[int, list]:
            rows, _ = _extract_rows_and_pagination(
                _gateway_request(namespace, p, per_page, updated_since)
            )
            return p, rows

        with ThreadPoolExecutor(max_workers=max(1, page_workers)) as pool:
            page_rows: dict[int, list] = {}
            for fut in as_completed(pool.submit(_fetch, p) for p in pages):
                p, rows = fut.result()
                page_rows[p] = rows

        for p in pages:
            all_rows.extend(page_rows.get(p, []))
        log.info("  %-35s  done  total_rows=%d", ns_short, len(all_rows))
        return all_rows

    # Unknown page count — sequential until the API returns an empty page.
    # Stop conditions (in order of priority):
    #   1. Empty rows — definitively the last page.
    #   2. last_page appears in a later response and current page has reached it.
    #   3. has_more_pages explicitly False and still no last_page.
    # We do NOT stop on len(rows) < per_page — the API may cap its own page
    # size below what we requested on every page.
    log.info("  %-35s  no last_page — sequential exhaustion", ns_short)
    page = 1
    while page < max_pages:
        page += 1
        rows, pag = _extract_rows_and_pagination(
            _gateway_request(namespace, page, per_page, updated_since)
        )
        if not rows:
            break
        all_rows.extend(rows)

        # Re-check: some APIs include last_page only from page 2 onwards
        lp = _parse_last_page(pag)
        if lp is not None and page >= lp:
            break

        # Explicit no-more-pages signal with no last_page to fan-out with
        if (
            pag.get("has_more_pages") is False
            or pag.get("hasMorePages") is False
        ) and lp is None:
            break

    log.info("  %-35s  done  pages=%d  total_rows=%d", ns_short, page, len(all_rows))
    return all_rows

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
                if not ak:
                    raise RuntimeError("Missing AWS_ACCESS_KEY_ID in .env.")
                if not sk:
                    raise RuntimeError("Missing AWS_SECRET_ACCESS_KEY in .env.")
                region = os.getenv("AWS_REGION", "us-east-1")
                try:
                    _s3_singleton = boto3.client(
                        "s3",
                        aws_access_key_id=ak,
                        aws_secret_access_key=sk,
                        region_name=region,
                    )
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to create S3 client (region={region}). Cause: {e}"
                    ) from e
    return _s3_singleton

def _safe(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-=\.\+]+", "_", (s or "").strip())

def _upload_page_to_s3(rows: list, table: str, run_id: str, page: int) -> str:
    """Gzip-encode a single page of rows and upload to S3. Returns the S3 key."""
    dt  = datetime.now(timezone.utc).date().isoformat()
    key = (
        f"{S3_PREFIX}/"
        f"model={_safe(table)}/"
        f"dt={dt}/"
        f"{run_id}_p{page:04d}.jsonl.gz"
    )
    jsonl_bytes = b"\n".join(_dumps_bytes(r) for r in rows) + b"\n"
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(jsonl_bytes)
    try:
        _s3().put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    except Exception as e:
        raise RuntimeError(
            f"S3 upload failed — bucket={S3_BUCKET} key={key} "
            f"table={table} page={page} rows={len(rows)}. Cause: {e}"
        ) from e
    log.info("    S3 ← s3://%s/%s  rows=%d", S3_BUCKET, key, len(rows))
    return key

# ─── EXTRACT ONE MODEL (page-level S3, one batched COPY INTO) ────────────────

def extract_one_model(
    model: dict,
    run_id: str,
    sf: "SnowflakeClient | None",
    *,
    updated_since: str | None = None,
    resume: bool = True,
    dry_run: bool = False,
    page_workers: int = DEFAULT_PAGE_WORKERS,
    per_page: int = DEFAULT_PER_PAGE,
) -> dict:
    """Fetch all pages, upload each to S3, then do ONE COPY INTO for the model.

    Performance design:
      - API pages are fetched concurrently (fan-out once last_page is known).
      - Each page is uploaded to its own S3 file immediately after fetching
        (cheap; enables page-level resume tracking).
      - A single COPY INTO covers all page files → one Snowflake round-trip
        per model regardless of page count.

    Resume (resume=True):
      - status='done'    → skip entirely, no API calls.
      - status='partial' → skip already-uploaded pages (their S3 keys are
        saved in the progress file), fetch only missing pages, then do ONE
        COPY INTO covering ALL s3_keys (saved + new).
    """
    namespace = model["namespace"]
    table     = model["table"]
    ns_short  = namespace.split("\\")[-1]

    prog         = _get_model_progress(table) if resume else {}
    pages_done   = set(prog.get("pages_done", []))
    saved_s3keys = prog.get("s3_keys", {})       # {str(page): s3_key}
    known_last   = prog.get("last_page")
    total_rows   = prog.get("total_rows", 0)

    if prog.get("status") == "done":
        log.info("  %-35s  already done (%d rows) — skipping", ns_short, total_rows)
        return {"table": table, "status": "skipped", "row_count": total_rows}

    # Collect all s3_keys accumulated this run (existing + new) for the final COPY INTO
    all_s3_keys: dict[int, str] = {int(p): k for p, k in saved_s3keys.items()}
    new_rows = 0

    def _upload_page(page: int, rows: list, last_page: int | None) -> str | None:
        """Upload rows for one page to S3 and checkpoint. Returns s3_key or None."""
        if not rows:
            return None
        if dry_run:
            log.info("    DRY-RUN  %-35s  page=%d  rows=%d", ns_short, page, len(rows))
            return None
        s3_key = _upload_page_to_s3(rows, table, run_id, page)
        _mark_page_done(table, page, s3_key, last_page, len(rows))
        return s3_key

    # ── Page 1: fetch first to discover last_page ─────────────────────────────
    if 1 not in pages_done:
        payload1         = _gateway_request(namespace, 1, per_page, updated_since)
        first_rows, pag1 = _extract_rows_and_pagination(payload1)
        if not first_rows:
            log.info("  %-35s  0 rows on page 1 — nothing to load", ns_short)
            if not dry_run:
                _mark_model_done(table, run_id, total_rows)
            return {"table": table, "status": "empty", "row_count": 0}
        last_page = _parse_last_page(pag1) or known_last
        key = _upload_page(1, first_rows, last_page)
        if key:
            all_s3_keys[1] = key
        new_rows   += len(first_rows)
        pages_done.add(1)
    else:
        last_page = known_last
        log.info("  %-35s  page 1 already uploaded, last_page=%s — resuming",
                 ns_short, last_page)

    # ── Fan-out when last_page is known ───────────────────────────────────────
    if last_page is not None:
        remaining = [p for p in range(2, last_page + 1) if p not in pages_done]
        if remaining:
            log.info(
                "  %-35s  last_page=%d  fetching+uploading %d page(s) concurrently",
                ns_short, last_page, len(remaining),
            )

            def _do_page(p: int) -> tuple[int, str | None, int]:
                rows, _ = _extract_rows_and_pagination(
                    _gateway_request(namespace, p, per_page, updated_since)
                )
                key = _upload_page(p, rows, last_page)  # fetch + upload in same thread
                return p, key, len(rows)

            with ThreadPoolExecutor(max_workers=max(1, page_workers)) as pool:
                for fut in as_completed(pool.submit(_do_page, p) for p in remaining):
                    p, key, cnt = fut.result()
                    if key:
                        all_s3_keys[p] = key
                    new_rows += cnt
        else:
            log.info("  %-35s  all pages already uploaded", ns_short)

    # ── Sequential exhaustion when last_page is unknown ───────────────────────
    else:
        log.info("  %-35s  last_page unknown — sequential exhaustion", ns_short)
        page = max(pages_done) if pages_done else 0
        while page < 10_000:
            page += 1
            if page in pages_done:
                continue
            rows, pag = _extract_rows_and_pagination(
                _gateway_request(namespace, page, per_page, updated_since)
            )
            if not rows:
                break
            lp  = _parse_last_page(pag)
            key = _upload_page(page, rows, lp)
            if key:
                all_s3_keys[page] = key
            new_rows   += len(rows)
            pages_done.add(page)

            if lp is not None:
                remaining = [p for p in range(page + 1, lp + 1) if p not in pages_done]
                if remaining:
                    log.info(
                        "  %-35s  last_page=%d discovered — fanning out %d page(s)",
                        ns_short, lp, len(remaining),
                    )

                    def _do_seq_page(p: int) -> tuple[int, str | None, int]:
                        rows, _ = _extract_rows_and_pagination(
                            _gateway_request(namespace, p, per_page, updated_since)
                        )
                        key = _upload_page(p, rows, lp)  # fetch + upload in same thread
                        return p, key, len(rows)

                    with ThreadPoolExecutor(max_workers=max(1, page_workers)) as pool:
                        for fut in as_completed(
                            pool.submit(_do_seq_page, p) for p in remaining
                        ):
                            p, key, cnt = fut.result()
                            if key:
                                all_s3_keys[p] = key
                            new_rows += cnt
                break

            if (
                pag.get("has_more_pages") is False
                or pag.get("hasMorePages") is False
            ):
                break

    # ── ONE COPY INTO for all pages (existing resumed + newly fetched) ─────────
    ordered_keys = [all_s3_keys[p] for p in sorted(all_s3_keys)]
    total_rows  += new_rows

    if ordered_keys and not dry_run:
        copy_into_snowflake(table, namespace, run_id, ordered_keys, sf)

    if not dry_run:
        _mark_model_done(table, run_id, total_rows)

    log.info("  %-35s  done  pages=%d  total_rows=%d", ns_short, len(ordered_keys), total_rows)
    return {"table": table, "namespace": namespace, "status": "ok", "row_count": total_rows}

# ─── SNOWFLAKE SCHEMA + TABLE BOOTSTRAP ──────────────────────────────────────

def _table_fqn(table: str) -> str:
    return f"{SF_DB}.{SF_RAW_SCHEMA}.{table.upper()}"

def ensure_schema_and_tables(sf: SnowflakeClient) -> None:
    """Create ORTHOPEDIC_RAW schema and all per-model tables if they don't exist."""
    sf.execute(
        f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_RAW_SCHEMA};",
        label="ensure_schema",
    )
    for model in MODELS:
        fqn = _table_fqn(model["table"])
        sf.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {fqn} (
                _run_id       VARCHAR       NOT NULL,
                _namespace    VARCHAR       NOT NULL,
                _ingested_at  TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
                payload       VARIANT       NOT NULL
            );
            """,
            label=f"ensure:{model['table']}",
        )
    log.info(
        "Schema and %d tables ready in %s.%s", len(MODELS), SF_DB, SF_RAW_SCHEMA
    )

# ─── SNOWFLAKE COPY ───────────────────────────────────────────────────────────

_SF_FILES_LIMIT = 1_000  # Snowflake hard limit per COPY INTO

def copy_into_snowflake(
    table: str,
    namespace: str,
    run_id: str,
    s3_keys: list[str],
    sf: SnowflakeClient,
) -> None:
    """COPY INTO Snowflake, automatically chunking at the 1,000-file limit."""
    if not s3_keys:
        return
    fqn     = _table_fqn(table)
    ns_esc  = namespace.replace("'", "\\'")
    run_esc = run_id.replace("'", "\\'")

    chunks = [
        s3_keys[i : i + _SF_FILES_LIMIT]
        for i in range(0, len(s3_keys), _SF_FILES_LIMIT)
    ]
    for idx, chunk in enumerate(chunks, start=1):
        files_sql = ", ".join(f"'{k}'" for k in chunk)
        label     = (
            f"copy:{table}({len(chunk)} files"
            + (f", batch {idx}/{len(chunks)})" if len(chunks) > 1 else ")")
        )
        sql = f"""
        COPY INTO {fqn} (_run_id, _namespace, _ingested_at, payload)
        FROM (
          SELECT
            '{run_esc}'::VARCHAR                AS _run_id,
            '{ns_esc}'::VARCHAR                 AS _namespace,
            CURRENT_TIMESTAMP::TIMESTAMP_TZ     AS _ingested_at,
            PARSE_JSON($1)                      AS payload
          FROM @{SF_STAGE}
        )
        FILES = ({files_sql})
        FILE_FORMAT = (FORMAT_NAME = {SF_FILE_FORMAT})
        ON_ERROR = 'CONTINUE';
        """
        try:
            sf.execute(sql, label=label)
        except Exception as e:
            raise RuntimeError(
                f"Snowflake COPY INTO failed — table={fqn} "
                f"namespace={namespace} batch={idx}/{len(chunks)} "
                f"files={len(chunk)}. Cause: {e}"
            ) from e

# ─── ORCHESTRATOR ─────────────────────────────────────────────────────────────

def run_pipeline(
    only_tables: list[str] | None = None,
    *,
    since: str | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
    resume: bool = True,
    update_watermark: bool = True,
    workers: int = DEFAULT_PIPELINE_WORKERS,
    page_workers: int = DEFAULT_PAGE_WORKERS,
    per_page: int = DEFAULT_PER_PAGE,
) -> None:
    run_id     = datetime.now(timezone.utc).strftime("orth__%Y-%m-%dT%H-%M-%SZ")
    started_at = datetime.now(timezone.utc)

    models = MODELS
    if only_tables:
        lower = {t.lower() for t in only_tables}
        models = [m for m in MODELS if m["table"] in lower]
        if not models:
            log.error(
                "No matching models for --models %s. "
                "Valid table names: %s",
                only_tables,
                ", ".join(m["table"] for m in MODELS),
            )
            sys.exit(1)

    if not resume and not dry_run:
        for m in models:
            _reset_model(m["table"])
        log.info("⟲ Resume disabled — cleared previous progress for %d model(s)", len(models))
    elif full_refresh and not dry_run:
        for m in models:
            _reset_model(m["table"])
        log.info("⟲ Full refresh — cleared page checkpoints for %d model(s)", len(models))

    done_tables = _done_tables() if (resume and not dry_run) else set()
    pending     = [m for m in models if m["table"] not in done_tables]

    if len(pending) < len(models):
        log.info(
            "⟲ Resuming: %d already done, %d pending (use --no-resume to redo all)",
            len(models) - len(pending), len(pending),
        )

    log.info(
        "══ START %s · run=%s · %d/%d models · workers=%d (pages=%d) ══",
        PIPELINE_NAME, run_id, len(pending), len(models), workers, page_workers,
    )

    successes: list[dict] = []
    failures:  list[dict] = []

    sf = None
    try:
        if not dry_run:
            sf = SnowflakeClient()
            ensure_schema_and_tables(sf)

        def _do_one(idx_and_model: tuple[int, dict]):
            idx, model = idx_and_model
            ns_short = model["namespace"].split("\\")[-1]
            log.info("──[%d/%d] start · %s", idx, len(pending), ns_short)
            try:
                wm = (
                    since
                    if since else
                    None
                    if full_refresh else
                    get_watermark(model["table"])
                )
                result = extract_one_model(
                    model, run_id, sf,
                    updated_since=wm,
                    resume=resume,
                    dry_run=dry_run,
                    page_workers=page_workers,
                    per_page=per_page,
                )
                return (result["status"], result, model)

            except Exception as e:
                log.error("✗ [%d] %s FAILED — %s", idx, ns_short, e, exc_info=True)
                return ("err", f"{e}", model)

        if pending:
            with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
                for status, payload, model in pool.map(
                    _do_one, list(enumerate(pending, start=1))
                ):
                    if status in ("ok", "empty", "skipped"):
                        successes.append(payload if isinstance(payload, dict) else {"status": status})
                    elif status == "err":
                        failures.append({"model": model, "error": payload})

    finally:
        if sf is not None:
            sf.close()

    # Advance watermarks only on a clean run (zero failures)
    if not dry_run and update_watermark and not failures:
        ts = started_at.isoformat().replace("+00:00", "Z")
        for model in models:
            set_watermark(model["table"], ts)
        log.info("✓ Clean run — watermarks advanced for %d model(s).", len(models))

    total_rows = sum(r.get("row_count", 0) for r in successes)
    log.info(
        "══ END   ✓ %d ok · ✗ %d failed · %d rows · %s ══",
        len(successes), len(failures), total_rows, PIPELINE_NAME,
    )

    if failures:
        log.warning(
            "%d model(s) failed — watermarks NOT advanced, re-run will retry them:",
            len(failures),
        )
        for f in failures[:10]:
            log.warning(
                "  · %-30s  %s",
                f["model"]["table"], str(f.get("error", ""))[:200],
            )
        if len(failures) > 10:
            log.warning("  … and %d more.", len(failures) - 10)
        sys.exit(1)

# ─── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--models",
        help=(
            "Comma-separated table names to extract, e.g. patients,orders,invoices. "
            "Uses table names (not namespaces). Omit to run all 26 models."
        ),
    )
    ap.add_argument(
        "--since",
        help="ISO timestamp override for updated_since on every job (skips per-job watermarks).",
    )
    ap.add_argument(
        "--full-refresh", action="store_true",
        help="Ignore all watermarks and fetch every record from scratch.",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Extract and log only — no S3 or Snowflake writes.",
    )
    ap.add_argument(
        "--no-resume", action="store_true",
        help="Ignore .orthopedic_progress.json and re-run every model from scratch.",
    )
    ap.add_argument(
        "--no-watermark-update", action="store_true",
        help="Do not advance watermarks even on a clean run.",
    )
    ap.add_argument(
        "--workers", type=int, default=DEFAULT_PIPELINE_WORKERS,
        help=f"Parallel model jobs (default {DEFAULT_PIPELINE_WORKERS}).",
    )
    ap.add_argument(
        "--page-workers", type=int, default=DEFAULT_PAGE_WORKERS,
        help=f"Parallel pages within a model job (default {DEFAULT_PAGE_WORKERS}).",
    )
    ap.add_argument(
        "--per-page", type=int, default=DEFAULT_PER_PAGE,
        help=f"Records per gateway page (default {DEFAULT_PER_PAGE}).",
    )
    ap.add_argument(
        "--list-models", action="store_true",
        help="Print all configured model → table mappings and exit.",
    )
    args = ap.parse_args()

    if args.list_models:
        print(f"\n{'Namespace':<50}  Table")
        print("-" * 70)
        for m in MODELS:
            print(f"{m['namespace']:<50}  {m['table']}")
        print(f"\n{len(MODELS)} models total.")
        return

    only_tables = None
    if args.models:
        only_tables = [t.strip().lower() for t in args.models.split(",") if t.strip()]

    run_pipeline(
        only_tables=only_tables,
        since=args.since,
        full_refresh=args.full_refresh,
        dry_run=args.dry_run,
        resume=not args.no_resume,
        update_watermark=not args.no_watermark_update,
        workers=args.workers,
        page_workers=args.page_workers,
        per_page=args.per_page,
    )


if __name__ == "__main__":
    main()
