#!/usr/bin/env python3
"""
orthopedic_s3_to_snowflake.py — Load already-uploaded S3 files into Snowflake.

Reads the S3 keys recorded in .orthopedic_progress.json and runs COPY INTO
HOSPITALS.ORTHOPEDIC_RAW for each model that has uploaded files but has not
yet been marked done.  No API calls — purely S3 → Snowflake.

USAGE
  # Load all partial models
  python orthopedic_s3_to_snowflake.py

  # Load specific tables only
  python orthopedic_s3_to_snowflake.py --models singleorderitems,orderitementries

  # Preview what would run without touching Snowflake
  python orthopedic_s3_to_snowflake.py --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

# ─── LOGGING ─────────────────────────────────────────────────────────────────

log = logging.getLogger("orth_s3_to_sf")
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

SF_DB         = os.getenv("SNOWFLAKE_DATABASE", "HOSPITALS")
SF_RAW_SCHEMA = "ORTHOPEDIC_RAW"
SF_SHARED     = "SHARED"
SF_STAGE      = f"{SF_DB}.{SF_SHARED}.FACILITY_RAW_STAGE"
SF_FILE_FORMAT = f"{SF_DB}.{SF_SHARED}.JSON_FF"

PROGRESS_FILE = Path(__file__).resolve().parent / ".orthopedic_progress.json"

# namespace for each table (needed for _namespace column in COPY INTO)
_TABLE_TO_NS: dict[str, str] = {
    "orderitementries":      r"App\Models\OrderItemEntry",
    "singleorderitems":      r"App\Models\SingleOrderItem",
    "orders":                r"App\Models\Order",
    "ledgerentries":         r"App\Models\LedgerEntry",
    "statemententries":      r"App\Models\StatementEntry",
    "inventoryledgerentries": r"App\Models\InventoryLedgerEntry",
    "payments":              r"App\Models\Payment",
    "requests":              r"App\Models\Request",
    "queueentries":          r"App\Models\QueueEntry",
    "codings":               r"App\Models\Coding",
    "patientschemes":        r"App\Models\PatientScheme",
    "systemlogs":            r"App\Models\SystemLog",
    "errorlogs":             r"App\Models\ErrorLog",
    "patientplans":          r"App\Models\PatientPlan",
    "reorderlevels":         r"App\Models\ReorderLevel",
    "saleitems":             r"App\Models\SaleItem",
    "inventoryitems":        r"App\Models\InventoryItem",
    "purchaseorders":        r"App\Models\PurchaseOrder",
    "reports":               r"App\Models\Report",
    "shifts":                r"App\Models\Shift",
    "suppliers":             r"App\Models\Supplier",
    "patientinvoices":       r"App\Models\PatientInvoice",
    "diagnoses2":            r"App\Models\Diagnosis2",
    "patients2":             r"App\Models\Patient2",
    "invoices2":             r"App\Models\Invoice2",
    "users2":                r"App\Models\Users2",
}

_SF_FILES_LIMIT = 1_000  # Snowflake hard limit per COPY INTO statement

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
            user=user, account=account, warehouse=warehouse,
            database=database, schema=SF_RAW_SCHEMA,
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
                f"Snowflake connection failed — account={account} user={user}. Cause: {e}"
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
        log.info("▶ %-40s | %s…", label, " ".join(sql.split())[:100])
        t0 = time.perf_counter()
        with self._lock, self._cursor() as cur:
            cur.execute(sql)
            rowcount, sfqid = cur.rowcount, cur.sfqid
        log.info("✓ %-40s rowcount=%s  %.2fs", label, rowcount, time.perf_counter() - t0)
        return {"rowcount": rowcount, "sfqid": sfqid}

    def __enter__(self): return self
    def __exit__(self, *_): self.close()

# ─── PROGRESS FILE ────────────────────────────────────────────────────────────

def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text())
        except Exception:
            pass
    return {"models": {}}

def _save_progress(data: dict) -> None:
    PROGRESS_FILE.write_text(json.dumps(data, indent=2))

def _mark_done(data: dict, table: str, total_rows: int) -> None:
    m = data.setdefault("models", {}).setdefault(table, {})
    m["status"]     = "done"
    m["total_rows"] = total_rows
    _save_progress(data)

# ─── COPY INTO ────────────────────────────────────────────────────────────────

def copy_into(table: str, namespace: str, run_id: str, s3_keys: list[str], sf: SnowflakeClient) -> int:
    """Run COPY INTO for the given S3 keys, chunked at 1,000 files. Returns total rowcount."""
    fqn     = f"{SF_DB}.{SF_RAW_SCHEMA}.{table.upper()}"
    ns_esc  = namespace.replace("'", "\\'")
    run_esc = run_id.replace("'", "\\'")

    chunks = [s3_keys[i : i + _SF_FILES_LIMIT] for i in range(0, len(s3_keys), _SF_FILES_LIMIT)]
    total_rows = 0

    for idx, chunk in enumerate(chunks, start=1):
        files_sql = ", ".join(f"'{k}'" for k in chunk)
        label = (
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
        result = sf.execute(sql, label=label)
        total_rows += result.get("rowcount") or 0

    return total_rows

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Load existing S3 files into Snowflake ORTHOPEDIC_RAW.")
    parser.add_argument("--models", help="Comma-separated table names to load (default: all partial)")
    parser.add_argument("--run-id", default="s3_reload", help="_run_id value to stamp rows with (default: s3_reload)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would run without executing")
    parser.add_argument("--force", action="store_true", help="Re-load tables already marked done")
    args = parser.parse_args()

    progress = _load_progress()
    models_data: dict[str, dict] = progress.get("models", {})

    # Determine target tables
    if args.models:
        targets = [t.strip().lower() for t in args.models.split(",")]
    else:
        targets = list(models_data.keys())

    if not targets:
        log.info("No models found in %s — nothing to do.", PROGRESS_FILE)
        return

    # Filter to tables that have s3_keys
    to_load: list[tuple[str, list[str]]] = []
    for table in targets:
        mdata = models_data.get(table, {})
        status = mdata.get("status")

        if status == "done" and not args.force:
            log.info("  %-35s  already done — skipping (use --force to reload)", table)
            continue

        raw_keys: dict = mdata.get("s3_keys", {})
        if not raw_keys:
            log.warning("  %-35s  no s3_keys found in progress file — skipping", table)
            continue

        # Sort keys by page number (integer sort on the dict keys)
        ordered_keys = [raw_keys[k] for k in sorted(raw_keys, key=lambda x: int(x))]
        to_load.append((table, ordered_keys))

    if not to_load:
        log.info("Nothing to load.")
        return

    log.info("Tables to load: %s", [t for t, _ in to_load])

    if args.dry_run:
        for table, keys in to_load:
            log.info("  DRY-RUN  %-35s  %d files  %d COPY INTO batches",
                     table, len(keys), (len(keys) + _SF_FILES_LIMIT - 1) // _SF_FILES_LIMIT)
        return

    with SnowflakeClient() as sf:
        for table, s3_keys in to_load:
            namespace = _TABLE_TO_NS.get(table, f"App\\Models\\{table.title()}")
            log.info("  %-35s  loading %d S3 files → Snowflake …", table, len(s3_keys))
            try:
                total_rows = copy_into(table, namespace, args.run_id, s3_keys, sf)
                _mark_done(progress, table, total_rows)
                log.info("  %-35s  done  rows_loaded=%d", table, total_rows)
            except Exception as e:
                log.error("  %-35s  FAILED: %s", table, e)

    log.info("All done.")


if __name__ == "__main__":
    main()
