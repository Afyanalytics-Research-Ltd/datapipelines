#!/usr/bin/env python3
"""
orthopedic_v2_clean_pipeline.py — HOSPITALS.ORTHOPEDIC_RAW_V2 → HOSPITALS.ORTHOPEDIC_CLEAN_V2

Flattens, de-identifies, and deduplicates the 19 raw model tables landed by
orthopedic_v2_raw_pipeline.py into an analyst-facing CLEAN schema.

WHAT THIS SCRIPT DOES
  1. Reads pii_classification_v2.json (produced/updated by
     orthopedic_v2_schema_discovery.py and reviewed by a human — see that
     script's docstring). Refuses to touch any model that isn't
     "approved": true, unless --force-unapproved is passed explicitly, OR the
     model itself has "bypass": true (see GOVERNANCE below).
  2. For each approved (or bypassed) model:
       a. Ensures HOSPITALS.ORTHOPEDIC_CLEAN_V2.<table> exists with one
          column per classified field (adding new columns as schema evolves).
       b. Reads new/changed rows from HOSPITALS.ORTHOPEDIC_RAW_V2.<table>
          (payload VARIANT) since the clean-layer watermark.
       c. DIRECT_IDENTIFIER fields → SHA2-256 pseudonymized into "<field>_hash"
          (irreversible; same source value always hashes the same way, so
          joins/counts across records for the same person still work without
          exposing the identifier itself).
       d. Free-text CLINICAL_CONTENT fields that commonly carry hand-typed
          identifiers (notes, history, impression, progress, remarks, ...)
          get a defense-in-depth REGEXP_REPLACE pass that redacts embedded
          phone numbers / emails, on top of keeping the clinical content.
       e. QUASI_IDENTIFIER / STAFF_IDENTIFIER / SYSTEM_META / other
          CLINICAL_CONTENT fields are kept as-is.
       f. QUALIFY ROW_NUMBER() ... = 1 picks only the LATEST raw snapshot per
          business key (id) before merging, and the MERGE itself is keyed on
          that same id — so re-running never creates duplicate rows in the
          clean table, and reprocessing after a correction upstream simply
          updates the existing row instead of appending a new one.
  3. Advances a per-table watermark (.orthopedic_v2_clean_watermarks.json)
     only after a table's MERGE succeeds.

GOVERNANCE
  - HOSPITALS.ORTHOPEDIC_CLEAN_V2 is the layer meant for analyst/BI access.
  - Direct identifiers never appear in plaintext in this schema — UNLESS a
    model has "bypass": true in pii_classification_v2.json. Per-model escape
    hatch for when "approved": true isn't set yet (classification not fully
    reviewed) but you want that model's data flowing into CLEAN_V2 now anyway:
    every field is flattened straight from RAW, unmasked, including fields
    that would otherwise be HASHed as DIRECT_IDENTIFIER. Unlike
    --force-unapproved (a blanket CLI flag covering every unapproved model AND
    still applying each field's classified action), "bypass" is opt-in per
    model in the JSON itself and always skips masking for that model,
    regardless of --force-unapproved. Ignored if the model also has
    "approved": true (approved models always get proper per-field masking).
  - "append_only": true on a model (set in pii_classification_v2.json) skips
    the id-based dedup/MERGE entirely and just INSERTs matching rows straight
    from RAW — for tables where no field is actually unique per record, so
    there's no reliable key to merge on. --full-refresh truncates first (else
    every rerun would duplicate all rows); incremental runs only ever append
    rows newer than the watermark. Confirm no field is unique before reaching
    for this — a real primary_key + normal MERGE is always preferable where
    one exists (see admnotes as the example: date/description/pid/time/
    username were all checked and none were unique).
  - patient_id-style foreign keys are NOT hashed here (they're the join key
    across clean tables) — but note that a facility_id + patient_id + a
    quasi-identifier (e.g. DOB) is, in aggregate, a linkage risk. Recommend
    Snowflake row/column access policies on ORTHOPEDIC_CLEAN_V2 in addition
    to this script's masking, per your org's data governance policy.

USAGE
  python orthopedic_v2_clean_pipeline.py                     # all approved/bypassed models, incremental
  python orthopedic_v2_clean_pipeline.py --models patients,triage
  python orthopedic_v2_clean_pipeline.py --full-refresh       # reprocess all raw history
  python orthopedic_v2_clean_pipeline.py --dry-run            # print SQL, no writes
  python orthopedic_v2_clean_pipeline.py --force-unapproved   # DANGEROUS: bypass the governance gate
                                                                #   (still masks per classified action —
                                                                #    set "bypass": true per-model instead
                                                                #    to flatten raw/unmasked)

ENV VARS — same Snowflake vars as orthopedic_v2_raw_pipeline.py
  (SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
   SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

log = logging.getLogger("orthopedic_v2_clean_pipeline")
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S",
    ))
    log.addHandler(_h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ─── CONFIG ──────────────────────────────────────────────────────────────────

SF_DB           = os.getenv("SNOWFLAKE_DATABASE", "HOSPITALS")
SF_RAW_SCHEMA   = "ORTHOPEDIC_RAW_V2"
SF_CLEAN_SCHEMA = "ORTHOPEDIC_CLEAN_V2"

HERE                 = Path(__file__).resolve().parent
CLASSIFICATION_FILE  = HERE / "pii_classification_v2.json"
CLEAN_WATERMARK_FILE = HERE / ".orthopedic_v2_clean_watermarks.json"

# Fields commonly holding hand-typed free text where a phone/email could be
# embedded even though the column itself is clinical content, not a contact
# field. Matched against the field name (case-insensitive substring).
NOTE_LIKE_FIELD_HINTS = (
    "note", "impression", "history", "progress", "remark", "comment",
    "complaint", "finding", "instruction", "summary",
)

# Regexes used for the defense-in-depth in-text redaction pass.
# NOTE: deliberately backslash-free. A backslash-escaped version of these
# (e.g. \+, \., \b) broke in transit to Snowflake — REGEXP_REPLACE received
# the pattern with backslashes silently stripped, turning "\+?" into a bare
# "+?" with nothing to repeat ("no argument for repetition operator: +").
# Character classes give the same matching power without needing any
# backslash escapes, so there's nothing left that can be mangled.
_EMAIL_RX = r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}'
_PHONE_RX = r'([+]254|0)[0-9]{9}|[+]?[0-9]{10,13}'

# Default assumed primary key per model (Laravel convention). Override per
# model via `"primary_key": "..."` inside pii_classification_v2.json if a
# table's real PK differs (schema discovery / describe should surface this).
DEFAULT_PRIMARY_KEY = "id"

# ─── JSON HELPERS ─────────────────────────────────────────────────────────────

def _load_json(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception as e:
            log.warning("Could not parse %s: %s", path, e)
    return {}

def _save_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True))

_wm_lock = threading.Lock()

def get_watermark(table: str, default: str = "1970-01-01T00:00:00Z") -> str:
    return _load_json(CLEAN_WATERMARK_FILE).get(table, default)

def set_watermark(table: str, ts_iso: str) -> None:
    with _wm_lock:
        wm = _load_json(CLEAN_WATERMARK_FILE)
        wm[table] = ts_iso
        _save_json(CLEAN_WATERMARK_FILE, wm)
    log.info("Clean watermark [%s] → %s", table, ts_iso)

def load_classification() -> dict:
    if not CLASSIFICATION_FILE.exists():
        raise RuntimeError(
            f"{CLASSIFICATION_FILE.name} not found. Run "
            "orthopedic_v2_schema_discovery.py first, review the suggested "
            "field classification, mark models \"approved\": true, then "
            "re-run this pipeline."
        )
    return json.loads(CLASSIFICATION_FILE.read_text())

# ─── SNOWFLAKE CLIENT (same shape as the v1/v2 raw pipelines) ────────────────

class SnowflakeClient:
    def __init__(self, schema: str = SF_CLEAN_SCHEMA):
        user      = os.getenv("SNOWFLAKE_USER", "").strip()
        account   = os.getenv("SNOWFLAKE_ACCOUNT", "").strip()
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "").strip()
        database  = os.getenv("SNOWFLAKE_DATABASE", SF_DB).strip()
        key_path  = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()
        password  = os.getenv("SNOWFLAKE_PASSWORD", "").strip()

        for name, val in [
            ("SNOWFLAKE_USER", user), ("SNOWFLAKE_ACCOUNT", account),
            ("SNOWFLAKE_WAREHOUSE", warehouse), ("SNOWFLAKE_DATABASE", database),
        ]:
            if not val:
                raise RuntimeError(f"Missing env var {name} — add it to your .env.")
        if not key_path and not password:
            raise RuntimeError(
                "Snowflake auth: set SNOWFLAKE_PRIVATE_KEY_PATH (key-pair) "
                "or SNOWFLAKE_PASSWORD in your .env."
            )

        kwargs: dict = dict(user=user, account=account, warehouse=warehouse,
                            database=database, schema=schema)
        if key_path:
            if not Path(key_path).exists():
                raise RuntimeError(f"Snowflake private key not found: {key_path}")
            kwargs["private_key_file"] = key_path
        else:
            kwargs["password"] = password

        try:
            self._conn = snowflake.connector.connect(**kwargs)
        except Exception as e:
            raise RuntimeError(f"Snowflake connection failed — account={account} user={user}. Cause: {e}") from e
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
        log.info("▶ %-35s | %s…", label, " ".join(sql.split())[:140])
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

    def query(self, sql: str, label: str | None = None) -> list[tuple]:
        label = label or f"q:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        with self._lock, self._cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def __enter__(self): return self
    def __exit__(self, *_): self.close()

# ─── COLUMN-EXPRESSION BUILDER ────────────────────────────────────────────────

def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def _column_expr(field: str, info: dict, bypass: bool = False) -> tuple[str, str]:
    """Return (select_expr, clean_column_name) for one classified field.

    bypass=True forces a raw passthrough regardless of the field's classified
    action/category — used for models with "bypass": true in the
    classification file, which flatten straight into CLEAN_V2 unmasked
    (including DIRECT_IDENTIFIER fields that would otherwise be hashed)."""
    action   = "KEEP" if bypass else info.get("action", "HASH")
    category = info.get("category", "UNKNOWN")
    src      = f'payload:{_quote_ident(field)}'

    if action == "HASH":
        col = f"{field}_hash"
        expr = (
            f"CASE WHEN {src} IS NULL OR {src}::STRING = '' THEN NULL "
            f"ELSE SHA2({src}::STRING, 256) END AS {_quote_ident(col)}"
        )
        return expr, col

    col = field
    is_note_like = (not bypass) and category == "CLINICAL_CONTENT" and any(
        hint in field.lower() for hint in NOTE_LIKE_FIELD_HINTS
    )
    if is_note_like:
        # Defense-in-depth: keep the clinical text, but scrub embedded
        # emails/phone numbers a staff member may have hand-typed into it.
        expr = (
            f"REGEXP_REPLACE(REGEXP_REPLACE({src}::STRING, "
            f"'{_EMAIL_RX}', '[REDACTED_EMAIL]'), "
            f"'{_PHONE_RX}', '[REDACTED_PHONE]') AS {_quote_ident(col)}"
        )
    else:
        expr = f"{src}::STRING AS {_quote_ident(col)}"
    return expr, col

# ─── SCHEMA BOOTSTRAP (CLEAN SIDE) ────────────────────────────────────────────

def _table_fqn(schema: str, table: str) -> str:
    return f"{SF_DB}.{schema}.{table.upper()}"

def ensure_clean_table(sf: SnowflakeClient, table: str, columns: list[str]) -> None:
    fqn = _table_fqn(SF_CLEAN_SCHEMA, table)
    col_defs = ",\n                ".join(f"{_quote_ident(c)} STRING" for c in columns)
    sf.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            {col_defs},
            _source_run_id      VARCHAR,
            _raw_ingested_at    TIMESTAMP_TZ,
            _clean_processed_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """,
        label=f"ensure_clean:{table}",
    )
    # Schema evolution: add any newly-classified columns to an existing table.
    for c in columns:
        try:
            sf.execute(
                f'ALTER TABLE {fqn} ADD COLUMN IF NOT EXISTS {_quote_ident(c)} STRING;',
                label=f"evolve:{table}.{c}",
            )
        except Exception as e:
            log.warning("Could not add column %s to %s (may already exist): %s", c, fqn, e)

# ─── CLEAN ONE MODEL (flatten + mask + dedup MERGE) ──────────────────────────

def clean_one_model(
    sf: SnowflakeClient,
    table: str,
    model_cls: dict,
    *,
    since: str,
    full_refresh: bool,
    dry_run: bool,
    bypass: bool = False,
) -> dict:
    fields: dict = model_cls.get("fields", {})
    if not fields:
        return {"table": table, "status": "skipped_no_fields"}

    append_only = bool(model_cls.get("append_only"))

    if not append_only:
        primary_key = model_cls.get("primary_key", DEFAULT_PRIMARY_KEY)
        if primary_key not in fields:
            # Always project the PK even if the classifier didn't see it as a
            # distinct "field" entry (e.g. it was folded under SYSTEM_META already).
            fields = {primary_key: {"category": "SYSTEM_META", "action": "KEEP"}, **fields}

    select_exprs, clean_columns = [], []
    pk_clean_col = None
    for fname, info in fields.items():
        expr, col = _column_expr(fname, info, bypass=bypass)
        select_exprs.append(expr)
        clean_columns.append(col)
        if not append_only and fname == primary_key:
            pk_clean_col = col  # PK is never hashed in practice, but stay generic

    raw_fqn   = _table_fqn(SF_RAW_SCHEMA, table)
    clean_fqn = _table_fqn(SF_CLEAN_SCHEMA, table)

    if not dry_run:
        ensure_clean_table(sf, table, clean_columns)

    where_clause = "" if full_refresh else f"WHERE _ingested_at > '{since}'"

    insert_cols_sql = ", ".join(_quote_ident(c) for c in clean_columns) + \
        ", _source_run_id, _raw_ingested_at"

    if append_only:
        # No field is reliably unique per record, so there's no key to
        # dedup/merge on — just append. A --full-refresh truncates first so
        # re-running doesn't duplicate every row; incremental runs only ever
        # select rows newer than the watermark, so appending is safe there
        # without truncating.
        insert_sql = f"""
        INSERT INTO {clean_fqn} ({insert_cols_sql})
        SELECT
          {", ".join(select_exprs)},
          _run_id      AS _source_run_id,
          _ingested_at AS _raw_ingested_at
        FROM {raw_fqn}
        {where_clause};
        """
        if dry_run:
            log.info("DRY-RUN append-only INSERT SQL for %s:\n%s", table, insert_sql)
            return {"table": table, "status": "dry_run"}

        if full_refresh:
            sf.execute(f"TRUNCATE TABLE IF EXISTS {clean_fqn};", label=f"truncate:{table}")
        result = sf.execute(insert_sql, label=f"append:{table}")
        set_watermark(table, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
        return {"table": table, "status": "ok", "rowcount": result.get("rowcount")}

    src_sql = f"""
    (
      SELECT
        {", ".join(select_exprs)},
        _run_id           AS _source_run_id,
        _ingested_at      AS _raw_ingested_at
      FROM {raw_fqn}
      {where_clause}
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payload:{_quote_ident(primary_key)}
        ORDER BY _ingested_at DESC
      ) = 1
    )
    """

    update_set = ", ".join(
        f"tgt.{_quote_ident(c)} = src.{_quote_ident(c)}" for c in clean_columns if c != pk_clean_col
    )
    update_set += (
        f", tgt._source_run_id = src._source_run_id"
        f", tgt._raw_ingested_at = src._raw_ingested_at"
        f", tgt._clean_processed_at = CURRENT_TIMESTAMP()"
    )
    # NOTE: _source_run_id / _raw_ingested_at were created UNQUOTED in
    # ensure_clean_table (so Snowflake normalized them to uppercase). They
    # must stay unquoted here too — quoting them would make Snowflake look
    # for a case-sensitive "_source_run_id" column that doesn't exist and
    # fail with "invalid identifier". Only the user-defined clean_columns
    # (arbitrary/mixed-case field names) need _quote_ident. insert_cols_sql
    # was already built above (shared with the append_only path).
    insert_vals_sql = ", ".join(f"src.{_quote_ident(c)}" for c in clean_columns) + \
        ", src._source_run_id, src._raw_ingested_at"

    merge_sql = f"""
    MERGE INTO {clean_fqn} AS tgt
    USING {src_sql} AS src
    ON tgt.{_quote_ident(pk_clean_col)} = src.{_quote_ident(pk_clean_col)}
    WHEN MATCHED THEN UPDATE SET {update_set}
    WHEN NOT MATCHED THEN INSERT ({insert_cols_sql})
      VALUES ({insert_vals_sql});
    """

    if dry_run:
        log.info("DRY-RUN merge SQL for %s:\n%s", table, merge_sql)
        return {"table": table, "status": "dry_run"}

    result = sf.execute(merge_sql, label=f"merge:{table}")
    set_watermark(table, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    return {"table": table, "status": "ok", "rowcount": result.get("rowcount")}

# ─── ORCHESTRATOR ─────────────────────────────────────────────────────────────

def run_pipeline(
    only_tables: list[str] | None = None,
    *,
    full_refresh: bool = False,
    dry_run: bool = False,
    force_unapproved: bool = False,
) -> None:
    classification = load_classification()
    models: dict = classification.get("models", {})

    if only_tables:
        wanted = {t.lower() for t in only_tables}
        models = {t: m for t, m in models.items() if t in wanted}
        if not models:
            log.error("No matching tables in %s for --models %s", CLASSIFICATION_FILE.name, only_tables)
            sys.exit(1)

    approved  = {t: m for t, m in models.items() if m.get("approved") or m.get("bypass") or force_unapproved}
    bypassed  = sorted(t for t, m in approved.items() if m.get("bypass") and not m.get("approved"))
    unapproved = sorted(set(models) - set(approved))
    if unapproved:
        log.warning(
            "⚠ SKIPPING %d unapproved model(s) (set \"approved\": true, or \"bypass\": true to flatten "
            "unmasked, in %s after review): %s",
            len(unapproved), CLASSIFICATION_FILE.name, ", ".join(unapproved),
        )
    if bypassed:
        log.warning(
            "⚠ BYPASS: %d model(s) have \"bypass\": true — flattening RAW straight into CLEAN_V2 "
            "UNMASKED (identities included, no hashing, no redaction) despite not being approved: %s",
            len(bypassed), ", ".join(bypassed),
        )
    if force_unapproved and unapproved:
        log.warning("--force-unapproved set: processing them anyway. This bypasses the governance gate.")

    if not approved:
        log.error("No approved models to process. Run orthopedic_v2_schema_discovery.py, "
                   "review pii_classification_v2.json, approve models (or set \"bypass\": true "
                   "on specific ones), then re-run.")
        sys.exit(1)

    log.info("══ START orthopedic_v2_clean_pipeline · %d model(s) ══", len(approved))

    sf = None if dry_run else SnowflakeClient()
    if not dry_run:
        sf.execute(f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_CLEAN_SCHEMA};", label="ensure_clean_schema")

    successes, failures = [], []
    try:
        for table, model_cls in approved.items():
            since = "1970-01-01T00:00:00Z" if full_refresh else get_watermark(table)
            model_bypass = bool(model_cls.get("bypass")) and not model_cls.get("approved")
            try:
                res = clean_one_model(
                    sf if not dry_run else _NullSF(),
                    table, model_cls,
                    since=since, full_refresh=full_refresh, dry_run=dry_run,
                    bypass=model_bypass,
                )
                successes.append(res)
            except Exception as e:
                log.error("✗ %s FAILED — %s", table, e, exc_info=True)
                failures.append({"table": table, "error": str(e)})
    finally:
        if sf is not None:
            sf.close()

    log.info("══ END ✓ %d ok · ✗ %d failed ══", len(successes), len(failures))
    if failures:
        for f in failures:
            log.warning("  · %-25s %s", f["table"], f["error"][:200])
        sys.exit(1)

class _NullSF:
    """Stand-in used only for --dry-run so ensure_clean_table/execute calls
    used inside clean_one_model don't need Snowflake creds just to print SQL."""
    def execute(self, *a, **k):
        return {"rowcount": 0, "sfqid": None}

# ─── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--models", help="Comma-separated table names, e.g. patients,triage. Omit for all approved models.")
    ap.add_argument("--full-refresh", action="store_true", help="Reprocess all raw history, not just since the last watermark.")
    ap.add_argument("--dry-run", action="store_true", help="Print the MERGE SQL for each model; no Snowflake writes.")
    ap.add_argument("--force-unapproved", action="store_true",
                     help="DANGEROUS: process models even if approved=false in pii_classification_v2.json.")
    args = ap.parse_args()

    only_tables = None
    if args.models:
        only_tables = [t.strip().lower() for t in args.models.split(",") if t.strip()]

    run_pipeline(
        only_tables=only_tables,
        full_refresh=args.full_refresh,
        dry_run=args.dry_run,
        force_unapproved=args.force_unapproved,
    )


if __name__ == "__main__":
    main()