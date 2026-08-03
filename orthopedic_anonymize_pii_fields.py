#!/usr/bin/env python3
"""
orthopedic_anonymize_pii_fields.py — Backfill HOSPITALS.ORTHOPEDIC_RAW to replace
real PII/PHI values with the gateway's anonymized equivalents, for fields that were
never covered by anonymization at ingestion time (or whose live coverage changed
after the historical rows were already stored).

For each configured table, re-fetches every current record using the gateway's
"anonymize_fields" (or, for users2, the plain default — its live default already
anonymizes correctly) and overwrites the target keys inside the existing `payload`
VARIANT in-place — all historical rows for a given `_id`, since RAW is append-only.

PATIENTS2 is the one mixed case: `gender` is deliberately EXCLUDED from
anonymize_fields (so the gateway returns it real) while still being written back,
since existing stored rows had it randomized by an older anonymizer pass.

Tables covered here: patients2, users2, suppliers, inventoryledgerentries,
ledgerentries, statemententries, queueentries.

NOT covered:
  - suppliers: emails/phones are ARRAY-typed fields — passing an array field
    name in anonymize_fields 500s the gateway ("Array to string conversion",
    confirmed empirically; the vendor's anonymizer can't mask array fields this
    way). Handled separately by orthopedic_redact_supplier_contacts.py, which
    redacts them directly in Snowflake (no API round-trip needed since the
    replacement is a fixed placeholder, not real anonymized data).
  - orderitementries: fields[].value is nested inside an array the gateway's
    anonymize_fields cannot reach — handled separately by
    orthopedic_redact_orderitementries.py.

NOTE: Payment's namespace 500s under plain default/anonymize:false (the vendor's
default anonymization pass chokes on an array field internally), but works fine
with anonymize_fields=["subjectName"] explicitly (verified through page 8, which
was previously stuck mid-sync) — so it's included here and should also unblock
Payment's stalled ingestion as a side effect.

It does NOT touch ORTHOPEDIC_CLEAN directly — use --refresh-clean to rebuild the
affected CLEAN tables from the corrected RAW afterward.

USAGE
  python orthopedic_anonymize_pii_fields.py                          # all 7 models
  python orthopedic_anonymize_pii_fields.py --models patients2,users2
  python orthopedic_anonymize_pii_fields.py --dry-run
  python orthopedic_anonymize_pii_fields.py --refresh-clean

ENV VARS — same .env as orthopedic_api_to_snowflake.py
"""

from __future__ import annotations

import argparse
import gzip
import logging
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import orthopedic_api_to_snowflake as opl

log = logging.getLogger("orth_anonymize_pii")
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S",
    ))
    log.addHandler(_h)
    log.setLevel("INFO")
    log.propagate = False

S3_FIX_PREFIX = "raw/orthopedic_pii_fix"
STAGE_TABLE   = f"{opl.SF_DB}.{opl.SF_RAW_SCHEMA}._PII_FIX_STAGE"
_SF_FILES_LIMIT = 900

# table -> (anonymize_fields to request | None for plain default, fields to write back to RAW)
MODEL_FIELD_CONFIG: dict[str, dict] = {
    "patients2": {
        "anonymize_fields": ["name", "phone", "nokName", "nokPhone", "email"],
        "write_fields": ["name", "phone", "nokName", "nokPhone", "email", "gender"],
    },
    "users2": {
        "anonymize_fields": None,  # live default already anonymizes these correctly
        "write_fields": ["name", "phone", "email", "username", "password"],
    },
    "payments": {
        "anonymize_fields": ["subjectName"],
        "write_fields": ["subjectName"],
    },
    "inventoryledgerentries": {
        "anonymize_fields": ["desc"],
        "write_fields": ["desc"],
    },
    "ledgerentries": {
        "anonymize_fields": ["subjectName", "notes"],
        "write_fields": ["subjectName", "notes"],
    },
    "statemententries": {
        "anonymize_fields": ["notes"],
        "write_fields": ["notes"],
    },
    "queueentries": {
        "anonymize_fields": ["speech"],
        "write_fields": ["speech"],
    },
}

# ─── FETCH ─────────────────────────────────────────────────────────────────────

def fetch_field_records(namespace: str, write_fields: list[str], per_page: int,
                         page_workers: int, anonymize_fields: list[str] | None) -> list[dict]:
    """Fetch every current record for a namespace, mapped down to {id, **write_fields}
    immediately so full row objects aren't retained in memory."""

    def _extract(rows: list) -> list[dict]:
        out = []
        for r in rows:
            rid = r.get("_id")
            if not rid:
                continue
            rec = {"id": str(rid)}
            rec.update({f: r.get(f) for f in write_fields})
            out.append(rec)
        return out

    payload1 = opl._gateway_request(namespace, 1, per_page, None, anonymize_fields=anonymize_fields)
    first_rows, pag1 = opl._extract_rows_and_pagination(payload1)
    if not first_rows:
        return []

    records = _extract(first_rows)
    last_page = opl._parse_last_page(pag1)

    if last_page is not None and last_page > 1:
        pages = list(range(2, last_page + 1))
        log.info("  %s  last_page=%d — fanning out %d page(s)", namespace.split("\\")[-1], last_page, len(pages))

        def _fetch(p: int) -> list:
            rows, _ = opl._extract_rows_and_pagination(
                opl._gateway_request(namespace, p, per_page, None, anonymize_fields=anonymize_fields)
            )
            return rows

        with ThreadPoolExecutor(max_workers=max(1, page_workers)) as pool:
            for fut in as_completed(pool.submit(_fetch, p) for p in pages):
                records.extend(_extract(fut.result()))

    elif last_page is None:
        page = 1
        while True:
            page += 1
            rows, pag = opl._extract_rows_and_pagination(
                opl._gateway_request(namespace, page, per_page, None, anonymize_fields=anonymize_fields)
            )
            if not rows:
                break
            records.extend(_extract(rows))
            lp = opl._parse_last_page(pag)
            if lp is not None and page >= lp:
                break
            if pag.get("has_more_pages") is False or pag.get("hasMorePages") is False:
                break

    return records

# ─── STAGE + UPDATE ────────────────────────────────────────────────────────────

def upload_records_to_s3(records: list[dict], table: str, run_id: str, chunk_size: int) -> list[str]:
    dt = datetime.now(timezone.utc).date().isoformat()
    keys = []
    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        jsonl_bytes = b"\n".join(opl._dumps_bytes(r) for r in chunk) + b"\n"
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(jsonl_bytes)
        key = f"{S3_FIX_PREFIX}/model={opl._safe(table)}/dt={dt}/{run_id}_p{i // chunk_size:04d}.jsonl.gz"
        opl._s3().put_object(Bucket=opl.S3_BUCKET, Key=key, Body=buf.getvalue())
        keys.append(key)
    return keys

def _build_update_expr(write_fields: list[str]) -> str:
    expr = "t.payload"
    for f in write_fields:
        fk = f.replace("'", "''")
        expr = f"OBJECT_INSERT({expr}, '{fk}', s.DATA['{fk}'], TRUE)"
    return expr

def apply_fix(table: str, write_fields: list[str], s3_keys: list[str], sf: "opl.SnowflakeClient") -> int:
    sf.execute(
        f"CREATE OR REPLACE TEMPORARY TABLE {STAGE_TABLE} (ID VARCHAR, DATA VARIANT);",
        label="stage:create",
    )

    chunks = [s3_keys[i : i + _SF_FILES_LIMIT] for i in range(0, len(s3_keys), _SF_FILES_LIMIT)]
    for idx, chunk in enumerate(chunks, start=1):
        files_sql = ", ".join(f"'{k}'" for k in chunk)
        sf.execute(
            f"""
            COPY INTO {STAGE_TABLE} (ID, DATA)
            FROM (
              SELECT PARSE_JSON($1):id::VARCHAR, PARSE_JSON($1)
              FROM @{opl.SF_STAGE}
            )
            FILES = ({files_sql})
            FILE_FORMAT = (FORMAT_NAME = {opl.SF_FILE_FORMAT})
            ON_ERROR = 'CONTINUE';
            """,
            label=f"stage:copy:{table}({idx}/{len(chunks)})",
        )

    fqn = f"{opl.SF_DB}.{opl.SF_RAW_SCHEMA}.{table.upper()}"
    update_expr = _build_update_expr(write_fields)
    result = sf.execute(
        f"""
        UPDATE {fqn} t
        SET payload = {update_expr}
        FROM {STAGE_TABLE} s
        WHERE t.payload:_id::VARCHAR = s.ID;
        """,
        label=f"update:{table}",
    )
    return result.get("rowcount") or 0

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--models", default=",".join(MODEL_FIELD_CONFIG),
        help=f"Comma-separated table names to fix (default: all — {','.join(MODEL_FIELD_CONFIG)}).",
    )
    ap.add_argument("--per-page", type=int, default=opl.DEFAULT_PER_PAGE)
    ap.add_argument("--page-workers", type=int, default=opl.DEFAULT_PAGE_WORKERS)
    ap.add_argument("--chunk-size", type=int, default=50_000, help="Rows per staged S3 file.")
    ap.add_argument("--dry-run", action="store_true", help="Fetch and report counts only — no S3/Snowflake writes.")
    ap.add_argument(
        "--refresh-clean", action="store_true",
        help="After fixing RAW, run orthopedic_raw_to_clean.py --full-refresh for the same tables.",
    )
    args = ap.parse_args()

    tables = [t.strip().lower() for t in args.models.split(",") if t.strip()]
    unknown = [t for t in tables if t not in MODEL_FIELD_CONFIG]
    if unknown:
        log.error("Unknown/unsupported table(s): %s. Valid: %s", unknown, ", ".join(MODEL_FIELD_CONFIG))
        sys.exit(1)

    run_id = datetime.now(timezone.utc).strftime("piifix__%Y-%m-%dT%H-%M-%SZ")
    log.info("══ START pii-fix · run=%s · tables=%s · dry_run=%s ══", run_id, tables, args.dry_run)

    sf = None if args.dry_run else opl.SnowflakeClient()
    try:
        for table in tables:
            cfg = MODEL_FIELD_CONFIG[table]
            namespace = opl._TABLE_TO_NS[table]
            write_fields = cfg["write_fields"]
            log.info("[%s] fetching %s (anonymize_fields=%s) …", table, write_fields, cfg["anonymize_fields"])
            records = fetch_field_records(namespace, write_fields, args.per_page, args.page_workers, cfg["anonymize_fields"])
            log.info("[%s] fetched %d record(s)", table, len(records))

            if not records:
                log.warning("[%s] nothing fetched — skipping", table)
                continue
            if args.dry_run:
                log.info("[%s] DRY-RUN sample: %s", table, records[:3])
                continue

            keys = upload_records_to_s3(records, table, run_id, args.chunk_size)
            updated = apply_fix(table, write_fields, keys, sf)
            log.info("[%s] RAW rows updated: %d", table, updated)
    finally:
        if sf is not None:
            sf.close()

    if args.dry_run:
        log.info("══ END (dry-run, no writes) ══")
        return

    if args.refresh_clean:
        log.info("Rebuilding CLEAN tables from corrected RAW …")
        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve().parent / "orthopedic_raw_to_clean.py"),
                "--tables", ",".join(tables),
                "--full-refresh",
            ],
            check=True,
        )
    else:
        log.info("RAW fixed. To rebuild CLEAN from the corrected RAW, run:")
        log.info("  python orthopedic_raw_to_clean.py --tables %s --full-refresh", ",".join(tables))

    log.info("══ END ══")


if __name__ == "__main__":
    main()
