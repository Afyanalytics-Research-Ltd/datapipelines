#!/usr/bin/env python3
"""
orthopedic_fix_anonymized_names.py — Backfill real `name` values into
HOSPITALS.ORTHOPEDIC_RAW for DIAGNOSES2, SINGLEORDERITEMS, SALEITEMS.

These three models were originally ingested with the Afya Extraction gateway's
anonymization on, so `payload:name` holds a fake generated name instead of the
real one. Passing `"anonymize": false` in the gateway request body returns the
real name. This script re-fetches every current record for the affected models
with anonymize=false, then patches the `name` key inside the existing `payload`
VARIANT in-place (all historical rows for a given `_id`, since RAW is append-only
and the same record can appear across multiple ingestion runs).

It does NOT touch ORTHOPEDIC_CLEAN directly — CLEAN is mechanically flattened
from RAW by orthopedic_raw_to_clean.py, so once RAW is corrected the fix is
picked up by re-running that script with --full-refresh on the same tables
(this script can do that for you with --refresh-clean).

Going forward, orthopedic_api_to_snowflake.py itself now sends anonymize=false
for these three models, so newly ingested/updated rows won't need this fix.

USAGE
  python orthopedic_fix_anonymized_names.py                       # all 3 models
  python orthopedic_fix_anonymized_names.py --models diagnoses2
  python orthopedic_fix_anonymized_names.py --dry-run              # fetch only, no writes
  python orthopedic_fix_anonymized_names.py --refresh-clean        # also rebuild CLEAN after

ENV VARS  — same .env as orthopedic_api_to_snowflake.py (Afya, Snowflake, S3 creds)
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

log = logging.getLogger("orth_fix_names")
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S",
    ))
    log.addHandler(_h)
    log.setLevel("INFO")
    log.propagate = False

DEFAULT_MODELS = ["diagnoses2", "singleorderitems", "saleitems"]
S3_FIX_PREFIX  = "raw/orthopedic_name_fix"
STAGE_TABLE    = f"{opl.SF_DB}.{opl.SF_RAW_SCHEMA}._NAME_FIX_STAGE"

# ─── FETCH (id, real_name) PAIRS FOR ONE MODEL ────────────────────────────────

def _extract_pairs(rows: list) -> list[tuple[str, str]]:
    out = []
    for r in rows:
        rid, name = r.get("_id"), r.get("name")
        if rid and name:
            out.append((str(rid), str(name)))
    return out

def fetch_id_name_pairs(namespace: str, per_page: int, page_workers: int) -> list[tuple[str, str]]:
    """Fetch every current record for a namespace with anonymize=false, mapped
    down to (id, real_name) immediately so full row objects aren't retained."""
    payload1 = opl._gateway_request(namespace, 1, per_page, None, anonymize=False)
    first_rows, pag1 = opl._extract_rows_and_pagination(payload1)
    if not first_rows:
        return []

    pairs = _extract_pairs(first_rows)
    last_page = opl._parse_last_page(pag1)

    if last_page is not None and last_page > 1:
        pages = list(range(2, last_page + 1))
        log.info("  %s  last_page=%d — fanning out %d page(s)", namespace.split("\\")[-1], last_page, len(pages))

        def _fetch(p: int) -> list:
            rows, _ = opl._extract_rows_and_pagination(
                opl._gateway_request(namespace, p, per_page, None, anonymize=False)
            )
            return rows

        with ThreadPoolExecutor(max_workers=max(1, page_workers)) as pool:
            for fut in as_completed(pool.submit(_fetch, p) for p in pages):
                pairs.extend(_extract_pairs(fut.result()))

    elif last_page is None:
        page = 1
        while True:
            page += 1
            rows, pag = opl._extract_rows_and_pagination(
                opl._gateway_request(namespace, page, per_page, None, anonymize=False)
            )
            if not rows:
                break
            pairs.extend(_extract_pairs(rows))
            lp = opl._parse_last_page(pag)
            if lp is not None and page >= lp:
                break
            if pag.get("has_more_pages") is False or pag.get("hasMorePages") is False:
                break

    return pairs

# ─── STAGE PAIRS IN S3 + LOAD + UPDATE ────────────────────────────────────────

def upload_pairs_to_s3(pairs: list[tuple[str, str]], table: str, run_id: str,
                        chunk_size: int) -> list[str]:
    dt = datetime.now(timezone.utc).date().isoformat()
    keys = []
    for i in range(0, len(pairs), chunk_size):
        chunk = pairs[i : i + chunk_size]
        rows = [{"id": rid, "name": name} for rid, name in chunk]
        jsonl_bytes = b"\n".join(opl._dumps_bytes(r) for r in rows) + b"\n"
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(jsonl_bytes)
        key = f"{S3_FIX_PREFIX}/model={opl._safe(table)}/dt={dt}/{run_id}_p{i // chunk_size:04d}.jsonl.gz"
        opl._s3().put_object(Bucket=opl.S3_BUCKET, Key=key, Body=buf.getvalue())
        keys.append(key)
    return keys

def apply_fix(table: str, s3_keys: list[str], sf: "opl.SnowflakeClient") -> int:
    sf.execute(
        f"CREATE OR REPLACE TEMPORARY TABLE {STAGE_TABLE} (ID VARCHAR, REAL_NAME VARCHAR);",
        label="stage:create",
    )
    files_sql = ", ".join(f"'{k}'" for k in s3_keys)
    sf.execute(
        f"""
        COPY INTO {STAGE_TABLE} (ID, REAL_NAME)
        FROM (
          SELECT
            PARSE_JSON($1):id::VARCHAR   AS ID,
            PARSE_JSON($1):name::VARCHAR AS REAL_NAME
          FROM @{opl.SF_STAGE}
        )
        FILES = ({files_sql})
        FILE_FORMAT = (FORMAT_NAME = {opl.SF_FILE_FORMAT})
        ON_ERROR = 'CONTINUE';
        """,
        label=f"stage:copy:{table}",
    )

    fqn = f"{opl.SF_DB}.{opl.SF_RAW_SCHEMA}.{table.upper()}"
    result = sf.execute(
        f"""
        UPDATE {fqn} t
        SET payload = OBJECT_INSERT(t.payload, 'name', TO_VARIANT(s.REAL_NAME), TRUE)
        FROM {STAGE_TABLE} s
        WHERE t.payload:_id::VARCHAR = s.ID
          AND s.REAL_NAME IS NOT NULL;
        """,
        label=f"update:{table}",
    )
    return result.get("rowcount") or 0

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--models", default=",".join(DEFAULT_MODELS),
        help=f"Comma-separated table names to fix (default: {','.join(DEFAULT_MODELS)}).",
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
    unknown = [t for t in tables if t not in opl._TABLE_TO_NS]
    if unknown:
        log.error("Unknown table(s): %s. Valid: %s", unknown, ", ".join(opl._TABLE_TO_NS))
        sys.exit(1)

    run_id = datetime.now(timezone.utc).strftime("namefix__%Y-%m-%dT%H-%M-%SZ")
    log.info("══ START name-fix · run=%s · tables=%s · dry_run=%s ══", run_id, tables, args.dry_run)

    sf = None if args.dry_run else opl.SnowflakeClient()
    try:
        for table in tables:
            namespace = opl._TABLE_TO_NS[table]
            log.info("[%s] fetching real names (anonymize=false) …", table)
            pairs = fetch_id_name_pairs(namespace, args.per_page, args.page_workers)
            log.info("[%s] fetched %d (id, real_name) pairs", table, len(pairs))

            if not pairs:
                log.warning("[%s] nothing fetched — skipping", table)
                continue
            if args.dry_run:
                log.info("[%s] DRY-RUN sample: %s", table, pairs[:5])
                continue

            keys = upload_pairs_to_s3(pairs, table, run_id, args.chunk_size)
            updated = apply_fix(table, keys, sf)
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
