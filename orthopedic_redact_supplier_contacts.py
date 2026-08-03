#!/usr/bin/env python3
"""
orthopedic_redact_supplier_contacts.py — Redact real business contact details
already stored in HOSPITALS.ORTHOPEDIC_RAW.SUPPLIERS.emails / .phones (Issue 24).

Both are ARRAY-typed fields. Passing an array field name in the gateway's
anonymize_fields 500s with "Array to string conversion" (confirmed empirically —
the vendor's anonymizer can't mask array-typed fields), so this can't be fixed
via a request parameter. Since the replacement is a fixed placeholder rather
than real per-record anonymized data, no API round-trip is needed — this
redacts directly in Snowflake.

orthopedic_api_to_snowflake.py now applies the same redaction to every row
BEFORE upload for newly ingested/updated Supplier records (see
_redact_supplier_contacts in that file), so this script is a one-time
historical backfill for the rows already in RAW.

USAGE
  python orthopedic_redact_supplier_contacts.py
  python orthopedic_redact_supplier_contacts.py --dry-run
  python orthopedic_redact_supplier_contacts.py --refresh-clean
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

import orthopedic_api_to_snowflake as opl

log = logging.getLogger("orth_redact_supplier")
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S",
    ))
    log.addHandler(_h)
    log.setLevel("INFO")
    log.propagate = False

RAW_FQN = f"{opl.SF_DB}.{opl.SF_RAW_SCHEMA}.SUPPLIERS"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="Report counts only — no writes.")
    ap.add_argument(
        "--refresh-clean", action="store_true",
        help="After redacting RAW, run orthopedic_raw_to_clean.py --full-refresh for suppliers.",
    )
    args = ap.parse_args()

    with opl.SnowflakeClient() as sf:
        total, with_email, with_phone = sf.query(
            f"""
            SELECT COUNT(*),
                   SUM(CASE WHEN ARRAY_SIZE(payload:emails) > 0 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN ARRAY_SIZE(payload:phones) > 0 THEN 1 ELSE 0 END)
            FROM {RAW_FQN}
            """,
            label="count_rows",
        )[0]
        log.info("SUPPLIERS: %d total rows, %d with emails, %d with phones", total, with_email, with_phone)

        if args.dry_run:
            log.info("DRY-RUN — no writes performed.")
            return

        result = sf.execute(
            f"""
            UPDATE {RAW_FQN}
            SET payload = OBJECT_INSERT(
                OBJECT_INSERT(payload, 'emails', ARRAY_CONSTRUCT('[REDACTED]'), TRUE),
                'phones', ARRAY_CONSTRUCT('[REDACTED]'), TRUE
            )
            WHERE ARRAY_SIZE(payload:emails) > 0 OR ARRAY_SIZE(payload:phones) > 0;
            """,
            label="redact:suppliers",
        )
        log.info("RAW rows updated: %s", result.get("rowcount"))

    if args.refresh_clean:
        log.info("Rebuilding CLEAN.SUPPLIERS from redacted RAW …")
        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve().parent / "orthopedic_raw_to_clean.py"),
                "--tables", "suppliers",
                "--full-refresh",
            ],
            check=True,
        )
    else:
        log.info("RAW redacted. To rebuild CLEAN, run:")
        log.info("  python orthopedic_raw_to_clean.py --tables suppliers --full-refresh")

    log.info("══ END ══")


if __name__ == "__main__":
    main()
