#!/usr/bin/env python3
"""
orthopedic_redact_orderitementries.py — Redact free-text clinical PHI already
stored in HOSPITALS.ORTHOPEDIC_RAW.ORDERITEMENTRIES (Issue 10).

payload:fields is an array of form-field entries. Two categories get redacted:
  - free-text entries (type containing 'textarea', case-insensitive — the form
    builder emits 'Textarea', 'IncrementalTextarea', and lowercase 'textarea'
    for the same kind of field) hold hand-typed clinical notes and staff names.
  - Surgeon/Anaesthetist entries (type SearchableFromPrevious, matched by field
    name) hold real theatre staff names (e.g. "DR MARANYA", "DR WANJALA").
The Afya Extraction gateway's anonymize_fields cannot reach values nested
inside an array (confirmed empirically against the live API), so this can't be
fixed via a request parameter — it has to be rewritten directly in Snowflake.

orthopedic_api_to_snowflake.py now applies the same redaction to every row
BEFORE upload for newly ingested/updated OrderItemEntry records (see
_redact_orderitementries_pii in that file), so this script is a one-time
historical backfill for the rows already sitting in RAW. An initial pass on
2026-07-08 only matched type == 'Textarea' exactly and missed
IncrementalTextarea/lowercase textarea/Surgeon/Anaesthetist — this version
covers those too and is safe to re-run (already-redacted entries stay redacted).

Rewrites the table via CREATE OR REPLACE TABLE rather than a row-by-row UPDATE,
since Snowflake can't correlate a LATERAL FLATTEN sub-aggregate back into an
UPDATE...SET without a stable per-row key, which ORTHOPEDIC_RAW rows don't have.
A temp table with a materialized ROW_NUMBER() supplies that stable key for the
duration of this rewrite only, then is dropped.

USAGE
  python orthopedic_redact_orderitementries.py               # rewrites the table
  python orthopedic_redact_orderitementries.py --dry-run      # counts only, no writes
  python orthopedic_redact_orderitementries.py --refresh-clean
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

import orthopedic_api_to_snowflake as opl

log = logging.getLogger("orth_redact_oie")
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S",
    ))
    log.addHandler(_h)
    log.setLevel("INFO")
    log.propagate = False

RAW_FQN  = f"{opl.SF_DB}.{opl.SF_RAW_SCHEMA}.ORDERITEMENTRIES"
NUMBERED = f"{opl.SF_DB}.{opl.SF_RAW_SCHEMA}._OIE_NUMBERED_TMP"

# Mirrors _redact_orderitementries_pii's logic in orthopedic_api_to_snowflake.py.
_REDACT_PREDICATE = (
    "LOWER(f.value:type::VARCHAR) LIKE '%textarea%' "
    "OR LOWER(f.value:name::VARCHAR) IN ('surgeon', 'anaesthetist')"
)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="Report counts only — no writes.")
    ap.add_argument(
        "--refresh-clean", action="store_true",
        help="After redacting RAW, run orthopedic_raw_to_clean.py --full-refresh for orderitementries.",
    )
    args = ap.parse_args()

    with opl.SnowflakeClient() as sf:
        total, with_fields = sf.query(
            f"""
            SELECT COUNT(*),
                   SUM(CASE WHEN ARRAY_SIZE(payload:fields) > 0 THEN 1 ELSE 0 END)
            FROM {RAW_FQN}
            """,
            label="count_rows",
        )[0]
        log.info("ORDERITEMENTRIES: %d total rows, %d with a non-empty fields[] array", total, with_fields)

        to_redact_count = sf.query(
            f"""
            SELECT COUNT(*)
            FROM {RAW_FQN} t, LATERAL FLATTEN(input => t.payload:fields) f
            WHERE {_REDACT_PREDICATE}
            """,
            label="count_to_redact",
        )[0][0]
        already_done = sf.query(
            f"""
            SELECT COUNT(*)
            FROM {RAW_FQN} t, LATERAL FLATTEN(input => t.payload:fields) f
            WHERE ({_REDACT_PREDICATE}) AND f.value:value[0]::VARCHAR = '[REDACTED]'
            """,
            label="count_already_done",
        )[0][0]
        log.info("Entries matching redaction rules: %d (already redacted: %d)", to_redact_count, already_done)

        if args.dry_run:
            log.info("DRY-RUN — no writes performed.")
            return

        log.info("Materializing stable row keys …")
        sf.execute(
            f"""
            CREATE OR REPLACE TEMPORARY TABLE {NUMBERED} AS
            SELECT *, ROW_NUMBER() OVER (ORDER BY NULL) AS RN
            FROM {RAW_FQN};
            """,
            label="numbered:create",
        )

        log.info("Rewriting RAW.ORDERITEMENTRIES with matching values redacted …")
        sf.execute(
            f"""
            CREATE OR REPLACE TABLE {RAW_FQN} AS
            SELECT
                n._run_id,
                n._namespace,
                n._ingested_at,
                OBJECT_INSERT(n.payload, 'fields', COALESCE(rf.new_fields, n.payload:fields), TRUE) AS payload
            FROM {NUMBERED} n
            LEFT JOIN (
                SELECT
                    n2.rn,
                    ARRAY_AGG(
                        CASE WHEN {_REDACT_PREDICATE}
                             THEN OBJECT_INSERT(f.value, 'value', ARRAY_CONSTRUCT('[REDACTED]'), TRUE)
                             ELSE f.value
                        END
                    ) WITHIN GROUP (ORDER BY f.index) AS new_fields
                FROM {NUMBERED} n2, LATERAL FLATTEN(input => n2.payload:fields) f
                GROUP BY n2.rn
            ) rf ON n.rn = rf.rn;
            """,
            label="rewrite:orderitementries",
        )

        sf.execute(f"DROP TABLE IF EXISTS {NUMBERED};", label="numbered:drop")

    if args.refresh_clean:
        log.info("Rebuilding CLEAN.ORDERITEMENTRIES (+ child tables) from redacted RAW …")
        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve().parent / "orthopedic_raw_to_clean.py"),
                "--tables", "orderitementries",
                "--full-refresh",
            ],
            check=True,
        )
    else:
        log.info("RAW redacted. To rebuild CLEAN, run:")
        log.info("  python orthopedic_raw_to_clean.py --tables orderitementries --full-refresh")

    log.info("══ END ══")


if __name__ == "__main__":
    main()
