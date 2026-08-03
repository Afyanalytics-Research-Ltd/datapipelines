#!/usr/bin/env python3
"""
orthopedic_dedup_singleorderitems.py — Deduplicate HOSPITALS.ORTHOPEDIC_CLEAN.SINGLEORDERITEMS (Issue 28).

RAW/CLEAN are append-only: every pipeline run re-uploads any record the source
API reports as updated, so the same _id can appear multiple times across
ingestion runs. For SINGLEORDERITEMS this reached 642,043 duplicate ids
(1,349,586 rows / 707,543 distinct ids as of 2026-07-08), which fans out every
downstream join keyed on id.

This rebuilds CLEAN.SINGLEORDERITEMS as the single-row-per-id table consumers
expect (latest snapshot by _ingested_at wins). RAW.SINGLEORDERITEMS keeps its
full historical record untouched for audit purposes — only CLEAN is deduped.

USAGE
  python orthopedic_dedup_singleorderitems.py
  python orthopedic_dedup_singleorderitems.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys

import orthopedic_api_to_snowflake as opl

log = logging.getLogger("orth_dedup_soi")
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S",
    ))
    log.addHandler(_h)
    log.setLevel("INFO")
    log.propagate = False

CLEAN_FQN = f"{opl.SF_DB}.ORTHOPEDIC_CLEAN.SINGLEORDERITEMS"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="Report counts only — no writes.")
    args = ap.parse_args()

    with opl.SnowflakeClient() as sf:
        total, distinct_ids = sf.query(
            f'SELECT COUNT(*), COUNT(DISTINCT "id") FROM {CLEAN_FQN}', label="count_before",
        )[0]
        log.info("Before: %d rows, %d distinct ids, %d duplicate rows", total, distinct_ids, total - distinct_ids)

        if args.dry_run:
            log.info("DRY-RUN — no writes performed.")
            return

        sf.execute(
            f"""
            CREATE OR REPLACE TABLE {CLEAN_FQN} AS
            SELECT * FROM {CLEAN_FQN}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY _ingested_at DESC) = 1;
            """,
            label="dedup:singleorderitems",
        )

        total_after, distinct_after = sf.query(
            f'SELECT COUNT(*), COUNT(DISTINCT "id") FROM {CLEAN_FQN}', label="count_after",
        )[0]
        log.info("After: %d rows, %d distinct ids", total_after, distinct_after)

    log.info("══ END ══")


if __name__ == "__main__":
    main()
