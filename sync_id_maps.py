#!/usr/bin/env python3
"""
sync_id_maps.py — Pre-flight id-map sync for the V2→V3 migration.

Reads all lookup/config tables from V2 and V3, matches them by name,
and writes V2-id → V3-id mappings into .migration_id_map.json so that
FK remapping works correctly when transactional tables are migrated.

Run this BEFORE the main migration (or between runs if new lookup tables
were seeded into V3 manually).

USAGE
  python sync_id_maps.py
  python sync_id_maps.py --facility kisumu
  python sync_id_maps.py --facility kisumu --namespace "Ignite\\Inpatient\\Entities\\AdmissionTypes"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

from v2_to_v3_api_migration import (
    FACILITY_V3_CONFIG,
    NAMESPACE_MAP,
    _fetch_available_models,
    _id_map,
    _load_id_map,
    _v3_alias,
    log,
    sync_id_map,
)

# ── Namespaces to sync ────────────────────────────────────────────────────────
# Every namespace listed here is a FK parent — its V2→V3 id map must be
# populated before any child table that references it is migrated.
# Only tables whose records carry a `name` field are matchable by sync_id_map.
SYNC_NAMESPACES: list[str] = [

    # Settings → Core (all have name fields)
    r"Ignite\Settings\Entities\Regions",
    r"Ignite\Settings\Entities\Counties",
    r"Ignite\Settings\Entities\Departments",
    r"Ignite\Settings\Entities\Clinics",
    r"Ignite\Settings\Entities\Specialties",
    r"Ignite\Settings\Entities\AgeGroups",
    r"Ignite\Settings\Entities\DocumentTypes",
    r"Ignite\Settings\Entities\DestinationTypes",
    r"Ignite\Settings\Entities\ServiceDestinations",
    r"Ignite\Settings\Entities\PurposeOfVisits",
    r"Ignite\Settings\Entities\EmployeeCategories",
    r"Ignite\Settings\Entities\CategoryFilters",
    r"Ignite\Settings\Entities\ApprovalLevels",
    r"Ignite\Settings\Entities\PartnerInstitutions",
    r"Ignite\Settings\Entities\TreatmentActions",

    # Insurance chain: company → scheme → rebate
    r"Ignite\Settings\Entities\Insurances",
    r"Ignite\Settings\Entities\Schemes",
    r"Ignite\Settings\Entities\Rebates",

    # Evaluation config
    r"Ignite\Evaluation\Entities\ProcedureCategories",
    r"Ignite\Evaluation\Entities\Procedures",
    r"Ignite\Evaluation\Entities\SampleCollectionMethods",
    r"Ignite\Evaluation\Entities\SampleTypes",
    r"Ignite\Evaluation\Entities\DiagnosisCodes",
    r"Ignite\Evaluation\Entities\Icd10Types",
    r"Ignite\Evaluation\Entities\Icd10Categories",
    r"Ignite\Evaluation\Entities\Icd10Subcategories",
    r"Ignite\Evaluation\Entities\LabTestCategories",
    r"Ignite\Evaluation\Entities\LabTestAdditives",
    r"Ignite\Evaluation\Entities\LabTestUnits",
    r"Ignite\Evaluation\Entities\EvaluationMachines",
    r"Ignite\Evaluation\Entities\PrescriptionFrequencies",
    r"Ignite\Evaluation\Entities\PrescriptionMeasures",
    r"Ignite\Evaluation\Entities\PrescriptionRoutes",
    r"Ignite\Evaluation\Entities\Formulations",

    # Finance config
    r"Ignite\Finance\Entities\Banks",
    r"Ignite\Finance\Entities\PaymentModes",
    r"Ignite\Finance\Entities\PaymentTerms",
    r"Ignite\Finance\Entities\TaxCategories",
    r"Ignite\Finance\Entities\GlAccountTypes",
    r"Ignite\Finance\Entities\GlAccountGroups",
    r"Ignite\Finance\Entities\Charges",

    # Inventory config
    r"Ignite\Inventory\Entities\Units",
    r"Ignite\Inventory\Entities\Categories",
    r"Ignite\Inventory\Entities\Suppliers",
    r"Ignite\Inventory\Entities\Stores",

    # Theatre config
    r"Ignite\Theatre\Entities\TheatreTypes",
    r"Ignite\Theatre\Entities\TheatreMedicTypes",
    r"Ignite\Theatre\Entities\TheatrePaymentTypes",
    r"Ignite\Theatre\Entities\TheatreSchedulingStatuses",

    # Inpatient config — critical for wards → beds → admissions → vitals chain
    r"Ignite\Inpatient\Entities\BedTypes",
    r"Ignite\Inpatient\Entities\AdmissionTypes",
    r"Ignite\Inpatient\Entities\DischargeTypes",
    r"Ignite\Inpatient\Entities\Wards",
]


def run_sync(facilities: list[str], namespaces: list[str]) -> None:
    _load_id_map()

    log.info("Fetching V3 gateway metadata to resolve aliases…")
    _fetch_available_models()

    rows: list[tuple[str, str, str, int, int, str]] = []  # facility, alias, ns, before, after, status

    for facility in facilities:
        log.info("")
        log.info("── %s ─────────────────────────────────────────", facility.upper())

        for ns in namespaces:
            if ns not in NAMESPACE_MAP:
                log.debug("  %s not in NAMESPACE_MAP — skipping", ns)
                continue

            v3_ns = NAMESPACE_MAP[ns]["v3"]
            alias = _v3_alias(v3_ns)
            before = len(_id_map.get(alias, {}))

            try:
                n = sync_id_map(alias, ns, facility)
                after = len(_id_map.get(alias, {}))
                status = "ok"
                label = f"+{n} new" if n else "no new matches"
                log.info("  ✓  %-45s  %-30s  %s  (map size: %d)", alias, ns.split("\\")[-1], label, after)
            except Exception as e:
                after = before
                status = "error"
                n = 0
                log.warning("  ✗  %-45s  FAILED: %s", alias, e)

            rows.append((facility, alias, ns.split("\\")[-1], before, after, status))

    # ── Summary table ─────────────────────────────────────────────────────────
    log.info("")
    log.info("══════════════════════  SYNC SUMMARY  ══════════════════════")

    total_new     = sum(r[4] - r[3] for r in rows)
    total_entries = sum(len(v) for v in _id_map.values())
    errors        = [r for r in rows if r[5] == "error"]
    empty         = [r for r in rows if r[4] == 0 and r[5] == "ok"]

    log.info("  New mappings recorded this run : %d", total_new)
    log.info("  Total entries in id map        : %d", total_entries)
    log.info("  Failed syncs                   : %d", len(errors))
    log.info("  Tables with 0 matches (check)  : %d", len(empty))

    if errors:
        log.warning("")
        log.warning("  ERRORS:")
        for r in errors:
            log.warning("    ✗  [%s] %s", r[0], r[1])

    if empty:
        log.info("")
        log.info("  ZERO MATCHES (V2/V3 may not share name values, or table not yet seeded):")
        for r in empty:
            log.info("    ·  [%s] %s  (%s)", r[0], r[1], r[2])

    log.info("═════════════════════════════════════════════════════════════")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync V2→V3 id maps for all lookup/config tables.",
    )
    parser.add_argument(
        "--facility", "-f",
        nargs="+",
        metavar="NAME",
        help="Facilities to sync (default: all with organization_id configured)",
    )
    parser.add_argument(
        "--namespace", "-n",
        nargs="+",
        metavar="NS",
        help="Limit sync to specific V2 namespaces (default: all in SYNC_NAMESPACES)",
    )
    args = parser.parse_args()

    facilities = args.facility or [
        f for f, cfg in FACILITY_V3_CONFIG.items()
        if cfg.get("organization_id") is not None
    ]
    if not facilities:
        print("No facilities have organization_id configured in FACILITY_V3_CONFIG. "
              "Add them before running.", file=sys.stderr)
        sys.exit(1)

    namespaces = args.namespace or SYNC_NAMESPACES

    log.info("Syncing id maps for facilities: %s", ", ".join(facilities))
    log.info("Namespaces to sync: %d", len(namespaces))

    run_sync(facilities, namespaces)


if __name__ == "__main__":
    main()
