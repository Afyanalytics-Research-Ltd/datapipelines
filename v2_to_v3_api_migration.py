#!/usr/bin/env python3
"""
v2_to_v3_api_migration.py — standalone V2 → V3 API-to-API migration pipeline.

For each V2 facility:
  1. Authenticates with the V2 facility API.
  2. Paginates through namespace data via /api/finance/access/data/point.
  3. Applies V2→V3 field transforms (renames, boolean prefixes, type coercions,
     organization_id injection).
  4. POSTs transformed records + IDs to the V3 Afya API in batches.

No Airflow, no S3, no Snowflake — pure API-to-API.

USAGE
  python v2_to_v3_api_migration.py
  python v2_to_v3_api_migration.py --facility kisumu
  python v2_to_v3_api_migration.py --facility kisumu --namespace "Ignite\\Finance\\Entities\\Invoice"
  python v2_to_v3_api_migration.py --since 2025-01-01T00:00:00Z
  python v2_to_v3_api_migration.py --dry-run
  python v2_to_v3_api_migration.py --workers 4 --batch-size 100

ENV VARS  (put them in a .env file next to this script)
  # V2 facility credentials  (one pair per facility)
  FACILITY_KAKAMEGA_USERNAME=...   FACILITY_KAKAMEGA_PASSWORD=...
  FACILITY_KISUMU_USERNAME=...     FACILITY_KISUMU_PASSWORD=...
  FACILITY_LODWAR_USERNAME=...     FACILITY_LODWAR_PASSWORD=...
  FACILITY_TENRI_USERNAME=...      FACILITY_TENRI_PASSWORD=...
  FACILITY_XANALIFE_USERNAME=...   FACILITY_XANALIFE_PASSWORD=...
  FACILITY_AFYA_API_AUTH_USERNAME=... FACILITY_AFYA_API_AUTH_PASSWORD=...

  # V3 destination credentials
  AFYA_USERNAME=...
  AFYA_PASSWORD=...

  # Tuning
  PIPELINE_WORKERS=8    # parallel (facility, namespace) jobs
  PAGE_WORKERS=4        # parallel pages within a single job
  TOKEN_TTL_SECONDS=3000
  LOG_LEVEL=INFO
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import requests.adapters
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, HTTPError, Timeout

# Optional fast JSON encoder
try:
    import orjson
    def _dumps(obj) -> str:
        return orjson.dumps(obj).decode()
except ImportError:
    def _dumps(obj) -> str:
        return json.dumps(obj, separators=(",", ":"))

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

# ─── LOGGING ─────────────────────────────────────────────────────────────────


log = logging.getLogger("v2_to_v3_migration")
if not log.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s",
        datefmt="%H:%M:%S",
    ))
    log.addHandler(h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ─── CONFIG ──────────────────────────────────────────────────────────────────

PIPELINE_WORKERS   = int(os.getenv("PIPELINE_WORKERS", "8"))
PAGE_WORKERS       = int(os.getenv("PAGE_WORKERS", "4"))
RECORD_WORKERS     = int(os.getenv("RECORD_WORKERS", "3"))    # parallel POSTs per job — keep low to avoid 504s
RECORD_LOG_EVERY   = int(os.getenv("RECORD_LOG_EVERY", "100")) # log progress every N records
RECORD_FLUSH_EVERY = int(os.getenv("RECORD_FLUSH_EVERY", "50")) # flush progress file every N records
TOKEN_TTL_SECONDS  = int(os.getenv("TOKEN_TTL_SECONDS", str(50 * 60)))
DEFAULT_BATCH_SIZE = 200
DEFAULT_LIMIT      = 500
V3_POST_THROTTLE   = float(os.getenv("V3_POST_THROTTLE", "0"))   # seconds to sleep after each V3 POST
V3_RETRY_WAIT      = int(os.getenv("V3_RETRY_WAIT", "30"))        # initial wait (s) before retrying 5xx

WATERMARK_FILE       = Path(__file__).resolve().parent / ".migration_watermarks.json"
PROGRESS_FILE        = Path(__file__).resolve().parent / ".migration_progress.json"
RECORD_PROGRESS_FILE = Path(__file__).resolve().parent / ".migration_record_progress.json"
ID_MAP_FILE          = Path(__file__).resolve().parent / ".migration_id_map.json"

# V2 source facilities
V2_FACILITIES: dict[str, dict] = {
    "afya_api_auth": {"base_url": "https://staging.afyanalytics.ai",    "db": "staging_db"},
    "kakamega":      {"base_url": "https://demo.collabmed.net",          "db": "kakamega_db"},
    "kisumu":        {"base_url": "https://kshospital.collabmed.net",    "db": "kisumu_db"},
    "lodwar":        {"base_url": "https://lcrh.collabmed.net",          "db": "lodwar_db"},
    "tenri":         {"base_url": "https://stageenv.collabmed.net",      "db": "tenri_db"},
    "xanalife":      {"base_url": "https://xanalife.afyanalytics.ai/",   "db": "xanalife_db"},
}

# V3 service base URLs — each service has its own gateway
V3_SERVICES: dict[str, str] = {
    "core":       "https://core.afyaanalytics.ai/api/",
    "finance":    "https://finance.afyaanalytics.ai/api/",
    "evaluation": "https://evaluation.afyaanalytics.ai/api/",
    "reception":  "https://reception.afyaanalytics.ai/api/",
    "inventory":  "https://inventory.afyaanalytics.ai/api/",
    "theatre":    "https://theatre.afyaanalytics.ai/api/",
    "inpatient":  "https://inpatient.afyaanalytics.ai/api/",
}
# Runtime dict built in run_migration: alias → service name
_alias_to_service: dict[str, str] = {}
# ─── FACILITY → V3 ORG MAPPING ──────────────────────────────────────────────
# Fill in organization_id and facility_id from the V3 core_organizations /
# core_facilities tables before running. application_id is typically 1.
FACILITY_V3_CONFIG: dict[str, dict] = {
    "afya_api_auth": {"organization_id": None, "facility_id": None, "application_id": 1},
    "kakamega":      {"organization_id": None, "facility_id": None, "application_id": 1},
    "kisumu":        {"organization_id": 1, "facility_id": 6, "application_id": 1},
    "lodwar":        {"organization_id": None, "facility_id": None, "application_id": 1},
    "tenri":         {"organization_id": None, "facility_id": None, "application_id": 1},
    "xanalife":      {"organization_id": None, "facility_id": None, "application_id": 1},
}

# ─── NAMESPACE MAP ────────────────────────────────────────────────────────────
# Maps V2 namespace → {"v3": V3 namespace, "transform": transform key}.
# V2 namespace convention: Ignite\{Module}\Entities\{Model}
# V3 namespace convention: App\Models\{Model}
# The fallback chain (singular / double-namespace) is tried at extraction time,
# so register the canonical plural form here.
NAMESPACE_MAP: dict[str, dict] = {
    # Finance
    r"Ignite\Finance\Entities\Invoices":             {"v3": r"App\Models\Invoice",              "transform": "finance_invoice"},
    r"Ignite\Finance\Entities\Invoice":              {"v3": r"App\Models\Invoice",              "transform": "finance_invoice"},
    r"Ignite\Finance\Entities\Waivers":              {"v3": r"App\Models\Waiver",               "transform": "generic"},
    r"Ignite\Finance\Entities\Waiver":               {"v3": r"App\Models\Waiver",               "transform": "generic"},
    r"Ignite\Finance\Entities\Copays":               {"v3": r"App\Models\Copay",                "transform": "finance_copay"},
    r"Ignite\Finance\Entities\Copay":                {"v3": r"App\Models\Copay",                "transform": "finance_copay"},
    r"Ignite\Finance\Entities\PatientDeposits":      {"v3": r"App\Models\PatientDeposit",       "transform": "generic"},
    r"Ignite\Finance\Entities\PatientDeposit":       {"v3": r"App\Models\PatientDeposit",       "transform": "generic"},
    r"Ignite\Finance\Entities\PatientWithdrawals":   {"v3": r"App\Models\PatientWithdrawal",    "transform": "generic"},
    r"Ignite\Finance\Entities\PatientWithdrawal":    {"v3": r"App\Models\PatientWithdrawal",    "transform": "generic"},
    r"Ignite\Finance\Entities\EvaluationPayments":   {"v3": r"App\Models\EvaluationPayment",    "transform": "finance_eval_payment"},
    r"Ignite\Finance\Entities\EvaluationPayment":    {"v3": r"App\Models\EvaluationPayment",    "transform": "finance_eval_payment"},
    r"Ignite\Finance\Entities\InvoicePayments":      {"v3": r"App\Models\InvoicePayment",       "transform": "generic"},
    r"Ignite\Finance\Entities\InvoicePayment":       {"v3": r"App\Models\InvoicePayment",       "transform": "generic"},
    r"Ignite\Finance\Entities\PettyCash":            {"v3": r"App\Models\PettyCash",            "transform": "generic"},
    r"Ignite\Finance\Entities\Banks":                {"v3": r"App\Models\Bank",                 "transform": "generic"},
    r"Ignite\Finance\Entities\Bank":                 {"v3": r"App\Models\Bank",                 "transform": "generic"},
    r"Ignite\Finance\Entities\Vouchers":             {"v3": r"App\Models\Voucher",              "transform": "finance_voucher"},
    r"Ignite\Finance\Entities\Voucher":              {"v3": r"App\Models\Voucher",              "transform": "finance_voucher"},
    r"Ignite\Finance\Entities\PatientAccounts":      {"v3": r"App\Models\PatientAccount",       "transform": "generic"},
    r"Ignite\Finance\Entities\PatientAccount":       {"v3": r"App\Models\PatientAccount",       "transform": "generic"},
    r"Ignite\Finance\Entities\Dispatches":           {"v3": r"App\Models\Dispatch",             "transform": "generic"},
    r"Ignite\Finance\Entities\Dispatch":             {"v3": r"App\Models\Dispatch",             "transform": "generic"},

    # Reception — insertable models first, then read-only (no insert in V3 gateway)
    r"Ignite\Reception\Entities\Customers":          {"v3": r"App\Models\Customer",             "transform": "generic"},
    r"Ignite\Reception\Entities\Customer":           {"v3": r"App\Models\Customer",             "transform": "generic"},
    # read-only in V3 reception gateway (no insert op) — kept for completeness / future enablement
    r"Ignite\Reception\Entities\Patients":           {"v3": r"App\Models\Patient",              "transform": "reception_patient"},
    r"Ignite\Reception\Entities\Patient":            {"v3": r"App\Models\Patient",              "transform": "reception_patient"},
    r"Ignite\Reception\Entities\Visits":             {"v3": r"App\Models\Visit",                "transform": "reception_visit"},
    r"Ignite\Reception\Entities\Visit":              {"v3": r"App\Models\Visit",                "transform": "reception_visit"},
    r"Ignite\Reception\Entities\Appointments":       {"v3": r"App\Models\Appointment",          "transform": "reception_appointment"},
    r"Ignite\Reception\Entities\Appointment":        {"v3": r"App\Models\Appointment",          "transform": "reception_appointment"},
    r"Ignite\Reception\Entities\PatientSchemes":     {"v3": r"App\Models\PatientInsurance",     "transform": "reception_patient_scheme"},
    r"Ignite\Reception\Entities\PatientScheme":      {"v3": r"App\Models\PatientInsurance",     "transform": "reception_patient_scheme"},
    r"Ignite\Reception\Entities\PatientNextOfKins":  {"v3": r"App\Models\PatientNextOfKin",     "transform": "generic"},
    r"Ignite\Reception\Entities\PatientNextOfKin":   {"v3": r"App\Models\PatientNextOfKin",     "transform": "generic"},
    r"Ignite\Reception\Entities\PatientDocuments":   {"v3": r"App\Models\PatientDocument",      "transform": "generic"},
    r"Ignite\Reception\Entities\PatientDocument":    {"v3": r"App\Models\PatientDocument",      "transform": "generic"},
    r"Ignite\Reception\Entities\PatientDependants":  {"v3": r"App\Models\PatientDependant",     "transform": "generic"},
    r"Ignite\Reception\Entities\PatientDependant":   {"v3": r"App\Models\PatientDependant",     "transform": "generic"},
    r"Ignite\Reception\Entities\PatientGuarantors":  {"v3": r"App\Models\PatientGuarantor",     "transform": "generic"},
    r"Ignite\Reception\Entities\PatientGuarantor":   {"v3": r"App\Models\PatientGuarantor",     "transform": "generic"},
    r"Ignite\Reception\Entities\PatientFollowups":   {"v3": r"App\Models\PatientFollowup",      "transform": "generic"},
    r"Ignite\Reception\Entities\PatientFollowup":    {"v3": r"App\Models\PatientFollowup",      "transform": "generic"},
    r"Ignite\Reception\Entities\PatientSamples":     {"v3": r"App\Models\PatientSample",        "transform": "generic"},
    r"Ignite\Reception\Entities\PatientSample":      {"v3": r"App\Models\PatientSample",        "transform": "generic"},
    r"Ignite\Reception\Entities\PatientRandomNotes": {"v3": r"App\Models\PatientRandomNote",    "transform": "generic"},
    r"Ignite\Reception\Entities\PatientRandomNote":  {"v3": r"App\Models\PatientRandomNote",    "transform": "generic"},
    r"Ignite\Reception\Entities\PatientConsents":    {"v3": r"App\Models\PatientConsent",       "transform": "generic"},
    r"Ignite\Reception\Entities\PatientConsent":     {"v3": r"App\Models\PatientConsent",       "transform": "generic"},
    r"Ignite\Reception\Entities\MorgueAdmissions":   {"v3": r"App\Models\MorgueAdmission",      "transform": "generic"},
    r"Ignite\Reception\Entities\MorgueAdmission":    {"v3": r"App\Models\MorgueAdmission",      "transform": "generic"},
    r"Ignite\Reception\Entities\VisitDestinations":  {"v3": r"App\Models\VisitDestination",     "transform": "generic"},
    r"Ignite\Reception\Entities\VisitDestination":   {"v3": r"App\Models\VisitDestination",     "transform": "generic"},
    r"Ignite\Reception\Entities\VisitConsultants":   {"v3": r"App\Models\VisitConsultant",      "transform": "generic"},
    r"Ignite\Reception\Entities\VisitConsultant":    {"v3": r"App\Models\VisitConsultant",      "transform": "generic"},
    r"Ignite\Reception\Entities\VisitPrecharges":    {"v3": r"App\Models\VisitPrecharge",       "transform": "generic"},
    r"Ignite\Reception\Entities\VisitPrecharge":     {"v3": r"App\Models\VisitPrecharge",       "transform": "generic"},
    r"Ignite\Reception\Entities\Queues":             {"v3": r"App\Models\Queue",                "transform": "generic"},
    r"Ignite\Reception\Entities\Queue":              {"v3": r"App\Models\Queue",                "transform": "generic"},
    r"Ignite\Reception\Entities\Referrals":          {"v3": r"App\Models\Referral",             "transform": "generic"},
    r"Ignite\Reception\Entities\Referral":           {"v3": r"App\Models\Referral",             "transform": "generic"},

    # Evaluation
    r"Ignite\Evaluation\Entities\DoctorNotes":       {"v3": r"App\Models\DoctorNote",           "transform": "evaluation_doctor_note"},
    r"Ignite\Evaluation\Entities\DoctorNote":        {"v3": r"App\Models\DoctorNote",           "transform": "evaluation_doctor_note"},
    r"Ignite\Evaluation\Entities\Vitals":            {"v3": r"App\Models\Vital",                "transform": "generic"},
    r"Ignite\Evaluation\Entities\Vital":             {"v3": r"App\Models\Vital",                "transform": "generic"},
    r"Ignite\Evaluation\Entities\Prescriptions":     {"v3": r"App\Models\Prescription",         "transform": "generic"},
    r"Ignite\Evaluation\Entities\Prescription":      {"v3": r"App\Models\Prescription",         "transform": "generic"},
    r"Ignite\Evaluation\Entities\InvestigationResults": {"v3": r"App\Models\InvestigationResult", "transform": "evaluation_inv_result"},
    r"Ignite\Evaluation\Entities\InvestigationResult":  {"v3": r"App\Models\InvestigationResult", "transform": "evaluation_inv_result"},
    r"Ignite\Evaluation\Entities\ExaminationReviews":{"v3": r"App\Models\ExaminationReview",    "transform": "generic"},
    r"Ignite\Evaluation\Entities\ExaminationReview": {"v3": r"App\Models\ExaminationReview",    "transform": "generic"},
    r"Ignite\Evaluation\Entities\EyeExams":          {"v3": r"App\Models\EyeExam",              "transform": "evaluation_eye_exam"},
    r"Ignite\Evaluation\Entities\EyeExam":           {"v3": r"App\Models\EyeExam",              "transform": "evaluation_eye_exam"},

    # Inventory — lookup tables FIRST (products depend on units + categories)
    r"Ignite\Inventory\Entities\Units":              {"v3": r"App\Models\Unit",                 "transform": "generic"},
    r"Ignite\Inventory\Entities\Unit":               {"v3": r"App\Models\Unit",                 "transform": "generic"},
    r"Ignite\Inventory\Entities\Categories":         {"v3": r"App\Models\ProductCategory",      "transform": "inventory_category"},
    r"Ignite\Inventory\Entities\Category":           {"v3": r"App\Models\ProductCategory",      "transform": "inventory_category"},
    r"Ignite\Inventory\Entities\Suppliers":          {"v3": r"App\Models\Supplier",             "transform": "generic"},
    r"Ignite\Inventory\Entities\Supplier":           {"v3": r"App\Models\Supplier",             "transform": "generic"},
    r"Ignite\Inventory\Entities\Stores":             {"v3": r"App\Models\Store",                "transform": "generic"},
    r"Ignite\Inventory\Entities\Store":              {"v3": r"App\Models\Store",                "transform": "generic"},
    # Products depend on units + categories — run after lookup tables above
    r"Ignite\Inventory\Entities\Products":           {"v3": r"App\Models\Product",              "transform": "inventory_product"},
    r"Ignite\Inventory\Entities\Product":            {"v3": r"App\Models\Product",              "transform": "inventory_product"},
    r"Ignite\Inventory\Entities\BatchPurchases":     {"v3": r"App\Models\Batch",                "transform": "inventory_batch"},
    r"Ignite\Inventory\Entities\BatchPurchase":      {"v3": r"App\Models\Batch",                "transform": "inventory_batch"},
    r"Ignite\Inventory\Entities\PurchaseOrders":     {"v3": r"App\Models\PurchaseOrder",        "transform": "inventory_purchase_order"},
    r"Ignite\Inventory\Entities\PurchaseOrder":      {"v3": r"App\Models\PurchaseOrder",        "transform": "inventory_purchase_order"},
    r"Ignite\Inventory\Entities\Requisitions":       {"v3": r"App\Models\Requisition",          "transform": "inventory_requisition"},
    r"Ignite\Inventory\Entities\Requisition":        {"v3": r"App\Models\Requisition",          "transform": "inventory_requisition"},
    r"Ignite\Inventory\Entities\GoodsReceived":      {"v3": r"App\Models\GoodsReceivedNote",    "transform": "inventory_grn"},

    # Settings → Core
    r"Ignite\Settings\Entities\Clinics":             {"v3": r"App\Models\Clinic",               "transform": "generic"},
    r"Ignite\Settings\Entities\Clinic":              {"v3": r"App\Models\Clinic",               "transform": "generic"},
    r"Ignite\Settings\Entities\Insurances":          {"v3": r"App\Models\InsuranceCompany",     "transform": "settings_insurance"},
    r"Ignite\Settings\Entities\Insurance":           {"v3": r"App\Models\InsuranceCompany",     "transform": "settings_insurance"},
    r"Ignite\Settings\Entities\Schemes":             {"v3": r"App\Models\InsuranceScheme",      "transform": "settings_scheme"},
    r"Ignite\Settings\Entities\Scheme":              {"v3": r"App\Models\InsuranceScheme",      "transform": "settings_scheme"},
    r"Ignite\Settings\Entities\Departments":         {"v3": r"App\Models\Department",           "transform": "generic"},
    r"Ignite\Settings\Entities\Department":          {"v3": r"App\Models\Department",           "transform": "generic"},
    r"Ignite\Settings\Entities\Regions":             {"v3": r"App\Models\Region",               "transform": "generic"},
    r"Ignite\Settings\Entities\Region":              {"v3": r"App\Models\Region",               "transform": "generic"},
    r"Ignite\Settings\Entities\Counties":            {"v3": r"App\Models\County",               "transform": "generic"},
    r"Ignite\Settings\Entities\County":              {"v3": r"App\Models\County",               "transform": "generic"},
    r"Ignite\Settings\Entities\Rebates":             {"v3": r"App\Models\Rebate",               "transform": "settings_rebate"},
    r"Ignite\Settings\Entities\Rebate":              {"v3": r"App\Models\Rebate",               "transform": "settings_rebate"},

    # Evaluation → Core (lookup/config tables)
    r"Ignite\Evaluation\Entities\ProcedureCategories":        {"v3": r"App\Models\ProcedureCategory",       "transform": "eval_procedure_category"},
    r"Ignite\Evaluation\Entities\ProcedureCategory":          {"v3": r"App\Models\ProcedureCategory",       "transform": "eval_procedure_category"},
    # V2 uses "Procedures" (not "EvaluationProcedures") — keep canonical names first so dedup
    # claims the right slot; EvaluationProcedures variants are fallback only
    r"Ignite\Evaluation\Entities\Procedures":                 {"v3": r"App\Models\Procedure",               "transform": "eval_procedure"},
    r"Ignite\Evaluation\Entities\Procedure":                  {"v3": r"App\Models\Procedure",               "transform": "eval_procedure"},
    r"Ignite\Evaluation\Entities\SampleTypes":                {"v3": r"App\Models\SampleType",              "transform": "eval_sample_type"},
    r"Ignite\Evaluation\Entities\SampleType":                 {"v3": r"App\Models\SampleType",              "transform": "eval_sample_type"},
    r"Ignite\Evaluation\Entities\SampleCollectionMethods":    {"v3": r"App\Models\SampleCollectionMethod",  "transform": "generic"},
    r"Ignite\Evaluation\Entities\SampleCollectionMethod":     {"v3": r"App\Models\SampleCollectionMethod",  "transform": "generic"},
    # TreatmentActions: try Settings module first (V2 may store these there)
    r"Ignite\Settings\Entities\TreatmentActions":             {"v3": r"App\Models\TreatmentAction",         "transform": "generic"},
    r"Ignite\Settings\Entities\TreatmentAction":              {"v3": r"App\Models\TreatmentAction",         "transform": "generic"},

    # Users → Core (Specialties: also try Settings module as V2 fallback)
    r"Ignite\Settings\Entities\Specialties":         {"v3": r"App\Models\Specialty",            "transform": "generic"},
    r"Ignite\Settings\Entities\Specialty":           {"v3": r"App\Models\Specialty",            "transform": "generic"},
    r"Ignite\Users\Entities\Users":                  {"v3": r"App\Models\User",                 "transform": "settings_user"},
    r"Ignite\Users\Entities\User":                   {"v3": r"App\Models\User",                 "transform": "settings_user"},

    # Theatre — lookup tables first, then transactional
    r"Ignite\Theatre\Entities\TheatreTypes":             {"v3": r"App\Models\TheatreType",           "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatreType":              {"v3": r"App\Models\TheatreType",           "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatreMedicTypes":        {"v3": r"App\Models\TheatreMedicType",      "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatreMedicType":         {"v3": r"App\Models\TheatreMedicType",      "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatrePaymentTypes":      {"v3": r"App\Models\TheatrePaymentType",    "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatrePaymentType":       {"v3": r"App\Models\TheatrePaymentType",    "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatreSchedulingStatuses": {"v3": r"App\Models\TheatreSchedulingStatus", "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatreSchedulingStatus":  {"v3": r"App\Models\TheatreSchedulingStatus", "transform": "generic"},
    r"Ignite\Theatre\Entities\Theatres":                 {"v3": r"App\Models\Theatre",              "transform": "theatre_theatre"},
    r"Ignite\Theatre\Entities\Theatre":                  {"v3": r"App\Models\Theatre",              "transform": "theatre_theatre"},
    r"Ignite\Theatre\Entities\TheatreBookings":          {"v3": r"App\Models\TheatreBooking",       "transform": "theatre_booking"},
    r"Ignite\Theatre\Entities\TheatreBooking":           {"v3": r"App\Models\TheatreBooking",       "transform": "theatre_booking"},
    r"Ignite\Theatre\Entities\TheatreOperations":        {"v3": r"App\Models\TheatreOperation",     "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatreOperation":         {"v3": r"App\Models\TheatreOperation",     "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatreSchedules":         {"v3": r"App\Models\TheatreSchedule",      "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatreSchedule":          {"v3": r"App\Models\TheatreSchedule",      "transform": "generic"},

    # Inpatient — ward/bed lookup tables, then transactional
    r"Ignite\Inpatient\Entities\AdmissionTypes":         {"v3": r"App\Models\AdmissionType",        "transform": "generic"},
    r"Ignite\Inpatient\Entities\AdmissionType":          {"v3": r"App\Models\AdmissionType",        "transform": "generic"},
    r"Ignite\Inpatient\Entities\DischargeTypes":         {"v3": r"App\Models\DischargeType",        "transform": "generic"},
    r"Ignite\Inpatient\Entities\DischargeType":          {"v3": r"App\Models\DischargeType",        "transform": "generic"},
    r"Ignite\Inpatient\Entities\BedTypes":               {"v3": r"App\Models\BedType",              "transform": "generic"},
    r"Ignite\Inpatient\Entities\BedType":                {"v3": r"App\Models\BedType",              "transform": "generic"},
    r"Ignite\Inpatient\Entities\Beds":                   {"v3": r"App\Models\Bed",                  "transform": "generic"},
    r"Ignite\Inpatient\Entities\Bed":                    {"v3": r"App\Models\Bed",                  "transform": "generic"},
    r"Ignite\Inpatient\Entities\Wards":                  {"v3": r"App\Models\Ward",                 "transform": "generic"},
    r"Ignite\Inpatient\Entities\Ward":                   {"v3": r"App\Models\Ward",                 "transform": "generic"},
    r"Ignite\Inpatient\Entities\WardCharges":            {"v3": r"App\Models\WardCharge",           "transform": "generic"},
    r"Ignite\Inpatient\Entities\WardCharge":             {"v3": r"App\Models\WardCharge",           "transform": "generic"},

    # Settings → Core (additional lookup tables from gateway list)
    r"Ignite\Settings\Entities\ServiceDestinations":     {"v3": r"App\Models\ServiceDestination",   "transform": "generic"},
    r"Ignite\Settings\Entities\ServiceDestination":      {"v3": r"App\Models\ServiceDestination",   "transform": "generic"},
    r"Ignite\Settings\Entities\DestinationTypes":        {"v3": r"App\Models\DestinationType",      "transform": "generic"},
    r"Ignite\Settings\Entities\DestinationType":         {"v3": r"App\Models\DestinationType",      "transform": "generic"},
    r"Ignite\Settings\Entities\PurposeOfVisits":         {"v3": r"App\Models\PurposeOfVisit",       "transform": "generic"},
    r"Ignite\Settings\Entities\PurposeOfVisit":          {"v3": r"App\Models\PurposeOfVisit",       "transform": "generic"},
    r"Ignite\Settings\Entities\PartnerInstitutions":     {"v3": r"App\Models\PartnerInstitution",   "transform": "generic"},
    r"Ignite\Settings\Entities\PartnerInstitution":      {"v3": r"App\Models\PartnerInstitution",   "transform": "generic"},
    r"Ignite\Settings\Entities\PartnerStaff":            {"v3": r"App\Models\PartnerStaff",         "transform": "generic"},
    r"Ignite\Settings\Entities\EmployeeCategories":      {"v3": r"App\Models\EmployeeCategory",     "transform": "generic"},
    r"Ignite\Settings\Entities\EmployeeCategory":        {"v3": r"App\Models\EmployeeCategory",     "transform": "generic"},
    r"Ignite\Settings\Entities\CategoryFilters":         {"v3": r"App\Models\CategoryFilter",       "transform": "generic"},
    r"Ignite\Settings\Entities\CategoryFilter":          {"v3": r"App\Models\CategoryFilter",       "transform": "generic"},
    r"Ignite\Settings\Entities\DocumentTypes":           {"v3": r"App\Models\DocumentType",         "transform": "generic"},
    r"Ignite\Settings\Entities\DocumentType":            {"v3": r"App\Models\DocumentType",         "transform": "generic"},
    r"Ignite\Settings\Entities\Themes":                  {"v3": r"App\Models\Theme",                "transform": "generic"},
    r"Ignite\Settings\Entities\Theme":                   {"v3": r"App\Models\Theme",                "transform": "generic"},
    r"Ignite\Settings\Entities\AgeGroups":               {"v3": r"App\Models\AgeGroup",             "transform": "generic"},
    r"Ignite\Settings\Entities\AgeGroup":                {"v3": r"App\Models\AgeGroup",             "transform": "generic"},
    r"Ignite\Settings\Entities\ApprovalLevels":          {"v3": r"App\Models\ApprovalLevel",        "transform": "generic"},
    r"Ignite\Settings\Entities\ApprovalLevel":           {"v3": r"App\Models\ApprovalLevel",        "transform": "generic"},

    # Finance → Finance service (additional lookup tables)
    r"Ignite\Finance\Entities\PaymentModes":             {"v3": r"App\Models\PaymentMode",          "transform": "generic"},
    r"Ignite\Finance\Entities\PaymentMode":              {"v3": r"App\Models\PaymentMode",          "transform": "generic"},
    r"Ignite\Finance\Entities\PaymentTerms":             {"v3": r"App\Models\PaymentTerm",          "transform": "generic"},
    r"Ignite\Finance\Entities\PaymentTerm":              {"v3": r"App\Models\PaymentTerm",          "transform": "generic"},
    r"Ignite\Finance\Entities\TaxCategories":            {"v3": r"App\Models\TaxCategory",          "transform": "generic"},
    r"Ignite\Finance\Entities\TaxCategory":              {"v3": r"App\Models\TaxCategory",          "transform": "generic"},
    r"Ignite\Finance\Entities\GlAccountGroups":          {"v3": r"App\Models\GlAccountGroup",       "transform": "generic"},
    r"Ignite\Finance\Entities\GlAccountGroup":           {"v3": r"App\Models\GlAccountGroup",       "transform": "generic"},
    r"Ignite\Finance\Entities\GlAccountTypes":           {"v3": r"App\Models\GlAccountType",        "transform": "generic"},
    r"Ignite\Finance\Entities\GlAccountType":            {"v3": r"App\Models\GlAccountType",        "transform": "generic"},
    r"Ignite\Finance\Entities\Charges":                  {"v3": r"App\Models\Charge",               "transform": "generic"},
    r"Ignite\Finance\Entities\Charge":                   {"v3": r"App\Models\Charge",               "transform": "generic"},
    # Customer lives in reception service in V3 — V2 namespace may be Reception or Finance

    # Evaluation → Evaluation service (additional lookup/reference tables)
    r"Ignite\Evaluation\Entities\DiagnosisCodes":        {"v3": r"App\Models\DiagnosisCode",        "transform": "generic"},
    r"Ignite\Evaluation\Entities\DiagnosisCode":         {"v3": r"App\Models\DiagnosisCode",        "transform": "generic"},
    r"Ignite\Evaluation\Entities\CriticalValues":        {"v3": r"App\Models\CriticalValue",        "transform": "generic"},
    r"Ignite\Evaluation\Entities\CriticalValue":         {"v3": r"App\Models\CriticalValue",        "transform": "generic"},
    r"Ignite\Evaluation\Entities\Icd10Types":            {"v3": r"App\Models\Icd10Type",            "transform": "generic"},
    r"Ignite\Evaluation\Entities\Icd10Type":             {"v3": r"App\Models\Icd10Type",            "transform": "generic"},
    r"Ignite\Evaluation\Entities\Icd10Categories":       {"v3": r"App\Models\Icd10Category",        "transform": "generic"},
    r"Ignite\Evaluation\Entities\Icd10Category":         {"v3": r"App\Models\Icd10Category",        "transform": "generic"},
    r"Ignite\Evaluation\Entities\Icd10Subcategories":    {"v3": r"App\Models\Icd10Subcategory",     "transform": "generic"},
    r"Ignite\Evaluation\Entities\Icd10Subcategory":      {"v3": r"App\Models\Icd10Subcategory",     "transform": "generic"},
    r"Ignite\Evaluation\Entities\BioReferenceRanges":    {"v3": r"App\Models\BioReferenceRange",    "transform": "generic"},
    r"Ignite\Evaluation\Entities\BioReferenceRange":     {"v3": r"App\Models\BioReferenceRange",    "transform": "generic"},
    r"Ignite\Evaluation\Entities\LabTestCategories":     {"v3": r"App\Models\LabTestCategory",      "transform": "generic"},
    r"Ignite\Evaluation\Entities\LabTestCategory":       {"v3": r"App\Models\LabTestCategory",      "transform": "generic"},
    r"Ignite\Evaluation\Entities\LabTestAdditives":      {"v3": r"App\Models\LabTestAdditive",      "transform": "generic"},
    r"Ignite\Evaluation\Entities\LabTestAdditive":       {"v3": r"App\Models\LabTestAdditive",      "transform": "generic"},
    r"Ignite\Evaluation\Entities\LabTestUnits":          {"v3": r"App\Models\LabTestUnit",          "transform": "generic"},
    r"Ignite\Evaluation\Entities\LabTestUnit":           {"v3": r"App\Models\LabTestUnit",          "transform": "generic"},
    r"Ignite\Evaluation\Entities\EvaluationFormulae":    {"v3": r"App\Models\EvaluationFormula",    "transform": "generic"},
    r"Ignite\Evaluation\Entities\EvaluationFormula":     {"v3": r"App\Models\EvaluationFormula",    "transform": "generic"},
    r"Ignite\Evaluation\Entities\EvaluationMachines":    {"v3": r"App\Models\EvaluationMachine",    "transform": "generic"},
    r"Ignite\Evaluation\Entities\EvaluationMachine":     {"v3": r"App\Models\EvaluationMachine",    "transform": "generic"},
    r"Ignite\Evaluation\Entities\PrescriptionFrequencies": {"v3": r"App\Models\PrescriptionFrequency", "transform": "generic"},
    r"Ignite\Evaluation\Entities\PrescriptionFrequency": {"v3": r"App\Models\PrescriptionFrequency", "transform": "generic"},
    r"Ignite\Evaluation\Entities\PrescriptionMeasures":  {"v3": r"App\Models\PrescriptionMeasure",  "transform": "generic"},
    r"Ignite\Evaluation\Entities\PrescriptionMeasure":   {"v3": r"App\Models\PrescriptionMeasure",  "transform": "generic"},
    r"Ignite\Evaluation\Entities\PrescriptionRoutes":    {"v3": r"App\Models\PrescriptionRoute",    "transform": "generic"},
    r"Ignite\Evaluation\Entities\PrescriptionRoute":     {"v3": r"App\Models\PrescriptionRoute",    "transform": "generic"},
    r"Ignite\Evaluation\Entities\ProcedureCategoryTemplates": {"v3": r"App\Models\ProcedureCategoryTemplate", "transform": "generic"},
    r"Ignite\Evaluation\Entities\ProcedureCategoryTemplate":  {"v3": r"App\Models\ProcedureCategoryTemplate", "transform": "generic"},
    r"Ignite\Evaluation\Entities\ProcedureTemplates":    {"v3": r"App\Models\ProcedureTemplate",    "transform": "generic"},
    r"Ignite\Evaluation\Entities\ProcedureTemplate":     {"v3": r"App\Models\ProcedureTemplate",    "transform": "generic"},
    r"Ignite\Evaluation\Entities\TemplateLabs":          {"v3": r"App\Models\TemplateLab",          "transform": "generic"},
    r"Ignite\Evaluation\Entities\TemplateLab":           {"v3": r"App\Models\TemplateLab",          "transform": "generic"},
    r"Ignite\Evaluation\Entities\Formulations":          {"v3": r"App\Models\Formulation",          "transform": "generic"},
    r"Ignite\Evaluation\Entities\Formulation":           {"v3": r"App\Models\Formulation",          "transform": "generic"},

    # Reception → Core service (appointment categories are lookup tables)
    r"Ignite\Reception\Entities\AppointmentCategories":  {"v3": r"App\Models\AppointmentCategory",  "transform": "generic"},
    r"Ignite\Reception\Entities\AppointmentCategory":    {"v3": r"App\Models\AppointmentCategory",  "transform": "generic"},
}

# ─── FIELD TRANSFORMS ────────────────────────────────────────────────────────

# Layer 1: global boolean renames (apply to every record)
_GLOBAL_BOOL_RENAMES: dict[str, str] = {
    "active":     "is_active",
    "consumable": "is_consumable",
    "for_cash":   "is_for_cash",
    "emergency":  "is_emergency",
    "approved":   "is_approved",
}

# Layer 1: bare int FK column → explicit _id name.
# Only applied when the bare name is present AND the _id form is absent,
# to avoid clobbering records that already use the V3 convention.
_GLOBAL_FK_RENAMES: dict[str, str] = {
    "patient":   "patient_id",
    "visit":     "visit_id",
    "sale":      "sale_id",
    "user":      "user_id",
    "company":   "company_id",
    "scheme":    "scheme_id",
    "procedure": "procedure_id",
    "category":  "category_id",
    "unit":      "unit_id",
}

# Layer 2: per-transform-key table-specific renames
_PER_KEY_RENAMES: dict[str, dict[str, str]] = {
    "generic": {},
    "finance_invoice": {
        "visit": "visit_id",
    },
    "finance_eval_payment": {
        "patient": "patient_id",
        "visit":   "visit_id",
        "sale":    "sale_id",
        "user":    "user_id",
    },
    "finance_copay": {},
    "finance_voucher": {
        "customer_id": "patient_id",
        "reward":      "discount_value",
        "condition":   "conditions",
    },
    "reception_patient": {
        "sex":   "gender",
        "image": "photo",
    },
    "reception_visit": {
        "type":      "visit_type",
        "complaint": "chief_complaint",
        "triage":    "triage_level",
    },
    "reception_appointment": {
        "instructions":          "notes",
        "external_appointment":  "is_external",
        "new_patient":           "is_new_patient",
    },
    "reception_patient_scheme": {
        "scheme":      "insurance_scheme_id",
        "patient":     "patient_id",
        "policy_number": "policy_no",
        "principal":   "principal_member_name",
        "dob":         "principal_dob",
        "company_id":  "insurance_company_id",
    },
    "evaluation_doctor_note": {
        "complaint":   "subjective",
        "examination": "objective",
        "diagnosis":   "assessment",
        "treatment":   "plan",
    },
    "evaluation_inv_result": {
        "approved": "is_approved",
    },
    "evaluation_eye_exam": {
        "visit":    "visit_id",
        "user":     "user_id",
        "comments": "notes",
        "od":       "right_eye_data",
        "os":       "left_eye_data",
    },
    "inventory_product": {
        "bar_code":  "barcode",
        "category":  "category_id",
        "unit":      "unit_id",
    },
    "inventory_batch": {
        "product":        "product_id",
        "quantity":       "quantity_received",
    },
    "inventory_purchase_order": {
        "delivery_date": "expected_delivery_date",
    },
    "inventory_requisition": {
        "requestor_id":     "requested_by",
        "approver_id":      "approved_by",
        "approval_weight":  "approval_level",
    },
    "inventory_category": {
        "parent_id": "parent_category_id",
    },
    "inventory_grn": {
        "user_id":       "received_by",
        "comment":       "comments",
        "date_received": "received_date",
    },
    "settings_scheme": {
        "company": "company_id",
    },
    "theatre_booking": {
        "emergency": "is_emergency",
    },
    "theatre_theatre": {
        "associated_procedure": "associated_procedures",
    },
    "settings_insurance": {
        "post_code":  "postal_code",
        "town":       "city",
        "telephone":  "phone",
        "mobile":     "mobile_number",
    },
    "eval_procedure_category": {
        "revenueAccount": "revenue_account",
    },
    "eval_procedure": {},
    "eval_sample_type": {},
    "settings_rebate": {},
    "settings_user": {
        "username":   "email",
        "first_name": "first_name",
        "last_name":  "last_name",
    },
}

# Fields to drop entirely per transform key (cannot be mapped in API-to-API)
_PER_KEY_DROP_FIELDS: dict[str, list] = {
    "settings_insurance":      ["manager_id", "customer_number"],
    "settings_scheme":         ["companies", "type_name", "full_name", "disabled"],
    "eval_procedure_category": ["procedures"],   # nested relation array
    "settings_user":           ["password", "remember_token", "api_token", "roles", "permissions", "abilities"],
}

# Fields that must be non-null for a record to be sent; records missing them are skipped
_PER_KEY_REQUIRED_FIELDS: dict[str, list] = {
    "settings_insurance":      ["name"],
    "eval_procedure_category": ["name"],
    "settings_user":           ["email"],
}

# Layer 2: coercions — value-level transforms applied after renaming.
# Each entry: (source_field_after_rename, transform_fn)
def _inactive_to_status(v) -> str:
    return "inactive" if v else "active"

def _active_flag_to_status(v) -> str:
    return "active" if v else "inactive"

def _wrap_in_list(v) -> list:
    if v is None:
        return []
    return v if isinstance(v, list) else [v]

_PER_KEY_COERCIONS: dict[str, dict[str, Any]] = {
    "reception_patient_scheme": {
        # V2 field is `inactive` (tinyint), which was renamed to nothing above
        # — handle via special case in transform_record
        "__inactive_to_status__": True,
    },
    "settings_scheme": {
        "__active_to_status__": True,
    },
    "theatre_theatre": {
        "associated_procedures": _wrap_in_list,
    },
    "evaluation_eye_exam": {
        # od/os were free-form columns; wrap as dict under the new key
        # so V3 receives a JSON object per eye rather than a raw scalar
        "right_eye_data": lambda v: {"raw": v} if v and not isinstance(v, dict) else v,
        "left_eye_data":  lambda v: {"raw": v} if v and not isinstance(v, dict) else v,
    },
}


def transform_record(record: dict, transform_key: str, org_cfg: dict) -> dict | None:
    """Apply V2→V3 field mapping to a single record dict.

    Returns None if the record fails a required-field check and should be skipped.

    Steps:
      1. Global boolean renames
      2. Global bare-FK renames (skip if _id form already exists)
      3. Per-key renames
      4. Per-key field drops
      5. Per-key coercions
      6. Required-field validation
    """
    out = dict(record)
    tk = transform_key if transform_key in _PER_KEY_RENAMES else "generic"

    # 1. Global boolean renames
    for old, new in _GLOBAL_BOOL_RENAMES.items():
        if old in out and new not in out:
            out[new] = out.pop(old)

    # 2. Global bare-FK renames
    for old, new in _GLOBAL_FK_RENAMES.items():
        if old in out and new not in out:
            out[new] = out.pop(old)

    # 3. Per-key renames
    for old, new in _PER_KEY_RENAMES.get(tk, {}).items():
        if old in out:
            if new not in out:
                out[new] = out.pop(old)
            else:
                out.pop(old)

    # 4. Per-key drops (fields that cannot be mapped in API-to-API)
    for field in _PER_KEY_DROP_FIELDS.get(tk, []):
        out.pop(field, None)

    # 5. Per-key coercions
    coercions = _PER_KEY_COERCIONS.get(tk, {})
    if coercions.get("__inactive_to_status__"):
        if "inactive" in out:
            out["status"] = _inactive_to_status(out.pop("inactive"))
    if coercions.get("__active_to_status__"):
        if "is_active" in out:
            out["status"] = _active_flag_to_status(out.pop("is_active"))
    for field, fn in coercions.items():
        if field.startswith("__"):
            continue
        if field in out:
            out[field] = fn(out[field])

    # 6. Required-field validation — skip records missing non-null required fields
    for field in _PER_KEY_REQUIRED_FIELDS.get(tk, []):
        if not out.get(field):
            return None

    return out


# ─── V2 AUTH ─────────────────────────────────────────────────────────────────

_v2_session_cache: dict[str, requests.Session] = {}
_v2_session_lock  = threading.Lock()
_v2_token_cache:  dict[str, tuple[str, float]] = {}  # facility -> (token, fetched_at)
_v2_token_lock    = threading.Lock()


def _v2_session(facility: str) -> requests.Session:
    with _v2_session_lock:
        s = _v2_session_cache.get(facility)
        if s is None:
            s = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=32, pool_maxsize=32, max_retries=0,
            )
            s.mount("https://", adapter)
            s.mount("http://", adapter)
            _v2_session_cache[facility] = s
        return s


def _generate_v2_token(facility: str) -> str:
    cfg   = V2_FACILITIES[facility]
    upper = facility.upper()
    user  = os.getenv(f"FACILITY_{upper}_USERNAME")
    pwd   = os.getenv(f"FACILITY_{upper}_PASSWORD")
    if not user or not pwd:
        raise RuntimeError(
            f"Missing FACILITY_{upper}_USERNAME / FACILITY_{upper}_PASSWORD env vars"
        )
    url = f"{cfg['base_url'].rstrip('/')}/api/users/authenticate/user"
    r = _v2_session(facility).post(url, json={"username": user, "password": pwd}, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"V2 auth failed for {facility}: {r.status_code} · {r.text[:200]}")
    token = (r.json().get("success") or {}).get("token")
    if not token:
        raise RuntimeError(f"V2 token not found in response for {facility}")
    return token


def _v2_token(facility: str) -> str:
    with _v2_token_lock:
        cached = _v2_token_cache.get(facility)
        if cached and (time.time() - cached[1]) < TOKEN_TTL_SECONDS:
            return cached[0]
        token = _generate_v2_token(facility)
        _v2_token_cache[facility] = (token, time.time())
        log.debug("V2 token refreshed for %s", facility)
        return token


def _v2_invalidate_token(facility: str) -> None:
    with _v2_token_lock:
        _v2_token_cache.pop(facility, None)


# ─── V3 AUTH ─────────────────────────────────────────────────────────────────

_v3_session_singleton: requests.Session | None = None
_v3_session_lock = threading.Lock()
_v3_token_cache: tuple[str, float] | None = None  # (token, fetched_at)
_v3_token_lock = threading.Lock()


def _v3_session() -> requests.Session:
    global _v3_session_singleton
    with _v3_session_lock:
        if _v3_session_singleton is None:
            s = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=32, pool_maxsize=32, max_retries=0,
            )
            s.mount("https://", adapter)
            s.mount("http://", adapter)
            _v3_session_singleton = s
        return _v3_session_singleton


def _generate_v3_token() -> str:
    user = os.getenv("AFYA_USERNAME")
    pwd  = os.getenv("AFYA_PASSWORD")
    if not user or not pwd:
        raise RuntimeError("Missing AFYA_USERNAME / AFYA_PASSWORD env vars")
    url = f"{V3_SERVICES['core'].rstrip('/')}/v1/login"
    r = _v3_session().post(url, json={"username": user, "password": pwd, "facility_id": 6}, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"V3 auth failed: {r.status_code} · {r.text[:200]}")
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError("V3 token not found in auth response")
    return token


def _v3_token() -> str:
    global _v3_token_cache
    with _v3_token_lock:
        if _v3_token_cache and (time.time() - _v3_token_cache[1]) < TOKEN_TTL_SECONDS:
            return _v3_token_cache[0]
        token = _generate_v3_token()
        _v3_token_cache = (token, time.time())
        log.debug("V3 token refreshed")
        return token


def _v3_invalidate_token() -> None:
    global _v3_token_cache
    with _v3_token_lock:
        _v3_token_cache = None


# ─── V2 EXTRACTION ───────────────────────────────────────────────────────────
# post_with_retry_and_fallback and _extract_all_pages mirror
# facility_to_snowflake_fast.py exactly, adjusted for V2-only auth.

def _post_with_retry(
    url: str,
    headers: dict,
    bodies: list[dict],
    *,
    facility: str,
    session: requests.Session,
    timeout: int = 60,
    max_retries: int = 6,
    default_retry_wait: int = 10,
    backoff_factor: int = 2,
) -> tuple[requests.Response, dict]:
    """Try each body in order. Handles 404 (next body), 429, 5xx, network errors."""
    for body in bodies:
        attempt, wait = 0, default_retry_wait
        while True:
            attempt += 1
            try:
                r = session.post(url=url, headers=headers, json=body, timeout=timeout)
                log.debug("ns=%s page=%s status=%s", body.get("namespace", "?"),
                          body.get("page", "?"), r.status_code)
                log.info("  · ns=%-60s page=%s status=%s",
                         body.get("namespace", "?"), body.get("page", "?"), r.status_code)

                if r.status_code == 404:
                    log.warning("  404 ns=%s — trying next fallback", body.get("namespace"))
                    break

                if r.status_code == 401:
                    if attempt >= max_retries:
                        r.raise_for_status()
                    _v2_invalidate_token(facility)
                    headers["Authorization"] = f"Bearer {_v2_token(facility)}"
                    log.warning("  401 — refreshed V2 token for %s (%s/%s)", facility, attempt, max_retries)
                    continue

                if r.status_code == 429:
                    retry_after = default_retry_wait
                    try:
                        retry_after = int(r.json().get("retry_after_seconds", default_retry_wait))
                    except Exception:
                        pass
                    if attempt >= max_retries:
                        r.raise_for_status()
                    log.warning("  429 ns=%s sleeping %ss (%s/%s)",
                                body.get("namespace"), retry_after, attempt, max_retries)
                    time.sleep(retry_after)
                    continue

                if r.status_code in {500, 502, 503, 504}:
                    if attempt >= max_retries:
                        r.raise_for_status()
                    log.warning("  %s ns=%s sleeping %ss (%s/%s)",
                                r.status_code, body.get("namespace"), wait, attempt, max_retries)
                    time.sleep(wait)
                    wait = min(wait * backoff_factor, 120)
                    continue

                r.raise_for_status()
                return r, body

            except (Timeout, ConnectionError) as e:
                if attempt >= max_retries:
                    raise
                log.warning("  Network error ns=%s %s sleeping %ss (%s/%s)",
                            body.get("namespace"), e, wait, attempt, max_retries)
                time.sleep(wait)
                wait = min(wait * backoff_factor, 120)
            except HTTPError:
                raise

    raise RuntimeError("All fallback namespaces returned 404")


def _extract_rows_from_payload(payload: dict) -> list[dict]:
    rows = payload.get("data")
    if rows is None:
        sv = payload.get("success")
        rows = sv.get("data") or [] if isinstance(sv, dict) else []
    if isinstance(rows, dict):
        rows = rows.get("data") or []
    elif not isinstance(rows, list):
        rows = []
    return rows


def _namespace_variants(namespace: str) -> list[str]:
    """Return [primary, singular, double, double-singular] fallback chain."""
    import inflect  # lazy import — only needed if inflect is installed

    try:
        engine = inflect.engine()
        parts = namespace.split("\\")
        cls = parts[-1]
        singular = engine.singular_noun(cls)
        singular = singular if singular else cls
    except ImportError:
        singular = namespace.split("\\")[-1].rstrip("s")
        parts = namespace.split("\\")

    primary  = namespace
    sing_ns  = "\\".join(parts[:-1] + [singular])
    double   = "\\".join(parts[:-1] + [parts[1] + parts[-1]]) if len(parts) > 1 else namespace
    dbl_sing = "\\".join(parts[:-1] + [parts[1] + singular])  if len(parts) > 1 else sing_ns

    seen: list[str] = []
    for ns in [primary, sing_ns, double, dbl_sing]:
        if ns not in seen:
            seen.append(ns)
    return seen


def extract_v2_records(job: dict) -> list[dict]:
    """Paginate V2 API and return all rows for the given job."""
    facility = job["facility"]
    cfg      = V2_FACILITIES[facility]
    url      = f"{cfg['base_url'].rstrip('/')}/api/finance/access/data/point"
    session  = _v2_session(facility)
    headers  = {
        "Authorization": f"Bearer {_v2_token(facility)}",
        "Content-Type": "application/json",
    }
    base_body = {
        "namespace":    job["namespace"],
        "action":       "get",
        "database":     job["database"],
        "updated_since": job["updated_since"],
        "limit":        job["limit"],
    }

    # Build fallback bodies (namespace variants × page=1)
    variants = _namespace_variants(job["namespace"])
    candidate_bodies = [{**base_body, "namespace": ns, "page": 1} for ns in variants]

    r, chosen_body = _post_with_retry(
        url=url, headers=headers, bodies=candidate_bodies,
        facility=facility, session=session,
    )
    payload  = r.json()
    all_rows = _extract_rows_from_payload(payload)

    pagination = payload.get("pagination") or {}
    has_more   = bool(pagination.get("has_more_pages", False))
    last_page  = pagination.get("last_page")

    if not has_more:
        return all_rows

    def _fetch_page(p: int) -> tuple[int, list]:
        r2, _ = _post_with_retry(
            url=url, headers=headers,
            bodies=[{**chosen_body, "page": p}],
            facility=facility, session=session,
        )
        return p, _extract_rows_from_payload(r2.json())

    if last_page is not None:
        last_page = min(int(last_page), 10_000)
        pages = list(range(2, last_page + 1))
        page_rows: dict[int, list] = {}
        with ThreadPoolExecutor(max_workers=max(1, PAGE_WORKERS)) as pool:
            for fut in as_completed(pool.submit(_fetch_page, p) for p in pages):
                p, rows = fut.result()
                page_rows[p] = rows
        for p in pages:
            all_rows.extend(page_rows.get(p, []))
        return all_rows

    # Sequential fallback when last_page unknown
    page = 1
    while has_more:
        page += 1
        if page > 10_000:
            log.warning("Pagination safety stop at page %s", page)
            break
        r2, _ = _post_with_retry(
            url=url, headers=headers,
            bodies=[{**chosen_body, "page": page}],
            facility=facility, session=session,
        )
        payload2 = r2.json()
        rows = _extract_rows_from_payload(payload2)
        all_rows.extend(rows)
        pagination = payload2.get("pagination") or {}
        has_more  = bool(pagination.get("has_more_pages", False))
        if not rows:
            break

    return all_rows


class GatewayModelNotRegistered(Exception):
    """Raised when the gateway returns 422 'model not registered' for a namespace."""


# ─── GATEWAY MODEL DISCOVERY ─────────────────────────────────────────────────

# alias → {tenant: str|None, facility: str|None, operations: list}
_gateway_model_meta: dict[str, dict] = {}


def _fetch_available_models() -> set[str]:
    """Query every V3 service gateway, union the insertable aliases, and build _alias_to_service."""
    global _gateway_model_meta, _alias_to_service
    available: set[str] = set()
    token = _v3_token()
    for service_name, base_url in V3_SERVICES.items():
        url = f"{base_url.rstrip('/')}/v1/gateway"
        try:
            r = _v3_session().post(
                url,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"action": "list"},
                timeout=30,
            )
            if not r.ok:
                log.warning("Gateway list [%s] %s: %s", service_name, r.status_code, r.text[:300])
                continue
            entries = r.json().get("data") or []
            for e in entries:
                alias = e.get("alias")
                if not alias:
                    continue
                ops = e.get("operations", [])
                # last-writer wins if alias appears in multiple services (unlikely)
                _gateway_model_meta[alias] = {
                    "tenant":     e.get("tenant"),
                    "facility":   e.get("facility"),
                    "operations": ops,
                    "service":    service_name,
                }
                _alias_to_service[alias] = service_name
                if "insert" in ops:
                    available.add(alias)
            log.info("Gateway [%s]: %d insertable models", service_name,
                     sum(1 for e in entries if "insert" in e.get("operations", [])))
        except Exception as e:
            log.warning("Could not reach gateway [%s]: %s", service_name, e)
    log.info("All gateways — total insertable models (%d): %s",
             len(available), ", ".join(sorted(available)))
    return available


_IRREGULAR_PLURALS: dict[str, str] = {
    # y → ies
    "facility":                    "facilities",
    "specialty":                   "specialties",
    "county":                      "counties",
    "category":                    "categories",
    "insurance_company":           "insurance_companies",
    "procedure_category":          "procedure_categories",
    "product_category":            "product_categories",
    "inventory_category":          "inventory_categories",
    "icd10_category":              "icd10_categories",
    "icd10_subcategory":           "icd10_subcategories",
    "lab_test_category":           "lab_test_categories",
    "employee_category":           "employee_categories",
    "prescription_frequency":      "prescription_frequencies",
    # Latin plural
    "evaluation_formula":          "evaluation_formulae",
}

def _v3_alias(v3_namespace: str) -> str:
    """Convert App\\Models\\FooBar to its gateway alias.

    Gateway aliases are NOT consistently plural — core-service uses plural
    (departments, facilities) while transactional services use singular
    (patient, invoice, theatre_booking).

    Resolution order (once gateway metadata is loaded):
      1. singular (snake_case class name) — matches most non-core aliases
      2. irregular plural (hardcoded table above)
      3. regular plural (snake + "s")

    Before gateway metadata loads (early startup calls), falls back to the
    same order but without the live lookup.
    """
    cls = v3_namespace.split("\\")[-1]
    snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", cls).lower()
    plural = _IRREGULAR_PLURALS.get(snake, snake + "s")

    # Once gateway metadata is populated, use it as ground truth
    if _gateway_model_meta:
        if snake in _gateway_model_meta:
            return snake
        if plural in _gateway_model_meta:
            return plural
        # Neither known form matched — return best-guess plural and let the
        # caller surface a "not in gateway" warning
        return plural

    # Gateway not yet loaded (startup) — return best-guess plural
    return plural


# ─── V3 POST ─────────────────────────────────────────────────────────────────

def _post_to_v3_batch(
    v3_namespace: str,
    org_cfg: dict,
    record: dict,
    *,
    max_retries: int = 6,
    default_retry_wait: int = V3_RETRY_WAIT,
    backoff_factor: int = 2,
) -> None:
    """POST a single record object to the V3 gateway. Retries on 429/5xx/401."""
    alias    = _v3_alias(v3_namespace)
    svc_url  = V3_SERVICES.get(_alias_to_service.get(alias, "core"), V3_SERVICES["core"])
    url      = f"{svc_url.rstrip('/')}/v1/gateway"
    session  = _v3_session()
    meta     = _gateway_model_meta.get(alias, {})
    headers: dict = {
        "Authorization": f"Bearer {_v3_token()}",
        "Content-Type":  "application/json",
    }
    if org_cfg.get("organization_id") is not None:
        headers["X-Tenant-Id"] = str(org_cfg["organization_id"])
    if meta.get("facility") and org_cfg.get("facility_id") is not None:
        headers["X-Facility-Id"] = str(org_cfg["facility_id"])
    body = {
        "action":                "insert",
        "model":                 alias,
        "destination_tenant_id": org_cfg.get("organization_id"),
        "data":                  record,  # gateway expects a single object, not an array
    }

    attempt, wait = 0, default_retry_wait
    while True:
        attempt += 1
        try:
            r = session.post(url=url, headers=headers, json=body, timeout=120)
            log.info("  V3 POST ns=%-50s status=%s",
                     v3_namespace, r.status_code)

            if r.status_code == 401:
                if attempt >= max_retries:
                    r.raise_for_status()
                _v3_invalidate_token()
                headers["Authorization"] = f"Bearer {_v3_token()}"
                log.warning("  V3 401 — refreshed token (%s/%s)", attempt, max_retries)
                continue

            if r.status_code == 429:
                retry_after = default_retry_wait
                try:
                    retry_after = int(r.json().get("retry_after_seconds", default_retry_wait))
                except Exception:
                    pass
                if attempt >= max_retries:
                    r.raise_for_status()
                log.warning("  V3 429 sleeping %ss (%s/%s)", retry_after, attempt, max_retries)
                time.sleep(retry_after)
                continue

            if r.status_code == 500:
                log.warning("  V3 500 — skipping record (no retry): %s", r.text[:500])
                return

            if r.status_code in {502, 503, 504}:
                log.error("  V3 %s response body: %s", r.status_code, r.text[:2000])
                if attempt >= max_retries:
                    r.raise_for_status()
                log.warning("  V3 %s sleeping %ss (%s/%s)", r.status_code, wait, attempt, max_retries)
                time.sleep(wait)
                wait = min(wait * backoff_factor, 120)
                continue

            if r.status_code == 422:
                log.error("  V3 422 response body: %s", r.text[:2000])
                r.raise_for_status()

            if not r.ok:
                log.error("  V3 %s response body: %s", r.status_code, r.text[:2000])
            r.raise_for_status()
            if V3_POST_THROTTLE > 0:
                time.sleep(V3_POST_THROTTLE)
            # Extract V3-assigned ID from response for FK remapping
            try:
                resp = r.json()
                return (resp.get("id")
                        or (resp.get("data") or {}).get("id")
                        or (resp.get("success") or {}).get("id"))
            except Exception:
                return None

        except (Timeout, ConnectionError) as e:
            if attempt >= max_retries:
                raise
            log.warning("  V3 network error %s sleeping %ss (%s/%s)", e, wait, attempt, max_retries)
            time.sleep(wait)
            wait = min(wait * backoff_factor, 120)


def post_to_v3(
    v3_namespace: str,
    org_cfg: dict,
    records: list[dict],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    job_key: str = "",
    transform_key: str = "",
) -> None:
    """POST records to V3 in parallel (gateway requires one object per request).

    Already-inserted records (by V2 id) are skipped for resume support.
    """
    pending = [
        r for r in records
        if not (r.get("id") is not None and job_key and _record_inserted(job_key, r.get("id")))
    ]
    skipped = len(records) - len(pending)
    total   = len(pending)
    if skipped:
        log.info("  Skipping %d already-inserted records, posting %d", skipped, total)

    done_count = 0

    alias = _v3_alias(v3_namespace)

    def _post_one(record: dict) -> None:
        nonlocal done_count
        remapped = _remap_fks(record, transform_key)
        v3_id = _post_to_v3_batch(v3_namespace, org_cfg, remapped)
        record_id = record.get("id")
        if record_id is not None:
            if job_key:
                _mark_record_inserted(job_key, record_id)
            if v3_id is not None:
                _store_id_mapping(alias, record_id, v3_id)
        with _record_progress_lock:
            done_count += 1
            n = done_count
        if n % RECORD_LOG_EVERY == 0 or n == total:
            log.info("  Posted %d / %d → %s", n, total, v3_namespace)

    with ThreadPoolExecutor(max_workers=RECORD_WORKERS) as pool:
        futures = {pool.submit(_post_one, r): r for r in pending}
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                rec = futures[fut]
                log.error("  Failed record id=%s: %s", rec.get("id"), e)

    # Final flush so no inserted IDs are lost between batch flushes
    with _record_progress_lock:
        if _inserted_ids:
            _flush_record_progress()


# ─── WATERMARKS ──────────────────────────────────────────────────────────────

_wm_lock = threading.Lock()


def _load_watermarks() -> dict:
    if WATERMARK_FILE.exists():
        try:
            return json.loads(WATERMARK_FILE.read_text())
        except Exception as e:
            log.warning("Could not parse %s: %s — starting fresh", WATERMARK_FILE, e)
    return {}


def _get_watermark(facility: str, namespace: str, default: str = "1970-01-01T00:00:00Z") -> str:
    return _load_watermarks().get(f"{facility}|{namespace}", default)


def _save_watermarks(wm: dict) -> None:
    WATERMARK_FILE.write_text(json.dumps(wm, indent=2, sort_keys=True))


# ─── PROGRESS CHECKPOINTS ────────────────────────────────────────────────────

_progress_lock = threading.Lock()
_completed_jobs: set[str] = set()


def _load_progress(run_id: str) -> set[str]:
    if PROGRESS_FILE.exists():
        try:
            data = json.loads(PROGRESS_FILE.read_text())
            if data.get("run_id") == run_id:
                return set(data.get("completed", []))
        except Exception:
            pass
    return set()


def _mark_done(run_id: str, job_key: str) -> None:
    with _progress_lock:
        _completed_jobs.add(job_key)
        PROGRESS_FILE.write_text(json.dumps(
            {"run_id": run_id, "completed": sorted(_completed_jobs)},
            indent=2,
        ))


def _job_key(facility: str, namespace: str) -> str:
    return f"{facility}|{namespace}"


# ─── RECORD-LEVEL PROGRESS ───────────────────────────────────────────────────
# Tracks individual V2 record IDs that were successfully inserted so that a
# re-run after a mid-job failure skips already-inserted records.

_record_progress_lock = threading.Lock()
_inserted_ids: dict[str, set] = {}  # job_key → set of inserted V2 ids


def _load_record_progress() -> None:
    global _inserted_ids
    if RECORD_PROGRESS_FILE.exists():
        try:
            data = json.loads(RECORD_PROGRESS_FILE.read_text())
            _inserted_ids = {k: set(v) for k, v in data.items()}
            total = sum(len(v) for v in _inserted_ids.values())
            if total:
                log.info("Record progress loaded — %d records already inserted across %d jobs",
                         total, len(_inserted_ids))
        except Exception as e:
            log.warning("Could not load record progress: %s — starting fresh", e)
            _inserted_ids = {}
    else:
        _inserted_ids = {}


def _record_inserted(job_key: str, record_id) -> bool:
    with _record_progress_lock:
        return record_id in _inserted_ids.get(job_key, set())


_record_flush_counter = 0


def _flush_record_progress() -> None:
    RECORD_PROGRESS_FILE.write_text(json.dumps(
        {k: sorted(v) for k, v in _inserted_ids.items()},
        indent=2,
    ))


def _mark_record_inserted(job_key: str, record_id) -> None:
    global _record_flush_counter
    with _record_progress_lock:
        _inserted_ids.setdefault(job_key, set()).add(record_id)
        _record_flush_counter += 1
        if _record_flush_counter % RECORD_FLUSH_EVERY == 0:
            _flush_record_progress()


# ─── V2→V3 ID MAP ────────────────────────────────────────────────────────────
# When a record is inserted into V3, the gateway returns a new auto-increment ID.
# We store v2_id → v3_id per model alias so FK fields can be remapped before
# inserting dependent records (e.g. scheme.company_id points to a V2 company id
# that doesn't exist in V3 — we replace it with the V3 id assigned on insert).

_id_map: dict[str, dict] = {}      # alias → {v2_id: v3_id}
_id_map_lock = threading.Lock()


# Which FK fields to remap, and which model alias holds their ID map.
_FK_REMAP: dict[str, dict[str, str]] = {
    # insurance_schemes.company_id → insurance_companies V3 id
    "settings_scheme": {
        "company_id": "insurance_companies",
    },
    # rebates.scheme_id → insurance_schemes V3 id
    "settings_rebate": {
        "scheme_id": "insurance_schemes",
    },
    # procedures.category_id → procedure_categories V3 id
    "eval_procedure": {
        "category_id": "procedure_categories",
    },
    # sample_types.procedure_id → procedures V3 id
    "eval_sample_type": {
        "procedure_id": "procedures",
    },
    # products depend on units, product_categories (migrate those first)
    "inventory_product": {
        "unit_id":     "unit",
        "category_id": "product_category",
    },
}


def _load_id_map() -> None:
    global _id_map
    if ID_MAP_FILE.exists():
        try:
            raw = json.loads(ID_MAP_FILE.read_text())
            # Keys are stored as strings in JSON; convert back to int where possible
            _id_map = {
                alias: {(int(k) if k.isdigit() else k): v for k, v in mapping.items()}
                for alias, mapping in raw.items()
            }
            total = sum(len(v) for v in _id_map.values())
            if total:
                log.info("ID map loaded — %d entries across %d models", total, len(_id_map))
        except Exception as e:
            log.warning("Could not load ID map: %s — starting fresh", e)
            _id_map = {}
    else:
        _id_map = {}


def _store_id_mapping(alias: str, v2_id, v3_id) -> None:
    with _id_map_lock:
        _id_map.setdefault(alias, {})[v2_id] = v3_id
        ID_MAP_FILE.write_text(json.dumps(_id_map, indent=2))


def _fetch_v3_records(alias: str, org_cfg: dict) -> list[dict]:
    """Fetch all existing V3 records for a model via the gateway read action."""
    svc_url = V3_SERVICES.get(_alias_to_service.get(alias, "core"), V3_SERVICES["core"])
    url     = f"{svc_url.rstrip('/')}/v1/gateway"
    headers = {
        "Authorization": f"Bearer {_v3_token()}",
        "Content-Type":  "application/json",
    }
    records, page = [], 1
    while True:
        body = {
            "action":           "read",
            "model":            alias,
            "source_tenant_id": org_cfg.get("organization_id"),
            "per_page":         500,
            "page":             page,
        }
        try:
            r = _v3_session().post(url, headers=headers, json=body, timeout=60)
            if not r.ok:
                log.warning("Gateway read %s page %s: %s %s", alias, page, r.status_code, r.text[:300])
                break
            payload = r.json()
            rows = payload.get("data") or []
            if isinstance(rows, dict):
                rows = rows.get("data") or list(rows.values())
            if not rows:
                break
            records.extend(rows)
            # Stop if fewer rows than per_page (last page)
            if len(rows) < 500:
                break
            page += 1
        except Exception as e:
            log.warning("Error fetching V3 %s page %s: %s", alias, page, e)
            break
    log.info("Fetched %d existing V3 records for %s", len(records), alias)
    return records


def sync_id_map(alias: str, v2_namespace: str, facility: str) -> int:
    """Match existing V3 records to V2 records by name and populate the ID map.

    Used when a model was migrated before the ID-mapping system existed.
    Returns the number of mappings added.
    """
    org_cfg = FACILITY_V3_CONFIG.get(facility, {})

    v3_records = _fetch_v3_records(alias, org_cfg)
    if not v3_records:
        log.warning("sync_id_map: no V3 records found for %s — cannot build map", alias)
        return 0

    # Build name → V3 id lookup (name is the universal match key)
    v3_by_name: dict[str, int] = {}
    for rec in v3_records:
        name = rec.get("name")
        v3_id = rec.get("id")
        if name and v3_id:
            v3_by_name[str(name).strip()] = v3_id

    # Extract V2 records
    cfg = V2_FACILITIES[facility]
    job = {
        "facility":      facility,
        "namespace":     v2_namespace,
        "database":      cfg["db"],
        "updated_since": "1970-01-01T00:00:00Z",
        "limit":         DEFAULT_LIMIT,
    }
    try:
        v2_records = extract_v2_records(job)
    except Exception as e:
        log.error("sync_id_map: V2 extraction failed for %s: %s", v2_namespace, e)
        return 0

    matched = 0
    for rec in v2_records:
        v2_id = rec.get("id")
        name  = str(rec.get("name") or "").strip()
        if v2_id and name:
            v3_id = v3_by_name.get(name)
            if v3_id:
                _store_id_mapping(alias, v2_id, v3_id)
                matched += 1

    log.info("sync_id_map %s [%s]: matched %d / %d V2 records to V3 ids",
             alias, facility, matched, len(v2_records))
    return matched


def _ensure_id_maps(transform_key: str, facility: str) -> None:
    """Auto-sync any FK dependency maps that are empty before a job runs."""
    for field, alias in _FK_REMAP.get(transform_key, {}).items():
        if not _id_map.get(alias):
            log.info("ID map for %s is empty — auto-syncing from V3 before %s job",
                     alias, transform_key)
            # Find the V2 namespace that produces this alias
            v2_ns = next(
                (ns for ns, m in NAMESPACE_MAP.items() if _v3_alias(m["v3"]) == alias),
                None,
            )
            if v2_ns:
                sync_id_map(alias, v2_ns, facility)
            else:
                log.warning("Cannot find V2 namespace for alias %s — skipping auto-sync", alias)


def _remap_fks(record: dict, transform_key: str) -> dict:
    """Replace V2 FK values with their V3-assigned IDs."""
    fk_config = _FK_REMAP.get(transform_key, {})
    if not fk_config:
        return record
    out = dict(record)
    for field, alias in fk_config.items():
        v2_id = out.get(field)
        if v2_id is None:
            continue
        v3_id = _id_map.get(alias, {}).get(v2_id)
        if v3_id is not None:
            out[field] = v3_id
        else:
            log.warning("  No V3 ID mapping for %s id=%s — %s will fail FK constraint",
                        alias, v2_id, field)
    return out


# ─── JOB RUNNER ──────────────────────────────────────────────────────────────

def run_job(job: dict, run_id: str, batch_size: int, dry_run: bool) -> bool:
    """Extract from V2, transform, POST to V3. Returns True on success."""
    facility  = job["facility"]
    namespace = job["namespace"]
    mapping   = NAMESPACE_MAP.get(namespace)
    if mapping is None:
        log.warning("No NAMESPACE_MAP entry for %s — skipping", namespace)
        return True

    org_cfg = FACILITY_V3_CONFIG.get(facility, {})
    if org_cfg.get("organization_id") is None or org_cfg.get("facility_id") is None:
        log.error(
            "FACILITY_V3_CONFIG for %s has organization_id/facility_id = None. "
            "Populate FACILITY_V3_CONFIG before running.",
            facility,
        )
        return False

    v3_namespace  = mapping["v3"]
    transform_key = mapping["transform"]
    alias         = _v3_alias(v3_namespace)
    label         = f"[{facility}] {namespace}  →  {alias}"

    log.info("▶ %s", label)
    t0 = time.perf_counter()

    try:
        rows = extract_v2_records(job)
    except Exception as e:
        log.error("✗ V2 extraction FAILED  %s: %s", label, e)
        return False

    if not rows:
        log.info("⊘ %s — 0 rows from V2 (no records or all before watermark)", label)
        _mark_done(run_id, _job_key(facility, namespace))
        return True

    log.info("  %s — %d rows fetched in %.2fs", label, len(rows), time.perf_counter() - t0)
    log.info("  %s — sample: %s", label, _dumps(rows[0])[:500])

    transformed_raw = [transform_record(r, transform_key, org_cfg) for r in rows]
    transformed = [r for r in transformed_raw if r is not None]
    n_skipped_transform = len(transformed_raw) - len(transformed)
    if n_skipped_transform:
        log.warning("  %s — %d/%d records dropped by required-field check",
                    label, n_skipped_transform, len(transformed_raw))

    if not transformed:
        log.warning("⊘ %s — all %d records failed transform, nothing to post",
                    label, len(transformed_raw))
        _mark_done(run_id, _job_key(facility, namespace))
        return True

    if dry_run:
        log.info("DRY-RUN ✓ %s — would POST %d records", label, len(transformed))
        log.info("  Sample transformed: %s", _dumps(transformed[0])[:400])
        return True

    # Auto-populate any FK maps that are missing before we start posting
    _ensure_id_maps(transform_key, facility)

    try:
        post_to_v3(v3_namespace, org_cfg, transformed, batch_size=batch_size,
                   job_key=_job_key(facility, namespace), transform_key=transform_key)
    except GatewayModelNotRegistered as e:
        log.warning("⊘ %s — model not registered in gateway: %s", label, e)
        return True
    except Exception as e:
        log.error("✗ V3 POST FAILED  %s: %s", label, e)
        return False

    elapsed = time.perf_counter() - t0
    log.info("✓ %s — %d records migrated in %.2fs", label, len(transformed), elapsed)
    _mark_done(run_id, _job_key(facility, namespace))
    return True


# ─── ORCHESTRATOR ────────────────────────────────────────────────────────────

def run_migration(
    facilities: list[str],
    namespaces: list[str] | None,
    *,
    since: str | None,
    workers: int,
    batch_size: int,
    dry_run: bool,
) -> None:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    global _completed_jobs
    _completed_jobs = _load_progress(run_id)
    _load_record_progress()
    _load_id_map()
    if _completed_jobs:
        log.info("Resuming run %s — %d jobs already done", run_id, len(_completed_jobs))

    # Fetch registered gateway models — skip anything not yet available
    available_models = _fetch_available_models()
    if available_models:
        log.info("Gateway has %d registered models: %s",
                 len(available_models), ", ".join(sorted(available_models)))
    else:
        log.warning("Could not determine available gateway models — all namespaces will be attempted")

    # Build job list — track every namespace disposition for the summary table
    all_namespaces = namespaces or list(NAMESPACE_MAP.keys())
    seen_v3: dict[tuple[str, str], str] = {}  # (facility, v3_ns) → v2_ns
    jobs: list[dict] = []

    # disposition buckets per facility|namespace
    skipped_no_insert:    list[str] = []   # in gateway but no insert permission
    skipped_unregistered: list[str] = []   # alias not found in any gateway
    skipped_already_done: list[str] = []
    skipped_dedup:        list[str] = []

    for facility in facilities:
        cfg = V2_FACILITIES[facility]
        watermark = since or _get_watermark(facility, "all")
        for ns in all_namespaces:
            if ns not in NAMESPACE_MAP:
                continue
            v3_ns = NAMESPACE_MAP[ns]["v3"]
            alias = _v3_alias(v3_ns)
            if available_models and alias not in available_models:
                cls   = v3_ns.split("\\")[-1]
                snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", cls).lower()
                plurl = _IRREGULAR_PLURALS.get(snake, snake + "s")
                if alias in _gateway_model_meta:
                    svc = _gateway_model_meta[alias].get("service", "?")
                    ops = _gateway_model_meta[alias].get("operations", [])
                    skipped_no_insert.append(
                        f"  {facility}|{ns}  →  {alias!r} in {svc} [{', '.join(ops)}] — no insert op"
                    )
                else:
                    tried = f"singular={snake!r}, plural={plurl!r}"
                    skipped_unregistered.append(
                        f"  {facility}|{ns}  →  tried {tried} — not registered in any gateway"
                    )
                continue
            dedup_key = (facility, v3_ns)
            if dedup_key in seen_v3:
                skipped_dedup.append(f"  {facility}|{ns}  (same V3 target as {seen_v3[dedup_key]})")
                continue
            seen_v3[dedup_key] = ns
            jk = _job_key(facility, ns)
            if jk in _completed_jobs:
                skipped_already_done.append(f"  {jk}")
                continue
            jobs.append({
                "facility":      facility,
                "namespace":     ns,
                "database":      cfg["db"],
                "updated_since": watermark,
                "limit":         DEFAULT_LIMIT,
            })

    if skipped_no_insert:
        log.info(
            "SKIPPED — registered in gateway but no insert permission (%d):\n%s",
            len(skipped_no_insert), "\n".join(skipped_no_insert),
        )
    if skipped_unregistered:
        log.warning(
            "SKIPPED — not registered in any gateway (%d):\n%s",
            len(skipped_unregistered), "\n".join(skipped_unregistered),
        )
    if skipped_already_done:
        log.info(
            "SKIPPED — already completed in this run (%d):\n%s",
            len(skipped_already_done), "\n".join(skipped_already_done),
        )
    if skipped_dedup:
        log.debug(
            "SKIPPED — duplicate V3 target (%d):\n%s",
            len(skipped_dedup), "\n".join(skipped_dedup),
        )

    log.info(
        "Run %s — %d jobs queued | %d no-insert | %d unregistered | %d already done%s",
        run_id, len(jobs), len(skipped_no_insert), len(skipped_unregistered),
        len(skipped_already_done), " | DRY-RUN" if dry_run else "",
    )

    failures: list[str] = []

    if workers <= 1:
        for job in jobs:
            ok = run_job(job, run_id, batch_size, dry_run)
            if not ok:
                failures.append(_job_key(job["facility"], job["namespace"]))
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_job = {
                pool.submit(run_job, job, run_id, batch_size, dry_run): job
                for job in jobs
            }
            for fut in as_completed(future_to_job):
                job = future_to_job[fut]
                try:
                    ok = fut.result()
                except Exception as e:
                    log.error("Unhandled error [%s] %s: %s",
                              job["facility"], job["namespace"], e)
                    ok = False
                if not ok:
                    failures.append(_job_key(job["facility"], job["namespace"]))

    # ── End-of-run summary ───────────────────────────────────────────────────
    n_ok           = len(jobs) - len(failures)
    n_failed       = len(failures)
    n_no_insert    = len(skipped_no_insert)
    n_unregistered = len(skipped_unregistered)
    n_done         = len(skipped_already_done)

    summary_lines = [
        "",
        "══════════════════════  MIGRATION SUMMARY  ══════════════════════",
        f"  Run ID   : {run_id}",
        f"  Queued   : {len(jobs)}   completed: {n_ok}   failed: {n_failed}",
        f"  Skipped  : {n_no_insert} no-insert | {n_unregistered} unregistered | {n_done} already-done",
    ]

    if failures:
        summary_lines.append("")
        summary_lines.append(f"  FAILED ({n_failed}) — fix and re-run:")
        for f in failures:
            summary_lines.append(f"    ✗ {f}")

    if skipped_no_insert:
        summary_lines.append("")
        summary_lines.append(f"  READ-ONLY IN GATEWAY ({n_no_insert}) — no insert op; enable in V3 to migrate:")
        for line in skipped_no_insert:
            summary_lines.append(f"    ⊘{line}")

    if skipped_unregistered:
        summary_lines.append("")
        summary_lines.append(f"  NOT REGISTERED IN ANY GATEWAY ({n_unregistered}) — add model to V3 gateway:")
        for line in skipped_unregistered:
            summary_lines.append(f"    ✗{line}")

    summary_lines.append("═════════════════════════════════════════════════════════════════")
    log.info("\n".join(summary_lines))

    # Advance watermarks only if zero failures
    if not failures and not dry_run:
        now_iso = datetime.now(timezone.utc).isoformat()
        wm = _load_watermarks()
        for facility in facilities:
            for ns in all_namespaces:
                if ns in NAMESPACE_MAP:
                    wm[f"{facility}|{ns}"] = now_iso
            wm[f"{facility}|all"] = now_iso
        _save_watermarks(wm)
        log.info("Watermarks advanced to %s", now_iso)
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
    elif failures:
        log.warning(
            "%d job(s) FAILED — watermarks NOT advanced. Re-run to retry.\n  %s",
            len(failures), "\n  ".join(failures),
        )


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate data from V2 facility APIs to the V3 Afya API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--facility", "-f",
        nargs="+",
        metavar="NAME",
        help="One or more facility keys (default: all in V2_FACILITIES)",
    )
    parser.add_argument(
        "--namespace", "-n",
        nargs="+",
        metavar="NS",
        help="One or more V2 namespaces to migrate (default: all in NAMESPACE_MAP)",
    )
    parser.add_argument(
        "--since",
        metavar="ISO8601",
        default=None,
        help="Override watermark — extract records updated after this timestamp",
    )
    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=PIPELINE_WORKERS,
        help=f"Parallel job workers (default: {PIPELINE_WORKERS})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Records per V3 POST request (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from V2 and show transformed records without posting to V3",
    )
    parser.add_argument(
        "--list-namespaces",
        action="store_true",
        help="Print all configured namespace mappings and exit",
    )
    args = parser.parse_args()

    if args.list_namespaces:
        print(f"{'V2 namespace':<60}  {'V3 namespace':<45}  transform")
        print("-" * 120)
        for v2_ns, cfg in NAMESPACE_MAP.items():
            print(f"{v2_ns:<60}  {cfg['v3']:<45}  {cfg['transform']}")
        return

    facilities = args.facility or list(V2_FACILITIES.keys())
    unknown = [f for f in facilities if f not in V2_FACILITIES]
    if unknown:
        parser.error(f"Unknown facilities: {', '.join(unknown)}. "
                     f"Valid: {', '.join(V2_FACILITIES)}")

    run_migration(
        facilities=facilities,
        namespaces=args.namespace,
        since=args.since,
        workers=args.workers,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
