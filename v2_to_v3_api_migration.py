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
import atexit
import signal
import base64
import contextlib
import hashlib
import hmac
import json
import mimetypes
import logging
import os
import re
import secrets
import sys
import threading
import time
from  migration_state import StatePaths, get_paths, run_lock, use_state_dir, STATE_ROOT
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


try:
    import fcntl   # state-file locking (see _file_lock); absent on Windows
except ImportError:
    fcntl = None

import requests
import requests.adapters
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, HTTPError, ReadTimeout, Timeout

def _json_default(o):
    """Types the encoders don't know. Snowflake NUMBER columns with a scale
    (prices, quantities) come back as Decimal: whole values go as ints, the
    rest as floats. Dates/datetimes go as ISO strings."""
    from datetime import date, datetime
    from decimal import Decimal
    if isinstance(o, Decimal):
        return int(o) if o == o.to_integral_value() else float(o)
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    raise TypeError(f"Type is not JSON serializable: {type(o).__module__}.{type(o).__name__}")


# Optional fast JSON encoder
try:
    import orjson
    def _dumps(obj) -> str:
        return orjson.dumps(obj, default=_json_default).decode()
except ImportError:
    def _dumps(obj) -> str:
        return json.dumps(obj, separators=(",", ":"), default=_json_default)

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
GARBLED_KEY = "__garbled__"   # transform → poster only: encoded fallbacks for nulled garbled fields
ENCODE_CORRUPTED_PII = os.getenv("ENCODE_CORRUPTED_PII", "0").strip() in ("1", "true", "yes")  # see transform_record step 4c
RECORD_LOG_EVERY   = int(os.getenv("RECORD_LOG_EVERY", "100")) # log progress every N records
RECORD_FLUSH_EVERY = int(os.getenv("RECORD_FLUSH_EVERY", "50")) # flush progress file every N records
RECORD_FLUSH_SECONDS = float(os.getenv("RECORD_FLUSH_SECONDS", "5")) 
TOKEN_TTL_SECONDS  = int(os.getenv("TOKEN_TTL_SECONDS", str(50 * 60)))
DEFAULT_BATCH_SIZE = 200
DEFAULT_LIMIT      = 500
V3_POST_THROTTLE   = float(os.getenv("V3_POST_THROTTLE", "0"))   # seconds to sleep after each V3 POST
V3_RETRY_WAIT      = int(os.getenv("V3_RETRY_WAIT", "30"))        # initial wait (s) before retrying 502/503/504
V3_GATEWAY_RETRIES = int(os.getenv("V3_GATEWAY_RETRIES", "5"))    # retries on 502/503/504 before failing the record

PROGRESS_FILE = None
RECORD_PROGRESS_FILE = None
ID_MAP_FILE = None
DONE_FILE = None
WATERMARK_FILE = None
DEAD_LETTER_FILE = None



def _sync_path_globals() -> None:
    """Temporary back-compat layer.
    Prefer get_paths().file("...") in all new code.
    """
    p = get_paths()
    global PROGRESS_FILE, RECORD_PROGRESS_FILE, ID_MAP_FILE
    global DONE_FILE,DEAD_LETTER_FILE,WATERMARK_FILE
    PROGRESS_FILE        = p.file("progress")
    RECORD_PROGRESS_FILE = p.file("record_progress")
    ID_MAP_FILE          = p.file("id_map")
    DONE_FILE            = p.file("done")
    WATERMARK_FILE       = p.watermark          # still shared at env level
    DEAD_LETTER_FILE     = p.file("dead_letter", ext="jsonl")
       
   
# Initialize defaults (repo root, no facility) so early imports/calls have real Paths
_sync_path_globals()

# Per-facility state. V2 ids are only unique WITHIN a facility (two
# facilities both have a patient #271700, in different V3 orgs), so the id
# map, progress, done list and visit maps of one facility must never be
# shared with another: use_state_dir(facility) points the files above at
# .migration_state/<facility>/. Without it (the old CLI path) they stay in
# the repo root. The failure/PII logs stay shared (append-only).



def use_state_dir(
    facility: str | None,
    pipeline: str = "v2_to_v3",
    *,
    process_wide: bool = False,
) -> Path:
    """Switch state directory for this facility + pipeline and refresh globals."""
    from migration_state import use_state_dir as _use
    d = _use(facility, pipeline=pipeline, process_wide=process_wide)
    _sync_path_globals()
    global _record_progress_mtime, _id_map_mtime
    _record_progress_mtime = _id_map_mtime = None
    return d

def v2_facility_config(facility: str) -> dict:
    """V2_FACILITIES[facility] with base_url/db overridable by
    FACILITY_<F>_BASE_URL / FACILITY_<F>_DB (see the loader's facility_config)."""
    up = facility.upper()
    cfg = dict(V2_FACILITIES.get(facility, {}))
    for key, env in (("base_url", f"FACILITY_{up}_BASE_URL"), ("db", f"FACILITY_{up}_DB")):
        val = (os.getenv(env) or "").strip().strip("'\"")
        if val:
            cfg[key] = val
    if not cfg.get("base_url"):
        raise KeyError(f"No V2 base_url for {facility!r} (V2_FACILITIES or FACILITY_{up}_BASE_URL)")
    return cfg


def facility_v3_config(facility: str | None) -> dict:
    """FACILITY_V3_CONFIG[facility], with organization_id / facility_id
    overridable by AFYA_<FACILITY>_ORGANIZATION_ID / _FACILITY_ID (which the
    DAGs fill from the Airflow Connection afya_v3_<facility>'s Extra), so a
    new facility needs no code change."""
    cfg = dict(FACILITY_V3_CONFIG.get(facility or "", {"application_id": 1}))
    for key in ("organization_id", "facility_id"):
        val = (os.getenv(f"AFYA_{(facility or '').upper()}_{key.upper()}") or "").strip()
        if val.isdigit():
            cfg[key] = int(val)
    return cfg

# V2 source facilities
V2_FACILITIES: dict[str, dict] = {
    "afya_api_auth": {"base_url": "https://staging.afyanalytics.ai",    "db": "staging_db"},
    "kakamega":      {"base_url": "https://demo.collabmed.net",          "db": "kakamega_db"},
    "kisumu":        {"base_url": "https://kshospital.collabmed.net",    "db": "kisumu_db"},
    "kisumu_v3":     {"base_url": "https://kshospital.collabmed.net",    "db": "kisumu_db"},
    "lodwar":        {"base_url": "https://lcrh.collabmed.net",          "db": "lodwar_db"},
    "tenri":         {"base_url": "https://stageenv.collabmed.net",      "db": "tenri_db"},
    "xanalife":      {"base_url": "https://xanalife.afyanalytics.ai/",   "db": "xanalife_db"},
}

# V3 service base URLs — each service has a DEDICATED migration-gateway host,
# <service>migrate.afyaanalytics.ai, distinct from that service's regular
# production API host (e.g. finance.afyaanalytics.ai is a different, real,
# but OAuth-untrusted-for-this-client deployment — confirmed by a live 401
# there with a fresh token while coremigrate accepted the same token fine).
# Load order per the ModelGateway Postman collection: core -> reception ->
# evaluation -> inventory -> finance -> theatre -> inpatient -> dialysis.
V3_SERVICES: dict[str, str] = {
    "core":       "https://coremigrate.afyaanalytics.ai/api/",
    "reception":  "https://receptionmigrate.afyaanalytics.ai/api/",
    "evaluation": "https://evaluationmigrate.afyaanalytics.ai/api/",
    "inventory":  "https://inventorymigrate.afyaanalytics.ai/api/",
    "finance":    "https://financemigrate.afyaanalytics.ai/api/",
    "theatre":    "https://theatremigrate.afyaanalytics.ai/api/",
    "inpatient":  "https://inpatientmigrate.afyaanalytics.ai/api/",
    "dialysis":   "https://dialysismigrate.afyaanalytics.ai/api/",
}
_DEFAULT_V3_SERVICES = dict(V3_SERVICES)


def _apply_v3_urls(facility: str | None) -> None:
    """Per-facility V3 URLs (they differ: coremigrate.… vs core.…). For each
    service: AFYA_<FACILITY>_<SERVICE>_URL, else AFYA_<FACILITY>_URL_TEMPLATE
    (or V3_URL_TEMPLATE) with {service} filled in, else the default above.
    The DAGs fill these from the Connection afya_v3_<facility>'s Extra
    ("url_template" / "urls"). Updates V3_SERVICES in place."""
    up = (facility or "").upper()
    tmpl = (os.getenv(f"AFYA_{up}_URL_TEMPLATE") or os.getenv("V3_URL_TEMPLATE") or "").strip()
    for svc, default in _DEFAULT_V3_SERVICES.items():
        url = (os.getenv(f"AFYA_{up}_{svc.upper()}_URL") or "").strip()
        V3_SERVICES[svc] = url or (tmpl.format(service=svc) if tmpl else default)
    if V3_SERVICES != _DEFAULT_V3_SERVICES:
        log.info("V3 URLs for %s: %s", facility,
                 ", ".join(f"{k}={v}" for k, v in V3_SERVICES.items() if v != _DEFAULT_V3_SERVICES[k]))

# Auth differs by service (per the live ModelGateway Postman collection):
#   core                 HMAC signing (X-App-Id/X-Timestamp/X-Signature) when
#                        CORE_APP_ID + CORE_APP_SECRET are set; otherwise the
#                        superadmin bearer token (see _service_headers).
#   dialysis, theatre    X-Migration-Key header AND a superadmin bearer token.
#   everything else      superadmin bearer token only.
V3_AUTH_SCHEME: dict[str, str] = {
    "core":       "hmac",
    "dialysis":   "migration_key",
    "theatre":    "migration_key",
    "reception":  "bearer",
    "evaluation": "bearer",
    "inventory":  "bearer",
    "finance":    "bearer",
    "inpatient":  "bearer",
}

# Runtime dict built in run_migration: alias → service name
_alias_to_service: dict[str, str] = {}


class ServiceAuthUnavailable(Exception):
    """Raised when a service needs a credential that isn't configured (the
    core HMAC secret, or the theatre/dialysis migration key). Callers should
    skip that service's jobs, not abort the whole run."""


def _hmac_headers(method: str, path: str, body_str: str) -> dict:
    """core-service auth: HMAC-SHA256 over 'METHOD\\nPATH\\nTS\\nsha256(body)',
    signed with the RegisteredApp's secret. Matches the Postman collection's
    pre-request script exactly."""
    app_id = (os.getenv("CORE_APP_ID") or "").strip()
    secret = (os.getenv("CORE_APP_SECRET") or "").strip()
    if not app_id or not secret:
        raise ServiceAuthUnavailable(
            "core-service requires CORE_APP_ID + CORE_APP_SECRET env vars (HMAC signing) — not set"
        )
    ts = str(int(time.time()))
    canonical = "\n".join([method, path, ts, hashlib.sha256(body_str.encode()).hexdigest()])
    sig = hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()
    return {"X-App-Id": app_id, "X-Timestamp": ts, "X-Signature": sig}


def _service_headers(service_name: str, body_str: str) -> dict:
    """Auth headers for one /v1/gateway call, per V3_AUTH_SCHEME. Takes the
    body as an already-serialized string, not a dict — for HMAC (core), the
    signature covers sha256(body), so the exact bytes signed here MUST be
    the exact bytes sent on the wire. See _gateway_post().

    Deliberately never sends X-Tenant-Id / X-Facility-Id: both are dead
    weight at best (TenantContext takes the org from the authenticated user
    and reads facility targeting from the request body) and actively
    dangerous at worst (X-Facility-Id 403s if it isn't one of the caller's
    OWN assigned facilities, checked before the controller even runs).
    Tenant/facility targeting belongs in the body — destination_tenant_id /
    source_tenant_id, and data.facility_id for facility-scoped models.
    """
    scheme = V3_AUTH_SCHEME.get(service_name, "bearer")
    # HMAC is optional: without CORE_APP_ID/CORE_APP_SECRET, core takes the
    # same superadmin bearer token as the other services.
    if scheme == "hmac" and not ((os.getenv("CORE_APP_ID") or "").strip() and (os.getenv("CORE_APP_SECRET") or "").strip()):
        scheme = "bearer"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if scheme == "hmac":
        headers.update(_hmac_headers("POST", "/api/v1/gateway", body_str))
    else:
        headers["Authorization"] = f"Bearer {_v3_token()}"
        if scheme == "migration_key":
            key = (os.getenv("MODEL_GATEWAY_MIGRATION_KEY") or "").strip()
            if not key:
                raise ServiceAuthUnavailable(
                    f"{service_name}-service requires MODEL_GATEWAY_MIGRATION_KEY "
                    f"env var (X-Migration-Key) — not set"
                )
            headers["X-Migration-Key"] = key
    return headers


def _gateway_post(service_name: str, body_obj: dict, *, timeout: int = 30):
    """POST to <service>/api/v1/gateway with the correct auth for that
    service. Serializes the body exactly once and sends those SAME bytes —
    critical for HMAC: requests' own json= parameter re-serializes
    independently (different whitespace than _dumps()), which would sign
    one byte string and transmit another, failing signature verification
    every time despite a perfectly valid secret."""
    body_str = _dumps(body_obj)

    headers = _service_headers(service_name, body_str)
    url = f"{V3_SERVICES[service_name].rstrip('/')}/v1/gateway"
    return _v3_session().post(url, headers=headers, data=body_str.encode(), timeout=timeout)

# V3 splits vitals into two separate tables: inp_vitals (inpatient-service,
# tied to an admission) vs a distinct vitals table for outpatient visits
# (patient-evaluation-service). V2 never had this distinction — every V2
# Vitals record just carries a visit_id, so the split is done at migration
# time based on whether that visit has a corresponding V2 Admission.
# NEEDS VERIFICATION: the exact V3 model class/gateway alias used by
# patient-evaluation-service for outpatient vitals — assumed identical name
# to the inpatient one (each microservice has its own model namespace) and
# disambiguated purely via service_override rather than alias discovery,
# since the discovery-based alias→service map can only hold one winner when
# two services register the same alias.
OUTPATIENT_VITAL_V3_NAMESPACE = r"App\Models\Vital"
OUTPATIENT_VITAL_TRANSFORM    = "outpatient_vital"
OUTPATIENT_VITAL_SERVICE      = "evaluation"
INPATIENT_VITAL_SERVICE       = "inpatient"
# ─── FACILITY → V3 ORG MAPPING ──────────────────────────────────────────────
# Fill in organization_id and facility_id from the V3 core_organizations /
# core_facilities tables before running. application_id is typically 1.
FACILITY_V3_CONFIG: dict[str, dict] = {
    "afya_api_auth": {"organization_id":  1, "facility_id": 6, "application_id": 1},
    "kakamega":      {"organization_id": None, "facility_id": None, "application_id": 1},
    "kisumu":        {"organization_id": 1, "facility_id": 6, "application_id": 1},
    "kisumu_v3":     {"organization_id": 4, "facility_id": 4, "application_id": 1},
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
    # ── TIER 1: Root lookup / config tables (no FK deps) ─────────────────────

    # Settings → Core
    r"Ignite\Settings\Entities\Regions":                     {"v3": r"App\Models\Region",                      "transform": "generic"},
    r"Ignite\Settings\Entities\Region":                      {"v3": r"App\Models\Region",                      "transform": "generic"},
    r"Ignite\Settings\Entities\Counties":                    {"v3": r"App\Models\County",                      "transform": "generic"},
    r"Ignite\Settings\Entities\County":                      {"v3": r"App\Models\County",                      "transform": "generic"},
    r"Ignite\Settings\Entities\Departments":                 {"v3": r"App\Models\Department",                  "transform": "generic"},
    r"Ignite\Settings\Entities\Department":                  {"v3": r"App\Models\Department",                  "transform": "generic"},
    # A V2 clinic is the hospital itself (facility_code, address, email...),
    # which V3 models as core-service's facilities, not inpatient's clinic.
    r"Ignite\Settings\Entities\Clinics":                     {"v3": r"App\Models\Facility",                    "transform": "settings_clinic"},
    r"Ignite\Settings\Entities\Clinic":                      {"v3": r"App\Models\Facility",                    "transform": "settings_clinic"},
    r"Ignite\Settings\Entities\Specialties":                 {"v3": r"App\Models\Specialty",                   "transform": "generic"},
    r"Ignite\Settings\Entities\Specialty":                   {"v3": r"App\Models\Specialty",                   "transform": "generic"},
    r"Ignite\Settings\Entities\AgeGroups":                   {"v3": r"App\Models\AgeGroup",                    "transform": "generic"},
    r"Ignite\Settings\Entities\AgeGroup":                    {"v3": r"App\Models\AgeGroup",                    "transform": "generic"},
    r"Ignite\Settings\Entities\DocumentTypes":               {"v3": r"App\Models\DocumentType",                "transform": "generic"},
    r"Ignite\Settings\Entities\DocumentType":                {"v3": r"App\Models\DocumentType",                "transform": "generic"},
    r"Ignite\Settings\Entities\Themes":                      {"v3": r"App\Models\Theme",                       "transform": "generic"},
    r"Ignite\Settings\Entities\Theme":                       {"v3": r"App\Models\Theme",                       "transform": "generic"},
    r"Ignite\Settings\Entities\PurposeOfVisits":             {"v3": r"App\Models\PurposeOfVisit",              "transform": "generic"},
    r"Ignite\Settings\Entities\PurposeOfVisit":              {"v3": r"App\Models\PurposeOfVisit",              "transform": "generic"},
    r"Ignite\Settings\Entities\DestinationTypes":            {"v3": r"App\Models\DestinationType",             "transform": "generic"},
    r"Ignite\Settings\Entities\DestinationType":             {"v3": r"App\Models\DestinationType",             "transform": "generic"},
    r"Ignite\Settings\Entities\ServiceDestinations":         {"v3": r"App\Models\ServiceDestination",          "transform": "generic"},
    r"Ignite\Settings\Entities\ServiceDestination":          {"v3": r"App\Models\ServiceDestination",          "transform": "generic"},
    r"Ignite\Settings\Entities\PartnerInstitutions":         {"v3": r"App\Models\PartnerInstitution",          "transform": "generic"},
    r"Ignite\Settings\Entities\PartnerInstitution":          {"v3": r"App\Models\PartnerInstitution",          "transform": "generic"},
    r"Ignite\Settings\Entities\PartnerStaff":                {"v3": r"App\Models\PartnerStaff",                "transform": "generic"},
    r"Ignite\Settings\Entities\EmployeeCategories":          {"v3": r"App\Models\EmployeeCategory",            "transform": "generic"},
    r"Ignite\Settings\Entities\EmployeeCategory":            {"v3": r"App\Models\EmployeeCategory",            "transform": "generic"},
    r"Ignite\Settings\Entities\CategoryFilters":             {"v3": r"App\Models\CategoryFilter",              "transform": "generic"},
    r"Ignite\Settings\Entities\CategoryFilter":              {"v3": r"App\Models\CategoryFilter",              "transform": "generic"},
    r"Ignite\Settings\Entities\ApprovalLevels":              {"v3": r"App\Models\ApprovalLevel",               "transform": "generic"},
    r"Ignite\Settings\Entities\ApprovalLevel":               {"v3": r"App\Models\ApprovalLevel",               "transform": "generic"},
    r"Ignite\Settings\Entities\TreatmentActions":            {"v3": r"App\Models\TreatmentAction",             "transform": "settings_treatment_action"},
    r"Ignite\Settings\Entities\TreatmentAction":             {"v3": r"App\Models\TreatmentAction",             "transform": "settings_treatment_action"},
    # Insurance chain: company → scheme → rebate
    r"Ignite\Settings\Entities\Insurances":                  {"v3": r"App\Models\InsuranceCompany",            "transform": "settings_insurance"},
    r"Ignite\Settings\Entities\Insurance":                   {"v3": r"App\Models\InsuranceCompany",            "transform": "settings_insurance"},
    r"Ignite\Settings\Entities\Schemes":                     {"v3": r"App\Models\InsuranceScheme",             "transform": "settings_scheme"},
    r"Ignite\Settings\Entities\Scheme":                      {"v3": r"App\Models\InsuranceScheme",             "transform": "settings_scheme"},
    r"Ignite\Settings\Entities\Rebates":                     {"v3": r"App\Models\Rebate",                      "transform": "settings_rebate"},
    r"Ignite\Settings\Entities\Rebate":                      {"v3": r"App\Models\Rebate",                      "transform": "settings_rebate"},

    # Users
    r"Ignite\Users\Entities\Users":                          {"v3": r"App\Models\User",                        "transform": "settings_user"},
    r"Ignite\Users\Entities\User":                           {"v3": r"App\Models\User",                        "transform": "settings_user"},

    # Evaluation → procedure chain: categories → procedures → sample_types
    r"Ignite\Evaluation\Entities\ProcedureCategories":       {"v3": r"App\Models\ProcedureCategory",           "transform": "eval_procedure_category"},
    r"Ignite\Evaluation\Entities\ProcedureCategory":         {"v3": r"App\Models\ProcedureCategory",           "transform": "eval_procedure_category"},
    r"Ignite\Evaluation\Entities\Procedures":                {"v3": r"App\Models\Procedure",                   "transform": "eval_procedure"},
    r"Ignite\Evaluation\Entities\Procedure":                 {"v3": r"App\Models\Procedure",                   "transform": "eval_procedure"},
    r"Ignite\Evaluation\Entities\SampleCollectionMethods":   {"v3": r"App\Models\SampleCollectionMethod",      "transform": "generic"},
    r"Ignite\Evaluation\Entities\SampleCollectionMethod":    {"v3": r"App\Models\SampleCollectionMethod",      "transform": "generic"},
    r"Ignite\Evaluation\Entities\SampleTypes":               {"v3": r"App\Models\SampleType",                  "transform": "eval_sample_type"},
    r"Ignite\Evaluation\Entities\SampleType":                {"v3": r"App\Models\SampleType",                  "transform": "eval_sample_type"},
    # Evaluation reference / lookup tables
    r"Ignite\Evaluation\Entities\DiagnosisCodes":            {"v3": r"App\Models\DiagnosisCode",               "transform": "generic"},
    r"Ignite\Evaluation\Entities\DiagnosisCode":             {"v3": r"App\Models\DiagnosisCode",               "transform": "generic"},
    # CriticalValues, TemplateLab: v3_extra_mappings (their procedure links need the id map)
    r"Ignite\Evaluation\Entities\Icd10Types":                {"v3": r"App\Models\Icd10Type",                   "transform": "generic"},
    r"Ignite\Evaluation\Entities\Icd10Type":                 {"v3": r"App\Models\Icd10Type",                   "transform": "generic"},
    r"Ignite\Evaluation\Entities\Icd10Categories":           {"v3": r"App\Models\Icd10Category",               "transform": "eval_icd10_range"},
    r"Ignite\Evaluation\Entities\Icd10Category":             {"v3": r"App\Models\Icd10Category",               "transform": "eval_icd10_range"},
    r"Ignite\Evaluation\Entities\Icd10Subcategories":        {"v3": r"App\Models\Icd10Subcategory",            "transform": "eval_icd10_range"},
    r"Ignite\Evaluation\Entities\Icd10Subcategory":          {"v3": r"App\Models\Icd10Subcategory",            "transform": "eval_icd10_range"},
    r"Ignite\Evaluation\Entities\BioReferenceRanges":        {"v3": r"App\Models\BioReferenceRange",           "transform": "generic"},
    r"Ignite\Evaluation\Entities\BioReferenceRange":         {"v3": r"App\Models\BioReferenceRange",           "transform": "generic"},
    r"Ignite\Evaluation\Entities\LabTestCategories":         {"v3": r"App\Models\LabTestCategory",             "transform": "generic"},
    r"Ignite\Evaluation\Entities\LabTestCategory":           {"v3": r"App\Models\LabTestCategory",             "transform": "generic"},
    r"Ignite\Evaluation\Entities\LabTestAdditives":          {"v3": r"App\Models\LabTestAdditive",             "transform": "generic"},
    r"Ignite\Evaluation\Entities\LabTestAdditive":           {"v3": r"App\Models\LabTestAdditive",             "transform": "generic"},
    r"Ignite\Evaluation\Entities\LabTestUnits":              {"v3": r"App\Models\LabTestUnit",                 "transform": "generic"},
    r"Ignite\Evaluation\Entities\LabTestUnit":               {"v3": r"App\Models\LabTestUnit",                 "transform": "generic"},
    r"Ignite\Evaluation\Entities\EvaluationFormulae":        {"v3": r"App\Models\EvaluationFormula",           "transform": "generic"},
    r"Ignite\Evaluation\Entities\EvaluationFormula":         {"v3": r"App\Models\EvaluationFormula",           "transform": "generic"},
    r"Ignite\Evaluation\Entities\EvaluationMachines":        {"v3": r"App\Models\EvaluationMachine",          "transform": "generic"},
    r"Ignite\Evaluation\Entities\EvaluationMachine":         {"v3": r"App\Models\EvaluationMachine",          "transform": "generic"},
    r"Ignite\Evaluation\Entities\PrescriptionFrequencies":   {"v3": r"App\Models\PrescriptionFrequency",       "transform": "generic"},
    r"Ignite\Evaluation\Entities\PrescriptionFrequency":     {"v3": r"App\Models\PrescriptionFrequency",       "transform": "generic"},
    r"Ignite\Evaluation\Entities\PrescriptionMeasures":      {"v3": r"App\Models\PrescriptionMeasure",         "transform": "generic"},
    r"Ignite\Evaluation\Entities\PrescriptionMeasure":       {"v3": r"App\Models\PrescriptionMeasure",         "transform": "generic"},
    r"Ignite\Evaluation\Entities\PrescriptionRoutes":        {"v3": r"App\Models\PrescriptionRoute",           "transform": "generic"},
    r"Ignite\Evaluation\Entities\PrescriptionRoute":         {"v3": r"App\Models\PrescriptionRoute",           "transform": "generic"},
    r"Ignite\Evaluation\Entities\Formulations":              {"v3": r"App\Models\Formulation",                 "transform": "generic"},
    r"Ignite\Evaluation\Entities\Formulation":               {"v3": r"App\Models\Formulation",                 "transform": "generic"},
    r"Ignite\Evaluation\Entities\ProcedureCategoryTemplates": {"v3": r"App\Models\ProcedureCategoryTemplate",  "transform": "generic"},
    r"Ignite\Evaluation\Entities\ProcedureCategoryTemplate": {"v3": r"App\Models\ProcedureCategoryTemplate",   "transform": "generic"},
    r"Ignite\Evaluation\Entities\ProcedureTemplates":        {"v3": r"App\Models\ProcedureTemplate",           "transform": "generic"},
    r"Ignite\Evaluation\Entities\ProcedureTemplate":         {"v3": r"App\Models\ProcedureTemplate",           "transform": "generic"},

    # Finance config (must precede transactional finance)
    r"Ignite\Finance\Entities\Banks":                        {"v3": r"App\Models\Bank",                        "transform": "generic"},
    r"Ignite\Finance\Entities\Bank":                         {"v3": r"App\Models\Bank",                        "transform": "generic"},
    r"Ignite\Finance\Entities\PaymentModes":                 {"v3": r"App\Models\PaymentMode",                 "transform": "generic"},
    r"Ignite\Finance\Entities\PaymentMode":                  {"v3": r"App\Models\PaymentMode",                 "transform": "generic"},
    r"Ignite\Finance\Entities\PaymentTerms":                 {"v3": r"App\Models\PaymentTerm",                 "transform": "generic"},
    r"Ignite\Finance\Entities\PaymentTerm":                  {"v3": r"App\Models\PaymentTerm",                 "transform": "generic"},
    r"Ignite\Finance\Entities\TaxCategories":                {"v3": r"App\Models\TaxCategory",                 "transform": "generic"},
    r"Ignite\Finance\Entities\TaxCategory":                  {"v3": r"App\Models\TaxCategory",                 "transform": "generic"},
    r"Ignite\Finance\Entities\GlAccountTypes":               {"v3": r"App\Models\GlAccountType",               "transform": "generic"},
    r"Ignite\Finance\Entities\GlAccountType":                {"v3": r"App\Models\GlAccountType",               "transform": "generic"},
    r"Ignite\Finance\Entities\GlAccountGroups":              {"v3": r"App\Models\GlAccountGroup",              "transform": "finance_gl_account_group"},
    r"Ignite\Finance\Entities\GlAccountGroup":               {"v3": r"App\Models\GlAccountGroup",              "transform": "finance_gl_account_group"},
    r"Ignite\Finance\Entities\Charges":                      {"v3": r"App\Models\Charge",                      "transform": "generic"},
    r"Ignite\Finance\Entities\Charge":                       {"v3": r"App\Models\Charge",                      "transform": "generic"},

    # Inventory config: units → categories (self-ref) → suppliers → stores
    r"Ignite\Inventory\Entities\Units":                      {"v3": r"App\Models\Unit",                        "transform": "inventory_unit"},
    r"Ignite\Inventory\Entities\Unit":                       {"v3": r"App\Models\Unit",                        "transform": "inventory_unit"},
    r"Ignite\Inventory\Entities\Categories":                 {"v3": r"App\Models\ProductCategory",             "transform": "inventory_category"},
    r"Ignite\Inventory\Entities\Category":                   {"v3": r"App\Models\ProductCategory",             "transform": "inventory_category"},
    r"Ignite\Inventory\Entities\Suppliers":                  {"v3": r"App\Models\Supplier",                    "transform": "generic"},
    r"Ignite\Inventory\Entities\Supplier":                   {"v3": r"App\Models\Supplier",                    "transform": "generic"},
    r"Ignite\Inventory\Entities\Stores":                     {"v3": r"App\Models\Store",                       "transform": "inventory_store"},
    r"Ignite\Inventory\Entities\Store":                      {"v3": r"App\Models\Store",                       "transform": "inventory_store"},

    # Theatre config
    r"Ignite\Theatre\Entities\TheatreTypes":                 {"v3": r"App\Models\TheatreType",                 "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatreType":                  {"v3": r"App\Models\TheatreType",                 "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatreMedicTypes":            {"v3": r"App\Models\TheatreMedicType",            "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatreMedicType":             {"v3": r"App\Models\TheatreMedicType",            "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatrePaymentTypes":          {"v3": r"App\Models\TheatrePaymentType",          "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatrePaymentType":           {"v3": r"App\Models\TheatrePaymentType",          "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatreSchedulingStatuses":    {"v3": r"App\Models\TheatreSchedulingStatus",     "transform": "generic"},
    r"Ignite\Theatre\Entities\TheatreSchedulingStatus":      {"v3": r"App\Models\TheatreSchedulingStatus",     "transform": "generic"},

    # Inpatient config (must precede wards → beds → admissions chain)
    r"Ignite\Inpatient\Entities\BedTypes":                   {"v3": r"App\Models\BedType",                     "transform": "generic"},
    r"Ignite\Inpatient\Entities\BedType":                    {"v3": r"App\Models\BedType",                     "transform": "generic"},
    r"Ignite\Inpatient\Entities\AdmissionTypes":             {"v3": r"App\Models\AdmissionType",               "transform": "inpatient_admission_type"},
    r"Ignite\Inpatient\Entities\AdmissionType":              {"v3": r"App\Models\AdmissionType",               "transform": "inpatient_admission_type"},
    r"Ignite\Inpatient\Entities\DischargeTypes":             {"v3": r"App\Models\DischargeType",               "transform": "inpatient_discharge_type"},
    r"Ignite\Inpatient\Entities\DischargeType":              {"v3": r"App\Models\DischargeType",               "transform": "inpatient_discharge_type"},

    # ── TIER 2: Reception — patients root, then all children ─────────────────
    r"Ignite\Reception\Entities\Customers":                  {"v3": r"App\Models\Customer",                    "transform": "generic"},
    r"Ignite\Reception\Entities\Customer":                   {"v3": r"App\Models\Customer",                    "transform": "generic"},
    r"Ignite\Reception\Entities\Patients":                   {"v3": r"App\Models\Patient",                     "transform": "reception_patient"},
    r"Ignite\Reception\Entities\Patient":                    {"v3": r"App\Models\Patient",                     "transform": "reception_patient"},
    r"Ignite\Reception\Entities\AppointmentCategories":      {"v3": r"App\Models\AppointmentCategory",         "transform": "generic"},
    r"Ignite\Reception\Entities\AppointmentCategory":        {"v3": r"App\Models\AppointmentCategory",         "transform": "generic"},
    r"Ignite\Reception\Entities\Appointments":               {"v3": r"App\Models\Appointment",                 "transform": "reception_appointment"},
    r"Ignite\Reception\Entities\Appointment":                {"v3": r"App\Models\Appointment",                 "transform": "reception_appointment"},
    # V2 visits live in the Evaluation module, not Reception — every
    # Ignite\Reception\Entities\Visit* variant 404s "Model class not found"
    # on the data point API, so the visits job never extracted a single row.
    r"Ignite\Evaluation\Entities\Visit":                     {"v3": r"App\Models\Visit",                       "transform": "reception_visit"},
    r"Ignite\Evaluation\Entities\Visits":                    {"v3": r"App\Models\Visit",                       "transform": "reception_visit"},
    r"Ignite\Reception\Entities\PatientSchemes":             {"v3": r"App\Models\PatientInsurance",            "transform": "reception_patient_scheme"},
    r"Ignite\Reception\Entities\PatientScheme":              {"v3": r"App\Models\PatientInsurance",            "transform": "reception_patient_scheme"},
    # V2's actual class for next of kin (reception_patients_nok)
    r"Ignite\Reception\Entities\NextOfKin":                     {"v3": r"App\Models\PatientNextOfKin",               "transform": "reception_patient_nok"},
    r"Ignite\Reception\Entities\PatientNextOfKins":          {"v3": r"App\Models\PatientNextOfKin",            "transform": "generic"},
    r"Ignite\Reception\Entities\PatientNextOfKin":           {"v3": r"App\Models\PatientNextOfKin",            "transform": "generic"},
    r"Ignite\Reception\Entities\PatientDocuments":           {"v3": r"App\Models\PatientDocument",             "transform": "reception_patient_document"},
    r"Ignite\Reception\Entities\PatientDocument":            {"v3": r"App\Models\PatientDocument",             "transform": "reception_patient_document"},
    r"Ignite\Reception\Entities\PatientDependants":          {"v3": r"App\Models\PatientDependant",            "transform": "reception_patient_dependant"},
    r"Ignite\Reception\Entities\PatientDependant":           {"v3": r"App\Models\PatientDependant",            "transform": "reception_patient_dependant"},
    r"Ignite\Reception\Entities\PatientGuarantors":          {"v3": r"App\Models\PatientGuarantor",            "transform": "generic"},
    r"Ignite\Reception\Entities\PatientGuarantor":           {"v3": r"App\Models\PatientGuarantor",            "transform": "generic"},
    r"Ignite\Reception\Entities\PatientFollowups":           {"v3": r"App\Models\PatientFollowup",             "transform": "generic"},
    r"Ignite\Reception\Entities\PatientFollowup":            {"v3": r"App\Models\PatientFollowup",             "transform": "generic"},
    r"Ignite\Reception\Entities\PatientSamples":             {"v3": r"App\Models\PatientSample",               "transform": "generic"},
    r"Ignite\Reception\Entities\PatientSample":              {"v3": r"App\Models\PatientSample",               "transform": "generic"},
    r"Ignite\Reception\Entities\PatientRandomNotes":         {"v3": r"App\Models\PatientRandomNote",           "transform": "generic"},
    r"Ignite\Reception\Entities\PatientRandomNote":          {"v3": r"App\Models\PatientRandomNote",           "transform": "generic"},
    r"Ignite\Reception\Entities\PatientConsents":            {"v3": r"App\Models\PatientConsent",              "transform": "generic"},
    r"Ignite\Reception\Entities\PatientConsent":             {"v3": r"App\Models\PatientConsent",              "transform": "generic"},
    r"Ignite\Reception\Entities\MorgueAdmissions":           {"v3": r"App\Models\MorgueAdmission",             "transform": "generic"},
    r"Ignite\Reception\Entities\MorgueAdmission":            {"v3": r"App\Models\MorgueAdmission",             "transform": "generic"},
    # V2 actually keeps visit destinations under Evaluation (evaluation_visit_destinations)
    r"Ignite\Evaluation\Entities\VisitDestinations":         {"v3": r"App\Models\VisitDestination",            "transform": "evaluation_visit_destination"},
    r"Ignite\Evaluation\Entities\VisitDestination":          {"v3": r"App\Models\VisitDestination",            "transform": "evaluation_visit_destination"},
    r"Ignite\Reception\Entities\VisitDestinations":          {"v3": r"App\Models\VisitDestination",            "transform": "generic"},
    r"Ignite\Reception\Entities\VisitDestination":           {"v3": r"App\Models\VisitDestination",            "transform": "generic"},
    r"Ignite\Reception\Entities\VisitConsultants":           {"v3": r"App\Models\VisitConsultant",             "transform": "generic"},
    r"Ignite\Reception\Entities\VisitConsultant":            {"v3": r"App\Models\VisitConsultant",             "transform": "generic"},
    r"Ignite\Reception\Entities\VisitPrecharges":            {"v3": r"App\Models\VisitPrecharge",              "transform": "generic"},
    r"Ignite\Reception\Entities\VisitPrecharge":             {"v3": r"App\Models\VisitPrecharge",              "transform": "generic"},
    r"Ignite\Reception\Entities\Queues":                     {"v3": r"App\Models\Queue",                       "transform": "generic"},
    r"Ignite\Reception\Entities\Queue":                      {"v3": r"App\Models\Queue",                       "transform": "generic"},
    r"Ignite\Reception\Entities\Referrals":                  {"v3": r"App\Models\Referral",                    "transform": "generic"},
    r"Ignite\Reception\Entities\Referral":                   {"v3": r"App\Models\Referral",                    "transform": "generic"},

    # Inventory transactional (after config)
    # V2 stock ledger (one row per stock change) → V3 inventory stock movements
    r"Ignite\Inventory\Entities\StockMovement":              {"v3": r"App\Models\StockMovement",               "transform": "inventory_stock_movement"},
    r"Ignite\Inventory\Entities\StockMovements":             {"v3": r"App\Models\StockMovement",               "transform": "inventory_stock_movement"},
    r"Ignite\Inventory\Entities\Products":                   {"v3": r"App\Models\Product",                     "transform": "inventory_product"},
    r"Ignite\Inventory\Entities\Product":                    {"v3": r"App\Models\Product",                     "transform": "inventory_product"},
    r"Ignite\Inventory\Entities\BatchPurchases":             {"v3": r"App\Models\Batch",                       "transform": "inventory_batch"},
    r"Ignite\Inventory\Entities\BatchPurchase":              {"v3": r"App\Models\Batch",                       "transform": "inventory_batch"},
    r"Ignite\Inventory\Entities\PurchaseOrders":             {"v3": r"App\Models\PurchaseOrder",               "transform": "inventory_purchase_order"},
    r"Ignite\Inventory\Entities\PurchaseOrder":              {"v3": r"App\Models\PurchaseOrder",               "transform": "inventory_purchase_order"},
    r"Ignite\Inventory\Entities\Requisitions":               {"v3": r"App\Models\Requisition",                 "transform": "inventory_requisition"},
    r"Ignite\Inventory\Entities\Requisition":                {"v3": r"App\Models\Requisition",                 "transform": "inventory_requisition"},
    r"Ignite\Inventory\Entities\GoodsReceived":              {"v3": r"App\Models\GoodsReceivedNote",           "transform": "inventory_grn"},

    # Theatre transactional: types → theatres → bookings → schedules → operations
    r"Ignite\Theatre\Entities\Theatres":                     {"v3": r"App\Models\Theatre",                     "transform": "theatre_theatre"},
    r"Ignite\Theatre\Entities\Theatre":                      {"v3": r"App\Models\Theatre",                     "transform": "theatre_theatre"},
    r"Ignite\Theatre\Entities\TheatreBookings":              {"v3": r"App\Models\TheatreBooking",              "transform": "theatre_booking"},
    r"Ignite\Theatre\Entities\TheatreBooking":               {"v3": r"App\Models\TheatreBooking",              "transform": "theatre_booking"},
    r"Ignite\Theatre\Entities\TheatreSchedules":             {"v3": r"App\Models\TheatreSchedule",             "transform": "theatre_schedule"},
    r"Ignite\Theatre\Entities\TheatreSchedule":              {"v3": r"App\Models\TheatreSchedule",             "transform": "theatre_schedule"},
    r"Ignite\Theatre\Entities\TheatreOperations":            {"v3": r"App\Models\TheatreOperation",            "transform": "theatre_operation"},
    r"Ignite\Theatre\Entities\TheatreOperation":             {"v3": r"App\Models\TheatreOperation",            "transform": "theatre_operation"},

    # ── TIER 3: Inpatient transactional ──────────────────────────────────────
    # Order: wards → beds → admission_requests → admissions
    # (vitals/notes/discharges in Tier 5 — all need admission_id)
    r"Ignite\Inpatient\Entities\Wards":                      {"v3": r"App\Models\Ward",                        "transform": "generic"},
    r"Ignite\Inpatient\Entities\Ward":                       {"v3": r"App\Models\Ward",                        "transform": "generic"},
    r"Ignite\Inpatient\Entities\Beds":                       {"v3": r"App\Models\Bed",                         "transform": "inpatient_bed"},
    r"Ignite\Inpatient\Entities\Bed":                        {"v3": r"App\Models\Bed",                         "transform": "inpatient_bed"},
    r"Ignite\Inpatient\Entities\WardCharges":                {"v3": r"App\Models\WardCharge",                  "transform": "generic"},
    r"Ignite\Inpatient\Entities\WardCharge":                 {"v3": r"App\Models\WardCharge",                  "transform": "generic"},
    r"Ignite\Inpatient\Entities\AdmissionRequests":          {"v3": r"App\Models\AdmissionRequest",            "transform": "inpatient_admission_request"},
    r"Ignite\Inpatient\Entities\AdmissionRequest":           {"v3": r"App\Models\AdmissionRequest",            "transform": "inpatient_admission_request"},
    r"Ignite\Inpatient\Entities\Admissions":                 {"v3": r"App\Models\Admission",                   "transform": "inpatient_admission"},
    r"Ignite\Inpatient\Entities\Admission":                  {"v3": r"App\Models\Admission",                   "transform": "inpatient_admission"},

    # ── TIER 4: Evaluation clinical (visits must exist) ───────────────────────
    r"Ignite\Evaluation\Entities\DoctorNotes":               {"v3": r"App\Models\DoctorNote",                  "transform": "evaluation_doctor_note"},
    r"Ignite\Evaluation\Entities\DoctorNote":                {"v3": r"App\Models\DoctorNote",                  "transform": "evaluation_doctor_note"},
    r"Ignite\Evaluation\Entities\Prescriptions":             {"v3": r"App\Models\Prescription",                "transform": "evaluation_prescription"},
    r"Ignite\Evaluation\Entities\Prescription":              {"v3": r"App\Models\Prescription",                "transform": "evaluation_prescription"},
    r"Ignite\Evaluation\Entities\ExaminationReviews":        {"v3": r"App\Models\ExaminationReview",           "transform": "evaluation_examination_review"},
    r"Ignite\Evaluation\Entities\ExaminationReview":         {"v3": r"App\Models\ExaminationReview",           "transform": "evaluation_examination_review"},
    r"Ignite\Evaluation\Entities\EyeExams":                  {"v3": r"App\Models\EyeExam",                     "transform": "evaluation_eye_exam"},
    r"Ignite\Evaluation\Entities\EyeExam":                   {"v3": r"App\Models\EyeExam",                     "transform": "evaluation_eye_exam"},
    # Investigations before results (result.investigation_id → investigations.id)
    r"Ignite\Evaluation\Entities\Investigations":            {"v3": r"App\Models\Investigation",               "transform": "evaluation_investigation"},
    r"Ignite\Evaluation\Entities\Investigation":             {"v3": r"App\Models\Investigation",               "transform": "evaluation_investigation"},
    r"Ignite\Evaluation\Entities\InvestigationResults":      {"v3": r"App\Models\InvestigationResult",         "transform": "evaluation_inv_result"},
    r"Ignite\Evaluation\Entities\InvestigationResult":       {"v3": r"App\Models\InvestigationResult",         "transform": "evaluation_inv_result"},
    # evaluation-service `samples` (lab samples: patient_id, visit_id, type …)
    r"Ignite\Evaluation\Entities\Samples":                   {"v3": r"App\Models\Sample",                      "transform": "evaluation_sample"},
    r"Ignite\Evaluation\Entities\Sample":                    {"v3": r"App\Models\Sample",                      "transform": "evaluation_sample"},
    # inp_discharge_requests.admission_id → inp_admissions.id (tier 3). Kept
    # in tier 4 so they land before the discharges that point at them (tier 5).
    r"Ignite\Inpatient\Entities\DischargeRequests":          {"v3": r"App\Models\DischargeRequest",            "transform": "inpatient_discharge_request"},
    r"Ignite\Inpatient\Entities\DischargeRequest":           {"v3": r"App\Models\DischargeRequest",            "transform": "inpatient_discharge_request"},
    # A V2 dispensing (pharmacy issue of a prescription: amount, payment
    # status) → inventory-service sale, V3's dispensing/POS record (since the
    # 2026-09-02 migrations it carries patient_id + payment_status). Needs
    # stores (tier 1) and patients/visits (tier 2) first.
    r"Ignite\Evaluation\Entities\Dispensing":                {"v3": r"App\Models\Sale",                        "transform": "inventory_dispensing"},

    # ── TIER 5: Inpatient vitals / notes (admissions must exist first) ────────
    # inp_vitals.admission_id → inp_admissions.id
    r"Ignite\Evaluation\Entities\Vitals":                    {"v3": r"App\Models\Vital",                       "transform": "inpatient_vital"},
    r"Ignite\Evaluation\Entities\Vital":                     {"v3": r"App\Models\Vital",                       "transform": "inpatient_vital"},
    # inp_discharges.discharge_request_id → inp_discharge_requests.id (tier 4)
    r"Ignite\Inpatient\Entities\Discharges":                 {"v3": r"App\Models\Discharge",                   "transform": "inpatient_discharge"},
    r"Ignite\Inpatient\Entities\Discharge":                  {"v3": r"App\Models\Discharge",                   "transform": "inpatient_discharge"},

    # ── TIER 6: Finance transactional (patients + insurance must exist) ───────
    r"Ignite\Finance\Entities\PatientAccounts":              {"v3": r"App\Models\PatientAccount",              "transform": "generic"},
    r"Ignite\Finance\Entities\PatientAccount":               {"v3": r"App\Models\PatientAccount",              "transform": "generic"},
    r"Ignite\Finance\Entities\Invoices":                     {"v3": r"App\Models\Invoice",                     "transform": "finance_invoice"},
    r"Ignite\Finance\Entities\Invoice":                      {"v3": r"App\Models\Invoice",                     "transform": "finance_invoice"},
    r"Ignite\Finance\Entities\Waivers":                      {"v3": r"App\Models\Waiver",                      "transform": "generic"},
    r"Ignite\Finance\Entities\Waiver":                       {"v3": r"App\Models\Waiver",                      "transform": "generic"},
    r"Ignite\Finance\Entities\Copays":                       {"v3": r"App\Models\Copay",                       "transform": "finance_copay"},
    r"Ignite\Finance\Entities\Copay":                        {"v3": r"App\Models\Copay",                       "transform": "finance_copay"},
    # V2 evaluation payments are the receipts (receipt no, amount, date, cashier);
    # V3 has no EvaluationPayment model — they go to finance-service receipts
    r"Ignite\Finance\Entities\EvaluationPayments":           {"v3": r"App\Models\Receipt",                     "transform": "finance_receipt"},
    r"Ignite\Finance\Entities\EvaluationPayment":            {"v3": r"App\Models\Receipt",                     "transform": "finance_receipt"},
    r"Ignite\Finance\Entities\InvoicePayments":              {"v3": r"App\Models\InvoicePayment",              "transform": "generic"},
    r"Ignite\Finance\Entities\InvoicePayment":               {"v3": r"App\Models\InvoicePayment",              "transform": "generic"},
    r"Ignite\Finance\Entities\Vouchers":                     {"v3": r"App\Models\Voucher",                     "transform": "finance_voucher"},
    r"Ignite\Finance\Entities\Voucher":                      {"v3": r"App\Models\Voucher",                     "transform": "finance_voucher"},
    r"Ignite\Finance\Entities\PatientDeposits":              {"v3": r"App\Models\PatientDeposit",              "transform": "generic"},
    r"Ignite\Finance\Entities\PatientDeposit":               {"v3": r"App\Models\PatientDeposit",              "transform": "generic"},
    r"Ignite\Finance\Entities\PatientWithdrawals":           {"v3": r"App\Models\PatientWithdrawal",           "transform": "generic"},
    r"Ignite\Finance\Entities\PatientWithdrawal":            {"v3": r"App\Models\PatientWithdrawal",           "transform": "generic"},
    r"Ignite\Finance\Entities\PettyCash":                    {"v3": r"App\Models\PettyCash",                   "transform": "generic"},
    r"Ignite\Finance\Entities\Dispatches":                   {"v3": r"App\Models\Dispatch",                    "transform": "generic"},
    r"Ignite\Finance\Entities\Dispatch":                     {"v3": r"App\Models\Dispatch",                    "transform": "generic"},
}

# Tier boundary — the first namespace that opens each tier.
# Jobs are dispatched tier-by-tier (all of tier N must finish before tier N+1 starts)
# so that FK parents are always present in _id_map before children are posted.
# Within a tier, jobs run in parallel up to --workers.
_TIER_BOUNDARIES: list[str] = [
    r"Ignite\Reception\Entities\Customers",     # Tier 2: reception / patients
    r"Ignite\Inpatient\Entities\Wards",         # Tier 3: inpatient transactional
    r"Ignite\Evaluation\Entities\DoctorNotes",  # Tier 4: evaluation clinical
    r"Ignite\Evaluation\Entities\Vitals",       # Tier 5: vitals (needs admissions)
    r"Ignite\Finance\Entities\PatientAccounts", # Tier 6: finance transactional
]


def _namespace_tier(ns: str) -> int:
    """Return the tier number (1-6) for a namespace based on _TIER_BOUNDARIES."""
    ns_keys = list(NAMESPACE_MAP.keys())
    try:
        ns_pos = ns_keys.index(ns)
    except ValueError:
        return 1
    tier = 1
    for boundary in _TIER_BOUNDARIES:
        try:
            if ns_keys.index(boundary) <= ns_pos:
                tier += 1
        except ValueError:
            pass
    return tier


# ─── FIELD TRANSFORMS ────────────────────────────────────────────────────────

# Layer 0: values inside an embedded relation blob worth keeping — lifted to a
# flat field (only if that field is empty) before step 0 strips the blob.
_PER_KEY_LIFT_NESTED: dict[str, dict[str, tuple[str, str]]] = {
    # the CLEAN view carries the destination as an object ({"id": 9, "name":
    # "Pharmacy", …}) rather than a flattened destination_name
    "evaluation_visit_destination": {"destination_name": ("destination", "name"),
                                     "destination_slug": ("destination", "slug")},
}

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
    "evaluation_investigation": {},
    "evaluation_visit_destination": {
        # V2's department is empty on every migrated row; the destination's
        # name ("Pharmacy", "Laboratory" …) is what's filled
        "destination_name": "department_name",
        "finish_at":        "completed_at",
    },
    "reception_patient_nok": {},
    # V2 ICD-10 categories/subcategories are ranges (code_from..code_to);
    # V3 needs a code (see _PER_KEY_INJECT)
    "eval_icd10_range": {},
    # the dependant's details are joined in as dependant_* (v3_extra_mappings.FETCH_SQL)
    "reception_patient_dependant": {},
    # V2's group → type link is a bare `type` (1 Profit & Loss, 2 Balance Sheet …)
    "finance_gl_account_group": {"type": "account_type_id"},
    "inventory_unit": {},
    "evaluation_examination_review": {},
    # V2's signed quantity / store / action / type are re-derived (see _movement_*)
    "inventory_stock_movement": {
        "quantity": "v2_quantity", "store_id": "v2_store_id", "action": "v2_action", "type": "v2_type",
        "opening": "opening_balance", "closing": "closing_balance",
    },
    "reception_patient_document": {
        "filename": "file_name",
        "document": "file_path",
        "mime":     "file_type",
    },
    "settings_clinic": {
        "telephone":     "phone",
        "town":          "city",
        "facility_code": "code",
    },
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
    # V2 evaluation payments → finance receipts (after the global user → user_id …)
    "finance_receipt": {
        "receipt": "receipt_no",       # V2's full receipt number, e.g. COLCOL220425132629 (unique)
        "user_id": "issued_by",        # the cashier
        "status":  "v2_status",        # credit-note states, kept in notes
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
        "approved":      "is_approved",
        # V2 names the parent link `investigation`; the required-field check
        # and the FK remap both expect investigation_id
        "investigation": "investigation_id",
    },
    "evaluation_eye_exam": {
        "visit":    "visit_id",
        "user":     "user_id",
        "comments": "notes",
        "od":       "right_eye_data",
        "os":       "left_eye_data",
    },
    "inventory_product": {
        # V3's inventory_products table names the column generic_name (the
        # gateway's describe still says `name`, but posting `name` 500s)
        "name":      "generic_name",
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
        "user_id":       "created_by",      # after the global user → user_id rename
    },
    "inventory_requisition": {
        "requestor_id":     "requested_by",
        "approver_id":      "approved_by",
        "approval_weight":  "approval_level",
        "store_id":         "requesting_store_id",    # NOT NULL + FK in V3
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
        "emergency":           "is_emergency",
        "operation_procedure": "operation_procedure_id",    # a V2 procedure id
        "proc_cash_price":     "procedure_cash_price",
        "proc_ins_price":      "procedure_insurance_price",
    },
    "theatre_schedule":  {},
    "theatre_operation": {},
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
    # V3 procedures names the category FK plain `category` (the global FK
    # rename turned V2's `category` into category_id)
    "eval_procedure": {"category_id": "category", "is_active": "active",
                       "departmentcode": "departmentCode"},
    "eval_sample_type": {},
    "settings_rebate": {},
    # V2 vitals → inpatient-service inp_vitals (admitted visits). admission_id
    # is injected from the visit (V2 vitals carry only `visit`) and remapped
    # via _FK_REMAP. user → user_id (global) → recorded_by.
    "inpatient_vital": {
        "pulse":        "pulse_rate",
        "respiration":  "respiratory_rate",
        "bp_systolic":  "systolic_bp",
        "bp_diastolic": "diastolic_bp",
        "oxygen":       "oxygen_saturation",
        "nurse_notes":  "notes",
        "user_id":      "recorded_by",
    },
    # V2 vitals → evaluation-service vitals (outpatient visits): its columns
    # mostly carry V2's own names already (2026-02-03 models.md migration).
    "outpatient_vital": {
        "user_id": "recorded_by",
    },
    "inpatient_bed": {},                 # ward_id / bed_type_id FK remapped via _FK_REMAP
    "inpatient_admission": {},           # admission_type_id FK remapped via _FK_REMAP
    "inpatient_admission_type": {},
    # preferred_ward_id / preferred_bed_type_id via _FK_REMAP
    "inpatient_admission_request": {
        "user_id": "requested_by",              # after the global user → user_id rename
        "reason":  "reason_for_admission",      # NOT NULL text in V3
    },
    "evaluation_prescription": {},       # prescribed_by dropped+reinjected as V3 user id below
    "evaluation_sample": {},             # V2 Sample columns match V3 samples' fillable 1:1
    "inpatient_discharge_type": {},
    "settings_treatment_action": {},
    # V2 discharge requests are the clinical discharge summary; the sections
    # with a V3 column of their own are renamed here, the rest are folded
    # into discharge_notes (_PER_KEY_INJECT) so nothing is lost.
    "inpatient_discharge_request": {
        "principal":    "reason",                  # principal diagnosis
        "conditions":   "discharge_summary",       # condition on discharge
        "treatment":    "medications_prescribed",
        "tca":          "follow_up_instructions",  # "to come again"
        "user_id":      "requested_by",
        "finalized_by": "reviewed_by",
    },
    "inpatient_discharge": {
        "doctor_id": "discharged_by",
    },
    "inventory_store": {},
    "inventory_dispensing": {
        "user_id": "served_by",   # after the global user → user_id rename
    },
    # username stays username (NOT NULL, unique per org in V3). It used to be
    # renamed to email — which, with email already set, dropped it.
    "settings_user": {},
}

# Fields to drop entirely per transform key (cannot be mapped in API-to-API)
_PER_KEY_DROP_FIELDS: dict[str, list] = {
    "settings_insurance":      ["manager_id", "customer_number"],
    "settings_scheme":         ["companies", "type_name", "full_name", "disabled"],
    "eval_procedure_category": ["procedures"],   # nested relation array
    "settings_user":           ["password", "remember_token", "api_token", "roles", "permissions", "abilities"],
    "evaluation_doctor_note":  ["nutrition_and_diatetics", "mohDiagnosis"],  # V2-only columns, not in V3 schema
    # V2's "prescribed_by" is a free-text name ("Dr. Christine Atolo") but V3's
    # column of the same name is an integer user id — drop the string here so
    # _PER_KEY_INJECT can populate the real id from "user_id" instead. Do NOT
    # drop "user_id" here — the injection below still needs to read it; it's
    # left in the payload afterwards and self-heals via the unknown-column
    # auto-strip-and-retry path, same as the nested "users"/"payment" blobs.
    "evaluation_prescription": ["prescribed_by"],
}

# Fields that are encrypted at rest in V2 and — confirmed 2026-09/2026-10 by
# hitting staging.collabmed.net directly — sometimes come back from the V2
# API as mojibake (U+FFFD replacement characters mixed with raw bytes): V2
# itself serializes undecrypted/mis-decrypted ciphertext as if it were UTF-8
# text. The original bytes are unrecoverable once that happens — there is no
# client-side fix, only a V2-backend one.
#
# Unlike _PER_KEY_DROP_FIELDS (unconditional), these are only replaced when a
# value is actually visibly corrupted — see _PER_KEY_CORRUPTION_WATCH's use
# in transform_record's step 4c (hash it, don't null it — see
# _hash_corrupted_value). A record with a clean value for one of these
# fields keeps it; only the broken ones get hashed, and every replacement is
# can be identified and backfilled once V2 fixes the decryption.
# first_name/last_name are the SAME kind of corrupted ciphertext-as-text and
# ARE included here even though isolated A/B testing confirmed V3 requires
# them non-null (dropping either one alone causes an opaque 500) — that
# constraint only rules out dropping them, not hashing them. A hash is a
# non-null string just like the raw garbled text was, and confirmed by
# direct testing to be accepted the same way.
# Per-key column whitelist, applied last: everything else is dropped. For
# models where V2 carries many columns the V3 table doesn't have (unknown
# columns make the gateway 500).
_PER_KEY_KEEP_ONLY: dict[str, set[str]] = {
    # region_id is required by core_facilities (passed through as V2's value)
    "settings_clinic": {"name", "code", "type", "address", "city", "phone", "email",
                        "status", "region_id", "created_at", "updated_at"},
    # columns of reception-service's patient documents (gateway describe) + the FK
    # finance-service Receipt::$fillable that V2 can fill
    "finance_receipt": {"receipt_no", "receipt_type", "invoice_id", "patient_id", "amount", "receipt_date",
                        "payment_method", "status", "notes", "issued_by"},
    # theatre-service Theatre::$fillable that V2 can fill. The theatre gateway
    # hides SQL errors ("the database refused the row"), so the poster can't
    # strip an unknown column (source_schema) — send only real columns.
    # associated_procedures is left out: V3 wants procedure ids, V2 holds text.
    "theatre_theatre": {"name", "number", "theatre_type_id", "facility_id", "description", "fee",
                        "charging_cycle", "is_active"},
    # theatre_bookings: the agreed insert payload (Muniu, 2026-10-12: uuid,
    # patient_id, preferred_date, status_id, requested_by, is_emergency,
    # reason, timestamps) + the columns whose V2 ids are remapped to V3.
    # Left out: package_id / requested_facility_id — V2 ids with no V3 mapping.
    "theatre_booking": {"visit_id", "patient_id", "preferred_date", "status_id", "requested_by", "is_emergency",
                        "reason", "theatre_deposit_id", "operation_procedure_id", "procedure_cash_price",
                        "procedure_insurance_price", "authorized_amount", "approved_by", "cancelled_by",
                        "rejected_by", "authorized_by", "cancellation_reason", "rejection_reason",
                        "created_at", "updated_at"},
    # inventory-service StockMovement::$fillable that V2 can fill
    # created_at / updated_at: when the movement happened (a ledger needs the V2 date)
    "inventory_stock_movement": {"product_id", "type", "direction", "from_store_id", "to_store_id", "quantity",
                                 "opening_balance", "closing_balance", "reason", "notes", "user_id",
                                 "idempotency_key", "created_at", "updated_at"},
    # reception-service PatientDependant::$fillable that V2 can fill
    "reception_patient_dependant": {"patient_id", "full_name", "relationship", "dob", "gender", "id_no",
                                    "mobile", "email"},
    "reception_patient_document": {"patient_id", "title", "document_type", "description", "file_name",
                                   "file_path", "file_type", "file_extension", "file_size", "document_date",
                                   "notes"},
    # V2 admissions arrive with the embedded doctor/ward/bed/bed_type records
    # flattened into ~90 extra columns (doctor_profile_mpdb, ward_cash_cost …)
    # that inp_admissions doesn't have. Keep the admission's own fields only.
    "inpatient_admission": {"patient_id", "visit_id", "ward_id", "bed_id", "admission_type_id",
                            "admission_request_id", "admitting_doctor_id", "admission_number",
                            "admission_date", "admitted_at", "discharged_at", "reason", "cost",
                            "days_admitted", "payment_mode", "external_doctor", "created_at", "updated_at"},
    # columns of reception-service's patient_next_of_kin (gateway describe) + the FK
    "reception_patient_nok": {"patient_id", "first_name", "middle_name", "last_name", "relationship",
                              "relationship_id", "id_no", "mobile", "alt_phone", "email", "address",
                              "city", "county", "is_primary", "is_emergency_contact"},
    # reception-service visit_destination columns we can fill reliably. Left
    # out: destination_id/department_id (V2 ids, their lookups aren't
    # migrated), status (V3's allowed values unknown), created_by (V2 users
    # don't exist in V3).
    # destination_id: V2's settings destination, remapped to V3's service destination (_FK_REMAP) —
    # it was left out, so every migrated row had none (reported 2026-10-15)
    "evaluation_visit_destination": {"visit_id", "department_id", "department_name", "arrived_at",
                                     "completed_at", "notes", "destination_id", "destination_slug"},
    # columns of inpatient-service's inp_admission_types (gateway describe)
    "inpatient_admission_type": {"code", "name", "description", "deposit", "associated_procedure"},
    # columns of evaluation-service's `procedures` table (gateway describe)
    "eval_procedure": {"name", "code", "category", "gender", "description", "sub_title", "td_spacing",
                       "use_bio_ref_flagging", "default_comment", "default_result", "active",
                       "has_quick_results", "sub_category_id", "departmentCode", "tag", "revenue_tag"},
    # evaluation-service App\Models\Sample::$fillable (minus organization_id,
    # set by the gateway). vtm_no is left out: V2's is empty on every row and
    # V3 generates one per org when it's missing.
    "evaluation_sample": {"patient_id", "visit_id", "type_id", "details", "user_id", "collection_method_id",
                          "collection_point", "queued", "queued_by", "queued_at", "collection_point_name",
                          "result", "region_id", "clinic_id", "request_type", "tobe_contacted", "status",
                          "received_on", "investigation_id", "procedure_id", "created_at", "updated_at"},
    # inpatient-service inp_discharge_types (gateway describe copy_safe)
    "inpatient_discharge_type": {"code", "name", "description"},
    # inpatient-service inp_discharge_requests (create + 2026-06/09 migrations)
    "inpatient_discharge_request": {"request_number", "admission_id", "visit_id", "discharge_type_id",
                                    "reason", "discharge_notes", "requested_by", "requested_at", "status",
                                    "discharge_summary", "follow_up_instructions", "medications_prescribed",
                                    "reviewed_by", "reviewed_at", "created_at", "updated_at"},
    # inpatient-service inp_vitals (create migration); V2 readings with no
    # column of their own go into additional_vitals (JSON)
    "inpatient_vital": {"admission_id", "recorded_at", "temperature", "pulse_rate", "respiratory_rate",
                        "blood_pressure", "systolic_bp", "diastolic_bp", "oxygen_saturation", "weight",
                        "height", "bmi", "additional_vitals", "notes", "recorded_by", "created_at", "updated_at"},
    # evaluation-service vitals (create + 2025-11-19 + 2026-02-03 migrations).
    # Left out: temperature_location(_id) — V3 enum / V2 option id don't line up.
    "outpatient_vital": {"visit_id", "patient_id", "recorded_at", "recorded_by", "temperature", "pulse",
                         "weight", "height", "bmi", "head_circumference", "muac", "random_blood_sugar",
                         "fasting_blood_sugar", "nurse_notes", "systolic_bp", "diastolic_bp", "bp_systolic",
                         "bp_diastolic", "blood_pressure", "respiratory_rate", "respiration",
                         "oxygen_saturation", "oxygen", "waist", "hip", "blood_sugar", "blood_sugar_units",
                         "symptoms", "allergies", "chronic_illnesses", "body_fat", "muscle_mass", "bone_mass",
                         "metabolic_age", "body_water", "visceral_fat", "current_medication",
                         "length_percentile", "weight_percentile", "bmi_percentile", "intracular_pressure",
                         "lmp", "uncorrected_near_vision", "corrected_near_vision", "created_at", "updated_at"},
    # inventory-service App\Models\Store::$fillable (minus organization_id /
    # facility_id, set by the gateway; department_id: V2 departments aren't
    # migrated)
    "inventory_store": {"name", "code", "description", "location", "type", "is_consumable", "parent_store_id",
                        "can_order_from_suppliers", "can_update_product_prices", "is_main_store",
                        "is_active", "open_time", "close_time"},
    # inventory-service App\Models\Sale::$fillable — the sale header only:
    # the gateway exposes no sale_item model, so the dispensed drug/quantity
    # stay on the (already migrated) prescription, referenced in notes.
    "inventory_dispensing": {"receipt_number", "store_id", "patient_id", "subtotal", "total_amount",
                             "status", "payment_status", "notes", "served_by", "created_at", "updated_at"},
    # inpatient-service inp_discharges (create migration)
    "inpatient_discharge": {"discharge_number", "admission_id", "discharge_request_id", "discharge_type_id",
                            "discharge_date", "discharged_by", "discharge_diagnosis", "discharge_summary",
                            "treatment_summary", "procedures_performed", "discharge_instructions",
                            "follow_up_instructions", "approved_by", "approved_at", "created_at", "updated_at"},
}

_PER_KEY_CORRUPTION_WATCH: dict[str, list] = {
    "reception_patient": ["id_no", "mobile", "email", "address",
                          "middle_name", "telephone", "alt_number",
                          "secondary_email", "first_name", "last_name"],
}

# Fields that must be prefixed with the facility name before posting because
# V3's uniqueness constraint on them spans every facility sharing a tenant,
# while V2 only guarantees uniqueness within a single facility. See the 4b
# comment in transform_record() for how this was confirmed.
_PER_KEY_FACILITY_SCOPE_FIELDS: dict[str, list] = {
    "reception_patient": ["patient_no"],
}

# *_id columns where V3 itself stores 0 (not an FK) — left as 0 by transform
# step 3b. V3 prescriptions hold walkin_sale_id=0 for every non-walk-in row.
_ZERO_IS_NOT_NULL: set[str] = {"walkin_sale_id"}

# Fields that must be non-null for a record to be sent; records missing them are skipped
_PER_KEY_REQUIRED_FIELDS: dict[str, list] = {
    "inpatient_admission":     ["admitting_doctor_id"],
    # derived through the visit; held back (not posted without one) if unresolved
    "evaluation_investigation": ["patient_id"],
    # required by V3; held back (not defaulted) if the department isn't there
    "evaluation_visit_destination": ["department_id"],
    "settings_insurance":      ["name"],
    "eval_procedure_category": ["name"],
    # V2 has blank unit rows (no name, symbol or description) — nothing to migrate
    "inventory_unit":          ["name"],
    # the dependant's patient record wasn't extracted — no name to give V3
    "reception_patient_dependant": ["full_name"],
    "settings_user":           ["email"],
    "evaluation_inv_result":   ["investigation_id", "patient_id"],
    "outpatient_vital":        ["patient_id"],
    # NOT NULL in V3. The *_by columns are V3 user ids: like admissions'
    # admitting doctor, they resolve only once the backend has created the
    # V2 staff as V3 users (and the users id map knows them).
    "inpatient_discharge_request": ["admission_id", "discharge_type_id", "requested_by"],
    "inpatient_discharge":         ["admission_id", "discharge_type_id", "discharged_by"],
    # inp_vitals: both NOT NULL (recorded_by = V3 user, usually a nurse)
    "inpatient_vital":             ["admission_id", "recorded_by"],
}

# Default values to inject when V3 returns a NOT NULL constraint violation for a column
# that V2 didn't have. Add entries here as new columns appear in the error log.
# Key = exact column name from the SQL error; value = the default to inject.
_V3_NULL_DEFAULTS: dict[str, Any] = {
    "credit_note_status": "none",        # V2 had no credit-note concept
    "invoice_type":       "standard",    # V2 didn't distinguish invoice types
    "currency":           "KES",         # default to Kenyan shilling
    "payment_method":     "cash",
    "created_by":          1,             # system/admin user for migrated records
    "updated_by":          1,
    "recorded_by":         1,
    "admitting_doctor_id":  1,
    "admission_diagnosis":  "Not specified",
    "is_a_split":           0,            # V2 had no split-prescription concept
    "appear_bold":          0,            # template_labs flag, null on some V2 rows
    "use_bio_ref_flagging": 0,            # procedures flag, null on some V2 rows
    "has_quick_results":    0,            # procedures flag
    "queued":               0,            # samples flags
    "tobe_contacted":       0,
    "exemption_amount":     0,            # invoices
    "is_walkin":            0,            # investigations flags, null on most V2 rows
    "preauth_approved":     0,
    "requires_preauth":     0,
    "requested_by":         1,            # requisitions with no V2 requestor (2026-10-15)
}
# Layer 2: coercions — value-level transforms applied after renaming.
# Each entry: (source_field_after_rename, transform_fn)
def _inactive_to_status(v) -> str:
    return "inactive" if v else "active"

def _active_flag_to_status(v) -> str:
    return "active" if v else "inactive"

def _null_to(default):
    """Coercion: a null / blank value becomes the V3 column's default."""
    return lambda v: default if v is None or (isinstance(v, str) and not v.strip()) else v


def _as_float(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


# V2 evaluation payment amount column → V3 receipts.payment_method (first non-zero wins)
_RECEIPT_METHODS = (("cash_amount", "cash"), ("mpesa_amount", "mpesa"), ("card_amount", "card"),
                    ("cheque_amount", "cheque"), ("pesa_pal_mpesa_amount", "mpesa"),
                    ("pesa_pal_card_amount", "card"), ("jambopay_amount", "jambopay"),
                    ("patientaccount_amount", "patient_account"), ("giftcard_amount", "gift_card"),
                    ("voucher_amount", "voucher"), ("loyalty_amount", "loyalty"), ("points_amount", "points"))


# V2 stock-movement action (free text, 20 values in kisumu_v3) → V3 (type, direction).
# None as direction = by the sign of V2's quantity (+ in, − out).
_MOVEMENT_ACTIONS: dict[str, tuple[str, str | None]] = {
    "dispensed": ("dispense", "out"), "consumable": ("issue", "out"),
    "return": ("return", "in"), "consumable return": ("return", "in"),
    "sys-purchase": ("receipt", "in"), "product added to store": ("receipt", "in"),
    "internal order (receive)": ("receipt", "in"), "in": ("receipt", "in"),
    "pharmacy pos sale": ("sale", "out"), "walkin_sale": ("sale", "out"),
    "knocked_off": ("disposal", "out"), "dispatched": ("transfer", "out"),
    "positive adjmt.": ("adjustment", "in"), "negative adjmt.": ("adjustment", "out"),
    "sys-transer": ("transfer", None), "sys-transfer": ("transfer", None),
    "sys-adjustment": ("adjustment", None), "adjustment": ("adjustment", None),
    "stock_take_adjustment": ("adjustment", None),
}


def _movement(r: dict) -> tuple[str, str]:
    """(V3 type, direction). V2's own type column says in / out on most rows
    and wins; elsewhere (transfer, adjust, adjustment, purchase order) the
    action, else the quantity's sign. An "in" with a negative quantity (310
    stock-take adjustments) is stock going down: out."""
    kind, direction = _MOVEMENT_ACTIONS.get(str(r.get("v2_action") or "").strip().lower(), ("adjustment", None))
    negative = _as_float(r.get("v2_quantity")) < 0
    v2_dir = str(r.get("v2_type") or "").strip().lower()
    if v2_dir in ("in", "out"):
        direction = "out" if v2_dir == "in" and negative else v2_dir
    elif direction is None:
        direction = "out" if negative else "in"
    return kind, direction


def _int_or_zero(v) -> int:
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return 0


_INV_RESULT_STATUSES = {"preliminary", "final", "corrected", "amended", "approved", "rejected", "retracted", "publish"}

_REQUISITION_STATUS = {s: s for s in ("draft", "submitted", "pending_approval", "approved", "partially_fulfilled",
                                       "fulfilled", "rejected", "cancelled")} | {"pending": "pending_approval"}


def _user_id_or_none(v) -> Any:
    """A V2 *_by column that should be a user id but often holds the person's
    name ('Ms. Kerubo  Venah') — a name can't become a V3 user id."""
    return int(v) if str(v).strip().isdigit() else None


def _wrap_in_list(v) -> list:
    if v is None:
        return []
    return v if isinstance(v, list) else [v]

def _clean_photo_path(v) -> Any:
    """Fix V2's "photo"/"image" path-accumulation bug: every time a patient
    record is re-saved, V2 appears to re-prepend "/storage/" to whatever is
    already there instead of checking if it's already prefixed, producing
    strings like "/storage//storage//storage/...https://host/img/x.png"
    (confirmed on a real record — 35 repeats, 367 chars total). V3's photo
    column almost certainly has a length limit the real URL alone fits in
    but the accumulated garbage does not; isolated A/B testing confirmed
    this exact field is what turns the insert into a generic 500, and that
    every other field in that same record (including clinic_id=null) is
    fine. If a full URL is embedded anywhere in the mess, keep only that —
    it's the actual useful part; otherwise collapse the repeated prefix
    down to one copy so a plain relative path doesn't balloon either."""
    if not isinstance(v, str):
        return v
    m = re.search(r"https?://\S+$", v)
    if m:
        return m.group(0)
    return re.sub(r"(?:/storage/)+", "/storage/", v)

# Layer 3: field injections — generate V3-required fields that V2 never had.
# Each entry: field_name → fn(record_dict) → value.
# Only called when the field is absent or None in the record after all renames.
# Use this (not _V3_NULL_DEFAULTS) whenever the default must be unique per row.
_v3_users_by_email: dict[str, int] | None = None
_v3_users_lock = threading.Lock()


# V2 staff created in V3 by create_v3_users.py get a placeholder address,
# "<real email>.v2-<V2 user id>.invalid": core sends no welcome email to
# *.invalid, and the V2 id rides in the address so the V2→V3 user mapping can
# be rebuilt from V3's own user list on any host, with no state file.
_V2_PLACEHOLDER_EMAIL = re.compile(r"^(?P<email>.*?)\.v2-(?P<v2_id>\d+)\.invalid$", re.IGNORECASE)
_v3_users_by_v2_id: dict[int, int] = {}


def _load_v3_users() -> None:
    """Caller holds _v3_users_lock. Read the destination org's users once."""
    global _v3_users_by_email, _v3_users_by_v2_id
    org_cfg = v3_login_org_cfg()
    users = _fetch_v3_records(_v3_alias(r"App\Models\User"), org_cfg, service_name="core")
    by_email, by_v2 = {}, {}
    for u in users:
        email = str(u.get("email") or "").strip().lower()
        if not email:
            continue
        by_email[email] = u["id"]
        m = _V2_PLACEHOLDER_EMAIL.match(email)
        if m:
            by_v2[int(m["v2_id"])] = u["id"]
            if m["email"]:
                by_email.setdefault(m["email"], u["id"])   # the real V2 address it stands for
    _v3_users_by_email, _v3_users_by_v2_id = by_email, by_v2
    log.info("Loaded %d V3 users for doctor/staff lookups (%d migrated V2 staff)", len(by_email), len(by_v2))


_v3_departments_by_name: dict[str, int] | None = None
_v3_departments_lock = threading.Lock()


def _v3_department_id_by_name(name) -> int | None:
    """V3 department id for a name (case/space-insensitive) in the
    destination org — read once per process; reset_v3_department_cache()
    after creating departments."""
    global _v3_departments_by_name
    if not name or not str(name).strip():
        return None
    with _v3_departments_lock:
        if _v3_departments_by_name is None:
            recs = _fetch_v3_records("departments", v3_login_org_cfg(), service_name="core")
            _v3_departments_by_name = {" ".join(str(r["name"]).split()).lower(): r["id"]
                                       for r in recs if r.get("name")}
            log.info("Loaded %d V3 departments for department lookups", len(_v3_departments_by_name))
    return _v3_departments_by_name.get(" ".join(str(name).split()).lower())


def reset_v3_department_cache() -> None:
    global _v3_departments_by_name
    with _v3_departments_lock:
        _v3_departments_by_name = None


# Migrated V2 staff are stored in V3 with email "<original>.v2-<V2 user id>.invalid"
# (the convention the backend used for the first batch, kept by
# migrate_facility.sync_users). The embedded V2 id is the join key — exact,
# and scoped to the destination org, so V2 ids repeating across facilities
# can't collide (which is why users are NOT kept in the shared id map).
V2_USER_EMAIL_RE = re.compile(r"\.v2-(\d+)\.invalid$")
_v3_users_by_v2_id: dict[int, int] | None = None


def v2_user_email(email, username, v2_id) -> str:
    base = str(email).strip() if email and "@" in str(email) else f"{str(username or 'user').strip()}@migrated.v2"
    return f"{base}.v2-{int(v2_id)}.invalid"


def reset_v3_user_cache() -> None:
    global _v3_users_by_v2_id, _v3_users_by_email
    with _v3_users_lock:
        _v3_users_by_v2_id = None
        _v3_users_by_email = None


def _v3_user_id_for_v2(v2_user_id, email=None) -> int | None:
    """V3 user for a V2 user: by the V2 id embedded in the migrated email,
    else by plain email (accounts created some other way)."""
    global _v3_users_by_v2_id
    if v2_user_id not in (None, "") and str(v2_user_id).isdigit():
        with _v3_users_lock:
            if _v3_users_by_v2_id is None:
                users = _fetch_v3_records(_v3_alias(r"App\Models\User"), v3_login_org_cfg(), service_name="core")
                _v3_users_by_v2_id = {}
                for u in users:
                    m = V2_USER_EMAIL_RE.search(str(u.get("email") or ""))
                    if m:
                        _v3_users_by_v2_id[int(m.group(1))] = u["id"]
                log.info("Loaded %d migrated V3 users (by V2 id)", len(_v3_users_by_v2_id))
        hit = _v3_users_by_v2_id.get(int(v2_user_id))
        if hit is not None:
            return hit
    return _v3_user_id_by_email(email)


def _v3_user_id_by_email(email) -> int | None:
    """V3 user id for an email (case-insensitive), from the destination org's
    users — read once per process. V3 users can't be inserted through the
    gateway; V2 staff resolve once they exist in V3 (create_v3_users.py)."""
    if not email:
        return None
    with _v3_users_lock:
        if _v3_users_by_email is None:
            _load_v3_users()
    return _v3_users_by_email.get(str(email).strip().lower())


def _v3_user_for_v2_id(v2_user_id) -> int | None:
    """V3 user id for a V2 user id — via the users id map, else via the V2 id
    in a migrated user's placeholder address. None when that V2 staff member
    isn't in V3 (no guessing: a wrong *_by would misattribute clinical records)."""
    if v2_user_id in (None, "", 0, "0"):
        return None
    key = int(v2_user_id) if str(v2_user_id).isdigit() else v2_user_id
    found = _id_map.get(_v3_alias(r"App\Models\User"), {}).get(key)
    if found is not None:
        return found
    with _v3_users_lock:
        if _v3_users_by_email is None:
            _load_v3_users()
    return _v3_users_by_v2_id.get(key)


def _v2_admission_for_visit(visit_id) -> int | None:
    """V2 admission id of a V2 visit (persisted visit→admission map). V2
    discharge requests only carry visit_id; V3 requires admission_id."""
    try:
        return _visit_admission_map.get(int(visit_id))
    except (TypeError, ValueError):
        return None


# Sections of a V2 discharge request with no V3 column of their own —
# folded, labelled, into inp_discharge_requests.discharge_notes.
_DISCHARGE_REQUEST_SECTIONS = [
    ("Presenting complaints", "complains"), ("General examination", "general_examination"),
    ("CVS", "cvs"), ("RS", "rs"), ("PA", "pa"), ("CNS", "cns"), ("ENT", "ent"), ("MSS", "mss"),
    ("Investigations", "investigations"), ("Procedures", "procedures"), ("Other", "other"),
]
_DISCHARGE_REQUEST_STATUSES = {"pending", "approved", "rejected", "discharged", "cancelled"}
_NOT_RECORDED = "Not recorded in V2"


def _v2_patient_for_visit(visit_id) -> int | None:
    """V2 patient id of a V2 visit (persisted visit→patient map)."""
    try:
        return _visit_patient_map.get(int(visit_id))
    except (TypeError, ValueError):
        return None


def _store_type(r: dict) -> str:
    """inv_stores.type enum from a V2 store name (V2 has no type column)."""
    name = str(r.get("name") or "").lower()
    for word, kind in (("pharmac", "pharmacy"), ("laborator", "laboratory"), ("theat", "theater"),
                       ("main store", "main"), ("maternity", "ward"), ("medsurg", "ward"),
                       ("ward", "ward")):
        if word in name:
            return kind
    return "department"


def _blank_to_none(v):
    """V2 leaves unrecorded readings as '' — V3's numeric columns need null."""
    return None if v is None or (isinstance(v, str) and not v.strip()) else v


_NUMBER = re.compile(r"-?\d+(?:\.\d+)?")


def _first_reading(v):
    """A numeric vital. Nurses sometimes typed several readings into one V2
    field ('204            231    161', '120/80', '36.7C') — V3's numeric
    columns reject that ("Data truncated"). The first reading is kept (all of
    them go into the notes, see _note_multiple_readings); text with no number
    at all becomes null."""
    v = _blank_to_none(v)
    if not isinstance(v, str):
        return v
    nums = _NUMBER.findall(v)
    if not nums:
        return None
    n = float(nums[0])
    return int(n) if n.is_integer() else n


# vitals with decimals: V2 sometimes holds a decimal comma ('36,6' — refused by V3 as "Incorrect decimal
# value" on kisumu_v3 inpatient temperatures, 2026-10-15)
_DECIMAL_VITALS = {"temperature", "weight", "height", "bmi", "head_circumference", "muac", "random_blood_sugar",
                   "fasting_blood_sugar", "blood_sugar", "waist", "hip", "body_fat", "muscle_mass", "bone_mass",
                   "body_water", "visceral_fat", "intracular_pressure"}
_DECIMAL_COMMA = re.compile(r"\s*-?\d+,\d{1,2}\s*")


def _vital_reading(field: str):
    """The coercion for one vital: a decimal comma becomes a point (decimal fields only — '120,80' in a
    blood pressure is two readings), then _first_reading. Height / weight 0 means not measured: V3
    computes BMI from them and a 0 failed the insert with "Division by zero"."""
    def read(v):
        if field in _DECIMAL_VITALS and isinstance(v, str) and _DECIMAL_COMMA.fullmatch(v):
            v = v.replace(",", ".")
        n = _first_reading(v)
        if isinstance(n, (int, float)) and abs(n) >= 1e7:      # a typo no vital can be (an outpatient value
            return None                                        # overflowed 64 bits: "Integer exceeds N-bit range")
        return None if field in ("height", "weight") and n == 0 else n
    return read


def _v3_user_or_system(v):
    """A V2 user id → V3 user; a V2 user V3 doesn't have → the system user (1, as _V3_NULL_DEFAULTS).
    For NOT NULL *_by columns only — agreed 2026-10-15 (as for admissions by an external doctor)."""
    if _blank_to_none(v) is None:
        return None
    return _v3_user_for_v2_id(v) or 1


def _note_multiple_readings(note_field: str, fields: tuple[str, ...]):
    """Pre-coercion hook: record every field holding more than one reading in
    the vital's notes before _first_reading keeps only the first."""
    def note(out: dict) -> None:
        extra = [f"{f}: {' '.join(out[f].split())}" for f in fields
                 if isinstance(out.get(f), str) and len(_NUMBER.findall(out[f])) > 1]
        if extra:
            line = "Multiple readings in V2 — " + "; ".join(extra)
            out[note_field] = f"{out[note_field]}\n{line}" if _blank_to_none(out.get(note_field)) else line
    return note


_INPATIENT_VITAL_NUMBERS = ("temperature", "pulse_rate", "respiratory_rate", "systolic_bp", "diastolic_bp",
                            "oxygen_saturation", "weight", "height", "bmi")
_OUTPATIENT_VITAL_NUMBERS = ("temperature", "pulse", "weight", "height", "bmi", "head_circumference", "muac",
                             "random_blood_sugar", "fasting_blood_sugar", "bp_systolic", "bp_diastolic",
                             "respiration", "oxygen", "waist", "hip", "blood_sugar", "body_fat", "muscle_mass",
                             "bone_mass", "metabolic_age", "body_water", "visceral_fat", "length_percentile",
                             "weight_percentile", "bmi_percentile", "intracular_pressure")

# Optional V3 columns sent as null when V3 rejects the V2 value as too long or
# out of range ("Data too long for column 'x'", "Out of range value for column
# 'x'"), instead of dead-lettering the record: insurance companies' postal_code
# holds whole addresses; a few vitals hold typos (height 15736.6, 7691761 in
# kisumu_v3 — V3 vitals are decimal(6,2)).
_V3_NULL_WHEN_UNFIT: set[str] = {"postal_code", *_INPATIENT_VITAL_NUMBERS, *_OUTPATIENT_VITAL_NUMBERS}

def _flag_with_message(flag: str, note_field: str):
    """Pre-coercion hook for a V2 flag column that sometimes holds a message
    instead of 0/1 — prescriptions' moved_from_chargesheet is 'Item removed
    from chargesheet by Mr. X on Sep 23, 2024 12:11:15 pm' on 2,718
    kisumu_v3 rows. The message means the flag is set: 1, and the message is
    kept in the notes."""
    def fix(out: dict) -> None:
        v = out.get(flag)
        if isinstance(v, str) and v.strip() and v.strip().lower() not in ("0", "1", "true", "false"):
            msg = " ".join(v.split())
            out[note_field] = f"{out[note_field]}\n{msg}" if _blank_to_none(out.get(note_field)) else msg
            out[flag] = 1
    return fix


# run before the per-key coercions, on the renamed record
_PER_KEY_PRE_COERCE: dict[str, Any] = {
    "evaluation_prescription": _flag_with_message("moved_from_chargesheet", "notes"),
    # same V2 message-in-a-flag on investigations (all 11,491 kisumu_v3 rows missing from V3 on
    # 2026-10-15 were refused for it: "Incorrect integer value: 'Item removed from chargesheet …'")
    "evaluation_investigation": _flag_with_message("moved_from_chargesheet", "comments"),
    "inpatient_vital":  _note_multiple_readings("notes", _INPATIENT_VITAL_NUMBERS),
    "outpatient_vital": _note_multiple_readings("nurse_notes", _OUTPATIENT_VITAL_NUMBERS),
}


def _blood_pressure(r: dict) -> str | None:
    s = _blank_to_none(r.get("systolic_bp") or r.get("bp_systolic"))
    d = _blank_to_none(r.get("diastolic_bp") or r.get("bp_diastolic"))
    return f"{s}/{d}" if s is not None and d is not None else None


# V2 vital readings inp_vitals has no column for — kept, as JSON, in
# additional_vitals rather than dropped.
_EXTRA_VITALS = ("blood_sugar", "blood_sugar_units", "random_blood_sugar", "fasting_blood_sugar", "muac",
                 "head_circumference", "waist", "hip", "symptoms", "allergies", "chronic_illnesses",
                 "current_medication", "body_fat", "muscle_mass", "bone_mass", "metabolic_age", "body_water",
                 "visceral_fat", "length_percentile", "weight_percentile", "bmi_percentile", "lmp",
                 "intracular_pressure", "uncorrected_near_vision", "corrected_near_vision", "visual_acuity",
                 "visual_acuity_aided", "visual_acuity_unaided")


def _additional_vitals(r: dict) -> dict | None:
    extra = {k: (str(r[k]) if not isinstance(r[k], (int, float, str)) else r[k])
             for k in _EXTRA_VITALS if _blank_to_none(r.get(k)) is not None}
    return extra or None


def _dispensing_notes(r: dict) -> str:
    return (f"Migrated V2 dispensing #{r.get('id')} of {r.get('created_at')}: "
            f"prescription #{r.get('prescription')}, visit #{r.get('visit_id')}")


def _discharge_request_notes(r: dict) -> str | None:
    parts = [f"{label}: {str(r[k]).strip()}" for label, k in _DISCHARGE_REQUEST_SECTIONS
             if r.get(k) is not None and str(r[k]).strip()]
    return "\n\n".join(parts) or None


def _v3_patient_for_v2_visit(v2_visit_id) -> int | None:
    """V2 visit -> V2 patient (persisted visit/patient map) -> V3 patient.
    Investigations and results carry no patient column in V2; their visit
    does. Runs before FK remap, so the V2 visit id is still on the record."""
    if v2_visit_id is None:
        return None
    v2_visit_id = int(v2_visit_id) if str(v2_visit_id).isdigit() else v2_visit_id
    return _id_map.get("patient", {}).get(_visit_patient_map.get(v2_visit_id))


_PER_KEY_INJECT: dict[str, dict[str, Any]] = {
    "evaluation_investigation": {
        "patient_id": lambda r: _v3_patient_for_v2_visit(r.get("visit_id")),
    },
    "evaluation_inv_result": {
        "patient_id": lambda r: _v3_patient_for_v2_visit(r.get("visit_id")),
    },
    "inventory_store": {
        "type":          _store_type,
        "is_main_store": lambda r: str(r.get("name") or "").strip().lower() == "main store",
    },
    "inventory_dispensing": {
        # V2 patient id here (via the visit); remapped to V3 at post time
        "patient_id":     lambda r: _v2_patient_for_visit(r.get("visit_id")),
        # unique in V3; source_schema keeps facilities' dispensing #1 apart
        "receipt_number": lambda r: f"DSP-{r.get('source_schema') or 'v2'}-{r.get('id')}",
        "subtotal":       lambda r: r.get("amount"),
        "total_amount":   lambda r: r.get("amount"),
        "status":         lambda r: "completed",
        "payment_status": lambda r: "pending",   # only if V2 had none (see _PER_KEY_COERCIONS)
        "notes":          _dispensing_notes,
    },
    "theatre_schedule": {
        # V3 scheduled_date (date) is empty on V2 rows: the day the schedule starts (reported 2026-10-15)
        "scheduled_date": lambda r: (str(r["schedule_start_time"])[:10]
                                     if re.match(r"\d{4}-\d{2}-\d{2}", str(r.get("schedule_start_time") or ""))
                                     else None),
    },
    "inpatient_discharge_type": {
        # inp_discharge_types needs a code (unique per org); V2 types only have a name
        "code": lambda r: "_".join(str(r.get("name") or f"TYPE {r.get('id')}").upper().split()),
    },
    "inventory_product": {
        # inv_products.generic_name (V2's name) is NOT NULL; a few V2 products
        # have no name at all, only an item number (4 in kisumu_v3)
        "generic_name": lambda r: f"Unnamed product {r.get('item_no') or r.get('id')}",
    },
    "settings_treatment_action": {
        # core_treatment_actions.display_name is NOT NULL with no default and
        # V2 has no such column: the label is V2's name/slug ("vitals",
        # "doctor_notes") made readable — "Vitals", "Doctor Notes"
        "display_name": lambda r: " ".join(
            w[:1].upper() + w[1:]
            for w in str(r.get("name") or f"Action {r.get('id')}").replace("_", " ").replace("-", " ").split()
        ),
    },
    "inpatient_discharge_request": {
        # V2 admission id here; remapped to the V3 admission at post time
        "admission_id":    lambda r: _v2_admission_for_visit(r.get("visit_id")),
        # unique in V3, absent in V2. source_schema keeps two facilities'
        # request #12 apart.
        "request_number":  lambda r: f"DRQ-{r.get('source_schema') or 'v2'}-{r.get('id')}",
        "requested_at":    lambda r: r.get("created_at"),
        "status":          lambda r: (r.get("discharge_status")
                                      if r.get("discharge_status") in _DISCHARGE_REQUEST_STATUSES
                                      else "pending"),
        "discharge_notes": _discharge_request_notes,
    },
    "inpatient_discharge": {
        "discharge_number":       lambda r: f"DIS-{r.get('source_schema') or 'v2'}-{r.get('id')}",
        "discharge_date":         lambda r: r.get("created_at"),
        # V2 discharges carry no clinical text — it lives on their discharge
        # request (joined in as request_* by snowflake_to_v3_migration). These
        # three are NOT NULL in V3, hence the explicit placeholder.
        "discharge_diagnosis":    lambda r: r.get("request_principal") or _NOT_RECORDED,
        "discharge_summary":      lambda r: r.get("request_conditions") or _NOT_RECORDED,
        "discharge_instructions": lambda r: r.get("request_tca") or _NOT_RECORDED,
        "follow_up_instructions": lambda r: r.get("request_tca"),
        "treatment_summary":      lambda r: r.get("request_treatment"),
        "procedures_performed":   lambda r: r.get("request_procedures"),
        "approved_at":            lambda r: r.get("updated_at") if r.get("approved_by") else None,
    },
    "evaluation_visit_destination": {
        "arrived_at": lambda r: r.get("begin_at") or r.get("created_at"),
        # the destination's name (lifted from the destination object); on
        # rows without one, the name of its destination_id in V2's
        # settings_destinations (joined in — 137k rows have only that), then
        # V2's free-text department ("Pharmacy", "Nursing")
        "department_name": lambda r: (" ".join(str(r.get("destination_setting_name") or "").split())
                                      or str(r.get("department") or "").strip() or None),
        # V2 has no departments; one V3 department per V2 destination is
        # created before this job runs (ensure_departments_from_destinations)
        "department_id": lambda r: _v3_department_id_by_name(r.get("department_name")),
        # the destination's slug (requested 2026-10-15): from the embedded destination, else its
        # settings_destinations row (joined in). NOTE: VisitDestination's $fillable lacks
        # destination_slug, so the gateway drops it until the backend adds it
        "destination_slug": lambda r: (str(r.get("destination_setting_slug") or "").strip() or None),
    },
    "reception_patient_nok": {
        # NOT NULL in V3; V2 next of kin carry neither relationship nor relationship_id
        "relationship": lambda r: _NOT_RECORDED,
    },
    "eval_icd10_range": {
        # V3 code is NOT NULL + unique; V2 has the range: A00..B99 → "A00-B99"
        "code": lambda r: "-".join(dict.fromkeys(
            c for c in (str(r.get("code_from") or "").strip(), str(r.get("code_to") or "").strip()) if c)) or None,
        # NOT NULL in V3; V2 has only the name
        "description": lambda r: r.get("name"),
    },
    "finance_gl_account_group": {
        # gl_account_groups.code is NOT NULL, varchar(20), unique per org; V2 groups only have a name
        "code": lambda r: "_".join(str(r.get("name") or f"GROUP {r.get('id')}").upper().split())[:20],
    },
    "settings_scheme": {
        # V3 code is NOT NULL, unique per org; every V2 scheme's code is empty.
        # source_schema keeps two facilities' scheme #12 apart.
        "code": lambda r: f"SCH-{r.get('source_schema') or 'v2'}-{r.get('id')}",
    },
    "reception_patient_document": {
        # V2 stores the literal string "null" (or "2") as document_type and
        # V3 org 4 has no document types set up — agreed default for all.
        "document_type":  lambda r: "medical_report",
        "title":          lambda r: r.get("file_name"),
        "file_extension": lambda r: (Path(str(r.get("file_name") or "")).suffix.lstrip(".").lower() or None),
        "file_type":      lambda r: mimetypes.guess_type(str(r.get("file_name") or ""))[0],
        "document_date":  lambda r: r.get("created_at"),
        # NOT NULL in V3 (bytes); V2 never recorded the size
        "file_size":      lambda r: 0,
    },
    # patient_dependants: full_name + relationship NOT NULL; V2 rows only link
    # the dependant's own patient record (joined in as dependant_*)
    "reception_patient_dependant": {
        "full_name": lambda r: " ".join(" ".join(str(r.get(f"dependant_{c}") or "").split())
                                        for c in ("first_name", "middle_name", "last_name")
                                        if str(r.get(f"dependant_{c}") or "").strip()) or None,
        "relationship": lambda r: _NOT_RECORDED,    # V2 relationship ids have no lookup table in the extract
        "dob":          lambda r: _blank_to_none(r.get("dependant_dob")),
        "gender":       lambda r: (str(r.get("dependant_sex") or "").strip().lower()
                                   if str(r.get("dependant_sex") or "").strip().lower() in ("male", "female", "other")
                                   else None),
        "mobile":       lambda r: _blank_to_none(r.get("dependant_mobile")),
        "id_no":        lambda r: _blank_to_none(r.get("dependant_id_no")),
        "email":        lambda r: _blank_to_none(r.get("dependant_email")),
    },
    # theatre_bookings / theatre_operations: patient_id NOT NULL; V2 rows only
    # link the visit (V2 patient id here, remapped to V3 at post time)
    "theatre_booking": {
        "patient_id": lambda r: _v2_patient_for_visit(r.get("visit_id")),
    },
    "theatre_operation": {
        "patient_id": lambda r: _v2_patient_for_visit(r.get("visit_id")),
    },
    "finance_receipt": {
        "receipt_type":   lambda r: "payment",
        "status":         lambda r: "valid",
        # V2 splits a payment across per-method amount columns
        "payment_method": lambda r: next((m for col, m in _RECEIPT_METHODS
                                          if _as_float(r.get(col)) > 0), None),
        "receipt_date":   lambda r: str(r.get("created_at") or "")[:10] or None,     # only if V2 has none
        "notes":          lambda r: (f"V2 status: {r['v2_status']}" if _blank_to_none(r.get("v2_status")) else None),
    },
    "settings_insurance": {
        # finance insurance_companies.code is NOT NULL + unique; V2 companies
        # have none. Same pattern as schemes' SCH-<facility>-<id>.
        "code": lambda r: f"INS-{r.get('source_schema') or 'v2'}-{r.get('id')}",
    },
    "inventory_stock_movement": {
        "type":            lambda r: _movement(r)[0],
        "direction":       lambda r: _movement(r)[1],
        "quantity":        lambda r: abs(_int_or_zero(r.get("v2_quantity"))),        # V3 keeps it positive
        "from_store_id":   lambda r: r.get("v2_store_id") if _movement(r)[1] == "out" else None,
        "to_store_id":     lambda r: r.get("v2_store_id") if _movement(r)[1] == "in" else None,
        "reason":          lambda r: _blank_to_none(r.get("v2_action")),
        "notes":           lambda r: f"V2 type: {r['v2_type']}" if _blank_to_none(r.get("v2_type")) else None,
        "idempotency_key": lambda r: f"v2:{r.get('source_schema') or 'v2'}:stock_movement:{r.get('id')}",
    },
    "inventory_requisition": {
        # inv_requisitions: requisition_number (unique per org) and request_date are NOT NULL
        "requisition_number": lambda r: f"REQ-{r.get('source_schema') or 'v2'}-{r.get('id')}",
        "request_date":       lambda r: str(r.get("created_at") or "")[:10] or None,
    },
    "inventory_grn": {
        # inv_goods_received_notes: grn_number (unique) and received_date are
        # NOT NULL; store / supplier come from the purchase order (FETCH_SQL)
        "grn_number":    lambda r: f"GRN-{r.get('source_schema') or 'v2'}-{r.get('id')}",
        "received_date": lambda r: str(r.get("created_at") or "")[:10] or None,
        "status":        lambda r: "received",
    },
    "inventory_purchase_order": {
        # inv_purchase_orders: po_number (unique per org) and order_date are
        # NOT NULL; V2 orders have neither
        "po_number":  lambda r: f"PO-{r.get('source_schema') or 'v2'}-{r.get('id')}",
        "order_date": lambda r: str(r.get("created_at") or "")[:10] or None,
    },
    "inpatient_bed": {
        # inp_beds has both: `number` (2026-06 simplified column) and the
        # original `bed_number`, still NOT NULL — same value
        "bed_number": lambda r: r.get("number"),
    },
    "settings_user": {
        # email is required; a V2 user without one falls back to its username
        "email": lambda r: r.get("username"),
        # NOT NULL in V3, and the V2 extract carries no password (hash). A
        # random one nobody ever sees (V3 hashes it; redacted from the
        # dead-letter file) + a forced reset: staff set theirs on first login.
        "password":               lambda r: secrets.token_urlsafe(32),
        "enforce_password_reset": lambda r: 1,
    },
    "inpatient_admission_type": {
        # inp_admission_types needs a code; V2 types only have a name
        "code": lambda r: "_".join(str(r.get("name") or f"TYPE {r.get('id')}").upper().split()),
    },
    "settings_clinic": {
        # code is required on core_facilities; V2 facility_code is often null
        "code":   lambda r: r.get("name"),
        "type":   lambda r: "medical",
        "status": lambda r: "active",
    },
    "inpatient_admission_request": {
        # V3 inp_admission_requests requires a unique request_number; V2 had none.
        "request_number": lambda r: f"REQ-{r.get('id', 'unknown')}",
        # NOT NULL (2025_12_03_000007): when the request was made, and why
        "requested_at":   lambda r: r.get("created_at") or r.get("updated_at"),
        "reason_for_admission": lambda r: _blank_to_none(r.get("diagnosis")) or _NOT_RECORDED,
    },
    "inpatient_admission": {
        # V2 doctor (embedded, flattened as doctor_email) -> V3 user by email.
        # None until the backend has created the doctor as a V3 user; the
        # required-field check then holds the admission back for a later run.
        # An admission by an external doctor only (doctor_id empty, external_doctor "DR BITTA" — 15
        # kisumu_v3 rows) has no V3 user to name: agreed 2026-10-15 — the system user (1, as
        # _V3_NULL_DEFAULTS) admits it and the doctor's name is kept in `reason`.
        "admitting_doctor_id": lambda r: (_v3_user_id_for_v2(r.get("doctor_id"), r.get("doctor_email"))
                                          or (1 if not r.get("doctor_id") and _blank_to_none(r.get("external_doctor"))
                                              else None)),
        "reason": lambda r: (f"External doctor: {' '.join(str(r['external_doctor']).split())}"
                             if not r.get("doctor_id") and _blank_to_none(r.get("external_doctor")) else None),
        # V3 inp_admissions requires a unique admission_number; V2 had none.
        "admission_number": lambda r: f"ADM-{r.get('id', 'unknown')}",
        # Fall back to created_at if V2 didn't carry an explicit admission_date.
        "admission_date":   lambda r: r.get("created_at") or r.get("updated_at"),
    },
    "evaluation_doctor_note": {
        # V2 DoctorNote has no patient_id column — it's visit → patient.
        # Chain: V2 visit_id → V2 patient_id (persisted map) → V3 patient_id (id_map).
        "patient_id": lambda r: _id_map.get("patient", {}).get(
            _visit_patient_map.get(r.get("visit_id"))
        ),
    },
    "outpatient_vital": {
        # V2 vitals have no patient — it's the visit's. V2 patient id here
        # (int-keyed lookup: Snowflake ids arrive as strings); remapped to
        # the V3 patient via _FK_REMAP.
        "patient_id":     lambda r: _v2_patient_for_visit(r.get("visit_id")),
        "recorded_at":    lambda r: r.get("created_at"),
        "systolic_bp":    lambda r: _blank_to_none(r.get("bp_systolic")),
        "diastolic_bp":   lambda r: _blank_to_none(r.get("bp_diastolic")),
        "respiratory_rate":  lambda r: _blank_to_none(r.get("respiration")),
        "oxygen_saturation": lambda r: _blank_to_none(r.get("oxygen")),
        "blood_pressure": _blood_pressure,
    },
    "inpatient_vital": {
        # V2 admission id via the visit; remapped to the V3 admission via _FK_REMAP
        "admission_id":      lambda r: _v2_admission_for_visit(r.get("visit_id")),
        "recorded_at":       lambda r: r.get("created_at"),
        "blood_pressure":    _blood_pressure,
        "additional_vitals": _additional_vitals,
    },
    "evaluation_prescription": {
        # V2 sends the prescriber as a free-text name in "prescribed_by"
        # (dropped in _PER_KEY_DROP_FIELDS) but V3's prescribed_by column is
        # an integer user id. The real V2 prescriber id is in "user_id" —
        # resolve it through the Users id map (bootstrapped by email, since
        # users have no "name" field to match on and are usually provisioned
        # outside this pipeline's generic insert).
        "prescribed_by": lambda r: _id_map.get(_v3_alias(r"App\Models\User"), {}).get(r.get("user_id")),
    },
    "reception_visit": {
        # V3's visits.in_morgue column is NOT NULL with no default (confirmed
        # 2026-10 straight from V3's own Laravel log — "SQLSTATE[23000]...
        # Column 'in_morgue' cannot be null"). This can't be caught by the
        # existing reactive _V3_NULL_DEFAULTS path: that depends on parsing
        # the SQL error text back out of the response body, but production
        # only ever returns the generic "Something went wrong" message with
        # no detail — the real error never reaches this script, only the
        # server's own log. So this has to be injected proactively instead.
        # 0 ("not in morgue"/"not an inpatient visit") is the correct
        # default for every migrated V2 record, since none of them were
        # ever tracked this way in V2. "inpatient" confirmed alongside
        # in_morgue — same NOT NULL class of column on the same table.
        "in_morgue": lambda r: 0,
        "inpatient": lambda r: 0,
    },
}

_PER_KEY_COERCIONS: dict[str, dict[str, Any]] = {
    # V2 user ids → V3 user ids (None when V3 doesn't have that user yet:
    # required *_by fields then hold the record back, optional ones go null)
    "inpatient_discharge_request": {
        "requested_by": _v3_user_or_system,         # NOT NULL in V3 (2026-10-15: user missing → system user)
        "reviewed_by":  _v3_user_for_v2_id,
    },
    "inpatient_discharge": {
        "discharged_by": _v3_user_or_system,        # NOT NULL in V3
        "approved_by":   _v3_user_for_v2_id,
    },
    # V3 applies_to is a module id (ProcedureCategory::DEPARTMENTS: lab 3, radiology 4); a few V2 rows hold
    # text ('Ward', the literal 'NULL') — refused as "Incorrect integer value" (2026-10-15)
    "eval_procedure_category": {
        "applies_to": lambda v: (int(v) if str(v).strip().isdigit() else
                                 {"lab": 3, "laboratory": 3, "radiology": 4}.get(str(v or "").strip().lower())),
    },
    "inpatient_vital": {
        "recorded_by": _v3_user_for_v2_id,
        **{f: _vital_reading(f) for f in _INPATIENT_VITAL_NUMBERS},
    },
    "outpatient_vital": {
        "recorded_by": _v3_user_for_v2_id,
        **{f: _vital_reading(f) for f in _OUTPATIENT_VITAL_NUMBERS},
        "lmp": _blank_to_none,                       # a date, not a reading
    },
    "inventory_dispensing": {
        "served_by":      _v3_user_for_v2_id,
        # V2 0 = not paid yet → V3's 'pending'; anything else → 'paid'
        "payment_status": lambda v: "pending" if str(v) in ("0", "", "None") else "paid",
    },
    "reception_patient_document": {
        # V2 holds the literal string "null" (truthy, so the injection default
        # never fires) or "2" — always replace with the agreed default.
        "document_type": lambda v: "medical_report",
    },
    "reception_patient": {
        "photo": _clean_photo_path,
    },
    "reception_patient_scheme": {
        # V2 field is `inactive` (tinyint), which was renamed to nothing above
        # — handle via special case in transform_record
        "__inactive_to_status__": True,
    },
    "settings_scheme": {
        "__active_to_status__": True,
    },
    "finance_receipt": {
        # a DATE column. V2's zero date (0000-00-00) arrives as "-0001-11-30"
        # and MySQL refuses it — null, so the inject falls back to created_at
        "receipt_date": lambda v: (str(v)[:10] if _blank_to_none(v) and re.match(r"(19|20)\d\d-\d\d-\d\d", str(v))
                                   else None),
    },
    # investigation_results.status is an enum of words; V2 holds 0 / 1. MySQL
    # read 1 as the enum's first value (preliminary — 47,326 kisumu_v3 rows
    # already in V3) and refused 0 ("Data truncated", 430 results + their
    # details). Both → preliminary, consistently; a valid word passes through.
    "evaluation_inv_result": {
        "status": lambda v: (str(v).strip().lower() if str(v or "").strip().lower() in _INV_RESULT_STATUSES
                             else "preliminary"),
    },
    # inv_requisitions.status is an enum without V2's "pending" ("Data truncated
    # for column 'status'"): pending → pending_approval, unknown → submitted
    "inventory_stock_movement": {
        "opening_balance": _int_or_zero, "closing_balance": _int_or_zero,     # NOT NULL integers
    },
    "inventory_requisition": {
        "status": lambda v: _REQUISITION_STATUS.get(str(v or "").strip().lower(), "submitted"),
        # NOT NULL with defaults (2025_11_25_000010)
        "approval_level":          _null_to(0),
        "required_approval_level": _null_to(1),
        "priority":                _null_to("normal"),
    },
    "evaluation_sample": {
        # V3 status (pending / collected / processing / completed) is NOT NULL;
        # every kisumu_v3 V2 sample has an empty status — a V2 sample row exists
        # once the sample is taken, so "collected", not V3's default "pending"
        "status": lambda v: v if str(v or "").strip() else "collected",
        # NOT NULL flags with a default (2026_02_03_000004 evaluation_samples)
        "queued":         _null_to(0),
        "tobe_contacted": _null_to(0),
    },
    "evaluation_prescription": {
        # V3 user ids; V2 holds the approver's / rejecter's name (all 9,401
        # filled kisumu_v3 rows) — sent as null, the *_at dates are kept
        "return_approved_by": _user_id_or_none,
        "return_rejected_by": _user_id_or_none,
    },
    # procedures (2026_03_10_000005): NOT NULL flags with defaults
    "eval_procedure": {
        "use_bio_ref_flagging": _null_to(0), "has_quick_results": _null_to(0), "active": _null_to(1),
    },
    # finance invoices (2025_11_20_140007 + 2026_03_15 + 2026_08_15): every
    # NOT NULL column with a default — a V2 null overrides it ("Column
    # 'exemption_amount' cannot be null"), so send the default instead
    "finance_invoice": {
        **{c: _null_to(0) for c in (
            "subtotal", "discount_amount", "tax_amount", "total_amount", "paid_amount", "balance_amount",
            "waiver_amount", "copay_amount", "credit_note_amount", "amount", "paid", "exemption_amount",
            "is_interim", "is_manual", "auto_cancelled", "for_cash", "is_exempted")},
        "invoice_type":       _null_to("cash"),
        "status":             _null_to("pending"),
        "payment_status":     _null_to("unpaid"),
        "credit_note_status": _null_to("none"),
    },
    # investigations' boolean flags are NOT NULL with a default in V3
    # (2026_03_10_000002_recreate_investigations_table…); V2 leaves them null
    # (is_walkin on ~89% of rows) and an explicit null overrides the column
    # default — so a null / blank flag is sent as that default
    "evaluation_investigation": {
        **{flag: _null_to(0) for flag in (
            "ordered", "on_credit", "is_walkin", "cancelled", "moved", "moved_from_chargesheet",
            "is_interim", "requires_preauth", "preauth_approved", "remove_from_report")},
        "can_be_offered": _null_to(1),
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


# The uuid is the join key between V2 and V3: it originates in V2, is
# written to V3 unchanged, and V2 id → V3 id maps are built by matching it
# (see sync_id_map). Never generate one here.
def _v2_uuid(record: dict) -> str | None:
    return str(record["uuid"]).strip().lower() if record.get("uuid") else None


def transform_record(record: dict, transform_key: str, org_cfg: dict, facility: str = "") -> dict | None:
    """Apply V2→V3 field mapping to a single record dict.

    Returns None if the record fails a required-field check and should be skipped.

    Steps:
      0. Strip V2's embedded relation blobs
      1. Global boolean renames
      2. Global bare-FK renames (skip if _id form already exists)
      3. Per-key renames
      4. Per-key field drops
      4b. Per-key cross-facility uniqueness fix-ups
      4c. Per-key corruption guard (drop + log only visibly-corrupted PII)
      5. Per-key coercions
      5b. Per-key injections (generate V3-required fields absent from V2)
      6. Required-field validation
    """
    out = dict(record)
    tk = transform_key if transform_key in _PER_KEY_RENAMES else "generic"

    # 0. Strip V2's embedded relation blobs. The V2 API eagerly embeds
    # related models inline as a convenience — a scalar FK like "doctor_id"
    # sits right next to a full "doctor": {...} object, or a hasMany relation
    # shows up as "schemes": [{...}, {...}]. Confirmed on real dead-lettered
    # Admission ("doctor"/"ward"/"bed"), Prescription ("users"/"payment") and
    # Patient ("schemes") records — every single nested value seen in
    # production data has been one of these, never real V3 column data. V3's
    # Eloquent models have no column for the embedded object/collection and
    # error out with an opaque, undiagnosable 500 if it's sent. Run this
    # FIRST, before any per-key coercion constructs its own intentional
    # nested value (e.g. eye_exam's right_eye_data/left_eye_data), so those
    # survive untouched.
    for field, (parent, key) in _PER_KEY_LIFT_NESTED.get(tk, {}).items():
        if not out.get(field) and isinstance(out.get(parent), dict):
            out[field] = out[parent].get(key)
    for field in list(out.keys()):
        v = out[field]
        if isinstance(v, dict) or (isinstance(v, list) and v and isinstance(v[0], dict)):
            out.pop(field)

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

    # 3b. V2 uses 0 for "no parent" in FK columns (samples: procedure_id=0,
    # region_id=0). V3 ids start at 1, so 0 can only fail the FK
    # constraint (generic 500) — send null, which is what V3 means by none.
    for field, v in out.items():
        if field.endswith("_id") and field not in _ZERO_IS_NOT_NULL and v in (0, "0"):
            out[field] = None

    # 3c. A V2 flag column holding both 0/1 and true/false is flattened as
    # text, so it arrives as 'true'/'false' — V3's tinyint columns reject
    # that ("Incorrect integer value: 'false' for column 'for_discharge'").
    for field, v in out.items():
        if isinstance(v, str) and v.lower() in ("true", "false"):
            out[field] = int(v.lower() == "true")

    # 4. Per-key drops (fields that cannot be mapped in API-to-API)
    for field in _PER_KEY_DROP_FIELDS.get(tk, []):
        out.pop(field, None)

    # 4b. Per-key cross-facility uniqueness fix-ups. Confirmed 2026-10 by
    # isolated A/B testing against the live gateway: every facility shares
    # ONE V3 tenant (organization_id=1), but fields like patient_no are only
    # unique *within* a V2 facility — two facilities' patient #45705 are two
    # different people that collide on V3's unique constraint and come back
    # as the same opaque "Something went wrong" 500 (no distinguishing detail
    # is returned, so this can't be caught and retried reactively; it has to
    # be avoided proactively).
    #
    # The facility prefix alone isn't enough, though — confirmed on a real
    # record (afya_api_auth patient_no=47193) that V2's own patient_no isn't
    # even guaranteed unique *within* one facility: two different real
    # patients there both have raw patient_no 47193, which still collide
    # once both get the same "{facility}-47193" prefix. Appending the
    # record's own V2 "id" (its table primary key, always unique within that
    # facility regardless of whatever patient_no duplication V2 has) closes
    # this completely — confirmed accepted the same way the plain prefix was.
    #
    # The unique index also spans organizations: loading the same facility
    # into a second org collides with the copies already in the first one.
    # For any org other than 1 the prefix also carries the org id; org 1
    # keeps the original format so re-runs of loads already there still
    # produce identical values (uuid-less records have no match_on, so a
    # changed patient_no would insert a duplicate instead of colliding).
    if facility:
        org_id = org_cfg.get("organization_id")
        prefix = facility if org_id in (None, 1) else f"{facility}-org{org_id}"
        for field in _PER_KEY_FACILITY_SCOPE_FIELDS.get(tk, []):
            if out.get(field) is not None:
                out[field] = f"{prefix}-{out[field]}-{out.get('id', '')}"

    # 4c. Per-key corruption guard — only touch a watched field if its value
    # is visibly corrupted (contains U+FFFD). Replaced with a SHA-256 hash of
    # the corrupted text rather than null/dropped: confirmed by direct
    # testing that V3 accepts an arbitrary hash string in every one of these
    # fields exactly like it accepts the mojibake itself (no format
    # validation blocks it). A hash isn't the real value — it can't be,
    # the real bytes are already gone — but it's a clean, non-null,
    # deterministic placeholder: two corrupted records with byte-for-byte
    # identical ciphertext (seen in practice — V2 sometimes reuses the same
    # ciphertext across multiple fields/records) hash identically, which is
    # a free signal for a backend investigation. Every replacement is also
    # logged so affected patients are a trackable backlog, not silent loss.
    # V2 now decrypts PII at source (confirmed 2026-10-07: names/phones arrive
    # as plain text), so values go to V3 exactly as received. A value that is
    # still visibly corrupted is logged for follow-up but no longer replaced;
    # set ENCODE_CORRUPTED_PII=1 to bring back the encoded placeholder.
    # Garbled values (V2 data that was corrupted before decryption) are sent
    # as null; the encoded form is stashed under GARBLED_KEY so the poster can
    # retry once with it if V3 refuses the null. GARBLED_KEY never reaches V3.
    for field in _PER_KEY_CORRUPTION_WATCH.get(tk, []):
        v = out.get(field)
        if isinstance(v, str) and "�" in v:
            if ENCODE_CORRUPTED_PII:
                out[field] = _encode_corrupted_value(v)
            else:
                out.setdefault(GARBLED_KEY, {})[field] = _encode_corrupted_value(v)
                out[field] = None

    # 5. Per-key coercions (after any pre-coercion hook, which still sees the raw values)
    if tk in _PER_KEY_PRE_COERCE:
        _PER_KEY_PRE_COERCE[tk](out)
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

    # 5b. Populate visit→patient side-channel so doctor_note can derive patient_id.
    # Read from `out`, not `record`: V2 visits carry a bare "patient" column
    # that only becomes "patient_id" after the global FK rename in step 2.
    if tk == "reception_visit" and out.get("id") and out.get("patient_id"):
        _record_visit_patient(int(out["id"]), int(out["patient_id"]))

    # Populate visit→admission side-channel so Vitals can be split into
    # inpatient (has an admission) vs outpatient (visit only).
    if tk == "inpatient_admission" and record.get("id") and record.get("visit_id"):
        _record_visit_admission(int(record["visit_id"]), int(record["id"]))

    # 5c. Per-key injections — add V3-required fields that V2 never had
    for field, fn in _PER_KEY_INJECT.get(tk, {}).items():
        if not out.get(field):
            out[field] = fn(out)

    # 5d. facility_id is the DESTINATION facility for models whose gateway
    # entry doesn't declare a facility column (V2 holds the old facility id)
    if tk in _PER_KEY_DEST_FACILITY and org_cfg.get("facility_id") is not None:
        out["facility_id"] = org_cfg["facility_id"]

    keep = _PER_KEY_KEEP_ONLY.get(tk)
    if keep is not None:
        # id / uuid always survive: they key progress tracking and the id map
        # (the gateway strips id from inserts itself)
        out = {k: v for k, v in out.items() if k in keep or k in ("id", "uuid")}

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
    user  = (os.getenv(f"FACILITY_{upper}_USERNAME") or "").strip()
    pwd   = (os.getenv(f"FACILITY_{upper}_PASSWORD") or "").strip()
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
_v3_org_cfg_cache: dict | None = None             # derived from the login response itself
_v3_token_lock = threading.Lock()
_v3_target_facility: str | None = None            # facility key whose V3 tenant we log into


def set_v3_target_facility(facility: str | None) -> None:
    """Point V3 login at one facility key's destination tenant: uses
    AFYA_<FACILITY>_USERNAME / AFYA_<FACILITY>_PASSWORD when set (falling
    back to AFYA_USERNAME / AFYA_PASSWORD), and on a step-2 multi-facility
    login picks FACILITY_V3_CONFIG[facility]['facility_id'] instead of
    whichever facility the account happens to list first. Drops any cached
    token so the next call logs in for the new target."""
    global _v3_target_facility, _v3_token_cache, _v3_org_cfg_cache
    _apply_v3_urls(facility)
    with _v3_token_lock:
        _v3_target_facility = facility
        _v3_token_cache = None
        _v3_org_cfg_cache = None


def _v3_credentials() -> tuple[str, str, str]:
    """(username, password, env-var prefix used) for the current V3 target."""
    if _v3_target_facility:
        prefix = f"AFYA_{_v3_target_facility.upper()}"
        user = (os.getenv(f"{prefix}_USERNAME") or "").strip()
        pwd  = (os.getenv(f"{prefix}_PASSWORD") or "").strip()
        if user and pwd:
            return user, pwd, prefix
    user = (os.getenv("AFYA_USERNAME") or "").strip()
    pwd  = (os.getenv("AFYA_PASSWORD") or "").strip()
    return user, pwd, "AFYA"


def _v3_session() -> requests.Session:
    global _v3_session_singleton
    with _v3_session_lock:
        if _v3_session_singleton is None:
            s = requests.Session()
            # one keep-alive connection per parallel POST, or the extra
            # workers queue for a connection instead of posting
            pool = max(32, RECORD_WORKERS)
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=pool, pool_maxsize=pool, max_retries=0,
            )
            s.mount("https://", adapter)
            s.mount("http://", adapter)
            _v3_session_singleton = s
        return _v3_session_singleton


def _generate_v3_token() -> tuple[str, dict]:
    """Logs in against core's /v1/login and returns (access_token, org_cfg).

    org_cfg is derived from the LOGIN RESPONSE itself — tenant/facility are
    NOT something the caller gets to pick: source_tenant_id/
    destination_tenant_id must equal the authenticated account's own
    organization or every gateway call 403s ("foreign tenant"), so whatever
    org this account belongs to IS the only valid destination.
    """
    user, pwd, cred_prefix = _v3_credentials()
    if not user or not pwd:
        raise RuntimeError("Missing AFYA_USERNAME / AFYA_PASSWORD env vars")
    expected = facility_v3_config(_v3_target_facility)
    url = f"{V3_SERVICES['core'].rstrip('/')}/v1/login"
    body = {"username": user, "password": pwd}

    r = _v3_session().post(url, json=body, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"V3 auth failed: {r.status_code} · {r.text[:200]}")
    res = r.json()

    if res.get("step") == 2:
        # Multi-facility account — the account has several facilities and
        # needs one named explicitly to complete login.
        facilities = res.get("allowed_facility_ids") or []
        if not facilities:
            raise RuntimeError(
                f"V3 auth returned step 2 (pick a facility) but no allowed_facility_ids: {r.text[:200]}"
            )
        wanted = expected.get("facility_id")
        if wanted is not None and wanted not in facilities:
            raise RuntimeError(
                f"V3 account {user!r} ({cred_prefix}_USERNAME) can't log into facility "
                f"{wanted} configured for {_v3_target_facility} — allowed_facility_ids="
                f"{facilities}. Set AFYA_{_v3_target_facility.upper()}_USERNAME/"
                f"_PASSWORD to an account in that facility."
            )
        body["facility_id"] = wanted if wanted is not None else facilities[0]
        r = _v3_session().post(url, json=body, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"V3 auth (step 2) failed: {r.status_code} · {r.text[:200]}")
        res = r.json()

    token = res.get("access_token")
    if not token:
        raise RuntimeError(f"V3 token not found in auth response: {r.text[:200]}")

    # core's login nests the account under `user` (confirmed against the live
    # response), not `data` — check both since that's what the Postman
    # collection's own test script does.
    # import pdb;pdb.set_trace()
    user_obj = res.get("user") or res.get("data") or {}
    org_id = (res.get("tenant") or {}).get("id") or user_obj.get("organization_id")
    fac_id = (
        (res.get("facility") or {}).get("id")
        or (res.get("allowed_facility_ids") or [None])[0]
        or user_obj.get("facility_id")
    )
    log.info("V3 login ok — user=%s org=%s facility=%s roles=%s",
              user_obj.get("username"), org_id, fac_id,
              ",".join(r.get("name", "") for r in (user_obj.get("roles") or [])))
    # The tenant comes from the account, so a wrong account silently loads
    # into the wrong org — fail instead when it disagrees with the config.
    if expected.get("organization_id") is not None and org_id != expected["organization_id"]:
        raise RuntimeError(
            f"V3 account {user!r} ({cred_prefix}_USERNAME) belongs to organization {org_id}, "
            f"but FACILITY_V3_CONFIG[{_v3_target_facility!r}] expects organization "
            f"{expected['organization_id']}. Set AFYA_{_v3_target_facility.upper()}_USERNAME/"
            f"_PASSWORD to an account in that organization."
        )
    # user_id: the migrating account itself — v3_passthrough uses it for
    # NOT NULL creator columns whose original user isn't in V3.
    return token, {"organization_id": org_id, "facility_id": fac_id, "application_id": 1,
                   "user_id": user_obj.get("id")}


def _v3_token() -> str:
    global _v3_token_cache, _v3_org_cfg_cache
    with _v3_token_lock:
        if _v3_token_cache and (time.time() - _v3_token_cache[1]) < TOKEN_TTL_SECONDS:
            return _v3_token_cache[0]
        token, org_cfg = _generate_v3_token()
        _v3_token_cache = (token, time.time())
        _v3_org_cfg_cache = org_cfg
        log.debug("V3 token refreshed")
        return token


def v3_login_org_cfg() -> dict:
    """organization_id / facility_id derived from the V3 login response —
    the authoritative destination tenant for this account (see
    _generate_v3_token's docstring). Triggers a login if not cached yet."""
    _v3_token()
    return _v3_org_cfg_cache or {}


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


def extract_v2_records(job: dict) -> tuple[list[dict], list[int]]:
    """Paginate V2 API and return (rows, failed_pages) for the given job.

    A page that exhausts all retries is skipped rather than aborting the whole
    job — one permanently-broken page (e.g. a corrupt V2 record crashing the
    server) would otherwise block every other page's data forever. Skipped
    page numbers are returned in failed_pages so the caller can avoid marking
    the job fully done (watermark must not advance on incomplete data).
    """
    facility = job["facility"]
    cfg      = v2_facility_config(facility)
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
    max_pages  = job.get("max_pages")  # None = unlimited

    if not has_more:
        return all_rows, []

    def _fetch_page(p: int) -> tuple[int, list, str | None]:
        try:
            r2, _ = _post_with_retry(
                url=url, headers=headers,
                bodies=[{**chosen_body, "page": p}],
                facility=facility, session=session,
            )
            return p, _extract_rows_from_payload(r2.json()), None
        except Exception as e:
            log.error("  ✗ ns=%s page=%s permanently failed after retries — skipping page: %s",
                       job["namespace"], p, e)
            return p, [], str(e)

    if last_page is not None:
        last_page = min(int(last_page), 10_000)
        if max_pages is not None:
            last_page = min(last_page, max_pages)
        pages = list(range(2, last_page + 1))
        page_rows: dict[int, list] = {}
        failed_pages: list[int] = []
        with ThreadPoolExecutor(max_workers=max(1, PAGE_WORKERS)) as pool:
            for fut in as_completed(pool.submit(_fetch_page, p) for p in pages):
                p, rows, err = fut.result()
                page_rows[p] = rows
                if err is not None:
                    failed_pages.append(p)
        for p in pages:
            all_rows.extend(page_rows.get(p, []))
        if failed_pages:
            failed_pages.sort()
            log.warning("  ns=%s — %d/%d page(s) failed and were skipped: %s",
                        job["namespace"], len(failed_pages), len(pages), failed_pages)
        return all_rows, failed_pages

    # Sequential fallback when last_page unknown
    page = 1
    failed_pages = []
    while has_more:
        page += 1
        if page > 10_000:
            log.warning("Pagination safety stop at page %s", page)
            break
        if max_pages is not None and page > max_pages:
            log.info("Pagination capped at %s pages (--max-pages)", max_pages)
            break
        try:
            r2, _ = _post_with_retry(
                url=url, headers=headers,
                bodies=[{**chosen_body, "page": page}],
                facility=facility, session=session,
            )
        except Exception as e:
            log.error("  ✗ ns=%s page=%s permanently failed after retries — stopping pagination early: %s",
                       job["namespace"], page, e)
            failed_pages.append(page)
            break
        payload2 = r2.json()
        rows = _extract_rows_from_payload(payload2)
        all_rows.extend(rows)
        pagination = payload2.get("pagination") or {}
        has_more  = bool(pagination.get("has_more_pages", False))
        if not rows:
            break

    return all_rows, failed_pages


class GatewayModelNotRegistered(Exception):
    """Raised when the gateway returns 422 'model not registered' for a namespace."""


# ─── GATEWAY MODEL DISCOVERY ─────────────────────────────────────────────────

# alias → {tenant: str|None, facility: str|None, operations: list}
_gateway_model_meta: dict[str, dict] = {}


def _fetch_available_models() -> set[str]:
    """Query every V3 service gateway, union the insertable aliases, and build _alias_to_service.

    Services whose auth isn't configured (core's HMAC secret, theatre/
    dialysis's migration key) or whose URL is still a placeholder are
    skipped with a warning, not fatal — their models just won't show up as
    available, so jobs targeting them get cleanly excluded downstream."""
    global _gateway_model_meta, _alias_to_service
    available: set[str] = set()
    body = {"action": "list"}
    for service_name in V3_SERVICES:
        try:
            r = _gateway_post(service_name, body, timeout=30)
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
                # import pdb;pdb.set_trace()
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


# ─── DEAD-LETTER ─────────────────────────────────────────────────────────────

_dead_letter_lock = threading.Lock()


def _encode_corrupted_value(value: str) -> str:
    """Base64 encoding of an already-corrupted (U+FFFD-containing) field
    value. Not a way to recover the real value — the original encrypted
    bytes are already gone by the time we see U+FFFD — just a clean,
    deterministic, non-null placeholder. Unlike a one-way hash, base64 is
    reversible: decoding it hands back the exact (already-mangled) text we
    received, byte-for-byte, which is occasionally useful for inspection.
    Two records whose V2 ciphertext happens to be byte-for-byte identical
    (confirmed to occur in practice) still encode identically, which is a
    useful free signal when investigating the encryption bug on the V2 side.
    """
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


class RecordDeadLettered(Exception):
    """Raised when a record is written to the dead-letter file instead of inserted.

    Distinct from a plain return so the caller (post_to_v3._post_one) can avoid
    marking the record as inserted — a dead-lettered record must be retried on
    the next run once the underlying data issue is fixed, not skipped forever.
    """


_REDACT_FIELDS = {"password", "remember_token", "api_token"}


def _write_dead_letter(v3_namespace: str, record: dict, error_body: dict) -> None:
    """Append one failed record to the JSONL dead-letter file for later replay."""
    entry = {
        "ts":        datetime.now(timezone.utc).isoformat(),
        "namespace": v3_namespace,
        # never write a credential to disk (users get a generated password)
        "record":    {k: ("<redacted>" if k in _REDACT_FIELDS and v else v) for k, v in record.items()},
        "error":     error_body,
    }
    with _dead_letter_lock:
        with DEAD_LETTER_FILE.open("a") as fh:
            fh.write(_dumps(entry) + "\n")
    log.warning("  Dead-letter → %s  (namespace=%s)", DEAD_LETTER_FILE.name, v3_namespace)


# ─── V3 POST ─────────────────────────────────────────────────────────────────

def _uncertain_dead_letter(v3_namespace: str, record: dict, why: str) -> None:
    """Park a uuid-less record whose POST may or may not have landed. It is
    not marked inserted and not retried in this run — check V3 for it before
    replaying, otherwise it could be inserted twice."""
    log.error("  V3 id=%-6s %s with no uuid — may already be in V3; NOT retrying (avoids a duplicate) → dead-letter",
              record.get("id", "?"), why)
    _write_dead_letter(v3_namespace, record, {
        "reason": f"{why}; record has no uuid so a retry could duplicate it — verify in V3 before replaying",
    })
    raise RecordUncertain()


class RecordUncertain(RecordDeadLettered):
    """Dead-lettered AND possibly inserted. post_to_v3 remembers these under
    '<job_key>::uncertain' so later runs skip them too instead of re-posting."""


def _post_to_v3_batch(
    v3_namespace: str,
    org_cfg: dict,
    record: dict,
    *,
    max_retries: int = 3,
    default_retry_wait: int = 5,
    backoff_factor: int = 2,
    service_override: str | None = None,
    alias_override: str | None = None,
    match_on: str | None = None,
) -> None:
    """POST a single record object to the V3 gateway. Retries on 429/5xx/401.

    match_on: the column the gateway upserts on (default "uuid"). With a
    match key present, a re-post updates the existing V3 row in place
    (200 created:false) instead of inserting.

    service_override bypasses the alias→service discovery lookup — required
    whenever two services register the same model alias (e.g. "vital" exists
    in both inpatient-service and patient-evaluation-service with different
    schemas); the discovery-based lookup can only remember one winner.

    alias_override bypasses _v3_alias()'s own resolution — required whenever
    singular and plural forms of a name are two genuinely DIFFERENT models
    under different services, not just a naming variant of the same one.
    Confirmed for Visit: "visit" (singular, reception service, insertable)
    and "visits" (plural, evaluation service / evaluationmigrate database)
    are separate tables — _v3_alias() always prefers the singular form when
    both exist, with no way to know they aren't the same thing. The real
    V3 Laravel error log (SQLSTATE in_morgue/inpatient NOT NULL violations
    against database "evaluationmigrate", app "patient-evaluation-service")
    confirmed "visits" is the one the application actually uses.
    """
    record       = {k: v for k, v in record.items() if k != GARBLED_KEY}
    alias        = alias_override or _v3_alias(v3_namespace)
    service_name = service_override or _alias_to_service.get(alias, "core")
    # Facility-scoped models (the gateway's `facility` column, e.g. ward/bed's
    # facility_id) must carry the DESTINATION facility: V2 rows hold the old
    # system's facility id, which the gateway rejects with 403 "facility_id 1
    # is not a facility you are assigned to".
    facility_col = (_gateway_model_meta.get(alias) or {}).get("facility")
    if facility_col and org_cfg.get("facility_id") is not None:
        record = {**record, facility_col: org_cfg["facility_id"]}
    body = {
        "action":                "insert",
        "model":                 alias,
        "destination_tenant_id": org_cfg.get("organization_id"),
        # UNIQUE(uuid) on every insertable table — without match_on the index
        # rejects a repeat outright; with it, a re-run updates in place
        # (200 created:false) instead of duplicating, which is what makes a
        # bulk load restartable.
        "match_on":              match_on or "uuid",
        "data":                  record,  # gateway expects a single object, not an array
    }
    # A record without a value for the match column must not be sent with
    # match_on — the gateway would match it against an existing row and
    # overwrite that row (200 created:false) instead of inserting.
    if not record.get(body["match_on"]):
        body.pop("match_on")

    attempt, wait, _patched = 0, default_retry_wait, False
    gw_attempt, gw_wait = 0, V3_RETRY_WAIT
    while True:
        attempt += 1
        try:
            # _gateway_post re-serializes + re-signs body fresh every call,
            # so a patched body["data"] below is always signed correctly.
            r = _gateway_post(service_name, body, timeout=120)
            log.info("  V3 POST ns=%-50s status=%s",
                     v3_namespace, r.status_code)

            if r.status_code == 401:
                if attempt >= max_retries:
                    r.raise_for_status()
                _v3_invalidate_token()
                log.warning("  V3 401 — refreshed auth (%s/%s)", attempt, max_retries)
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

            # The gateway now answers a rejected insert with 422 "insert
            # rejected: <SQL error>" where it used to 500 — same errors, same
            # recovery (NULL default, unknown column, duplicate).
            if r.status_code == 500 or (r.status_code == 422 and "insert rejected" in r.text):
                try:
                    err_body = r.json()
                except Exception:
                    err_body = {}
                # Gateway error shape is {"message", "error", "exception", "details": "<SQL error>"}
                # — there is no "debug" key. Fall back through every shape we've seen so the
                # NOT NULL / duplicate / unknown-column pattern matches below actually run.
                debug_msg = (
                    (err_body.get("debug") or {}).get("message")
                    or err_body.get("details")
                    or err_body.get("message")
                    or ""
                )
                rec_id    = record.get("id", "?")
                reason    = debug_msg or r.text[:300]
                # Production hides the SQL error; error_id is the reference the
                # backend team needs to find it in the V3 server log.
                if isinstance(err_body, dict) and err_body.get("error_id"):
                    reason = f"error_id={err_body['error_id']} {reason}"

                # Category 1 — NOT NULL / missing-default constraint: patch and retry
                # Catches both:
                #   SQLSTATE[23000] "Column 'x' cannot be null"       (null sent explicitly)
                #   SQLSTATE[HY000] "Field 'x' doesn't have a default value"  (column omitted)
                null_col = re.search(
                    r"(?:Column|Field) '(\w+)' (?:cannot be null|doesn't have a default value)",
                    debug_msg,
                )
                if null_col:
                    col     = null_col.group(1)
                    default = _V3_NULL_DEFAULTS.get(col)
                    if default is not None and body["data"].get(col) is None:
                        log.warning(
                            "  V3 %s id=%-6s NULL constraint on '%s' — injecting default %r and retrying",
                            r.status_code, rec_id, col, default,
                        )
                        body["data"] = {**body["data"], col: default}
                        _patched = True
                        continue  # one retry with the patched record
                    log.error(
                        "  V3 %s id=%-6s NULL constraint on '%s' — no default in _V3_NULL_DEFAULTS "
                        "→ dead-letter  |  reason: %s",
                        r.status_code, rec_id, col, reason,
                    )
                    _write_dead_letter(v3_namespace, record, err_body)
                    raise RecordDeadLettered()

                # NOTE: a duplicate unique value is NOT retried with the V2 id
                # appended. That retry (2026-10-12) re-created 1,763 procedures
                # that were already in V3 (ADMIN-00001 → ADMIN-00001-29552): the
                # taken code was the SAME record. Codes V2 itself duplicates are
                # made unique before posting instead (snowflake_to_v3_migration
                # ._dedupe_v2_codes), so a duplicate here means "already exists".

                # Category 2 — Duplicate entry: record already exists in V3, skip silently
                if "Duplicate entry" in debug_msg:
                    log.info(
                        "  V3 %s id=%-6s duplicate — already exists in V3, skipping  |  reason: %s",
                        r.status_code, rec_id, reason,
                    )
                    return

                # Category 2.5 — Unknown column: V2 has a column V3 schema doesn't.
                # Strip it from the payload and retry immediately.
                unknown_col = re.search(r"Unknown column '(\w+)'", debug_msg)
                if unknown_col:
                    col = unknown_col.group(1)
                    if col in body["data"]:
                        log.warning(
                            "  V3 %s id=%-6s Unknown column '%s' — dropping and retrying",
                            r.status_code, rec_id, col,
                        )
                        body["data"] = {k: v for k, v in body["data"].items() if k != col}
                        if body.get("match_on") == col:
                            # never match on a value we no longer send — the
                            # gateway would overwrite an existing row instead
                            body.pop("match_on")
                        continue
                    # Column isn't in our payload (computed by SQL) — nothing to strip
                    log.error(
                        "  V3 %s id=%-6s Unknown column '%s' not in payload → dead-letter",
                        r.status_code, rec_id, col,
                    )
                    _write_dead_letter(v3_namespace, record, err_body)
                    raise RecordDeadLettered()

                # Category 2.6 — Value too long / out of range for an optional V3
                # column (_V3_NULL_WHEN_UNFIT): send it as null and retry
                unfit = re.search(r"(Data too long|Out of range value) for column '(\w+)'", debug_msg)
                if unfit and unfit.group(2) in _V3_NULL_WHEN_UNFIT:
                    col = unfit.group(2)
                    if body["data"].get(col) is not None:
                        log.warning(
                            "  V3 %s id=%-6s '%s' %s (%r) — sending null and retrying",
                            r.status_code, rec_id, col, unfit.group(1).lower(), str(body["data"][col])[:80],
                        )
                        body["data"] = {**body["data"], col: None}
                        continue

                # Category 3 — Generic server fault: dead-letter immediately, no retry.
                # 500s on insert are almost always data issues, not transient faults.
                log.error(
                    "  V3 %s id=%-6s server fault → dead-letter  |  reason: %s",
                    r.status_code, rec_id, reason,
                )
                # import pdb;pdb.set_trace()
                _write_dead_letter(v3_namespace, record, err_body)
                raise RecordDeadLettered()

            # A superadmin token that suddenly gets 403 "required role(s)" means
            # the service couldn't verify roles with core (seen right as
            # coremigrate went down, followed by 504s/401s) — transient, not a
            # real permission problem. Refresh the token and back off like a 504.
            role_check_failed = r.status_code == 403 and "required_roles" in r.text
            if role_check_failed:
                _v3_invalidate_token()

            if r.status_code in {502, 503, 504} or role_check_failed:
                # nginx gave up waiting on an overloaded upstream (504 lands
                # ~60s after the POST). Back off hard instead of re-hammering it
                # every few seconds. Retrying is safe for records with a uuid:
                # match_on=uuid turns a post that did land into an update.
                if r.status_code == 504 and "match_on" not in body:
                    # nginx timed out but the insert may still have landed.
                    # Without a uuid there is no match_on to make a retry
                    # safe, so a retry could insert it twice. Park it instead.
                    _uncertain_dead_letter(v3_namespace, record, "504 gateway timeout")
                gw_attempt += 1
                if gw_attempt > V3_GATEWAY_RETRIES:
                    log.error("  V3 %s id=%s — giving up after %d gateway retries",
                              r.status_code, record.get("id", "?"), V3_GATEWAY_RETRIES)
                    r.raise_for_status()
                log.warning("  V3 %s id=%s (%s) sleeping %ss (%s/%s)",
                            r.status_code, record.get("id", "?"),
                            "role check failed — core unreachable" if role_check_failed else "upstream timeout",
                            gw_wait, gw_attempt, V3_GATEWAY_RETRIES)
                time.sleep(gw_wait)
                gw_wait = min(gw_wait * backoff_factor, 300)
                continue

            if r.status_code == 422:
                # Validation / schema error on this one record — dead-letter and move on.
                # Do NOT raise: a single bad record must not kill the entire job.
                try:
                    err_body = r.json()
                except Exception:
                    err_body = {}
                rec_id = record.get("id", "?")
                log.error(
                    "  V3 422 id=%-6s validation error → dead-letter  |  error_id=%s %s",
                    rec_id, (err_body.get("error_id") if isinstance(err_body, dict) else None) or "-",
                    r.text[:500],
                )
                _write_dead_letter(v3_namespace, record, err_body)
                raise RecordDeadLettered()

            if not r.ok:
                log.error("  V3 %s response body: %s", r.status_code, r.text[:2000])
            r.raise_for_status()
            if V3_POST_THROTTLE > 0:
                time.sleep(V3_POST_THROTTLE)
            # Extract V3-assigned ID from response for FK remapping
            try:
                resp = r.json()
                # match_on=uuid turns a repeat into an in-place update: 200 with
                # created:false. That is NOT a new row — surface it, otherwise a
                # record with no uuid silently overwrites an existing V3 row.
                created = resp.get("created", (resp.get("data") or {}).get("created")
                                   if isinstance(resp.get("data"), dict) else None)
                if created is False:
                    v3_row = (resp.get("data") or {}).get("id") if isinstance(resp.get("data"), dict) else resp.get("id")
                    log.info("  V3 200 id=%-6s updated existing V3 row %s (uuid=%s)",
                             record.get("id", "?"), v3_row, record.get("uuid"))
                return (resp.get("id")
                        or (resp.get("data") or {}).get("id")
                        or (resp.get("success") or {}).get("id"))
            except Exception:
                return None

        except (Timeout, ConnectionError) as e:
            # A read timeout on the gateway call itself means the POST was sent
            # and may have been applied. (Timeouts on the token login, or
            # connect errors, mean it was never sent — safe to retry.)
            req = getattr(e, "request", None)
            if (isinstance(e, ReadTimeout) and "match_on" not in body
                    and req is not None and str(req.url).rstrip("/").endswith("/v1/gateway")):
                _uncertain_dead_letter(v3_namespace, record, f"read timeout: {e}")
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
    service_override: str | None = None,
) -> int:
    """POST records to V3 in parallel (gateway requires one object per request).

    Already-inserted records (by V2 id) are skipped for resume support.

    Returns the number of records dead-lettered. A dead-lettered record
    doesn't raise — the batch has to keep going — so this return value is
    the ONLY signal the caller has that the job wasn't fully clean; without
    it, a job where every single record 500'd would look identical to one
    where every record succeeded. Callers MUST treat a nonzero count the
    same way they treat V2 extraction failures: do not mark the job done.
    """
    uncertain_key = f"{job_key}::uncertain"
    pending = [
        r for r in records
        if not (r.get("id") is not None and job_key and _record_inserted(job_key, r.get("id")))
    ]
    skipped = len(records) - len(pending)
    if skipped:
        log.info("  Skipping %d records already inserted by earlier runs", skipped)
    if job_key:
        n_before = len(pending)
        pending = [r for r in pending if not (r.get("id") is not None and _record_inserted(uncertain_key, r["id"]))]
        if len(pending) != n_before:
            log.warning("  Skipping %d uuid-less record(s) whose earlier POST timed out and may already be in V3 "
                        "— verify in V3, then remove them from '%s' in %s to replay",
                        n_before - len(pending), uncertain_key, RECORD_PROGRESS_FILE.name)

    alias = _v3_alias(v3_namespace)


    # One V3 uuid scan for this model (used by uncertain reconcile + already-in-V3 skip)
    existing: dict[str, int] = {}
    if pending:
        existing = _existing_v3_uuids(alias, org_cfg, service_name=service_override)

    # ── Reconcile ::uncertain from earlier timed-out POSTs ─────────────────────
    if job_key:
        uncertain_ids = {
            r["id"] for r in pending
            if r.get("id") is not None and _record_inserted(uncertain_key, r["id"])
        }
        if uncertain_ids:
            existing = _existing_v3_uuids(alias, org_cfg, service_name=service_override)
            promoted, retryable, still_blocked = [], [], []
            still_pending = []
            for r in pending:
                rid = r.get("id")
                if rid is None or rid not in uncertain_ids:
                    still_pending.append(r)
                    continue
                u = _v2_uuid(r)
                if u and u in existing:
                    # POST did land — treat as successfully inserted
                    _store_id_mapping(alias, rid, existing[u])
                    _mark_record_inserted(job_key, rid)
                    promoted.append(rid)
                    # do not add to still_pending (already done)
                elif u:
                    # Has uuid but not in V3 — safe to retry
                    retryable.append(rid)
                    still_pending.append(r)
                else:
                    # No uuid — still must not auto-retry (duplicate risk)
                    still_blocked.append(rid)
                    # omit from pending
            pending = still_pending
            if promoted:
                log.info(
                    "  Uncertain reconcile: %d id(s) found in V3 by uuid — promoted to inserted",
                    len(promoted),
                )
            if retryable:
                log.info(
                    "  Uncertain reconcile: %d id(s) not in V3 — will retry POST",
                    len(retryable),
                )
            if still_blocked:
                log.warning(
                    "  Uncertain reconcile: %d uuid-less id(s) still blocked under '%s' "
                    "(verify in V3, then remove from %s to replay)",
                    len(still_blocked), uncertain_key, RECORD_PROGRESS_FILE.name,
                )
        # Skip records V3 already has: the uuid comes from V2 and is carried to V3
        # unchanged, so a uuid already present in V3 means the record is already
        # there (from an earlier run, a reset progress file, or another loader).
        # No re-post, no re-insert — just record the V2 id → V3 id mapping.
        if any(_v2_uuid(r) for r in pending):
            existing = _existing_v3_uuids(alias, org_cfg, service_name=service_override)
            already, still_pending = [], []
            for r in pending:
                u = _v2_uuid(r)
                (already if u and u in existing else still_pending).append(r)
            if already:
                _store_id_mappings(alias, [(r["id"], existing[_v2_uuid(r)]) for r in already if r.get("id") is not None])
                if job_key:
                    for r in already:
                        if r.get("id") is not None:
                            _mark_record_inserted(job_key, r["id"])
                log.info("  Skipping %d records already in V3 (matched on uuid) — mapped, not re-posted", len(already))
            pending = still_pending

    total = len(pending)
    log.info("  Posting %d new record(s) → %s", total, v3_namespace)
    done_count = 0
    dead_letter_count = 0

    def _post_one(record: dict) -> None:
        nonlocal done_count, dead_letter_count
        remapped = _remap_fks(record, transform_key, v3_namespace)
        record_id = record.get("id")
        try:
            v3_id = _post_to_v3_batch(v3_namespace, org_cfg, remapped, service_override=service_override)
        except RecordUncertain:
            if record_id is not None and job_key:
                _mark_record_inserted(uncertain_key, record_id)
            with _record_progress_lock:
                dead_letter_count += 1
        except RecordDeadLettered:
            # Not inserted — do NOT mark as done, so the next run retries it
            # once the underlying data issue is fixed instead of skipping it forever.
            with _record_progress_lock:
                dead_letter_count += 1
        else:
            if record_id is not None:
                # mapping first — see snowflake_to_v3_migration.post_table_to_v3
                if v3_id is not None:
                    _store_id_mapping(alias, record_id, v3_id)
                if job_key:
                    _mark_record_inserted(job_key, record_id)
        with _record_progress_lock:
            done_count += 1
            n = done_count
        if n % RECORD_LOG_EVERY == 0 or n == total:
            log.info("  Posted %d / %d → %s", n, total, v3_namespace)

    try:
        with ThreadPoolExecutor(max_workers=RECORD_WORKERS) as pool:
            futures = {pool.submit(_post_one, r): r for r in pending}
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    # 403 / exhausted 504 retries / network errors: the record was
                    # NOT inserted and not dead-lettered either. Count it, or the
                    # job gets marked done and these records are never retried.
                    rec = futures[fut]
                    log.error("  Failed record id=%s: %s", rec.get("id"), e)
                    with _record_progress_lock:
                        dead_letter_count += 1
    finally:
        # Always flush so inserted IDs are on disk even if the pool raised
        # or an outer path is about to abort the job.
        with _record_progress_lock:
            if _record_progress_unflushed or _inserted_ids:
                _flush_record_progress()
    if dead_letter_count:
        log.warning("  %d / %d record(s) dead-lettered → %s", dead_letter_count, total, v3_namespace)
    return dead_letter_count


# ─── WATERMARKS ──────────────────────────────────────────────────────────────

_wm_lock = threading.Lock()


def _load_watermarks() -> dict:
    """Load watermark JSON. Missing → {}; corrupt → bak + {}."""
    if not WATERMARK_FILE.exists():
        return {}

    try:
        data = json.loads(WATERMARK_FILE.read_text())
        if not isinstance(data, dict):
            raise ValueError(f"expected object, got {type(data).__name__}")
        return data
    except Exception as e:
        bak = WATERMARK_FILE.with_suffix(
            WATERMARK_FILE.suffix + f".bak.{int(time.time())}"
        )
        try:
            WATERMARK_FILE.replace(bak)
            log.warning(
                "Could not parse %s: %s — moved to %s and starting fresh",
                WATERMARK_FILE.name, e, bak.name,
            )
        except OSError:
            log.warning(
                "Could not parse %s: %s — starting fresh",
                WATERMARK_FILE.name, e,
            )
        return {}


def _get_watermark(facility: str, namespace: str, default: str = "1970-01-01T00:00:00Z") -> str:
    return _load_watermarks().get(f"{facility}|{namespace}", default)


def _save_watermarks(wm: dict) -> None:
    """Merge-safe, atomic write so parallel facility tasks cannot clobber each other
    and a crash mid-write cannot leave invalid JSON."""
    text = json.dumps(wm, indent=2, sort_keys=True)
    with _wm_lock, _file_lock(WATERMARK_FILE):
        # Another process may have written newer keys since we loaded; union them in.
        if WATERMARK_FILE.exists():
            try:
                on_disk = json.loads(WATERMARK_FILE.read_text())
                if isinstance(on_disk, dict):
                    merged = dict(on_disk)
                    merged.update(wm)
                    text = json.dumps(merged, indent=2, sort_keys=True)
            except (OSError, ValueError) as e:
                log.warning("Could not merge %s before writing: %s", WATERMARK_FILE.name, e)
        _atomic_write(WATERMARK_FILE, text)

# ─── PROGRESS CHECKPOINTS ────────────────────────────────────────────────────

_progress_lock = threading.Lock()
_completed_jobs: set[str] = set()

def _load_progress(run_id: str) -> set[str]:
    """Load jobs completed in an in-progress migration.

    run_id is used when *writing*; on load we accept any file so a restarted
    process still skips jobs already finished before the crash. Cross-run
    skip of fully successful jobs remains DONE_FILE / record progress.
    """
    if not PROGRESS_FILE.exists():
        return set()
    try:
        data = json.loads(PROGRESS_FILE.read_text())
        completed = set(data.get("completed", []))
        file_run = data.get("run_id")
        if file_run and file_run != run_id and completed:
            log.info(
                "Loaded %d completed job(s) from progress file (file run_id=%s, this run_id=%s)",
                len(completed), file_run, run_id,
            )
        return completed
    except Exception as e:
        bak = PROGRESS_FILE.with_suffix(PROGRESS_FILE.suffix + f".bak.{int(time.time())}")
        try:
            PROGRESS_FILE.replace(bak)
            log.warning(
                "Could not parse %s: %s — moved to %s; treating run as having no completed jobs",
                PROGRESS_FILE.name, e, bak.name,
            )
        except OSError:
            log.warning(
                "Could not parse %s: %s — treating run as having no completed jobs",
                PROGRESS_FILE.name, e,
            )
        return set()


def _mark_done(run_id: str, job_key: str) -> None:
    with _progress_lock:
        _completed_jobs.add(job_key)
        _atomic_write(PROGRESS_FILE, json.dumps(
            {"run_id": run_id, "completed": sorted(_completed_jobs)},
            indent=2,
        ))
  


# ─── CROSS-RUN DONE TRACKING ─────────────────────────────────────────────────
# Jobs recorded here are skipped before V2 is even called on any future run.
# Delete .migration_done.json (or use --no-resume) to force a full re-migration.

_done_lock             = threading.Lock()
_permanently_done: set[str] = set()


def _load_permanently_done() -> None:
    global _permanently_done
    if not DONE_FILE.exists():
        _permanently_done = set()
        return
    try:
        data = json.loads(DONE_FILE.read_text())
        _permanently_done = set(data.get("done", []))
        if _permanently_done:
            log.info(
                "Skipping %d already-migrated jobs (delete %s to re-run them)",
                len(_permanently_done), DONE_FILE.name,
            )
    except Exception as e:
        bak = DONE_FILE.with_suffix(DONE_FILE.suffix + f".bak.{int(time.time())}")
        try:
            DONE_FILE.replace(bak)
            log.warning(
                "Could not load %s: %s — moved to %s; starting with empty done list",
                DONE_FILE.name, e, bak.name,
            )
        except OSError:
            log.warning(
                "Could not load %s: %s — starting with empty done list",
                DONE_FILE.name, e,
            )
        _permanently_done = set()


def _mark_permanently_done(job_key: str) -> None:
    with _done_lock, _file_lock(DONE_FILE):
        _permanently_done.add(job_key)
        # union with what other processes (parallel Airflow tasks) marked done
        try:
            _permanently_done.update(json.loads(DONE_FILE.read_text()).get("done", []))
        except (OSError, ValueError):
            pass
        _atomic_write(DONE_FILE, json.dumps({"done": sorted(_permanently_done)}, indent=2))


def _job_key(facility: str, namespace: str) -> str:
    return f"{facility}|{namespace}"


# ─── RECORD-LEVEL PROGRESS ───────────────────────────────────────────────────
# Tracks individual V2 record IDs that were successfully inserted so that a
# re-run after a mid-job failure skips already-inserted records.

_record_progress_lock = threading.Lock()
_inserted_ids: dict[str, set] = {}  # job_key → set of inserted V2 ids


def _read_state_json(path: Path, attempts: int = 10, wait: float = 2.0) -> tuple[dict, int]:
    """(parsed content, mtime_ns) of a state file. A file that exists but
    doesn't parse is almost always another process mid-write (older code
    rewrote these in place on every insert), so re-read for a while — and if
    it still won't parse, stop. Carrying on with an empty map would hold
    every child record back; an empty progress file would re-post records
    that are already in V3."""
    for attempt in range(1, attempts + 1):
        try:
            sig = _file_sig(path)
            return json.loads(path.read_text()), sig
        except (OSError, ValueError) as e:
            if attempt == attempts:
                raise RuntimeError(
                    f"{path.name} exists but can't be read ({e}). Another migration process may be "
                    f"writing it — wait for it, or restore the newest {path.name}.bak_*."
                ) from e
            log.warning("%s unreadable (%s) — another process writing it? retry %d/%d in %.0fs",
                        path.name, e, attempt, attempts - 1, wait)
            time.sleep(wait)


def _load_record_progress() -> None:
    global _inserted_ids, _record_progress_mtime
    if RECORD_PROGRESS_FILE.exists():
        data, _record_progress_mtime = _read_state_json(RECORD_PROGRESS_FILE)
        _inserted_ids = {k: set(v) for k, v in data.items()}
        total = sum(len(v) for v in _inserted_ids.values())
        if total:
            log.info("Record progress loaded — %d records already inserted across %d jobs",
                     total, len(_inserted_ids))
    else:
        _inserted_ids = {}


def _record_inserted(job_key: str, record_id) -> bool:
    with _record_progress_lock:
        return record_id in _inserted_ids.get(job_key, set())


_record_flush_counter = 0
_record_progress_unflushed = False   # inserted ids not yet on disk (see _flush_state_at_exit)
_record_last_flush_time = 0.0
_record_progress_mtime: tuple | None = None   # _file_sig of the version we last read/wrote


@contextlib.contextmanager
def _file_lock(path: Path):
    """Inter-process lock for a state file's read-merge-write (several
    processes — parallel Airflow table tasks, CLI runs — update the same
    files). Without it two processes can read the same version and the later
    write drops the other's newest entries. flock on a sidecar file; a no-op
    where fcntl doesn't exist (Windows)."""
    if fcntl is None:
        yield
        return
    with open(path.with_name(path.name + ".lock"), "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def _file_sig(path: Path) -> tuple:
    """Identity of a state file's current version. The inode is in it
    because every _atomic_write creates a new one: mtimes tick only every
    few ms, so two processes writing within one tick leave equal mtimes."""
    st = path.stat()
    return (st.st_ino, st.st_mtime_ns, st.st_size)


def _atomic_write(path: Path, text: str) -> tuple:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()           # push from Python to OS
        os.fsync(f.fileno())  # force OS to write to disk
    os.replace(tmp, path)   # now the rename is safe
    return _file_sig(path)


def _changed_on_disk(path: Path, last_sig: tuple | None) -> bool:
    try:
        return _file_sig(path) != last_sig
    except FileNotFoundError:
        return False


def _flush_record_progress() -> None:
    """Caller holds _record_progress_lock. If another process (the DAG, a
    second CLI run) wrote the file since we last did, union its ids in first
    — ids are only ever added, and writing just our own view would erase
    theirs, and for records without a uuid an erased id means a duplicate
    insert on the next run."""
    global _record_progress_mtime, _record_progress_unflushed, _record_last_flush_time
    # mappings first: an id saved as inserted must have its V3 id on disk too
    _flush_id_map()
    _record_progress_unflushed = False
    _record_last_flush_time = time.monotonic()
    with _file_lock(RECORD_PROGRESS_FILE):
        if _changed_on_disk(RECORD_PROGRESS_FILE, _record_progress_mtime):
            try:
                for k, ids in json.loads(RECORD_PROGRESS_FILE.read_text()).items():
                    _inserted_ids.setdefault(k, set()).update(ids)
            except (OSError, ValueError) as e:
                log.warning("Could not merge %s before writing: %s", RECORD_PROGRESS_FILE.name, e)
        _record_progress_mtime = _atomic_write(RECORD_PROGRESS_FILE, json.dumps(
            {k: sorted(v, key=str) for k, v in _inserted_ids.items()},
            indent=2,
        ))


def _mark_record_inserted(job_key: str, record_id) -> None:
    global _record_flush_counter, _record_progress_unflushed
    with _record_progress_lock:
        _inserted_ids.setdefault(job_key, set()).add(record_id)
        _record_flush_counter += 1
        _record_progress_unflushed = True
        by_count = _record_flush_counter % RECORD_FLUSH_EVERY == 0
        by_time = (
            RECORD_FLUSH_SECONDS > 0
            and (time.monotonic() - _record_last_flush_time) >= RECORD_FLUSH_SECONDS
        )
        if by_count or by_time:
            _flush_record_progress()


# ─── V2→V3 ID MAP ────────────────────────────────────────────────────────────
# When a record is inserted into V3, the gateway returns a new auto-increment ID.
# We store v2_id → v3_id per model alias so FK fields can be remapped before
# inserting dependent records (e.g. scheme.company_id points to a V2 company id
# that doesn't exist in V3 — we replace it with the V3 id assigned on insert).

_id_map: dict[str, dict] = {}      # alias → {v2_id: v3_id}
_id_map_lock = threading.Lock()

# Secondary lookup: V2 visit_id → V2 patient_id.
# Persisted so it survives across interrupted/resumed runs.
# Populated when reception_visit records are transformed; used by DoctorNote inject.
_visit_patient_map: dict[int, int] = {}
_visit_patient_dirty: int = 0


def _load_visit_patient_map() -> None:
    global _visit_patient_map
    path = get_paths().file("visit_patient")
    if path.exists():
        try:
            raw = json.loads(path.read_text())
            _visit_patient_map = {int(k): int(v) for k, v in raw.items() if v is not None}
            log.info("Visit→patient map loaded — %d entries", len(_visit_patient_map))
        except Exception as e:
            log.warning("Could not load visit→patient map: %s — starting fresh", e)
            _visit_patient_map = {}
    else:
        _visit_patient_map = {}

def _record_visit_patient(v2_visit_id: int, v2_patient_id: int) -> None:
    global _visit_patient_dirty
    _visit_patient_map[v2_visit_id] = v2_patient_id
    _visit_patient_dirty += 1
    if _visit_patient_dirty % RECORD_FLUSH_EVERY == 0:  # was 500
        _write_int_map(get_paths().file("visit_patient"), _visit_patient_map)
        _visit_patient_dirty = 0

def _record_visit_admission(v2_visit_id: int, v2_admission_id: int) -> None:
    global _visit_admission_dirty
    _visit_admission_map[v2_visit_id] = v2_admission_id
    _visit_admission_dirty += 1
    if _visit_admission_dirty % RECORD_FLUSH_EVERY == 0:  # was 500
        _write_int_map(get_paths().file("visit_admission"), _visit_admission_map)
        _visit_admission_dirty = 0


# Secondary lookup: V2 visit_id → V2 admission_id.
# Persisted when Admission records are migrated.
# transformed. Used to split V2 Vitals into inpatient (has an admission)
# vs outpatient (visit only, no admission) before posting — V3 has two
# separate vitals tables (inp_vitals vs patient-evaluation vitals).
_visit_admission_map: dict[int, int] = {}
_visit_admission_dirty: int = 0


def _load_visit_admission_map() -> None:
    global _visit_admission_map
    path = get_paths().file("visit_admission")
    if path.exists():
        try:
            raw = json.loads(path.read_text())
            _visit_admission_map = {int(k): int(v) for k, v in raw.items() if v is not None}
            log.info("Visit→admission map loaded — %d entries", len(_visit_admission_map))
        except Exception as e:
            log.warning("Could not load visit→admission map: %s — starting fresh", e)
            _visit_admission_map = {}
    else:
        _visit_admission_map = {}



def _write_int_map(path: Path, mapping: dict) -> None:
    """Merge-then-atomic write for the int→int visit maps: entries another
    process added are kept (ours win), and a kill mid-write can't truncate."""
    with _file_lock(path):
        try:
            for k, v in json.loads(path.read_text()).items():
                if v is not None:
                    mapping.setdefault(int(k), int(v))
        except (OSError, ValueError):
            pass
        _atomic_write(path, json.dumps(mapping))
        
def _flush_visit_maps() -> None:
    """Write dirty visit→patient / visit→admission maps to disk."""
    global _visit_patient_dirty, _visit_admission_dirty
    if _visit_patient_dirty:
       _write_int_map(get_paths().file("visit_patient"), _visit_patient_map)
    _visit_patient_dirty = 0
    if _visit_admission_dirty:
        _write_int_map(get_paths().file("visit_admission"), _visit_admission_map)
        _visit_admission_dirty = 0
# Which FK fields to remap, and which model alias holds their ID map.
_FK_REMAP: dict[str, dict[str, str]] = {
    # insurance_schemes.company_id → the insurance company's V3 id. Companies
    # post as finance `insurance_company` (the class's own alias); naming that
    # alias here lets an empty / partial company map be rebuilt by name.
    "settings_scheme": {
        "company_id": "insurance_company",
    },
    # rebates.scheme_id → insurance_schemes V3 id
    "settings_rebate": {
        "scheme_id": "insurance_schemes",
    },
    # procedures.category_id → procedure_categories V3 id
    "eval_procedure": {
        "category": "procedure_categories",
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

    # ── Inpatient FK chain ────────────────────────────────────────────────────
    # beds.ward_id → wards.id  |  beds.bed_type_id → bed_types.id
    "inpatient_bed": {
        "ward_id":     "ward",
        "bed_type_id": "bed_type",
    },
    # admission_requests.preferred_ward_id / preferred_bed_type_id
    "inpatient_admission_request": {
        "preferred_ward_id":     "ward",
        "preferred_bed_type_id": "bed_type",
        # sent as raw V2 ids before
        "patient_id":            "patient",
        "visit_id":              "visit",
        "requested_by":          "users",
        "admission_type_id":     "admission_type",
    },
    # admissions.admission_type_id → admission_types.id
    # admissions.admission_request_id → admission_requests.id
    "inpatient_admission": {
        "admission_type_id":    "admission_type",
        "admission_request_id": "admission_request",
        # without these the V2 ids went out unchanged (patient 271806, visit
        # 309215 …) — ids that don't exist, or are other records, in V3
        "patient_id":           "patient",
        "visit_id":             "visit",
        "ward_id":              "ward",
        "bed_id":               "bed",
    },
    "evaluation_visit_destination": {
        "visit_id": "visit",
        "destination_id": "service_destinations",
    },
    # Here (not only in _NS_FK_REMAP) so _ensure_id_maps rebuilds the map by
    # name from V2 + V3 when it's empty — 6 rows, one page — e.g. after the
    # id map file lost it, or on a host that never had it.
    "inpatient_discharge_request": {"discharge_type_id": "discharge_type"},
    "inpatient_discharge":         {"discharge_type_id": "discharge_type"},
    # inp_vitals.admission_id → inp_admissions.id  (THE critical blocker)
    "inpatient_vital": {
        "admission_id": "admission",
    },

    # ── Evaluation FK chain ───────────────────────────────────────────────────
    # investigation_results.investigation_id → investigations.id
    "evaluation_investigation": {
        "visit_id": "visit",
    },
    "evaluation_inv_result": {
        "visit_id":         "visit",
        "investigation_id": "investigation",
    },
    # doctor_notes.visit_id → visits.id  (patient_id is injected via _PER_KEY_INJECT)
    "evaluation_doctor_note": {
        "visit_id": "visit",
    },
    # receipts: issued_by NOT NULL (the cashier, a V3 user); patient / invoice optional
    "finance_receipt": {
        "issued_by":  "users",
        "patient_id": "patient",
        "invoice_id": "invoice",
    },
    # patient_dependants.patient_id → patients (the account holder, FK-constrained)
    "reception_patient_dependant": {
        "patient_id": "patient",
    },
    # theatres.theatre_type_id → theatre_types (FK-constrained; V2 type ids differ)
    "theatre_theatre": {
        "theatre_type_id": "theatre_type",
    },
    # theatre chain: booking → schedule → operation (each FK-constrained to
    # its parent in theatre-service); visit / patient / users are other services
    "theatre_booking": {
        "visit_id":               "visit",
        "patient_id":             "patient",
        "status_id":              "theatre_scheduling_status",
        "theatre_deposit_id":     "theatre_deposit",
        "operation_procedure_id": "procedures",
        "requested_by":           "users",
        "approved_by":            "users",
        "cancelled_by":           "users",
        "rejected_by":            "users",
        "authorized_by":          "users",
    },
    "theatre_schedule": {
        "booking_id":   "theatre_booking",
        "theatre_id":   "theatre",
        "scheduled_by": "users",
    },
    "theatre_operation": {
        "schedule_id": "theatre_schedule",
        "visit_id":    "visit",
        "patient_id":  "patient",
    },
    "inventory_requisition": {
        "requesting_store_id": "store",
        "requested_by":        "users",
        "approved_by":         "users",
    },
    # inv_stock_movements: product + store FK-constrained, user NOT NULL
    "inventory_stock_movement": {
        "product_id":    "product",
        "from_store_id": "store",
        "to_store_id":   "store",
        "user_id":       "users",
    },
    # inv_goods_received_notes: PO / store / supplier FK-constrained
    "inventory_grn": {
        "purchase_order_id": "purchase_order",
        "store_id":          "store",
        "supplier_id":       "supplier",
        "received_by":       "users",
    },
    # inv_purchase_orders: supplier / store FK-constrained, created_by NOT NULL
    "inventory_purchase_order": {
        "supplier_id": "supplier",
        "store_id":    "store",
        "created_by":  "users",
        "approved_by": "users",
    },
    # evaluation_examination_reviews.visit_id → visits.id (FK-constrained)
    "evaluation_examination_review": {
        "visit_id": "visit",
    },
    # gl_account_groups.account_type_id → gl_account_types (here, not only in
    # _NS_FK_REMAP, so _ensure_id_maps matches the types by name when the map is empty)
    "finance_gl_account_group": {
        "account_type_id": "gl_account_type",
    },
    # patient-evaluation vitals.visit_id → visits.id  (patient_id is injected via _PER_KEY_INJECT)
    "outpatient_vital": {
        "visit_id":   "visit",
        "patient_id": "patient",
    },
    # prescriptions.visit → visits.id — V2's own field is literally "visit",
    # not "visit_id" (confirmed against the live V3 field mapping, 2026-09;
    # this FK was previously never remapped at all since _NS_FK_REMAP's
    # Prescription entry looked for the nonexistent "visit_id" key).
    # Keyed visit_id though: transform_record's global rename (visit ->
    # visit_id) runs before the remap, so a "visit" key never matched and
    # every prescription went out with its raw V2 visit id. The post step
    # (snowflake_to_v3_migration._V3_COLUMN_RENAMES) moves it back to `visit`.
    "evaluation_prescription": {
        "visit_id":           "visit",
        "return_approved_by": "users",
        "return_rejected_by": "users",
    },
    # invoices.patient_id → patients.id  |  invoices.visit → visits.id — this
    # transform key had NO FK-remap entry at all before (confirmed against
    # the live V3 field mapping, 2026-09), so every migrated invoice carried
    # its raw V2 patient_id/visit straight through unremapped.
    "finance_invoice": {
        "patient_id": "patient",
        "visit":      "visit",
    },

    # ── Reception FK chain ────────────────────────────────────────────────────
    # appointments.appointment_category_id → appointment_categories.id
    "reception_appointment": {
        "appointment_category_id": "appointment_category",
    },
    # visits.patient_id → patients.id  |  visits.appointment_id → appointments.id
    "reception_visit": {
        "patient_id":     "patient",
        "appointment_id": "appointment",
    },
    # patient_insurance.patient_id + insurance_scheme_id
    "reception_patient_scheme": {
        "patient_id":          "patient",
        "insurance_scheme_id": "insurance_scheme",
    },

    # ── Users / settings ─────────────────────────────────────────────────────
    # user profiles carry department, employee_category, speciality FKs
    "settings_user": {
        "department_id":        "department",
        "employee_category_id": "employee_category",
        "speciality_id":        "specialty",
    },
}

# FK remaps keyed by V3 namespace for tables whose transform is "generic".
# _remap_fks merges this with _FK_REMAP so both dicts apply.
_NS_FK_REMAP: dict[str, dict[str, str]] = {

    # ── Reception: patient children ──────────────────────────────────────────
    r"App\Models\Appointment":      {"appointment_category_id": "appointment_category"},
    r"App\Models\PatientDocument":  {"patient_id": "patient"},
    r"App\Models\PatientFollowup":  {"patient_id": "patient",  "visit_id": "visit"},
    r"App\Models\PatientGuarantor": {"patient_id": "patient"},
    r"App\Models\PatientNextOfKin": {"patient_id": "patient"},
    r"App\Models\PatientConsent":   {"patient_id": "patient"},
    r"App\Models\PatientSample":    {"patient_id": "patient",  "visit_id": "visit"},
    r"App\Models\PatientDependant": {"patient_id": "patient"},
    r"App\Models\PatientRandomNote":{"patient_id": "patient"},

    # ── Reception: visit children ─────────────────────────────────────────────
    r"App\Models\MorgueAdmission":  {"patient_id": "patient",  "visit_id": "visit"},
    r"App\Models\Queue":            {"patient_id": "patient",  "visit_id": "visit"},
    r"App\Models\VisitDestination": {"visit_id": "visit"},
    r"App\Models\VisitConsultant":  {"visit_id": "visit"},
    r"App\Models\VisitPrecharge":   {"visit_id": "visit"},

    # ── Evaluation clinical ───────────────────────────────────────────────────
    r"App\Models\Investigation":    {"visit_id": "visit"},
    # Prescription's entry used to live here keyed on "visit_id", but V2's
    # own field is "visit" — moved to _FK_REMAP["evaluation_prescription"]
    # above, which is keyed correctly and takes precedence anyway.
    r"App\Models\EyeExam":          {"visit_id": "visit"},
    r"App\Models\Sample":           {"patient_id": "patient", "visit_id": "visits",
                                     "investigation_id": "investigations"},
    r"App\Models\Diagnosis":        {"visit_id": "visit"},

    # ── Evaluation reference chain ────────────────────────────────────────────
    r"App\Models\Icd10Subcategory": {"category_id":    "icd10_category"},
    r"App\Models\Icd10Type":        {"subcategory_id": "icd10_subcategory"},

    # ── Finance GL chain ──────────────────────────────────────────────────────
    r"App\Models\GlAccountGroup":   {"account_type_id":  "gl_account_type"},
    r"App\Models\GlAccount":        {"account_group_id": "gl_account_group",
                                     "account_type_id":  "gl_account_type"},
    r"App\Models\PettyCash":        {"gl_account_id": "gl_account"},

    # ── Finance invoices ──────────────────────────────────────────────────────
    r"App\Models\InvoiceItem":      {"invoice_id": "invoice"},
    r"App\Models\Payment":          {"invoice_id": "invoice"},
    r"App\Models\CreditNote":       {"invoice_id": "invoice"},
    r"App\Models\InsuranceClaim":   {"company_id": "insurance_company",
                                     "scheme_id":  "insurance_scheme"},

    # ── Settings ──────────────────────────────────────────────────────────────
    r"App\Models\ServiceDestination": {"department_id": "department"},
    r"App\Models\CategoryFilter":     {"treatment_action_id": "treatment_action"},

    # ── Inpatient ─────────────────────────────────────────────────────────────
    r"App\Models\WardCharge":       {"ward_id": "ward", "charge_id": "charge"},
    # Declared here rather than in _FK_REMAP on purpose: _ensure_id_maps
    # live-syncs every empty _FK_REMAP alias from the V2 API (one ~20s
    # request per page), and the admission / discharge_type maps stay empty
    # until those tables are migrated — these records just wait meanwhile.
    r"App\Models\DischargeRequest": {"admission_id": "admission", "visit_id": "visits",
                                     "discharge_type_id": "discharge_type"},
    r"App\Models\Discharge":        {"admission_id": "admission", "discharge_type_id": "discharge_type",
                                     "discharge_request_id": "discharge_request"},

    # ── Inventory ─────────────────────────────────────────────────────────────
    r"App\Models\Store":            {"parent_store_id": "store"},
    r"App\Models\Sale":             {"store_id": "store", "patient_id": "patient"},

    # ── Patient account ───────────────────────────────────────────────────────
    r"App\Models\PatientAccount":   {"patient_id": "patient"},
}


def _load_id_map() -> None:
    global _id_map, _id_map_mtime
    if ID_MAP_FILE.exists():
        raw, _id_map_mtime = _read_state_json(ID_MAP_FILE)
        # Keys are stored as strings in JSON; convert back to int where possible
        _id_map = {
            alias: {(int(k) if k.isdigit() else k): v for k, v in mapping.items()}
            for alias, mapping in raw.items()
        }
        total = sum(len(v) for v in _id_map.values())
        if total:
            log.info("ID map loaded — %d entries across %d models", total, len(_id_map))
    else:
        _id_map = {}


_id_map_mtime: tuple | None = None   # _file_sig of the version we last read/wrote


def _write_id_map() -> None:
    """Caller holds _id_map_lock. Same merge-then-atomic-write as
    _flush_record_progress: entries another process added since our last
    write are kept (ours win on conflict), instead of being overwritten."""
    global _id_map_mtime
    with _file_lock(ID_MAP_FILE):
        if _changed_on_disk(ID_MAP_FILE, _id_map_mtime):
            try:
                for alias, mapping in json.loads(ID_MAP_FILE.read_text()).items():
                    mine = _id_map.setdefault(alias, {})
                    for k, v in mapping.items():
                        mine.setdefault(int(k) if k.isdigit() else k, v)
            except (OSError, ValueError) as e:
                log.warning("Could not merge %s before writing: %s", ID_MAP_FILE.name, e)
        _id_map_mtime = _atomic_write(ID_MAP_FILE, json.dumps(_id_map, indent=2))


_id_map_dirty = 0


def _store_id_mapping(alias: str, v2_id, v3_id) -> None:
    """Record a V2→V3 mapping. Written to disk in batches, not per insert:
    rewriting the whole (8+ MB) id map after every record cost ~230 ms CPU +
    8.5 MB of disk writes per insert — with a few tables in parallel that
    saturated the server within seconds. It's flushed every
    RECORD_FLUSH_EVERY mappings, before every progress flush, and at exit."""
    global _id_map_dirty
    with _id_map_lock:
        _id_map.setdefault(alias, {})[v2_id] = v3_id
        _id_map_dirty += 1
        if _id_map_dirty >= RECORD_FLUSH_EVERY:
            _write_id_map()
            _id_map_dirty = 0


def _flush_id_map() -> None:
    """Write any buffered mappings now (end of a table, before a progress
    flush, at exit)."""
    global _id_map_dirty
    with _id_map_lock:
        if _id_map_dirty:
            _write_id_map()
            _id_map_dirty = 0


def _flush_state_at_exit() -> None:
    """Only if this process has unwritten state — processes that never
    inserted anything (plan, dry runs) must not rewrite the big files."""
    try:
        if _record_progress_unflushed:
            with _record_progress_lock:
                _flush_record_progress()   # writes the id map first
        else:
            _flush_id_map()
        _flush_visit_maps()                # always — independent of record progress
    except Exception as e:
        log.warning("State flush at exit failed: %s", e)


atexit.register(_flush_state_at_exit)
        
def _on_terminate(signum, frame) -> None:
    """Airflow / K8s / systemd send SIGTERM before kill. Flush progress so a
    graceful stop does not lose the last few successful inserts."""
    log.warning("Received signal %s — flushing migration state before exit", signum)
    _flush_state_at_exit()
    # Restore default and re-raise so the process still exits
    signal.signal(signum, signal.SIG_DFL)
    os.kill(os.getpid(), signum)

for _sig in (signal.SIGTERM, signal.SIGINT):
    try:
        signal.signal(_sig, _on_terminate)
    except (ValueError, OSError):
        # Not main thread, or platform does not support it — atexit still runs
        pass

def _store_id_mappings(alias: str, pairs: list[tuple]) -> None:
    """Bulk _store_id_mapping — one file write instead of one per record."""
    if not pairs:
        return
    with _id_map_lock:
        m = _id_map.setdefault(alias, {})
        for v2_id, v3_id in pairs:
            m[v2_id] = v3_id
        _write_id_map()


def _existing_v3_uuids(alias: str, org_cfg: dict, service_name: str | None = None) -> dict[str, int]:
    """uuid → V3 id for every row V3 already has for this model (read-only).

    Used to skip records that are already in V3 before posting them. If the
    read fails partway, the result is partial — the records it misses just
    get posted, and match_on=uuid still turns those into updates, not
    duplicates."""
    out: dict[str, int] = {}
    for rec in _fetch_v3_records(alias, org_cfg, service_name=service_name):
        if rec.get("uuid") and rec.get("id") is not None:
            out[str(rec["uuid"]).strip().lower()] = rec["id"]
    return out


def _fetch_v3_records(alias: str, org_cfg: dict, service_name: str | None = None) -> list[dict]:
    """Fetch all existing V3 records for a model via the gateway read action."""
    service_name = service_name or _alias_to_service.get(alias, "core")
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
            r = _gateway_post(service_name, body, timeout=60)
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


# Match field override per V3 namespace — most V2/V3 tables share a "name"
# field, but some (e.g. users) don't and need a different unique key to
# match on. Keyed by v3 namespace (static) rather than the discovered gateway
# alias (singular/plural varies by service) so lookups can't get out of sync.
_SYNC_MATCH_FIELD_BY_NS: dict[str, str] = {
    r"App\Models\User": "email",
    # products: code is per-org unique in V3 (V3 renamed name → generic_name)
    r"App\Models\Product": "code",
    # ICD-10 types: V3 keeps the description empty — the code is the key
    # (all 1,910 kisumu_v3 types match by code)
    r"App\Models\Icd10Type": "code",
    # procedures: never by name — V2 has several procedures per name (5×
    # "Colonoscopy", each its own code and price) and V3 a row per code; by
    # name, 233 kisumu_v3 procedures were linked to a same-named sibling.
    # Codes V2 itself repeats are "<code>-<V2 id>" in V3 (tried first).
    r"App\Models\Procedure": "code",
}
# further fields tried (in order) when the match field finds nothing, per alias.
# Usernames are unique per org in V3; 18 kisumu_v3 staff had placeholder emails.
_SYNC_FALLBACK_FIELDS: dict[str, tuple[str, ...]] = {
    "users": ("username",),
}

# Extra id-map dependencies (by v3 namespace) a transform's _PER_KEY_INJECT
# relies on, beyond what _FK_REMAP already declares. _remap_fks never touches
# these fields directly (they're populated by injection, not FK remap), so
# _ensure_id_maps needs an explicit hint to bootstrap them too.
_PER_KEY_EXTRA_ID_DEPS: dict[str, list[str]] = {
    "evaluation_prescription": [r"App\Models\User"],
}


# Filled by snowflake_to_v3_migration from its _ALIAS_OVERRIDE / _SERVICE_OVERRIDE:
# a model posted under an overridden alias (eval_procedure → `procedures` in
# evaluation-service, not the core / inpatient model the class name resolves
# to) must also be found — and read from the right service — when its empty id
# map is rebuilt, or every child FK to it stays unmapped ("Cannot find V2
# namespace for alias procedures").
_ALIAS_FOR_TRANSFORM: dict[str, str] = {}
_SERVICE_FOR_ALIAS: dict[str, str] = {}


def sync_id_map(alias: str, v2_namespace: str, facility: str, match_field: str = "name") -> int:
    """Match existing V3 records to V2 records by match_field and populate the ID map.

    Used when a model was migrated before the ID-mapping system existed, or
    (for users) is provisioned entirely outside this pipeline (e.g. a
    separate auth/onboarding flow) so it can never appear in _id_map any
    other way.
    Returns the number of mappings added.
    """
    org_cfg = facility_v3_config(facility)

    v3_records = _fetch_v3_records(alias, org_cfg, service_name=_SERVICE_FOR_ALIAS.get(alias))
    if not v3_records:
        log.warning("sync_id_map: no V3 records found for %s — cannot build map", alias)
        return 0

    # Build uuid → V3 id and match_field → V3 id lookups. uuid is the primary
    # join key (V2 uuid == V3 uuid); match_field is only a fallback for rows
    # migrated before uuids were carried across.
    v3_by_uuid: dict[str, int] = {}
    v3_by_key: dict[str, int] = {}
    v3_by_fallback: dict[tuple[str, str], int] = {}
    v3_by_v2_id: dict[int, int] = {}
    v3_name_by_key: dict[str, str] = {}
    # Oldest row first, and the first row per name wins: when V3 holds copies of
    # a record (1,763 procedures re-created on 2026-10-12), children must link
    # to the original, never to the copy.
    for rec in sorted(v3_records, key=lambda x: int(x["id"]) if str(x.get("id") or "").isdigit() else 0):
        v3_id = rec.get("id")
        if not v3_id:
            continue
        if rec.get("uuid"):
            v3_by_uuid[str(rec["uuid"]).strip().lower()] = v3_id
        key = rec.get(match_field)
        if key:
            v3_by_key.setdefault(str(key).strip().lower(), v3_id)
            v3_name_by_key.setdefault(str(key).strip().lower(), str(rec.get("name") or "").strip().lower())
        for fb in _SYNC_FALLBACK_FIELDS.get(alias, ()):
            if rec.get(fb):
                v3_by_fallback.setdefault((fb, str(rec[fb]).strip().lower()), v3_id)
        # users the V3 backend created from V2 carry the V2 id in a placeholder
        # email (miteigyn@gmail.com.v2-1533.invalid) — the surest match there is
        m = _V2_PLACEHOLDER_EMAIL.match(str(rec.get("email") or "").strip())
        if m:
            v3_by_v2_id.setdefault(int(m["v2_id"]), v3_id)

    # Extract V2 records
    cfg = v2_facility_config(facility)
    job = {
        "facility":      facility,
        "namespace":     v2_namespace,
        "database":      cfg["db"],
        "updated_since": "1970-01-01T00:00:00Z",
        "limit":         DEFAULT_LIMIT,
    }
    try:
        v2_records, failed_pages = extract_v2_records(job)
    except Exception as e:
        log.error("sync_id_map: V2 extraction failed for %s: %s", v2_namespace, e)
        return 0
    if failed_pages:
        log.warning("sync_id_map: %s — %d page(s) failed to extract and were skipped: %s",
                    v2_namespace, len(failed_pages), failed_pages)

    v2_code_count: dict[str, int] = {}
    if match_field == "code":
        for rec in v2_records:
            k = str(rec.get("code") or "").strip().lower()
            if k:
                v2_code_count[k] = v2_code_count.get(k, 0) + 1

    by_uuid = by_field = 0
    for rec in v2_records:
        v2_id = rec.get("id")
        if not v2_id:
            continue
        u = _v2_uuid(rec)
        v3_id = v3_by_uuid.get(u) if u else None
        if v3_id:
            by_uuid += 1
        else:
            v3_id = v3_by_v2_id.get(int(v2_id)) if str(v2_id).isdigit() else None
            if not v3_id:
                key = str(rec.get(match_field) or "").strip().lower()
                # The plain-code row is the original (oldest first). Every
                # "<code>-<V2 id>" row is a 2026-10-12 copy — used ONLY when V2
                # repeats the code for a DIFFERENT procedure (CON-0003: Doctor's
                # Fee / Consultant Review / REVIEW …), where the original holds
                # the code and that copy is the procedure's only V3 row.
                if key and match_field == "code" and v2_code_count.get(key, 0) > 1:
                    orig_name = v3_name_by_key.get(key, "")
                    if str(rec.get("name") or "").strip().lower() != orig_name:
                        v3_id = v3_by_key.get(f"{key}-{v2_id}")
                if not v3_id:
                    v3_id = v3_by_key.get(key) if key else None
            for fb in _SYNC_FALLBACK_FIELDS.get(alias, ()):
                if v3_id:
                    break
                val = str(rec.get(fb) or "").strip().lower()
                v3_id = v3_by_fallback.get((fb, val)) if val else None
            if v3_id:
                by_field += 1
        if v3_id:
            _store_id_mapping(alias, v2_id, v3_id)
            # uuid-era maps are keyed by the V2 uuid and looked up by it first —
            # repoint that key too, or a stale entry (e.g. to a duplicate) wins
            if u:
                _store_id_mapping(alias, u, v3_id)

    matched = by_uuid + by_field
    log.info("sync_id_map %s [%s]: matched %d / %d V2 records to V3 ids (%d on uuid, %d on %s)",
             alias, facility, matched, len(v2_records), by_uuid, by_field, match_field)
    return matched


# only reference-sized tables are re-synced when partial (products: 6,440 V3 rows
# in org 4 — 94 V2 products had no mapping, holding 686 stock rows and movements)
_RESYNC_MAX_ROWS = 10000

# transform keys whose payload carries facility_id that must be the destination facility
_PER_KEY_DEST_FACILITY: set[str] = {"theatre_theatre", "gen_settings_destinations", "gen_settings_scheme_exclusions"}
# transform keys whose rows link to the row V3 already has with the same value
# of this field (V3 seeds org 4 with Laboratory / Triage … destinations; the
# shared ICD-10 codes) instead of adding a twin
_LINK_BY_FIELD: dict[str, str] = {"gen_settings_destinations": "name",
                                  "gen_evaluation_icd10_variations": "code"}
_v3_total_cache: dict[str, int | None] = {}
_resynced: set[str] = set()       # aliases whose id map was rebuilt in this process


def _v3_total(alias: str, facility: str) -> int | None:
    """Rows this org has for a gateway model (one 1-row read, cached per process)."""
    if alias not in _v3_total_cache:
        org_cfg = facility_v3_config(facility)
        try:
            r = _gateway_post(_SERVICE_FOR_ALIAS.get(alias) or _alias_to_service.get(alias, "core"),
                              {"action": "read", "model": alias, "source_tenant_id": org_cfg.get("organization_id"),
                               "per_page": 1, "page": 1}, timeout=60)
            _v3_total_cache[alias] = ((r.json().get("meta") or {}).get("total") if r.ok else None)
        except Exception:
            _v3_total_cache[alias] = None
    return _v3_total_cache[alias]


def _ensure_id_maps(transform_key: str, facility: str) -> None:
    """Auto-sync any FK dependency maps that are empty before a job runs."""
    # (alias, v3_namespace) pairs from _FK_REMAP (alias already resolved at
    # dict-authoring time) plus any extra injection-only deps (v3_namespace
    # only — alias resolved here, since discovery may prefer singular/plural
    # and the two lookups must agree on the same key).
    aliases: list[tuple[str, str | None]] = [
        (alias, None) for alias in _FK_REMAP.get(transform_key, {}).values()
    ]
    for v3_ns in _PER_KEY_EXTRA_ID_DEPS.get(transform_key, []):
        aliases.append((_v3_alias(v3_ns), v3_ns))

    for alias, known_v3_ns in aliases:
        have = len(_id_map.get(alias) or {})
        if have and alias in _resynced:
            continue                       # once per process — not again on every page
        if have:
            # a partial map never rebuilt itself (e.g. 3 procedures mapped by a
            # canary, 1,674 in V3 → every procedure FK stayed unmapped). For
            # small reference tables, re-sync when V3 holds more than we know.
            total = _v3_total(alias, facility)
            if total is None or total <= have or total > _RESYNC_MAX_ROWS:
                continue
            log.info("ID map for %s knows %d of %d V3 rows — re-syncing before %s job",
                     alias, have, total, transform_key)
        else:
            log.info("ID map for %s is empty — auto-syncing from V3 before %s job",
                     alias, transform_key)
        variants = {alias, alias[:-1] if alias.endswith("s") else alias + "s"}
        v2_ns = next(
            (ns for ns, m in NAMESPACE_MAP.items()
             if (known_v3_ns and m["v3"] == known_v3_ns) or _v3_alias(m["v3"]) == alias
             or _ALIAS_FOR_TRANSFORM.get(m["transform"]) in variants),
            None,
        )
        _resynced.add(alias)
        if v2_ns:
            match_field = _SYNC_MATCH_FIELD_BY_NS.get(NAMESPACE_MAP[v2_ns]["v3"], "name")
            sync_id_map(alias, v2_ns, facility, match_field=match_field)
        else:
            log.warning("Cannot find V2 namespace for alias %s — skipping auto-sync", alias)


def _remap_fks(record: dict, transform_key: str, v3_namespace: str = "") -> dict:
    """Replace V2 FK values with their V3-assigned IDs.

    Merges two lookup sources:
    - _FK_REMAP[transform_key]  — for tables with a specific transform
    - _NS_FK_REMAP[v3_namespace] — for tables that use the generic transform
    Transform-specific entries take precedence on conflict.
    """
    fk_config = {
        **_NS_FK_REMAP.get(v3_namespace, {}),
        **_FK_REMAP.get(transform_key, {}),
    }
    if not fk_config:
        return record
    out = dict(record)
    for field, alias in fk_config.items():
        v2_id = out.get(field)
        if v2_id is None:
            continue
        v3_id = _id_map.get(alias, {}).get(int(v2_id) if str(v2_id).isdigit() else v2_id)
        if v3_id is not None:
            out[field] = v3_id
        else:
            log.warning("  No V3 ID mapping for %s id=%s — %s will fail FK constraint",
                        alias, v2_id, field)
    return out


# ─── VISIT → PATIENT BACKFILL ────────────────────────────────────────────────
# Staging (afya_api_auth) is a copy of Kisumu, but its visits have lost their
# patient link: patient / patient_uuid / reception_patient_uuid are null on
# essentially every row. Kisumu still has the link for the same visit ids.
# facility → donor facility whose visits (same ids) still carry the patient.
VISIT_PATIENT_DONOR: dict[str, str] = {
    "afya_api_auth": "kisumu",
}
# Columns that must agree before two rows are accepted as the same patient in
# both databases. Staging scrambles names/phones (ciphertext) and also dob and
# sex (differ on ~100% of rows), and renumbers patient_no on some (79 of
# 10,696) — so only system_id (unique, 0 duplicates) + created_at are usable.
# Verified 2026-09-25: all 10,696 referenced patients agree on both.
_PATIENT_IDENTITY_COLS = ("system_id", "created_at")


def _v2_all(facility: str, namespace: str) -> dict:
    cfg = v2_facility_config(facility)
    rows, failed = extract_v2_records({
        "facility": facility, "namespace": namespace, "database": cfg["db"],
        "updated_since": "1970-01-01T00:00:00Z", "limit": DEFAULT_LIMIT,
    })
    if failed:
        log.warning("  backfill: %s %s — %d page(s) failed; those rows can't be used for linking",
                    facility, namespace, len(failed))
    return {r["id"]: r for r in rows if r.get("id") is not None}


def _backfill_visit_patients(rows: list[dict], facility: str, org_cfg: dict) -> None:
    """Fill `patient` on V2 visits that lack it, from the donor facility.

    Chain, every link verified:
      staging visit id → donor visit with the same id AND same unique_id + created_at
      → donor patient id → staging patient with the same id AND same identity cols
      → that staging patient's uuid → the V3 patient with that uuid.
    Only when the whole chain resolves is `patient` set, and the V2 patient id →
    V3 id mapping is stored from the uuid match (overriding any stale entry), so
    _remap_fks puts the right V3 patient on the visit. Anything else is left
    unlinked for run_job to hold back.
    """
    donor = VISIT_PATIENT_DONOR.get(facility)
    missing = [r for r in rows if r.get("patient") is None and r.get("patient_id") is None]
    if not donor or not missing:
        return
    log.info("  backfill: %d/%d visits have no patient — linking via %s", len(missing), len(rows), donor)

    donor_visits   = _v2_all(donor, r"Ignite\Evaluation\Entities\Visit")
    donor_patients = _v2_all(donor, r"Ignite\Reception\Entities\Patients")
    own_patients   = _v2_all(facility, r"Ignite\Reception\Entities\Patients")
    v3_by_uuid     = _existing_v3_uuids(_v3_alias(r"App\Models\Patient"), org_cfg)
    if not v3_by_uuid:
        log.error("  backfill: could not read any patients (with uuids) from V3 — the read failed or "
                  "returned no uuid field. Not linking; %d visits stay held back.", len(missing))
        return

    linked, reasons, pairs = 0, {}, {}
    for r in missing:
        dv = donor_visits.get(r["id"])
        if not dv or dv.get("unique_id") != r.get("unique_id") or str(dv.get("created_at")) != str(r.get("created_at")):
            reasons["visit not found / differs in donor"] = reasons.get("visit not found / differs in donor", 0) + 1
            continue
        p = dv.get("patient")
        sp, dp = own_patients.get(p), donor_patients.get(p)
        if p is None or not sp or not dp:
            reasons["patient missing in donor or here"] = reasons.get("patient missing in donor or here", 0) + 1
            continue
        if any(str(sp.get(c)) != str(dp.get(c)) for c in _PATIENT_IDENTITY_COLS):
            reasons["patient identity differs"] = reasons.get("patient identity differs", 0) + 1
            continue
        u = _v2_uuid(sp)
        if not u or u not in v3_by_uuid:
            reasons["patient uuid not in V3"] = reasons.get("patient uuid not in V3", 0) + 1
            continue
        r["patient"] = p
        r["patient_uuid"] = sp["uuid"]
        pairs[p] = v3_by_uuid[u]
        linked += 1

    _store_id_mappings("patient", list(pairs.items()))
    log.info("  backfill: linked %d/%d visits to a patient (%d distinct patients); unlinked: %s",
             linked, len(missing), len(pairs), reasons or "none")


# ─── JOB RUNNER ──────────────────────────────────────────────────────────────

# job_key -> list of V2 page numbers that permanently failed extraction (for the summary)
_job_failed_pages: dict[str, list[int]] = {}
_failed_pages_lock = threading.Lock()


def _run_vitals_split_job(
    job: dict, run_id: str, batch_size: int, dry_run: bool,
    rows: list[dict], failed_pages: list[int], org_cfg: dict, label: str, t0: float,
) -> bool:
    """V2 Vitals only carry visit_id — V3 splits vitals into two tables:
    inp_vitals (has an admission) vs patient-evaluation vitals (visit only,
    no admission). Split by looking each row's visit_id up in the
    visit→admission map (built from migrated Admissions) and, failing that,
    the visit→patient map (built from migrated Visits).
    """
    facility  = job["facility"]
    namespace = job["namespace"]
    base_key  = _job_key(facility, namespace)

    if failed_pages:
        with _failed_pages_lock:
            _job_failed_pages[base_key] = failed_pages
        log.warning("  %s — %d page(s) permanently failed and were skipped (their records are MISSING): %s",
                    label, len(failed_pages), failed_pages)

    if not rows:
        if failed_pages:
            log.warning("⊘ %s — 0 rows extracted and %d page(s) failed — NOT marking done", label, len(failed_pages))
            return False
        log.info("⊘ %s — 0 rows from V2 (no records or all before watermark)", label)
        _mark_done(run_id, base_key)
        return True

    admitted, outpatient, orphans = [], [], []
    for r in rows:
        visit_id = r.get("visit_id")
        if visit_id in _visit_admission_map:
            admitted.append(r)
        elif visit_id in _visit_patient_map:
            outpatient.append(r)
        else:
            orphans.append(r)

    log.info("  %s — split %d rows: %d admitted, %d outpatient, %d unroutable (visit not migrated yet)",
              label, len(rows), len(admitted), len(outpatient), len(orphans))

    if orphans:
        for r in orphans:
            _write_dead_letter(
                "App\\Models\\Vital[unrouted]", r,
                {"reason": f"visit_id={r.get('visit_id')} not found in visit→admission or visit→patient map — "
                           f"parent Visit/Admission not migrated yet"},
            )
        log.warning("  %s — %d row(s) unroutable (dead-lettered) — parent Visit/Admission not migrated yet",
                    label, len(orphans))

    def _prep(partition: list[dict], transform_key: str) -> list[dict]:
        raw = [transform_record(r, transform_key, org_cfg) for r in partition]
        ok  = [r for r in raw if r is not None]
        if len(ok) != len(raw):
            log.warning("  %s [%s] — %d/%d records dropped by required-field check",
                        label, transform_key, len(raw) - len(ok), len(raw))
        return ok

    admitted_t   = _prep(admitted, "inpatient_vital")
    outpatient_t = _prep(outpatient, OUTPATIENT_VITAL_TRANSFORM)

    if dry_run:
        log.info("DRY-RUN ✓ %s — would POST %d inpatient + %d outpatient vitals",
                  label, len(admitted_t), len(outpatient_t))
        if admitted_t:
            log.info("  Sample inpatient: %s", _dumps(admitted_t[0])[:400])
        if outpatient_t:
            log.info("  Sample outpatient: %s", _dumps(outpatient_t[0])[:400])
        return True

    _ensure_id_maps("inpatient_vital", facility)
    _ensure_id_maps(OUTPATIENT_VITAL_TRANSFORM, facility)

    dead_letters = 0
    try:
        if admitted_t:
            dead_letters += post_to_v3(r"App\Models\Vital", org_cfg, admitted_t, batch_size=batch_size,
                       job_key=f"{base_key}::inpatient", transform_key="inpatient_vital",
                       service_override=INPATIENT_VITAL_SERVICE)
        if outpatient_t:
            dead_letters += post_to_v3(OUTPATIENT_VITAL_V3_NAMESPACE, org_cfg, outpatient_t, batch_size=batch_size,
                       job_key=f"{base_key}::outpatient", transform_key=OUTPATIENT_VITAL_TRANSFORM,
                       service_override=OUTPATIENT_VITAL_SERVICE)
    except GatewayModelNotRegistered as e:
        log.warning("⊘ %s — model not registered in gateway: %s", label, e)
        return True
    except Exception as e:
        log.error("✗ V3 POST FAILED  %s: %s", label, e)
        return False

    elapsed = time.perf_counter() - t0
    if failed_pages or orphans or dead_letters:
        log.warning(
            "◐ %s — %d inpatient + %d outpatient vitals migrated in %.2fs, but %d page(s) failed, "
            "%d row(s) unroutable, %d dead-lettered (job NOT marked done — re-run will retry)",
            label, len(admitted_t), len(outpatient_t), elapsed, len(failed_pages), len(orphans), dead_letters,
        )
        return False
    log.info("✓ %s — %d inpatient + %d outpatient vitals migrated in %.2fs",
              label, len(admitted_t), len(outpatient_t), elapsed)
    _mark_done(run_id, base_key)
    return True


def run_job(job: dict, run_id: str, batch_size: int, dry_run: bool) -> bool:
    """Extract from V2, transform, POST to V3. Returns True on success."""
    facility  = job["facility"]
    use_state_dir(facility, pipeline="v2_to_v3")   
    namespace = job["namespace"]
    mapping   = NAMESPACE_MAP.get(namespace)
    if mapping is None:
        log.warning("No NAMESPACE_MAP entry for %s — skipping", namespace)
        return True

    org_cfg = facility_v3_config(facility)
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
        rows, failed_pages = extract_v2_records(job)
    except Exception as e:
        log.error("✗ V2 extraction FAILED  %s: %s", label, e)
        return False

    if transform_key == "inpatient_vital":
        return _run_vitals_split_job(job, run_id, batch_size, dry_run,
                                      rows, failed_pages, org_cfg, label, t0)

    if failed_pages:
        with _failed_pages_lock:
            _job_failed_pages[_job_key(facility, namespace)] = failed_pages
        log.warning("  %s — %d page(s) permanently failed and were skipped (their records are MISSING): %s",
                    label, len(failed_pages), failed_pages)

    if not rows:
        if failed_pages:
            log.warning("⊘ %s — 0 rows extracted and %d page(s) failed — NOT marking done", label, len(failed_pages))
            return False
        log.info("⊘ %s — 0 rows from V2 (no records or all before watermark)", label)
        _mark_done(run_id, _job_key(facility, namespace))
        return True

    log.info("  %s — %d rows fetched in %.2fs", label, len(rows), time.perf_counter() - t0)
    log.info("  %s — sample: %s", label, _dumps(rows[0])[:500])

    if transform_key == "reception_visit":
        _backfill_visit_patients(rows, facility, org_cfg)

    transformed_raw = [transform_record(r, transform_key, org_cfg, facility) for r in rows]
    transformed = [r for r in transformed_raw if r is not None]

    # A visit without a patient is rejected by V3 (opaque 500) — hold it back
    # locally instead of posting it, and keep the job un-done so it's retried
    # once the link can be resolved.
    unlinked = 0
    if transform_key == "reception_visit":
        held = [x.get("id") for x in transformed if x.get("patient_id") is None]
        unlinked = len(held)
        transformed = [x for x in transformed if x.get("patient_id") is not None]
        if unlinked:
            log.warning("  %s — %d visit(s) have no patient link — held back, not posted (e.g. ids %s)",
                        label, unlinked, held[:10])
    n_no_uuid = sum(1 for r in transformed if not r.get("uuid"))
    if n_no_uuid:
        log.warning("  %s — %d/%d records have no V2 uuid: they will be inserted without "
                    "match_on and cannot be mapped to V3 by uuid", label, n_no_uuid, len(transformed))
    n_skipped_transform = len(transformed_raw) - len(transformed)
    if n_skipped_transform:
        log.warning("  %s — %d/%d records dropped by required-field check",
                    label, n_skipped_transform, len(transformed_raw))

    if not transformed:
        if unlinked:
            log.warning("⊘ %s — all %d visits held back for missing patient link — NOT marking done", label, unlinked)
            return False
        if failed_pages:
            log.warning("⊘ %s — all %d records failed transform and %d page(s) failed extraction — NOT marking done",
                        label, len(transformed_raw), len(failed_pages))
            return False
        log.warning("⊘ %s — all %d records failed transform, nothing to post",
                    label, len(transformed_raw))
        _mark_done(run_id, _job_key(facility, namespace))
        return True

    if dry_run:
        log.info("DRY-RUN ✓ %s — would POST %d records%s", label, len(transformed),
                 f" ({unlinked} held back: no patient link)" if unlinked else "")
        log.info("  Sample transformed: %s", _dumps(transformed[0])[:400])
        return True

    # Auto-populate any FK maps that are missing before we start posting
    _ensure_id_maps(transform_key, facility)

    try:
        dead_letters = post_to_v3(v3_namespace, org_cfg, transformed, batch_size=batch_size,
                   job_key=_job_key(facility, namespace), transform_key=transform_key)
    except GatewayModelNotRegistered as e:
        log.warning("⊘ %s — model not registered in gateway: %s", label, e)
        return True
    except Exception as e:
        log.error("✗ V3 POST FAILED  %s: %s", label, e)
        return False

    elapsed = time.perf_counter() - t0
    if failed_pages or dead_letters or unlinked:
        log.warning(
            "◐ %s — %d/%d records migrated in %.2fs, but %d page(s) failed to extract, "
            "%d dead-lettered and %d held back unlinked (job NOT marked done — re-run will retry; "
            "watermark will not advance)",
            label, len(transformed) - dead_letters, len(transformed), elapsed, len(failed_pages),
            dead_letters, unlinked,
        )
        return False
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
    max_pages: int | None = None,
) -> None:
    _started = datetime.now(timezone.utc)                      # CHANGED: keep the start time
    run_id = _started.strftime("%Y%m%dT%H%M%SZ")

    # NEW: several facilities -> one process each (state lives in module globals)
    if len(facilities) > 1:
        from concurrent.futures import ProcessPoolExecutor
        import multiprocessing as mp
        kw = dict(namespaces=namespaces, since=since, workers=workers,
                  batch_size=batch_size, dry_run=dry_run, max_pages=max_pages)
        with ProcessPoolExecutor(max_workers=min(len(facilities), 4),
                                 mp_context=mp.get_context("spawn")) as pool:
            futs = {f: pool.submit(run_migration, [f], **kw) for f in facilities}
            for f, fut in futs.items():
                try:
                    fut.result()
                except BaseException as e:      # a facility failing must not hide the others
                    log.error("Facility %s failed: %s", f, e)
        return

    use_state_dir(facilities[0], pipeline="v2_to_v3", process_wide=True)                             
    global _completed_jobs
    _completed_jobs = _load_progress(run_id)
    _load_record_progress()
    _load_id_map()
    _load_visit_patient_map()
    _load_visit_admission_map()
    _load_permanently_done()
    if _completed_jobs:
        log.info("Resuming run %s — %d jobs already done", run_id, len(_completed_jobs))

    # Fetch registered gateway models — skip anything not yet available
    available_models = _fetch_available_models()
    if available_models:
        log.info("Gateway has %d registered models: %s",
                 len(available_models), ", ".join(sorted(available_models)))
    elif not dry_run:
        # Without discovery every model routes to "core" (the default), which
        # needs HMAC creds and hosts none of the transactional models — every
        # read and post would fail, after a long V2 extraction. Stop now.
        log.error("No V3 gateway answered the model list (see 'Could not reach gateway' above) — "
                  "V3 is unreachable or login failed. Aborting before extracting anything.")
        return
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
        cfg = v2_facility_config(facility)
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
                    # Model is registered but gateway list doesn't advertise insert —
                    # the operations list is unreliable (some services underreport).
                    # Attempt the insert anyway; a genuine 422/403 will surface it.
                    svc = _gateway_model_meta[alias].get("service", "?")
                    ops = _gateway_model_meta[alias].get("operations", [])
                    log.debug(
                        "  %s|%s → %r in %s [%s] — no insert in ops list, attempting anyway",
                        facility, ns, alias, svc, ", ".join(ops),
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
            if jk in _completed_jobs or jk in _permanently_done:
                skipped_already_done.append(f"  {jk}")
                continue
            jobs.append({
                "facility":      facility,
                "namespace":     ns,
                "database":      cfg["db"],
                "updated_since": watermark,
                "limit":         DEFAULT_LIMIT,
                "max_pages":     max_pages,
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

    # Group jobs by tier so FK parents always finish before children start.
    # Within a tier jobs still run in parallel up to `workers`.
    from collections import defaultdict as _ddict
    tier_groups: dict[int, list] = _ddict(list)
    for job in jobs:
        tier_groups[_namespace_tier(job["namespace"])].append(job)

    for tier_num in sorted(tier_groups):
        tier_jobs = tier_groups[tier_num]
        if not tier_jobs:
            continue
        log.info("── Tier %d ── %d job(s)", tier_num, len(tier_jobs))
        if workers <= 1:
            for job in tier_jobs:
                ok = run_job(job, run_id, batch_size, dry_run)
                if not ok:
                    failures.append(_job_key(job["facility"], job["namespace"]))
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                future_to_job = {
                    pool.submit(run_job, job, run_id, batch_size, dry_run): job
                    for job in tier_jobs
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
            pages = _job_failed_pages.get(f)
            if pages:
                summary_lines.append(f"    ✗ {f}  (pages failed after retries: {pages})")
            else:
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

    # Flush any unsaved visit→patient / visit→admission map entries
    _flush_visit_maps()                                        # CHANGED: merge-safe + atomic

    # Advance watermarks only if zero failures
    if not failures and not dry_run:
        now_iso = _started.isoformat()                         # CHANGED: run START, not run end
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
        "--max-pages",
        type=int,
        default=None,
        metavar="N",
        help="Stop V2 pagination after N pages (useful for testing)",
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
        max_pages=args.max_pages,
    )


if __name__ == "__main__":
    main()
