#!/usr/bin/env python3
"""
siaya_v3_visits_to_snowflake.py — append V3 (afyaanalytics.ai) visit data for
organization_id=70 ("Siaya Hospital Clinic", facilities 110 "Bondo Health
Centre" and 111 "Ugunja Medical Centre") into
HOSPITALS.SIAYA_MEDICAL_CAMP.SIAYA_MEDICAL_CAMP_COMPLETE_DATA.

BACKGROUND
  The user originally asked for organization_id=87 — that organization does
  not exist in the V3 core service ("Organization [87] not found."). The
  account that IS assigned to facilities 110/111 resolves to
  organization_id=70, named "Siaya Hospital Clinic", city Siaya, Kenya —
  clearly the intended tenant. This script uses 70.

  A single login (any assigned facility_id) plus `source_tenant_id: 70` on
  each gateway read returns data for BOTH facilities at once — facility_id
  does not need to be looped over separately.

  Per the V3 gateway, `visit` lives on the EVALUATION service (alias
  "visits"), not reception (reception's "visit" alias 500s — the reception
  service's visits table has no organization_id column server-side; it is
  not used for this org). reception.patient records for these same patients
  DO exist, but land under organization_id=1/facility_id=1 instead of 70 —
  a quirk of the platform's quick-registration flow, confirmed by matching
  patient names 1:1 against evaluation.visits' patient_full_name. So patient
  demographics (dob, gender, mobile, address, allergies, emergency contact)
  are fetched from reception.patient using PATIENT_SOURCE_TENANT_ID=1, keyed
  by the patient ids referenced from the org-70 visits — not from
  ORGANIZATION_ID (70) directly, which returns nothing for this model.
  Unlike the V2 pipeline, V3 does not encrypt these fields for this
  endpoint.

  Volumes at time of writing: 43 visits (24 @ facility 110, 19 @ facility
  111), 4 doctor_notes, 13 vitals, 0 investigations/prescriptions/invoices —
  this is a small, ongoing dataset, not the ~900-record volume once assumed
  to exist here; there is no larger source under this org/facility pair.

IDEMPOTENCY
  Rows are tagged SOURCE_FILE = "v3_api:visit:<visit_id>" and each run
  deletes any existing rows with those keys before inserting fresh ones —
  safe to rerun (e.g. to pick up a doctor's note filed after the first
  sync).

USAGE
  python siaya_v3_visits_to_snowflake.py --dry-run
  python siaya_v3_visits_to_snowflake.py
  python siaya_v3_visits_to_snowflake.py --since 2026-06-25

ENV VARS (.env)
  AFYA_ORG70_USERNAME=ryankimeli_8f4d6f6b
  AFYA_ORG70_PASSWORD=...
  SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_WAREHOUSE / SNOWFLAKE_ROLE
  SNOWFLAKE_PRIVATE_KEY_PATH  (or SNOWFLAKE_PASSWORD)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
import requests.adapters
import snowflake.connector
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, HTTPError, Timeout

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

# ─── LOGGING ─────────────────────────────────────────────────────────────────

log = logging.getLogger("siaya_v3_visits")
if not log.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S",
    ))
    log.addHandler(h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ─── CONFIG ──────────────────────────────────────────────────────────────────

CORE_URL       = "https://core.afyaanalytics.ai/api"
EVALUATION_URL = "https://evaluation.afyaanalytics.ai/api"
RECEPTION_URL  = "https://reception.afyaanalytics.ai/api"

ORGANIZATION_ID = 70      # "Siaya Hospital Clinic" — see module docstring re: 87 vs 70
LOGIN_FACILITY_ID = 110   # any facility this account is assigned to; source_tenant_id scopes reads

# reception-service patient records for these visits land under organization_id=1
# / facility_id=1 instead of 70 — a quirk of the platform's quick-registration
# flow (confirmed by matching patient names 1:1 against evaluation.visits'
# patient_full_name). The visit/vitals/doctor_notes data is correctly tagged
# 70; only the reception patient master record is mistagged.
PATIENT_SOURCE_TENANT_ID = 1
DEFAULT_SINCE = "2026-06-25"
TOKEN_TTL_SECONDS = int(os.getenv("TOKEN_TTL_SECONDS", str(50 * 60)))
PER_PAGE = 100

TARGET_DB     = "HOSPITALS"
TARGET_SCHEMA = "SIAYA_MEDICAL_CAMP"
TARGET_TABLE  = "SIAYA_MEDICAL_CAMP_COMPLETE_DATA"

TARGET_COLUMNS = [
    "RECORD_TYPE", "PATIENT_ID", "PATIENT_NAME", "PHONE_NUMBER", "SOURCE_FILE",
    "SEX", "AGE", "DATE_OF_BIRTH", "CAMP_FILE_NUMBER", "PATIENT_ID_FILE_NO",
    "ADDRESS", "VISIT_DATE", "TIME_IN", "TIME_OUT", "ATTENDING_CLINICIAN",
    "EMERGENCY_CONTACT", "BLOOD_PRESSURE", "HEART_RATE", "OXYGEN_SATURATION",
    "TEMPERATURE", "RESPIRATORY_RATE", "WEIGHT", "HEIGHT", "BODY_MASS_INDEX",
    "BLOOD_SUGAR", "NURSES_TRIAGE_NOTES", "CHIEF_COMPLAINTS",
    "HISTORY_OF_PRESENT_ILLNESS", "PAST_MEDICAL_SURGICAL_HISTORY",
    "KNOWN_ALLERGIES", "CURRENT_MEDICATIONS", "GENERAL_APPEARANCE",
    "SYSTEMIC_EXAM_CVS", "SYSTEMIC_EXAM_RESP", "SYSTEMIC_EXAM_GIT",
    "SYSTEMIC_EXAM_CNS", "SYSTEMIC_EXAM_MSK_SKIN", "PROVISIONAL_DIAGNOSIS",
    "LAB_IMAGING_INVESTIGATIONS_ORDERED", "TREATMENT_PLAN_PRESCRIPTIONS",
    "FOLLOW_UP_REFERRAL_NOTES", "DOCTORS_NOTES", "LAB_RESULTS", "RAW_NOTES",
    "DOCUMENT_TYPE", "DOCUMENT_DATE", "BABY_NAME", "GESTATIONAL_AGE",
    "AGE_OR_DAY_OF_LIFE", "BIRTH_WEIGHT", "CURRENT_WEIGHT", "DELIVERY_DETAILS",
    "APGAR_SCORES", "DIAGNOSES", "CLINICAL_SUMMARY", "VITALS",
    "EXAMINATION_FINDINGS", "INVESTIGATIONS", "TREATMENTS",
    "FEEDING_AND_SUPPORT", "DISCHARGE_PLAN", "CLINICIAN_NAME",
]

# ─── V3 AUTH ─────────────────────────────────────────────────────────────────

_session: requests.Session | None = None
_token_cache: tuple[str, float] | None = None


def _http() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=0)
        _session.mount("https://", adapter)
        _session.mount("http://", adapter)
    return _session


def _generate_token() -> str:
    user = os.getenv("AFYA_ORG70_USERNAME")
    pwd  = os.getenv("AFYA_ORG70_PASSWORD")
    if not user or not pwd:
        raise RuntimeError("Missing AFYA_ORG70_USERNAME / AFYA_ORG70_PASSWORD env vars")
    r = _http().post(f"{CORE_URL}/v1/login",
                      json={"username": user, "password": pwd, "facility_id": LOGIN_FACILITY_ID},
                      timeout=30)
    if not r.ok:
        raise RuntimeError(f"V3 login failed: {r.status_code} · {r.text[:200]}")
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in login response: {r.text[:200]}")
    return token


def _token(invalidate: bool = False) -> str:
    global _token_cache
    if invalidate:
        _token_cache = None
    if _token_cache and (time.time() - _token_cache[1]) < TOKEN_TTL_SECONDS:
        return _token_cache[0]
    token = _generate_token()
    _token_cache = (token, time.time())
    return token


# ─── V3 GATEWAY ──────────────────────────────────────────────────────────────

def _read_page(model: str, page: int, *, max_retries: int = 6) -> dict:
    url = f"{EVALUATION_URL}/v1/gateway"
    body = {"action": "read", "model": model, "per_page": PER_PAGE, "page": page,
            "source_tenant_id": ORGANIZATION_ID}
    attempt, wait = 0, 10
    while True:
        attempt += 1
        headers = {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json", "Accept": "application/json"}
        try:
            r = _http().post(url, headers=headers, json=body, timeout=60)
        except (Timeout, ConnectionError) as e:
            if attempt >= max_retries:
                raise
            log.warning("network error %s — retry in %ss (%s/%s)", e, wait, attempt, max_retries)
            time.sleep(wait)
            wait = min(wait * 2, 120)
            continue

        if r.status_code == 401 and attempt < max_retries:
            _token(invalidate=True)
            continue
        if r.status_code == 429 and attempt < max_retries:
            time.sleep(wait)
            wait = min(wait * 2, 120)
            continue
        if r.status_code in (500, 502, 503, 504) and attempt < max_retries:
            time.sleep(wait)
            wait = min(wait * 2, 120)
            continue

        r.raise_for_status()
        return r.json()


def read_all(model: str) -> list[dict]:
    out: list[dict] = []
    page = 1
    while True:
        payload = _read_page(model, page)
        rows = payload.get("data") or []
        out.extend(rows)
        meta = payload.get("pagination") or {}
        if page >= meta.get("last_page", 1):
            break
        page += 1
    log.info("read_all %-20s -> %d rows (org %s)", model, len(out), ORGANIZATION_ID)
    return out


def fetch_patients_by_ids(ids: set[int], chunk_size: int = 100) -> dict[int, dict]:
    """Batch-fetch reception.patient rows by id. Must use
    PATIENT_SOURCE_TENANT_ID (1), not ORGANIZATION_ID (70) — see the constant's
    docstring comment above.
    """
    ids = sorted(i for i in ids if i)   # drop None/0 — 0 is a "no patient" sentinel some visits carry
    out: dict[int, dict] = {}
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i + chunk_size]
        body = {"action": "read", "model": "patient", "per_page": len(chunk), "page": 1,
                "ids": chunk, "source_tenant_id": PATIENT_SOURCE_TENANT_ID}
        headers = {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json", "Accept": "application/json"}
        r = _http().post(f"{RECEPTION_URL}/v1/gateway", headers=headers, json=body, timeout=60)
        r.raise_for_status()
        for row in r.json().get("data") or []:
            out[row["id"]] = row
    log.info("fetch_patients_by_ids -> matched %d of %d requested", len(out), len(ids))
    return out


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


# ─── FIELD HELPERS ───────────────────────────────────────────────────────────

def _join(*parts: str | None, sep: str = "; ") -> str | None:
    vals = []
    for p in parts:
        if isinstance(p, list):
            p = "; ".join(str(x) for x in p if x)
        if p and str(p).strip():
            vals.append(str(p).strip())
    return sep.join(vals) if vals else None


def _fmt(value: Any, suffix: str = "") -> str | None:
    if value is None or value == "":
        return None
    return f"{value}{suffix}"


def _latest_by(rows: list[dict], key_field: str) -> dict[Any, dict]:
    out: dict[Any, dict] = {}
    for row in rows:
        k = row.get(key_field)
        if k is None:
            continue
        prev = out.get(k)
        if prev is None or (row.get("id") or 0) > (prev.get("id") or 0):
            out[k] = row
    return out


def build_row(
    visit: dict,
    note: dict | None,
    vitals: dict | None,
    patient: dict | None,
    clinician_names: dict[int, str],
) -> dict:
    visit_id = visit["id"]
    created  = _parse_dt(visit.get("created_at"))

    clinician = None
    for uid in (note and note.get("doctor_id"), visit.get("user")):
        if uid in clinician_names:
            clinician = clinician_names[uid]
            break

    # vitals.height is stored in metres (bmi = weight_kg / height_m^2 checks out
    # against sample data) — convert to cm to match the cm convention used by
    # every other source feeding this table.
    height_cm = None
    if vitals and vitals.get("height") not in (None, ""):
        try:
            height_cm = round(float(vitals["height"]) * 100)
        except (TypeError, ValueError):
            height_cm = vitals.get("height")

    bp    = _fmt(vitals and vitals.get("blood_pressure"), " mmHg")
    hr    = _fmt(vitals and vitals.get("pulse"), " bpm")
    spo2  = _fmt(vitals and vitals.get("oxygen_saturation"), " %")
    temp  = _fmt(vitals and vitals.get("temperature"), " °C")
    resp  = _fmt(vitals and vitals.get("respiratory_rate"), " breaths/min")
    wt    = _fmt(vitals and vitals.get("weight"), " kg")
    ht    = _fmt(height_cm, " cm") if height_cm is not None else None
    sugar = _join(
        _fmt(vitals and (vitals.get("random_blood_sugar") or vitals.get("fasting_blood_sugar") or vitals.get("blood_sugar"))),
        vitals and (vitals.get("blood_sugar_unit") or vitals.get("blood_sugar_units")),
        sep=" ",
    )

    vitals_summary = _join(bp, hr, temp, resp, spo2, wt, ht, sep=", ")

    exam_by_system = _join(
        note and note.get("cardiovascular_exam"),
        note and note.get("respiratory_exam"),
        note and note.get("abdominal_exam"),
        note and note.get("neurological_exam"),
        note and note.get("musculoskeletal_exam"),
        sep="\n",
    )
    diagnosis = _join(
        note and note.get("primary_diagnosis"), note and note.get("diagnosis"),
        note and note.get("moh_diagnosis"), note and note.get("secondary_diagnoses"),
        sep="; ",
    )
    past_history = _join(
        note and note.get("past_medical_history"), note and note.get("surgical_history"),
        patient and patient.get("chronic_conditions"), sep="; ",
    )
    patient_dob = _parse_dt(patient.get("dob")) if patient else None
    address = _join(patient and patient.get("address"), patient and patient.get("city"),
                     patient and patient.get("sub_county"), patient and patient.get("county"), sep=", ")
    emergency_contact = _join(
        patient and patient.get("emergency_contact_name"),
        patient and patient.get("emergency_contact_phone"),
        patient and patient.get("emergency_contact_relationship"),
        sep=" / ",
    )
    doctors_notes = _join(
        note and note.get("presenting_complaints"),
        note and note.get("history_of_present_illness"),
        note and note.get("examination"),
        note and note.get("physical_examination"),
        diagnosis,
        note and note.get("treatment_plan"),
        sep="\n",
    )

    return {
        "RECORD_TYPE":    "V3_EMR_VISIT",
        "PATIENT_ID":     f"V3-P{visit.get('patient')}" if visit.get("patient") else None,
        "PATIENT_NAME":   visit.get("patient_full_name") or (patient and patient.get("full_name")),
        "PHONE_NUMBER":   patient and patient.get("mobile"),
        "SOURCE_FILE":    f"v3_api:visit:{visit_id}",
        "SEX":            patient and patient.get("gender"),
        "AGE":            patient and (patient.get("age_friendly") or patient.get("age")),
        "DATE_OF_BIRTH":  patient_dob.strftime("%Y-%m-%d") if patient_dob else None,
        "CAMP_FILE_NUMBER": None,
        "PATIENT_ID_FILE_NO": (patient and patient.get("patient_no")) or (str(visit["patient"]) if visit.get("patient") else None),
        "ADDRESS":        address,
        "VISIT_DATE":     created.strftime("%Y-%m-%d") if created else None,
        "TIME_IN":        created.strftime("%H:%M:%S") if created else None,
        "TIME_OUT":       None,
        "ATTENDING_CLINICIAN": clinician,
        "EMERGENCY_CONTACT": emergency_contact,
        "BLOOD_PRESSURE": bp,
        "HEART_RATE":     hr,
        "OXYGEN_SATURATION": spo2,
        "TEMPERATURE":    temp,
        "RESPIRATORY_RATE": resp,
        "WEIGHT":         wt,
        "HEIGHT":         ht,
        "BODY_MASS_INDEX": _fmt(vitals and vitals.get("bmi")),
        "BLOOD_SUGAR":    sugar,
        "NURSES_TRIAGE_NOTES": _join(vitals and vitals.get("symptoms"), vitals and vitals.get("notes"), sep="; "),
        "CHIEF_COMPLAINTS": _join(note and note.get("chief_complaints"), note and note.get("presenting_complaints"), sep=" / "),
        "HISTORY_OF_PRESENT_ILLNESS": note and note.get("history_of_present_illness"),
        "PAST_MEDICAL_SURGICAL_HISTORY": past_history,
        "KNOWN_ALLERGIES": _join(note and note.get("allergy_history"), vitals and vitals.get("allergies"), patient and patient.get("allergies"), sep="; "),
        "CURRENT_MEDICATIONS": note and note.get("medication_history"),
        "GENERAL_APPEARANCE": note and note.get("general_appearance"),
        "SYSTEMIC_EXAM_CVS": note and note.get("cardiovascular_exam"),
        "SYSTEMIC_EXAM_RESP": note and note.get("respiratory_exam"),
        "SYSTEMIC_EXAM_GIT": note and note.get("abdominal_exam"),
        "SYSTEMIC_EXAM_CNS": note and note.get("neurological_exam"),
        "SYSTEMIC_EXAM_MSK_SKIN": note and note.get("musculoskeletal_exam"),
        "PROVISIONAL_DIAGNOSIS": diagnosis,
        "LAB_IMAGING_INVESTIGATIONS_ORDERED": note and note.get("investigations"),
        "TREATMENT_PLAN_PRESCRIPTIONS": note and note.get("treatment_plan"),
        "FOLLOW_UP_REFERRAL_NOTES": None,
        "DOCTORS_NOTES":  doctors_notes,
        "LAB_RESULTS":    None,
        "RAW_NOTES":      None,
        "DOCUMENT_TYPE":  None,
        "DOCUMENT_DATE":  None,
        "BABY_NAME":      None,
        "GESTATIONAL_AGE": None,
        "AGE_OR_DAY_OF_LIFE": None,
        "BIRTH_WEIGHT":   None,
        "CURRENT_WEIGHT": None,
        "DELIVERY_DETAILS": None,
        "APGAR_SCORES":   None,
        "DIAGNOSES":      diagnosis,
        "CLINICAL_SUMMARY": doctors_notes,
        "VITALS":         vitals_summary,
        "EXAMINATION_FINDINGS": _join(note and note.get("examination"), exam_by_system, sep="\n"),
        "INVESTIGATIONS": note and note.get("investigations"),
        "TREATMENTS":     note and note.get("treatment_plan"),
        "FEEDING_AND_SUPPORT": None,
        "DISCHARGE_PLAN": None,
        "CLINICIAN_NAME": clinician,
    }


# ─── SNOWFLAKE ───────────────────────────────────────────────────────────────

def _snowflake_connect():
    kwargs: dict[str, Any] = dict(
        user=os.getenv("SNOWFLAKE_USER", "").strip(),
        account=os.getenv("SNOWFLAKE_ACCOUNT", "").strip(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "").strip(),
        role=os.getenv("SNOWFLAKE_ROLE", "").strip() or None,
        database=TARGET_DB,
        schema=TARGET_SCHEMA,
    )
    pk_path = (os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or "").strip()
    pwd     = (os.getenv("SNOWFLAKE_PASSWORD") or "").strip()
    if pk_path:
        kwargs["private_key_file"] = pk_path
    elif pwd:
        kwargs["password"] = pwd
    else:
        raise RuntimeError("Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD in .env")
    return snowflake.connector.connect(**kwargs)


def upsert_rows(rows: list[dict]) -> None:
    if not rows:
        log.info("nothing to write")
        return

    source_files = [r["SOURCE_FILE"] for r in rows]
    conn = _snowflake_connect()
    try:
        cur = conn.cursor()
        placeholders = ", ".join(["%s"] * len(source_files))
        cur.execute(
            f"DELETE FROM {TARGET_DB}.{TARGET_SCHEMA}.{TARGET_TABLE} "
            f"WHERE SOURCE_FILE IN ({placeholders})",
            source_files,
        )
        log.info("cleared %d pre-existing row(s) for these visits (idempotent rerun)", cur.rowcount)

        col_list = ", ".join(TARGET_COLUMNS)
        val_placeholders = ", ".join(["%s"] * len(TARGET_COLUMNS))
        values = [[row[c] for c in TARGET_COLUMNS] for row in rows]
        cur.executemany(
            f"INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.{TARGET_TABLE} ({col_list}) "
            f"VALUES ({val_placeholders})",
            values,
        )
        conn.commit()
        log.info("inserted %d row(s) into %s.%s.%s", len(rows), TARGET_DB, TARGET_SCHEMA, TARGET_TABLE)
    finally:
        conn.close()


# ─── ORCHESTRATION ───────────────────────────────────────────────────────────

def run(since: datetime, limit: int | None, dry_run: bool) -> list[dict]:
    visits = read_all("visits")
    visits = [v for v in visits if (dt := _parse_dt(v.get("created_at"))) and dt >= since]
    visits.sort(key=lambda v: v["id"])
    if limit:
        visits = visits[:limit]
    if not visits:
        log.info("no v3 visits found for org %s since %s", ORGANIZATION_ID, since.date())
        return []

    notes_by_visit  = _latest_by(read_all("doctor_notes"), "visit_id")
    vitals_by_visit = _latest_by(read_all("vitals"), "visit_id")

    user_ids = {v.get("user") for v in visits} | {n.get("doctor_id") for n in notes_by_visit.values()}
    users = read_all_users_by_ids({u for u in user_ids if u is not None})

    patients = fetch_patients_by_ids({v.get("patient") for v in visits})

    rows = [
        build_row(
            visit,
            notes_by_visit.get(visit["id"]),
            vitals_by_visit.get(visit["id"]),
            patients.get(visit.get("patient")),
            users,
        )
        for visit in visits
    ]

    if dry_run:
        log.info("DRY RUN — %d row(s) would be appended (not writing to Snowflake)", len(rows))
        for row in rows:
            print(json.dumps(row, indent=2, ensure_ascii=False, default=str))
    else:
        upsert_rows(rows)

    return rows


def read_all_users_by_ids(ids: set[int]) -> dict[int, str]:
    if not ids:
        return {}
    url = f"{CORE_URL}/v1/gateway"
    names: dict[int, str] = {}
    page = 1
    while True:
        headers = {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json", "Accept": "application/json"}
        body = {"action": "read", "model": "users", "per_page": PER_PAGE, "page": page, "source_tenant_id": ORGANIZATION_ID}
        r = _http().post(url, headers=headers, json=body, timeout=60)
        r.raise_for_status()
        d = r.json()
        for u in d.get("data") or []:
            if u.get("id") in ids:
                names[u["id"]] = u.get("full_name")
        meta = d.get("pagination") or {}
        if page >= meta.get("last_page", 1):
            break
        page += 1
    return names


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--since", default=DEFAULT_SINCE, help=f"only visits created on/after this date (default {DEFAULT_SINCE})")
    p.add_argument("--limit", type=int, default=None, help="cap number of visits processed (for testing)")
    p.add_argument("--dry-run", action="store_true", help="print rows instead of writing to Snowflake")
    args = p.parse_args()

    since = datetime.strptime(args.since, "%Y-%m-%d")
    run(since=since, limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
