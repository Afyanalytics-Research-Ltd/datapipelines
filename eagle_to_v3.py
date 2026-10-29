"""
eagle_to_v3.py — Eagle Nursing Home's Patcom HMS (custom MySQL 8) → V3, through the model gateway.

Targets (.env, same loader for both, chosen with --target / EAGLE_TARGET):
  eagle       migrate platform, org 13 / facility 14 (AFYA_EAGLE_*)
  eagle_prod  production, org 199 EAGLE NURSING HOME / facility 196 (AFYA_EAGLE_PROD_*)

Source: the Snowflake mirror HOSPITALS.EAGLE_NURSING_HOME_HISTORICAL_DATA (load_eagle_to_snowflake.py). Patcom keys
everything by text codes, not ids: a patient is patien_registration.customercode ('ENH-095451'), a visit is
patient_visit.visitcode / patient_ipvisit.visitcode ('ENH/OP/000027/2024', 'ENH/IP/000006/2024'), and every bill,
result and note points back with opnumber (+ registrationnu). Rows are matched on those codes; each V3 row carries a
uuid5 of its source key and is posted with match_on=uuid, so a re-run updates instead of duplicating.

Steps (run in STEPS order):
  users / user_profiles   umanusers → core/users (random password, reset enforced, "<user>@eagle.migrated.eagle-<id>.invalid"
                          so no mail goes out) + profiles (names).
  insurance               insurances + the insurer / scheme pairs patients and visits use → core insurance companies
                          (matched to the ones V3 already has) and schemes.
  patients / patient_insurance   patien_registration → reception/patient (patient_no = Patcom's customercode);
                          insured patients → reception/patient_insurance.
  visits                  patient_visit (outpatient) and patient_ipvisit (inpatient) → evaluation/visits, unique_id = visitcode.
  vitals                  triage → evaluation/vitals.
  doctor_notes            finalconsultation (+ that visit's patient_medical_history) → evaluation/doctor_notes;
                          medical_reports, sickleave, theatre_booking → doctor notes too.
  diagnoses               diagnosis → evaluation/diagnoses.
  catalogue / procedure_prices   lab_codes, radiologyservices, servicesmaster, consultation types → evaluation/procedures
                          (code "ENH-<Patcom code>") + cash / insurance prices.
  investigations          labills / radbills / servbills → evaluation/investigations, lab and radiology results as text;
  lab_results             results → evaluation/investigation_results (one per test) + investigation_result_details (one
                          per parameter, 1.5M rows: run last).
  stores / products / stock   store, pharmacy_item → inventory; openingstock batches with stock on hand → batches + stock.
  prescriptions           pharmbills → evaluation/prescriptions (status 'active' — what V3 lists).
  wards / admissions / ward_stays / discharges / inpatient_notes   addward, bedmaster, patient_ipvisit, dischargesummary,
                          nursingnotes, nursingcardex, patientdata → inpatient.
  payments                modesofpayments, deposit, ipreceipts → finance/evaluation_payment (+ one detail each), what
                          V3's cash receipts and sales-summary screens read.
  invoices                invoicebills / ipinvoicebills → finance/invoice, totals summed from the bills carrying the code.
  suppliers / appointments   suppliers → inventory/supplier; bookings_tca → reception/appointment.

    python eagle_to_v3.py --target eagle --steps users patients --limit 3 --dry-run
    python eagle_to_v3.py --target eagle --steps users patients --limit 3
"""
from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
import sys
import threading
import time
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from itertools import islice
from pathlib import Path

sys.path.insert(0, "/opt/airflow/dags")
log = logging.getLogger("eagle_to_v3")

SF = "HOSPITALS.EAGLE_NURSING_HOME_HISTORICAL_DATA"
NS = uuid.UUID("6b0f3c2e-5d4a-4e8b-9c71-2a8e4f1d7b35")      # never change: rows are matched by it
CHUNK = 20_000                                             # payloads built and posted per batch (big tables stream)
# models with a facility_id column (gateway describe, 2026-10-27): a branch's rows carry their facility
FACILITY_MODELS = {"patient", "patient_insurance", "visits", "vitals", "doctor_notes", "investigations", "prescriptions",
                   "consumables", "procedure_prices", "evaluation_payment", "evaluation_payment_detail", "invoice",
                   "admission", "note", "ward", "bed", "visit_ward", "discharge", "store", "stock", "batch", "product",
                   "supplier", "appointment"}
# admissions come before orders: inpatient investigations / prescriptions carry their admission_id
STEPS = ("users", "user_profiles", "insurance", "patients", "patient_insurance", "visits", "wards", "admissions",
         "ward_stays", "discharges", "inpatient_notes", "vitals", "doctor_notes", "diagnoses", "catalogue",
         "procedure_prices", "investigations", "stores", "products", "stock", "prescriptions", "payments",
         "payment_details", "invoices", "suppliers", "appointments", "lab_results")


# ── pure helpers (tests/test_eagle_to_v3.py) ────────────────────────────────
def gid(table: str, key) -> str:
    """Stable uuid of a source row (key = its Patcom code or id)."""
    return str(uuid.uuid5(NS, f"eagle/{table}/{str(key).strip()}"))


def clean(v) -> str | None:
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="replace")
    s = "" if v is None else str(v).strip()
    return None if not s or s.lower() in ("null", "none", "nan", "undefined") or s.startswith("Select ") or s.startswith("-Select") else s


def words(v) -> str:
    """Case-, dot- and spacing-insensitive form of a name, for matching."""
    return " ".join(str(v or "").lower().replace(".", " ").split())


def num(v) -> float | None:
    m = re.search(r"-?\d+(?:\.\d+)?", (clean(v) or "").replace(",", ""))
    return float(m[0]) if m else None


def phone(v) -> str | None:
    s = re.sub(r"[^\d+]", "", clean(v) or "")
    if len(s) == 9 and s[0] in "17":          # Patcom drops the leading 0 ('715817687')
        s = "0" + s
    return s if len(s) >= 7 and s.strip("0") else None


def day(v) -> str | None:
    """'YYYY-MM-DD' from ISO ('2024-03-14', '2024-03-14 13:10:38') or Patcom's month-first form ('3/14/2024');
    None for empty, zero or impossible dates."""
    s = clean(v)
    if not s or s.startswith("0000"):
        return None
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y, mo, d = int(m[1]), int(m[2]), int(m[3])
    else:
        m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", s)
        if not m:
            return None
        mo, d, y = int(m[1]), int(m[2]), int(m[3])
        if mo > 12 and d <= 12:               # a day-first slip ('31/3/2024')
            mo, d = d, mo
    try:
        out = date(y, mo, d)
    except ValueError:
        return None
    return out.isoformat() if 1900 <= y <= 2100 else None


def stamp(d, t=None) -> str | None:
    """'YYYY-MM-DD HH:MM:SS' from a date (+ optional time, or the time inside a datetime), or None."""
    dd = day(d)
    if not dd:
        return None
    tm = clean(t) or ((clean(d) or "")[10:].strip() if re.match(r"^\d{4}-", clean(d) or "") else "")
    m = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?", tm or "")
    if m and int(m[1]) < 24 and int(m[2]) < 60:
        return f"{dd} {int(m[1]):02d}:{m[2]}:{m[3] or '00'}"
    return f"{dd} 00:00:00"


_TAG = re.compile(r"<\s*(br|/p|/div|/li|/h\d)\s*/?\s*>", re.I)


def strip_html(v) -> str | None:
    """Patcom's notes are CKEditor HTML ('<p>DYSPHAGIA</p>\\n<p>&nbsp;</p>') → plain lines."""
    s = clean(v)
    if not s:
        return None
    s = _TAG.sub("\n", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s).replace("\xa0", " ")
    lines = [" ".join(x.split()) for x in s.splitlines()]
    out = "\n".join(x for x in lines if x)
    return out or None


def text_join(*parts) -> str | None:
    out = "\n".join(f"{label}: {clean(v)}" if label else clean(v) for label, v in parts if clean(v))
    return out or None


def split_name(first, middle, last) -> tuple[str, str | None, str]:
    """Patcom splits names itself, but leaves blanks: fall back to the other parts so V3's required ones are set."""
    f, m, l = (" ".join((clean(x) or "").split()) or None for x in (first, middle, last))
    if not f and not l:
        parts = (m or "Unknown").split()
        return parts[0], " ".join(parts[1:-1]) or None, parts[-1]
    if not l:
        if m:
            return f, None, m
        return f, None, f
    if not f:
        return (m, None, l) if m else (l, None, l)
    return f, m, l


TITLES = {"dr": "Dr", "doc": "Dr", "doctor": "Dr", "prof": "Prof", "mr": "Mr", "mrs": "Mrs", "ms": "Ms", "miss": "Ms",
          "mss": "Ms", "sr": None, "nurse": None}
TITLE_CODES = {"Mr": 1, "Mrs": 2, "Ms": 3, "Dr": 4, "Prof": 5}   # core UserProfile::getTitleNameAttribute


def person_name(full) -> tuple[str | None, str, str | None, str]:
    """'Dr Peter Wanyama Situma' → ('Dr', 'Peter', 'Wanyama', 'Situma'); 'dr.roy' → ('Dr', 'roy', None, 'roy')."""
    parts = (clean(full) or "").replace(".", ". ").replace("_", " ").split()
    title = None
    if parts and parts[0].rstrip(".").lower() in TITLES:
        title = TITLES[parts.pop(0).rstrip(".").lower()]
    parts = [p.rstrip(".") for p in parts if p.rstrip(".")]
    if not parts:
        return title, "Staff", None, "Member"
    if len(parts) == 1:
        return title, parts[0], None, parts[0]
    return title, parts[0], " ".join(parts[1:-1]) or None, parts[-1]


def gender(v) -> str | None:
    g = (clean(v) or "").lower()
    return "male" if g.startswith("m") else "female" if g.startswith("f") else None


INSURER_HINT = ("insur", "assur", "nhif", "sha", "shif", "medical scheme", "health plan")


def is_insured(maintype, subtype) -> bool:
    """Patcom: maintype 'Insurance' / 'CASH' (and corporate accounts) — anything but cash is billed to an account."""
    mt, st = words(maintype), words(subtype)
    return bool(mt) and mt != "cash" and bool(st) and st != "cash"


# ── payloads ──
def user_payload(r: dict) -> dict:
    uname = re.sub(r"[^a-z0-9._-]", "", (clean(r.get("username")) or f"user{r['usrid']}").lower()) or f"user{r['usrid']}"
    return {"uuid": gid("umanusers", r["usrid"]), "username": f"eagle.{uname}"[:60],
            "email": f"{uname}@eagle.migrated.eagle-{r['usrid']}.invalid", "password": str(uuid.uuid4()),
            "enforce_password_reset": 1, "active": 0 if words(r.get("userstatus")) in ("false", "inactive", "0") else 1,
            "employee_number": f"ENH-U-{r['usrid']}"}


def user_profile_payload(r: dict, user_id: int) -> dict:
    title, first, middle, last = person_name(r.get("userfullnames") or r.get("username"))
    email = clean(r.get("email"))
    return {"uuid": gid("user_profiles", r["usrid"]), "user_id": user_id,
            **({"title": TITLE_CODES[title]} if title in TITLE_CODES else {}),
            "first_name": first[:100], "middle_name": (middle or "")[:100] or None, "last_name": last[:100],
            "job_description": " — ".join(x for x in (clean(r.get("userdescription")), clean(r.get("usergroup"))) if x) or None,
            "staff_number": f"ENH-U-{r['usrid']}", "phone": phone(r.get("phoneno")), "id_number": clean(r.get("idnumber")),
            "work_email": email if email and "@" in email else None, "date_of_employment": day(r.get("joindate"))}


def patient_payload(r: dict, branch: str | None = None) -> dict:
    first, middle, last = split_name(r.get("customername"), r.get("customermiddlename"), r.get("customerlastname"))
    reg = day(r.get("registrationdate")) or day(r.get("updatedtime"))
    dob = day(r.get("birth_date")) or day(r.get("datefrom"))
    if not dob and reg and num(r.get("datefrom")) is not None and r.get("datefrom") and str(r.get("datefrom")).isdigit():
        dob = None                                       # an age, not a date: kept in notes
    email = clean(r.get("emailid1"))
    insured = is_insured(r.get("maintype"), r.get("subtype"))
    notes = text_join(("Patcom patient code", r.get("customercode")), ("MRD no", r.get("mrdno")),
                      ("Age at registration", " ".join(x for x in (clean(r.get("datefrom")) and f"{clean(r.get('datefrom'))} y",
                                                                   clean(r.get("ageduration")) and f"{clean(r.get('ageduration'))} m") if x)),
                      ("Account", " / ".join(x for x in (clean(r.get("maintype")), clean(r.get("subtype")), clean(r.get("accountname"))) if x)),
                      ("Member no", r.get("memberno")), ("Occupation", r.get("occupation")), ("Mother", r.get("mothername")),
                      ("Branch", branch), ("Registered by", r.get("user")))
    marital = (clean(r.get("maritalstatus")) or "").lower() or None
    when = stamp(r.get("registrationdate"), r.get("registrationtime")) or stamp(r.get("updatedtime"))
    return {"uuid": gid("patient", r["customercode"]), "patient_no": clean(r["customercode"]),
            "first_name": first[:100], "middle_name": (middle or "")[:100] or None, "last_name": last[:100],
            "dob": dob, "gender": gender(r.get("gender")), "id_no": clean(r.get("nationalidnumber")),
            "id_type": "national_id" if clean(r.get("nationalidnumber")) else None,
            "marital_status": marital, "blood_group": clean(r.get("bloodgroup")), "occupation": clean(r.get("occupation")),
            "mobile": phone(r.get("mobilenumber")), "alt_phone": phone(r.get("phonenumber1")),
            "email": email if email and "@" in email else None,
            "address": ", ".join(x for x in (clean(r.get("address1")), clean(r.get("address2")), clean(r.get("area"))) if x)[:255] or None,
            "city": clean(r.get("city")), "county": clean(r.get("state")), "postal_code": clean(r.get("pincode")),
            "country": clean(r.get("country")),
            "emergency_contact_name": clean(r.get("kinname")), "emergency_contact_phone": phone(r.get("kincontactnumber")),
            "emergency_contact_relationship": clean(r.get("kinrelation")),
            "insurance_status": "insured" if insured else "uninsured",
            "insurance_policy_no": clean(r.get("memberno")) if insured else None,
            "registration_date": reg, "status": "active", "notes": notes,
            "created_at": when, "updated_at": stamp(r.get("updatedtime")) or when}


def visit_payload(r: dict, patient_id: int, *, inpatient: bool, clinic: int | None, doctor: int | None) -> dict:
    when = (stamp(r.get("consultationdate"), r.get("consultationtime")) or stamp(r.get("registrationdate"))
            or stamp(r.get("updateddate")))
    insured = words(r.get("paymenttypename")) not in ("cash", "")
    return {"uuid": gid("visit", r["visitcode"]), "patient": patient_id, "clinic": clinic, "inpatient": 1 if inpatient else 0,
            "patient_full_name": " ".join(x for x in (clean(r.get("patientfirstname")), clean(r.get("patientmiddlename")),
                                                      clean(r.get("patientlastname"))) if x)[:255] or None,
            "purpose": clean(r.get("department")), "type_of_visit": "IP Visit" if inpatient else (clean(r.get("consultationtype")) or "OP Visit"),
            "payment_mode": "insurance" if insured else "cash", "scheme": None,
            "status": "completed", "unique_id": clean(r["visitcode"]), "user": doctor,
            "created_at": when, "updated_at": stamp(r.get("updateddate")) or when}


# plausible range per vital; a value 10× too big ('366' for 36.6, '17200' for 172) is a slipped decimal point
VITAL_RANGES = {"height": (20, 250), "weight": (0.3, 350), "temperature": (25, 45), "bmi": (5, 90), "pulse": (20, 300),
                "respiratory_rate": (3, 100), "systolic_bp": (40, 300), "diastolic_bp": (20, 200)}


def vital(name: str, v, dropped: list) -> float | None:
    """A vital that fits V3's column: as typed, or with the decimal point put back; else None (kept in `dropped`)."""
    x = num(v)
    if x is None or x == 0:
        return None
    lo, hi = VITAL_RANGES[name]
    for cand in (x, x / 10, x / 100):
        if lo <= cand <= hi:
            return round(cand, 2)
    dropped.append(f"{name} {clean(v)}")
    return None


def vitals_payload(r: dict, visit_id: int, patient_id: int) -> dict:
    dropped: list[str] = []
    sys_, dia = vital("systolic_bp", r.get("bpsystolic"), dropped), vital("diastolic_bp", r.get("bpdiastolic"), dropped)
    pulse, resp = vital("pulse", r.get("pulse"), dropped), vital("respiratory_rate", r.get("respiration"), dropped)
    chronic = ", ".join(n for n, k in (("Diabetes", "dm"), ("Hypertension", "hypertension"), ("Respiratory", "respiratory"),
                                       ("Cardiac", "cardiac"), ("Epilepsy", "epilepsy"), ("Renal", "renal"))
                        if words(r.get(k)) in ("checked", "on", "yes", "1"))
    other = clean(r.get("other"))
    when = stamp(r.get("vitaldate"), r.get("vitaltime")) or stamp(r.get("updatedate"))
    va = text_join(("Right VA", r.get("rightvisualacuity")), ("Left VA", r.get("leftvisualacuity")),
                   ("Unaided R", r.get("unaidedr")), ("Unaided L", r.get("unaidedl")))
    iop = text_join(("Right", r.get("iopright")), ("Left", r.get("iopleft"))) if (num(r.get("iopright")) or num(r.get("iopleft"))) else None
    spo2 = num(r.get("spo2"))
    height, weight = vital("height", r.get("height"), dropped), vital("weight", r.get("weight"), dropped)
    temp, bmi = vital("temperature", r.get("celsius"), dropped), vital("bmi", r.get("bmi"), dropped)
    notes = "\n".join(x for x in (other, f"Out-of-range in Patcom (not loaded as numbers): {', '.join(dropped)}" if dropped else None) if x)
    return {"uuid": gid("triage", r["vitalid"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            "height": height, "weight": weight, "temperature": temp,
            "systolic_bp": int(sys_) if sys_ else None, "diastolic_bp": int(dia) if dia else None,
            "bp_systolic": int(sys_) if sys_ else None, "bp_diastolic": int(dia) if dia else None,
            "blood_pressure": f"{int(sys_)}/{int(dia)}" if sys_ and dia else None,
            "pulse": int(pulse) if pulse else None, "respiratory_rate": int(resp) if resp else None, "bmi": bmi,
            "oxygen_saturation": int(spo2) if spo2 and 0 < spo2 <= 100 else None,
            "chronic_illnesses": chronic or None, "notes": notes or None, "visual_acuity_uncorrected": va,
            "intraocular_pressure": iop, "recorded_by_name": clean(r.get("user")),
            "recorded_at": when, "created_at": when, "updated_at": stamp(r.get("updatedate")) or when}


HISTORY_FLAGS = (("Diabetes", "dm"), ("Cardiac", "cardiac"), ("Hypertension", "hypertension"), ("Epilepsy", "epilepsy"),
                 ("Renal", "renal"))


def history_fields(h: dict | None) -> dict:
    """patient_medical_history (the triage history form) → doctor-note history fields."""
    if not h:
        return {}
    flags = [n for n, k in HISTORY_FLAGS if str(h.get(k) or "0") == "1"]
    if str(h.get("none") or "0") == "1" and not flags:
        flags = ["None"]
    social = [n for n, k in (("Smoking", "smoking"), ("Alcohol", "alcohol"), ("Drugs", "drugs")) if str(h.get(k) or "0") == "1"]
    allergy = text_join(("Food", h.get("food_allergy")), ("Drug", h.get("drug_allergy")))
    obst = text_join(("Para", h.get("para")), ("Gravida", h.get("gravida")), ("LMP", day(h.get("lmp")) or h.get("lmp")),
                     ("EDD", day(h.get("edd")) or h.get("edd")))
    return {k: v for k, v in {
        "chief_complaints": strip_html(h.get("complaints")), "past_medical_history": ", ".join(flags) or None,
        "social_history": ", ".join(social) or None, "family_history": strip_html(h.get("family_history")),
        "surgical_history": strip_html(h.get("surgical_history")), "allergy_history": allergy,
        "medication_history": text_join(("Transfusion history", strip_html(h.get("transfusion_history")))),
        "remarks": obst}.items() if v}


EYE_PARTS = ("notes", "lid", "conjunctiva", "cornea", "pupil", "vitreous", "lens", "ac")


def consultation_note_payload(r: dict, visit_id: int, patient_id: int, history: dict | None) -> dict:
    text = strip_html(r.get("consultation"))
    eye = "\n".join(f"{p.upper()}: R {clean(r.get('r_' + p)) or '-'} / L {clean(r.get('l_' + p)) or '-'}"
                    for p in EYE_PARTS if clean(r.get("r_" + p)) or clean(r.get("l_" + p)))
    dx = " ".join(x for x in (clean(r.get("diagnosis")), f"[{clean(r.get('code'))}]" if clean(r.get("code")) else None) if x)
    when = stamp(r.get("timestamp")) or stamp(r.get("consultingdate"))
    hist = history_fields(history)
    remarks = "\n".join(x for x in (hist.pop("remarks", None), f"Patcom notes id: {clean(r.get('notesid'))}" if clean(r.get("notesid")) else None) if x)
    return {"uuid": gid("finalconsultation", r["consultationid"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            "presenting_complaints": text, "history_of_present_illness": text, **hist,
            "examination": eye or None, "diagnosis": dx or None, "primary_diagnosis": clean(r.get("diagnosis")),
            "primary_diagnosis_code": clean(r.get("code")), "remarks": remarks or None,
            "doctor_name": clean(r.get("consultingdoctor")), "status": "final", "created_at": when, "updated_at": when}


def history_note_payload(h: dict, visit_id: int, patient_id: int) -> dict:
    """A history form with no consultation note on its visit → a note of its own."""
    hist = history_fields(h)
    when = stamp(h.get("created_at")) or stamp(h.get("updated_at"))
    return {"uuid": gid("patient_medical_history", h["id"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            "presenting_complaints": hist.get("chief_complaints"), **hist, "status": "final",
            "created_at": when, "updated_at": stamp(h.get("updated_at")) or when}


def report_note_payload(r: dict, visit_id: int, patient_id: int) -> dict:
    when = stamp(r.get("timestamp")) or stamp(r.get("reportdate"))
    text = strip_html(r.get("report"))
    return {"uuid": gid("medical_reports", r["id"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            "presenting_complaints": f"Medical report ({day(r.get('reportdate')) or ''})".strip(), "remarks": text,
            "history_of_present_illness": text, "doctor_name": clean(r.get("doctorname")), "status": "final",
            "created_at": when, "updated_at": when}


def sick_leave_note_payload(r: dict, visit_id: int, patient_id: int) -> dict:
    days_ = int(num(r.get("nodays1")) or 0)
    text = text_join(("Sick leave", f"{day(r.get('fromdate')) or clean(r.get('fromdate'))} to {day(r.get('dateto')) or clean(r.get('dateto'))}"
                                    + (f" ({days_} days)" if days_ else "")),
                     ("Type", r.get("sicktype")), ("Work", r.get("work")),
                     ("Light duty", " to ".join(x for x in (clean(r.get("lightdutyfrom")), clean(r.get("lightdutyto"))) if x)),
                     ("Light duty days", r.get("nodays2")), ("Review date", r.get("reviewdate")),
                     ("Remarks", strip_html(r.get("remarks"))), ("Certificate", r.get("sickleavcode")))
    when = stamp(r.get("visitdate")) or stamp(r.get("fromdate"))
    return {"uuid": gid("sickleave", r["id"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            "presenting_complaints": "Sick leave certificate", "next_steps": text, "remarks": text,
            "doctor_name": clean(r.get("user")), "status": "final", "created_at": when, "updated_at": when}


def theatre_note_payload(r: dict, visit_id: int, patient_id: int) -> dict:
    text = text_join(("Procedure", r.get("procedure_name")), ("Procedure type", r.get("procedure_type")),
                     ("Category", r.get("category")), ("Theatre", r.get("theatre")), ("Surgery date", r.get("surgery_date")),
                     ("Estimated time", r.get("estimated_time")), ("Surgeon", r.get("surgeon_1")),
                     ("Second surgeon", r.get("surgeon_2")), ("Assistant", r.get("assistant_surgeon")),
                     ("Anaesthetist", r.get("anaesthesia_name")), ("Anaesthesia", r.get("anaesthesia_type")),
                     ("Side", r.get("side")), ("Anaesthetist note", strip_html(r.get("anaesthetist_note"))),
                     ("Doctor note", strip_html(r.get("doctor_note"))), ("Approval", r.get("approval")))
    when = stamp(r.get("surgery_date"))
    return {"uuid": gid("theatre_booking", r["id"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            "presenting_complaints": "Theatre booking", "next_steps": text, "remarks": text,
            "doctor_name": clean(r.get("surgeon_1")), "status": "final", "created_at": when, "updated_at": when}


def diagnosis_payload(r: dict, visit_id: int, patient_id: int, primary: bool, user: int | None) -> dict | None:
    name = clean(r.get("diagnosis"))
    if not name:
        return None
    when = stamp(r.get("timestamp")) or stamp(r.get("consultingdate"))
    return {"uuid": gid("diagnosis", r["consultationid"]), "visit_id": visit_id, "patient_id": patient_id,
            "diagnosis_text": f"{name} [{clean(r.get('code'))}]" if clean(r.get("code")) else name,
            "diagnosis_date": (when or "")[:10] or None, "diagnosis_type": "final", "is_primary": 1 if primary else 0,
            "is_provisional": 0, "status": "confirmed", "recorded_by": user,
            "notes": text_join(("ICD-10", r.get("code")), ("Clinical notes", strip_html(r.get("consultation"))),
                               ("Doctor", r.get("consultingdoctor"))),
            "created_at": when, "updated_at": when}


def procedure_payload(kind: str, code: str, name: str, category: int, active: bool = True, description=None) -> dict:
    return {"uuid": gid(f"procedure-{kind}", code), "code": f"ENH-{code}"[:100], "name": (clean(name) or code)[:255],
            "category": category, "active": 1 if active else 0, "description": clean(description)}


def price_payload(kind: str, code: str, procedure_id: int, facility_id: int, facility_name, cash, insurance) -> dict:
    c = num(cash) or 0.0
    i = num(insurance) or c
    return {"uuid": gid(f"price-{kind}", f"{code}|{facility_id}"), "evaluation_procedure_id": procedure_id, "facility_id": facility_id,
            "facility_name": facility_name, "cash_charge": c, "insurance_charge": i, "charge_insurance": 0,
            "precharge": 0, "status": 1}


def investigation_payload(uid: str, visit_id: int, patient_id: int, *, type_: str, procedure, price, amount, quantity=1,
                          when=None, result=None, comments=None, admission_id=None, cancelled=False) -> dict:
    p = num(price) or 0.0
    q = num(quantity) or 1
    a = num(amount)
    return {"uuid": uid, "visit": visit_id, "patient_id": patient_id, "type": type_, "procedure": procedure,
            "quantity": int(q) if q >= 1 else 1, "price": p, "amount": a if a is not None else round(p * q, 2),
            "discount": 0, "ordered": 1, "admission_id": admission_id, "cancelled": 1 if cancelled else 0,
            "result": {"text": result} if result else None, "comments": clean(comments),
            "created_at": when, "updated_at": when}


def lab_line(r: dict) -> str:
    """One parameter: 'Haemoglobin: 12.1 g/dl (11.5 - 16.5)'."""
    lo, hi = num(r.get("low")), num(r.get("high"))
    rng = f" ({clean(r.get('low'))} - {clean(r.get('high'))})" if (lo or hi) else ""
    unit = f" {clean(r.get('units'))}" if clean(r.get("units")) else ""
    name = clean(r.get("parameter")) or clean(r.get("testname")) or "Result"
    return f"{name}: {strip_html(r.get('value')) or '-'}{unit}{rng}"


def lab_flag(r: dict) -> str | None:
    v, lo, hi = num(r.get("value")), num(r.get("low")), num(r.get("high"))
    if v is None or not (lo or hi) or not re.match(r"^\s*-?\d+(\.\d+)?\s*$", clean(r.get("value")) or ""):
        return None
    return "L" if lo is not None and v < lo else "H" if hi and v > hi else None


def product_payload(r: dict, category: int, unit: int) -> dict:
    cash = num(r.get("rateperunit")) or num(r.get("cashfixedrate"))
    ins = num(r.get("insurance_rate")) or num(r.get("ffsfixedrate")) or cash
    cost = num(r.get("costprice")) or num(r.get("purchaseprice"))
    return {"uuid": gid("pharmacy_item", r["itemcode"]), "generic_name": (clean(r.get("itemname")) or r["itemcode"])[:255],
            "code": f"ENH-{clean(r['itemcode'])}"[:100], "item_no": clean(r["itemcode"]),
            "description": text_join(("Generic", r.get("addgeneric")), ("Strength", r.get("medicinestrength")),
                                     ("Description", r.get("description"))),
            "strength": clean(r.get("medicinestrength")), "category_id": category, "unit_id": unit,
            "reorder_level": int(num(r.get("rol")) or num(r.get("minimumstock")) or 0) or None,
            "selling_price": cash, "insurance_price": ins, "cost_price": cost, "trade_price": cost,
            "is_active": 0 if words(r.get("status")) == "deleted" else 1, "requires_prescription": 1,
            "track_batch": 1, "track_expiry": 1, "created_at": stamp(r.get("timestamp")), "updated_at": stamp(r.get("timestamp"))}


FREQ = {"od": "once a day (OD)", "bd": "twice a day (BD)", "tds": "three times a day (TDS)", "tid": "three times a day (TDS)",
        "qid": "four times a day (QID)", "stat": "immediately (STAT)", "nocte": "at night (NOCTE)", "prn": "when required (PRN)"}


def prescription_payload(r: dict, visit_id: int, drug: int | None, store: tuple | None, admission_id: int | None,
                         user: int | None) -> dict:
    q = num(r.get("quantity")) or 0
    issued = words(r.get("issued")) == "yes" or bool(clean(r.get("medicinedispensedby"))) or words(r.get("stockdeducted")) == "yes"
    refunded = num(r.get("refundedqty")) or 0
    freq_code = words(r.get("freq"))
    freq = clean(r.get("freqname")) or FREQ.get(freq_code) or clean(r.get("freq"))
    dose = " ".join(x for x in (clean(r.get("dose")), clean(r.get("dosemeasur"))) if x) or None
    when = stamp(r.get("timeupdated")) or stamp(r.get("date"))
    notes = text_join(("Instructions", r.get("instructions")), ("Remarks", strip_html(r.get("remarks"))),
                      ("Request", r.get("pharmrequest")), ("Bill", r.get("transactioncode")),
                      ("Payment", r.get("paymentstatus")), ("Approved by", r.get("medicineapprovedby")),
                      ("Dispensed by", r.get("medicinedispensedby")), ("Given", r.get("given")),
                      ("Administered by", r.get("administeredby")), ("Refunded qty", refunded or None))
    return {"uuid": gid("pharmbills", r["pharmbillid"]), "visit": visit_id, "drug": drug,
            "drug_name": (clean(r.get("itemname")) or clean(r.get("medicinebrandname")) or clean(r.get("pharmname")) or "")[:255] or None,
            "take": dose, "dosage": dose, "frequency": freq, "whereto": clean(r.get("route")), "method": clean(r.get("route")),
            "duration": int(num(r.get("days"))) if num(r.get("days")) else None, "time_measure": "days",
            "quantity": q, "price": num(r.get("rate")) or num(r.get("pharmrate")), "notes": notes,
            "dispensed": 1 if issued else 0, "dispensed_quantity": q if issued else None,
            "return_quantity": refunded or None, "status": "active", "paid": 1 if words(r.get("paymentstatus")) == "paid" else 0,
            "invoiced": 1 if "invoice" in words(r.get("paymentstatus")) else 0,
            "for_discharge": 1 if words(r.get("dischargemedication")) in ("yes", "for discharge") else 0,
            "store_id": store[0] if store else None, "store_name": store[1] if store else None,
            "admission_id": admission_id, "user": user, "prescribed_by": user,
            "created_at": when, "updated_at": stamp(r.get("timeddispensed")) or when}


def pay_mode(v) -> str:
    m = words(v)
    if "mpesa" in m.replace("-", "") or "m pesa" in m:
        return "mpesa"
    if m.startswith("cash"):
        return "cash"
    if any(k in m for k in ("card", "visa", "pdq")):
        return "card"
    if "cheque" in m or "check" in m:
        return "cheque"
    if any(k in m for k in ("bank", "eft", "rtgs", "transfer")):
        return "bank_transfer"
    if any(k in m for k in ("insur", "nhif", "sha", "credit")):
        return "insurance"
    return "other"


MODE_COLUMN = {"cash": "cash_amount", "mpesa": "mpesa_amount", "card": "card_amount", "cheque": "cheque_amount"}


def evaluation_payment_payload(uid: str, receipt: str, *, amount, mode_text, when, patient: tuple | None, visit_id,
                               cashier: int | None, cashier_name, reference=None, deposit: bool = False) -> dict | None:
    a = num(amount)
    if a is None or not when:
        return None
    mode = pay_mode(mode_text)
    cols = {c: 0.0 for c in MODE_COLUMN.values()}
    if mode in MODE_COLUMN:
        cols[MODE_COLUMN[mode]] = a
    return {"uuid": uid, "receipt": receipt[:255], "receipt_prefix": "ENH", "patient": patient[0] if patient else None,
            "patient_name": patient[1] if patient else None, "patient_no": patient[2] if patient else None,
            "user": cashier, "user_name": clean(cashier_name), "visit": visit_id, "deposit": 1 if deposit else 0,
            "amount": a, **cols, "total_cash_payment": a, "total_amount_was": a, "discount": 0, "change": 0, "vat_amount": 0,
            "payment_mode": mode, "transaction_id": (clean(reference) or "")[:255] or None, "receipt_date": when[:10],
            "created_at": when, "updated_at": when}


# ── run ─────────────────────────────────────────────────────────────────────
def patient_login(login, target: str, waits=(60, 120, 180, 300, 300, 300)) -> str:
    """V3 sign-in that waits out its throttle (HTTP 422 "Too many failed sign-in attempts from this device")."""
    for wait in (*waits, None):
        try:
            return login()
        except Exception as e:
            if wait is None or "too many" not in str(e).lower():
                raise
            log.warning("%s: V3 is throttling sign-ins — retrying in %ds", target, wait)
            time.sleep(wait)


class Run:
    def __init__(self, target: str, dry_run: bool, limit: int, workers: int, only_missing: bool = False):
        from pipelines_common import use_pipelines_dir
        use_pipelines_dir([target])
        from v3_to_v3_client import V3Client
        import snowflake_to_v3_migration as s2v3
        self.target, self.dry_run, self.limit, self.workers, self.only_missing = target, dry_run, limit, workers, only_missing
        self.c = V3Client(target)
        login = self.c.login
        self.c.login = lambda: patient_login(login, self.target)   # token refreshes wait out a throttle too
        self.c._token = (self.c.login(), time.time())    # kept: token() would otherwise log in a second time
        self.sf = s2v3._snowflake_connect()
        self.today = date.today().isoformat()
        self._cache: dict = {}
        self.failures: dict[str, list] = defaultdict(list)     # model → refused rows (uuid + V3's answer)
        self.only_patients: set[str] | None = None             # --patients: a canary on these Patcom patient codes
        # Patcom runs several branches ('001' Kawangware, '002' Kangemi, '003' Kibra, '004' Mathare); each is its own
        # V3 facility: AFYA_<TARGET>_BRANCH_FACILITIES='001:15,003:14'. Rows of a branch with no facility are skipped.
        raw = os.environ.get(f"AFYA_{target.upper()}_BRANCH_FACILITIES", "")
        self.branch_fac = {b.strip().zfill(3): int(f) for b, f in (x.split(":") for x in raw.split(",") if ":" in x)}
        if not self.branch_fac:      # without it every visit would be skipped as "branch has no facility"
            raise RuntimeError(f"AFYA_{target.upper()}_BRANCH_FACILITIES is not set (.env, or Extra \"branch_facilities\" "
                               f"on connection afya_v3_{target}), e.g. '001:15,002:16,003:17,004:14'")
        self.clients = {}          # facility → its own login (with X-Facility-Id); self.c writes org-wide rows
        for fac in sorted(set(self.branch_fac.values())):
            self.client(fac)
        log.info("branches → facilities: %s", self.branch_fac)

    def client(self, fac: int | None):
        """The gateway client logged into facility `fac` (the row is written in that facility); None → the main one."""
        if fac is None:
            return self.c
        if fac not in self.clients:
            from v3_to_v3_client import V3Client
            # no sign-in of its own: it borrows the main client's token and names its facility in X-Facility-Id.
            # V3 throttles sign-ins ("too many failed sign-in attempts from this device"): a login per facility
            # per task, with the DAG's groups starting together, tripped it on the server.
            c = V3Client(self.target, facility_id=fac)
            c.send_facility_header = True
            c.token = self.c.token
            c.organization_id, c.user_id = self.c.organization_id, self.c.user_id
            # an account offered one facility logs in without choosing, so access to `fac` only shows on first use
            try:
                c.read_page("core", "departments", 1, per_page=1)
            except Exception as e:
                if "login" in str(e).lower() or "sign-in" in str(e).lower():
                    raise RuntimeError(f"{self.target}: V3 refused {c.username}'s login (facility {fac}) — check the "
                                       f"password on connection afya_v3_{self.target} / .env; after failed tries V3 locks "
                                       f"the account for a few minutes ({str(e)[:200]})") from e
                raise RuntimeError(f"{self.target}: {c.username} can't work in facility {fac} — give the account access "
                                   f"to it in V3 ({str(e)[:150]})") from e
            self.clients[fac] = c
        return self.clients[fac]

    def branch_facility(self, code) -> int | None:
        c = clean(code)
        return self.branch_fac.get(c.zfill(3)) if c and c.isdigit() else None

    def read_all(self, svc: str, alias: str):
        """Every row of the organisation. A read may be scoped to the client's facility, so when the main facility's
        read shows no rows from other facilities, the other facilities are read too (rows deduplicated by id)."""
        seen, facs = set(), set()
        for r in self.c.read_all(svc, alias, workers=8):
            seen.add(r.get("id"))
            facs.add(r.get("facility_id"))
            yield r
        if facs - {self.c.facility_id, None}:
            return
        for fac, cl in self.clients.items():
            if cl is self.c:
                continue
            for r in cl.read_all(svc, alias, workers=8):
                if r.get("id") not in seen:
                    seen.add(r.get("id"))
                    yield r

    # ── source ──
    def q(self, sql: str):
        """Rows of a Snowflake query as dicts (lower-case keys), streamed."""
        cur = self.sf.cursor()
        cur.execute(sql)
        cols = [c[0].lower() for c in cur.description]
        while True:
            batch = cur.fetchmany(50_000)
            if not batch:
                return
            for r in batch:
                yield dict(zip(cols, r))

    def table(self, name: str, order: str | None = None, cols: str = "*", where: str = "") -> list[dict]:
        o = f' ORDER BY TRY_TO_NUMBER("{order.upper()}")' if order else ""
        return list(self.q(f'SELECT {cols} FROM {SF}."{name.upper()}" {where}{o}'))

    # ── V3 lookups ──
    def ids_by_uuid(self, svc: str, alias: str, fresh: bool = False) -> dict[str, int]:
        key = ("uuid", svc, alias)
        if fresh or key not in self._cache:
            self._cache[key] = {str(r.get("uuid")).lower(): r["id"] for r in self.read_all(svc, alias) if r.get("uuid")}
        return self._cache[key]

    def visit_map(self) -> dict[str, tuple[int, int]]:
        """Patcom visitcode → (V3 visit id, V3 patient id)."""
        if "visits" not in self._cache:
            vm, fac = {}, {}
            for v in self.read_all("evaluation", "visits"):
                if v.get("uuid"):
                    vm[str(v["uuid"]).lower()] = (v["id"], v.get("patient"))
                    fac[v["id"]] = v.get("facility_id") or v.get("clinic")
            self._cache["visits"], self._cache["visit_fac"] = vm, fac
        return self._cache["visits"]

    def visit_facility(self, visit_id) -> int | None:
        """The facility a V3 visit was made in (everything on the visit is written there too)."""
        self.visit_map()
        return self._cache["visit_fac"].get(visit_id)

    def visit(self, code) -> tuple[int, int] | None:
        c = clean(code)
        if not c:
            return None
        vm = self.visit_map()
        return vm.get(gid("visit", c)) or vm.get(gid("visit", c.rstrip(". ")))   # 'ENH/IP/05536(618)/2025.' in bed bills

    def patients_by_code(self) -> dict[str, int]:
        return self.ids_by_uuid("reception", "patient")

    def patient(self, code) -> int | None:
        return self.patients_by_code().get(gid("patient", clean(code))) if clean(code) else None

    def users_by_name(self) -> dict[str, int]:
        """Patcom stores staff as login names ('elicathe.kemunto', 'dr.roy') or full names ('Dr Roy Wafula') → V3 user."""
        if "users_by_name" not in self._cache:
            ids = self.ids_by_uuid("core", "users")
            out: dict[str, int] = {}
            for u in self.table("umanusers", "usrid", 'USRID, USERNAME, USERFULLNAMES'):
                uid = ids.get(gid("umanusers", u["usrid"]))
                if not uid:
                    continue
                for nm in (u.get("username"), u.get("userfullnames")):
                    if clean(nm):
                        out.setdefault(words(nm), uid)
                        t, f, m, l = person_name(nm)
                        out.setdefault(words(f"{f} {l}"), uid)
            for d in self.table("doctors", "doctorid"):
                k = words(d.get("doctorname"))
                code_user = out.get(words(d.get("doctor_code"))) or out.get(words(d.get("user")))
                if k and k not in out and code_user:
                    out[k] = code_user
            self._cache["users_by_name"] = out
        return self._cache["users_by_name"]

    def user(self, name) -> int | None:
        if not clean(name):
            return None
        by = self.users_by_name()
        k = words(name)
        if k in by:
            return by[k]
        t, f, m, l = person_name(name)
        return by.get(words(f"{f} {l}"))

    def facility_name(self) -> str | None:
        if "facility_name" not in self._cache:
            self._cache["facility_name"] = next((f.get("name") for f in self.c.read_all("core", "facilities")
                                                 if f.get("id") == self.c.facility_id), None)
        return self._cache["facility_name"]

    def branches(self) -> dict[str, str]:
        if "branches" not in self._cache:
            out = {}
            for b in self.table("branches"):
                vals = {k: clean(v) for k, v in b.items()}
                code = next((v for k, v in vals.items() if v and "code" in k), None)
                name = next((v for k, v in vals.items() if v and "name" in k), None)
                if code:
                    out[code] = name or code
                    if code.isdigit():
                        out[str(int(code))] = name or code      # registration rows say '1' for branch '001'
            self._cache["branches"] = out
        return self._cache["branches"]

    # ── posting ──
    def post(self, svc: str, alias: str, payloads) -> Counter:
        """Insert (match_on=uuid → update when it exists). payloads may be a generator: it is consumed in chunks."""
        res = Counter()
        have = set(self.ids_by_uuid(svc, alias, fresh=True)) if self.only_missing else None
        it = iter(payloads)
        sent = 0
        while True:
            chunk = list(islice(it, CHUNK))
            if not chunk:
                break
            if have is not None:
                before = len(chunk)
                chunk = [p for p in chunk if p["uuid"] not in have]
                res["already in V3 (skipped)"] += before - len(chunk)
            if self.limit:
                chunk = chunk[:max(self.limit - sent, 0)]
            if not chunk:
                if self.limit and sent >= self.limit:
                    break
                continue
            sent += len(chunk)
            if self.dry_run:
                for p in chunk[:2]:
                    log.info("  dry-run %s/%s: %s", svc, alias, json.dumps(p, default=str)[:700])
                res["would_post"] += len(chunk)
            else:
                res.update(self._post_chunk(svc, alias, chunk))
            if self.limit and sent >= self.limit:
                break
        return res

    def _post_chunk(self, svc: str, alias: str, payloads: list[dict]) -> Counter:
        res, lock = Counter(), threading.Lock()

        def one(p):
            data = {k: v for k, v in p.items() if v is not None and k != "_fac"}
            fac = p.get("_fac")
            if fac and alias in FACILITY_MODELS:
                data["facility_id"] = fac
            try:
                r = self.client(fac).insert(svc, alias, data, match_on="uuid")
                ok = r.ok and (r.json() or {}).get("success", True)
                err = None if ok else f"{r.status_code} {r.text[:300]}"
            except Exception as e:          # network gone past the client's retries: counted, a re-run fixes it
                ok, err = False, f"{type(e).__name__}: {str(e)[:200]}"
            with lock:
                res["ok" if ok else "failed"] += 1
                if not ok:
                    self.failures[f"{svc}/{alias}"].append({"uuid": p.get("uuid"), "error": err})
                    if res["failed"] <= 5:
                        log.error("  %s/%s %s: %s", svc, alias, p.get("uuid"), err)
                n = sum(res.values())
                if n % 5000 == 0:
                    log.info("  %s/%s: %d / %d (%d failed)", svc, alias, n, len(payloads), res["failed"])
        with ThreadPoolExecutor(self.workers) as ex:
            list(ex.map(one, payloads))
        return res

    def update(self, svc: str, alias: str, items: list[tuple[int, dict]]) -> Counter:
        if self.limit:
            items = items[:self.limit]
        if self.dry_run:
            return Counter(would_update=len(items))
        res, lock = Counter(), threading.Lock()

        def one(item):
            rid, data = item
            try:
                r = self.c.post(svc, {"action": "update", "model": alias, "source_tenant_id": self.c.organization_id,
                                      "destination_tenant_id": self.c.organization_id, "ids": [rid], "data": data}, timeout=120)
                ok = r.ok and not any((x or {}).get("ignored") for x in ((r.json() or {}).get("data") or []))
            except Exception:
                ok = False
            with lock:
                res["ok" if ok else "failed"] += 1
        with ThreadPoolExecutor(self.workers) as ex:
            list(ex.map(one, items))
        return res

    def ref(self, svc: str, alias: str, name: str, extra: dict | None = None, match: str = "name") -> int:
        """A reference row (category, unit, type…) by name: an existing one with the same name is reused (production
        has V3's seeded lists), otherwise one is created with a stable uuid."""
        key = ("ref", svc, alias)
        if key not in self._cache:
            self._cache[key] = list(self.c.read_all(svc, alias))
        rows = self._cache[key]
        want = words(name)
        hit = next((r["id"] for r in rows if words(r.get(match)) == want), None)
        if hit:
            return hit
        uid = gid(f"{alias}-ref", want)
        hit = next((r["id"] for r in rows if str(r.get("uuid")).lower() == uid), None)
        if hit or self.dry_run:
            return hit or 0
        r = self.c.insert(svc, alias, {"uuid": uid, "name": name, **(extra or {})}, match_on="uuid")
        if not r.ok:
            raise RuntimeError(f"{svc}/{alias} '{name}': {r.status_code} {r.text[:300]}")
        new_id = ((r.json() or {}).get("data") or {}).get("id")
        if not new_id:                           # the answer didn't carry the id: read the list again
            self._cache[key] = list(self.c.read_all(svc, alias))
            return next(x["id"] for x in self._cache[key] if str(x.get("uuid")).lower() == uid)
        rows.append({"id": new_id, "uuid": uid, match: name})
        return new_id

    # ── staff ──
    def users(self) -> dict:
        src = self.table("umanusers", "usrid")
        return {"source": len(src), **self.post("core", "users", (user_payload(r) for r in src))}

    def user_profiles(self) -> dict:
        ids, src = self.ids_by_uuid("core", "users", fresh=True), self.table("umanusers", "usrid")
        pl = [user_profile_payload(r, ids[gid("umanusers", r["usrid"])]) for r in src if gid("umanusers", r["usrid"]) in ids]
        return {"source": len(src), "users_in_v3": len(pl), **self.post("core", "user_profile", pl)}

    # ── insurance ──
    def insurer_pairs(self) -> dict[str, set[str]]:
        """insurer name → scheme names, from the insurer list and what patients / visits were registered with."""
        pairs: dict[str, set[str]] = defaultdict(set)
        for r in self.table("insurances", "insuranceid"):
            if clean(r.get("insurancename")) and words(r.get("insurancename")) != "cash":
                pairs[clean(r["insurancename"])]
        for r in self.q(f'SELECT DISTINCT MAINTYPE, SUBTYPE, ACCOUNTNAME FROM {SF}."PATIEN_REGISTRATION"'):
            if is_insured(r.get("maintype"), r.get("subtype")):
                pairs[clean(r["subtype"])].add(clean(r.get("accountname")) or clean(r["subtype"]))
        for r in self.q(f'SELECT DISTINCT PAYMENTTYPENAME, SUBTYPENAME, ACCOUNTNAMENAME FROM {SF}."PATIENT_VISIT"'):
            if is_insured(r.get("paymenttypename"), r.get("subtypename")):
                pairs[clean(r["subtypename"])].add(clean(r.get("accountnamename")) or clean(r["subtypename"]))
        for r in self.table("planmaster", "planid"):
            if is_insured(r.get("maintype"), r.get("subtype")) and clean(r.get("schemes")):
                pairs[clean(r["subtype"])].add(clean(r["schemes"]))
        # one spelling per insurer (Patcom has 'Nhif Insuarnce' and 'NHIF INSUARNCE')
        merged: dict[str, set[str]] = {}
        canon: dict[str, str] = {}
        for name, schemes in pairs.items():
            k = words(name)
            canon.setdefault(k, name)
            merged.setdefault(canon[k], set()).update(s for s in schemes if s)
        return merged

    def insurance(self) -> dict:
        from gyno_to_v3 import core_insurer_match, scheme_type
        pairs = self.insurer_pairs()
        core = {words(r.get("name")): r["id"] for r in self.c.read_all("core", "insurance_companies")}
        why, new = Counter(), {}
        for name in pairs:
            if core_insurer_match(name, core) or core.get(words(name)):
                why["insurer matched to one already in V3"] += 1
            else:
                new[words(name)] = {"uuid": gid("core-insurance-company", words(name)), "name": name[:255],
                                    "code": "ENH-INS-" + gid("core-insurance-company", words(name))[:8].upper(), "status": "active"}
        res = {"companies": dict(self.post("core", "insurance_companies", new.values()))}
        core = {words(r.get("name")): r["id"] for r in self.c.read_all("core", "insurance_companies")}
        by_uuid = self.ids_by_uuid("core", "insurance_companies", fresh=True)
        schemes = {}
        for name, names in pairs.items():
            cid = core_insurer_match(name, core) or core.get(words(name)) or by_uuid.get(gid("core-insurance-company", words(name)))
            for s in (names or {name}):
                k = f"{words(name)}|{words(s)}"
                schemes.setdefault(k, {"uuid": gid("core-insurance-scheme", k), "company_id": cid, "name": s[:255],
                                       "code": "ENH-SCH-" + gid("core-insurance-scheme", k)[:8].upper(), "type": scheme_type(s), "status": "active"})
        res["schemes"] = dict(self.post("core", "insurance_schemes", [s for s in schemes.values() if s["company_id"]]))
        return {"insurers": len(pairs), **why, "created": len(new), **res}

    def insurance_link(self) -> tuple[dict, dict, dict]:
        """(insurer words → core company id, 'insurer|scheme' words → scheme id, company id → name)."""
        from gyno_to_v3 import core_insurer_match
        rows = list(self.c.read_all("core", "insurance_companies"))
        core = {words(r.get("name")): r["id"] for r in rows}
        by_uuid = {str(r.get("uuid")).lower(): r["id"] for r in rows}
        comp = {}
        for name in self.insurer_pairs():
            comp[words(name)] = core_insurer_match(name, core) or core.get(words(name)) or by_uuid.get(gid("core-insurance-company", words(name)))
        sch = {}
        for r in self.c.read_all("core", "insurance_schemes"):
            sch[str(r.get("uuid")).lower()] = r["id"]
        return comp, sch, {r["id"]: r.get("name") for r in rows}

    # ── patients ──
    def wanted(self, patient_code) -> bool:
        return self.only_patients is None or (clean(patient_code) or "") in self.only_patients

    def patient_rows(self):
        """One row per customercode (Patcom re-saves a patient as a new row now and then: the latest wins)."""
        return (r for r in self._patient_rows() if self.wanted(r.get("customercode")))

    def _patient_rows(self):
        return self.q(f'SELECT * FROM {SF}."PATIEN_REGISTRATION" WHERE NULLIF(TRIM(CUSTOMERCODE), \'\') IS NOT NULL '
                      f'QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CUSTOMERCODE) ORDER BY TRY_TO_NUMBER(PATIENTID) DESC) = 1 '
                      f'ORDER BY TRY_TO_NUMBER(PATIENTID)')

    def patients(self) -> dict:
        br = self.branches()
        n = Counter()

        def gen():
            for r in self.patient_rows():
                n["source (distinct patient codes)"] += 1
                yield {**patient_payload(r, br.get(clean(r.get("location")) or "")),
                       "_fac": self.branch_facility(r.get("location"))}
        res = self.post("reception", "patient", gen())
        self._cache.pop(("uuid", "reception", "patient"), None)
        return {**n, **res}

    def patient_insurance(self) -> dict:
        comp, sch, names = self.insurance_link()
        why = Counter()

        def gen():
            for r in self.patient_rows():
                if not is_insured(r.get("maintype"), r.get("subtype")):
                    continue
                pid = self.patient(r["customercode"])
                if not pid:
                    why["patient not in V3"] += 1
                    continue
                ins = clean(r.get("subtype"))
                scheme = clean(r.get("accountname")) or ins
                cid = comp.get(words(ins))
                sid = sch.get(gid("core-insurance-scheme", f"{words(ins)}|{words(scheme)}"))
                why["linked to insurer" if cid else "insurer not in V3"] += 1
                yield {"uuid": gid("patient_insurance", r["customercode"]), "patient_id": pid, "insurance_type": "insurance",
                       "insurance_company_id": cid, "insurance_company_name": names.get(cid) or ins,
                       "insurance_scheme_id": sid, "insurance_scheme_name": scheme, "member_no": clean(r.get("memberno")),
                       "policy_no": clean(r.get("memberno")), "relationship": clean(r.get("membertype")),
                       "expiry_date": day(r.get("accountexpirydate")) or day(r.get("planexpirydate")),
                       "status": "active", "is_primary": 1}
        res = self.post("reception", "patient_insurance", gen())
        return {**why, **res}

    # ── visits ──
    def visits(self) -> dict:
        why = Counter()

        def gen():
            for table, ip in (("PATIENT_VISIT", False), ("PATIENT_IPVISIT", True)):
                for r in self.q(f'SELECT * FROM {SF}."{table}" WHERE NULLIF(TRIM(VISITCODE), \'\') IS NOT NULL '
                                f'QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(VISITCODE) ORDER BY TRY_TO_NUMBER(VISITID) DESC) = 1 '
                                f'ORDER BY TRY_TO_NUMBER(VISITID)'):
                    why[f"{table.lower()} source"] += 1
                    if not self.wanted(r.get("patientcode")):
                        continue
                    pid = self.patient(r.get("patientcode"))
                    if not pid:
                        why[f"{table.lower()}: patient not in Patcom's register (skipped)"] += 1
                        continue
                    fac = self.branch_facility(r.get("location"))
                    if fac is None:
                        why[f"{table.lower()}: branch {clean(r.get('location'))} has no V3 facility (skipped)"] += 1
                        continue
                    doctor = self.user(r.get("consultingdoctor")) or self.user(r.get("admittingdoctor"))
                    yield {**visit_payload(r, pid, inpatient=ip, clinic=fac, doctor=doctor), "_fac": fac}
        res = self.post("evaluation", "visits", gen())
        self._cache.pop("visits", None)
        return {**why, **res}

    def per_visit(self, sql: str, svc: str, alias: str, build, visit_col: str = "opnumber") -> dict:
        why = Counter()

        def gen():
            for r in self.q(sql):
                why["source"] += 1
                v = self.visit(r.get(visit_col))
                if v is None:
                    why["visit not in V3 (orphan in Patcom)"] += 1
                    continue
                p = build(r, *v)
                if p is None:
                    why["nothing to load"] += 1
                    continue
                p["_fac"] = self.visit_facility(v[0])
                yield p
        res = self.post(svc, alias, gen())
        return {**why, **res}

    def vitals(self) -> dict:
        return self.per_visit(f'SELECT * FROM {SF}."TRIAGE" ORDER BY TRY_TO_NUMBER(VITALID)', "evaluation", "vitals",
                              vitals_payload, "opnum")

    def doctor_notes(self) -> dict:
        hist: dict[str, dict] = {}
        for h in self.q(f'SELECT * FROM {SF}."PATIENT_MEDICAL_HISTORY" ORDER BY TRY_TO_NUMBER(ID)'):
            # the latest form per visit; some keys carry browser-translation markup ('<font …>ENH/OP/…</font>')
            h["visit_id"] = strip_html(h.get("visit_id"))
            hist[h["visit_id"] or ""] = h
        noted: set[str] = set()

        def consult(r, vid, pid):
            code = clean(r.get("opnumber")) or ""
            h = hist.get(code) if code not in noted else None
            noted.add(code)
            return consultation_note_payload(r, vid, pid, h)
        out = {"consultations": self.per_visit(f'SELECT * FROM {SF}."FINALCONSULTATION" ORDER BY TRY_TO_NUMBER(CONSULTATIONID)',
                                               "evaluation", "doctor_notes", consult)}
        rest = [h for code, h in hist.items() if code not in noted]
        why, pl = Counter(), []
        for h in rest:
            v = self.visit(h.get("visit_id"))
            if v is None:
                why["visit not in V3"] += 1
                continue
            pl.append(history_note_payload(h, *v))
        out["history_forms_without_consultation"] = {"source": len(rest), **why, **self.post("evaluation", "doctor_notes", pl)}
        out["medical_reports"] = self.per_visit(f'SELECT * FROM {SF}."MEDICAL_REPORTS" ORDER BY TRY_TO_NUMBER(ID)',
                                                "evaluation", "doctor_notes", report_note_payload, "visitnumbermedicalreport")
        out["sick_leave"] = self.per_visit(f'SELECT * FROM {SF}."SICKLEAVE" ORDER BY TRY_TO_NUMBER(ID)',
                                           "evaluation", "doctor_notes", sick_leave_note_payload)
        out["theatre_bookings"] = self.per_visit(f'SELECT * FROM {SF}."THEATRE_BOOKING" ORDER BY TRY_TO_NUMBER(ID)',
                                                 "evaluation", "doctor_notes", theatre_note_payload, "visit_code")
        return out

    def diagnoses(self) -> dict:
        first: set[str] = set()

        def build(r, vid, pid):
            code = clean(r.get("opnumber"))
            primary = code not in first
            first.add(code)
            return diagnosis_payload(r, vid, pid, primary, self.user(r.get("consultingdoctor")))
        return self.per_visit(f'SELECT * FROM {SF}."DIAGNOSIS" ORDER BY TRY_TO_NUMBER(CONSULTATIONID)',
                              "evaluation", "diagnoses", build)

    # ── catalogue ──
    def catalogue_rows(self) -> list[tuple[str, str, str, str, bool]]:
        """(kind, Patcom code, name, V3 category name, active) for every orderable item Patcom bills."""
        out, seen = [], set()

        def add(kind, code, name, cat, active=True):
            code = clean(code)
            if code and (kind, code) not in seen:
                seen.add((kind, code))
                out.append((kind, code, clean(name) or code, cat, active))
        for r in self.table("lab_codes", "labcodid"):
            add("lab", r.get("itemcode"), r.get("itemname"), "Laboratory")
        for r in self.table("lab_rates", "labid"):
            add("lab", r.get("itemcode"), r.get("itemname"), "Laboratory")
        for r in self.table("radiologyservices", "radiologyid"):
            add("rad", r.get("radiologycode"), r.get("radiologyname"), "Radiology")
        for r in self.table("radiologyrate", "radiologyrateid"):
            add("rad", r.get("itemcode"), r.get("itemname"), "Radiology")
        for r in self.table("servicesmaster", "serviceitemid"):
            add("serv", r.get("servicecode"), r.get("servicename"), "Services")
        for r in self.table("service_rates", "servicerateid"):
            add("serv", r.get("pnservicecodeame"), r.get("serviceitem"), "Services")
        # items billed under a code the masters no longer list (renamed / deleted tests) — kept, inactive
        for kind, table, code_col, name_col, cat in (("lab", "LABILLS", "CODE", "LABNAME", "Laboratory"),
                                                     ("rad", "RADBILLS", "CODE", "RADNAME", "Radiology"),
                                                     ("serv", "SERVBILLS", "CODE", "SERVNAME", "Services")):
            for r in self.q(f'SELECT {code_col} AS CODE, MAX({name_col}) AS NAME FROM {SF}."{table}" GROUP BY 1'):
                add(kind, r.get("code"), r.get("name"), cat, active=False)
        return out

    def catalogue(self) -> dict:
        rows = self.catalogue_rows()
        cats = {c: self.ref("evaluation", "procedure_categories", c) for c in sorted({r[3] for r in rows})}
        pl = [procedure_payload(k, code, name, cats[cat], active) for k, code, name, cat, active in rows]
        # a code used by two kinds (lab and service both have 'false') must stay unique per organization
        shared = {c for c, n in Counter(r[1] for r in rows).items() if n > 1}
        for p, (k, code, *_rest) in zip(pl, rows):
            if code in shared:
                p["code"] = f"ENH-{k.upper()}-{code}"[:100]
        return {"procedures": len(pl), "inactive (billed but not in Patcom's masters)": sum(1 for r in rows if not r[4]),
                **self.post("evaluation", "procedures", pl)}

    def price_lists(self) -> dict[tuple[str, str], tuple[float | None, float | None]]:
        """(kind, code) → (cash price, insurance price). Patcom prices per template ('Cash-Lab', 'Lab-Insurance',
        'Lab-Corporate' — insurances.labtemplate says which one an insurer uses): the cash template is the cash price,
        the insurance template the insurance price (else the most common other template)."""
        prices: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
        for kind, sql in (("lab", f'SELECT ITEMCODE AS CODE, TEMPLATENAME AS T, CHARGES AS P FROM {SF}."LAB_RATES"'),
                          ("rad", f'SELECT ITEMCODE AS CODE, TEMPLATE AS T, RATE AS P FROM {SF}."RADIOLOGYRATE"'),
                          ("serv", f'SELECT PNSERVICECODEAME AS CODE, SERVICETEMPLATE AS T, SERVICECHARGE AS P FROM {SF}."SERVICE_RATES"'),
                          ("serv", f'SELECT SERVICECODE AS CODE, \'master\' AS T, CHARGES AS P FROM {SF}."SERVICESMASTER"')):
            for r in self.q(sql):
                if clean(r.get("code")) and num(r.get("p")) is not None:
                    prices[(kind, clean(r["code"]))].setdefault(words(r.get("t")) or "cash", num(r["p"]))
        out = {}
        for k, t in prices.items():
            cash = next((v for n, v in t.items() if "cash" in n), None) or t.get("master")
            others = [v for n, v in t.items() if "cash" not in n and n != "master" and v]
            ins = next((v for n, v in t.items() if "insurance" in n and v), None) \
                or (Counter(others).most_common(1)[0][0] if others else cash)
            out[k] = (cash if cash is not None else ins, ins)
        return out

    def procedure_prices(self) -> dict:
        procs = self.ids_by_uuid("evaluation", "procedures", fresh=True)
        prices = self.price_lists()
        names = {f.get("id"): f.get("name") for f in self.c.read_all("core", "facilities")}
        pl, why = [], Counter()
        for (kind, code), (cash, ins) in prices.items():
            pid = procs.get(gid(f"procedure-{kind}", code))
            if not pid:
                why["procedure not in V3"] += 1
                continue
            for f in sorted(set(self.branch_fac.values())) or [self.c.facility_id]:
                pl.append({**price_payload(kind, code, pid, f, names.get(f), cash, ins), "_fac": f})
        return {**why, **self.post("evaluation", "procedure_prices", pl)}

    # ── investigations ──
    def admission_map(self) -> dict[str, int]:
        """Patcom IP visitcode → V3 admission id."""
        if "admissions" not in self._cache:
            self._cache["admissions"] = {str(a.get("uuid")).lower(): a["id"] for a in self.c.read_all("inpatient", "admission", workers=8)}
        return self._cache["admissions"]

    def admission(self, code) -> int | None:
        c = clean(code)
        return self.admission_map().get(gid("admission", c)) if c and "/IP/" in c.upper() else None

    def lab_text_sql(self) -> str:
        return (f'SELECT TRIM(REQUESTID) AS REQ, TRIM(CODE) AS CODE, '
                f'LISTAGG(COALESCE(PARAMETER, TESTNAME, \'Result\') || \': \' || COALESCE(VALUE, \'-\') '
                f'|| IFF(NULLIF(TRIM(UNITS), \'\') IS NULL, \'\', \' \' || TRIM(UNITS)) '
                f'|| IFF(COALESCE(TRY_TO_DOUBLE(LOW), 0) = 0 AND COALESCE(TRY_TO_DOUBLE(HIGH), 0) = 0, \'\', \' (\' || LOW || \' - \' || HIGH || \')\'), '
                f'\'\\n\') WITHIN GROUP (ORDER BY TRY_TO_NUMBER(RESULTID)) AS TXT, MAX("USER") AS BY_USER '
                f'FROM {SF}."LAB_RESULTS" GROUP BY 1, 2')

    def investigations(self) -> dict:
        procs = self.ids_by_uuid("evaluation", "procedures", fresh=True)
        self.admission_map()
        out = {}
        out["lab"] = self.per_visit(
            f'SELECT B.*, R.TXT AS RESULT_TEXT, R.BY_USER AS RESULT_BY FROM {SF}."LABILLS" B LEFT JOIN ({self.lab_text_sql()}) R '
            f'ON R.REQ = TRIM(B.LABREQUEST) AND R.CODE = TRIM(B.CODE) ORDER BY TRY_TO_NUMBER(B.LABBILLID)',
            "evaluation", "investigations", lambda r, vid, pid: investigation_payload(
                gid("labills", r["labbillid"]), vid, pid, type_="laboratory", procedure=procs.get(gid("procedure-lab", clean(r.get("code")))),
                price=r.get("labrate"), amount=r.get("totallab"), when=stamp(r.get("request_time")) or stamp(r.get("date"), None) or stamp(r.get("timeupdated")),
                result=clean(r.get("result_text")), admission_id=self.admission(r.get("opnumber")),
                cancelled=words(r.get("refundstatus")) == "yes",
                comments=text_join(("Request", r.get("labrequest")), ("Bill", r.get("transactioncode")),
                                   ("Payment", r.get("paymentstatus")), ("Status", r.get("requestresultstatus")),
                                   ("Sample", r.get("sampletype")), ("Results by", r.get("result_by")),
                                   ("Ordered by", r.get("doctoras") or r.get("user")))))
        out["radiology"] = self.per_visit(
            f'SELECT B.*, R.REPORT AS REPORT, R."USER" AS REPORT_BY FROM {SF}."RADBILLS" B LEFT JOIN '
            f'(SELECT * FROM {SF}."RADIOLOGYREPORTS" QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(REQUESTID), TRIM(RADNAME) '
            f'ORDER BY TRY_TO_NUMBER(REPORTID) DESC) = 1) R ON TRIM(R.REQUESTID) = TRIM(B.RADREQUEST) AND TRIM(R.RADNAME) = TRIM(B.RADNAME) '
            f'ORDER BY TRY_TO_NUMBER(B.RADBILLID)',
            "evaluation", "investigations", lambda r, vid, pid: investigation_payload(
                gid("radbills", r["radbillid"]), vid, pid, type_="radiology", procedure=procs.get(gid("procedure-rad", clean(r.get("code")))),
                price=r.get("radrate"), amount=r.get("totalrad"), when=stamp(r.get("date")) or stamp(r.get("timeupdated")),
                result=strip_html(r.get("report")), admission_id=self.admission(r.get("opnumber")),
                cancelled=words(r.get("refundstatus")) == "yes",
                comments=text_join(("Request", r.get("radrequest")), ("Bill", r.get("transactioncode")),
                                   ("Payment", r.get("paymentstatus")), ("Reported by", r.get("report_by")),
                                   ("Ordered by", r.get("doctoras") or r.get("user")))))
        out["services"] = self.per_visit(
            f'SELECT * FROM {SF}."SERVBILLS" ORDER BY TRY_TO_NUMBER(SERVBILLID)',
            "evaluation", "investigations", lambda r, vid, pid: investigation_payload(
                gid("servbills", r["servbillid"]), vid, pid, type_="procedure", procedure=procs.get(gid("procedure-serv", clean(r.get("code")))),
                price=r.get("servrate"), quantity=r.get("serviceqty"),
                amount=r.get("serviceamount") if num(r.get("serviceamount")) is not None else r.get("totalserv"),
                when=stamp(r.get("date")) or stamp(r.get("timeupdated")), admission_id=self.admission(r.get("opnumber")),
                cancelled=words(r.get("refundstatus")) == "yes",
                comments=text_join(("Request", r.get("servrequest")), ("Bill", r.get("transactioncode")),
                                   ("Payment", r.get("paymentstatus")), ("Ordered by", r.get("doctoras") or r.get("user")))))
        return out

    def lab_results(self) -> dict:
        """Lab results the way V3's result screens read them: one investigation_results row per test (published) and one
        investigation_result_details row per parameter."""
        inv = self.ids_by_uuid("evaluation", "investigations", fresh=True)
        bill = {}
        for r in self.q(f'SELECT LABBILLID, TRIM(LABREQUEST) AS REQ, TRIM(CODE) AS CODE, OPNUMBER FROM {SF}."LABILLS"'):
            bill.setdefault((r["req"], r["code"]), (gid("labills", r["labbillid"]), clean(r.get("opnumber"))))
        why = Counter()
        heads = {}
        for r in self.q(f'SELECT TRIM(REQUESTID) AS REQ, TRIM(CODE) AS CODE, MIN(TRY_TO_NUMBER(RESULTID)) AS FIRST_ID, '
                        f'MAX("USER") AS BY_USER, MAX("DATE") AS D, MAX("TIME") AS T, MAX(UPDATEDTIME) AS U, '
                        f'MAX(TESTNAME) AS TEST FROM {SF}."LAB_RESULTS" GROUP BY 1, 2'):
            b = bill.get((r["req"], r["code"]))
            iid = inv.get(b[0]) if b else None
            v = self.visit(b[1]) if b else None
            if not iid or not v:
                why["results: no investigation in V3 (request not billed / visit missing)"] += 1
                continue
            when = stamp(r.get("d"), r.get("t")) or stamp(r.get("u"))
            uid = gid("lab_result", f"{r['req']}|{r['code']}")
            heads[(r["req"], r["code"])] = (uid, v, {
                "uuid": uid, "investigation_id": iid, "patient_id": v[1], "test_parameter": clean(r.get("test")),
                "status": "publish", "result_date": when, "performed_by_name": clean(r.get("by_user")),
                "published_on": when, "approved_on": when, "created_at": when, "updated_at": when,
                "_fac": self.visit_facility(v[0])})
        res = {"results": dict(self.post("evaluation", "investigation_results", (h[2] for h in heads.values())))}
        rid = self.ids_by_uuid("evaluation", "investigation_results", fresh=True)

        def gen():
            for r in self.q(f'SELECT * FROM {SF}."LAB_RESULTS" ORDER BY TRY_TO_NUMBER(RESULTID)'):
                h = heads.get((clean(r.get("requestid")), clean(r.get("code"))))
                if not h or h[0] not in rid:
                    why["details: no result header in V3"] += 1
                    continue
                lo, hi = clean(r.get("low")), clean(r.get("high"))
                when = stamp(r.get("updatedtime"))
                yield {"uuid": gid("lab_results", r["resultid"]), "investigation_result_id": rid[h[0]], "visit_id": h[1][0],
                       "test": (clean(r.get("parameter")) or clean(r.get("testname")) or "Result")[:255],
                       "result": strip_html(r.get("value")), "unit": clean(r.get("units")),
                       "reference_ranges": f"{lo} - {hi}" if (num(lo) or num(hi)) else None, "flag": lab_flag(r),
                       "created_at": when, "updated_at": when, "_fac": self.visit_facility(h[1][0])}
        res["details"] = dict(self.post("evaluation", "investigation_result_details", gen()))
        return {**why, **res}

    # ── inventory ──
    def stores(self) -> dict:
        pl, why = [], Counter()
        for r in self.table("store", "storeid"):
            fac = self.branch_facility(r.get("location"))
            if fac is None:
                why[f"branch {clean(r.get('location'))} has no V3 facility (skipped)"] += 1
                continue
            name = clean(r.get("store_name")) or clean(r.get("store_code"))
            low = words(name)
            kind = ("pharmacy" if "pharm" in low else "laboratory" if "lab" in low else "theater" if "theat" in low
                    else "ward" if "ward" in low else "main" if "main" in low else "department")
            pl.append({"uuid": gid("store", clean(r.get("store_code")) or r["storeid"]), "name": name[:255],
                       "code": f"ENH-{clean(r.get('store_code')) or r['storeid']}"[:50], "type": kind,
                       "is_consumable": 1 if words(r.get("isconsumingstore")) in ("yes", "1", "true") else 0,
                       "location": self.branches().get(clean(r.get("location")) or "", clean(r.get("location"))),
                       "is_active": 1, "_fac": fac})
        return {"source": len(pl), **why, **self.post("inventory", "store", pl)}

    def store_lookup(self) -> dict[str, tuple[int, str]]:
        """store code or name (words) → (V3 store id, name)."""
        if "stores" not in self._cache:
            ids = self.ids_by_uuid("inventory", "store", fresh=True)
            out = {}
            for r in self.table("store", "storeid"):
                sid = ids.get(gid("store", clean(r.get("store_code")) or r["storeid"]))
                if sid:
                    nm = clean(r.get("store_name")) or clean(r.get("store_code"))
                    for k in (r.get("store_code"), r.get("store_name")):
                        if clean(k):
                            out[words(k)] = (sid, nm, self.branch_facility(r.get("location")))
            self._cache["stores"] = out
        return self._cache["stores"]

    def products(self) -> dict:
        src = self.table("pharmacy_item", "pharmacyitem_id")
        src = list({clean(r.get("itemcode")): r for r in src if clean(r.get("itemcode"))}.values())   # latest per code
        cats = {c: self.ref("inventory", "product_category", c, {"is_drug": 1})
                for c in sorted({clean(r.get("categoryname")) or "Uncategorised" for r in src})}
        units = {u: self.ref("inventory", "unit", u) for u in sorted({clean(r.get("unitname_abbreviation")) or "Unit" for r in src})}
        pl = [product_payload(r, cats[clean(r.get("categoryname")) or "Uncategorised"], units[clean(r.get("unitname_abbreviation")) or "Unit"])
              for r in src]
        # items prescribed under a code the item master no longer has: kept (inactive) so prescriptions keep their drug
        known = {clean(r["itemcode"]) for r in src}
        unit = units.get("Unit") or self.ref("inventory", "unit", "Unit")
        cat = cats.get("Uncategorised") or self.ref("inventory", "product_category", "Uncategorised", {"is_drug": 1})
        extra = 0
        for r in self.q(f'SELECT TRIM(CODE) AS ITEMCODE, MAX(ITEMNAME) AS ITEMNAME, MAX(RATE) AS RATEPERUNIT '
                        f'FROM {SF}."PHARMBILLS" WHERE NULLIF(TRIM(CODE), \'\') IS NOT NULL GROUP BY 1'):
            if r["itemcode"] not in known:
                p = product_payload({**r, "status": "Deleted"}, cat, unit)
                pl.append(p)
                extra += 1
        return {"source": len(src), "categories": len(cats), "units": len(units),
                "prescribed codes not in item master (inactive)": extra, **self.post("inventory", "product", pl)}

    def stock(self) -> dict:
        """openingstock is Patcom's batch ledger (one row per store / item / batch, physicalqty = on hand): batches with
        stock on hand → inventory/batch, and one inventory/stock row per batch holding its quantity (V3 sells only
        from batches)."""
        prods, stores = self.ids_by_uuid("inventory", "product", fresh=True), self.store_lookup()
        why, batches, stocks = Counter(), [], []
        for r in self.q(f'SELECT * FROM {SF}."OPENINGSTOCK" WHERE TRY_TO_DOUBLE(PHYSICALQTY) > 0 ORDER BY TRY_TO_NUMBER(OPENINGID)'):
            pid = prods.get(gid("pharmacy_item", clean(r.get("itemcode"))))
            st = stores.get(words(r.get("storecode"))) or stores.get(words(r.get("storename")))
            if not pid or not st:
                why["no product / store in V3"] += 1
                continue
            q = int(num(r.get("physicalqty")) or 0)
            cost = num(r.get("rate")) or 0.0
            sell = num(r.get("sellingrate")) or num(r.get("cashrate"))
            key = r["openingid"]
            batches.append({"uuid": gid("batch", key), "batch_no": (clean(r.get("batch")) or f"ENH-OPEN-{key}")[:100],
                            "product_id": pid, "store_id": st[0], "expiry_date": day(r.get("expiry")), "cost_price": cost,
                            "selling_price": sell, "quantity_received": q, "quantity_available": q, "status": "active",
                            "invoice_no": clean(r.get("mrn")), "notes": f"Patcom stock row {key} ({clean(r.get('itemname'))})"[:500]})
            batches[-1]["_fac"] = st[2]
            stocks.append((key, pid, st[0], q, cost, sell, st[2]))
            why["batches with stock"] += 1
        res = {"batches": dict(self.post("inventory", "batch", batches))}
        bids = self.ids_by_uuid("inventory", "batch", fresh=True)
        res["stock"] = dict(self.post("inventory", "stock", (
            {"uuid": gid("stock", key), "product_id": pid, "store_id": sid, "batch_id": bids.get(gid("batch", key)),
             "quantity": q, "available_quantity": q, "reserved_quantity": 0, "average_cost": cost,
             "total_value": round(q * cost, 2), "selling_price": sell, "_fac": fac}
            for key, pid, sid, q, cost, sell, fac in stocks)))
        return {**why, **res}

    def prescriptions(self) -> dict:
        prods, stores = self.ids_by_uuid("inventory", "product", fresh=True), self.store_lookup()
        self.admission_map()
        return self.per_visit(f'SELECT * FROM {SF}."PHARMBILLS" ORDER BY TRY_TO_NUMBER(PHARMBILLID)', "evaluation", "prescriptions",
                              lambda r, vid, pid: prescription_payload(
                                  r, vid, prods.get(gid("pharmacy_item", clean(r.get("code")) or clean(r.get("medicinecodehidden")))),
                                  stores.get(words(r.get("defaultstore"))), self.admission(r.get("opnumber")),
                                  self.user(r.get("doctoras")) or self.user(r.get("user"))))

    # ── inpatient ──
    def wards(self) -> dict:
        wards = {}
        for r in self.table("addward", "waid"):
            if clean(r.get("wardname")):
                wards.setdefault(words(r["wardname"]), r)
        beds = self.table("bedmaster", "bedid")
        for b in beds:                          # beds in a ward the ward list doesn't have
            if clean(b.get("ward")) and words(b["ward"]) not in wards:
                wards[words(b["ward"])] = {"wardname": b["ward"], "wardactivestatus": "Active"}
        rate = {}
        for b in beds:
            rate.setdefault(words(b.get("ward")), b)
        pl = []
        for k, r in wards.items():
            name = clean(r["wardname"])
            low = words(name)
            b = rate.get(k, {})
            daily = sum(num(b.get(c)) or 0 for c in ("bedcharges", "nursingcharges", "rmocharges", "inh_review"))
            pl.append({"uuid": gid("ward", k), "name": name[:100], "display_name": name[:150], "code": clean(r.get("wardid")),
                       "gender": "female" if "female" in low else "male" if "male" in low else None,
                       "age_group": "children" if any(x in low for x in ("paed", "child", "nbu", "newborn")) else "adult",
                       "cash_cost": daily or None, "insurance_cost": daily or None,
                       "is_active": 0 if words(r.get("wardactivestatus")) == "deleted" else 1,
                       "is_icu": 1 if "icu" in low or "hdu" in low else 0, "is_maternity": 1 if "mater" in low or "labour" in low else 0,
                       "_fac": self.ward_facility(k, r)})
        res = {"wards": dict(self.post("inpatient", "ward", pl))}
        wid = self.ids_by_uuid("inpatient", "ward", fresh=True)
        bt = self.ref("inpatient", "bed_type", "General")
        res["beds"] = dict(self.post("inpatient", "bed", [
            {"uuid": gid("bed", f"{words(b.get('ward'))}|{clean(b.get('bed'))}"), "ward_id": wid.get(gid("ward", words(b.get("ward")))),
             "number": clean(b.get("bed"))[:50], "bed_number": clean(b.get("bed"))[:50], "bed_name": clean(b.get("bed")),
             "bed_type_id": bt, "is_available": 1, "is_active": 0 if words(b.get("bedactivestatus")) == "deleted" else 1,
             "description": text_join(("Bed charges", b.get("bedcharges")), ("Nursing", b.get("nursingcharges")),
                                      ("RMO", b.get("rmocharges")), ("Review", b.get("inh_review")))}
            | {"_fac": self.ward_facility(words(b.get("ward")), wards.get(words(b.get("ward"))) or {})}
            for b in beds if clean(b.get("bed")) and wid.get(gid("ward", words(b.get("ward"))))]))
        return res

    def ward_facility(self, key: str, ward_row: dict) -> int | None:
        """A ward's facility: its own branch code, else the branch most of its admissions came from."""
        fac = self.branch_facility(ward_row.get("location"))
        if fac:
            return fac
        if "ward_branch" not in self._cache:
            counts = defaultdict(Counter)
            for r in self.q(f'SELECT WARD, LOCATION, COUNT(*) AS N FROM {SF}."PATIENT_IPVISIT" GROUP BY 1, 2'):
                counts[words(r.get("ward"))][self.branch_facility(r.get("location"))] += int(r["n"])
            self._cache["ward_branch"] = {k: c.most_common(1)[0][0] for k, c in counts.items()}
        return self._cache["ward_branch"].get(key)

    def ip_visits(self):
        return self.q(f'SELECT * FROM {SF}."PATIENT_IPVISIT" WHERE NULLIF(TRIM(VISITCODE), \'\') IS NOT NULL '
                      f'QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(VISITCODE) ORDER BY TRY_TO_NUMBER(VISITID) DESC) = 1 '
                      f'ORDER BY TRY_TO_NUMBER(VISITID)')

    def admissions(self) -> dict:
        type_id = self.ref("inpatient", "admission_type", "Inpatient Admission", {"code": "ENH-INPATIENT"})
        why = Counter()

        def gen():
            for r in self.ip_visits():
                v = self.visit(r["visitcode"])
                if v is None:
                    why["IP visit not in V3"] += 1
                    continue
                admitted = stamp(r.get("date"), r.get("time")) or stamp(r.get("consultationdate"), r.get("consultationtime")) \
                    or stamp(r.get("updateddate"))
                out = day(r.get("dischargedate"))
                done = bool(out) or words(r.get("dischargestatus")) in ("completed", "discharged")
                why["discharged" if done else "still admitted"] += 1
                doc = self.user(r.get("admittingdoctor")) or self.user(r.get("consultingdoctor")) or self.c.user_id
                yield {"_fac": self.visit_facility(v[0]), "uuid": gid("admission", r["visitcode"]), "admission_number": clean(r["visitcode"]),
                       "inpatient_no": clean(r.get("mrdno")), "patient_id": v[1], "visit_id": v[0],
                       "patient_name": " ".join(x for x in (clean(r.get("patientfirstname")), clean(r.get("patientmiddlename")),
                                                            clean(r.get("patientlastname"))) if x)[:255] or None,
                       "admission_type_id": type_id, "admission_date": admitted, "admission_time": (admitted or "")[11:] or None,
                       "admitting_doctor_id": doc, "admission_notes": strip_html(r.get("admissionnotes")),
                       "reason_for_admission": strip_html(r.get("admissionnotes")), "diagnosis": strip_html(r.get("finaldiagnosis")),
                       "admission_source": "outpatient" if clean(r.get("admitid")) else "direct",
                       "status": "discharged" if done else "admitted",
                       "discharge_date": (stamp(r.get("dischargedate")) or admitted) if done else None,
                       "payment_mode": "cash" if words(r.get("paymenttypename")) in ("cash", "") else "insurance",
                       "insurance_number": clean(r.get("memberno")), "total_charges": num(r.get("totalipbill")),
                       "special_instructions": text_join(("Ward / bed", " / ".join(x for x in (clean(r.get("ward")), clean(r.get("firstbed"))) if x)),
                                                         ("Discharge outcome", r.get("statusfilter")), ("OP visit", r.get("admitid"))),
                       "created_by": doc, "created_at": admitted, "updated_at": stamp(r.get("updateddate")) or admitted}
        res = self.post("inpatient", "admission", gen())
        self._cache.pop("admissions", None)
        return {"admission_type": type_id, **why, **res}

    def ward_stays(self) -> dict:
        """Each admission's bed (Patcom keeps the first bed on the IP visit; bedbills has the daily nights)."""
        adm, wid, bid = self.admission_map(), self.ids_by_uuid("inpatient", "ward", fresh=True), self.ids_by_uuid("inpatient", "bed", fresh=True)
        nights = {clean(r["ipnum"]): (int(r["n"]), r.get("ward"), r.get("bed")) for r in self.q(
            f'SELECT RTRIM(TRIM(IPNUM), \'. \') AS IPNUM, COUNT(*) AS N, MIN(WARD) AS WARD, MIN(BED) AS BED '
            f'FROM {SF}."BEDBILLS" GROUP BY 1') if clean(r.get("ipnum"))}
        why, pl, occupied = Counter(), [], []
        for r in self.ip_visits():
            a = adm.get(gid("admission", clean(r["visitcode"])))
            n = nights.get(clean(r["visitcode"]))
            ward = clean(r.get("ward")) or (n[1] if n else None)
            bed = clean(r.get("firstbed")) or (n[2] if n else None)
            w = wid.get(gid("ward", words(ward))) if ward else None
            if not a or not w:
                why["no admission / ward"] += 1
                continue
            b = bid.get(gid("bed", f"{words(ward)}|{clean(bed)}")) if bed else None
            when = stamp(r.get("date"), r.get("time")) or stamp(r.get("consultationdate"), r.get("consultationtime"))
            out = stamp(r.get("dischargedate"))
            done = bool(out) or words(r.get("dischargestatus")) in ("completed", "discharged")
            who = self.user(r.get("bedallocatedby")) or self.c.user_id
            v = self.visit(r["visitcode"])
            pl.append({"_fac": self.visit_facility(v[0]) if v else None,
                       "uuid": gid("visit_ward", r["visitcode"]), "admission_id": a, "ward_id": w, "bed_id": b,
                       "allocation_date": when, "transfer_in_date": (when or "")[:10] or None, "allocation_type": "admission",
                       "release_date": out if done else None, "discharge_date": out if done else None,
                       "status": "released" if done else "active", "allocated_by": who, "created_by": who,
                       "days_stayed": n[0] if n else None, "created_at": when, "updated_at": out or when})
            if b and not done:
                occupied.append((b, {"is_available": 0}))
        res = {"stays": dict(self.post("inpatient", "visit_ward", pl))}
        res["beds_occupied_now"] = dict(self.update("inpatient", "bed", occupied)) if occupied else {}
        return {**why, **res}

    def discharges(self) -> dict:
        type_id = self.ref("inpatient", "discharge_type", "Patient is Stable", {"code": "PATIENT_IS_STABLE"})
        dead_id = self.ref("inpatient", "discharge_type", "Deceased", {"code": "DECEASED"})
        adm = self.admission_map()
        summary = {clean(r.get("visitnumber")): r for r in self.table("dischargesummary", "id")}
        why, pl = Counter(), []
        for r in self.ip_visits():
            a = adm.get(gid("admission", clean(r["visitcode"])))
            out = stamp(r.get("dischargedate"))
            if not a or not (out or words(r.get("dischargestatus")) in ("completed", "discharged")):
                continue
            s = summary.get(clean(r["visitcode"])) or {}
            out = out or stamp(s.get("dischargedate")) or stamp(r.get("updateddate"))
            dead = words(r.get("statusfilter")) == "dead"
            text = text_join(("Final diagnosis", strip_html(r.get("finaldiagnosis"))), ("History / findings", strip_html(s.get("history_finding"))),
                             ("Investigations", strip_html(s.get("otherlabimage"))), ("Procedures", strip_html(s.get("opprocedure"))),
                             ("Discharged", s.get("discharged")), ("Outcome", r.get("statusfilter"))) or "Discharged (no summary in Patcom)"
            who = self.user(r.get("discharging_doctor")) or self.user(r.get("dischargingdoctor")) or self.user(s.get("doctorname")) or self.c.user_id
            v = self.visit(r["visitcode"])
            pl.append({"_fac": self.visit_facility(v[0]) if v else None,
                       "uuid": gid("discharge", r["visitcode"]), "discharge_number": f"DIS-{clean(r['visitcode'])}"[:100],
                       "admission_id": a, "discharge_type_id": dead_id if dead else type_id, "discharge_date": out,
                       "discharged_by": who, "discharge_diagnosis": (strip_html(s.get("diagnosis")) or strip_html(r.get("finaldiagnosis")) or "See discharge summary")[:2000],
                       "discharge_summary": text, "discharge_instructions": strip_html(s.get("referother")) or text,
                       "medications_on_discharge": strip_html(s.get("rx_medication")),
                       "follow_up_date": day(s.get("returndate")), "follow_up_instructions": text_join(("Return", s.get("returndate")),
                                                                                                      ("Refer to", s.get("referto")), ("Transfer to", s.get("transferto"))),
                       "patient_condition": "deceased" if dead else "stable", "created_at": out, "updated_at": out})
            why["deceased" if dead else "discharged"] += 1
        return {**why, **self.post("inpatient", "discharge", pl)}

    def inpatient_notes(self) -> dict:
        adm = self.admission_map()
        out = {}

        def note(uid, code, when, title, content, kind, by_name):
            a = adm.get(gid("admission", clean(code))) if clean(code) else None
            v = self.visit(code)
            if not (a or v) or not clean(content) or not when:
                return None
            return {"_fac": self.visit_facility(v[0]) if v else None,
                    "uuid": uid, "admission_id": a, "visit_id": v[0] if v else None, "note_type": kind, "note_date": when,
                    "title": title[:200], "note_content": content, "created_by": self.user(by_name) or self.c.user_id,
                    "created_at": when, "updated_at": when}
        why = Counter()

        def gen(rows, build):
            for r in rows:
                p = build(r)
                why["loaded" if p else "no admission / text"] += 1
                if p:
                    yield p
        out["nursing_notes"] = dict(self.post("inpatient", "note", gen(self.q(f'SELECT * FROM {SF}."NURSINGNOTES"'), lambda r: note(
            gid("nursingnotes", r["nurisingnotesid"]), r.get("pvisitnum"), stamp(r.get("updatedtime")),
            f"Nursing note — {clean(r.get('user')) or 'nurse'}", strip_html(r.get("notes")), "nursing_note", r.get("user")))))
        out["cardex"] = dict(self.post("inpatient", "note", gen(self.q(f'SELECT * FROM {SF}."NURSINGCARDEX"'), lambda r: note(
            gid("nursingcardex", r["nursingcardexid"]), r.get("opnumberc"), stamp(r.get("updatedtime")) or stamp(r.get("datec")),
            f"Nursing cardex — {clean(r.get('user')) or 'nurse'}",
            text_join(("Notes", strip_html(r.get("notes"))), ("Diagnosis", strip_html(r.get("diagnosis"))),
                      ("Past medication", strip_html(r.get("pastmed"))), ("Present medication", strip_html(r.get("presentmed"))),
                      ("Family history", strip_html(r.get("familyhistory"))), ("Procedure", r.get("procedure1")),
                      ("Date of procedure", r.get("dop")), ("Baby weight", r.get("weightofbaby")),
                      ("Condition", r.get("condition")), ("Notification", strip_html(r.get("notification")))),
            "nursing_note", r.get("user")))))
        out["doctor_ward_notes"] = dict(self.post("inpatient", "note", gen(self.q(f'SELECT * FROM {SF}."PATIENTDATA"'), lambda r: note(
            gid("patientdata", r["id"]), r.get("ipnumber"), stamp(r.get("dateupdate")), f"Ward round — {clean(r.get('doctor')) or 'doctor'}",
            text_join(("Clinical notes", strip_html(r.get("clinicalnotes"))), ("Examination", strip_html(r.get("examination"))),
                      ("Doctor's orders", strip_html(r.get("doctororder")))), "doctors_note", r.get("doctor")))))
        return {**why, **out}

    # ── money ──
    def patient_card(self) -> dict[int, tuple]:
        if "pcard" not in self._cache:
            self._cache["pcard"] = {p["id"]: (p["id"], " ".join(x for x in (p.get("first_name"), p.get("middle_name"), p.get("last_name")) if x),
                                              p.get("patient_no")) for p in self.c.read_all("reception", "patient", workers=8)}
        return self._cache["pcard"]

    def payment_sources(self):
        """(uuid, receipt no, amount, mode, when, visit code, patient code, cashier, reference, deposit) per Patcom payment."""
        seen = Counter(clean(r["c"]) for r in self.q(f'SELECT TRANSACTIONCODE AS C FROM {SF}."MODESOFPAYMENTS"'))
        k = Counter()
        for r in self.q(f'SELECT * FROM {SF}."MODESOFPAYMENTS" ORDER BY TRY_TO_NUMBER(PAYMENTID)'):
            code = clean(r.get("transactioncode")) or f"ENH-PAY-{r['paymentid']}"
            k[code] += 1
            rc = code if seen.get(code, 0) <= 1 else f"{code}-{k[code]}"
            yield (gid("modesofpayments", r["paymentid"]), rc, r.get("amount"), r.get("paymentmode"),
                   stamp(r.get("transactiondate"), r.get("transactiontime")), r.get("opnumber"), r.get("registrationnum"),
                   r.get("cashier"), clean(r.get("mpesatransaction")) or code, False)
        # Patcom also writes a paid deposit's money into modesofpayments (12,446 of 12,468 share its bill code), and
        # the same for 69 inpatient receipts — only the ones it doesn't are loaded from these tables. Insurance
        # inpatient receipts are the insurer settling an invoice, not cash: they go on the invoice (invoices step).
        for r in self.q(f'SELECT * FROM {SF}."DEPOSIT" WHERE UPPER(TRIM(DEPOSITPAID)) = \'YES\' ORDER BY TRY_TO_NUMBER(DEPOSITID)'):
            if seen.get(clean(r.get("transactioncode"))):
                continue
            yield (gid("deposit", r["depositid"]), clean(r.get("depositcode")) or f"ENH-DEP-{r['depositid']}", r.get("amount"),
                   r.get("mode"), stamp(r.get("datemodified")) or stamp(r.get("timeupdated")), r.get("opnumber") or r.get("opnumbero"),
                   r.get("registrationnu"), r.get("user"), clean(r.get("transactioncode")), True)
        for r in self.q(f'SELECT * FROM {SF}."IPRECEIPTS" WHERE UPPER(TRIM(BILLTYPE)) <> \'INSURANCE\' ORDER BY TRY_TO_NUMBER(RECEIPTID)'):
            if seen.get(clean(r.get("transactioncode"))):
                continue
            yield (gid("ipreceipts", r["receiptid"]), f"{clean(r.get('transactioncode')) or 'ENH-IPR'}-{r['receiptid']}", r.get("amount"),
                   r.get("billtype") or r.get("insurancename"), stamp(r.get("updatedate")), r.get("visitnumber"),
                   r.get("registrationnumber"), r.get("user"), clean(r.get("transactioncode")), False)

    def payments(self) -> dict:
        cards, why = self.patient_card(), Counter()

        def gen():
            for uid, rc, amount, mode, when, vcode, pcode, cashier, ref, dep in self.payment_sources():
                if not self.wanted(pcode):
                    continue
                v = self.visit(vcode)
                pid = (v[1] if v else None) or self.patient(pcode)
                p = evaluation_payment_payload(uid, rc, amount=amount, mode_text=mode, when=when, patient=cards.get(pid),
                                               visit_id=v[0] if v else None, cashier=self.user(cashier) or self.c.user_id,
                                               cashier_name=cashier, reference=ref, deposit=dep)
                if p is None:
                    why["no amount / date"] += 1
                    continue
                why[f"mode {p['payment_mode']}"] += 1
                why["no visit in V3 (kept, patient-level)"] += v is None
                p["_fac"] = self.visit_facility(v[0]) if v else None
                yield p
        return {**why, **self.post("finance", "evaluation_payment", gen())}

    def payment_details(self) -> dict:
        paid = self.ids_by_uuid("finance", "evaluation_payment", fresh=True)

        def gen():
            for uid, rc, amount, mode, when, vcode, pcode, cashier, ref, dep in self.payment_sources():
                pid = paid.get(uid)
                if not pid:
                    continue
                v = self.visit(vcode)
                a = num(amount) or 0.0
                what = "Inpatient deposit" if dep else f"Payment {rc}"
                yield {"_fac": self.visit_facility(v[0]) if v else None,
                       "uuid": gid("payment-detail", uid), "payment": pid, "visit": v[0] if v else None, "price": a, "amount": a,
                       "name": what[:255], "description": " — ".join(x for x in (what, clean(mode), clean(ref)) if x)[:500],
                       "created_at": when, "updated_at": when}
        return self.post("finance", "evaluation_payment_detail", gen())

    def invoices(self) -> dict:
        """invoicebills / ipinvoicebills hold the invoice numbers; the money is on the bills stamped with that code."""
        totals = {clean(r["code"]): num(r["total"]) or 0.0 for r in self.q(
            f'SELECT TRIM(TRANSACTIONCODE) AS CODE, SUM(AMT) AS TOTAL FROM ('
            f'SELECT TRANSACTIONCODE, TRY_TO_DOUBLE(TOTALLAB) AS AMT FROM {SF}."LABILLS" UNION ALL '
            f'SELECT TRANSACTIONCODE, TRY_TO_DOUBLE(TOTALRAD) FROM {SF}."RADBILLS" UNION ALL '
            f'SELECT TRANSACTIONCODE, COALESCE(TRY_TO_DOUBLE(SERVICEAMOUNT), TRY_TO_DOUBLE(TOTALSERV)) FROM {SF}."SERVBILLS" UNION ALL '
            f'SELECT TRANSACTIONCODE, TRY_TO_DOUBLE(AMOUNT) FROM {SF}."PHARMBILLS" UNION ALL '
            f'SELECT TRANSACTIONCODE, TRY_TO_DOUBLE(CONSULTATIONFEES) FROM {SF}."PATIENT_VISIT" UNION ALL '
            f'SELECT TRANSACTIONCODE, COALESCE(TRY_TO_DOUBLE(BEDCHARGES),0) + COALESCE(TRY_TO_DOUBLE(NURSINGCHARGES),0) '
            f'+ COALESCE(TRY_TO_DOUBLE(RMOCHARGES),0) + COALESCE(TRY_TO_DOUBLE(INH_REVIEW),0) FROM {SF}."BEDBILLS") '
            f'WHERE NULLIF(TRIM(TRANSACTIONCODE), \'\') IS NOT NULL GROUP BY 1') if clean(r.get("code"))}
        settled = {clean(r["code"]): num(r["amt"]) or 0.0 for r in self.q(
            f'SELECT TRIM(TRANSACTIONCODE) AS CODE, SUM(TRY_TO_DOUBLE(AMOUNT)) AS AMT FROM {SF}."IPRECEIPTS" '
            f'WHERE UPPER(TRIM(BILLTYPE)) = \'INSURANCE\' GROUP BY 1') if clean(r.get("code"))}
        comp, _, names = self.insurance_link()
        visit_ins = {}
        for t in ("PATIENT_VISIT", "PATIENT_IPVISIT"):
            for r in self.q(f'SELECT TRIM(VISITCODE) AS V, SUBTYPENAME, ACCOUNTNAMENAME, MEMBERNO FROM {SF}."{t}"'):
                visit_ins[clean(r["v"])] = r
        cards, why = self.patient_card(), Counter()

        def gen():
            done: set[str] = set()      # a code in both lists is one invoice (the inpatient one is kept)
            for t in ("IPINVOICEBILLS", "INVOICEBILLS"):
                for r in self.q(f'SELECT * FROM {SF}."{t}" QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(BILLCODE) '
                                f'ORDER BY TRY_TO_NUMBER(BILLID) DESC) = 1 ORDER BY TRY_TO_NUMBER(BILLID)'):
                    code = clean(r.get("billcode"))
                    if not code or code in done or not self.wanted(r.get("patientcode")):
                        if code in done:
                            why["in both invoice lists (one invoice)"] += 1
                        continue
                    done.add(code)
                    v = self.visit(r.get("visitcode"))
                    pid = (v[1] if v else None) or self.patient(r.get("patientcode"))
                    vi = visit_ins.get(clean(r.get("visitcode"))) or {}
                    cid = comp.get(words(vi.get("subtypename")))
                    paid = settled.get(code, 0.0)
                    # not every Patcom charge carries the invoice code: the insurer's settlement is the floor
                    total = max(totals.get(code, 0.0), paid)
                    bal = round(max(total - paid, 0.0), 2)
                    status = "paid" if paid and bal <= 0 else "partially_paid" if paid else "pending"
                    when = stamp(r.get("opdate")) or stamp(r.get("timeupdated"))
                    why[t.lower()] += 1
                    why["no visit in V3"] += v is None
                    card = cards.get(pid) or (None, None, None)
                    yield {"_fac": self.visit_facility(v[0]) if v else None,
                           "uuid": gid("invoice", code), "invoice_no": code[:100], "invoice_date": (when or self.today)[:10],
                           "invoice_type": "inpatient" if t.startswith("IP") else "outpatient", "for_cash": 0,
                           "patient_id": pid, "patient_name": card[1], "patient_no": card[2], "visit_id": v[0] if v else None,
                           "visit": v[0] if v else None, "company_id": cid, "policy_no": clean(vi.get("memberno")),
                           "subtotal": total, "total_amount": total, "amount": total, "paid_amount": paid, "paid": paid,
                           "balance_amount": bal, "status": status, "payment_status": status, "source": "eagle_migration",
                           "notes": text_join(("Insurer", names.get(cid) or vi.get("subtypename")), ("Scheme", vi.get("accountnamename")),
                                              ("Raised by", r.get("user"))),
                           "created_by": self.user(r.get("user")) or self.c.user_id, "created_at": when, "updated_at": when}
        return {**self.post("finance", "invoice", gen()), **why}

    # ── suppliers, appointments ──
    def suppliers(self) -> dict:
        src = self.table("suppliers", "supid")
        # Patcom reuses a supplier code now and then: the first keeps it, later ones are keyed by their own id
        first: dict[str, str] = {}
        for r in src:
            c = clean(r.get("suppliercode")) or str(r["supid"])
            first.setdefault(c, str(r["supid"]))
        key = lambda r: (clean(r.get("suppliercode")) or str(r["supid"])) if first.get(clean(r.get("suppliercode")) or str(r["supid"])) == str(r["supid"]) \
            else f"{clean(r.get('suppliercode'))}-{r['supid']}"
        pl = [{"uuid": gid("suppliers", key(r)), "name": (clean(r.get("suppliername")) or f"Supplier {r['supid']}")[:255],
               "code": f"ENH-{key(r)}"[:50], "email": clean(r.get("emailid1")),
               "phone": phone(r.get("phonenumber1")), "mobile": phone(r.get("mobilenumber")),
               "address": ", ".join(x for x in (clean(r.get("address1")), clean(r.get("address2"))) if x)[:255] or None,
               "town": clean(r.get("city")), "country": clean(r.get("country")), "post_code": clean(r.get("pincode")),
               "tax_id": clean(r.get("tinnumber")), "current_balance": num(r.get("openingbalance")), "is_active": 1}
              for r in src]
        return {"source": len(pl), **self.post("inventory", "supplier", pl)}

    def appointments(self) -> dict:
        pl, why = [], Counter()
        for r in self.table("bookings_tca"):
            vals = {k: clean(v) for k, v in r.items()}
            pcode = next((v for k, v in vals.items() if v and ("reg" in k or "patientcode" in k or "patient_code" in k)), None)
            pid = self.patient(pcode)
            d = next((day(v) for k, v in vals.items() if v and "date" in k and day(v)), None)
            if not pid or not d:
                why["no patient / date"] += 1
                continue
            rid = next(iter(r.values()))
            pl.append({"uuid": gid("bookings_tca", rid), "appointment_no": f"ENH-A-{rid}", "patient_id": pid,
                       "appointment_date": d, "appointment_time": "09:00:00",
                       "notes": "; ".join(f"{k}: {v}" for k, v in vals.items() if v)[:2000]})
        return {**why, **self.post("reception", "appointment", pl)}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--target", default=os.environ.get("EAGLE_TARGET", "eagle"), choices=("eagle", "eagle_prod"))
    ap.add_argument("--steps", nargs="*", default=list(STEPS), choices=STEPS)
    ap.add_argument("--limit", type=int, default=0, help="post at most N rows per step (canary)")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-missing", action="store_true", help="post only rows whose uuid V3 doesn't have yet")
    ap.add_argument("--patients", nargs="*", help="canary: only these Patcom patient codes (and their visits, orders, money)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run = Run(a.target, a.dry_run, a.limit, a.workers, a.only_missing)
    run.only_patients = set(a.patients) if a.patients else None
    report = {"target": a.target, "org": run.c.organization_id, "facility": run.c.facility_id, "dry_run": a.dry_run,
              "limit": a.limit, "started": time.strftime("%Y-%m-%d %H:%M:%S"), "steps": {}}
    out = Path(__file__).resolve().parent / "output" / "eagle_to_v3"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{a.target}-{time.strftime('%Y%m%d_%H%M%S')}.json"
    for s in STEPS:
        if s in a.steps:
            t0 = time.time()
            try:
                report["steps"][s] = getattr(run, s)()
            except Exception as e:
                log.exception("%s failed", s)
                report["steps"][s] = {"error": f"{type(e).__name__}: {str(e)[:300]}"}
            report["steps"][s]["seconds"] = round(time.time() - t0)
            log.info("%s: %s", s, json.dumps(report["steps"][s], default=str))
            report["failures"] = {k: v[:200] for k, v in run.failures.items()}
            path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")   # kept current: long runs
    log.info("report: %s", path)
    sys.exit(1 if any("error" in r for r in report["steps"].values()) else 0)


if __name__ == "__main__":
    main()
