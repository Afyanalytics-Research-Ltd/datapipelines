"""
gyno_to_v3.py — Gyno Solutions HMS (custom PHP / MariaDB) → V3 migrate platform, org 6 / facility 8 (target key
"gyno", AFYA_GYNO_* in .env). Reads the Snowflake mirror HOSPITALS.GYNOSOLUTIONS_HISTORICAL_DATA (see
load_gyno_to_snowflake.py) and posts through the model gateway, in order:

  patients   patients → reception/patient. patient_no "GYN-<source id>" (the source's file_no is empty for 63%
             and repeats, so it is kept in notes, not used as the number); name split first / middle / last;
             '#' stripped from phone numbers; zero dates → empty.
  visits     visits → evaluation/visits, linked to the patient above (9,854 source visits point at no patient:
             counted and skipped). Visits before today → 'completed'; today's / later → 'waiting'; future
             bookings are left for an appointments step.
  vitals     triage → evaluation/vitals (weight / BP / temperature / height / SpO2 / LMP / allergies).
  doctor_notes / discharges / extra_notes   doctorsnotes, discharge, doctorsnotesfornurses,
             inpatientexternalprocedures → evaluation/doctor_notes.   nurse_notes  nursecardex → inpatient/note.
  anc        edd → evaluation/anc_profiles.
  mch        mch_destinations: ANC visits (edd) → 'mch', child (<5y) vaccine visits → 'cwc' visit destinations;
             patient_meta: current pregnancies (lmp/edd/pregnant) + blood group, filling blanks only;
             immunizations: vaccine pharmacy_records / services_records → evaluation/immunization_notes;
             anc_enrollments / fp_services: current pregnancies → ANC awaiting queue, FP items → FP register,
             both through the evaluation REST API (no gateway model; the API skips existing enrollments).
  catalogue / investigations   laboratory, otherservices, otherprocedures → evaluation/procedures (+ categories);
             lab_requests, services_records, otherequests → evaluation/investigations.
  products / prescriptions     pharmacy → inventory/product (+ categories, units); pharmacy_records → prescriptions.
             Drug insurance price = cash × 1.33 (Gyno's rule; it never filled pharmacy.insurancecost) —
             drug_insurance_prices sets it on products / stock rows already in V3.
  payments   payments, inpatientdeposit → finance/payment (method from insurancetype → paymentype).
  appointments  bookings → reception/appointment.     users  users → core/users (random password, reset enforced,
             "<email>.gyno-<id>.invalid" so no mail goes out).
  insurance  insurers in paymentype → finance/insurance_company; preferedpayment → reception/patient_insurance.
  invoices   generalpaymentdetails (one per visit) → finance/invoice.
  documents  externaltests, scanneddocument → reception/patient_document (S3 key + size from the manifest; the
             files themselves follow once a fresh manifest is available).
  stock / procurement   stocks → products; stock levels → inventory/stock + opening-balance movements;
             stock_requests → issues; suppliers / lpos / order_products / deliveries → supplier, purchase order,
             items, goods received (Main Store).
  activity   the tables with no V3 model (specificpaymentdetails — the same money as payments, so not invoices —,
             expenses, LPO payments, vendor invoices, supplier products, daily totals, doctor availability,
             consultants, stock logs, settings, SMS, portal_logs) → reception/activity_log, every field in properties.

Every row carries a stable uuid5 of its source id (the source has no uuids), posted with match_on=uuid, so a
re-run updates instead of duplicating. --limit N posts the first N per step (canary); --dry-run posts nothing.

    python gyno_to_v3.py --steps patients visits vitals --limit 3 --dry-run
    python gyno_to_v3.py --steps patients --limit 3
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
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

import requests

sys.path.insert(0, "/opt/airflow/dags")
log = logging.getLogger("gyno_to_v3")

TARGET = os.environ.get("GYNO_TARGET", "gyno")       # .env key: gyno = migrate org 6, gyno159 = production org 159
SF = "HOSPITALS.GYNOSOLUTIONS_HISTORICAL_DATA"
NS = uuid.UUID("3c6d0b6e-9a1f-4f57-8d2e-6b1f0a9c4e21")      # never change: rows are matched by it
INPUT_DIR = Path(__file__).resolve().parent / "input" / "gyno"   # sheets Gyno fills in (git-ignored *.csv)
STEPS = ("patients", "visits", "vitals", "doctor_notes", "discharges", "nurse_notes", "anc", "catalogue",
         "procedure_prices", "investigations", "products", "prescriptions", "payments", "appointments", "users", "user_profiles", "visit_doctors", "insurance", "invoices",
         "extra_notes", "documents", "stock", "procurement", "destinations", "destinations_reception", "payment_links", "receipts", "admissions", "inpatient_discharges",
         "inpatient_links", "prescription_fix", "close_admissions", "wards", "po_history", "theatre_bookings", "evaluation_payments", "evaluation_payment_details",
         "mch_destinations", "patient_meta", "immunizations", "anc_enrollments", "fp_services", "drug_insurance_prices", "prescription_dose", "insurance_schemes", "procedure_price_facility", "prescription_drug_links", "activity")


# ── pure helpers (tests/test_gyno_to_v3.py) ────────────────────────────────
def gid(table: str, source_id) -> str:
    """Stable uuid of a source row."""
    return str(uuid.uuid5(NS, f"gyno/{table}/{source_id}"))


def clean(v) -> str | None:
    s = "" if v is None else str(v).strip()
    return s or None


def words(v) -> str:
    """Case- and spacing-insensitive form of a name, for matching."""
    return " ".join(str(v or "").lower().split())


def day(v) -> str | None:
    """'YYYY-MM-DD', or None for empty / zero / unparsable dates."""
    s = clean(v)
    if not s or s.startswith("0000"):
        return None
    try:
        return date.fromisoformat(s[:10]).isoformat()
    except ValueError:
        return None


def stamp(d, t=None) -> str | None:
    """'YYYY-MM-DD HH:MM:SS' from a date (+ optional time / datetime text), or None."""
    dd = day(d)
    if not dd:
        return None
    tm = clean(t) or (clean(d)[11:19] if len(clean(d) or "") >= 19 else "")
    m = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?", tm or "")
    return f"{dd} {int(m[1]):02d}:{m[2]}:{m[3] or '00'}" if m else f"{dd} 00:00:00"


def split_name(full) -> tuple[str, str | None, str]:
    parts = (clean(full) or "Unknown").split()
    if len(parts) == 1:
        return parts[0], None, parts[0]
    return parts[0], " ".join(parts[1:-1]) or None, parts[-1]


def phone(v) -> str | None:
    s = re.sub(r"[^\d+]", "", clean(v) or "")
    return s or None


def num(v) -> float | None:
    m = re.search(r"-?\d+(?:\.\d+)?", clean(v) or "")
    return float(m[0]) if m else None


def patient_payload(r: dict) -> dict:
    first, middle, last = split_name(r.get("full_names"))
    notes = "; ".join(x for x in (f"Gyno file no: {clean(r.get('file_no'))}" if clean(r.get("file_no")) else None,
                                  f"Gyno payment type: {clean(r.get('paymenttype'))}" if clean(r.get("paymenttype")) else None,
                                  f"Gyno patient id: {r['id']}") if x)
    email = clean(r.get("email"))
    reg = day(r.get("reg_date"))
    return {"uuid": gid("patients", r["id"]), "patient_no": f"GYN-{r['id']}", "first_name": first,
            "middle_name": middle, "last_name": last, "dob": day(r.get("dateofbirth")),
            "gender": (clean(r.get("gender")) or "").lower() or None, "mobile": phone(r.get("tel")),
            "email": email if email and "@" in email else None, "address": clean(r.get("location")),
            "allergies": clean(r.get("allergy")), "blood_group": clean(r.get("bloodgroup")),
            "registration_date": reg, "status": "active", "notes": notes,
            "created_at": stamp(r.get("reg_date")) or stamp(r.get("updated_on")),
            "updated_at": stamp(r.get("updated_on")) or stamp(r.get("reg_date"))}


def visit_payload(r: dict, patient_id: int, today: str, clinic: int | None = None) -> dict | None:
    """None for a future booking (left for an appointments step). clinic: the facility id, as V3's check-in
    sets it (the frontend requires visit.clinic and prints it)."""
    d = day(r.get("dateofvisit"))
    st = (clean(r.get("status")) or "").lower()
    if st == "booking" and d and d > today:
        return None
    return {"uuid": gid("visits", r["id"]), "patient": patient_id, "clinic": clinic,
            "inpatient": 1 if clean(r.get("is_inpatient")) == "Yes" else 0,
            "purpose": clean(r.get("pstatus")), "payment_mode": "insurance" if clean(r.get("is_insurance")) == "Yes" else "cash",
            "status": "completed" if not d or d < today else "waiting",
            "unique_id": f"GYN-V-{r['id']}", "type_of_visit": clean(r.get("pstatus")),
            "created_at": stamp(r.get("dateofvisit"), r.get("timeofvisit")) or stamp(r.get("updated_on")),
            "updated_at": stamp(r.get("updated_on")) or stamp(r.get("dateofvisit"), r.get("timeofvisit"))}


def vitals_payload(r: dict, visit_id: int, patient_id: int) -> dict:
    bp = clean(r.get("pressure"))
    m = re.match(r"^\s*(\d{2,3})\s*/\s*(\d{2,3})", bp or "")
    return {"uuid": gid("triage", r["id"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            "weight": num(r.get("weight")), "height": num(r.get("height")), "temperature": num(r.get("temprature")),
            "blood_pressure": bp, "systolic_bp": int(m[1]) if m else None, "diastolic_bp": int(m[2]) if m else None,
            "oxygen_saturation": int(num(r.get("po2"))) if num(r.get("po2")) else None, "lmp": day(r.get("lmp")),
            "allergies": clean(r.get("allergy")), "recorded_by_name": clean(r.get("updated_by")),
            "recorded_at": stamp(r.get("triage_date")), "created_at": stamp(r.get("triage_date")),
            "updated_at": stamp(r.get("updated_on")) or stamp(r.get("triage_date"))}


NOTE_FIELDS = ("presenting_complaints",         # what the consultation notes tab + patient history show
               "history_of_present_illness")    # what the note view dialog shows
PAYMENT_TYPES = {"mpesa": "mpesa", "cash": "cash", "visa": "card", "cheque": "cheque", "direct bank deposit": "bank_transfer"}


def text_join(*parts) -> str | None:
    out = "\n".join(f"{label}: {clean(v)}" if label else clean(v) for label, v in parts if clean(v))
    return out or None


def doctor_note_payload(r: dict, visit_id: int, patient_id: int, doctor: tuple | None) -> dict:
    return {"uuid": gid("doctorsnotes", r["id"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            **dict.fromkeys(NOTE_FIELDS, clean(r.get("notes"))), "treatment_plan": clean(r.get("eprescription")),
            "investigations": text_join(("External request", r.get("externalrequest")),
                                        ("External results", r.get("externalresults"))),
            "next_visit_date": day(r.get("nextappointment")), "follow_up_date": day(r.get("nextappointment")),
            "next_steps": text_join(("Surgery", r.get("surgeryprocedure")),
                                    ("Surgery date", day(r.get("surgeryappointment")))),
            "remarks": text_join(("LMP", day(r.get("lmp")) or day(r.get("first_day_of_last_lmp"))),
                                 ("EDD", day(r.get("due_date"))), ("Weeks", r.get("noofweeks")),
                                 ("Nurse procedure", r.get("nurses_procedure")), ("Nurse result", r.get("nurses_result")),
                                 ("Nurse", r.get("nurse_by"))),
            "doctor_name": doctor[0] if doctor else clean(r.get("updated_by")),
            "doctor_specialization": doctor[1] if doctor else None, "status": "final",
            "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


def discharge_payload(r: dict, visit_id: int, patient_id: int) -> dict:
    return {"uuid": gid("discharge", r["id"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            "discharge_instructions": clean(r.get("dischargenote")), "disposition": "home", "status": "final",
            "doctor_name": clean(r.get("updated_by")),
            "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


def nurse_note_payload(r: dict, visit_id: int, user_id: int) -> dict | None:
    if not clean(r.get("cardex")) or not stamp(r.get("updated_on")):
        return None
    return {"uuid": gid("nursecardex", r["id"]), "visit_id": visit_id, "note_date": stamp(r.get("updated_on")),
            "title": f"Nursing cardex — {clean(r.get('updated_by')) or 'nurse'}"[:200], "note_content": clean(r.get("cardex")),
            "created_by": user_id, "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


def anc_payload(r: dict, visit_id: int, patient_id: int) -> dict:
    return {"uuid": gid("edd", r["id"]), "visit_id": visit_id, "patient_id": patient_id,
            "lmp": day(r.get("first_day_of_last_lmp")), "edd": day(r.get("due_date")),
            "gestation": f"{clean(r.get('noofweeks'))} weeks" if clean(r.get("noofweeks")) else None,
            "remarks": f"Recorded by {clean(r.get('updated_by'))}" if clean(r.get("updated_by")) else None,
            "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


def procedure_payload(table: str, prefix: str, r: dict, category: int) -> dict:
    return {"uuid": gid(table, r["id"]), "code": f"GYN-{prefix}-{r['id']}", "name": clean(r.get("title")) or f"{prefix} {r['id']}",
            "category": category, "active": 1, "default_result": clean(r.get("lab_template")),
            "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


FREQUENCIES = [("stat", "immediately (STAT)"), ("od", "once a day (OD)"), ("bd", "twice a day (BD)"),
               ("tds", "three times a day (TDS)"), ("tid", "three times a day (TDS)"), ("qid", "four times a day (QID)"),
               ("nocte", "at night (NOCTE)"), ("prn", "when required (PRN)")]


def rx_dosing(notes) -> tuple[str | None, str | None]:
    """Gyno's dosing is free text in pharmacy_records.notes ('1 od', 'stat', '1 tab bd') → (dosage, frequency)."""
    text = clean(notes)
    if not text:
        return None, None
    words = re.findall(r"[a-z]+", text.lower())
    freq = next((label for key, label in FREQUENCIES if key in words), None)
    return text[:255], freq


# evaluation config/options.php ids: the prescriptions API builds the "Dose" column from take + unit + whereto +
# method + duration + time_measure, labelling these ids (a text `frequency` overrides method, so it is cleared)
RX_METHOD = [(("bd", "bid", "b.d", "b.i.d"), 1), (("tds", "tid", "t.d.s", "t.i.d"), 2), (("qid", "qds"), 3),
             (("stat",), 4), (("od", "o.d", "daily", "once"), 5), (("prn", "p.r.n"), 19), (("nocte", "night"), 23),
             (("mane", "morning"), 25)]
RX_METHOD_PHRASES = [(r"\b(twice|two times|2 ?times|2x)\b", 1), (r"\b(thrice|three times|3 ?times|3x)\b", 2),
                     (r"\b(four times|4 ?times|4x)\b", 3), (r"\bonce (a |per )?(day|daily)\b|\bonce daily\b", 5),
                     (r"\b(at night|bed ?time)\b", 23), (r"\b(when|as) (needed|required)\b", 19)]
RX_ROUTE = {"im": 10, "iv": 11, "pv": 21, "insert": 21, "apply": 20, "sc": 9, "sl": 5}
RX_MEASURE = {"7": 1, "52": 2, "12": 3, "day": 1, "days": 1, "week": 2, "weeks": 2, "month": 3, "months": 3}
NUMBER_WORDS = {"one": "1", "two": "2", "three": "3", "half": "0.5"}


def rx_dose(notes) -> dict:
    """Gyno's free-text dosing ('1 bd for 10/7', '120 mg od 5/7', 'insert pv nocte') → V3's structured dose fields."""
    raw = clean(notes) or ""
    if not raw:
        return {}
    if len([ln for ln in raw.splitlines() if ln.strip()]) > 1:
        # several lines = the whole visit's prescription typed on one drug ('cefasyn 1 bd 5/7 / nuclin v 1 od 3/7'):
        # its first dose may belong to another drug, so it stays notes-only (fields cleared)
        return dict.fromkeys(("frequency", "take", "unit", "method", "whereto", "duration", "time_measure"))
    t = " ".join(raw.lower().replace("*", " ").split())
    out = {"frequency": None}
    words = [w.strip(".") for w in re.findall(r"[a-z.]+", t)]
    m = re.match(r"^[a-z ]*?(\d+(?:\.\d+)?|\bone\b|\btwo\b|\bthree\b|\bhalf\b)\s*(?:(mg|gm|g|mls|ml|mcg|iu)(?![a-z]))?", t)
    if m:
        out["take"] = NUMBER_WORDS.get(m[1], m[1])
        if m[2]:
            out["unit"] = {"gm": "g", "mls": "ml"}.get(m[2], m[2])
    # spelled-out frequencies first: 'twice daily' must not read as 'daily' (O.D)
    method = next((mid for pat, mid in RX_METHOD_PHRASES if re.search(pat, t)), None) or \
        next((mid for keys, mid in RX_METHOD if any(k in words for k in keys)), None)
    if method:
        out["method"] = method
    route = next((rid for k, rid in RX_ROUTE.items() if k in words), None)
    if route:
        out["whereto"] = route
    d = re.search(r"(?<![\d.])(\d+)\s*/\s*(7|52|12)\b", t) or re.search(r"(?<![\d.])(\d+)\s*(days?|weeks?|months?)\b", t)
    if d:
        out["duration"], out["time_measure"] = d[1], RX_MEASURE[d[2]]
    return out


def rx_fix_payload(r: dict, store: tuple[int, str], admission_id: int | None) -> dict:
    """V3 lists prescriptions with status 'active' (dispensed is its own flag); dosing text and the store added."""
    dosage, freq = rx_dosing(r.get("notes"))
    out = {"status": "active", "dosage": dosage, "frequency": freq, "store_id": store[0], "store_name": store[1],
           **rx_dose(r.get("notes"))}
    if admission_id:
        out["admission_id"] = admission_id
    return out


def admission_payload(r: dict, visit_id: int, patient_id: int, *, type_id: int, doctor: int, discharge: str | None,
                      today: str) -> dict | None:
    """An inpatient visit → inpatient/admission: 'discharged' when Gyno has a discharge (or the stay is over),
    'admitted' for recent stays with none."""
    admitted = stamp(r.get("dateofvisit"), r.get("timeofvisit"))
    if not admitted:
        return None
    recent = admitted[:10] >= (date.fromisoformat(today) - timedelta(days=14)).isoformat()
    if discharge or not recent or (clean(r.get("status")) or "").lower() == "completed":
        checkout = stamp(r.get("check_out"))
        out_at = discharge or (checkout if checkout and checkout >= admitted else admitted)
        status, discharged = "discharged", out_at
    else:
        status, discharged = "admitted", None
    return {"uuid": gid("admissions", r["id"]), "admission_number": f"GYN-ADM-{r['id']}", "patient_id": patient_id,
            "visit_id": visit_id, "admission_type_id": type_id, "admission_date": admitted, "admitting_doctor_id": doctor,
            "status": status, "discharge_date": discharged,
            "payment_mode": "insurance" if clean(r.get("is_insurance")) == "Yes" else "cash",
            "created_at": admitted, "updated_at": discharged or admitted}


def discharge_record_payload(r: dict, admission_id: int, type_id: int, by: int) -> dict | None:
    when = stamp(r.get("updated_on"))
    note = clean(r.get("dischargenote")) or "Discharged (no note in Gyno)"
    if not when:
        return None
    return {"uuid": gid("inpatient-discharge", r["id"]), "discharge_number": f"GYN-DIS-{r['id']}", "admission_id": admission_id,
            "discharge_type_id": type_id, "discharge_date": when, "discharged_by": by,
            "discharge_diagnosis": "See discharge summary", "discharge_summary": note, "discharge_instructions": note,
            "created_at": when, "updated_at": when}


def theatre_booking_payload(r: dict, visit_id: int, patient_id: int, *, status: dict[str, int], requested_by: int,
                            today: str) -> dict | None:
    """A Gyno surgery booking (doctorsnotes.surgeryprocedure / surgeryappointment) → theatre/theatre_booking:
    'booked' when the date is today or later, 'completed' when it has passed."""
    proc, when = clean(r.get("surgeryprocedure")), day(r.get("surgeryappointment"))
    if not proc and not when:
        return None
    preferred = when or day(r.get("updated_on"))
    if not preferred:
        return None
    return {"uuid": gid("theatre_booking", r["id"]), "patient_id": patient_id, "visit_id": visit_id,
            "preferred_date": preferred, "status_id": status["booked" if preferred >= today else "completed"],
            "reason": proc, "operation_procedure_name": (proc or "")[:255] or None, "is_emergency": 0,
            "requested_by": requested_by, "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


def procedure_price_payload(table: str, r: dict, procedure_id: int, facility_id: int, facility_name: str | None = None) -> dict:
    """V3 keeps a procedure's price in evaluation/procedure_prices (cash + insurance), not on the procedure; the
    procedures screen's Facility column shows the price row's facility_name."""
    cash = num(r.get("cost")) or 0.0
    ins = num(r.get("insurance_cost") if r.get("insurance_cost") is not None else r.get("insurancecost"))
    return {"uuid": gid(f"{table}-price", r["id"]), "evaluation_procedure_id": procedure_id, "facility_id": facility_id,
            "facility_name": facility_name,
            "cash_charge": cash, "insurance_charge": ins if ins else cash, "charge_insurance": 0, "precharge": 0,
            "status": 1, "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


def investigation_payload(uid: str, visit_id: int, patient_id: int, *, type_: str, procedure, quantity, price,
                          amount, when, updated=None, result=None, instructions=None, comments=None) -> dict:
    p = num(price) or 0.0
    return {"uuid": uid, "visit": visit_id, "patient_id": patient_id, "type": type_, "procedure": procedure,
            "quantity": int(num(quantity) or 1), "price": p, "amount": num(amount) if num(amount) is not None else p,
            "discount": 0, "ordered": 1, "result": {"text": clean(result)} if clean(result) else None,
            "instructions": clean(instructions), "comments": clean(comments),
            "created_at": stamp(when), "updated_at": stamp(updated) or stamp(when)}


def item_name(v, source_id) -> tuple[str, bool]:
    """(name, active): the source has blank / '0' titles on junk rows — named 'Unnamed item <id>', inactive."""
    s = clean(v)
    return (s, True) if s and s not in ("0", "null") else (f"Unnamed item {source_id}", False)


def prices(selling, buying, insurance=None) -> dict:
    """V3 reads a product's own selling / insurance price (and cost / trade price for margins)."""
    sell, buy, ins = num(selling), num(buying), num(insurance)
    return {"selling_price": sell, "insurance_price": ins or sell, "cost_price": buy, "trade_price": buy}


DRUG_INSURANCE_MARKUP = 1.33      # Gyno bills insurers cash × 1.33 for drugs (pharmacy.insurancecost is never filled)


def drug_insurance_price(cash, insurance=None) -> float | None:
    """A drug's insurance price: Gyno's own when set, else cash × the markup, to the whole shilling."""
    return num(insurance) or (float(int(round(num(cash) * DRUG_INSURANCE_MARKUP, 2) + 0.5)) if num(cash) else None)   # half up


def product_payload(r: dict, category: int, unit: int) -> dict:
    name, active = item_name(r.get("title"), r["id"])
    details = clean(r.get("details"))
    return {"uuid": gid("pharmacy", r["id"]), "generic_name": name, "is_active": active,
            "code": f"GYN-DRG-{r['id']}", "item_no": str(r["id"]), "description": None if details == "null" else details,
            "category_id": category, "unit_id": unit,
            **prices(r.get("cost"), r.get("buying_price"), drug_insurance_price(r.get("cost"), r.get("insurancecost"))),
            "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


def prescription_payload(r: dict, visit_id: int, drug: int | None, drug_name: str | None, user_id: int) -> dict:
    dispensed = clean(r.get("status")) == "Yes"
    q = num(r.get("quantity"))
    return {"uuid": gid("pharmacy_records", r["id"]), "visit": visit_id, "drug": drug, "drug_name": drug_name,
            "quantity": q, "price": num(r.get("unitpharmacycost")), "notes": clean(r.get("notes")),
            "dispensed": 1 if dispensed else 0, "dispensed_quantity": q if dispensed else None,
            "status": "dispensed" if dispensed else "pending", "user": user_id, "prescribed_by": user_id,
            "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


def payment_type(name, is_insurance) -> str:
    n = " ".join((clean(name) or "").lower().split())
    for k, v in PAYMENT_TYPES.items():
        if n.startswith(k):
            return v
    if clean(is_insurance) == "Yes" or any(w in n for w in ("sha", "nhif", "shif", "insurance", "assurance")):
        return "insurance"
    return "other"


def payment_payload(r: dict, patient_id: int | None, ptype: tuple | None, user_id: int, *, deposit: bool = False) -> dict | None:
    when = stamp(r.get("date_of_transaction") or r.get("updated_on"))
    if not when or num(r.get("amount")) is None:
        return None
    name = ptype[0] if ptype else None
    prefix = "D" if deposit else "R"
    return {"uuid": gid("inpatientdeposit" if deposit else "payments", r["id"]), "receipt_no": f"GYN-{prefix}-{r['id']}",
            "payment_type": payment_type(name, ptype[1] if ptype else None), "payment_method": clean(name),
            "patient_id": patient_id, "amount": num(r.get("amount")), "payment_date": when[:10], "paid_at": when,
            "reference_no": clean(r.get("transaction_no")) or clean(r.get("invoiceno")),
            "reference": (clean(r.get("descrpt")) or "")[:100] or None, "notes": clean(r.get("descrpt")),
            "status": "completed", "is_deposit": 1 if deposit else 0,
            "processed_by": user_id, "created_by": user_id, "created_at": when, "updated_at": when}


def receipt_payload(r: dict, patient_id: int | None, ptype: tuple | None, user_id: int, *, payment_id: int | None,
                    invoice_id: int | None, deposit: bool = False) -> dict | None:
    """What V3's cashier / payments screens list (finance/receipt — they don't read finance/payment)."""
    when = stamp(r.get("date_of_transaction") or r.get("updated_on"))
    if not when or num(r.get("amount")) is None:
        return None
    name = ptype[0] if ptype else None
    return {"uuid": gid("receipt-" + ("inpatientdeposit" if deposit else "payments"), r["id"]),
            "receipt_no": f"GYN-{'D' if deposit else 'R'}-{r['id']}", "receipt_type": "deposit" if deposit else "payment",
            "payment_id": payment_id, "invoice_id": invoice_id, "patient_id": patient_id, "amount": num(r.get("amount")),
            "receipt_date": when[:10], "payment_method": payment_type(name, ptype[1] if ptype else None),
            "description": text_join(("Method", name), ("Reference", r.get("transaction_no") or r.get("invoiceno"))),
            "notes": clean(r.get("descrpt")), "status": "valid", "issued_by": user_id,
            "created_at": when, "updated_at": when}


MODE_COLUMN = {"cash": "cash_amount", "mpesa": "mpesa_amount", "card": "card_amount", "cheque": "cheque_amount"}


def evaluation_payment_payload(r: dict, *, patient: tuple | None, visit_id: int | None, invoice_id: int | None,
                               ptype: tuple | None, cashier: int, insurer: int | None, deposit: bool = False) -> dict | None:
    """A Gyno payment / inpatient deposit → finance/evaluation_payment, the cashier receipt V3's cash receipts and
    sales-summary screens read (patient = (V3 id, name, patient_no)). The amount sits in its mode's column."""
    when = stamp(r.get("date_of_transaction") or r.get("updated_on"))
    amount = num(r.get("amount"))
    if not when or amount is None:
        return None
    mode = payment_type(ptype[0] if ptype else None, ptype[1] if ptype else None)
    amounts = {col: 0.0 for col in MODE_COLUMN.values()}
    if mode in MODE_COLUMN:
        amounts[MODE_COLUMN[mode]] = amount
    ref = clean(r.get("transaction_no")) or clean(r.get("descrpt"))
    return {"uuid": gid("evaluation-" + ("inpatientdeposit" if deposit else "payments"), r["id"]),
            "receipt": (f"GYN-D-{r['id']}" if deposit else (clean(r.get("invoiceno")) or f"GYN-R-{r['id']}"))[:255],
            "receipt_prefix": "GYN", "patient": patient[0] if patient else None,
            "patient_name": patient[1] if patient else None, "patient_no": patient[2] if patient else None,
            "user": cashier, "user_name": clean(r.get("updated_by")), "visit": visit_id, "invoice_id": invoice_id,
            "company": insurer if mode == "insurance" else None, "amount": amount, **amounts,
            "total_cash_payment": amount, "total_amount_was": amount, "discount": 0, "change": 0, "vat_amount": 0,
            "payment_mode": mode, "transaction_id": (ref or "")[:255] or None, "receipt_date": when[:10],
            "created_at": when, "updated_at": when}


def evaluation_payment_detail_payload(r: dict, payment_id: int, *, visit_id: int | None, invoice_id: int | None,
                                      method: str | None, deposit: bool = False) -> dict:
    amount = num(r.get("amount")) or 0.0
    what = "Inpatient deposit" if deposit else (clean(r.get("descrpt")) or "Payment")
    return {"uuid": gid("evaluation-detail-" + ("inpatientdeposit" if deposit else "payments"), r["id"]),
            "payment": payment_id, "visit": visit_id, "patient_invoice": invoice_id, "price": amount, "amount": amount,
            "name": what[:255], "description": " — ".join(x for x in (what, clean(method)) if x),
            "created_at": stamp(r.get("date_of_transaction") or r.get("updated_on")),
            "updated_at": stamp(r.get("date_of_transaction") or r.get("updated_on"))}


def appointment_payload(r: dict, patient_id: int) -> dict | None:
    d = day(r.get("booking_date"))
    if not d:
        return None
    kind = ", ".join(k for k, f in (("Antenatal", "is_antenatal"), ("Lamaze", "is_lamaze"), ("Paediatric", "is_pedicatric"))
                     if clean(r.get(f)) == "Yes")
    return {"uuid": gid("bookings", r["id"]), "appointment_no": f"GYN-A-{r['id']}", "patient_id": patient_id,
            "appointment_date": d, "appointment_time": "09:00:00", "reason": kind or None,
            "notes": clean(r.get("description")), "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


INSURER_WORDS = ("sha", "nhif", "shif", "insurance", "assurance")
MIME = {"pdf": "application/pdf", "jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "htm": "text/html",
        "pub": "application/x-mspublisher"}


def is_insurer(name, is_insurance) -> bool:
    n = (clean(name) or "").lower()
    return clean(is_insurance) == "Yes" or any(w in n for w in INSURER_WORDS)


def activity_payload(table: str, r: dict, description: str, when, subject: tuple | None = None) -> dict:
    """A source row with no V3 model of its own → reception/activity_log, every field kept in properties."""
    return {"uuid": gid(table, r["id"]), "log_name": f"gyno_{table}", "event": "imported",
            "description": (clean(description) or table)[:2000],
            "subject_type": subject[0] if subject else None, "subject_id": subject[1] if subject else None,
            "properties": {"source_table": table, **{k: (None if v is None else str(v)) for k, v in r.items()}},
            "created_at": stamp(when), "updated_at": stamp(when)}


def user_payload(r: dict) -> dict:
    email = clean(r.get("email"))
    base = email if email and "@" in email else f"{clean(r.get('username')) or 'user'}@gyno.migrated"
    return {"uuid": gid("users", r["id"]), "username": f"gyno.{clean(r.get('username')) or r['id']}",
            "email": f"{base}.gyno-{r['id']}.invalid", "password": str(uuid.uuid4()),
            "enforce_password_reset": 1, "employee_number": f"GYN-U-{r['id']}"}


TITLES = ("dr.", "dr", "prof.", "prof", "mr.", "mr", "mrs.", "mrs", "ms.", "ms", "sr.", "sr")


def person_name(full) -> tuple[str | None, str, str | None, str]:
    """'Dr Salome Waweru' → ('Dr', 'Salome', None, 'Waweru'); also 'Dr.Shiphrah Kuria' (no space)."""
    s = " ".join((clean(full) or "").replace(".", ". ").split())
    parts = s.split()
    title = None
    if parts and parts[0].lower() in TITLES:
        title = parts.pop(0).rstrip(".").capitalize()
    if not parts:
        return title, "Staff", None, "Member"
    if len(parts) == 1:
        return title, parts[0], None, parts[0]
    return title, parts[0], " ".join(parts[1:-1]) or None, parts[-1]


TITLE_CODES = {"Mr": 1, "Mrs": 2, "Ms": 3, "Dr": 4, "Prof": 5}   # core UserProfile::getTitleNameAttribute


def user_profile_payload(r: dict, user_id: int) -> dict:
    """V3 shows a user's name from core/user_profile (title code + first / last name), not from the user row;
    no title in Gyno → none (the column otherwise defaults to 1 = 'Mr.')."""
    title, first, middle, last = person_name(r.get("name") or r.get("username"))
    code = {"title": TITLE_CODES[title]} if title in TITLE_CODES else {}   # NOT NULL: left out → V3's default
    return {"uuid": gid("user_profiles", r["id"]), "user_id": user_id, **code,
            "first_name": first, "middle_name": middle,
            "last_name": last, "job_description": " ".join(x for x in (title, clean(r.get("description"))) if x) or None,
            "staff_number": f"GYN-U-{r['id']}", "phone": phone(r.get("tel")),
            "work_email": clean(r.get("email")) if "@" in (clean(r.get("email")) or "") else None}


def insurance_company_payload(r: dict) -> dict:
    return {"uuid": gid("paymentype", r["id"]), "code": f"GYN-INS-{r['id']}", "name": clean(r.get("paymentype")),
            "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


def patient_insurance_payload(r: dict, patient_id: int, company: tuple | None) -> dict:
    return {"uuid": gid("preferedpayment", r["id"]), "patient_id": patient_id, "insurance_type": "insurance",
            "insurance_company_id": company[0] if company else None, "insurance_company_name": company[1] if company else None,
            "insurance_scheme_name": clean(r.get("scheme")), "member_no": clean(r.get("memberno")),
            "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


# Gyno insurer → an insurer already in V3's core setup (reception's insurance form lists core companies/schemes);
# word match on the Gyno name, first rule wins. Gyno has no schemes: each Gyno insurer becomes one scheme.
CORE_INSURER_RULES = [(("sha", "shif"), "social health"), (("aar",), "aar"), (("jubilee",), "jubilee"),
                      (("cic", "cooperative insurance"), "cic"), (("britam",), "britam"), (("madison",), "madison"),
                      (("resolution",), "resolution"), (("apa",), "apa")]


def core_insurer_match(gyno_name, core_names: dict[str, int]) -> int | None:
    """Core company id for a Gyno insurer name, or None (→ create one)."""
    n = words(gyno_name)
    toks = set(re.findall(r"[a-z]+", n))
    for keys, core_key in CORE_INSURER_RULES:
        if any((k in toks) if " " not in k else (k in n) for k in keys):
            return next((cid for cname, cid in core_names.items() if core_key in cname), None)
    return None


def scheme_type(gyno_name) -> str:
    return "government" if set(re.findall(r"[a-z]+", words(gyno_name))) & {"sha", "shif", "nhif"} else "private"


def invoice_payload(r: dict, visit_id: int | None, patient_id: int | None) -> dict | None:
    d = day(r.get("dateofvisit"))
    if not d:
        return None
    total, paid, bal = num(r.get("totalpayment")) or 0, num(r.get("totalpaid")) or 0, num(r.get("balance")) or 0
    status = "paid" if total and bal <= 0 else ("partially_paid" if paid > 0 else "pending")
    return {"uuid": gid("generalpaymentdetails", r["id"]), "invoice_no": f"GYN-INV-{r['id']}", "invoice_date": d,
            "patient_id": patient_id, "patient_name": clean(r.get("full_names")), "visit_id": visit_id, "visit": visit_id,
            "subtotal": total, "total_amount": total, "amount": total, "discount_amount": num(r.get("discount")) or 0,
            "paid_amount": paid, "paid": paid, "balance_amount": bal, "status": status, "payment_status": status,
            "invoice_type": clean(r.get("typeofpatient")), "source": "gyno_migration",
            # V3's invoices.notes is a TEXT column (64 KB): 46 Gyno visits list more services than fit
            "notes": (text_join(("Services", r.get("serviceoffered")), ("Payments", r.get("payments"))) or "")[:60000] or None,
            "created_at": f"{d} 00:00:00", "updated_at": f"{d} 00:00:00"}


def document_payload(table: str, r: dict, patient_id: int, sizes: dict, *, title_col: str, file_col: str,
                     doc_type: str) -> dict | None:
    key = clean(r.get(file_col))
    if not key:
        return None
    ext = key.rsplit(".", 1)[-1].lower() if "." in key else "bin"
    when = r.get("updated_on")
    return {"uuid": gid(table, r["id"]), "patient_id": patient_id, "title": (clean(r.get(title_col)) or key)[:255],
            "document_type": doc_type, "file_name": key, "file_path": f"gyno-s3/{key}", "file_type": MIME.get(ext, "application/octet-stream"),
            "file_size": int(sizes.get(key) or 0), "file_extension": ext[:10], "document_date": day(when),
            "notes": "Gyno S3 object; file copy pending" if key not in sizes else "Gyno S3 object",
            "created_at": stamp(when), "updated_at": stamp(when)}


def supplier_payload(r: dict) -> dict:
    return {"uuid": gid("suppliers", r["id"]), "name": clean(r.get("supplier_name")) or f"Supplier {r['id']}",
            "code": f"GYN-SUP-{r['id']}", "email": clean(r.get("email")), "phone": phone(r.get("telephone")),
            "address": (clean(r.get("physicaladdress")) or "")[:255] or None, "post_code": None,
            "notes": text_join(("Postal", r.get("postaladdress")), ("Supplies", r.get("typeofproduct"))),
            "created_at": stamp(r.get("mydate")), "updated_at": stamp(r.get("mydate"))}


def po_payload(r: dict, supplier_id: int, store_id: int, user_id: int) -> dict | None:
    d = day(r.get("LPO_date") or r.get("lpo_date")) or day(r.get("updated_on"))
    if not d:
        return None
    return {"uuid": gid("lpos", r["id"]), "po_number": clean(r.get("lpo_no")) or f"GYN-LPO-{r['id']}",
            "supplier_id": supplier_id, "store_id": store_id, "order_date": d, "created_by": user_id,
            "notes": text_join(("Notes", r.get("additional_notes")), ("Status", r.get("lpostatus")),
                               ("Subtotal", r.get("subtotal")), ("VAT", r.get("vat")), ("Grand total", r.get("grandtotal")),
                               ("Paid", r.get("totalpayment")), ("Balance", r.get("balance")),
                               ("1st approval", r.get("1stapproval")), ("2nd approval", r.get("2ndapproval"))),
            "created_at": stamp(r.get("updated_on")) or f"{d} 00:00:00", "updated_at": stamp(r.get("updated_on")) or f"{d} 00:00:00"}


PO_STATUS = {"full delivery": "received", "completed": "received", "partial delivery": "partially_received",
             "approved": "approved", "confirmed": "accepted", "canceled": "cancelled", "cancelled": "cancelled",
             "firstapproval": "pending_approval"}


def po_history_payload(r: dict, paid_by_payments: float = 0.0) -> dict:
    """The amounts and status V3's supplier account reads (it only lists approved / received / … orders):
    paid = the order's own total payment, or the sum of its lpopayments when that is higher."""
    total = num(r.get("grandtotal")) or 0.0
    paid = max(num(r.get("totalpayment")) or 0.0, paid_by_payments)
    pay = "paid" if total and paid >= total else ("partial" if paid > 0 else "unpaid")
    return {"status": PO_STATUS.get(" ".join((clean(r.get("lpostatus")) or "").lower().split()), "approved"),
            "subtotal": num(r.get("subtotal")) or 0.0, "tax_amount": num(r.get("vat")) or 0.0, "total_amount": total,
            "paid_amount": paid, "payment_status": pay}


def po_item_payload(r: dict, po_id: int, product_id: int | None) -> dict:
    q, cost = int(num(r.get("quantity")) or 0), num(r.get("buyingprice")) or 0.0
    return {"uuid": gid("order_products", r["id"]), "purchase_order_id": po_id, "product_id": product_id,
            "quantity_ordered": q, "unit_cost": cost, "line_total": round(q * cost, 2),
            "notes": text_join(("Item", r.get("product_name")), ("Status", r.get("status"))),
            "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


def stock_product_payload(r: dict, category: int, unit: int) -> dict:
    name, active = item_name(r.get("stock_title"), f"S{r['id']}")
    return {"uuid": gid("stocks", r["id"]), "generic_name": name, "is_active": active,
            "code": clean(r.get("stock_code")) and f"GYN-STK-{clean(r.get('stock_code'))}-{r['id']}" or f"GYN-STK-{r['id']}",
            "item_no": f"S{r['id']}", "category_id": category, "unit_id": unit, **prices(r.get("cost"), r.get("cost")),
            "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


def stock_row_payload(uid: str, product_id: int, store_id: int, *, quantity, unit_cost, selling, insurance=None) -> dict:
    """An inventory/stock row with its stock on hand (negative source counts → 0)."""
    q = max(int(num(quantity) or 0), 0)
    cost, sell = num(unit_cost) or 0.0, num(selling)
    return {"uuid": uid, "product_id": product_id, "store_id": store_id, "quantity": q, "available_quantity": q,
            "reserved_quantity": 0, "average_cost": cost, "total_value": round(q * cost, 2),
            "selling_price": sell, "insurance_price": num(insurance) or sell}


def movement_payload(uid: str, product_id: int, store_id: int, user_id: int, *, type_: str, quantity, unit_cost=None,
                     when=None, reason=None, incoming: bool = True) -> dict | None:
    q = int(num(quantity) or 0)
    if q <= 0:
        return None
    return {"uuid": uid, "product_id": product_id, "type": type_, "quantity": q,
            "to_store_id": store_id if incoming else None, "from_store_id": None if incoming else store_id,
            "unit_cost": num(unit_cost), "total_value": round(q * (num(unit_cost) or 0), 2) if num(unit_cost) else None,
            "user_id": user_id, "reason": clean(reason), "idempotency_key": uid,
            "created_at": stamp(when), "updated_at": stamp(when)}


CONSULT_SLUG, WALKIN_SLUG = "doctor", "general-opd"   # org 6's seeded destinations (core service_destinations slugs)


def visit_route(pstatus, *, has_vitals: bool, has_lab: bool, has_ultra: bool, has_rx: bool, inpatient: bool) -> list[str]:
    """The destination slugs a Gyno visit went through, in order."""
    first = WALKIN_SLUG if (clean(pstatus) or "").lower().startswith("walk") else CONSULT_SLUG
    route = (["triage"] if has_vitals else []) + [first]
    route += (["laboratory"] if has_lab else []) + (["ultrasound"] if has_ultra else [])
    route += (["pharmacy"] if has_rx else []) + (["inpatient"] if inpatient else [])
    return route


def destination_payloads(source_visit_id, visit_id: int, when: str | None, done: bool, route: list[str],
                         dests: dict[str, tuple[int, str]]) -> tuple[list[dict], list[dict]]:
    """→ (evaluation/visit_destinations rows, reception/visit_destination rows), one per stop."""
    ev, rc = [], []
    for seq, slug in enumerate(route):
        if slug not in dests:
            continue
        did, name = dests[slug]
        key = f"{source_visit_id}:{slug}"
        ev.append({"uuid": gid("visit-destination-eval", key), "visit_id": visit_id, "destination_id": did,
                   "department": name, "checkout": 1 if done else 0, "status": "completed" if done else "pending",
                   "begin_at": when, "finish_at": when if done else None, "created_at": when, "updated_at": when})
        rc.append({"uuid": gid("visit-destination-reception", key), "visit_id": visit_id, "destination_id": did,
                   "department_name": name, "destination_slug": slug, "sequence": seq,
                   "status": "completed" if done else "pending", "arrived_at": when, "completed_at": when if done else None,
                   "begin_at": when, "finish_at": when if done else None, "checkout": 1 if done else 0,
                   "created_at": when, "updated_at": when})
    return ev, rc


# ── MCH (ANC / CWC / FP) ──
VACCINE_RE = re.compile(r"VACCIN|MEASLES|PRIORIX|PREVENAR|HEXAXIM|CERVARIX|MENFIVE|GARDASIL|ROTARIX|SYNFLORIX|"
                        r"INFANRIX|ENGERIX|HEPATITIS B IMMUNOGLOBULIN")
FP_ITEMS = [   # (pattern on the Gyno item title, FpService methods, removals) — first match wins
    (r"PRIMOLUT", None, None),                          # hormone treatment, not contraception
    (r"IUD SET", None, None),                            # insertion kit, billed beside the device
    (r"(IUD|LIPPES).*REMOVAL", [], ["iucd"]),
    (r"(IMPLANON|JADELLE|IMPLANT).*REMOVAL", [], ["implant"]),
    (r"MIRENA", ["iucd_hormonal"], []),
    (r"IUD|MULTILOAD|COPPER", ["iucd_non_hormonal"], []),
    (r"IMPLANON", ["implant_1_rod"], []),
    (r"JADELLE", ["implant_2_rods"], []),
    (r"\bBTL\b|TUBAL LIG", ["btl"], []),
    (r"SAYANA", ["dmpa_sc"], []),
    (r"DEPO", ["dmpa_im"], []),
    (r"POSTINOR", ["emergency_contraceptive_pill"], []),
    (r"MICROLUT|LEVONORGESTREL 0\.03", ["progestin_only_pills"], []),
    (r"MICROGYNON|FEMIPLAN|YASMIN|LOESTRIN|DIANE", ["combined_oral_pills"], []),
    (r"CONDOM", ["condom_male"], []),
]
LONG_ACTING = {"implant_1_rod", "implant_2_rods", "iucd_hormonal", "iucd_non_hormonal", "btl"}


def fp_item(title) -> tuple[list[str], list[str]] | None:
    """Gyno item title → (FP methods, removals), or None when it isn't family planning."""
    t = words(title).upper()
    for pat, methods, removals in FP_ITEMS:
        if re.search(pat, t):
            return None if methods is None else (methods, removals)
    return None


def fp_service_payload(patient_id: int, visit_id: int, source_visit, when: str, items: list[tuple[list, list]],
                       titles: list[str], *, revisit: bool, reinsertion: bool) -> dict:
    methods = sorted({m for ms, _ in items for m in ms})
    removals = sorted({x for _, rs in items for x in rs})
    long_acting = [m for m in methods if m in LONG_ACTING]
    return {"patient_id": patient_id, "visit_id": visit_id, "service_date": when,
            "client_type": "revisit" if revisit else "new", "methods": methods, "removals": removals,
            "insertion_type": (None if not long_acting else
                               "reinsertion" if reinsertion and "btl" not in long_acting else "first_insertion"),
            "notes": f"Gyno visit {source_visit}: {'; '.join(sorted(set(titles)))}"[:2000]}


def blood_group(v) -> tuple[str | None, str | None]:
    """'O POSITIVE' / 'o+' / 'B -ve' → ('O', '+'); unknown → (None, None)."""
    t = words(v).upper().replace(" ", "")
    m = re.match(r"^(AB|A|B|O)", t)
    if not m:
        return None, None
    rest = t[len(m[1]):]
    rh = "+" if rest.startswith(("+", "POS", "RH+", "RHESUSPOS")) else "-" if rest.startswith(("-", "NEG", "RH-")) else None
    return m[1], rh


def current_pregnancy(r: dict, today: str) -> tuple[str, str] | None:
    """(lmp, edd) of an edd row still due within ~10 months; lmp falls back to edd - 280 days."""
    edd = day(r.get("due_date"))
    if not edd or not today <= edd <= (date.fromisoformat(today) + timedelta(days=300)).isoformat():
        return None
    lmp = day(r.get("first_day_of_last_lmp")) or (date.fromisoformat(edd) - timedelta(days=280)).isoformat()
    return lmp, edd


def immunization_payload(table: str, r: dict, visit_id: int, patient_id: int, name: str, user_id: int) -> dict:
    when = stamp(r.get("updated_on"))
    status = "dispensed" if clean(r.get("status")) == "Yes" else "not marked dispensed"
    return {"uuid": gid(f"immunization-{table}", r["id"]), "visit_id": visit_id, "patient_id": patient_id, "user_id": user_id,
            "vaccine_name": name[:255], "administration_date": day(r.get("updated_on")),
            "notes": f"From Gyno {table} #{r['id']} (qty {clean(r.get('quantity')) or 1}, {status}, by {clean(r.get('updated_by')) or 'unknown'})",
            "created_at": when, "updated_at": when}


# ── runner ─────────────────────────────────────────────────────────────────
class Run:
    def __init__(self, dry_run: bool, limit: int, workers: int, only_missing: bool = False):
        from pipelines_common import use_pipelines_dir
        use_pipelines_dir([TARGET])
        from v3_to_v3_client import V3Client
        import snowflake_to_v3_migration as s2v3
        self.dry_run, self.limit, self.workers, self.only_missing = dry_run, limit, workers, only_missing
        self.c = V3Client(TARGET)
        self.c.login()
        self.sf = s2v3._snowflake_connect()
        self.today = date.today().isoformat()

    def rows(self, table: str) -> list[dict]:
        cur = self.sf.cursor()
        cur.execute(f'SELECT * FROM {SF}."{table.upper()}" ORDER BY TRY_TO_NUMBER("ID")')
        cols = [c[0].lower() for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def ids_by_uuid(self, svc: str, alias: str) -> dict[str, int]:
        return {str(r.get("uuid")).lower(): r["id"] for r in self.c.read_all(svc, alias, workers=8) if r.get("uuid")}

    def post(self, svc: str, alias: str, payloads: list[dict]) -> Counter:
        skipped = 0
        if self.only_missing:                    # re-run after failures: post only what V3 doesn't have yet
            have = set(self.ids_by_uuid(svc, alias))
            before = len(payloads)
            payloads = [p for p in payloads if p["uuid"] not in have]
            skipped = before - len(payloads)
        if self.limit:
            payloads = payloads[:self.limit]
        if self.dry_run:
            for p in payloads[:2]:
                log.info("  dry-run %s/%s: %s", svc, alias, json.dumps(p, default=str)[:600])
            return Counter(would_post=len(payloads))
        res, lock = Counter(), threading.Lock()

        def one(p):
            r = self.c.insert(svc, alias, p, match_on="uuid")
            ok = r.ok and (r.json() or {}).get("success", True)
            with lock:
                res["ok" if ok else "failed"] += 1
                if not ok and res["failed"] <= 5:
                    log.error("  %s/%s %s: %s %s", svc, alias, p.get("uuid"), r.status_code, r.text[:300])
                if sum(res.values()) % 2000 == 0:
                    log.info("  %s/%s: %d / %d (%d failed)", svc, alias, sum(res.values()), len(payloads), res["failed"])
        with ThreadPoolExecutor(self.workers) as ex:
            list(ex.map(one, payloads))
        if skipped:
            res["already in V3 (skipped)"] = skipped
        return res

    def patients(self) -> dict:
        src = self.rows("patients")
        res = self.post("reception", "patient", [patient_payload(r) for r in src])
        return {"source": len(src), **res}

    def visits(self) -> dict:
        pat = self.ids_by_uuid("reception", "patient")
        src, payloads, why = self.rows("visits"), [], Counter()
        for r in src:
            pid = pat.get(gid("patients", r.get("patientsid")))
            if pid is None:
                why["patient not in V3 (source has none, or not loaded)"] += 1
                continue
            p = visit_payload(r, pid, self.today, self.c.facility_id)
            if p is None:
                why["future booking (appointments step)"] += 1
                continue
            payloads.append(p)
        return {"source": len(src), **why, **self.post("evaluation", "visits", payloads)}

    def vitals(self) -> dict:
        vis = {str(v.get("uuid")).lower(): (v["id"], v.get("patient")) for v in self.c.read_all("evaluation", "visits", workers=8)}
        src, payloads, why = self.rows("triage"), [], Counter()
        for r in src:
            v = vis.get(gid("visits", r.get("qid")))
            if v is None:
                why["visit not in V3"] += 1
                continue
            payloads.append(vitals_payload(r, v[0], v[1]))
        return {"source": len(src), **why, **self.post("evaluation", "vitals", payloads)}

    # ── visit-linked rows ──
    def visit_map(self) -> dict[str, tuple[int, int]]:
        """source visit uuid → (V3 visit id, V3 patient id)."""
        if not hasattr(self, "_visits"):
            self._visits = {str(v.get("uuid")).lower(): (v["id"], v.get("patient"))
                            for v in self.c.read_all("evaluation", "visits", workers=8)}
        return self._visits

    def per_visit(self, table: str, visit_col: str, svc: str, alias: str, build) -> dict:
        vis, src, payloads, why = self.visit_map(), self.rows(table), [], Counter()
        for r in src:
            v = vis.get(gid("visits", r.get(visit_col)))
            if v is None:
                why["visit not in V3"] += 1
                continue
            p = build(r, *v)
            if p is None:
                why["nothing to load (empty)"] += 1
                continue
            payloads.append(p)
        return {"source": len(src), **why, **self.post(svc, alias, payloads)}

    def doctor_notes(self) -> dict:
        docs = {str(r["id"]): (clean(r.get("name")), clean(r.get("specialization"))) for r in self.rows("doctors")}
        return self.per_visit("doctorsnotes", "qid", "evaluation", "doctor_notes",
                              lambda r, vid, pid: doctor_note_payload(r, vid, pid, docs.get(str(r.get("doctorid")))))

    def discharges(self) -> dict:
        return self.per_visit("discharge", "qid", "evaluation", "doctor_notes", discharge_payload)

    def nurse_notes(self) -> dict:
        return self.per_visit("nursecardex", "qid", "inpatient", "note",
                              lambda r, vid, pid: nurse_note_payload(r, vid, self.c.user_id))

    def anc(self) -> dict:
        return self.per_visit("edd", "qid", "evaluation", "anc_profiles", anc_payload)

    # ── MCH ──
    def api(self, method: str, path: str, **kw):
        """The evaluation service's own REST API (the MCH enrollment / FP tables have no gateway model)."""
        url = self.c.urls["evaluation"].rstrip("/") + path
        for attempt in range(4):
            try:
                return self.c._session.request(method, url, headers=self.c._headers("evaluation"), timeout=120, **kw)
            except requests.RequestException:
                if attempt == 3:
                    raise
                time.sleep(5 * (attempt + 1))

    def latest_edd(self) -> dict[str, dict]:
        """source patient id → that patient's most recent edd row."""
        out = {}
        for r in self.rows("edd"):
            k = str(r.get("patientid"))
            if k not in out or (stamp(r.get("updated_on")) or "", int(r["id"])) >= (stamp(out[k].get("updated_on")) or "", int(out[k]["id"])):
                out[k] = r
        return out

    def vaccine_rows(self) -> list[tuple[str, dict, str]]:
        """(source table, row, vaccine name) for every vaccine ordered in Gyno."""
        drugs = {str(r["id"]): clean(r.get("title")) for r in self.rows("pharmacy")}
        services = {str(r["id"]): clean(r.get("title")) for r in self.rows("otherservices")}
        out = [("pharmacy_records", r, drugs[str(r.get("pid"))]) for r in self.rows("pharmacy_records")
               if VACCINE_RE.search(words(drugs.get(str(r.get("pid")))).upper())]
        out += [("services_records", r, services[str(r.get("pid"))]) for r in self.rows("services_records")
                if VACCINE_RE.search(words(services.get(str(r.get("pid")))).upper())]
        return out

    def mch_destinations(self) -> dict:
        dests = {d.get("slug"): (d["id"], d.get("name")) for d in self.c.read_all("core", "service_destinations")}
        have_depts = {words(d.get("name")): d["id"] for d in self.c.read_all("core", "departments")}
        names = [dests[s][1] for s in ("mch", "cwc") if s in dests]
        depts = {n: have_depts.get(words(n)) for n in names}
        if any(v is None for v in depts.values()):
            depts.update(self.named("core", "departments", [n for n, v in depts.items() if v is None]))
        visits = {str(r["id"]): r for r in self.rows("visits")}
        dob = {str(r["id"]): day(r.get("dateofbirth")) for r in self.rows("patients")}
        route, why = {}, Counter()
        for r in self.rows("edd"):
            route[str(r.get("qid"))] = "mch"
        for _, r, _ in self.vaccine_rows():
            v = visits.get(str(r.get("qid")))
            born, seen = dob.get(str((v or {}).get("patientsid"))), day((v or {}).get("dateofvisit"))
            if born and seen and born <= seen <= (date.fromisoformat(born) + timedelta(days=5 * 365)).isoformat():
                route.setdefault(str(r.get("qid")), "cwc")
        vis, ev, rc = self.visit_map(), [], []
        for sid, slug in route.items():
            v, src = vis.get(gid("visits", sid)), visits.get(sid)
            if v is None or src is None:
                why["visit not in V3"] += 1
                continue
            why[slug] += 1
            e, c_ = destination_payloads(sid, v[0], stamp(src.get("dateofvisit"), src.get("timeofvisit")), True, [slug], dests)
            ev += e
            rc += [{**x, "department_id": depts.get(x["department_name"])} for x in c_]
        return {**why, "missing_destinations": [s for s in ("mch", "cwc") if s not in dests],
                "evaluation": dict(self.post("evaluation", "visit_destinations", ev)),
                "reception": dict(self.post("reception", "visit_destination", rc))}

    def patient_meta(self) -> dict:
        pat = self.ids_by_uuid("reception", "patient")
        existing = {r.get("patient_id"): r for r in self.c.read_all("evaluation", "patient_meta", workers=8)}
        edd, why = self.latest_edd(), Counter()
        inserts, updates = [], []
        for r in self.rows("patients"):
            pid = pat.get(gid("patients", r["id"]))
            if pid is None:
                continue
            want = {}
            grp, rh = blood_group(r.get("bloodgroup"))
            if grp:      # the Vitals screen's select values are lower case ('a', 'b', 'ab', 'o')
                want.update(blood_group=grp.lower(), **({"rhesus_factor": rh} if rh else {}))
            preg = current_pregnancy(edd[str(r["id"])], self.today) if str(r["id"]) in edd else None
            if preg:
                want.update(lmp=preg[0], edd=preg[1], pregnant=1)
            if not want:
                continue
            have = existing.get(pid)
            if have is not None and str(have.get("uuid")).lower() == gid("patient-meta", r["id"]):   # our own row: keep it current
                fix = {k: v for k, v in want.items() if k in ("blood_group", "rhesus_factor") and have.get(k) != v}
                if fix:
                    updates.append((have["id"], fix))
                    why["ours: corrected"] += 1
                else:
                    why["ours: up to date"] += 1
                continue
            if have is None:
                inserts.append({"uuid": gid("patient-meta", r["id"]), "patient_id": pid, **want})
                why["new: pregnancy" if preg else "new: blood group"] += 1
                continue
            if have.get("lmp") or have.get("edd"):              # staff already recorded this pregnancy in V3
                for k in ("lmp", "edd", "pregnant"):
                    want.pop(k, None)
            want = {k: v for k, v in want.items() if not have.get(k)}
            if want:
                updates.append((have["id"], want))
                why["existing: blanks filled"] += 1
            else:
                why["existing: already filled in V3 (kept)"] += 1
        if self.limit:
            inserts, updates = inserts[:self.limit], updates[:self.limit]
        if self.dry_run:
            log.info("  dry-run patient_meta: %s | %s", inserts[:2], updates[:2])
            return {**why, "would_insert": len(inserts), "would_update": len(updates)}
        return {**why, "insert": dict(self.post("evaluation", "patient_meta", inserts)),
                "update": dict(self.update("evaluation", "patient_meta", updates))}

    def immunizations(self) -> dict:
        vis, src, payloads, why = self.visit_map(), self.vaccine_rows(), [], Counter()
        for table, r, name in src:
            v = vis.get(gid("visits", r.get("qid")))
            if v is None:
                why["visit not in V3"] += 1
                continue
            payloads.append(immunization_payload(table, r, v[0], v[1], name, self.c.user_id))
        return {"source": len(src), **why, **self.post("evaluation", "immunization_notes", payloads)}

    def anc_enrollments(self) -> dict:
        """Mothers still pregnant → the ANC 'Awaiting enrollment' queue (staff confirm the schedule in V3)."""
        pat, vis, why, todo = self.ids_by_uuid("reception", "patient"), self.visit_map(), Counter(), []
        for spid, r in self.latest_edd().items():
            preg = current_pregnancy(r, self.today)
            if not preg:
                why["no current pregnancy (EDD passed)"] += 1
                continue
            pid = pat.get(gid("patients", spid))
            if pid is None:
                why["patient not in V3"] += 1
                continue
            v = vis.get(gid("visits", r.get("qid")))
            todo.append({"patient_id": pid, "visit_id": v[0] if v else None, "lmp": preg[0], "edd": preg[1]})
        if self.limit:
            todo = todo[:self.limit]
        if self.dry_run:
            log.info("  dry-run anc_enrollments: %s", todo[:2])
            return {**why, "would_post": len(todo)}
        for body in todo:
            r = self.api("POST", "/v1/anc-enrollments", json=body)
            why[{201: "added to awaiting queue", 200: "already enrolled / awaiting in V3 (kept)"}.get(r.status_code, "failed")] += 1
            if r.status_code not in (200, 201) and why["failed"] <= 5:
                log.error("  anc-enrollment %s: %s %s", body, r.status_code, r.text[:300])
        return dict(why)

    def fp_services(self) -> dict:
        drugs = {str(r["id"]): clean(r.get("title")) for r in self.rows("pharmacy")}
        visits = {str(r["id"]): r for r in self.rows("visits")}
        by_visit: dict[str, list] = {}
        for r in self.rows("pharmacy_records"):
            title = drugs.get(str(r.get("pid")))
            item = fp_item(title)
            if item:
                by_visit.setdefault(str(r.get("qid")), []).append((r, item, title))
        vis, why, todo = self.visit_map(), Counter(), []
        seen_patient, had_long_acting = set(), set()
        order = sorted(by_visit, key=lambda q: (stamp(by_visit[q][0][0].get("updated_on")) or "", int(q)))
        for q in order:
            rows = by_visit[q]
            spid = str((visits.get(q) or {}).get("patientsid"))
            items = [x[1] for x in rows]
            long_acting = any(m in LONG_ACTING for ms, _ in items for m in ms)
            v = vis.get(gid("visits", q))
            when = day(rows[0][0].get("updated_on"))
            if v is None or not when:
                why["visit not in V3"] += 1
            else:
                todo.append(fp_service_payload(v[1], v[0], q, when, items, [x[2] for x in rows],
                                               revisit=spid in seen_patient, reinsertion=spid in had_long_acting))
            seen_patient.add(spid)
            if long_acting:
                had_long_acting.add(spid)
        r = self.api("GET", "/v1/fp-services", params={"date_from": "2000-01-01", "date_to": self.today, "per_page": "all"})
        r.raise_for_status()
        done = {m[0] for x in (r.json().get("data") or []) for m in [re.findall(r"^Gyno visit \d+", x.get("notes") or "")] if m}
        before = len(todo)
        todo = [p for p in todo if p["notes"].split(":")[0] not in done]
        why["already in V3 (skipped)"] = before - len(todo)
        if self.limit:
            todo = todo[:self.limit]
        if self.dry_run:
            log.info("  dry-run fp_services: %s", todo[:2])
            return {**why, "would_post": len(todo)}
        for body in todo:
            r = self.api("POST", "/v1/fp-services", json=body)
            why["ok" if r.status_code == 201 else "failed"] += 1
            if r.status_code != 201 and why["failed"] <= 5:
                log.error("  fp-service %s: %s %s", body["notes"][:40], r.status_code, r.text[:300])
        return dict(why)

    # ── catalogues ──
    def named(self, svc: str, alias: str, names: list[str]) -> dict[str, int]:
        """Reference rows by name (created with a stable uuid when missing) → {name: V3 id}."""
        want = {n: gid(f"{alias}-name", n.lower()) for n in names}
        have = self.ids_by_uuid(svc, alias)
        missing = [{"uuid": u, "name": n} for n, u in want.items() if u not in have]
        if missing and not self.dry_run:
            for p in missing:
                self.c.insert(svc, alias, p, match_on="uuid")
            have = self.ids_by_uuid(svc, alias)
        return {n: have.get(u, 0) for n, u in want.items()}

    def catalogue(self) -> dict:
        cats = self.named("evaluation", "procedure_categories", ["Laboratory", "Ultrasound", "Services", "Procedures"])
        payloads = [procedure_payload("laboratory", "LAB", r, cats["Ultrasound" if clean(r.get("is_ultra")) == "Yes" else "Laboratory"])
                    for r in self.rows("laboratory")]
        payloads += [procedure_payload("otherservices", "SVC", r, cats["Services"]) for r in self.rows("otherservices")]
        payloads += [procedure_payload("otherprocedures", "PRC", r, cats["Procedures"]) for r in self.rows("otherprocedures")]
        return {"categories": len(cats), "procedures": len(payloads), **self.post("evaluation", "procedures", payloads)}

    def procedure_prices(self) -> dict:
        procs, payloads, why = self.ids_by_uuid("evaluation", "procedures"), [], Counter()
        for table in ("laboratory", "otherservices", "otherprocedures"):
            for r in self.rows(table):
                pid = procs.get(gid(table, r["id"]))
                if pid is None:
                    why["procedure not in V3"] += 1
                    continue
                payloads.append(procedure_price_payload(table, r, pid, self.c.facility_id, self.facility_name()))
                why["no price in Gyno (0)"] += not num(r.get("cost"))
        return {**why, **self.post("evaluation", "procedure_prices", payloads)}

    def facility_name(self) -> str | None:
        if not hasattr(self, "_facility_name"):
            self._facility_name = next((f.get("name") for f in self.c.read_all("core", "facilities")
                                        if f["id"] == self.c.facility_id), None)
        return self._facility_name

    def procedure_price_facility(self) -> dict:
        """Facility name on this facility's procedure price rows that lack it (the procedures screen's Facility column)."""
        name, why = self.facility_name(), Counter()
        items = []
        for r in self.c.read_all("evaluation", "procedure_prices", workers=8):
            if r.get("facility_id") != self.c.facility_id:
                why["other facility"] += 1
            elif r.get("facility_name"):
                why["already named"] += 1
            else:
                items.append((r["id"], {"facility_name": name}))
        return {"facility": name, **why, **({"would_update": len(items)} if self.dry_run else dict(self.update("evaluation", "procedure_prices", items)))}

    def investigations(self) -> dict:
        procs = self.ids_by_uuid("evaluation", "procedures")
        ultra = {str(r["id"]) for r in self.rows("laboratory") if clean(r.get("is_ultra")) == "Yes"}
        out = {}
        out["lab_requests"] = self.per_visit("lab_requests", "qid", "evaluation", "investigations", lambda r, vid, pid: investigation_payload(
            gid("lab_requests", r["id"]), vid, pid, type_="radiology" if str(r.get("lid")) in ultra else "laboratory",
            procedure=procs.get(gid("laboratory", r.get("lid"))), quantity=1,
            price=r.get("unitlabcost") or r.get("unitinsurancelabcost"), amount=r.get("unitlabcost") or r.get("unitinsurancelabcost"),
            when=r.get("lab_date") or r.get("updated_on"), updated=r.get("updated_on"), result=r.get("lab_results"),
            comments="Not chargeable" if clean(r.get("chargable")) == "No" else None))
        out["services_records"] = self.per_visit("services_records", "qid", "evaluation", "investigations", lambda r, vid, pid: investigation_payload(
            gid("services_records", r["id"]), vid, pid, type_="treatment", procedure=procs.get(gid("otherservices", r.get("pid"))),
            quantity=r.get("quantity"), price=r.get("unitcost"), amount=r.get("totalcost"), when=r.get("updated_on"),
            comments=r.get("notes")))
        out["otherequests"] = self.per_visit("otherequests", "qid", "evaluation", "investigations", lambda r, vid, pid: investigation_payload(
            gid("otherequests", r["id"]), vid, pid, type_="procedure", procedure=None, quantity=1, price=r.get("cost"),
            amount=r.get("cost"), when=r.get("request_date"), instructions=r.get("orequest")))
        return out

    # ── pharmacy ──
    def products(self) -> dict:
        src = self.rows("pharmacy")
        cats = self.named("inventory", "product_category", sorted({clean(r.get("category")) or "Uncategorised" for r in src}))
        units = self.named("inventory", "unit", sorted({clean(r.get("unit")) or "Unit" for r in src}))
        payloads = [product_payload(r, cats[clean(r.get("category")) or "Uncategorised"], units[clean(r.get("unit")) or "Unit"])
                    for r in src]
        return {"source": len(src), "categories": len(cats), "units": len(units), **self.post("inventory", "product", payloads)}

    def prescription_drug_links(self) -> dict:
        """Prescriptions loaded before their products have drug = NULL (pharmacy can't stock-check or dispense them):
        link each to its V3 product (+ the store), and replace Gyno's '0' item names with the product's name."""
        prods = {str(p.get("uuid")).lower(): p for p in self.c.read_all("inventory", "product", workers=8) if p.get("uuid")}
        rx = {str(r.get("uuid")).lower(): r for r in self.c.read_all("evaluation", "prescriptions", workers=8) if r.get("uuid")}
        store = self.store()
        items, why = [], Counter()
        for r in self.rows("pharmacy_records"):
            have = rx.get(gid("pharmacy_records", r["id"]))
            prod = prods.get(gid("pharmacy", r.get("pid")))
            if have is None:
                continue
            if prod is None:
                why["no product in V3 for the Gyno item"] += 1
                continue
            want = {}
            if have.get("drug") != prod["id"]:
                want["drug"] = prod["id"]
            if not have.get("store_id"):
                want.update(store_id=store, store_name="Main Store")
            if (clean(have.get("drug_name")) or "0") == "0" and prod.get("generic_name"):
                want["drug_name"] = prod["generic_name"]
            if want:
                items.append((have["id"], want))
            else:
                why["already linked"] += 1
        why["to link"] = len(items)
        if self.limit:
            items = items[:self.limit]
        return {**why, **({"would_update": len(items)} if self.dry_run else dict(self.update("evaluation", "prescriptions", items)))}

    def prescription_dose(self) -> dict:
        """Gyno's dosing text → take / unit / route / method / duration on prescriptions already in V3 (the Dose column)."""
        rx = {str(r.get("uuid")).lower(): r for r in self.c.read_all("evaluation", "prescriptions", workers=8) if r.get("uuid")}
        same = lambda have, want: all(num(have.get(k)) == num(v) if k in ("take", "duration") else have.get(k) == v
                                      for k, v in want.items() if k != "frequency") and not have.get("frequency")
        items, why = [], Counter()
        for r in self.rows("pharmacy_records"):
            have, dose = rx.get(gid("pharmacy_records", r["id"])), rx_dose(r.get("notes"))
            if have is None or not dose:
                continue
            if same(have, dose):
                why["already done"] += 1
                continue
            pid = have["id"]
            why["multi-line note (notes only)" if dose.get("method", 0) is None else "with frequency" if "method" in dose else "text only (kept in notes / dosage)"] += 1
            items.append((pid, dose))
        if self.limit:
            items = items[:self.limit]
        return {**why, **({"would_update": len(items)} if self.dry_run else dict(self.update("evaluation", "prescriptions", items)))}

    def drug_insurance_prices(self) -> dict:
        """Set the insurance price (cash × markup) on drugs already in V3: the product and its store stock row."""
        prods = {str(r.get("uuid")).lower(): r for r in self.c.read_all("inventory", "product", workers=8)}
        stocks = {str(r.get("uuid")).lower(): r for r in self.c.read_all("inventory", "stock", workers=8)}
        out, why = {}, Counter()
        for alias, rows, key in (("product", prods, "pharmacy"), ("stock", stocks, "pharmacy-stock")):
            items = []
            for r in self.rows("pharmacy"):
                have, want = rows.get(gid(key, r["id"])), drug_insurance_price(r.get("cost"), r.get("insurancecost"))
                if have is None or want is None:
                    why[f"{alias}: not in V3 / no cash price"] += 1
                elif num(have.get("insurance_price")) == want:
                    why[f"{alias}: already right"] += 1
                else:
                    items.append((have["id"], {"insurance_price": want}))
            if self.limit:
                items = items[:self.limit]
            out[alias] = {"would_update": len(items)} if self.dry_run else dict(self.update("inventory", alias, items))
        return {**why, **out}

    def prescriptions(self) -> dict:
        prods = self.ids_by_uuid("inventory", "product")
        names = {str(r["id"]): clean(r.get("title")) for r in self.rows("pharmacy")}
        return self.per_visit("pharmacy_records", "qid", "evaluation", "prescriptions", lambda r, vid, pid: prescription_payload(
            r, vid, prods.get(gid("pharmacy", r.get("pid"))), names.get(str(r.get("pid"))), self.c.user_id))

    # ── money and bookings ──
    def payments(self) -> dict:
        ptypes = {str(r["id"]): (clean(r.get("paymentype")), clean(r.get("is_insurance"))) for r in self.rows("paymentype")}
        pat = self.ids_by_uuid("reception", "patient")
        vis = self.visit_map()

        def patient_of(r, visit_col):
            v = vis.get(gid("visits", r.get(visit_col)))
            return (v[1] if v else None) or pat.get(gid("patients", r.get("patientid")))
        out, why = {}, Counter()
        pays = []
        for r in self.rows("payments"):
            p = payment_payload(r, patient_of(r, "visitid"), ptypes.get(str(r.get("insurancetype"))), self.c.user_id)
            if p is None:
                why["payments: no date / amount"] += 1
                continue
            why["payments: no patient in V3 (kept unlinked)"] += p["patient_id"] is None
            pays.append(p)
        for r in self.rows("inpatientdeposit"):
            p = payment_payload(r, patient_of(r, "visitid"), ptypes.get(str(r.get("ptype"))), self.c.user_id, deposit=True)
            if p is not None:
                pays.append(p)
        return {"to_post": len(pays), **why, **self.post("finance", "payment", pays)}

    def appointments(self) -> dict:
        pat = self.ids_by_uuid("reception", "patient")
        src, payloads, why = self.rows("bookings"), [], Counter()
        for r in src:
            pid = pat.get(gid("patients", r.get("patientid")))
            p = appointment_payload(r, pid) if pid else None
            if p is None:
                why["no patient / date"] += 1
                continue
            payloads.append(p)
        return {"source": len(src), **why, **self.post("reception", "appointment", payloads)}

    # ── V3 structure: visit routing and payment → invoice links ──
    def destinations_reception(self) -> dict:
        """Only reception's visit_destination (run beside an evaluation-side run to halve the wait)."""
        return self.destinations(sides=("reception",))

    def destinations(self, sides: tuple = ("evaluation", "reception")) -> dict:
        dests ={d.get("slug"): (d["id"], d.get("name")) for d in self.c.read_all("core", "service_destinations")}
        missing = [s for s in (CONSULT_SLUG, WALKIN_SLUG, "triage", "laboratory", "ultrasound", "pharmacy", "inpatient")
                   if s not in dests]
        ids = lambda table, col: {str(r.get(col)) for r in self.rows(table)}
        vitals, rx = ids("triage", "qid"), ids("pharmacy_records", "qid")
        ultra_lab = {str(r["id"]) for r in self.rows("laboratory") if clean(r.get("is_ultra")) == "Yes"}
        lab, ultra = set(), set()
        for r in self.rows("lab_requests"):
            (ultra if str(r.get("lid")) in ultra_lab else lab).add(str(r.get("qid")))
        # reception's visit_destination needs a core department (org 6 has none): one per destination, same name
        depts = self.named("core", "departments", sorted({dests[s][1] for s in dests if s in
                                                          (CONSULT_SLUG, WALKIN_SLUG, "triage", "laboratory", "ultrasound",
                                                           "pharmacy", "inpatient")}))
        vis = self.visit_map()
        ev, rc = [], []
        for r in self.rows("visits"):
            v = vis.get(gid("visits", r["id"]))
            if v is None:
                continue
            sid = str(r["id"])
            when = stamp(r.get("dateofvisit"), r.get("timeofvisit"))
            done = not day(r.get("dateofvisit")) or day(r.get("dateofvisit")) < self.today
            route = visit_route(r.get("pstatus"), has_vitals=sid in vitals, has_lab=sid in lab, has_ultra=sid in ultra,
                                has_rx=sid in rx, inpatient=clean(r.get("is_inpatient")) == "Yes")
            e, c_ = destination_payloads(sid, v[0], when, done, route, dests)
            ev += e
            rc += [{**x, "department_id": depts.get(x["department_name"])} for x in c_]
        out = {"missing_destinations": missing}
        if "evaluation" in sides:
            out["evaluation"] = dict(self.post("evaluation", "visit_destinations", ev))
        if "reception" in sides:
            out["reception"] = dict(self.post("reception", "visit_destination", rc))
        return out

    def payment_links(self) -> dict:
        """Each payment → its visit's invoice (invoice_id), once both are in V3."""
        inv_by_visit = {}
        for i in self.c.read_all("finance", "invoice", workers=8):
            if i.get("visit_id") and str(i.get("source") or "") == "gyno_migration":
                inv_by_visit.setdefault(i["visit_id"], i["id"])
        vis, pay_ids = self.visit_map(), self.ids_by_uuid("finance", "payment")
        updates = []
        for table in ("payments", "inpatientdeposit"):
            for r in self.rows(table):
                v, pid = vis.get(gid("visits", r.get("visitid"))), pay_ids.get(gid(table, r["id"]))
                if v and pid and v[0] in inv_by_visit:
                    updates.append({"uuid": gid(table, r["id"]), "invoice_id": inv_by_visit[v[0]]})
        if self.limit:
            updates = updates[:self.limit]
        if self.dry_run:
            return {"would_link": len(updates)}
        res, lock = Counter(), threading.Lock()

        def one(u):
            r = self.c.post("finance", {"action": "update", "model": "payment", "source_tenant_id": self.c.organization_id,
                                        "destination_tenant_id": self.c.organization_id, "ids": [pay_ids[u["uuid"]]],
                                        "data": {"invoice_id": u["invoice_id"]}}, timeout=120)
            with lock:
                res["ok" if r.ok else "failed"] += 1
        with ThreadPoolExecutor(self.workers) as ex:
            list(ex.map(one, updates))
        return {"to_link": len(updates), **res}

    def receipts(self) -> dict:
        """Every Gyno payment / deposit → finance/receipt (what the money screens show), linked to its V3 payment
        and its visit's invoice where those exist; receipts take any method, so payments V3 refused are covered."""
        ptypes = {str(r["id"]): (clean(r.get("paymentype")), clean(r.get("is_insurance"))) for r in self.rows("paymentype")}
        pat, vis, pays = self.ids_by_uuid("reception", "patient"), self.visit_map(), self.ids_by_uuid("finance", "payment")
        inv_by_visit = {}
        for i in self.c.read_all("finance", "invoice", workers=8):
            if i.get("visit_id") and str(i.get("source") or "") == "gyno_migration":
                inv_by_visit.setdefault(i["visit_id"], i["id"])
        payloads, why = [], Counter()
        for table, type_col, deposit in (("payments", "insurancetype", False), ("inpatientdeposit", "ptype", True)):
            for r in self.rows(table):
                v = vis.get(gid("visits", r.get("visitid")))
                p = receipt_payload(r, (v[1] if v else None) or pat.get(gid("patients", r.get("patientid"))),
                                    ptypes.get(str(r.get(type_col))), self.c.user_id, payment_id=pays.get(gid(table, r["id"])),
                                    invoice_id=inv_by_visit.get(v[0]) if v else None, deposit=deposit)
                if p is None:
                    why["no date / amount"] += 1
                    continue
                why["no V3 payment row (method refused there)"] += p["payment_id"] is None
                payloads.append(p)
        return {"to_post": len(payloads), **why, **self.post("finance", "receipt", payloads)}

    # ── staff ──
    def users(self) -> dict:
        src = self.rows("users")
        return {"source": len(src), **self.post("core", "users", [user_payload(r) for r in src])}

    def user_profiles(self) -> dict:
        users, src, payloads = self.ids_by_uuid("core", "users"), self.rows("users"), []
        for r in src:
            uid = users.get(gid("users", r["id"]))
            if uid:
                payloads.append(user_profile_payload(r, uid))
        return {"source": len(src), "users_in_v3": len(payloads), **self.post("core", "user_profile", payloads)}

    def visit_doctors(self) -> dict:
        """visit.user = the attending doctor's V3 user (V3 shows it as the visit's doctor_name):
        visits.doctorid → doctors.userid → users. Only that field is sent, by V3 visit id."""
        users = self.ids_by_uuid("core", "users")
        doctor_user = {str(d["id"]): users.get(gid("users", d.get("userid"))) for d in self.rows("doctors")}
        vis, updates, why = self.visit_map(), [], Counter()
        for r in self.rows("visits"):
            v = vis.get(gid("visits", r["id"]))
            if v is None:
                continue
            did = str(r.get("doctorid") or "0")
            if did in ("0", "", "None"):
                why["no doctor in Gyno"] += 1
                continue
            uid = doctor_user.get(did)
            if not uid:
                why["doctor has no login in Gyno"] += 1
                continue
            updates.append((v[0], {"user": uid}))
        if self.limit:
            updates = updates[:self.limit]
        if self.dry_run:
            return {**why, "would_set": len(updates)}
        return {**why, "to_set": len(updates), **self.update("evaluation", "visits", updates)}

    # ── inpatient: admissions, discharges, links; prescriptions as V3 lists them ──
    def coded(self, svc: str, alias: str, name: str, code: str) -> int:
        """A reference row with name + code (created when missing) → its V3 id."""
        uid = gid(f"{alias}-code", code)
        have = self.ids_by_uuid(svc, alias)
        if uid not in have and not self.dry_run:
            self.c.insert(svc, alias, {"uuid": uid, "name": name, "code": code}, match_on="uuid")
            have = self.ids_by_uuid(svc, alias)
        return have.get(uid, 0)

    def visit_doctor_users(self) -> dict[str, int]:
        users = self.ids_by_uuid("core", "users")
        doc = {str(d["id"]): users.get(gid("users", d.get("userid"))) for d in self.rows("doctors")}
        return {str(v["id"]): doc.get(str(v.get("doctorid"))) for v in self.rows("visits") if doc.get(str(v.get("doctorid")))}

    def admissions(self) -> dict:
        type_id = self.coded("inpatient", "admission_type", "Inpatient Admission", "GYN-INPATIENT")
        vis, doctors = self.visit_map(), self.visit_doctor_users()
        disc = {}
        for r in self.rows("discharge"):
            disc[str(r.get("qid"))] = stamp(r.get("updated_on"))
        payloads, why = [], Counter()
        for r in self.rows("visits"):
            if clean(r.get("is_inpatient")) != "Yes":
                continue
            v = vis.get(gid("visits", r["id"]))
            if v is None:
                why["inpatient visit not in V3 (no patient)"] += 1
                continue
            p = admission_payload(r, v[0], v[1], type_id=type_id, doctor=doctors.get(str(r["id"])) or self.c.user_id,
                                  discharge=disc.get(str(r["id"])), today=self.today)
            if p is None:
                why["no admission date"] += 1
                continue
            why[p["status"]] += 1
            payloads.append(p)
        return {"admission_type": type_id, **why, **self.post("inpatient", "admission", payloads)}

    def admission_by_source_visit(self) -> dict[str, int]:
        adm = self.ids_by_uuid("inpatient", "admission")
        return {str(r["id"]): adm[gid("admissions", r["id"])] for r in self.rows("visits")
                if clean(r.get("is_inpatient")) == "Yes" and gid("admissions", r["id"]) in adm}

    def inpatient_discharges(self) -> dict:
        type_id = self.coded("inpatient", "discharge_type", "Patient is Stable", "PATIENT_IS_STABLE")
        adm, doctors = self.admission_by_source_visit(), self.visit_doctor_users()
        payloads, why = [], Counter()
        for r in self.rows("discharge"):
            a = adm.get(str(r.get("qid")))
            p = discharge_record_payload(r, a, type_id, doctors.get(str(r.get("qid"))) or self.c.user_id) if a else None
            if p is None:
                why["no admission in V3 (visit not inpatient in Gyno / no patient)"] += 1
                continue
            payloads.append(p)
        return {"discharge_type": type_id, **why, **self.post("inpatient", "discharge", payloads)}

    def inpatient_links(self) -> dict:
        """Nursing cardex notes → their admission (admission_id), so they show under the stay."""
        adm, notes = self.admission_by_source_visit(), self.ids_by_uuid("inpatient", "note")
        updates = [(notes[gid("nursecardex", r["id"])], {"admission_id": adm[str(r.get("qid"))]})
                   for r in self.rows("nursecardex") if str(r.get("qid")) in adm and gid("nursecardex", r["id"]) in notes]
        if self.limit:
            updates = updates[:self.limit]
        return {"to_link": len(updates), **(Counter() if self.dry_run else self.update("inpatient", "note", updates))}

    def prescription_fix(self) -> dict:
        """status 'active' (what V3 lists), Gyno's dosing text, the store, and admission_id on inpatient stays."""
        store_id = self.store()
        rx, adm = self.ids_by_uuid("evaluation", "prescriptions"), self.admission_by_source_visit()
        updates = [(rx[gid("pharmacy_records", r["id"])], rx_fix_payload(r, (store_id, "Main Store"), adm.get(str(r.get("qid")))))
                   for r in self.rows("pharmacy_records") if gid("pharmacy_records", r["id"]) in rx]
        if self.limit:
            updates = updates[:self.limit]
        why = Counter(with_dosage=sum(1 for _, d in updates if d.get("dosage")),
                      inpatient=sum(1 for _, d in updates if d.get("admission_id")))
        return {"to_fix": len(updates), **why, **(Counter() if self.dry_run else self.update("evaluation", "prescriptions", updates))}

    # ── wards and beds: Gyno's HMS never recorded them — loaded from the two sheets Gyno fills in ──
    def wards(self) -> dict:
        """input/gyno/gyno_wards_and_beds.csv (ward, gender, age group, daily charges, bed, bed type) → wards,
        bed types, beds; input/gyno/gyno_current_inpatients.csv (admission_number, ward, bed) → an open ward
        stay (visit_ward) per currently admitted patient, so the bed board shows them."""
        import csv as _csv
        base = INPUT_DIR
        sheet = lambda name:[{k.split(" (")[0].strip(): (v or "").strip() for k, v in r.items()}
                              for r in _csv.DictReader(open(base / name, encoding="utf-8-sig"))]
        beds_rows = [r for r in sheet("gyno_wards_and_beds.csv") if r.get("ward_name") and not r["ward_name"].startswith("EXAMPLE")]
        wards = {}
        for r in beds_rows:
            wards.setdefault(r["ward_name"], r)
        res = {"wards": dict(self.post("inpatient", "ward", [
            {"uuid": gid("ward", w.lower()), "name": w[:100], "display_name": w[:150],
             "gender": (r.get("ward_gender") or "").lower() or None if (r.get("ward_gender") or "").lower() in ("male", "female", "other") else None,
             "age_group": (r.get("age_group") or "").lower() if (r.get("age_group") or "").lower() in ("adult", "children") else None,
             "cash_cost": num(r.get("daily_cash_charge")), "insurance_cost": num(r.get("daily_insurance_charge"))}
            for w, r in wards.items()]))}
        ward_ids = self.ids_by_uuid("inpatient", "ward")
        types = {t: gid("bed_type", t.lower()) for t in {r.get("bed_type") or "Standard" for r in beds_rows}}
        res["bed_types"] = dict(self.post("inpatient", "bed_type", [{"uuid": u, "name": t[:100]} for t, u in types.items()]))
        type_ids = self.ids_by_uuid("inpatient", "bed_type")
        res["beds"] = dict(self.post("inpatient", "bed", [
            {"uuid": gid("bed", f"{r['ward_name'].lower()}:{r['bed_number']}"), "ward_id": ward_ids.get(gid("ward", r["ward_name"].lower())),
             "number": r["bed_number"][:50], "bed_number": r["bed_number"][:50], "is_available": 1,
             "bed_type_id": type_ids.get(types[r.get("bed_type") or "Standard"])} for r in beds_rows if r.get("bed_number")]))
        bed_ids, adm = self.ids_by_uuid("inpatient", "bed"), {}
        for a in self.c.read_all("inpatient", "admission"):
            adm[str(a.get("admission_number"))] = a
        stays, occupied, why = [], [], Counter()
        for r in sheet("gyno_current_inpatients.csv"):
            a = adm.get(r.get("admission_number"))
            w = ward_ids.get(gid("ward", (r.get("ward_name") or "").lower()))
            b = bed_ids.get(gid("bed", f"{(r.get('ward_name') or '').lower()}:{r.get('bed_number')}"))
            if not (a and w):
                why["no admission / ward"] += 1
                continue
            when = stamp(r.get("date_placed_in_bed")) or stamp(a.get("admission_date"))
            stays.append({"uuid": gid("visit_ward", r["admission_number"]), "admission_id": a["id"], "ward_id": w, "bed_id": b,
                          "allocation_date": when, "transfer_in_date": when, "allocated_by": self.c.user_id})
            if b:
                occupied.append((b, {"is_available": 0}))
        res["ward_stays"] = dict(self.post("inpatient", "visit_ward", stays))
        res["beds_marked_occupied"] = dict(self.update("inpatient", "bed", occupied)) if occupied and not self.dry_run else {}
        return {**why, **res}

    def _evaluation_payment_sources(self):
        """(table, row, deposit flag, mode column) for every Gyno payment and inpatient deposit, with the lookups."""
        ptypes = {str(r["id"]): (clean(r.get("paymentype")), clean(r.get("is_insurance"))) for r in self.rows("paymentype")}
        insurers = self.ids_by_uuid("finance", "insurance_company")
        insurer = {k: insurers.get(gid("paymentype", k)) for k in ptypes}
        pats = {p["id"]: (p["id"], " ".join(x for x in (p.get("first_name"), p.get("middle_name"), p.get("last_name")) if x),
                          p.get("patient_no")) for p in self.c.read_all("reception", "patient", workers=8)}
        pat_by_uuid = {str(p.get("uuid")).lower(): p["id"] for p in self.c.read_all("reception", "patient", workers=8)}
        inv_by_visit = {}
        for i in self.c.read_all("finance", "invoice", workers=8):
            if i.get("visit_id") and str(i.get("source") or "") == "gyno_migration":
                inv_by_visit.setdefault(i["visit_id"], i["id"])
        # cashier: Gyno stores the name typed in (payments.updated_by) → the staff account whose name matches
        users = self.ids_by_uuid("core", "users")
        by_name = {}
        for u in self.rows("users"):
            for nm in (u.get("name"), u.get("username")):
                if clean(nm):
                    by_name.setdefault(words(nm), users.get(gid("users", u["id"])))
        cashier = lambda name: by_name.get(words(name)) or self.c.user_id
        vis = self.visit_map()
        # Gyno records a deposit's money twice: inpatientdeposit AND a payments row pointing at it (deposit_id,
        # 11,072 of 11,499) — that payment carries the method, so only deposits with no payment are loaded
        paid_deposits = {str(p.get("deposit_id")) for p in self.rows("payments") if str(p.get("deposit_id") or "0") not in ("0", "None", "")}
        for table, type_col, deposit in (("payments", "insurancetype", False), ("inpatientdeposit", "ptype", True)):
            for r in self.rows(table):
                if deposit and str(r["id"]) in paid_deposits:
                    continue
                v = vis.get(gid("visits", r.get("visitid")))
                pid = (v[1] if v else None) or pat_by_uuid.get(gid("patients", r.get("patientid")))
                yield (table, r, deposit, v[0] if v else None, pats.get(pid), inv_by_visit.get(v[0]) if v else None,
                       ptypes.get(str(r.get(type_col))), insurer.get(str(r.get(type_col))), cashier(r.get("updated_by")))

    def evaluation_payments(self) -> dict:
        payloads, why = [], Counter()
        for table, r, deposit, vid, pat, inv, ptype, ins, cashier in self._evaluation_payment_sources():
            p = evaluation_payment_payload(r, patient=pat, visit_id=vid, invoice_id=inv, ptype=ptype, cashier=cashier,
                                           insurer=ins, deposit=deposit)
            if p is None:
                why["no date / amount"] += 1
                continue
            why[f"mode {p['payment_mode']}"] += 1
            why["cashier matched by name"] += cashier != self.c.user_id
            payloads.append(p)
        return {"to_post": len(payloads), **why, **self.post("finance", "evaluation_payment", payloads)}

    def evaluation_payment_details(self) -> dict:
        paid = self.ids_by_uuid("finance", "evaluation_payment")
        payloads = []
        for table, r, deposit, vid, pat, inv, ptype, ins, cashier in self._evaluation_payment_sources():
            pid = paid.get(gid("evaluation-" + ("inpatientdeposit" if deposit else "payments"), r["id"]))
            if pid:
                payloads.append(evaluation_payment_detail_payload(r, pid, visit_id=vid, invoice_id=inv,
                                                                  method=ptype[0] if ptype else None, deposit=deposit))
        return {"to_post": len(payloads), **self.post("finance", "evaluation_payment_detail", payloads)}

    def theatre_bookings(self) -> dict:
        have = {r.get("name"): r["id"] for r in self.c.read_all("theatre", "theatre_scheduling_status")}
        for name in ("booked", "completed"):
            if name not in have and not self.dry_run:
                self.c.insert("theatre", "theatre_scheduling_status",
                              {"uuid": gid("theatre_scheduling_status", name), "name": name}, match_on="uuid")
        status = {r.get("name"): r["id"] for r in self.c.read_all("theatre", "theatre_scheduling_status")}
        users = self.ids_by_uuid("core", "users")
        doc = {str(d["id"]): users.get(gid("users", d.get("userid"))) for d in self.rows("doctors")}
        vis, payloads, why = self.visit_map(), [], Counter()
        for r in self.rows("doctorsnotes"):
            v = vis.get(gid("visits", r.get("qid")))
            if v is None:
                continue
            p = theatre_booking_payload(r, v[0], v[1], status=status if status else {"booked": 0, "completed": 0},
                                        requested_by=doc.get(str(r.get("doctorid"))) or self.c.user_id, today=self.today)
            if p:
                why["booked (upcoming)" if p["status_id"] == status.get("booked") else "completed (past)"] += 1
                payloads.append(p)
        return {**why, **self.post("theatre", "theatre_booking", payloads)}

    def po_history(self) -> dict:
        """Supplier history: purchase orders get Gyno's status + amounts (+ payments from lpopayments), goods
        received notes get the supplier's invoice number (vendorinvoices) — what V3's supplier account shows."""
        pos, grns = self.ids_by_uuid("inventory", "purchase_order"), self.ids_by_uuid("inventory", "goods_received_note")
        paid = Counter()
        for p in self.rows("lpopayments"):
            paid[str(p.get("lpoid"))] += num(p.get("totalpayment")) or 0.0
        po_updates = [(pos[gid("lpos", r["id"])], po_history_payload(r, paid.get(str(r["id"]), 0.0)))
                      for r in self.rows("lpos") if gid("lpos", r["id"]) in pos]
        invoice = {}
        for v in sorted(self.rows("vendorinvoices"), key=lambda v: str(v.get("updated_on") or "")):
            if clean(v.get("invoiceno")):
                invoice[str(v.get("lpoid"))] = (clean(v.get("invoiceno"))[:255], day(v.get("updated_on")))
        grn_updates = []
        for r in self.rows("deliveries"):
            inv = invoice.get(str(r.get("lpoid")))
            if inv and gid("deliveries", r["id"]) in grns:
                grn_updates.append((grns[gid("deliveries", r["id"])], {"supplier_invoice_no": inv[0], "supplier_invoice_date": inv[1]}))
        if self.limit:
            po_updates, grn_updates = po_updates[:self.limit], grn_updates[:self.limit]
        if self.dry_run:
            return {"purchase_orders": len(po_updates), "goods_received_invoices": len(grn_updates),
                    "statuses": dict(Counter(d["status"] for _, d in po_updates))}
        return {"purchase_orders": dict(self.update("inventory", "purchase_order", po_updates)),
                "goods_received_invoices": dict(self.update("inventory", "goods_received_note", grn_updates)),
                "statuses": dict(Counter(d["status"] for _, d in po_updates))}

    def close_admissions(self) -> dict:
        """Admissions V3 has as 'admitted' that aren't on Gyno's current-inpatient sheet → 'discharged', dated by
        the stay's last recorded activity (nursing note, medication, doctor note), else the visit's checkout."""
        import csv as _csv
        current = {r["admission_number"].strip() for r in _csv.DictReader(open(INPUT_DIR / "gyno_current_inpatients.csv",
                                                                               encoding="utf-8-sig"))}
        last = {}
        for table, col, when in (("nursecardex", "qid", "updated_on"), ("pharmacy_records", "qid", "updated_on"),
                                 ("doctorsnotes", "qid", "updated_on")):
            for r in self.rows(table):
                t = stamp(r.get(when))
                if t and t > last.get(str(r.get(col)), ""):
                    last[str(r.get(col))] = t
        checkout = {str(v["id"]): stamp(v.get("check_out")) for v in self.rows("visits") if clean(v.get("is_inpatient")) == "Yes"}
        updates, why = [], Counter()
        for a in self.c.read_all("inpatient", "admission"):
            if a.get("status") != "admitted":
                continue
            if str(a.get("admission_number")) in current:
                why["still admitted (on Gyno's sheet)"] += 1
                continue
            src = str(a.get("admission_number")).rsplit("-", 1)[-1]
            admitted = str(a.get("admission_date") or "")[:19].replace("T", " ")
            out = max(x for x in (last.get(src), checkout.get(src), admitted) if x)
            updates.append((a["id"], {"status": "discharged", "discharge_date": out}))
            why["discharged (left, per Gyno)"] += 1
        if self.limit:
            updates = updates[:self.limit]
        return {**why, **(Counter() if self.dry_run else self.update("inpatient", "admission", updates))}

    def update(self, svc: str, alias: str, items: list[tuple[int, dict]]) -> Counter:
        res, lock = Counter(), threading.Lock()

        def one(item):
            vid, data = item
            try:
                r = self.c.post(svc, {"action": "update", "model": alias, "source_tenant_id": self.c.organization_id,
                                      "destination_tenant_id": self.c.organization_id, "ids": [vid], "data": data}, timeout=120)
            except Exception as e:       # network gone past the client's retries: count it, keep going (re-run fixes it)
                with lock:
                    res["failed (network)"] += 1
                    if res["failed (network)"] <= 3:
                        log.error("  %s/%s update %s: %s", svc, alias, vid, str(e)[:200])
                return
            body = r.json() if r.ok else {}
            ok = r.ok and not any((x or {}).get("ignored") for x in (body.get("data") or []))
            with lock:
                res["ok" if ok else "failed"] += 1
                if not ok and res["failed"] <= 3:
                    log.error("  %s/%s update %s: %s %s", svc, alias, vid, r.status_code, r.text[:250])
                if sum(res.values()) % 2000 == 0:
                    log.info("  %s/%s: %d / %d updated (%d failed)", svc, alias, sum(res.values()), len(items), res["failed"])
        with ThreadPoolExecutor(self.workers) as ex:
            list(ex.map(one, items))
        return res

    # ── insurance and invoices ──
    def insurance(self) -> dict:
        types = self.rows("paymentype")
        res = self.post("finance", "insurance_company",
                        [insurance_company_payload(r) for r in types if is_insurer(r.get("paymentype"), r.get("is_insurance"))])
        cos = self.ids_by_uuid("finance", "insurance_company")
        company = {str(r["id"]): (cos.get(gid("paymentype", r["id"])), clean(r.get("paymentype"))) for r in types}
        insurer = {str(r["id"]) for r in types if is_insurer(r.get("paymentype"), r.get("is_insurance"))}
        pat, payloads, why = self.ids_by_uuid("reception", "patient"), [], Counter()
        for r in self.rows("preferedpayment"):
            pid = pat.get(gid("patients", r.get("patientid")))
            if pid is None:
                why["no patient"] += 1
            elif str(r.get("paymenttype")) not in insurer:
                why["cash / mpesa preference (no insurance)"] += 1
            else:
                payloads.append(patient_insurance_payload(r, pid, company.get(str(r.get("paymenttype")))))
        return {"companies": dict(res), **why, **self.post("reception", "patient_insurance", payloads)}

    def insurance_schemes(self) -> dict:
        """Link patient insurance to V3's core insurers + schemes (what the reception patient form lists): Gyno insurers
        are matched to core insurers or created, each gets one core scheme named as in Gyno, and every Gyno
        patient_insurance row gets that company / scheme id. Employer text already in the scheme name is kept."""
        insurers = [r for r in self.rows("paymentype") if is_insurer(r.get("paymentype"), r.get("is_insurance"))]
        core = {words(r.get("name")): r["id"] for r in self.c.read_all("core", "insurance_companies")}
        why, company = Counter(), {}
        new_cos = {}
        for r in insurers:
            name = clean(r.get("paymentype"))
            cid = core_insurer_match(name, core) or core.get(words(name))
            if cid:
                company[str(r["id"])] = cid
                why["insurer matched to V3"] += 1
            else:
                new_cos.setdefault(words(name), {"uuid": gid("core-insurance-company", words(name)), "name": name,
                                                  "code": f"GYN-INS-{r['id']}", "status": "active"})
        why["insurers created"] = len(new_cos)
        res = {"companies": dict(self.post("core", "insurance_companies", list(new_cos.values())))}
        by_uuid = self.ids_by_uuid("core", "insurance_companies")
        for r in insurers:
            company.setdefault(str(r["id"]), by_uuid.get(gid("core-insurance-company", words(r.get("paymentype")))))
        schemes = {}
        for r in insurers:
            name, cid = clean(r.get("paymentype")), company.get(str(r["id"]))
            if cid:
                schemes.setdefault(words(name), {"uuid": gid("core-insurance-scheme", words(name)), "company_id": cid, "name": name,
                                                 "code": f"GYN-SCH-{r['id']}", "type": scheme_type(name), "status": "active"})
        res["schemes"] = dict(self.post("core", "insurance_schemes", list(schemes.values())))
        sch = self.ids_by_uuid("core", "insurance_schemes")
        co_name = {r["id"]: r.get("name") for r in self.c.read_all("core", "insurance_companies")}
        link = {str(r["id"]): (company.get(str(r["id"])), sch.get(gid("core-insurance-scheme", words(r.get("paymentype")))),
                               clean(r.get("paymentype"))) for r in insurers}
        pref = {gid("preferedpayment", r["id"]): r for r in self.rows("preferedpayment")}
        items = []
        for p in self.c.read_all("reception", "patient_insurance", workers=8):
            src = pref.get(str(p.get("uuid")).lower())
            if src is None:
                why["entered in V3 (left as is)"] += 1
                continue
            cid, sid, sname = link.get(str(src.get("paymenttype")), (None, None, None))
            if not (cid and sid):
                why["no insurer"] += 1
                continue
            want = {"insurance_company_id": cid, "insurance_company_name": co_name.get(cid), "insurance_scheme_id": sid,
                    "insurance_scheme_name": clean(src.get("scheme")) or sname}
            if all(p.get(k) == v for k, v in want.items()):
                why["already linked"] += 1
            else:
                items.append((p["id"], want))
        if self.limit:
            items = items[:self.limit]
        res["patient_insurance"] = {"would_update": len(items)} if self.dry_run else dict(self.update("reception", "patient_insurance", items))
        return {**why, **res}

    def invoices(self) -> dict:
        vis, pat = self.visit_map(), self.ids_by_uuid("reception", "patient")
        payloads, why = [], Counter()
        for r in self.rows("generalpaymentdetails"):
            v = vis.get(gid("visits", r.get("visit_id")))
            p = invoice_payload(r, v[0] if v else None, (v[1] if v else None) or pat.get(gid("patients", r.get("patient_id"))))
            if p is None:
                why["no date"] += 1
                continue
            why["visit not in V3 (kept unlinked)"] += v is None
            payloads.append(p)
        return {**why, **self.post("finance", "invoice", payloads)}

    # ── small clinical tables ──
    def extra_notes(self) -> dict:
        out = {"doctorsnotesfornurses": self.per_visit("doctorsnotesfornurses", "qid", "evaluation", "doctor_notes",
                                                       lambda r, vid, pid: {"uuid": gid("doctorsnotesfornurses", r["id"]), "visit_id": vid,
                                                                            "visit": vid, "patient_id": pid, "plan": clean(r.get("thenotes")),
                                                                            "remarks": "Doctor's instructions to nurses", "status": "final",
                                                                            "doctor_name": clean(r.get("updated_by")),
                                                                            "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))})}
        out["inpatientexternalprocedures"] = self.per_visit(
            "inpatientexternalprocedures", "qid", "evaluation", "doctor_notes",
            lambda r, vid, pid: {"uuid": gid("inpatientexternalprocedures", r["id"]), "visit_id": vid, "visit": vid, "patient_id": pid,
                                 "investigations": text_join(("External lab request", r.get("lab_request")),
                                                             ("External pharmacy request", r.get("pharmacy_request"))),
                                 "status": "final", "doctor_name": clean(r.get("updated_by")),
                                 "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))})
        return out

    # ── documents (metadata now; the files follow once a fresh S3 manifest is available) ──
    def documents(self) -> dict:
        sizes = {}
        mf = Path(self.manifest)
        if mf.exists():
            raw = mf.read_text(encoding="utf-8")
            sizes = {f["filename"]: f.get("size_bytes") for f in json.loads(raw[raw.index("{"):]).get("files", [])}
        vis = self.visit_map()
        out = {"manifest_files": len(sizes)}
        for table, visit_col, title_col, file_col, kind in (("externaltests", "qid", "document_title", "document", "lab_result"),
                                                            ("scanneddocument", "visitid", "title", "sdocument", "other")):
            payloads, why = [], Counter()
            pat = self.ids_by_uuid("reception", "patient")
            for r in self.rows(table):
                v = vis.get(gid("visits", r.get(visit_col)))
                pid = (v[1] if v else None) or pat.get(gid("patients", r.get("patientid")))
                p = document_payload(table, r, pid, sizes, title_col=title_col, file_col=file_col, doc_type=kind) if pid else None
                if p is None:
                    why["no patient / no file"] += 1
                    continue
                why["file not in manifest"] += p["file_size"] == 0
                payloads.append(p)
            out[table] = {**why, **self.post("reception", "patient_document", payloads)}
        return out

    # ── procurement and stock ──
    def store(self) -> int:
        return self.named("inventory", "store", ["Main Store"])["Main Store"]

    def stock(self) -> dict:
        src = self.rows("stocks")
        cats = self.named("inventory", "product_category", ["Kitchen", "General stores"])
        unit = self.named("inventory", "unit", ["Unit"])["Unit"]
        res = self.post("inventory", "product", [stock_product_payload(r, cats["Kitchen" if clean(r.get("is_kitchen")) == "Yes"
                                                                          else "General stores"], unit) for r in src])
        prods, store, user = self.ids_by_uuid("inventory", "product"), self.store(), self.c.user_id
        stock_rows, moves = [], []
        for table, rows, qty_col, price_col, ins_col in (("pharmacy", self.rows("pharmacy"), "stock", "cost", "insurancecost"),
                                                         ("stocks", src, "quantity", "cost", None)):
            for r in rows:
                pid = prods.get(gid(table, r["id"]))
                if pid is None:
                    continue
                # V3 keeps stock on hand on the stock row itself (movements posted through the gateway don't
                # update it), so the source quantity, cost and value are written here
                stock_rows.append(stock_row_payload(gid(f"{table}-stock", r["id"]), pid, store, quantity=r.get(qty_col),
                                                    unit_cost=r.get("buying_price") or r.get("cost"),
                                                    selling=r.get(price_col),
                                                    insurance=drug_insurance_price(r.get(price_col), r.get(ins_col)) if ins_col else None))
                m = movement_payload(gid(f"{table}-opening", r["id"]), pid, store, user, type_="adjustment", quantity=r.get(qty_col),
                                     unit_cost=r.get("buying_price") or r.get("cost"), when=r.get("updated_on"),
                                     reason="Opening balance from Gyno HMS")
                if m:
                    moves.append(m)
        for r in self.rows("stock_requests"):
            pid = prods.get(gid("stocks", r.get("stock_id")))
            m = movement_payload(gid("stock_requests", r["id"]), pid, store, user, type_="issue", quantity=r.get("quantity"),
                                 when=r.get("updated_on"), incoming=False,
                                 reason=f"Stock request ({'dispatched' if clean(r.get('dispatched')) == 'Yes' else 'not dispatched'}) by {clean(r.get('updated_by'))}") if pid else None
            if m:
                moves.append(m)
        return {"stock_products": dict(res), "stock": dict(self.post("inventory", "stock", stock_rows)),
                "movements": dict(self.post("inventory", "stock_movement", moves))}

    def procurement(self) -> dict:
        user, store = self.c.user_id, self.store()
        out = {"suppliers": dict(self.post("inventory", "supplier", [supplier_payload(r) for r in self.rows("suppliers")]))}
        sup = self.ids_by_uuid("inventory", "supplier")
        lpos = self.rows("lpos")
        lpo_supplier = {str(r["id"]): r.get("supplier_id") for r in lpos}
        # Gyno reuses an LPO number now and then; V3 keeps PO numbers unique per organization → "<no>-<Gyno id>"
        seen_no = Counter(clean(r.get("lpo_no")) for r in lpos if clean(r.get("lpo_no")))
        pos = []
        for r in lpos:
            if not sup.get(gid("suppliers", r.get("supplier_id"))):
                continue
            p = po_payload(r, sup.get(gid("suppliers", r.get("supplier_id"))), store, user)
            if p and seen_no.get(clean(r.get("lpo_no")), 0) > 1:
                p["po_number"] = f"{p['po_number']}-{r['id']}"[:255]
            if p:
                pos.append(p)
        out["purchase_orders"] = dict(self.post("inventory", "purchase_order", pos))
        po_ids, prods = self.ids_by_uuid("inventory", "purchase_order"), self.ids_by_uuid("inventory", "product")
        orders = self.rows("order_products")
        prod_of = lambda r: prods.get(gid("stocks" if (clean(r.get("thetable")) or "").lower().startswith("stock") else "pharmacy",
                                          r.get("product_id")))
        items = [po_item_payload(r, po_ids[gid("lpos", r.get("lpoid"))], prod_of(r)) for r in orders if gid("lpos", r.get("lpoid")) in po_ids]
        out["purchase_order_items"] = dict(self.post("inventory", "purchase_order_item", items))
        item_ids, order = self.ids_by_uuid("inventory", "purchase_order_item"), {str(r["id"]): r for r in orders}
        grns, grn_items = [], []
        for r in self.rows("deliveries"):
            po = po_ids.get(gid("lpos", r.get("lpoid")))
            sid = sup.get(gid("suppliers", lpo_supplier.get(str(r.get("lpoid")))))
            d = day(r.get("deliverydate")) or day(r.get("updated_on"))
            if not (po and sid and d):
                continue
            grns.append({"uuid": gid("deliveries", r["id"]), "grn_number": f"GYN-GRN-{r['id']}", "purchase_order_id": po,
                         "store_id": store, "supplier_id": sid, "received_date": d, "received_by": user,
                         "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))})
        out["goods_received"] = dict(self.post("inventory", "goods_received_note", grns))
        grn_ids = self.ids_by_uuid("inventory", "goods_received_note")
        for r in self.rows("deliveries"):
            g_id, o = grn_ids.get(gid("deliveries", r["id"])), order.get(str(r.get("orderproductid")))
            if not g_id:
                continue
            q, cost = int(num(r.get("quantity")) or 0), num(o.get("buyingprice")) if o else 0.0
            grn_items.append({"uuid": gid("deliveries-item", r["id"]), "grn_id": g_id, "product_id": prod_of(o) if o else None,
                              "po_item_id": item_ids.get(gid("order_products", r.get("orderproductid"))),
                              "quantity_ordered": int(num(o.get("quantity")) or q) if o else q, "quantity_received": q,
                              "unit_cost": cost or 0.0, "line_total": round(q * (cost or 0.0), 2)})
        no_product = [i for i in grn_items if not i.get("product_id")]
        grn_items = [i for i in grn_items if i.get("product_id")]          # V3 requires the item's product
        out["goods_received_items"] = {**dict(self.post("inventory", "goods_received_item", grn_items)),
                                       **({"no product in V3 (not sent)": len(no_product)} if no_product else {})}
        return out

    # ── everything else → activity_log ──
    def activity(self) -> dict:
        pat = self.ids_by_uuid("reception", "patient")
        vis = self.visit_map()
        P = r"App\Models\Patient"
        by_patient = lambda col: (lambda r: (P, pat.get(gid("patients", r.get(col)))) if pat.get(gid("patients", r.get(col))) else None)
        by_visit = lambda col: (lambda r: (r"App\Models\Visit", vis[gid("visits", r.get(col))][0]) if gid("visits", r.get(col)) in vis else None)
        specs = [  # table, description, date column, subject
            ("specificpaymentdetails", lambda r: f"Payment detail {clean(r.get('invoice_no'))}: {clean(r.get('productservice'))} — {clean(r.get('totalamount'))}", "servicedate", by_patient("patientsid")),
            ("expenses", lambda r: f"Expense ({clean(r.get('expense_type'))}, {clean(r.get('payment_type'))}): {clean(r.get('description'))} — {clean(r.get('cost'))}", "date_of_transaction", None),
            ("lpopayments", lambda r: f"LPO payment for LPO {clean(r.get('lpoid'))}: {clean(r.get('totalpayment'))} by {clean(r.get('modeofpayment'))} (voucher {clean(r.get('voucherno'))})", "updated_on", None),
            ("vendorinvoices", lambda r: f"Vendor invoice {clean(r.get('invoiceno'))} for LPO {clean(r.get('lpoid'))}", "updated_on", None),
            ("supplier_products", lambda r: f"Supplier {clean(r.get('supplier_id'))} supplies {clean(r.get('mytable'))} item {clean(r.get('product_id'))}", "updated_on", None),
            ("total_in_out", lambda r: f"Daily totals: income {clean(r.get('income'))}, expenditure {clean(r.get('expenditure'))}, balance {clean(r.get('balance'))}", "date_of_transaction", None),
            ("doctor_availability", lambda r: f"Doctor {clean(r.get('doctorid'))} {clean(r.get('status'))} {clean(r.get('thedate'))} {clean(r.get('checkin'))}–{clean(r.get('checkout'))}", "updated_on", by_visit("qid")),
            ("consultants", lambda r: f"Consultant {clean(r.get('name'))} ({clean(r.get('specialization'))})", "updated_on", None),
            ("stock_logs", lambda r: clean(r.get("description")), "updated_on", None),
            ("hospitalsettings", lambda r: f"Hospital settings: consultation {clean(r.get('consultation'))}, review {clean(r.get('review'))}", None, None),
            ("groupsms", lambda r: f"SMS group {clean(r.get('name_of_group'))}", "mydate", None),
            ("patientgroupsms", lambda r: f"SMS group {clean(r.get('group_id'))} member {clean(r.get('patients_name'))}", "mydate", by_patient("patiend_id")),
            ("smssent", lambda r: clean(r.get("sms")), "updated_on", None),
            ("smstoa", lambda r: f"SMS sender {clean(r.get('toa'))}", None, None),
            ("sendsms", lambda r: f"SMS to {clean(r.get('phone'))} ({clean(r.get('status'))}): {clean(r.get('themessage'))}", None, None),
            ("portal_logs", lambda r: f"{clean(r.get('updated_by'))}: {clean(r.get('what_happened'))}", "updated_on", None),
        ]
        out = {}
        for table, desc, when_col, subject in specs:
            src = self.rows(table)
            payloads = [activity_payload(table, r, desc(r), r.get(when_col) if when_col else None, subject(r) if subject else None)
                        for r in src]
            out[table] = {"source": len(src), **self.post("reception", "activity_log", payloads)}
        return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--steps", nargs="*", default=list(STEPS), choices=STEPS)
    ap.add_argument("--limit", type=int, default=0, help="post at most N rows per step (canary)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-missing", action="store_true", help="post only rows whose uuid V3 doesn't have yet")
    ap.add_argument("--manifest", default="/tmp/gyno_s3_files_manifest.json", help="documents: the S3 files manifest (sizes)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run = Run(a.dry_run, a.limit, a.workers, a.only_missing)
    run.manifest = a.manifest
    report = {"target": TARGET, "org": run.c.organization_id, "dry_run": a.dry_run, "limit": a.limit,
              "started": time.strftime("%Y-%m-%d %H:%M:%S"), "steps": {}}
    for s in STEPS:
        if s in a.steps:
            t0 = time.time()
            try:
                report["steps"][s] = getattr(run, s)()
            except Exception as e:
                log.exception("%s failed", s)
                report["steps"][s] = {"error": f"{type(e).__name__}: {str(e)[:300]}"}
            report["steps"][s]["seconds"] = round(time.time() - t0)
            log.info("%s: %s", s, report["steps"][s])
    out = Path(__file__).resolve().parent / "output" / "gyno_to_v3"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"gyno-{time.strftime('%Y%m%d_%H%M%S')}.json"
    path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    log.info("report: %s", path)
    sys.exit(1 if any("error" in r for r in report["steps"].values()) else 0)


if __name__ == "__main__":
    main()
