"""
medhanit_to_v3.py — Medhanit Healthcare's Collabmed V1 data → V3, through the model gateway.

Target (.env, AFYA_MEDHANIT_*): migrate platform, org 16 / facility 20, user Medhanit (1502).
Source: the Snowflake mirror HOSPITALS.MEDHANIT_V1 (medhanit_v1_to_snowflake.py — one VARCHAR table per V1 table,
same names). Every V3 row carries a uuid5 of its V1 table + id and is posted with match_on=uuid, so a re-run updates
instead of duplicating. All rows are written in facility 20 (X-Facility-Id: V3 ignores facility_id in the payload).

Steps (run in STEPS order):
  users / user_profiles   users + users_user_profiles → core/users (random password, reset enforced,
                          "<user>@medhanit.migrated.medhanit-<id>.invalid" so no mail goes out) + profiles.
  insurance               settings_insurance / settings_schemes → core insurance companies / schemes.
  patients                reception_patients (+ next of kin) → reception/patient (patient_no MDH-<V1 number>) and
                          reception/patient_next_of_kin. Refuses to post names that are still encrypted.
  patient_insurance       reception_patient_schemes → reception/patient_insurance.
  catalogue / procedure_prices   procedure categories + procedures (code MDH-<V1 code or id>) + cash / insurance prices.
  lab_templates           labtest categories, subprocedures, templates_lab (section title = the V1 haemogram title's
                          name), reference ranges → what V3 renders a lab template from (template_labs + subprocedures +
                          bio_reference_ranges).
  visits                  evaluation_visits → evaluation/visits (unique_id MDH-<id>; doctor = the visit's doctor queue).
  visit_destinations      evaluation_visit_destinations → evaluation/visit_destinations, linked to the org's service
                          destination of the same name (Doctor → Doctor Consultation).
  vitals / doctor_notes / nursing_notes   → evaluation/vitals, evaluation/doctor_notes, inpatient/note (visit only).
  investigations          evaluation_investigations → evaluation/investigations.
  lab_results             evaluation_investigation_results → investigation_results (published: V1 never sets its
                          status) + one investigation_result_details row per sub-test value in its `results` JSON.
  products / stock        inventory categories, units, products (+ latest price) → inventory; stock on hand → one
                          opening batch + stock row each in a "Main Store" (V3 dispenses only from batches).
  payments / payment_details / invoices   finance_evaluation_payments (+ cash / M-Pesa splits) → finance/evaluation_payment
                          (+ details); finance_patient_invoices → finance/invoice.

    python medhanit_to_v3.py --steps users catalogue --limit 3 --dry-run
    python medhanit_to_v3.py --steps users catalogue --limit 3
    python medhanit_to_v3.py --patients 62709 62396           # canary: these V1 patients and everything on them
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
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from itertools import islice
from pathlib import Path

sys.path.insert(0, "/opt/airflow/dags")
from eagle_to_v3 import (MODE_COLUMN, TITLE_CODES, clean, day, gender, num, patient_login, pay_mode, phone, stamp,
                         strip_html, text_join, vital, words)
from medhanit_v1_to_snowflake import looks_encrypted

log = logging.getLogger("medhanit_to_v3")

SF = "HOSPITALS.MEDHANIT_V1"
NS = uuid.UUID("3c7d9e21-6b84-4f0a-a5d2-8e1f4b6c9a07")      # never change: rows are matched by it
CHUNK = 20_000
TARGET = "medhanit"
# models with a facility_id column (gateway describe, 2026-10-29)
FACILITY_MODELS = {"patient", "patient_insurance", "patient_next_of_kin", "visits", "visit_destinations", "vitals",
                   "doctor_notes", "investigations", "procedure_prices", "evaluation_payment", "evaluation_payment_detail",
                   "invoice", "note", "store", "stock", "batch", "product", "product_category", "unit"}
STEPS = ("users", "user_profiles", "insurance", "patients", "patient_insurance", "catalogue", "procedure_prices",
         "lab_templates", "visits", "visit_destinations", "vitals", "doctor_notes", "nursing_notes", "investigations",
         "lab_results", "products", "stock", "payments", "payment_details", "invoices")
# V1 visit-destination department → the org's service destination (by name; V3 seeds "Doctor Consultation")
DESTINATION_ALIASES = {"doctor": "doctor consultation", "consultation": "doctor consultation", "lab": "laboratory",
                       "nurse": "nursing", "x-ray": "radiology", "xray": "radiology"}
ADOPTED = "already in V3 (left as is)"
# targets that already hold an earlier load of this V1 (prod org 71: patients, visits, notes, orders, results up to
# 2026-10-06, with uuids of its own) — run with --adopt there, so only what is missing is posted
ADOPT_TARGETS = {"medhanit_prod"}


# ── pure helpers (tests/test_medhanit_to_v3.py) ─────────────────────────────
def gid(table: str, key) -> str:
    """Stable uuid of a V1 row (key = its id)."""
    return str(uuid.uuid5(NS, f"medhanit/{table}/{str(key).strip()}"))


def as_int(v) -> int | None:
    x = num(v)
    return int(x) if x is not None else None


def flag(v) -> bool:
    return words(v) in ("1", "true", "yes", "active", "on")


def results_pairs(raw) -> list[tuple[int, str]]:
    """V1 investigation_results.results: '[[843,"Loose, mucoid"],[867,"<p>No ova</p>"]]' → [(843, 'Loose, mucoid'), …]
    (sub-test procedure id, value as text). A dict {"843": "…"} and a plain string (one value, no sub-test) also occur."""
    if raw is None:
        return []
    data = raw
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except ValueError:
            return [(0, raw)] if clean(raw) else []
    if isinstance(data, dict):
        data = list(data.items())
    out = []
    for item in data if isinstance(data, list) else []:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            sid = as_int(item[0])
            val = item[1] if not isinstance(item[1], (dict, list)) else json.dumps(item[1])
            out.append((sid or 0, "" if val is None else str(val)))
    return out


def range_text(lo, hi) -> str | None:
    lo, hi = clean(lo), clean(hi)
    if lo and hi:
        return f"{lo} - {hi}"
    return f"> {lo}" if lo else f"< {hi}" if hi else None


def result_flag(value, lo, hi) -> str | None:
    v, l, h = num(value), num(lo), num(hi)
    if v is None or not re.match(r"^\s*-?\d+(\.\d+)?\s*$", clean(value) or ""):
        return None
    return "L" if l is not None and v < l else "H" if h is not None and v > h else None


def patient_no(r: dict, dup_numbers: set[str]) -> str:
    """MDH-<V1 patient number>; a number two V1 patients share gets the row id too."""
    no = clean(r.get("patient_no")) or f"ID{r['id']}"
    return f"MDH-{no}" if no not in dup_numbers else f"MDH-{no}-{r['id']}"


def prior_patient_no(r: dict) -> str:
    """How the earlier V1 load into prod org 71 numbered a patient: MH-<V1 number>, MH-V2-<V1 id> without one."""
    no = clean(r.get("patient_no"))
    return f"MH-{no}" if no else f"MH-V2-{r['id']}"


def eat_stamp(v3_time) -> str | None:
    """A V3 timestamp (UTC, '2026-08-14T15:22:20.000000Z') as V1 wrote it (Nairobi time, '2026-08-14 18:22:20')."""
    if not clean(v3_time):
        return None
    t = datetime.fromisoformat(str(v3_time).replace("Z", "+00:00"))
    return (t.astimezone(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")


def name_key(v) -> str:
    return re.sub(r"[^a-z0-9]", "", (clean(v) or "").lower())


def user_payload(r: dict) -> dict:
    uname = re.sub(r"[^a-z0-9._-]", "", (clean(r.get("username")) or f"user{r['id']}").lower()) or f"user{r['id']}"
    return {"uuid": gid("users", r["id"]), "username": f"medhanit.{uname}"[:60],
            "email": f"{uname}@medhanit.migrated.medhanit-{r['id']}.invalid", "password": str(uuid.uuid4()),
            "enforce_password_reset": 1, "active": 1 if flag(r.get("active")) else 0,
            "employee_number": f"MDH-U-{r['id']}", "created_at": stamp(r.get("created_at"))}


def user_profile_payload(r: dict, user_id: int) -> dict:
    title = as_int(r.get("title"))
    first = clean(r.get("first_name")) or clean(r.get("last_name")) or "Staff"
    return {"uuid": gid("users_user_profiles", r["user_id"]), "user_id": user_id,
            **({"title": title} if title in TITLE_CODES.values() else {}),
            "first_name": first[:100], "middle_name": (clean(r.get("middle_name")) or "")[:100] or None,
            "last_name": (clean(r.get("last_name")) or first)[:100], "job_description": clean(r.get("job_description")),
            "phone": phone(r.get("phone")), "mpdb": clean(r.get("mpdb")), "pin": clean(r.get("pin")),
            "staff_number": f"MDH-U-{r['user_id']}"}


def patient_payload(r: dict, number: str, kin: dict | None = None) -> dict:
    first = clean(r.get("first_name")) or clean(r.get("last_name")) or "Unknown"
    last = clean(r.get("last_name")) or first
    email = clean(r.get("email"))
    when = stamp(r.get("created_at"))
    kin = kin or {}
    kin_name = " ".join(x for x in (clean(kin.get("first_name")), clean(kin.get("middle_name")), clean(kin.get("last_name"))) if x)
    age = " ".join(x for x in (clean(r.get("age")), clean(r.get("age_in"))) if x) if not day(r.get("dob")) else None
    return {"uuid": gid("reception_patients", r["id"]), "patient_no": number,
            "first_name": first[:100], "middle_name": (clean(r.get("middle_name")) or "")[:100] or None, "last_name": last[:100],
            "dob": day(r.get("dob")), "gender": gender(r.get("sex")), "id_no": clean(r.get("id_no")),
            "id_type": "national_id" if clean(r.get("id_no")) else None,
            "mobile": phone(r.get("mobile")), "alt_phone": phone(r.get("telephone")) or phone(r.get("alt_number")),
            "email": email if email and "@" in email else None, "address": clean(r.get("address")),
            "city": clean(r.get("town")), "postal_code": clean(r.get("post_code")),
            "emergency_contact_name": kin_name or None, "emergency_contact_phone": phone(kin.get("mobile")),
            "emergency_contact_relationship": clean(kin.get("relationship")),
            "insurance_status": "uninsured", "registration_date": (when or "")[:10] or None,
            "status": "active" if str(r.get("status") or "1") == "1" else "inactive",
            "notes": text_join(("V1 patient number", r.get("patient_no")), ("Age at registration", age),
                               ("External institution", r.get("external_institution"))),
            "created_at": when, "updated_at": stamp(r.get("updated_at")) or when,
            "deleted_at": stamp(r.get("deleted_at"))}


def visit_payload(r: dict, patient_id: int, facility: int, doctor: int | None, scheme: int | None) -> dict:
    when = stamp(r.get("created_at"))
    return {"uuid": gid("evaluation_visits", r["id"]), "patient": patient_id, "clinic": facility, "inpatient": 0,
            "payment_mode": "insurance" if words(r.get("payment_mode")) == "insurance" else "cash", "scheme": scheme,
            "type_of_visit": "OP Visit", "status": "completed", "unique_id": f"MDH-{r['id']}", "user": doctor,
            "created_at": when, "updated_at": stamp(r.get("updated_at")) or when, "deleted_at": stamp(r.get("deleted_at"))}


def destination_payload(r: dict, visit_id: int, dest: int | None, dest_name, user_id: int | None) -> dict:
    """Same shape as v3_org_corrections.vd_payload (the V2 load)."""
    out = flag(r.get("checkout"))
    fin = stamp(r.get("finish_at"))
    return {"uuid": gid("evaluation_visit_destinations", r["id"]), "visit_id": visit_id, "user_id": user_id,
            "department": clean(r.get("department")) or dest_name, "checkout": 1 if out else 0,
            "begin_at": stamp(r.get("begin_at")), "finish_at": fin, "status": "completed" if (fin or out) else "pending",
            "destination_id": dest, "created_at": stamp(r.get("created_at")), "updated_at": stamp(r.get("updated_at")),
            "deleted_at": stamp(r.get("deleted_at"))}


def vitals_payload(r: dict, visit_id: int, patient_id: int, user_id: int | None) -> dict:
    dropped: list[str] = []
    sys_, dia = vital("systolic_bp", r.get("bp_systolic"), dropped), vital("diastolic_bp", r.get("bp_diastolic"), dropped)
    pulse, resp = vital("pulse", r.get("pulse"), dropped), vital("respiratory_rate", r.get("respiration"), dropped)
    height, weight = vital("height", r.get("height"), dropped), vital("weight", r.get("weight"), dropped)
    temp = vital("temperature", r.get("temperature"), dropped)
    bmi = round(weight / (height / 100) ** 2, 1) if weight and height and height > 50 else None
    spo2 = num(r.get("oxygen"))
    when = stamp(r.get("created_at"))
    notes = "\n".join(x for x in (clean(r.get("nurse_notes")),
                                  f"Out-of-range in V1 (not loaded as numbers): {', '.join(dropped)}" if dropped else None) if x)
    return {"uuid": gid("evaluation_vitals", r["id"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            "height": height, "weight": weight, "temperature": temp, "temperature_location": clean(r.get("temperature_location")),
            "systolic_bp": int(sys_) if sys_ else None, "diastolic_bp": int(dia) if dia else None,
            "bp_systolic": int(sys_) if sys_ else None, "bp_diastolic": int(dia) if dia else None,
            "blood_pressure": f"{int(sys_)}/{int(dia)}" if sys_ and dia else None,
            "pulse": int(pulse) if pulse else None, "respiratory_rate": int(resp) if resp else None, "bmi": bmi,
            "oxygen_saturation": int(spo2) if spo2 and 0 < spo2 <= 100 else None,
            "waist": num(r.get("waist")), "hip": num(r.get("hip")), "blood_sugar": num(r.get("blood_sugar")),
            "blood_sugar_units": clean(r.get("blood_sugar_units")) if num(r.get("blood_sugar")) is not None else None,
            "allergies": clean(r.get("allergies")), "chronic_illnesses": clean(r.get("chronic_illnesses")),
            "lmp": day(r.get("lmp")), "notes": notes or None, "nurse_notes": clean(r.get("nurse_notes")),
            "recorded_by": user_id, "user": user_id, "recorded_at": when,
            "created_at": when, "updated_at": stamp(r.get("updated_at")) or when}


def doctor_note_payload(r: dict, visit_id: int, patient_id: int, user_id: int | None) -> dict | None:
    fields = {k: strip_html(r.get(k)) for k in ("presenting_complaints", "past_medical_history", "examination", "diagnosis",
                                                "investigations", "treatment_plan")}
    extra = [clean(r.get(f"d{i}")) for i in range(1, 9)]
    office = strip_html(r.get("office_treatment"))
    if not any(fields.values()) and not any(extra) and not office:
        return None
    when = stamp(r.get("created_at"))
    secondary = {f"secondary_diagnosis_{i}": v for i, v in enumerate((x for x in extra if x), 1)}
    return {"uuid": gid("evaluation_doctor_notes", r["id"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            **fields, "history_of_present_illness": fields["presenting_complaints"], "primary_diagnosis": fields["diagnosis"],
            **secondary, "remarks": text_join(("Office treatment", office)), "doctor_id": user_id, "user": user_id,
            "status": "final", "created_at": when, "updated_at": stamp(r.get("updated_at")) or when}


def nursing_note_payload(r: dict, visit_id: int, user_id: int | None) -> dict | None:
    content = text_join(("Notes", strip_html(r.get("notes"))), ("Treatment given", strip_html(r.get("prescription"))))
    when = stamp(r.get("created_at"))
    if not content or not when:
        return None
    return {"uuid": gid("evaluation_nursing_notes", r["id"]), "visit_id": visit_id, "note_type": "nursing_note",
            "note_date": when, "title": "Nursing note", "note_content": content, "created_by": user_id,
            "created_at": when, "updated_at": stamp(r.get("updated_at")) or when}


def procedure_codes(rows: list[dict]) -> dict[str, str]:
    """V1 procedure id → its V3 code, unique in the org (V3: procedures_org_code_unique, case-insensitive): MDH-<V1 code>,
    else MDH-P<id> — and a generated code that is also someone's real code (V1 has a procedure coded 'P1') gets -<n>."""
    out, taken = {}, set()
    real = Counter((clean(r.get("code")) or "").upper() for r in rows)
    for r in rows:
        c = clean(r.get("code"))
        if c and real[c.upper()] == 1:
            out[str(r["id"])] = f"MDH-{c}"[:100]
            taken.add(out[str(r["id"])].upper())
    for r in rows:
        if str(r["id"]) in out:
            continue
        code, n = f"MDH-P{r['id']}", 1
        while code.upper() in taken:
            n += 1
            code = f"MDH-P{r['id']}-{n}"
        out[str(r["id"])] = code
        taken.add(code.upper())
    return out


def procedure_payload(r: dict, category: int | None, code: str) -> dict:
    return {"uuid": gid("evaluation_procedures", r["id"]), "code": code, "name": (clean(r.get("name")) or code)[:255],
            "category": category, "gender": clean(r.get("gender")), "description": strip_html(r.get("description")),
            "template": clean(r.get("template")), "active": 1 if str(r.get("status") or "1") == "1" else 0,
            "sensitivity": 1 if flag(r.get("sensitivity")) else 0, "precharge": 1 if flag(r.get("precharge")) else 0}


def price_payload(r: dict, procedure_id: int, facility: int, facility_name) -> dict:
    cash = num(r.get("cash_charge")) or 0.0
    ins = num(r.get("insurance_charge"))
    return {"uuid": gid("procedure_price", f"{r['id']}|{facility}"), "evaluation_procedure_id": procedure_id,
            "facility_id": facility, "facility_name": facility_name, "cash_charge": cash,
            "insurance_charge": ins if ins is not None else cash, "charge_insurance": 1 if flag(r.get("charge_insurance")) else 0,
            "precharge": 1 if flag(r.get("precharge")) else 0, "status": 1}


SUBPROCEDURE_COPY = ("lab_result_type", "result_type_details", "lab_result_options", "gender", "lab_min_range",
                     "lab_max_range", "_0_3d_minrange", "_0_3d_maxrange", "_4_30d_minrange", "_4_30d_maxrange",
                     "_1_24m_minrange", "_1_24m_maxrange", "_25_60m_minrange", "_25_60m_maxrange", "_5_19y_minrange",
                     "_5_19y_maxrange", "adult_minrange", "adult_maxrange", "lab_default", "lab_ordered_independently",
                     "lab_multiple_orders_allowed", "turn_around_time", "method")
RANGE_COPY = ("lab_min_range", "lab_max_range", "_0_3d_minrange", "_0_3d_maxrange", "_4_30d_minrange", "_4_30d_maxrange",
              "_1_24m_minrange", "_1_24m_maxrange", "_25_60m_minrange", "_25_60m_maxrange", "_5_19y_minrange",
              "_5_19y_maxrange", "adult_minrange", "adult_maxrange")


def subprocedure_payload(r: dict, procedure: int, parent: int | None, category: int | None, unit, title) -> dict:
    d = {"uuid": gid("evaluation_subprocedures", r["id"]), "procedure": procedure, "parent": parent, "category": category,
         "units": unit, "title": title, "created_at": stamp(r.get("created_at")), "updated_at": stamp(r.get("updated_at"))}
    for k in SUBPROCEDURE_COPY:
        if clean(r.get(k)) is not None:
            d[k] = clean(r.get(k))
    return d


def template_lab_payload(r: dict, procedure: int, subtest: int, title) -> dict:
    d = {"uuid": gid("evaluation_templates_lab", r["id"]), "procedure_id": procedure, "subtest_id": subtest, "title": title,
         "sort_order": as_int(r.get("sort_order")), "alias": clean(r.get("alias")),
         "created_at": stamp(r.get("created_at")), "updated_at": stamp(r.get("updated_at"))}
    for k in RANGE_COPY:
        if clean(r.get(k)) is not None:
            d[k] = clean(r.get(k))
    return d


def reference_range_payload(r: dict, procedure: int, test_name) -> dict:
    lo, hi = clean(r.get("lower")), clean(r.get("upper"))
    d = {"uuid": gid("evaluation_reference_range", r["id"]), "procedure_id": procedure, "test_name": test_name,
         "type": clean(r.get("type")) or "range", "gender": clean(r.get("gender")), "flag": clean(r.get("flag")),
         "lower": lo, "upper": hi, "lg_type": clean(r.get("lg_type")), "lg_value": clean(r.get("lg_value")),
         "description": text_join(("Age", r.get("age")), ("Other", r.get("other_type"))),
         "created_at": stamp(r.get("created_at")), "updated_at": stamp(r.get("updated_at"))}
    isnum = lambda v: v is None or re.fullmatch(r"\s*-?\d+(\.\d+)?\s*", v)
    if not (isnum(lo) and isnum(hi)):            # words in a decimal column: shown as text, the way V1 shows it
        d["text_reference"] = " – ".join(x for x in (lo, hi) if x)
        d["type"], d["lower"], d["upper"] = "text", None, None
    return {k: v for k, v in d.items() if v is not None}


def investigation_payload(r: dict, visit_id: int, patient_id: int, procedure: int | None, user_id: int | None) -> dict:
    p, q = num(r.get("price")) or 0.0, num(r.get("quantity")) or 1
    a = num(r.get("amount"))
    when = stamp(r.get("created_at"))
    return {"uuid": gid("evaluation_investigations", r["id"]), "visit": visit_id, "patient_id": patient_id,
            "type": clean(r.get("type")) or "laboratory", "procedure": procedure, "quantity": int(q) if q >= 1 else 1,
            "price": p, "discount": num(r.get("discount")) or 0, "amount": a if a is not None else round(p * q, 2),
            "user": user_id, "instructions": strip_html(r.get("instructions")),
            "ordered": 1 if flag(r.get("ordered")) else 0, "invoiced": 1 if flag(r.get("invoiced")) else 0,
            "created_at": when, "updated_at": stamp(r.get("updated_at")) or when}


def product_payload(r: dict, category: int, unit: int, price: dict | None) -> dict:
    price = price or {}
    sell = num(price.get("selling")) or num(price.get("price"))
    return {"uuid": gid("inventory_products", r["id"]), "generic_name": (clean(r.get("name")) or f"Product {r['id']}")[:255],
            "code": f"MDH-{r['id']}", "item_no": str(r["id"]), "description": strip_html(r.get("description")),
            "strength": clean(r.get("strength")), "formulation": clean(r.get("formulation")),
            "label_type": clean(r.get("label_type")), "category_id": category, "unit_id": unit,
            "reorder_level": as_int(r.get("reorder_level")), "selling_price": sell,
            "insurance_price": num(price.get("ins_price")) or sell, "cost_price": num(price.get("price")),
            "is_active": 1, "requires_prescription": 0 if flag(r.get("consumable")) else 1, "track_batch": 1, "track_expiry": 1,
            "created_at": stamp(r.get("created_at")), "updated_at": stamp(r.get("updated_at"))}


def payment_payload(r: dict, modes: dict[str, float], patient: tuple | None, visit_id, cashier: int | None,
                    cashier_name) -> dict | None:
    a = num(r.get("amount"))
    when = stamp(r.get("created_at"))
    if a is None or not when:
        return None
    cols = {c: 0.0 for c in MODE_COLUMN.values()}
    for mode, amt in modes.items():
        if mode in MODE_COLUMN:
            cols[MODE_COLUMN[mode]] += amt
    mode = max(modes, key=modes.get) if modes else "cash"
    if not modes:
        cols["cash_amount"] = a
    return {"uuid": gid("finance_evaluation_payments", r["id"]), "receipt": f"MDH-{clean(r.get('receipt')) or r['id']}"[:255],
            "receipt_prefix": "MDH", "patient": patient[0] if patient else None, "patient_name": patient[1] if patient else None,
            "patient_no": patient[2] if patient else None, "user": cashier, "user_name": cashier_name, "visit": visit_id,
            "deposit": 1 if flag(r.get("deposit")) else 0, "amount": a, **cols, "total_cash_payment": a,
            "total_amount_was": a, "discount": 0, "change": 0, "vat_amount": 0, "payment_mode": mode,
            "receipt_date": when[:10], "created_at": when, "updated_at": stamp(r.get("updated_at")) or when}


# ── run ─────────────────────────────────────────────────────────────────────
class Run:
    def __init__(self, target: str, dry_run: bool, limit: int, workers: int, only_missing: bool = False,
                 adopt: bool = False):
        from pipelines_common import use_pipelines_dir
        use_pipelines_dir([target])
        from v3_to_v3_client import V3Client
        import snowflake_to_v3_migration as s2v3
        self.target, self.dry_run, self.limit, self.workers, self.only_missing = target, dry_run, limit, workers, only_missing
        self.adopt = adopt
        self.c = V3Client(target)
        if not self.c.facility_id:
            raise RuntimeError(f"AFYA_{target.upper()}_FACILITY_ID is not set (.env, or Extra \"facility_id\" on connection "
                               f"afya_v3_{target}) — every row is written in that facility")
        self.c.send_facility_header = True       # V3 files an insert under the login's facility, not the payload's
        login = self.c.login
        self.c.login = lambda: patient_login(login, self.target)
        self.c._token = (self.c.login(), time.time())
        self.fac = self.c.facility_id
        self.sf = s2v3._snowflake_connect()
        self.today = date.today().isoformat()
        self._cache: dict = {}
        self.failures: dict[str, list] = defaultdict(list)
        self.only_patients: set[str] | None = None             # --patients: a canary on these V1 patient ids

    # ── source ──
    def q(self, sql: str):
        cur = self.sf.cursor()
        cur.execute(sql)
        cols = [c[0].lower() for c in cur.description]
        while True:
            batch = cur.fetchmany(50_000)
            if not batch:
                return
            for r in batch:
                yield dict(zip(cols, r))

    def rows(self, table: str, where: str = "") -> list[dict]:
        """A V1 table, one row per id (oldest first). A table the extract hasn't made yet reads as empty."""
        key = ("rows", table, where)
        if key not in self._cache:
            have = {r["name"].lower() for r in self.q(f"SHOW TABLES LIKE '{table.upper()}' IN SCHEMA {SF}")}
            if table.lower() not in have:
                log.warning("%s.%s isn't in Snowflake yet (run medhanit_v1_to_snowflake.py)", SF, table.upper())
                self._cache[key] = []
            else:
                cols = [r["column_name"].lower() for r in self.q(f"SHOW COLUMNS IN TABLE {SF}.\"{table.upper()}\"")]
                order = ' ORDER BY TRY_TO_NUMBER("ID")' if "id" in cols else ""
                self._cache[key] = list(self.q(f'SELECT * FROM {SF}."{table.upper()}" {where}{order}'))
        return self._cache[key]

    def by_id(self, table: str) -> dict[str, dict]:
        key = ("by_id", table)
        if key not in self._cache:
            self._cache[key] = {str(r["id"]): r for r in self.rows(table)}
        return self._cache[key]

    # ── V3 lookups ──
    def ids_by_uuid(self, svc: str, alias: str, fresh: bool = False) -> dict[str, int]:
        key = ("uuid", svc, alias)
        if fresh or key not in self._cache:
            ids = {str(r.get("uuid")).lower(): r["id"] for r in self.c.read_all(svc, alias, workers=8) if r.get("uuid")}
            for u, i in self.adopted(svc, alias).items():
                ids.setdefault(u, i)
            self._cache[key] = ids
        return self._cache[key]

    def v3rows(self, svc: str, alias: str) -> list[dict]:
        """The target's rows of a model (cached; --adopt matching reads them once)."""
        key = ("v3rows", svc, alias)
        if key not in self._cache:
            self._cache[key] = [r for r in self.c.read_all(svc, alias, workers=8)
                                if r.get("organization_id") in (None, self.c.organization_id)]
        return self._cache[key]

    def adopted(self, svc: str, alias: str) -> dict[str, int]:
        """--adopt: our uuid → the id of the row an earlier load of this V1 left in the target (prod org 71 got one up to
        2026-10-06, with its own uuids). Those rows are linked to and never re-posted; see the _adopt_* matchers."""
        if not self.adopt:
            return {}
        key = ("adopt", svc, alias)
        if key not in self._cache:
            fn = getattr(self, f"_adopt_{alias}", None)
            self._cache[key] = fn() if fn else {}
            if fn:
                log.info("  adopt %s/%s: %d V1 rows matched to rows already in V3", svc, alias, len(self._cache[key]))
        return self._cache[key]

    # --adopt matchers: what the earlier load left, and how its rows point back to V1. Ids that nothing looks up later
    # (notes, queue rows…) map to 0 — they only mark the V1 row as already loaded.
    def _adopt_users(self) -> dict[str, int]:
        by = {}
        for u in self.v3rows("core", "users"):
            for k in (u.get("username"), u.get("email")):
                if words(k):
                    by.setdefault(words(k), u["id"])
        return {gid("users", r["id"]): i for r in self.rows("users")
                if (i := by.get(words(r.get("username"))) or by.get(words(r.get("email")) or "-"))}

    def _adopt_user_profile(self) -> dict[str, int]:
        users = self.adopted("core", "users")
        return {gid("users_user_profiles", r["user_id"]): 0 for r in self.rows("users_user_profiles")
                if gid("users", r["user_id"]) in users}

    def _adopt_insurance_companies(self) -> dict[str, int]:
        v3 = {name_key(c.get("name")): c["id"] for c in self.v3rows("core", "insurance_companies")}
        first = lambda n: (words(n) or "").split(" ")[0]
        v3_first = Counter(first(c.get("name")) for c in self.v3rows("core", "insurance_companies"))
        v1_first = Counter(first(r.get("name")) for r in self.rows("settings_insurance"))
        by_first = {first(c.get("name")): c["id"] for c in self.v3rows("core", "insurance_companies")}
        out = {}
        for r in self.rows("settings_insurance"):     # same name, or one insurer brand both sides ('AAR HEALTH CARE' = 'AAR Insurance')
            f = first(r.get("name"))
            i = v3.get(name_key(r.get("name"))) or (by_first.get(f) if v3_first[f] == 1 and v1_first[f] == 1 and len(f) > 2 else None)
            if i:
                out[gid("settings_insurance", r["id"])] = i
        return out

    def _adopt_insurance_schemes(self) -> dict[str, int]:
        comp = self.adopted("core", "insurance_companies")
        v3 = {(s.get("company_id"), name_key(s.get("name"))): s["id"] for s in self.v3rows("core", "insurance_schemes")}
        return {gid("settings_schemes", r["id"]): i for r in self.rows("settings_schemes")
                if (i := v3.get((comp.get(gid("settings_insurance", as_int(r.get("company")))), name_key(r.get("name")))))}

    def _adopt_patient(self) -> dict[str, int]:
        """The earlier load numbered patients MH-<V1 number>, or MH-V2-<V1 id> when V1 has none."""
        v3 = {str(p.get("patient_no") or "").upper(): p for p in self.v3rows("reception", "patient")}
        out, odd = {}, 0
        for r in self.rows("reception_patients"):
            p = v3.get(prior_patient_no(r).upper())
            if p:
                out[gid("reception_patients", r["id"])] = p["id"]
                odd += name_key(p.get("first_name")) != name_key(r.get("first_name")) and \
                    name_key(p.get("last_name")) != name_key(r.get("last_name"))
        if odd:
            log.warning("  adopt patients: %d matched by number have neither first nor last name in common", odd)
        return out

    def _adopt_visits(self) -> dict[str, int]:
        v3 = {str(v.get("unique_id") or ""): v["id"] for v in self.v3rows("evaluation", "visits")}
        return {gid("evaluation_visits", r["id"]): v3[f"V2-{r['id']}"] for r in self.rows("evaluation_visits")
                if f"V2-{r['id']}" in v3}

    def by_visit(self, table: str, svc: str, alias: str) -> dict[str, int]:
        """V1 rows of a visit the earlier load brought over together with rows of this model: that visit's rows are
        taken as loaded (the earlier load gave them uuids of its own). A loaded visit without any gets V1's."""
        uuid_of = {v["id"]: str(v.get("uuid")).lower() for v in self.v3rows("evaluation", "visits")}
        visits = {i for u, i in self.adopted("evaluation", "visits").items() if uuid_of.get(i) != u}   # not ones this load made
        have = {r.get("visit_id") or r.get("visit") for r in self.v3rows(svc, alias)}
        vmap = self.visit_map()
        return {gid(table, r["id"]): 0 for r in self.rows(table)
                if (v := vmap.get(str(as_int(r.get("visit"))))) and v[0] in visits and v[0] in have}

    def _adopt_visit_destinations(self) -> dict[str, int]:
        return self.by_visit("evaluation_visit_destinations", "evaluation", "visit_destinations")

    def _adopt_vitals(self) -> dict[str, int]:
        return self.by_visit("evaluation_vitals", "evaluation", "vitals")

    def _adopt_doctor_notes(self) -> dict[str, int]:
        return self.by_visit("evaluation_doctor_notes", "evaluation", "doctor_notes")

    def _adopt_note(self) -> dict[str, int]:
        return self.by_visit("evaluation_nursing_notes", "inpatient", "note")

    def _adopt_procedure_categories(self) -> dict[str, int]:
        v3 = {name_key(c.get("name")): c["id"] for c in self.v3rows("evaluation", "procedure_categories")}
        return {gid("evaluation_procedure_categories", r["id"]): v3[k] for r in self.rows("evaluation_procedure_categories")
                if (k := name_key(r.get("name"))) in v3}

    def _adopt_procedures(self) -> dict[str, int]:
        """Same name (an active one first): the earlier load ordered V1's tests against the org's own catalogue."""
        v3 = {}
        for p in sorted(self.v3rows("evaluation", "procedures"), key=lambda p: (not p.get("active"), p["id"])):
            v3.setdefault(name_key(p.get("name")), p["id"])
        return {gid("evaluation_procedures", r["id"]): v3[k] for r in self.rows("evaluation_procedures")
                if (k := name_key(r.get("name"))) in v3}

    def _adopt_procedure_prices(self) -> dict[str, int]:
        """The org prices its own tests: V1 prices go only on the tests this load adds."""
        procs = self.adopted("evaluation", "procedures")
        return {gid("procedure_price", f"{r['id']}|{self.fac}"): 0 for r in self.rows("evaluation_procedures")
                if gid("evaluation_procedures", r["id"]) in procs}

    def _adopt_lab_test_categories(self) -> dict[str, int]:
        v3 = {name_key(c.get("name")): c["id"] for c in self.v3rows("evaluation", "lab_test_categories")}
        return {gid("evaluation_labtest_categories", r["id"]): v3[k] for r in self.rows("evaluation_labtest_categories")
                if (k := name_key(r.get("name"))) in v3}

    def lab_setup(self, table: str, svc: str, alias: str, col: str, v1_col: str) -> dict[str, int]:
        """A test the org already set up (its own sub-test / template / range rows) keeps them: V1's are not added."""
        procs = self.adopted("evaluation", "procedures")
        have = {r.get(col) for r in self.v3rows(svc, alias)}
        return {gid(table, r["id"]): 0 for r in self.rows(table)
                if procs.get(gid("evaluation_procedures", as_int(r.get(v1_col)))) in have}

    def _adopt_subprocedures(self) -> dict[str, int]:
        return self.lab_setup("evaluation_subprocedures", "evaluation", "subprocedures", "procedure", "procedure")

    def _adopt_template_labs(self) -> dict[str, int]:
        return self.lab_setup("evaluation_templates_lab", "evaluation", "template_labs", "procedure_id", "procedure")

    def _adopt_bio_reference_ranges(self) -> dict[str, int]:
        return self.lab_setup("evaluation_reference_range", "evaluation", "bio_reference_ranges", "procedure_id", "procedure")

    def _adopt_investigations(self) -> dict[str, int]:
        """Per loaded visit, a V1 order is the V3 order with its timestamp and price (the earlier load kept both; its
        procedure ids are of a catalogue the org has since replaced, so the test can't be compared)."""
        v3 = defaultdict(list)
        for i in sorted(self.v3rows("evaluation", "investigations"), key=lambda i: i["id"]):
            v3[(i.get("visit"), eat_stamp(i.get("created_at")), as_int(i.get("price")) or 0)].append(i["id"])
        visits, vmap, out = set(self.adopted("evaluation", "visits").values()), self.visit_map(), {}
        for r in self.rows("evaluation_investigations"):
            v = vmap.get(str(as_int(r.get("visit"))))
            if v and v[0] in visits and (ids := v3.get((v[0], str(r.get("created_at") or "")[:19], as_int(r.get("price")) or 0))):
                out[gid("evaluation_investigations", r["id"])] = ids.pop(0)
        return out

    def _adopt_investigation_results(self) -> dict[str, int]:
        inv = self.adopted("evaluation", "investigations")
        v3 = defaultdict(list)
        for x in sorted(self.v3rows("evaluation", "investigation_results"), key=lambda x: x["id"]):
            v3[x.get("investigation_id")].append(x["id"])
        out = {}
        for r in self.rows("evaluation_investigation_results"):
            ids = v3.get(inv.get(gid("evaluation_investigations", as_int(r.get("investigation")))))
            if ids:
                out[gid("evaluation_investigation_results", r["id"])] = ids.pop(0)
        return out

    def _adopt_evaluation_payment(self) -> dict[str, int]:
        v3 = {str(p.get("receipt") or ""): p["id"] for p in self.v3rows("finance", "evaluation_payment")}
        return {gid("finance_evaluation_payments", r["id"]): v3[k] for r in self.rows("finance_evaluation_payments")
                if (k := clean(r.get("receipt")) or "-") in v3}

    def _adopt_evaluation_payment_detail(self) -> dict[str, int]:
        paid = self.adopted("finance", "evaluation_payment")
        return {gid("finance_evaluation_payment_details", d["id"]): 0 for d in self.rows("finance_evaluation_payment_details")
                if gid("finance_evaluation_payments", as_int(d.get("payment"))) in paid}

    def v3(self, svc: str, alias: str, table: str, v1_id) -> int | None:
        """The V3 id of a V1 row (by its uuid)."""
        return self.ids_by_uuid(svc, alias).get(gid(table, v1_id)) if clean(v1_id) and clean(v1_id) != "0" else None

    def user(self, v1_id) -> int | None:
        return self.v3("core", "users", "users", as_int(v1_id)) if as_int(v1_id) else None

    def patient(self, v1_id) -> int | None:
        return self.v3("reception", "patient", "reception_patients", as_int(v1_id)) if as_int(v1_id) else None

    def procedure(self, v1_id) -> int | None:
        return self.v3("evaluation", "procedures", "evaluation_procedures", as_int(v1_id)) if as_int(v1_id) else None

    def wanted(self, v1_patient) -> bool:
        return self.only_patients is None or str(as_int(v1_patient)) in self.only_patients

    def visit_map(self) -> dict[str, tuple[int, int]]:
        """V1 visit id → (V3 visit id, V3 patient id)."""
        if "visits" not in self._cache:
            patient = {v["id"]: v.get("patient") for v in self.c.read_all("evaluation", "visits", workers=8)}
            ids = self.ids_by_uuid("evaluation", "visits", fresh=True)
            self._cache["visits"] = {str(r["id"]): (ids[g], patient[ids[g]]) for r in self.rows("evaluation_visits")
                                     if (g := gid("evaluation_visits", r["id"])) in ids and ids[g] in patient}
        return self._cache["visits"]

    def visit(self, v1_id) -> tuple[int, int] | None:
        i = as_int(v1_id)
        if not i:
            return None
        v = self.by_id("evaluation_visits").get(str(i))
        if v is not None and not self.wanted(v.get("patient")):
            return None
        return self.visit_map().get(str(i))

    def facility_name(self) -> str | None:
        if "facility_name" not in self._cache:
            self._cache["facility_name"] = next((f.get("name") for f in self.c.read_all("core", "facilities")
                                                 if f.get("id") == self.fac), None)
        return self._cache["facility_name"]

    # ── posting ──
    def post(self, svc: str, alias: str, payloads) -> Counter:
        """Insert (match_on=uuid → update when it exists). payloads may be a generator: it is consumed in chunks."""
        res = Counter()
        have = set(self.ids_by_uuid(svc, alias, fresh=True)) if self.only_missing else None
        adopted = self.adopted(svc, alias)
        it = iter(payloads)
        sent = 0
        while True:
            chunk = list(islice(it, CHUNK))
            if not chunk:
                break
            if adopted:
                before = len(chunk)
                chunk = [p for p in chunk if p["uuid"] not in adopted]
                res[ADOPTED] += before - len(chunk)
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
        self._cache.pop(("uuid", svc, alias), None)
        return res

    def _post_chunk(self, svc: str, alias: str, payloads: list[dict]) -> Counter:
        res, lock = Counter(), threading.Lock()

        def one(p):
            data = {k: v for k, v in p.items() if v is not None}
            if alias in FACILITY_MODELS:
                data["facility_id"] = self.fac
            try:
                r = self.c.insert(svc, alias, data, match_on="uuid")
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
        with ThreadPoolExecutor(self.workers) as ex:
            list(ex.map(one, payloads))
        return res

    def ref(self, svc: str, alias: str, name: str, extra: dict | None = None, match: str = "name") -> int:
        """A reference row (category, unit, store…) by name: an existing one with the same name is reused, otherwise one
        is created with a stable uuid."""
        key = ("ref", svc, alias)
        if key not in self._cache:
            self._cache[key] = list(self.c.read_all(svc, alias))
        rows = self._cache[key]
        hit = next((r["id"] for r in rows if words(r.get(match)) == words(name)), None)
        if hit:
            return hit
        uid = gid(f"{alias}-ref", words(name))
        if self.dry_run:
            return 0
        r = self.c.insert(svc, alias, {"uuid": uid, "name": name, **(extra or {}),
                                       **({"facility_id": self.fac} if alias in FACILITY_MODELS else {})}, match_on="uuid")
        if not r.ok:
            raise RuntimeError(f"{svc}/{alias} '{name}': {r.status_code} {r.text[:300]}")
        new_id = ((r.json() or {}).get("data") or {}).get("id")
        if not new_id:
            self._cache[key] = list(self.c.read_all(svc, alias))
            return next(x["id"] for x in self._cache[key] if str(x.get("uuid")).lower() == uid)
        rows.append({"id": new_id, "uuid": uid, match: name})
        return new_id

    def per_visit(self, table: str, svc: str, alias: str, build, visit_col: str = "visit") -> dict:
        why = Counter()

        def gen():
            for r in self.rows(table):
                why["source"] += 1
                v = self.visit(r.get(visit_col))
                if v is None:
                    why["visit not in V3 (not on the V1 server, or not in this canary)"] += 1
                    continue
                p = build(r, *v)
                if p is None:
                    why["nothing to load"] += 1
                    continue
                yield p
        res = self.post(svc, alias, gen())
        return {**why, **res}

    # ── staff ──
    def users(self) -> dict:
        src = [r for r in self.rows("users") if words(r.get("username")) != "extract_bot"]     # our gateway login
        return {"source": len(src), **self.post("core", "users", (user_payload(r) for r in src))}

    def user_profiles(self) -> dict:
        ids, src = self.ids_by_uuid("core", "users", fresh=True), self.rows("users_user_profiles")
        pl = [user_profile_payload(r, ids[gid("users", r["user_id"])]) for r in src if gid("users", r["user_id"]) in ids]
        return {"source": len(src), "users_in_v3": len(pl), **self.post("core", "user_profile", pl)}

    # ── insurance ──
    def insurance(self) -> dict:
        from gyno_to_v3 import scheme_type
        comp = [{"uuid": gid("settings_insurance", r["id"]), "name": (clean(r.get("name")) or f"Insurer {r['id']}")[:255],
                 "code": f"MDH-INS-{r['id']}", "address": clean(r.get("address")), "postal_code": clean(r.get("post_code")),
                 "city": clean(r.get("town")), "street": clean(r.get("street")), "building": clean(r.get("building")),
                 "phone": phone(r.get("telephone")), "mobile_number": phone(r.get("mobile")), "fax": clean(r.get("fax")),
                 "email": clean(r.get("email")) if "@" in (clean(r.get("email")) or "") else None,
                 "status": "inactive" if clean(r.get("deleted_at")) else "active", "created_at": stamp(r.get("created_at"))}
                for r in self.rows("settings_insurance")]
        res = {"companies": dict(self.post("core", "insurance_companies", comp))}
        cid = self.ids_by_uuid("core", "insurance_companies", fresh=True)
        why = Counter()
        sch = []
        for r in self.rows("settings_schemes"):
            company = cid.get(gid("settings_insurance", as_int(r.get("company"))))
            if not company:
                why["scheme's insurer not in V3"] += 1
                continue
            sch.append({"uuid": gid("settings_schemes", r["id"]), "company_id": company,
                        "name": (clean(r.get("name")) or f"Scheme {r['id']}")[:255], "code": f"MDH-SCH-{r['id']}",
                        "type": scheme_type(r.get("name")), "amount": num(r.get("amount")), "attention": clean(r.get("attention")),
                        "smart_enabled": 1 if flag(r.get("smart")) else 0, "effective_date": day(r.get("effective_date")),
                        "expiry_date": day(r.get("expiry_date")), "status": "inactive" if clean(r.get("deleted_at")) else "active",
                        "notes": text_join(("V1 scheme type", r.get("type"))), "created_at": stamp(r.get("created_at"))})
        res["schemes"] = dict(self.post("core", "insurance_schemes", sch))
        return {**why, **res}

    def scheme(self, v1_scheme_link) -> int | None:
        """V1 visits.scheme / patient_schemes.scheme are V1 settings_schemes ids."""
        return self.v3("core", "insurance_schemes", "settings_schemes", as_int(v1_scheme_link)) if as_int(v1_scheme_link) else None

    # ── patients ──
    def patients(self) -> dict:
        if not self.rows("reception_patients"):
            # the extract stops on the first page while V1's gateway can't decrypt patients: nothing to load from
            raise RuntimeError("no V1 patients in Snowflake (HOSPITALS.MEDHANIT_V1.RECEPTION_PATIENTS is empty) — the "
                               "extract can't decrypt them until the V1 gateway is fixed; then run "
                               "medhanit_v1_to_snowflake.py --tables reception_patients reception_patients_nok")
        src = [r for r in self.rows("reception_patients") if self.wanted(r["id"])]
        if looks_encrypted(src, ("first_name", "last_name", "mobile")):
            raise RuntimeError("reception_patients in Snowflake still holds encrypted names — re-extract once the V1 "
                               "gateway decrypts them (medhanit_v1_to_snowflake.py --tables reception_patients "
                               "reception_patients_nok --full)")
        counts = Counter(clean(r.get("patient_no")) for r in self.rows("reception_patients"))
        dups = {n for n, k in counts.items() if n and k > 1}
        kin = {str(as_int(k.get("patient"))): k for k in self.rows("reception_patients_nok")}
        number = (lambda r: prior_patient_no(r) if clean(r.get("patient_no")) not in dups else f"MH-V2-{r['id']}") \
            if self.adopt else (lambda r: patient_no(r, dups))      # --adopt: numbered as the earlier load did
        res = {"source": len(src), "patient numbers shared by two V1 patients": len(dups),
               "patients": dict(self.post("reception", "patient",
                                          (patient_payload(r, number(r), kin.get(str(r["id"]))) for r in src)))}
        why = Counter()

        def nok():
            for k in self.rows("reception_patients_nok"):
                pid = self.patient(k.get("patient")) if self.wanted(k.get("patient")) else None
                if not pid:
                    why["next of kin: patient not in V3"] += 1
                    continue
                first = clean(k.get("first_name")) or clean(k.get("last_name"))
                if not first:
                    continue
                yield {"uuid": gid("reception_patients_nok", as_int(k["patient"])), "patient_id": pid, "first_name": first[:100],
                       "middle_name": clean(k.get("middle_name")), "last_name": clean(k.get("last_name")) or first,
                       "relationship": clean(k.get("relationship")), "mobile": phone(k.get("mobile")),
                       "is_primary": 1, "is_emergency_contact": 1, "created_at": stamp(k.get("created_at"))}
        res["next_of_kin"] = dict(self.post("reception", "patient_next_of_kin", nok()))
        return {**why, **res}

    def patient_insurance(self) -> dict:
        companies = {r["id"]: r.get("name") for r in self.c.read_all("core", "insurance_companies")}
        schemes = {r["id"]: r for r in self.c.read_all("core", "insurance_schemes")}
        why = Counter()

        def gen():
            for r in self.rows("reception_patient_schemes"):
                if not self.wanted(r.get("patient")):
                    continue
                pid, sid = self.patient(r.get("patient")), self.scheme(r.get("scheme"))
                if not pid:
                    why["patient not in V3"] += 1
                    continue
                s = schemes.get(sid) or {}
                why["linked to scheme" if sid else "scheme not in V3"] += 1
                yield {"uuid": gid("reception_patient_schemes", r["id"]), "patient_id": pid, "insurance_type": "insurance",
                       "insurance_company_id": s.get("company_id"), "insurance_company_name": companies.get(s.get("company_id")),
                       "insurance_scheme_id": sid, "insurance_scheme_name": s.get("name"),
                       "policy_no": clean(r.get("policy_number")), "member_no": clean(r.get("policy_number")),
                       "principal_member_name": clean(r.get("principal")), "principal_dob": day(r.get("dob")),
                       "status": "active", "is_primary": 1, "created_at": stamp(r.get("created_at"))}
        res = self.post("reception", "patient_insurance", gen())
        return {**why, **res}

    # ── catalogue ──
    def catalogue(self) -> dict:
        # applies_to is V3's module id (1 doctor, 2 pharmacy, 3 lab, 4 radiology …, as in V1); V1's 'all' has none
        used = None
        if self.adopt:      # only the categories of tests that will be added
            ordered = {str(as_int(i.get("procedure"))) for i in self.rows("evaluation_investigations")}
            procs = self.adopted("evaluation", "procedures")
            used = {str(as_int(r.get("category"))) for r in self.rows("evaluation_procedures")
                    if str(r["id"]) in ordered and gid("evaluation_procedures", r["id"]) not in procs}
        cats = [{"uuid": gid("evaluation_procedure_categories", r["id"]), "name": (clean(r.get("name")) or f"Category {r['id']}")[:255],
                 "applies_to": as_int(r.get("applies_to")) if (clean(r.get("applies_to")) or "").isdigit() else None,
                 "status": "inactive" if clean(r.get("deleted_at")) else "active",
                 "is_active": 0 if clean(r.get("deleted_at")) else 1}
                for r in self.rows("evaluation_procedure_categories") if used is None or str(r["id"]) in used]
        res = {"categories": dict(self.post("evaluation", "procedure_categories", cats))}
        cid = self.ids_by_uuid("evaluation", "procedure_categories", fresh=True)
        src = self.rows("evaluation_procedures")
        codes = procedure_codes(src)
        if self.adopt:      # the org has its own catalogue: add only the tests V1 orders that it has no test of that name for
            ordered = {str(as_int(i.get("procedure"))) for i in self.rows("evaluation_investigations")}
            res["not ordered in V1 (not added)"] = sum(1 for r in src if str(r["id"]) not in ordered)
            src = [r for r in src if str(r["id"]) in ordered]
        res["procedures"] = dict(self.post("evaluation", "procedures", (
            procedure_payload(r, cid.get(gid("evaluation_procedure_categories", as_int(r.get("category")))), codes[str(r["id"])])
            for r in src)))
        return {"source": len(src), "without a V1 code (MDH-P<id>)": sum(1 for c in codes.values() if c.startswith("MDH-P")),
                **res}

    def procedure_prices(self) -> dict:
        why = Counter()
        pl = []
        for r in self.rows("evaluation_procedures"):
            pid = self.procedure(r["id"])
            if not pid:
                why["procedure not in V3"] += 1
                continue
            pl.append(price_payload(r, pid, self.fac, self.facility_name()))
        res = self.post("evaluation", "procedure_prices", pl)
        return {**why, **res}

    # ── lab templates ──
    def lab_templates(self) -> dict:
        res, why = {}, Counter()
        res["lab_test_categories"] = dict(self.post("evaluation", "lab_test_categories", (
            {"uuid": gid("evaluation_labtest_categories", r["id"]), "name": clean(r.get("name")) or f"Category {r['id']}",
             "description": clean(r.get("description")), "is_active": 1} for r in self.rows("evaluation_labtest_categories"))))
        lab_cat = self.ids_by_uuid("evaluation", "lab_test_categories", fresh=True)
        # V3's haemogram_titles is one list for the whole instance (no organisation): not written. A template line
        # carries its section title as text, so V1's title id is resolved to the name here.
        titles = {str(r["id"]): clean(r.get("name")) for r in self.rows("hemogram_titles")}
        def unit(v):              # the unit itself ('%', 'mmol/L'); empty ones are '' or the text 'NULL'
            c = clean(v)
            return c if c and c.upper() != "NULL" else None

        def title(v):             # a haemogram_titles id (templates_lab); subprocedures' 1 / 2 / 3 are not titles
            return titles.get(clean(v) or "")

        subs = []
        for r in self.rows("evaluation_subprocedures"):
            pr = self.procedure(r.get("procedure"))
            if not pr:
                why["subprocedure: test not in V3"] += 1
                continue
            parent = self.procedure(r.get("parent"))
            subs.append(subprocedure_payload(r, pr, parent, lab_cat.get(gid("evaluation_labtest_categories", as_int(r.get("category")))),
                                             unit(r.get("units")), title(r.get("title"))))
        res["subprocedures"] = dict(self.post("evaluation", "subprocedures", subs))
        tl = []
        for r in self.rows("evaluation_templates_lab"):
            pr, st = self.procedure(r.get("procedure")), self.procedure(r.get("subtest"))
            if not pr or not st:
                why["template line: panel / sub-test not in V3"] += 1
                continue
            tl.append(template_lab_payload(r, pr, st, title(r.get("title"))))
        res["template_labs"] = dict(self.post("evaluation", "template_labs", tl))
        names = {str(r["id"]): clean(r.get("name")) for r in self.rows("evaluation_procedures")}
        rr = []
        for r in self.rows("evaluation_reference_range"):
            pr = self.procedure(r.get("procedure"))
            if not pr:
                why["range: test not in V3"] += 1
                continue
            rr.append(reference_range_payload(r, pr, names.get(str(as_int(r.get("procedure"))))))
        res["bio_reference_ranges"] = dict(self.post("evaluation", "bio_reference_ranges", rr))
        return {**why, **res}

    # ── visits ──
    def visit_doctors(self) -> dict[str, int]:
        """V1 visit → the doctor its 'Doctor' queue named (evaluation_visit_destinations.destination is a user id)."""
        out = {}
        for r in self.rows("evaluation_visit_destinations"):
            if as_int(r.get("destination")) and words(r.get("department")).startswith("doctor"):
                out.setdefault(str(as_int(r.get("visit"))), as_int(r.get("destination")))
        return out

    def visits(self) -> dict:
        why = Counter()
        doctors = self.visit_doctors()

        def gen():
            for r in self.rows("evaluation_visits"):
                why["source"] += 1
                if not self.wanted(r.get("patient")):
                    continue
                pid = self.patient(r.get("patient"))
                if not pid:
                    why["patient not in V3"] += 1
                    continue
                doctor = self.user(doctors.get(str(r["id"]))) or self.user(r.get("user"))
                p = visit_payload(r, pid, self.fac, doctor, self.scheme(r.get("scheme")))
                if self.adopt:
                    p["unique_id"] = f"V2-{r['id']}"           # as the earlier load numbered them
                yield p
        res = self.post("evaluation", "visits", gen())
        self._cache.pop("visits", None)
        return {**why, **res}

    def visit_destinations(self) -> dict:
        dests: dict[str, tuple[int, str]] = {}
        for d in self.c.read_all("core", "service_destinations", workers=4):
            for k in (d.get("slug"), d.get("name")):
                if words(k):
                    dests.setdefault(words(k).replace("-", " "), (d["id"], d.get("name")))
        why = Counter()

        def build(r, vid, pid):
            dep = words(r.get("department"))
            hit = dests.get(dep) or dests.get(DESTINATION_ALIASES.get(dep, ""))
            why[f"department {clean(r.get('department'))} → {hit[1] if hit else 'no service destination'}"] += 1
            return destination_payload(r, vid, hit[0] if hit else None, hit[1] if hit else None, self.user(r.get("user")))
        res = self.per_visit("evaluation_visit_destinations", "evaluation", "visit_destinations", build)
        return {**why, **res}

    def vitals(self) -> dict:
        return self.per_visit("evaluation_vitals", "evaluation", "vitals",
                              lambda r, vid, pid: vitals_payload(r, vid, pid, self.user(r.get("user"))))

    def doctor_notes(self) -> dict:
        return self.per_visit("evaluation_doctor_notes", "evaluation", "doctor_notes",
                              lambda r, vid, pid: doctor_note_payload(r, vid, pid, self.user(r.get("user"))))

    def nursing_notes(self) -> dict:
        return self.per_visit("evaluation_nursing_notes", "inpatient", "note",
                              lambda r, vid, pid: nursing_note_payload(r, vid, self.c.user_id))

    # ── orders and results ──
    def investigations(self) -> dict:
        return self.per_visit("evaluation_investigations", "evaluation", "investigations",
                              lambda r, vid, pid: investigation_payload(r, vid, pid, self.procedure(r.get("procedure")),
                                                                        self.user(r.get("user"))))

    def lab_results(self) -> dict:
        """One investigation_results row per V1 result (published — V1 leaves status 0 on every result, final or not) and
        one investigation_result_details row per sub-test value, with the sub-test's unit and adult range."""
        inv = self.by_id("evaluation_investigations")
        procs = self.by_id("evaluation_procedures")
        subs = {}
        for s in self.rows("evaluation_subprocedures"):
            subs.setdefault(str(as_int(s.get("procedure"))), s)
        why = Counter()
        heads = []
        for r in self.rows("evaluation_investigation_results"):
            why["source"] += 1
            i = inv.get(str(as_int(r.get("investigation"))))
            if i is None:
                why["investigation not on the V1 server (orphan result)"] += 1
                continue
            v = self.visit(i.get("visit"))
            iid = self.v3("evaluation", "investigations", "evaluation_investigations", as_int(r.get("investigation")))
            if not v or not iid:
                why["investigation / visit not in V3"] += 1
                continue
            when = stamp(r.get("created_at"))
            by = self.user(r.get("examiner")) or self.user(r.get("user"))
            heads.append((r, v, {"uuid": gid("evaluation_investigation_results", r["id"]), "investigation_id": iid,
                                 "patient_id": v[1], "test_parameter": clean((procs.get(str(as_int(i.get("procedure")))) or {}).get("name")),
                                 "status": "publish", "result_date": when, "performed_by": by, "published_by": by,
                                 "published_on": when, "approved_on": when, "approved_by": by,
                                 "impression": strip_html(r.get("comments")), "technical_notes": strip_html(r.get("instructions")),
                                 "emailed": 1 if flag(r.get("emailed")) else 0, "created_at": when,
                                 "updated_at": stamp(r.get("updated_at")) or when}))
        res = {"results": dict(self.post("evaluation", "investigation_results", (h[2] for h in heads)))}
        rid = self.ids_by_uuid("evaluation", "investigation_results", fresh=True)
        loaded = self.adopted("evaluation", "investigation_results")

        def gen():
            for r, v, head in heads:
                if head["uuid"] in loaded:
                    why["details: result already in V3 from the earlier load"] += 1
                    continue
                if head["uuid"] not in rid:
                    why["details: result header not in V3"] += 1
                    continue
                for n, (sid, value) in enumerate(results_pairs(r.get("results"))):
                    val = strip_html(value)
                    if not val:
                        why["details: empty value"] += 1
                        continue
                    s = subs.get(str(sid)) or {}
                    lo = clean(s.get("adult_minrange")) or clean(s.get("lab_min_range"))
                    hi = clean(s.get("adult_maxrange")) or clean(s.get("lab_max_range"))
                    u = clean(s.get("units"))
                    u = u if u and u.upper() != "NULL" else None
                    yield {"uuid": gid("result_detail", f"{r['id']}|{n}"), "investigation_result_id": rid[head["uuid"]],
                           "visit_id": v[0], "sub_test_id": self.procedure(sid),
                           "test": (clean((procs.get(str(sid)) or {}).get("name")) or head["test_parameter"] or "Result")[:255],
                           "result": val, "unit": u, "reference_ranges": range_text(lo, hi), "flag": result_flag(val, lo, hi),
                           "created_at": head["created_at"], "updated_at": head["updated_at"]}
        res["details"] = dict(self.post("evaluation", "investigation_result_details", gen()))
        return {**why, **res}

    # ── inventory ──
    def store(self) -> int:
        return self.ref("inventory", "store", "Main Store", {"code": "MDH-MAIN", "type": "main", "is_main_store": 1,
                                                              "is_active": 1, "is_consumable": 0})

    def products(self) -> dict:
        if self.adopt:
            return {"skipped": "--adopt: the target runs its own inventory (products, stores, live stock) — V1's is not added"}
        cats = {}
        for r in self.rows("inventory_categories"):
            cats[str(r["id"])] = self.ref("inventory", "product_category", clean(r.get("name")) or f"Category {r['id']}",
                                          {"is_drug": 1, "cash_markup": num(r.get("cash_markup")),
                                           "credit_markup": num(r.get("credit_markup"))})
        units = {str(r["id"]): self.ref("inventory", "unit", clean(r.get("name")) or f"Unit {r['id']}",
                                        {"symbol": clean(r.get("symbol")), "description": clean(r.get("description"))})
                 for r in self.rows("inventory_units")}
        prices = {}
        for p in self.rows("inventory_product_price"):           # oldest first: the latest price wins
            prices[str(as_int(p.get("product")))] = p
        dcat = cats.get("1") or self.ref("inventory", "product_category", "Uncategorised", {"is_drug": 1})
        dunit = self.ref("inventory", "unit", "Unit")
        src = self.rows("inventory_products")
        pl = [product_payload(r, cats.get(str(as_int(r.get("category")))) or dcat, units.get(str(as_int(r.get("unit")))) or dunit,
                              prices.get(str(r["id"]))) for r in src]
        return {"source": len(src), "categories": len(cats), "units": len(units), "with a price": len(prices),
                **self.post("inventory", "product", pl)}

    def stock(self) -> dict:
        """inventory_stocks keeps one quantity per product (V1 has no stores in use): each product with stock on hand gets an
        opening batch and a stock row in the Main Store."""
        if self.adopt:
            return {"skipped": "--adopt: the target runs its own inventory (products, stores, live stock) — V1's is not added"}
        prods, store = self.ids_by_uuid("inventory", "product", fresh=True), self.store()
        prices = {str(as_int(p.get("product"))): p for p in self.rows("inventory_product_price")}
        why, batches, stocks = Counter(), [], []
        for r in self.rows("inventory_stocks"):
            q = as_int(r.get("quantity")) or 0
            pid = prods.get(gid("inventory_products", as_int(r.get("product"))))
            if q <= 0:
                why["no stock on hand"] += 1
                continue
            if not pid:
                why["product not in V3"] += 1
                continue
            p = prices.get(str(as_int(r.get("product")))) or {}
            cost, sell = num(p.get("price")) or 0.0, num(p.get("selling"))
            batches.append({"uuid": gid("opening_batch", r["id"]), "batch_no": f"MDH-OPEN-{r['id']}", "product_id": pid,
                            "store_id": store, "cost_price": cost, "selling_price": sell, "quantity_received": q,
                            "quantity_available": q, "status": "active", "notes": "Opening stock from Collabmed V1"})
            stocks.append((r["id"], pid, q, cost, sell, num(p.get("ins_price"))))
        res = {"batches": dict(self.post("inventory", "batch", batches))}
        bids = self.ids_by_uuid("inventory", "batch", fresh=True)
        res["stock"] = dict(self.post("inventory", "stock", (
            {"uuid": gid("inventory_stocks", key), "product_id": pid, "store_id": store, "batch_id": bids.get(gid("opening_batch", key)),
             "quantity": q, "available_quantity": q, "reserved_quantity": 0, "average_cost": cost,
             "total_value": round(q * cost, 2), "selling_price": sell, "insurance_price": ins}
            for key, pid, q, cost, sell, ins in stocks)))
        return {**why, **res}

    # ── money ──
    def patient_card(self) -> dict[int, tuple]:
        if "pcard" not in self._cache:
            self._cache["pcard"] = {p["id"]: (p["id"], " ".join(x for x in (p.get("first_name"), p.get("middle_name"), p.get("last_name")) if x),
                                              p.get("patient_no")) for p in self.c.read_all("reception", "patient", workers=8)}
        return self._cache["pcard"]

    def payment_visits(self) -> dict[str, str]:
        """V1 payment → its visit (V1 leaves payments.visit empty; the detail lines carry it)."""
        out = {}
        for d in self.rows("finance_evaluation_payment_details"):
            if as_int(d.get("visit")):
                out.setdefault(str(as_int(d.get("payment"))), str(as_int(d.get("visit"))))
        return out

    def payments(self) -> dict:
        modes: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        for table, mode in (("finance_payments_cash", "cash"), ("finance_payments_mpesa", "mpesa"),
                            ("finance_payments_card", "card"), ("finance_payments_cheque", "cheque")):
            for m in self.rows(table):
                modes[str(as_int(m.get("payment")))][mode] += num(m.get("amount")) or 0.0
        cards, pv = self.patient_card(), self.payment_visits()
        profiles = {str(as_int(p.get("user_id"))): " ".join(x for x in (clean(p.get("first_name")), clean(p.get("last_name"))) if x)
                    for p in self.rows("users_user_profiles")}
        why = Counter()

        def gen():
            for r in self.rows("finance_evaluation_payments"):
                if not self.wanted(r.get("patient")):
                    continue
                v = self.visit(r.get("visit") or pv.get(str(r["id"])))
                pid = (v[1] if v else None) or self.patient(r.get("patient"))
                p = payment_payload(r, dict(modes.get(str(r["id"]), {})), cards.get(pid), v[0] if v else None,
                                    self.user(r.get("user")) or self.c.user_id, profiles.get(str(as_int(r.get("user")))))
                if p is None:
                    why["no amount / date"] += 1
                    continue
                if self.adopt:
                    p["receipt"] = clean(r.get("receipt")) or str(r["id"])     # as the earlier load wrote them
                why[f"mode {p['payment_mode']}"] += 1
                yield p
        res = self.post("finance", "evaluation_payment", gen())
        return {**why, **res}

    def payment_details(self) -> dict:
        paid = self.ids_by_uuid("finance", "evaluation_payment", fresh=True)
        procs = self.by_id("evaluation_procedures")
        inv = self.by_id("evaluation_investigations")
        why = Counter()

        def gen():
            for d in self.rows("finance_evaluation_payment_details"):
                pid = paid.get(gid("finance_evaluation_payments", as_int(d.get("payment"))))
                if not pid:
                    why["payment not in V3"] += 1
                    continue
                v = self.visit(d.get("visit"))
                i = inv.get(str(as_int(d.get("investigation")))) or {}
                name = clean((procs.get(str(as_int(i.get("procedure")))) or {}).get("name")) or clean(d.get("description")) or "Payment"
                a = num(d.get("price")) or num(d.get("cost")) or 0.0
                when = stamp(d.get("created_at"))
                yield {"uuid": gid("finance_evaluation_payment_details", d["id"]), "payment": pid, "visit": v[0] if v else None,
                       "investigation": self.v3("evaluation", "investigations", "evaluation_investigations", as_int(d.get("investigation"))),
                       "price": a, "amount": a, "name": name[:255], "description": clean(d.get("description")),
                       "created_at": when, "updated_at": stamp(d.get("updated_at")) or when}
        res = self.post("finance", "evaluation_payment_detail", gen())
        return {**why, **res}

    def invoices(self) -> dict:
        lines = defaultdict(list)
        for d in self.rows("finance_patient_invoice_details"):
            lines[str(as_int(d.get("invoice_id")))].append(d)
        cards, why = self.patient_card(), Counter()

        def gen():
            for r in self.rows("finance_patient_invoices"):
                if not self.wanted(r.get("patient_id")):
                    continue
                pid = self.patient(r.get("patient_id"))
                if not pid:
                    why["patient not in V3"] += 1
                    continue
                items = lines.get(str(r["id"]), [])
                total = num(r.get("amount")) or round(sum(num(d.get("amount")) or 0 for d in items), 2)
                paid = total if words(r.get("status")) == "paid" else 0.0
                status = "paid" if paid else "pending"
                when = stamp(r.get("created_at"))
                card = cards.get(pid) or (None, None, None)
                yield {"uuid": gid("finance_patient_invoices", r["id"]), "invoice_no": f"MDH-INV-{r['id']}",
                       "invoice_date": (when or self.today)[:10], "invoice_type": "outpatient", "for_cash": 1,
                       "patient_id": pid, "patient_name": card[1], "patient_no": card[2], "subtotal": total,
                       "total_amount": total, "amount": total, "paid_amount": paid, "paid": paid,
                       "balance_amount": round(total - paid, 2), "status": status, "payment_status": status,
                       "source": "medhanit_migration",
                       "notes": "\n".join(f"{clean(d.get('item_name')) or d.get('item_type')}: {clean(d.get('amount'))}" for d in items) or None,
                       "created_by": self.user(r.get("user_id")) or self.c.user_id, "created_at": when, "updated_at": when}
        res = self.post("finance", "invoice", gen())
        return {**why, **res}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--target", default=os.environ.get("MEDHANIT_TARGET", TARGET))
    ap.add_argument("--steps", nargs="*", default=list(STEPS), choices=STEPS)
    ap.add_argument("--limit", type=int, default=0, help="post at most N rows per model (canary)")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-missing", action="store_true", help="post only rows whose uuid V3 doesn't have yet")
    ap.add_argument("--patients", nargs="*", help="canary: only these V1 patient ids (and their visits, orders, money)")
    ap.add_argument("--adopt", action=argparse.BooleanOptionalAction, default=None,
                    help="link to the rows an earlier load of this V1 left in the target instead of re-posting them "
                         f"(default: on for {', '.join(sorted(ADOPT_TARGETS))})")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    adopt = a.target in ADOPT_TARGETS if a.adopt is None else a.adopt
    run = Run(a.target, a.dry_run, a.limit, a.workers, a.only_missing, adopt)
    run.only_patients = {str(as_int(p)) for p in a.patients} if a.patients else None
    report = {"target": a.target, "org": run.c.organization_id, "facility": run.fac, "dry_run": a.dry_run, "adopt": adopt,
              "limit": a.limit, "started": time.strftime("%Y-%m-%d %H:%M:%S"), "steps": {}}
    out = Path(__file__).resolve().parent / "output" / "medhanit_to_v3"
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
            path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    log.info("report: %s", path)
    sys.exit(1 if any("error" in r for r in report["steps"].values()) else 0)


if __name__ == "__main__":
    main()
