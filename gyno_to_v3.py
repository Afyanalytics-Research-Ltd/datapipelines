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
  catalogue / investigations   laboratory, otherservices, otherprocedures → evaluation/procedures (+ categories);
             lab_requests, services_records, otherequests → evaluation/investigations.
  products / prescriptions     pharmacy → inventory/product (+ categories, units); pharmacy_records → prescriptions.
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
import re
import sys
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

sys.path.insert(0, "/opt/airflow/dags")
log = logging.getLogger("gyno_to_v3")

TARGET = "gyno"
SF = "HOSPITALS.GYNOSOLUTIONS_HISTORICAL_DATA"
NS = uuid.UUID("3c6d0b6e-9a1f-4f57-8d2e-6b1f0a9c4e21")      # never change: rows are matched by it
STEPS = ("patients", "visits", "vitals", "doctor_notes", "discharges", "nurse_notes", "anc", "catalogue",
         "investigations", "products", "prescriptions", "payments", "appointments", "users", "insurance", "invoices",
         "extra_notes", "documents", "stock", "procurement", "destinations", "payment_links", "receipts", "activity")


# ── pure helpers (tests/test_gyno_to_v3.py) ────────────────────────────────
def gid(table: str, source_id) -> str:
    """Stable uuid of a source row."""
    return str(uuid.uuid5(NS, f"gyno/{table}/{source_id}"))


def clean(v) -> str | None:
    s = "" if v is None else str(v).strip()
    return s or None


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


NOTE_FIELD = "history_of_present_illness"      # where a Gyno doctor's free-text note lands in V3
PAYMENT_TYPES = {"mpesa": "mpesa", "cash": "cash", "visa": "card", "cheque": "cheque", "direct bank deposit": "bank_transfer"}


def text_join(*parts) -> str | None:
    out = "\n".join(f"{label}: {clean(v)}" if label else clean(v) for label, v in parts if clean(v))
    return out or None


def doctor_note_payload(r: dict, visit_id: int, patient_id: int, doctor: tuple | None) -> dict:
    return {"uuid": gid("doctorsnotes", r["id"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            NOTE_FIELD: clean(r.get("notes")), "treatment_plan": clean(r.get("eprescription")),
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


def product_payload(r: dict, category: int, unit: int) -> dict:
    name, active = item_name(r.get("title"), r["id"])
    details = clean(r.get("details"))
    return {"uuid": gid("pharmacy", r["id"]), "generic_name": name, "is_active": active,
            "code": f"GYN-DRG-{r['id']}", "item_no": str(r["id"]), "description": None if details == "null" else details,
            "category_id": category, "unit_id": unit, **prices(r.get("cost"), r.get("buying_price"), r.get("insurancecost")),
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


def insurance_company_payload(r: dict) -> dict:
    return {"uuid": gid("paymentype", r["id"]), "code": f"GYN-INS-{r['id']}", "name": clean(r.get("paymentype")),
            "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


def patient_insurance_payload(r: dict, patient_id: int, company: tuple | None) -> dict:
    return {"uuid": gid("preferedpayment", r["id"]), "patient_id": patient_id, "insurance_type": "insurance",
            "insurance_company_id": company[0] if company else None, "insurance_company_name": company[1] if company else None,
            "insurance_scheme_name": clean(r.get("scheme")), "member_no": clean(r.get("memberno")),
            "created_at": stamp(r.get("updated_on")), "updated_at": stamp(r.get("updated_on"))}


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
            "notes": text_join(("Services", r.get("serviceoffered")), ("Payments", r.get("payments"))),
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
    def destinations(self) -> dict:
        dests = {d.get("slug"): (d["id"], d.get("name")) for d in self.c.read_all("core", "service_destinations")}
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
        return {"missing_destinations": missing, "evaluation": dict(self.post("evaluation", "visit_destinations", ev)),
                "reception": dict(self.post("reception", "visit_destination", rc))}

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
                stock_rows.append({"uuid": gid(f"{table}-stock", r["id"]), "product_id": pid, "store_id": store,
                                   "selling_price": num(r.get(price_col)), "insurance_price": num(r.get(ins_col)) if ins_col else None})
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
        pos = [p for p in (po_payload(r, sup.get(gid("suppliers", r.get("supplier_id"))), store, user) for r in lpos
                           if sup.get(gid("suppliers", r.get("supplier_id")))) if p]
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
        out["goods_received_items"] = dict(self.post("inventory", "goods_received_item", grn_items))
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
