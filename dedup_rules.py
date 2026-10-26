"""
dedup_rules.py — default duplicate rules, one per V3 table, for every hospital.

A field a hospital lacks is skipped. A hospital can override a table in its
mappings JSON: "dedup": {"patients": {"burst_seconds": 300}, "invoice_items": {"disabled": true}}.
Every table also gets the load / version pass (same ID twice → newest copy kept).

person     merged on a strong id, same name + exact DOB, same name registered within
           `burst_seconds`, a near name + exact DOB + phone, or a rare name + phone;
           weaker matches go to review. A differing DOB, gender or strong id blocks.
event      same `group` values within `auto_seconds` of the entry before → merged;
           `agree` fields must not differ; `optional` fields may be blank.
           review_differences / review_untimed / review_minutes False: those repeats
           are normal and migrate as they are. auto_identical_text N: the same text
           (N+ characters) twice on one parent is merged.
catalogue  same normalised name, no differing `attrs` → merged; a differing
           `review_if_differs` value (price) → review.
"""
from __future__ import annotations

import copy

# Field order for "newest copy" and for tie-breaking survivors.
TIME_FIELDS = ("updated_at", "created_at")

# V3 link field → the tables it can point at (first present wins). Links are moved
# to the parent's survivor before matching, so a line on each of two duplicate visits is caught.
LINKS: dict[str, tuple[str, ...]] = {
    "patient_id": ("patients",), "patient": ("patients",),
    "visit_id": ("visits",), "visit": ("visits",),
    "invoice_id": ("invoices",),
    "admission_id": ("inp_admissions",),
    "ward_id": ("inp_wards",), "bed_id": ("inp_beds",),
    "department_id": ("core_departments", "departments"),
    "product_id": ("inv_products",), "procedure_id": ("procedures",),
    "prescription_id": ("prescriptions",),
    "supplier_id": ("inv_suppliers", "suppliers"),
    "insurance_company_id": ("insurance_companies",),
    "user_id": ("users",), "doctor_id": ("users",),
}

_PERSON_NAME = ("first_name", "middle_name", "last_name", "other_names", "full_name", "name")
_PERSON_DOB = ("dob", "date_of_birth", "birth_date")
_PERSON_GENDER = ("gender", "sex")
_PERSON_PHONE = ("mobile", "phone", "phone_number", "telephone", "mobile_number")
_PERSON_IDS = ("national_id", "id_number", "id_no", "identification_number", "passport_number",
               "birth_certificate_number", "nhif_number", "sha_number", "insurance_number", "member_number")
_VISIT = ("visit_id", "visit")
_PATIENT = ("patient_id", "patient")

DEFAULT_RULES: dict[str, dict] = {
    # ── people ────────────────────────────────────────────────────────────
    "patients": {
        "type": "person", "name": _PERSON_NAME, "dob": _PERSON_DOB, "gender": _PERSON_GENDER,
        "phone": _PERSON_PHONE, "strong_ids": _PERSON_IDS,
        "registered": ("registration_date", "created_at"),
        "burst_seconds": 600,          # same name registered this close together = one person
    },
    "users": {
        "type": "catalogue",
        "keys": [{"fields": ("email",), "action": "auto"},
                 {"fields": ("employee_number",), "action": "auto"},
                 {"fields": ("username",), "action": "auto"},
                 {"fields": ("name",), "action": "review"}],
        # one shared value isn't enough when the other identifiers disagree
        "attrs": ("email", "employee_number", "username", "national_id", "name"),
    },
    # ── catalogue ─────────────────────────────────────────────────────────
    "procedures": {
        "type": "catalogue", "keys": [{"fields": ("name",), "action": "auto"}],
        "attrs": ("departmentcode", "department_id", "category", "category_id", "type"),
        "review_if_differs": ("price", "amount", "selling_price", "cost"),
    },
    "inv_products": {
        "type": "catalogue", "keys": [{"fields": ("name",), "action": "auto"}],
        "attrs": ("generic_name", "dosage", "strength", "formulation", "unit", "unit_id", "pack_size"),
        "review_if_differs": ("selling_price", "buying_price", "price", "cost"),
    },
    "core_departments": {"type": "catalogue", "keys": [{"fields": ("name",), "action": "auto"}]},
    "departments": {"type": "catalogue", "keys": [{"fields": ("name",), "action": "auto"}]},
    "inp_wards": {"type": "catalogue", "keys": [{"fields": ("name",), "action": "auto"}]},
    "inp_beds": {"type": "catalogue", "keys": [{"fields": ("bed_number", "number"), "action": "auto"}],
                 "attrs": ("ward_id",)},
    "inv_suppliers": {"type": "catalogue", "keys": [{"fields": ("name",), "action": "auto"}],
                      "review_if_differs": ("phone", "email")},
    "insurance_companies": {"type": "catalogue", "keys": [{"fields": ("name",), "action": "auto"}]},
    "payment_modes": {"type": "catalogue", "keys": [{"fields": ("name",), "action": "auto"}]},
    # ── events ────────────────────────────────────────────────────────────
    "visits": {
        "type": "event", "group": (_PATIENT, ("department_id",)),
        "time": ("visit_date", "created_at"), "agree": ("payment_mode", "visit_type"),
        "optional": ("department_id",), "auto_seconds": 120, "review_seconds": 900,
    },
    "inp_admissions": {
        "type": "event", "group": (_PATIENT,), "time": ("admission_date", "created_at"),
        "agree": ("ward_id",), "auto_seconds": 600, "review_seconds": 3600,
    },
    "invoices": {
        "type": "event", "group": (_PATIENT, _VISIT, ("invoice_type",)),
        "time": ("invoice_date", "created_at"), "agree": ("total", "amount"),
        "optional": ("visit_id", "invoice_type"), "auto_seconds": 120, "review_seconds": 900,
    },
    "payments": {
        "type": "event", "group": (("invoice_id",), _PATIENT, ("amount",), ("payment_type", "payment_mode")),
        "time": ("payment_date", "created_at"), "agree": ("reference", "transaction_code"),
        "optional": ("invoice_id", "payment_type"), "auto_seconds": 120, "review_seconds": 3600,
    },
    "appointments": {
        "type": "event", "group": (_PATIENT, ("doctor_id",)),
        "time": ("appointment_date", "created_at"), "optional": ("doctor_id",),
        "auto_seconds": 120, "review_seconds": 900,
    },
    # ── lines under an event ──────────────────────────────────────────────
    "prescriptions": {
        "type": "event", "group": (_VISIT, ("drug_name", "product_id", "drug_id")),
        "time": ("created_at", "prescription_date"), "agree": ("dose", "dosage", "frequency", "duration", "quantity"),
        "auto_seconds": 120, "review_seconds": 900, "review_minutes": False,
    },
    "prescription_administrations": {
        "type": "event", "group": (_VISIT, ("drug_name", "prescription_id")),
        "time": ("administered_at", "created_at"), "agree": ("dose", "dosage"),
        "auto_seconds": 120, "review_seconds": 600,
        "review_differences": False, "review_untimed": False,      # doses repeat through the day
    },
    "invoice_items": {
        "type": "event", "group": (("invoice_id",), ("item_name", "procedure_id", "product_id")),
        "time": ("created_at",), "agree": ("unit_price", "line_total", "quantity"),
        "auto_seconds": 120, "review_seconds": 900, "review_minutes": False,
    },
    "vitals": {
        "type": "event", "group": (_VISIT,), "time": ("recorded_at", "created_at"),
        "agree": ("weight", "height", "temperature", "pulse", "systolic_bp", "diastolic_bp",
                  "respiratory_rate", "spo2", "oxygen_saturation"),
        "auto_seconds": 120, "review_seconds": 600,
        "review_differences": False, "review_untimed": False,      # readings are re-taken
    },
    "doctor_notes": {
        "type": "event", "group": (_VISIT,), "time": ("created_at", "note_date"),
        "agree": ("chief_complaints", "presenting_complaints", "assessment", "plan", "notes"),
        "auto_seconds": 300, "review_seconds": 900, "review_differences": False,
        "review_minutes": False, "auto_identical_text": 20,
    },
    "inp_notes": {
        "type": "event", "group": (("admission_id",), _VISIT), "time": ("note_date", "created_at"),
        "agree": ("note_content", "note_type"), "optional": ("visit_id",),
        "auto_seconds": 300, "review_seconds": 900, "review_differences": False,   # a shift writes several
        "review_minutes": False, "auto_identical_text": 20,
    },
    "investigations": {
        "type": "event", "group": (_VISIT, ("procedure_id", "test_name", "name", "investigation")),
        "time": ("order_date", "created_at"), "agree": ("result",),
        "auto_seconds": 120, "review_seconds": 900, "review_minutes": False,
    },
    "copays": {
        "type": "event", "group": (("invoice_id",), _PATIENT, ("company_id",)),
        "time": ("created_at",), "agree": ("amount",), "optional": ("patient_id", "company_id"),
        "auto_seconds": 120, "review_seconds": 900,
    },
}


def rules_for(table: str, overrides: dict | None = None) -> dict | None:
    """The rule for a V3 table: the default, with a hospital's mappings-JSON
    override laid over it ({"disabled": true} switches it off). None = no
    rule (the table still gets the load / version pass)."""
    table = table.lower()
    rule = copy.deepcopy(DEFAULT_RULES.get(table))
    extra = (overrides or {}).get(table)
    if extra:
        if extra.get("disabled"):
            return None
        rule = {**(rule or {}), **{k: (tuple(v) if isinstance(v, list) and k not in ("keys", "group") else v)
                                   for k, v in extra.items()}}
        if "group" in rule:            # JSON lists of lists → tuples of tuples
            rule["group"] = tuple(tuple(g) if isinstance(g, (list, tuple)) else (g,) for g in rule["group"])
    if rule and rule.get("type") not in ("person", "event", "catalogue"):
        raise ValueError(f"dedup rule for {table}: unknown type {rule.get('type')!r}")
    return rule
