"""
v3_extra_mappings.py — V2 → V3 mappings added on top of the hand-written ones
in v2_to_v3_api_migration.py, plus the record of every other table's fate,
so a facility's whole CLEAN schema is accounted for (2026-10, for the
KISUMU_CLEAN → org 168 migration).

Built from three sources, per table:
  * the V2 columns actually in Snowflake CLEAN,
  * the V3 model's $fillable and its migrations (required columns, enums) in
    ~/erp-platform-3.0/services, and
  * the live gateway (model alias, service, tenant / facility columns).

Each MAPPINGS entry is a table the migration can now post. It never changes a
hand-written mapping: apply() only adds what isn't defined yet.

  renames   V2 column → V3 column (after the global patient→patient_id … renames)
  keep      the V3 columns posted (the model's $fillable that V2 can fill)
  fk        V3 column → gateway alias of its parent (resolved through the id map)
  critical  links a row can't be posted without — held back until the parent
            is in V3 (users included: rows wait for the users table, never a
            stand-in account)
  inject    values V3 requires that V2 never had, from the row itself

Every other table in a facility's CLEAN schema must be in NOT_MIGRATED (with
why) or DUPLICATES (with the copy that is migrated) — coverage() reports any
table that is in none of these, and tests/test_v3_extra_mappings.py fails on it.
"""
from __future__ import annotations

# ─── tables the migration now posts ─────────────────────────────────────────

def _admission_for_visit(r):
    """inp notes / vitals sometimes carry admission_id 0 — the visit's admission."""
    import v2_to_v3_api_migration as v2v3
    return r.get("admission_id") or v2v3._v2_admission_for_visit(r.get("visit_id"))


def _patient_for_visit(r):
    """V2 patient id of the row's visit (V2 diagnoses carry no patient column)."""
    import v2_to_v3_api_migration as v2v3
    return r.get("patient_id") or v2v3._v2_patient_for_visit(r.get("visit_id"))


MAPPINGS: dict[str, dict] = {
    # ── evaluation-service ────────────────────────────────────────────────
    "evaluation_procedure_prices": {
        "namespace": r"Ignite\Evaluation\Entities\ProcedurePrices",
        "v3": r"App\Models\EvaluationProcedurePrice", "alias": "procedure_prices", "service": "evaluation",
        "keep": ["evaluation_procedure_id", "cash_charge", "insurance_charge", "charge_insurance", "precharge",
                 "status", "variable_pricing", "requires_preauth", "preauth_cash", "preauth_insurance",
                 "preauth_specific_insurance", "preauth_specific_scheme"],
        "fk": {"evaluation_procedure_id": "procedures"},
        "critical": ["evaluation_procedure_id"],
    },
    "evaluation_refraction": {
        "namespace": r"Ignite\Evaluation\Entities\Refraction",
        "v3": r"App\Models\Refraction", "alias": "refractions", "service": "evaluation",
        "keep": ["user_id", "visit_id", "patient_id", "aided_va", "unaided_va", "pinhole_va", "unaided_near_va",
                 "aided_near_va", "iop", "subjective", "present_glasses", "autorefraction", "intra_ocular_pressure",
                 "objective_refraction", "prisms", "eyewear_prescription", "aided_spectacles_va", "aided_contact_va"],
        "fk": {"visit_id": "visit", "patient_id": "patient", "user_id": "users"},
        "critical": ["visit_id", "patient_id"],
        "null_if_unresolved": ["user_id"],
    },
    "evaluation_age_groups": {
        "namespace": r"Ignite\Evaluation\Entities\AgeGroups",
        "v3": r"App\Models\AgeGroup", "alias": "age_groups", "service": "evaluation",
        "keep": ["lower_value", "upper_value", "group", "name"],
        "inject": {"name": ("join", "group", "lower_value", "upper_value")},
    },
    "evaluation_treatment_locations": {
        "namespace": r"Ignite\Evaluation\Entities\TreatmentLocations",
        "v3": r"App\Models\TreatmentLocation", "alias": "treatment_locations", "service": "evaluation",
        "keep": ["name", "description", "code"],
        "inject": {"code": ("upper", "name")},                      # code is NOT NULL, V2 has none
    },
    "evaluation_facilities": {
        "namespace": r"Ignite\Evaluation\Entities\Facilities",
        "v3": r"App\Models\Facility", "alias": "facilities", "service": "evaluation",
        "keep": ["name", "description"],
    },
    "evaluation_icd10_sub_categories": {
        "namespace": r"Ignite\Evaluation\Entities\Icd10SubCategories",
        "v3": r"App\Models\Icd10SubCategory", "alias": "icd10_subcategories", "service": "evaluation",
        "renames": {"code_from": "code", "name": "description"},
        "keep": ["category_id", "code", "description"],
        "fk": {"category_id": "icd10_categories"},
        "null_if_unresolved": ["category_id"],
    },
    # ── inpatient-service ─────────────────────────────────────────────────
    "inpatient_charges": {
        "namespace": r"Ignite\Inpatient\Entities\Charges",
        "v3": r"App\Models\Charge", "alias": "charge", "service": "inpatient",
        "keep": ["name", "cost", "type", "associated_procedure"],   # charge_category nullable since 2026-09-02
    },
    "partograph": {
        "namespace": r"Ignite\Inpatient\Entities\Partograph",
        "v3": r"App\Models\Partograph", "alias": "partograph", "service": "inpatient",
        "keep": ["admission_id", "patient_id", "cervix", "doh", "afm", "bp_systolic", "bp_dystolic", "urine_temp",
                 "urine_protein", "urine_acetone", "urine_volume", "oxytocin", "liquor_fluid_moulding_first",
                 "liquor_fluid_moulding_last", "para_start", "para_finish", "fhr", "gravida", "oxytocin_unit",
                 "contraction_per_10_min", "temperature", "perineal_status", "induction_medication", "moulding",
                 "number_of_contractions", "pulse"],
        "fk": {"admission_id": "admission", "patient_id": "patient"},
        "critical": ["admission_id", "patient_id"],
    },
    "patient_management": {
        "namespace": r"Ignite\Inpatient\Entities\PatientManagement",
        "v3": r"App\Models\PatientManagement", "alias": "patient_management", "service": "inpatient",
        "keep": ["visit_id", "notes", "title", "type", "user_id"],
        "fk": {"visit_id": "visit", "user_id": "users"},
        "critical": ["visit_id"],
        "null_if_unresolved": ["user_id"],
    },
    "inpatient_notes": {
        "namespace": r"Ignite\Inpatient\Entities\Notes",
        "v3": r"App\Models\Note", "alias": "note", "service": "inpatient",
        # V2 `type` is 0/1 with no recorded meaning; inp_notes.note_type is a
        # required enum — general_note rather than a guess
        "renames": {"notes": "note_content", "user_id": "created_by"},
        "keep": ["admission_id", "visit_id", "title", "note_type", "note_date", "note_content", "created_by"],
        "inject": {"admission_id": ("admission_for_visit",), "note_type": ("const", "general_note"),
                   "note_date": ("copy", "created_at")},
        "fk": {"admission_id": "admission", "visit_id": "visit", "created_by": "users"},
        "critical": ["admission_id", "created_by"],
        "null_if_unresolved": ["visit_id"],
    },
    "inpatient_vitals": {
        "namespace": r"Ignite\Inpatient\Entities\Vitals",
        "v3": r"App\Models\Vital", "alias": "vital", "service": "inpatient",
        "renames": {"bp_systolic": "systolic_bp", "bp_diastolic": "diastolic_bp", "respiration": "respiratory_rate",
                    "oxygen": "oxygen_saturation", "pulse": "pulse_rate", "blood_sugar": "blood_glucose",
                    "user_id": "recorded_by", "date_recorded": "recorded_date", "time_recorded": "recorded_time"},
        "keep": ["admission_id", "temperature", "weight", "height", "pulse_rate", "respiratory_rate", "systolic_bp",
                 "diastolic_bp", "oxygen_saturation", "blood_glucose", "recorded_by", "recorded_date",
                 "recorded_time", "recorded_at"],
        "inject": {"admission_id": ("admission_for_visit",), "recorded_at": ("copy", "created_at")},
        "fk": {"admission_id": "admission", "recorded_by": "users"},
        "critical": ["admission_id", "recorded_by"],               # both NOT NULL
    },
    # ── reception-service ─────────────────────────────────────────────────
    "evaluation_visit_consultants": {
        "namespace": r"Ignite\Evaluation\Entities\VisitConsultants",
        "v3": r"App\Models\VisitConsultant", "alias": "visit_consultant", "service": "reception",
        "keep": ["visit_id", "patient_id", "consultant_id", "consultant_name"],
        "fk": {"visit_id": "visit", "patient_id": "patient", "consultant_id": "users"},
        "critical": ["visit_id"],
        "null_if_unresolved": ["consultant_id", "patient_id"],
    },
    # ── inventory-service ─────────────────────────────────────────────────
    "inventory_inventory_payment_terms": {
        "namespace": r"Ignite\Inventory\Entities\InventoryPaymentTerms",
        "v3": r"App\Models\PaymentTerm", "alias": "payment_term", "service": "inventory",
        "renames": {"terms": "name"},
        "keep": ["name", "description"],
    },
    "inventory_inventory_tax_categories": {
        "namespace": r"Ignite\Inventory\Entities\InventoryTaxCategories",
        "v3": r"App\Models\TaxCategory", "alias": "tax_category", "service": "inventory",
        "keep": ["name", "rate"],
    },
    "inventory_internal_order": {
        "namespace": r"Ignite\Inventory\Entities\InternalOrder",
        "v3": r"App\Models\StoreOrder", "alias": "store_order", "service": "inventory",
        # V2 status is 0/1; V3's is an enum of words — derived from V2's
        # cancelled / order_dispatched flags instead (v2_status is not posted)
        "renames": {"requesting_store": "requesting_store_id", "dispatching_store": "supplying_store_id",
                    "status": "v2_status"},
        "keep": ["order_number", "requesting_store_id", "supplying_store_id", "order_date", "delivery_date",
                 "status", "ordered_by", "dispatched_by", "cancellation_reason"],
        "inject": {"order_number": ("prefix", "IO-", "id"), "order_date": ("copy", "created_at"),
                   "status": ("flags", [("cancelled", "cancelled"), ("order_dispatched", "dispatched")], "pending")},
        "fk": {"requesting_store_id": "store", "supplying_store_id": "store", "ordered_by": "users",
               "dispatched_by": "users"},
        "critical": ["requesting_store_id", "supplying_store_id", "ordered_by"],   # all NOT NULL
        "null_if_unresolved": ["dispatched_by"],
    },
    "inventory_internal_order_details": {
        "namespace": r"Ignite\Inventory\Entities\InternalOrderDetails",
        "v3": r"App\Models\StoreOrderItem", "alias": "store_order_item", "service": "inventory",
        "renames": {"internal_order": "store_order_id", "quantity": "quantity_requested"},
        "keep": ["store_order_id", "product_id", "quantity_requested"],
        "fk": {"store_order_id": "store_order", "product_id": "product"},
        "critical": ["store_order_id", "product_id"],
    },
    "inventory_purchase_order_details": {
        "namespace": r"Ignite\Inventory\Entities\PurchaseOrderDetails",
        "v3": r"App\Models\PurchaseOrderItem", "alias": "purchase_order_item", "service": "inventory",
        "renames": {"quantity": "quantity_ordered", "total": "line_total", "received_quantity": "quantity_received"},
        "keep": ["purchase_order_id", "product_id", "unit_cost", "pack_size", "quantity_ordered", "line_total",
                 "quantity_received"],
        "fk": {"purchase_order_id": "purchase_order", "product_id": "product"},
        "critical": ["purchase_order_id", "product_id"],
    },
    "inventory_requisition_details": {
        "namespace": r"Ignite\Inventory\Entities\RequisitionDetails",
        "v3": r"App\Models\RequisitionItem", "alias": "requisition_item", "service": "inventory",
        "keep": ["requisition_id", "product_id", "quantity_requested", "quantity_approved"],
        "fk": {"requisition_id": "requisition", "product_id": "product"},
        "critical": ["requisition_id", "product_id"],
    },
    # ── diagnoses (ICD10 notes) ────────────────────────────────────────────
    # V2 keeps the diagnosis as a link to an ICD10 variation; the code and
    # name are joined in by FETCH_SQL below (variation_code / variation_name).
    # diagnosis_code_id is nullable in V3 — left empty (no V3 code lookup yet).
    "evaluation_icd10_notes": {
        "namespace": r"Ignite\Evaluation\Entities\Icd10Notes",
        "namespaces": [r"Ignite\Evaluation\Entities\Icd10Note"],
        "v3": r"App\Models\Diagnosis", "alias": "diagnoses", "service": "evaluation",
        "renames": {"user_id": "recorded_by"},
        "keep": ["visit_id", "patient_id", "diagnosis_text", "diagnosis_date", "recorded_by"],
        "inject": {"patient_id": ("patient_for_visit",),
                   "diagnosis_text": ("join_or", ["variation_code", "variation_name"], "code"),
                   "diagnosis_date": ("copy", "created_at")},
        "fk": {"visit_id": "visit", "patient_id": "patient", "recorded_by": "users"},
        "critical": ["visit_id", "patient_id"],                    # both NOT NULL
        "null_if_unresolved": ["recorded_by"],
    },
    # ── core-service ──────────────────────────────────────────────────────
    "inventory_store_departments": {
        "namespace": r"Ignite\Inventory\Entities\StoreDepartment",
        "namespaces": [r"Ignite\Inventory\Entities\StoreDepartments"],
        "v3": r"App\Models\Department", "alias": "departments", "service": "core",
        "keep": ["name", "description"],                           # code is unique + nullable: left empty
    },
    "inventory_goods_received_details": {
        "namespace": r"Ignite\Inventory\Entities\GoodsReceivedDetails",
        "v3": r"App\Models\GoodsReceivedItem", "alias": "goods_received_item", "service": "inventory",
        "renames": {"goods_receipt_id": "grn_id", "ordered_quantity": "quantity_ordered",
                    "received_quantity": "quantity_received", "bonus": "bonus_quantity",
                    "discount": "discount_amount", "tax": "tax_amount"},
        "keep": ["grn_id", "product_id", "unit_cost", "pack_size", "quantity_ordered", "quantity_received",
                 "bonus_quantity", "discount_amount", "tax_amount", "line_total"],
        "inject": {"line_total": ("mul", "quantity_received", "receiving_price")},
        "fk": {"grn_id": "goods_received_note", "product_id": "product"},
        "critical": ["grn_id", "product_id"],
    },
}

# V2 namespaces that are another spelling of a table the hand-written mappings
# already cover — same transform, nothing new to define.
NAMESPACE_ALIASES: dict[str, str] = {
    # the V2 class behind patient_insurance (reception_patient_scheme transform)
    r"Ignite\Reception\Entities\PatientInsurance": r"Ignite\Reception\Entities\PatientSchemes",
}

# CLEAN objects with no RAW rows (loaded straight into CLEAN) that are still
# real V2 tables: the namespace to migrate them as.
CLEAN_ONLY: dict[str, str] = {}

# Same V2 entity under several CLEAN names: only the canonical one is migrated
# (posting each copy would put the same records into V3 two or three times).
DUPLICATES: dict[str, str] = {
    "reception_patient_insurance": "patient_insurance",      # same rows + flattened scheme columns
    "reception_patient_schemes":   "patient_insurance",      # CLEAN-only copy, fewer rows
    "inventory_inventory_products":   "inventory_products",
    "inventory_inventory_units":      "inventory_units",
    "inventory_inventory_categories": "inventory_categories",
    "payments_mpesa": "finance_payments_mpesa",
    "payments_cash":  "finance_payments_cash",
    "payments_card":  "finance_payments_card",
    "notifications":  "core_notifications",
    "inpatient_inpatient_visit_rebates": "inpatient_visit_rebates",
    "theatre_theatre_before_leaving_theatres": "theatre_before_leaving_theatres",
}

# The same V2 table extracted twice under two names (an old-system-history
# copy and a sheet copy). Each copy holds rows the other lacks (measured on
# KISUMU_CLEAN 2026-10-11, e.g. admissions 573 only-here, investigations 1,929
# here vs 82,543 only in the twin), so both are migrated: the table on the
# left posts only the ids its twin doesn't have — no record is posted twice,
# none is lost. Only applies when the twin exists in the facility's CLEAN.
TWINS: dict[str, str] = {
    "admissions":                      "inpatient_admissions",
    "discharges":                      "inpatient_discharges",
    "evaluation_doctor_notes":         "doctor_notes",
    "evaluation_investigations":       "investigations",
    "inventory_evaluation_dispensing": "evaluation_dispensing",
    "reception_patients_nok":          "reception_next_of_kin",
    "evaluation_visits":               "visits",
    "reception_patients":              "patients",
}

# ─── tables that are not migrated, and why ──────────────────────────────────

SHARED = "V3 keeps this as shared reference data for every organisation (no tenant column) — not copied per org"
SETUP = "organisation setup — the destination org uses its own"
NO_MODEL = "no V3 model for this data — needs a decision from the V3 backend team"
LOG = "audit / view log or system bookkeeping — not part of the clinical or financial record"
PENDING_KEY = "theatre / dialysis model — gateway not reachable without MODEL_GATEWAY_MIGRATION_KEY"

NOT_MIGRATED: dict[str, str] = {
    # V3 model exists, but it isn't this data's place
    "evaluation_icd10_variations": SHARED, "symptoms": SHARED, "evaluation_tags": SHARED,
    "evaluation_reference_range": SHARED, "evaluation_investigation_approval_statuses": SHARED,
    "users_roles": SETUP, "users_permissions": SETUP, "activations": SETUP,
    "finance_account_groups": SETUP, "finance_account_types": SETUP,
    "settings_options": SETUP, "settings_settings": SETUP,
    "inventory_inventory_stocks": "V3 stock is per product + store (+ batch); V2 rows carry the product only",
    "inventory_customers": "V3 customer needs customer_no + name; V2 rows are patient-linked walk-ins",
    "inventory_batches": "V3 batch needs batch_no, product, cost; the one V2 row is a supplier delivery",
    "settings_destinations": "V3 core_service_destinations was reworked (destination_type_id …) and its "
                             "required columns are unclear — confirm with the V3 backend before mapping",
    "credit_notes": "V3 credit_note needs invoice_id, total_amount, reason; V2 notes are receipt-linked",
    "inpatient_visit_wards": "V2 rows are ward charges on a visit; V3 visit_ward is a bed allocation on an admission",
    "evaluation_sub_procedures": "no column in common with V3 subprocedures",
    "settings_scheme_exclusions": "V3 needs exclusion_type + exclusion_name; V2 holds only procedure / product ids",
    "finance_payments_mpesa": NO_MODEL, "finance_payments_cash": NO_MODEL, "finance_payments_card": NO_MODEL,
    "finance_payments_pesa_pal_mpesa": NO_MODEL, "finance_payments_pesa_pal_card": NO_MODEL,
    # no V3 model at all
    "evaluation_investigation_result_details": NO_MODEL, "finance_invoice_items": NO_MODEL,
    "evaluation_result_details_reference_ranges": NO_MODEL, "evaluation_consumables": NO_MODEL,
    "evaluation_prescription_payments": NO_MODEL, "evaluation_dispensing_details": NO_MODEL,
    "inventory_inventory_stock_adjustments": NO_MODEL, "evaluation_investigation_approvals": NO_MODEL,
    "evaluation_pos_sale_details": NO_MODEL, "finance_evaluation_payment_details": NO_MODEL,
    "inventory_store_products": NO_MODEL, "inventory_inventory_batch_product_sales": NO_MODEL,
    "finance_invoice_dispatches": NO_MODEL, "inventory_batch_sales": NO_MODEL, "invoice_payers": NO_MODEL,
    "inpatient_inpatient_visit_charges": NO_MODEL, "inventory_inventory_dispensing": NO_MODEL,
    "inpatient_visit_rebates": NO_MODEL, "evaluation_slit_lamp_exam": NO_MODEL,
    "evaluation_eyewear_prescriptions": NO_MODEL, "evaluation_dimensions": NO_MODEL,
    "evaluation_dimension_categories": NO_MODEL, "evaluation_systems_reviews": NO_MODEL,
    "evaluation_patient_meta": NO_MODEL, "evaluation_examination_review_details": NO_MODEL,
    "evaluation_consumable_types": NO_MODEL,
    "inpatient_nursing_care_plan": "V3 nursing_care_plan needs a care-plan date and diagnosis per row; V2 rows are free-text assessments",
    "inpatient_partial_bill_breakdowns": NO_MODEL, "finance_transactions": NO_MODEL,
    "core_approvals": NO_MODEL, "reception_time_trackers": NO_MODEL, "manual_data": NO_MODEL,
    "events": "Snowflake event table, not a V2 table", "events_lab_flags": "Snowflake derived table, not a V2 table",
    # logs / bookkeeping
    "inpatient_nurse_notes_view_logs": LOG, "inpatient_doctor_notes_view_logs": LOG,
    "inpatient_prescriptions_view_logs": LOG, "out_patient_notes_history_view_log": LOG,
    "inpatient_investigation_result_view_logs": LOG, "inpatient_history_view_logs": LOG,
    "doctor_notes_view_logs": LOG, "core_notifications": LOG, "system_logs": LOG,
    # theatre / dialysis
    "theatre_medic": PENDING_KEY, "theatre_operation_procedures": PENDING_KEY,
    "theatre_operation_investigations": PENDING_KEY, "theatre_before_leaving_theatres": PENDING_KEY,
    "theatre_theatre_before_anaesthesias": PENDING_KEY, "theatre_theatre_before_skin_incisions": PENDING_KEY,
    "theatre_theatre_payments": PENDING_KEY, "theatre_theatre_operation_pulses": PENDING_KEY,
    "theatre_theatre_operation_fluids": PENDING_KEY, "theatre_theatre_deposits": PENDING_KEY,
    "theatre_consumable_requirements": PENDING_KEY,
    "dialysis_patients": PENDING_KEY, "dialysis_chairs": PENDING_KEY, "dialysis_schedules": PENDING_KEY,
    "dialysis_machines": PENDING_KEY, "dialysis_wards": PENDING_KEY, "dialysis_ward_types": PENDING_KEY,
    "dialysis_sessions": PENDING_KEY, "dialysis_plants": PENDING_KEY, "dialysis_vascular_accesses": PENDING_KEY,
    "dialysis_coagulation_types": PENDING_KEY,
}


# ─── applying the mappings ──────────────────────────────────────────────────

def _injector(spec):
    kind, *args = spec
    if kind == "const":
        return lambda r: args[0]
    if kind == "copy":
        return lambda r: r.get(args[0])
    if kind == "upper":
        return lambda r: str(r.get(args[0]) or "").strip().upper()[:50] or None
    if kind == "prefix":
        return lambda r: f"{args[0]}{r.get(args[1])}" if r.get(args[1]) is not None else None
    if kind == "join":
        return lambda r: " ".join(str(r.get(a)) for a in args if r.get(a) not in (None, "")) or None
    if kind == "mul":
        def mul(r):
            try:
                return round(float(r.get(args[0])) * float(r.get(args[1])), 2)
            except (TypeError, ValueError):
                return None
        return mul
    if kind == "flags":
        pairs, default = args

        def flags(r):
            for col, value in pairs:
                if str(r.get(col) or "0") not in ("0", "False", "false", ""):
                    return value
            return default
        return flags
    if kind == "join_or":
        cols, fallback = args

        def join_or(r):
            joined = " ".join(str(r.get(c)).strip() for c in cols if str(r.get(c) or "").strip())
            return joined or (str(r.get(fallback)).strip() if str(r.get(fallback) or "").strip() else None)
        return join_or
    if kind == "admission_for_visit":
        return _admission_for_visit
    if kind == "patient_for_visit":
        return _patient_for_visit
    raise ValueError(f"unknown inject {spec!r}")


def transform_key(table: str) -> str:
    return f"gen_{table}"


def apply(v2v3, s2v3) -> int:
    """Register MAPPINGS / NAMESPACE_ALIASES in the migration's tables. Only
    adds: a namespace or transform key that already exists is left alone.
    Returns how many namespaces were added."""
    added = 0
    for table, spec in MAPPINGS.items():
        tk = transform_key(table)
        if spec["namespace"] in v2v3.NAMESPACE_MAP:
            continue
        v2v3.NAMESPACE_MAP[spec["namespace"]] = {"v3": spec["v3"], "transform": tk}
        v2v3._PER_KEY_RENAMES.setdefault(tk, dict(spec.get("renames") or {}))
        v2v3._PER_KEY_KEEP_ONLY.setdefault(tk, set(spec["keep"]))
        if spec.get("fk"):
            v2v3._FK_REMAP.setdefault(tk, dict(spec["fk"]))
        if spec.get("inject"):
            v2v3._PER_KEY_INJECT.setdefault(tk, {f: _injector(s) for f, s in spec["inject"].items()})
        if spec.get("critical"):
            s2v3._CRITICAL_FK_FIELDS.setdefault(tk, list(spec["critical"]))
        if spec.get("null_if_unresolved"):
            s2v3._NULL_IF_UNRESOLVED.setdefault(tk, set(spec["null_if_unresolved"]))
        # a row whose visit was never extracted for the facility can never be
        # linked — skipped and counted, not held back on every run
        if "visit_id" in spec.get("critical", []) and (spec.get("fk") or {}).get("visit_id") == "visit":
            s2v3._SKIP_IF_PARENT_NOT_IN_FACILITY.setdefault(tk, [("visit_id", "visits", "id")])
        s2v3._ALIAS_OVERRIDE.setdefault(tk, spec["alias"])
        s2v3._SERVICE_OVERRIDE.setdefault(tk, spec["service"])
        added += 1
    for table, spec in MAPPINGS.items():          # other spellings of the same V2 class
        for ns in spec.get("namespaces", []):
            if ns not in v2v3.NAMESPACE_MAP and spec["namespace"] in v2v3.NAMESPACE_MAP:
                v2v3.NAMESPACE_MAP[ns] = dict(v2v3.NAMESPACE_MAP[spec["namespace"]])
                added += 1
    for ns, same_as in NAMESPACE_ALIASES.items():
        if ns not in v2v3.NAMESPACE_MAP and same_as in v2v3.NAMESPACE_MAP:
            v2v3.NAMESPACE_MAP[ns] = dict(v2v3.NAMESPACE_MAP[same_as])
            added += 1
    return added


# ─── tables read with their own query ───────────────────────────────────────

def _icd10_notes_sql(clean: str, cols_of, clean_set: set[str]) -> str | None:
    """ICD10 notes with their variation's code and name. The new (uuid-era)
    extract links the variation by variation_uuid, the old one by
    variation_id — join on whichever both sides have."""
    var = "evaluation_icd10_variations"
    if var not in clean_set:
        return None
    notes, variations = cols_of("evaluation_icd10_notes"), cols_of(var)
    if not {"code", "name", "id"} <= variations:
        return None
    on = ['TO_VARCHAR(v."ID") = TO_VARCHAR(n."VARIATION_ID")'] if "variation_id" in notes else []
    if "variation_uuid" in notes and "uuid" in variations:
        on.append('v."UUID" = n."VARIATION_UUID"')
    if not on:
        return None
    return (f'SELECT n.*, v."CODE" AS "VARIATION_CODE", v."NAME" AS "VARIATION_NAME" '
            f'FROM {clean}.EVALUATION_ICD10_NOTES n LEFT JOIN {clean}.{var.upper()} v ON ' + " OR ".join(on))


# table → builder(clean schema, cols_of(table) → lower-case column names, CLEAN
# object names) → SQL, or None to read the table as is
FETCH_SQL = {"evaluation_icd10_notes": _icd10_notes_sql}


def duplicate_of(table: str, clean_set=None) -> str | None:
    """The canonical copy this table duplicates — only when that copy is in
    the facility's CLEAN too (elsewhere this table may be the only copy;
    clean_set None = assume it is)."""
    canonical = DUPLICATES.get(table.lower())
    if canonical and (clean_set is None or canonical in clean_set):
        return canonical
    return None


def coverage(entries: list[dict], clean_tables) -> dict:
    """Every CLEAN table → migrated | duplicate | not_migrated | UNACCOUNTED.
    `entries` are discover_tables() results (with v3/transform), `clean_tables`
    the names of the facility's CLEAN objects."""
    mapped = {e["table"].lower() for e in entries if e.get("transform") and e.get("v3")}
    clean_set = {str(t).lower() for t in clean_tables}
    out = {}
    for t in sorted(clean_set):
        if duplicate_of(t, clean_set):
            out[t] = f"duplicate of {DUPLICATES[t]}"
        elif t in mapped:
            out[t] = "migrated"
        elif t in NOT_MIGRATED:
            out[t] = f"not migrated: {NOT_MIGRATED[t]}"
        else:
            out[t] = "UNACCOUNTED"
    return out
