"""
v3_to_v3_links.py — how a V3 row's id columns map to other gateway models,
for copying one organisation into another (v3_to_v3_migration.py).

A source row's link holds a SOURCE id; on the target it must hold the id the
same record got there. Each id column is one of:

  link      → "service/alias": remapped through that model's id map
  CRITICAL  the row can't exist without that parent (a visit's patient, a
            result's investigation …): held back until the parent is copied
            (for a model without a tenant column: a row whose parent isn't
            in the source org is another org's row — not copied at all)
  FACILITY  the target facility
  DROP      no gateway model holds what it points at (batches, payments,
            Dynamics ids …): copied as null, counted in the plan

`audit_links` (the plan / test step) checks every rule against the source's
own data — a rule pointing at the wrong model shows up as unresolvable ids —
and lists id-like columns no rule covers, so a copy never carries a source id
into the target unmapped.
"""
from __future__ import annotations

USERS = "core/users"
FACILITY = "@facility"
DROP = "@drop"

# column → target, for every model unless MODEL_LINKS says otherwise
LINKS: dict[str, str] = {
    "patient_id": "reception/patient", "patient": "reception/patient",
    "visit_id": "evaluation/visits", "visit": "evaluation/visits",
    "admission_id": "inpatient/admission",
    "admission_request_id": "inpatient/admission_request",
    "admission_type_id": "inpatient/admission_type",
    "age_group_id": "evaluation/age_groups",
    "bed_type_id": "inpatient/bed_type", "preferred_bed_type_id": "inpatient/bed_type",
    "ward_id": "inpatient/ward", "preferred_ward_id": "inpatient/ward",
    "booking_id": "theatre/theatre_booking", "theatre_booking_id": "theatre/theatre_booking",
    "collection_method_id": "evaluation/sample_collection_methods",
    "department_id": "core/departments",
    "destination_id": "core/service_destinations",
    "discharge_request_id": "inpatient/discharge_request",
    "discharge_type_id": "inpatient/discharge_type",
    "evaluation_procedure_id": "evaluation/procedures", "procedure_id": "evaluation/procedures",
    "procedure": "evaluation/procedures", "parent_procedure_id": "evaluation/procedures",
    "sub_test_id": "evaluation/procedures", "operation_procedure_id": "evaluation/procedures",
    "investigation_id": "evaluation/investigations",
    "investigation_result_id": "evaluation/investigation_results",
    "result_detail_id": "evaluation/investigation_result_details",
    "insurance_scheme_id": "finance/insurance_scheme", "scheme_id": "finance/insurance_scheme",
    "insurance_company_id": "finance/insurance_company",
    "medic_type_id": "theatre/theatre_medic_type",
    "operation_id": "theatre/theatre_operation",
    "schedule_id": "theatre/theatre_schedule",
    "theatre_id": "theatre/theatre",
    "theatre_type_id": "theatre/theatre_type",
    "theatre_deposit_id": "theatre/theatre_deposit",
    "parent_note_id": "evaluation/doctor_notes",
    "store_id": "inventory/store", "parent_store_id": "inventory/store", "from_store_id": "inventory/store",
    "to_store_id": "inventory/store", "requesting_store_id": "inventory/store",
    "supplying_store_id": "inventory/store",
    "payment_mode_id": "finance/payment_mode",
    "payment_term_id": "inventory/payment_term",
    "pos_config_id": "core/pos_configs",
    "product_id": "inventory/product", "drug": "inventory/product",
    "recollected_from_sample_id": "evaluation/samples",
    "region_id": "core/regions",
    "store_order_id": "inventory/store_order",
    "tax_category_id": "inventory/tax_category",
    "unit_id": "inventory/unit",
    "title_id": "evaluation/haemogram_titles",
    "treatment_location_id": "evaluation/treatment_locations",
    "next_of_kin_id": "reception/patient_next_of_kin",
    "requested_facility_id": FACILITY, "facility_id": FACILITY,
    # people
    "user_id": USERS, "user": USERS, "doctor_id": USERS, "consultant_id": USERS,
    "admitting_doctor_id": USERS, "consulting_doctor_id": USERS, "staff_id": USERS,
    "head_user_id": USERS, "nurse_in_charge_id": USERS, "procuring_officer_id": USERS,
    # nothing on the gateway holds these
    "batch_id": DROP, "variant_id": DROP, "brand_id": DROP, "chemical_name_id": DROP,
    "formulation_id": DROP, "manufacturer_id": DROP, "tax_id": DROP, "gl_account_id": DROP,
    "reference_id": DROP, "finance_payment_id": DROP, "payment_details_id": DROP,
    "service_sale_id": DROP, "walkin_sale_id": DROP, "batch_sale_id": DROP, "split_bill_id": DROP,
    "shift_id": DROP, "corporate_id": DROP, "device_id": DROP, "requisition_id": DROP,
    "package_id": DROP, "refraction_id": DROP, "temperature_location_id": DROP,
    "employment_type_id": DROP, "specialty_id": DROP, "relationship_id": DROP,
}

# per model: column → target (overrides LINKS); None = not an id here
MODEL_LINKS: dict[str, dict[str, str | None]] = {
    "core/departments": {"parent_id": "core/departments"},
    # staff profiles (gateway model since 2026-10-15): user_id/department_id via LINKS
    "core/user_profile": {"speciality_id": "core/specialties",
                          "employee_category_id": "core/employee_categories",
                          "partner_institution_id": "core/partner_institutions"},
    "core/insurance_schemes": {"company_id": "core/insurance_companies"},
    "finance/insurance_scheme": {"company_id": "finance/insurance_company"},
    "finance/invoice": {"company_id": "finance/insurance_company"},
    "finance/insurance_scheme_pricing": {"item_id": "evaluation/procedures"},
    "evaluation/icd10_subcategories": {"category_id": "evaluation/icd10_categories"},
    "evaluation/icd10_types": {"sub_category_id": "evaluation/icd10_subcategories"},
    "evaluation/icd10_variations": {"type_id": "evaluation/icd10_types"},
    "evaluation/samples": {"type_id": "evaluation/sample_types", "clinic_id": DROP},
    "evaluation/procedure_categories": {"parent_category_id": "evaluation/procedure_categories"},
    "evaluation/procedures": {"category": "evaluation/procedure_categories", "sub_category_id": DROP},
    "evaluation/subprocedures": {"procedure": "evaluation/procedures"},
    "evaluation/template_labs": {"subtest_id": "evaluation/procedures"},
    "evaluation/diagnoses": {"diagnosis_code_id": "evaluation/icd10_variations"},
    "inventory/product": {"category_id": "inventory/product_category"},
    "inventory/product_category": {"parent_category_id": "inventory/product_category"},
    "reception/visit_destination": {"clinic_id": DROP, "room_id": DROP},
    "inpatient/admission": {"insurance_id": "reception/patient_insurance"},
    "theatre/theatre_booking": {"status_id": "theatre/theatre_scheduling_status"},
    "evaluation/visits": {"unique_id": None, "sha_patient_cr_id": None},
    "evaluation/investigations": {"invoiced": None},
    "finance/receipt": {"invoice_id": "finance/invoice"},
    "inpatient/visit_ward": {"bed_id": "inpatient/bed"},
    # polymorphic, but every row of kisumu_v3 is priceable_type = 'procedure'.
    # scheme_id: core-service's own schemes (FK → core_insurance_schemes), NOT finance's — the two
    # id ranges overlap, so the finance map silently links the wrong scheme (found 2026-10-16)
    "core/scheme_pricing": {"priceable_id": "evaluation/procedures", "scheme_id": "core/insurance_schemes"},
    "core/scheme_exclusions": {"scheme_id": "core/insurance_schemes", "item_id": "evaluation/procedures"},
}

# Columns a V2 → V3 load left holding V2 ids (kisumu_v3, audited 2026-10-15:
# invoice.visit = 311001 where org 4's visits are 104394–158514, prescriptions
# .drug = 34781 where its products are 1–10040 …). The copy translates them
# through that load's id map (V2 id → source V3 id) first; what the map
# doesn't have is copied as null (optional) or held (critical) and counted.
# (model, column) → alias in the V2→V3 id map
V2_KEYED: dict[tuple[str, str], str] = {
    ("finance/invoice", "visit"): "visits", ("finance/invoice", "visit_id"): "visits",
    ("finance/invoice", "user_id"): "users", ("finance/invoice", "company_id"): "insurance_company",
    ("finance/invoice", "scheme_id"): "insurance_scheme",
    ("evaluation/prescriptions", "drug"): "product", ("evaluation/prescriptions", "store_id"): "store",
    ("evaluation/prescriptions", "admission_request_id"): "admission_request",
    ("evaluation/investigations", "admission_id"): "admission",
    ("evaluation/investigations", "date_changed_by"): "users",
    ("evaluation/investigation_results", "approved_by"): "users",
    ("evaluation/investigation_results", "published_by"): "users",
    ("evaluation/examination_reviews", "user_id"): "users",
    ("evaluation/samples", "user_id"): "users",
    ("evaluation/visits", "marked_as_em_by"): "users",
    ("reception/patient_random_note", "patient_id"): "patient",
    ("evaluation/subprocedures", "procedure"): "procedures",
}

# A link whose value isn't one of the source org's ids can still be resolved by the
# parent's code: (model, column) → (parent model, parent's code column, the row's code
# column, how many leading characters of the row's code are the parent's code).
# ICD-10 variations are shared reference data: their type_id points at whichever org's
# copy of the 1,910 types loaded them (2026-10-16: 73,125 org 4's own, 11,200 another
# org's) — a variation A00.1 belongs to type A00; where both resolve they agree 55,557/55,557
LINK_BY_CODE: dict[tuple[str, str], tuple[str, str, str, int]] = {
    ("evaluation/icd10_variations", "type_id"): ("evaluation/icd10_types", "code", "code", 3),
    # a block A00-A09 belongs to the chapter whose range contains it (A00-B99); 0 = range match.
    # 53 of org 4's 283 point at another org's copy of the 21 chapters (category_id 1–4)
    ("evaluation/icd10_subcategories", "category_id"): ("evaluation/icd10_categories", "code", "code", 0),
}

# Links the source itself has broken beyond repair (V2 ids no map holds):
# copied as null and reported — the plan doesn't fail on them.
# (model, column) → why
ACCEPT_UNRESOLVED: dict[tuple[str, str], str] = {
    ("evaluation/samples", "type_id"): "V2 sample type ids (1–119); org 4's are 244–362 and no id map has them",
    ("evaluation/samples", "collection_method_id"): "V2 collection method ids (1–8); org 4's are 18–25, no map",
    ("evaluation/investigations", "treatment_location_id"): "V2 treatment location ids; org 4's are 5–8, no map",
    ("evaluation/prescriptions", "treatment_location_id"): "V2 treatment location ids; org 4's are 5–8, no map",
    ("evaluation/icd10_subcategories", "category_id"): "some categories are not org 4's (shared ICD-10 data)",
}

# links a row can't exist without (held back / not this org's row when unresolved)
CRITICAL: dict[str, tuple[str, ...]] = {
    "reception/patient_insurance": ("patient_id",),
    "reception/patient_next_of_kin": ("patient_id",),
    "reception/patient_document": ("patient_id",),
    "reception/patient_random_note": ("patient_id",),
    "reception/visit_consultant": ("visit_id",),
    "reception/visit_destination": ("visit_id",),
    "evaluation/visits": ("patient",),
    "evaluation/vitals": ("visit",),
    "evaluation/doctor_notes": ("visit_id",),
    "evaluation/diagnoses": ("visit_id",),
    "evaluation/investigations": ("visit",),
    "evaluation/prescriptions": ("visit",),
    "evaluation/samples": ("investigation_id",),
    "evaluation/investigation_results": ("investigation_id",),
    "evaluation/investigation_result_details": ("investigation_result_id",),
    "evaluation/result_detail_ranges": ("result_detail_id",),
    "evaluation/examination_reviews": ("visit_id",),
    "evaluation/eyewear_prescriptions": ("visit_id",),
    "evaluation/slit_lamp_exams": ("visit_id",),
    "evaluation/ophthalmology_notes": ("visit_id",),
    "evaluation/immunization_notes": ("visit_id",),
    "evaluation/procedure_prices": ("evaluation_procedure_id",),
    "evaluation/critical_values": ("procedure_id",),
    "evaluation/reference_ranges": ("procedure_id",),
    "evaluation/haemogram_titles": ("procedure_id",),
    "evaluation/sample_types": (),
    "finance/invoice": ("visit",),
    "finance/insurance_scheme_pricing": ("scheme_id", "item_id"),
    "finance/patient_account": ("patient_id",),
    "inpatient/admission": ("patient_id", "visit_id"),
    "inpatient/admission_request": ("patient_id",),
    "inpatient/discharge": ("admission_id",),
    "inpatient/discharge_request": ("admission_id",),
    "inpatient/daily_charges_verification": ("admission_id",),
    "inpatient/note": ("admission_id",),
    "inpatient/vital": ("admission_id",),
    "inpatient/fluid_balance": ("admission_id",),
    "inpatient/blood_transfusion": ("admission_id",),
    "inpatient/bed": ("ward_id",),
    "inventory/stock": ("product_id", "store_id"),
    "inventory/stock_movement": ("product_id",),
    "inventory/stock_take": ("store_id",),
    "inventory/store_order_item": ("store_order_id",),
    "inventory/sale": (),
    "theatre/theatre_booking": ("patient_id",),
    "theatre/theatre_schedule": ("booking_id",),
    "theatre/theatre_operation": ("schedule_id",),
    "theatre/theatre_operation_medic": ("operation_id",),
    "theatre/theatre_operation_procedure": ("operation_id",),
    "theatre/theatre_operation_pulse": ("operation_id",),
    "theatre/theatre_anaesthetist_check": ("booking_id",),
    "theatre/theatre_doctor_check": ("booking_id",),
    "theatre/theatre_nurse_check": ("booking_id",),
}

# models never copied: the target instance keeps its own
SKIP_MODELS: dict[str, str] = {
    "core/organizations": "the organisation itself — the target org already exists",
    "core/facilities": "facilities belong to the target org's own setup (rows link to the target facility)",
    "evaluation/facilities": "facilities belong to the target org's own setup",
    # the source org's own audit trail (who changed what, when): polymorphic
    # subject/causer ids, and the target writes its own as rows go in
    **{f"{svc}/activity_log": "the source's audit trail — the target keeps its own"
       for svc in ("core", "reception", "evaluation", "inventory", "finance", "theatre", "inpatient", "dialysis")},
}

# V3 tables the copy can't reach because the gateway has no model for them (checked 2026-10-15 on every
# service) — they're copied with SQL by a DBA instead, so a "complete" copy still needs these done
NOT_IN_GATEWAY: dict[str, str] = {
    # (core user_profiles had none until 2026-10-15 — it's core/user_profile now and copies like any model)
}

# A key unique across the whole instance (not per org) that another org's row
# already holds: the row is created under "<value>.<target org id>", everything
# else as in the source (decided 2026-10-16 for org 4 → 284: 230 usernames were
# held by another org's copies). model → column
RENAME_ON_CLASH: dict[str, str] = {
    "core/users": "username",
    # instance-wide numbers another org's copy already took (2026-10-16, org 284): same rule
    "inpatient/admission": "admission_number",
    "finance/invoice": "invoice_no",
    "core/pos_configs": "code",
    "core/service_destinations": "code",
}

# Fields the TARGET computes itself (not stored): a patch can't set them, so they're reported, never
# patched. model → {column: why}
TARGET_COMPUTED: dict[str, dict[str, str]] = {
    "evaluation/visits": {"patient_full_name": "org 284 derives it from the visit's patient and returns it empty "
                                               "(likely the facility 247 vs 283 mismatch on its patients)"},
}

# models with no tenant column that are reference data shared by every org on
# an instance: linked to the target's own row (uuid, then these fields), and
# inserted only when the target lacks it
REFERENCE_MATCH: dict[str, tuple[str, ...]] = {
    "evaluation/icd10_variations": ("code",),
    "evaluation/investigation_approval_statuses": ("name",),
    "evaluation/procedure_tags": ("name",),
    "evaluation/symptoms": ("name",),
    "evaluation/subprocedures": ("name",),
    "finance/bank": ("code", "name"),
    "inpatient/bed_type": ("name",),
}

# never sent: the gateway sets them, or they're the source's own bookkeeping.
# created_at / updated_at ARE sent (the gateway takes them, checked on org 284
# 2026-10-15), as the source's wall-clock time — see Copier.translate
NOT_COPIED = {"id", "organization_id", "deleted_at"}
# never compared by verify (the gateway / target sets them)
NOT_COMPARED = NOT_COPIED | {"password", "remember_token", "email_token", "signature",
                             "last_login", "last_login_ip"}


def is_instance_derived(col: str) -> bool:
    """Values each V3 instance computes with its own key (mobile_hash,
    id_no_hash …): the target recomputes them, so they can't — and mustn't —
    equal the source's."""
    return col.endswith("_hash")

_ID_SUFFIXES = ("_id", "_by")


def is_user_column(col: str) -> bool:
    return col.endswith("_by")


def link_target(model: str, col: str) -> str | None:
    """'service/alias', FACILITY, DROP, or None (not a link)."""
    per = MODEL_LINKS.get(model, {})
    if col in per:
        return per[col]
    if col in LINKS:
        return LINKS[col]
    if is_user_column(col):
        return USERS
    return None


def looks_like_id(col: str) -> bool:
    return col.endswith(_ID_SUFFIXES) and col not in ("id", "uuid", "organization_id")


def critical_cols(model: str) -> tuple[str, ...]:
    return CRITICAL.get(model, ())
