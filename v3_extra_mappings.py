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


# Surgical safety checklists with no certifier in V2 (all 92 kisumu_v3 rows):
# the operation's lead surgeon stands in (joined in as certifier_user_id,
# _checklist_certifier_sql) and the signature field records that it's a
# stand-in. Order matters: the note is decided before certified_by is filled.
_STAND_IN_CERTIFIER_INJECT = {
    "certified_by_signature": ("if_empty", "certified_by",
                               "Certifier not recorded in V2 — set to the operation's lead surgeon at migration"),
    "certified_by": ("copy", "certifier_user_id"),
}

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
    "ophthalmology_notes": {
        "namespace": r"Ignite\Evaluation\Entities\OphthalmologyNotes",
        "v3": r"App\Models\OphthalmologyNotes", "alias": "ophthalmology_notes", "service": "evaluation",
        "keep": ["visit_id", "patient_id", "user_id", "ocular_history", "presenting_complaints",
                 "past_medical_history", "systems_review", "examination", "investigations", "applanation",
                 "diagnosis", "treatment_plan", "comments", "additional_examination", "recorded_date"],
        "inject": {"patient_id": ("patient_for_visit",), "recorded_date": ("copy", "created_at")},
        "fk": {"visit_id": "visit", "patient_id": "patient", "user_id": "users"},
        "critical": ["visit_id"],
        "null_if_unresolved": ["patient_id", "user_id"],
    },
    "immunization_notes": {
        "namespace": r"Ignite\Evaluation\Entities\ImmunizationNote",
        "v3": r"App\Models\ImmunizationNote", "alias": "immunization_notes", "service": "evaluation",
        "renames": {"lot_number": "vaccine_batch_no", "dose": "dose_number", "date_given": "administration_date"},
        "keep": ["visit_id", "patient_id", "vaccine_name", "vaccine_batch_no", "dose_number",
                 "administration_date", "notes"],
        "inject": {"patient_id": ("patient_for_visit",), "notes": ("prefix", "Manufacturer: ", "manufacturer")},
        "fk": {"visit_id": "visit", "patient_id": "patient"},
        "critical": ["visit_id"],
        "null_if_unresolved": ["patient_id"],
    },
    # ── evaluation-service: lab result values ────────────────────────────
    # V3 investigation_results carry no values (result_value / unit null on
    # migrated rows) — the values are V2's result details: one row per test
    # line (test, result, unit, flag, reference range). The gateway now
    # exposes both tables (no tenant column: children of a tenant-scoped
    # result) under V2's own table names; columns posted as V2 has them, and a
    # column V3 lacks is dropped by the poster's unknown-column retry.
    "evaluation_investigation_result_details": {
        "namespace": r"Ignite\Evaluation\Entities\InvestigationResultDetails",
        "v3": r"App\Models\InvestigationResultDetails", "alias": "investigation_result_details",
        "service": "evaluation",
        "keep": ["investigation_result_id", "visit_id", "test", "sub_test_id", "result", "result_value_type",
                 "unit", "flag", "reference_ranges", "title", "sub_title", "title_sub_text", "type_wysiwg",
                 "appear_bold"],
        # appear_bold is NOT NULL; sub_test_id is an FK to V3 procedures (probe 2026-10-12)
        "inject": {"appear_bold": ("const", 0)},
        "fk": {"investigation_result_id": "investigation_results", "visit_id": "visit", "sub_test_id": "procedures"},
        "critical": ["investigation_result_id"],
        "null_if_unresolved": ["visit_id", "sub_test_id"],
    },
    "evaluation_result_details_ranges": {
        "namespace": r"Ignite\Evaluation\Entities\ResultDetailsReferenceRanges",
        "v3": r"App\Models\ResultDetailsReferenceRanges", "alias": "result_detail_ranges", "service": "evaluation",
        "keep": ["result_detail_id", "type", "lower", "upper", "flag", "bio_reference", "lg_type", "lg_value"],
        "fk": {"result_detail_id": "investigation_result_details"},
        "critical": ["result_detail_id"],
    },
    # ── setup: scheme prices and service destinations ─────────────────────
    # V2 scheme "exclusions" are per-scheme procedure prices (2,639 priced, 3
    # excluded). V2 schemes migrate to FINANCE insurance_scheme, so the prices
    # go to finance insurance_scheme_pricing; core scheme_pricing /
    # scheme_exclusions link CORE insurance schemes, which hold none of V2's.
    # standard_price is the procedure's cash price (FETCH_SQL).
    "settings_scheme_exclusions": {
        "namespace": r"Ignite\Settings\Entities\SchemeExclusion",
        "namespaces": [r"Ignite\Settings\Entities\SchemeExclusions"],
        "v3": r"App\Models\InsuranceSchemePricing", "alias": "insurance_scheme_pricing", "service": "finance",
        "renames": {"procedure_id": "item_id", "price": "scheme_price"},
        "keep": ["scheme_id", "item_id", "item_type", "standard_price", "scheme_price", "is_covered",
                 "requires_preauth", "facility_id"],
        "fk": {"scheme_id": "insurance_scheme", "item_id": "procedures"},
        "critical": ["scheme_id", "item_id"],
        "inject": {"item_type": ("const", "procedure"),
                   "is_covered": ("map", "excluded", {"1": 0, "true": 0}, 1)},
    },
    # V2 destination types 1 room / 2 department / 3 clinic → V3's static keys;
    # code is unique across V3, so it carries the facility and V2 id. Names V3
    # already has for the org (its seeded Laboratory, Triage …) are linked, not
    # re-added (v2v3._LINK_BY_FIELD).
    "settings_destinations": {
        "namespace": r"Ignite\Settings\Entities\SettingsDestination",
        "v3": r"App\Models\ServiceDestination", "alias": "service_destinations", "service": "core",
        "renames": {"hidden_from_normal_queues": "hidden_from_queues"},
        "keep": ["facility_id", "name", "slug", "code", "destination_type", "icon", "auto_checkin",
                 "hidden_from_queues", "associated_procedure_category"],
        "inject": {"destination_type": ("map", "destination_type_id",
                                        {"1": "room", "2": "department", "3": "specialized_clinic"}, "department"),
                   "code": ("fmt", "DST-{source_schema}-{id}")},
    },
    # ── theatre-service: operation details and checklists ────────────────
    # (_STAND_IN_CERTIFIER_INJECT: see _checklist_certifier_sql)
    # The theatre gateway hides SQL errors ("the database refused the row"),
    # so every payload holds only real V3 columns. V3 renamed almost every V2
    # checklist field; V2 yes/no values are on / yes / no / 1 / 0 / non.
    "theatre_operation_medics": {
        "namespace": r"Ignite\Theatre\Entities\TheatreMedic",
        "v3": r"App\Models\TheatreOperationMedic", "alias": "theatre_operation_medic", "service": "theatre",
        "keep": ["operation_id", "user_id", "medic_type_id", "role"],
        # role (NOT NULL enum) from the V2 medic type's name (joined in, FETCH_SQL)
        "inject": {"role": ("map", "medic_type_name", {"surgeon": "lead_surgeon", "anaesthetist": "anaesthetist",
                                                       "nurse": "scrub_nurse", "assistant": "assistant_surgeon"},
                            "assistant_surgeon")},
        "fk": {"operation_id": "theatre_operation", "user_id": "users", "medic_type_id": "theatre_medic_type"},
        "critical": ["operation_id", "user_id", "medic_type_id"],
    },
    "theatre_operation_procedures": {
        "namespace": r"Ignite\Theatre\Entities\OperationProcedure",
        "namespaces": [r"Ignite\Theatre\Entities\OperationProcedures"],     # the older extract's name
        "v3": r"App\Models\TheatreOperationProcedure", "alias": "theatre_operation_procedure", "service": "theatre",
        "renames": {"type": "v2_type"},
        "keep": ["operation_id", "procedure_id", "procedure_name", "procedure_notes"],
        # procedure_name NOT NULL — the V2 procedure's name (joined in, FETCH_SQL)
        "inject": {"procedure_notes": ("copy", "v2_type")},
        "fk": {"operation_id": "theatre_operation", "procedure_id": "procedures"},
        "critical": ["operation_id"],
        "null_if_unresolved": ["procedure_id"],
    },
    "theatre_operation_pulses": {
        "namespace": r"Ignite\Theatre\Entities\OperationPulses",
        "v3": r"App\Models\TheatreOperationPulse", "alias": "theatre_operation_pulse", "service": "theatre",
        "renames": {"value": "pulse_rate"},
        "keep": ["operation_id", "pulse_rate", "recorded_at"],
        "inject": {"recorded_at": ("copy", "created_at")},
        "fk": {"operation_id": "theatre_operation"},
        "critical": ["operation_id"],
    },
    "theatre_deposits": {
        "namespace": r"Ignite\Theatre\Entities\TheatreDeposit",
        "v3": r"App\Models\TheatreDeposit", "alias": "theatre_deposit", "service": "theatre",
        "keep": ["name", "amount", "description"],
    },
    "theatre_anaesthetist_checks": {
        "namespace": r"Ignite\Theatre\Entities\AnaesthetistCheck",
        "v3": r"App\Models\TheatreAnaesthetistCheck", "alias": "theatre_anaesthetist_check", "service": "theatre",
        "renames": {f: f"v2_{f}" for f in ("hb", "pvc", "electrolyte_normal", "temp", "sugar", "albumim", "systolic",
                                           "diastolic", "pulse", "dentures", "patient_fit_operation",
                                           "consent_given", "premedication_given")},
        "keep": ["booking_id", "hb", "pvc", "electrolyte_normal", "temperature", "blood_sugar", "albumin",
                 "systolic_bp", "diastolic_bp", "pulse", "has_dentures", "patient_fit_for_operation",
                 "consent_given", "premedication_given", "certified_by"],
        "inject": {"hb": ("number", "v2_hb"), "pvc": ("number", "v2_pvc"), "temperature": ("number", "v2_temp"),
                   "blood_sugar": ("number", "v2_sugar"), "albumin": ("number", "v2_albumim"),
                   "systolic_bp": ("number", "v2_systolic"), "diastolic_bp": ("number", "v2_diastolic"),
                   "pulse": ("number", "v2_pulse"), "electrolyte_normal": ("yesno", "v2_electrolyte_normal"),
                   "has_dentures": ("yesno", "v2_dentures"),
                   "patient_fit_for_operation": ("yesno", "v2_patient_fit_operation", 0),
                   "consent_given": ("yesno", "v2_consent_given", 0),
                   "premedication_given": ("yesno", "v2_premedication_given", 0)},
        "fk": {"booking_id": "theatre_booking", "certified_by": "users"},
        "critical": ["booking_id", "certified_by"],
    },
    "theatre_doctor_checks": {
        "namespace": r"Ignite\Theatre\Entities\DoctorCheck",
        "v3": r"App\Models\TheatreDoctorCheck", "alias": "theatre_doctor_check", "service": "theatre",
        "renames": {f: f"v2_{f}" for f in ("hydration_normal", "hb", "electrolyte_normal", "chest_normal",
                                           "bp_systolic", "diastolic", "pulse", "pvc", "temp",
                                           "relevant_medical_preoperative_preparation_completed",
                                           "patient_fit_operation", "blood_available", "consent_given")},
        "keep": ["booking_id", "hydration_normal", "hb", "electrolyte_normal", "chest_normal", "systolic_bp",
                 "diastolic_bp", "pulse", "pvc", "temperature", "preoperative_preparation_completed",
                 "patient_fit_for_operation", "blood_available", "consent_given", "certified_by"],
        "inject": {"hb": ("number", "v2_hb"), "pvc": ("number", "v2_pvc"), "temperature": ("number", "v2_temp"),
                   "systolic_bp": ("number", "v2_bp_systolic"), "diastolic_bp": ("number", "v2_diastolic"),
                   "pulse": ("number", "v2_pulse"), "hydration_normal": ("yesno", "v2_hydration_normal"),
                   "electrolyte_normal": ("yesno", "v2_electrolyte_normal"),
                   "chest_normal": ("yesno", "v2_chest_normal"),
                   "preoperative_preparation_completed":
                       ("yesno", "v2_relevant_medical_preoperative_preparation_completed", 0),
                   "patient_fit_for_operation": ("yesno", "v2_patient_fit_operation", 0),
                   "blood_available": ("yesno", "v2_blood_available"),
                   "consent_given": ("yesno", "v2_consent_given", 0)},
        "fk": {"booking_id": "theatre_booking", "certified_by": "users"},
        "critical": ["booking_id", "certified_by"],
    },
    "theatre_nurse_checks": {
        "namespace": r"Ignite\Theatre\Entities\NurseChecks",
        "namespaces": [r"Ignite\Theatre\Entities\NurseCheck"],
        "v3": r"App\Models\TheatreNurseCheck", "alias": "theatre_nurse_check", "service": "theatre",
        "renames": {f: f"v2_{f}" for f in ("gown_and_socks", "wigs", "jewellery", "dentures", "preparation",
                                           "gastric_tube", "xrays", "blood_available", "consent_given",
                                           "premedication_given", "bladder_check_urinary_catheter")},
        "keep": ["booking_id", "has_gown_and_socks", "has_wigs", "has_jewellery", "has_dentures",
                 "preparation_done", "urinalysis_sugar", "urinalysis_albumin", "has_gastric_tube", "has_xrays",
                 "blood_available", "consent_given", "premedication_given", "bladder_check_notes", "certified_by"],
        "inject": {"has_gown_and_socks": ("yesno", "v2_gown_and_socks"), "has_wigs": ("yesno", "v2_wigs"),
                   "has_jewellery": ("yesno", "v2_jewellery"), "has_dentures": ("yesno", "v2_dentures"),
                   "preparation_done": ("yesno", "v2_preparation", 0), "has_gastric_tube": ("yesno", "v2_gastric_tube"),
                   "has_xrays": ("yesno", "v2_xrays"), "blood_available": ("yesno", "v2_blood_available"),
                   "consent_given": ("yesno", "v2_consent_given", 0),
                   "premedication_given": ("yesno", "v2_premedication_given", 0),
                   "urinalysis_sugar": ("copy", "sugar"), "urinalysis_albumin": ("copy", "albumim"),
                   "bladder_check_notes": ("copy", "v2_bladder_check_urinary_catheter")},
        "fk": {"booking_id": "theatre_booking", "certified_by": "users"},
        "critical": ["booking_id", "certified_by"],
    },
    "theatre_before_anaesthesias": {
        "namespace": r"Ignite\Theatre\Entities\BeforeAnaesthesias",
        "v3": r"App\Models\TheatreBeforeAnaesthesia", "alias": "theatre_before_anaesthesia", "service": "theatre",
        "renames": {f: f"v2_{f}" for f in ("patient_confirmed", "site_marked", "anaesthesia_safety_check_completed",
                                           "pulse_oximeter_on_patient_functioning", "known_allergy",
                                           "difficult_airway_aspiration_risk", "risk_of_over_500ml_blood_loss")}
                   | {"known_allergy_marked_notes": "known_allergy_notes"},
        "keep": ["operation_id", "patient_confirmed", "site_marked", "site_marked_notes",
                 "anaesthesia_safety_check_completed", "pulse_oximeter_functioning", "known_allergy",
                 "known_allergy_notes", "difficult_airway_risk", "blood_loss_risk", "certified_by",
                 "certified_by_signature"],
        "inject": {**_STAND_IN_CERTIFIER_INJECT,
                   "patient_confirmed": ("yesno", "v2_patient_confirmed", 0), "site_marked": ("yesno", "v2_site_marked"),
                   "anaesthesia_safety_check_completed": ("yesno", "v2_anaesthesia_safety_check_completed", 0),
                   "pulse_oximeter_functioning": ("yesno", "v2_pulse_oximeter_on_patient_functioning"),
                   "known_allergy": ("yesno", "v2_known_allergy"),
                   "difficult_airway_risk": ("yesno", "v2_difficult_airway_aspiration_risk"),
                   "blood_loss_risk": ("yesno", "v2_risk_of_over_500ml_blood_loss")},
        "fk": {"operation_id": "theatre_operation", "certified_by": "users"},
        "critical": ["operation_id", "certified_by"],
    },
    "theatre_before_skin_incisions": {
        "namespace": r"Ignite\Theatre\Entities\TheatreBeforeSkinIncision",
        "v3": r"App\Models\TheatreBeforeSkinIncision", "alias": "theatre_before_skin_incision", "service": "theatre",
        "renames": {"confirm_members": "v2_confirm_members",
                    "surgeon_anaesthesia_nurse_confirm": "v2_surgeon_anaesthesia_nurse_confirm",
                    "antibiotic_prophylaxis_sixty_minutes": "v2_antibiotic_prophylaxis",
                    "essential_imaging_displayed": "v2_essential_imaging_displayed",
                    "nurse_team_review_notes": "nursing_team_review_notes"},
        "keep": ["operation_id", "team_members_confirmed", "surgeon_anaesthesia_nurse_confirmed",
                 "surgeon_review_notes", "anaesthesia_team_review_notes", "nursing_team_review_notes",
                 "antibiotic_prophylaxis_given", "essential_imaging_displayed", "certified_by",
                 "certified_by_signature"],
        "inject": {**_STAND_IN_CERTIFIER_INJECT,
                   "team_members_confirmed": ("yesno", "v2_confirm_members", 0),
                   "surgeon_anaesthesia_nurse_confirmed": ("yesno", "v2_surgeon_anaesthesia_nurse_confirm", 0),
                   "antibiotic_prophylaxis_given": ("yesno", "v2_antibiotic_prophylaxis"),
                   "essential_imaging_displayed": ("yesno", "v2_essential_imaging_displayed")},
        "fk": {"operation_id": "theatre_operation", "certified_by": "users"},
        "critical": ["operation_id", "certified_by"],
    },
    "theatre_before_leaving_theatres": {
        "namespace": r"Ignite\Theatre\Entities\BeforeLeavingTheatres",
        "v3": r"App\Models\TheatreBeforeLeavingTheatre", "alias": "theatre_before_leaving_theatre",
        "service": "theatre",
        "renames": {"name_of_procedure_recorded": "v2_name_of_procedure_recorded",
                    "name_of_procedure_recorded_notes": "procedure_name_notes",
                    "that_instrument_sponge_needle_count_are_correct": "v2_count_correct",
                    "that_instrument_sponge_needle_count_are_correct_notes": "count_notes",
                    "how_specimen_is_recorded": "v2_specimen_recorded",
                    "how_specimen_is_recorded_notes": "specimen_notes",
                    "whether_there_are_any_equipment_problems": "v2_equipment_problems",
                    "whether_there_are_any_equipment_problems_notes": "equipment_notes",
                    "surgeon_anaesthesia_professional_nurse_review": "v2_team_review",
                    "surgeon_anaesthesia_professional_nurse_review_notes": "review_notes"},
        "keep": ["operation_id", "procedure_name_recorded", "procedure_name_notes",
                 "instrument_sponge_needle_count_correct", "count_notes", "specimen_labeling_recorded",
                 "specimen_notes", "equipment_problems", "equipment_notes", "team_review_completed",
                 "review_notes", "certified_by", "certified_by_signature"],
        "inject": {**_STAND_IN_CERTIFIER_INJECT,
                   "procedure_name_recorded": ("yesno", "v2_name_of_procedure_recorded", 0),
                   "instrument_sponge_needle_count_correct": ("yesno", "v2_count_correct"),
                   "specimen_labeling_recorded": ("yesno", "v2_specimen_recorded"),
                   "equipment_problems": ("yesno", "v2_equipment_problems"),
                   "team_review_completed": ("yesno", "v2_team_review", 0)},
        "fk": {"operation_id": "theatre_operation", "certified_by": "users"},
        "critical": ["operation_id", "certified_by"],
    },
    # ── core-service: roles ──────────────────────────────────────────────
    # Role definitions only (Spatie roles: unique per name + guard + org; the
    # org's own roles use guard "api"). V2's extract has no user↔role table,
    # so assignments and permissions are set up in V3. A name the org already
    # has (doctor, nurse …) is a duplicate → left as is.
    "users_roles": {
        "namespace": r"Ignite\Users\Entities\Role",
        "namespaces": [r"Ignite\Users\Entities\Roles"],              # the older extract's name
        "v3": r"App\Models\Role", "alias": "roles", "service": "core",
        "keep": ["name", "guard_name", "description"],
        "inject": {"guard_name": ("const", "api"),
                   "description": ("join_or", ["description"], "display_name")},
    },
    # ── inventory-service: current stock balances ────────────────────────
    # V2 store_products is one row per product + store with its on-hand
    # quantity and unit cost — exactly V3 inv_stocks (unique product + store +
    # batch; no V2 batches). V3 refuses negative stock (CHECK >= 0, and the
    # model floors it): the 68 negative kisumu_v3 balances arrive as 0.
    "inventory_store_products": {
        "namespace": r"Ignite\Inventory\Entities\StoreProduct",
        "namespaces": [r"Ignite\Inventory\Entities\StoreProducts"],     # the older extract's name
        "v3": r"App\Models\Stock", "alias": "stock", "service": "inventory",
        "renames": {"quantity": "v2_quantity", "unit_cost": "average_cost"},
        "keep": ["product_id", "store_id", "quantity", "available_quantity", "reserved_quantity",
                 "average_cost", "total_value"],
        "inject": {"quantity": ("nonneg", "v2_quantity"), "available_quantity": ("nonneg", "v2_quantity"),
                   "reserved_quantity": ("const", 0),
                   "total_value": ("mul_nonneg", "v2_quantity", "average_cost")},
        "fk": {"product_id": "product", "store_id": "store"},
        "critical": ["product_id", "store_id"],
    },
    # ── inpatient-service ────────────────────────────────────────────────
    "inpatient_daily_charges_verifications": {
        "namespace": r"Ignite\Inpatient\Entities\DailyChargesVerifications",
        "v3": r"App\Models\DailyChargesVerification", "alias": "daily_charges_verification", "service": "inpatient",
        # V2 links the visit, V3 the admission (admission_id + user_id NOT NULL, unique(admission_id, date))
        "keep": ["admission_id", "date", "user_id"],
        "inject": {"admission_id": ("admission_for_visit",)},
        "fk": {"admission_id": "admission", "user_id": "users"},
        "critical": ["admission_id", "user_id"],
    },
    "inpatient_blood_transfusion": {
        "namespace": r"Ignite\Inpatient\Entities\BloodTransfusion",
        "v3": r"App\Models\BloodTransfusion", "alias": "blood_transfusion", "service": "inpatient",
        # every V2 row has date_recorded '0000-00-00' and time_recorded '' — moved aside (not posted);
        # the date comes from created_at instead
        "renames": {"date_recorded": "v2_date_recorded", "time_recorded": "v2_time_recorded"},
        "keep": ["admission_id", "visit_id", "user_id", "bp_systolic", "bp_diastolic", "temperature",
                 "respiration", "remarks", "transfusion_time", "date_recorded"],
        "inject": {"date_recorded": ("copy", "created_at")},
        "fk": {"admission_id": "admission", "visit_id": "visit", "user_id": "users"},
        "critical": ["admission_id", "visit_id", "user_id"],      # all NOT NULL
    },
    "inpatient_fluid_balances": {
        "namespace": r"Ignite\Inpatient\Entities\FluidBalance",
        "v3": r"App\Models\FluidBalance", "alias": "fluid_balance", "service": "inpatient",
        # V2 `type` is the fluid ('normal saline', 'DNS'); V3 `type` is an enum (iv/urine/…) — the fluid
        # and its route (IV/PO/NGT) go into remarks, V3 type stays null
        "renames": {"type": "v2_fluid", "remarks": "v2_remarks"},
        "keep": ["admission_id", "patient_id", "recorded_at", "io_type", "amount", "remarks"],
        "inject": {"io_type": ("const", "input"), "recorded_at": ("copy", "created_at"),
                   "remarks": ("join", "input_type", "v2_fluid", "v2_remarks")},
        "fk": {"admission_id": "admission", "patient_id": "patient"},
        "critical": ["admission_id"],
        "null_if_unresolved": ["patient_id"],
    },
    # lab critical values: procedure_id / parent_procedure_id are V2 procedure
    # ids (the V2 "procedure" column); type / lg_type normalised in FETCH_SQL
    "evaluation_critical_values": {
        "namespace": r"Ignite\Evaluation\Entities\CriticalValues",
        "namespaces": [r"Ignite\Evaluation\Entities\CriticalValue"],
        "v3": r"App\Models\CriticalValue", "alias": "critical_values", "service": "evaluation",
        "renames": {"procedure": "procedure_id"},
        "keep": ["procedure_id", "parent_procedure_id", "age_group_id", "critical_value", "type", "lg_type",
                 "lg_value", "gender", "is_active"],
        "fk": {"procedure_id": "procedures", "parent_procedure_id": "procedures", "age_group_id": "age_groups"},
        "critical": ["procedure_id"],
        "null_if_unresolved": ["parent_procedure_id", "age_group_id"],
        "inject": {"lg_value": ("copy", "critical_value"), "is_active": ("const", 1)},
    },
    # V3 holds the 11,200 base ICD-10 codes shared by every org; V2 kisumu_v3
    # has 84,325 (ICD-11 and finer ICD-10 codes too) and diagnoses use them.
    # Codes V3 already has are linked by code, never re-added
    # (v2v3._LINK_BY_FIELD); the rest are inserted.
    "evaluation_icd10_variations": {
        "namespace": r"Ignite\Evaluation\Entities\Icd10Variation",
        "namespaces": [r"Ignite\Evaluation\Entities\Icd10Variations"],
        "v3": r"App\Models\Icd10Variation", "alias": "icd10_variations", "service": "evaluation",
        "renames": {"name": "description"},
        "keep": ["code", "description", "type_id", "is_active"],
        "fk": {"type_id": "icd10_types"},
        "null_if_unresolved": ["type_id"],
        "inject": {"is_active": ("const", 1), "description": ("copy", "code")},
    },
    # lab templates: V2 `procedure` / `subtest` are V2 procedure ids (was
    # registered "generic", which posted them raw — 116 FK refusals)
    "evaluation_templates_lab": {
        "namespace": r"Ignite\Evaluation\Entities\TemplateLab",
        "namespaces": [r"Ignite\Evaluation\Entities\TemplateLabs"],
        "v3": r"App\Models\TemplateLab", "alias": "template_labs", "service": "evaluation",
        "renames": {"procedure": "procedure_id", "subtest": "subtest_id"},
        "keep": ["procedure_id", "title", "subtest_id", "sort_order", "alias", "lab_min_range", "lab_max_range",
                 "adult_minrange", "adult_maxrange", "appear_bold"],
        "fk": {"procedure_id": "procedures", "subtest_id": "procedures"},
        "critical": ["procedure_id"],
        "null_if_unresolved": ["subtest_id"],
        "inject": {"appear_bold": ("const", 0)},
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
        # all three NOT NULL in reception visit_consultants — a row waits for its
        # consultant (a V3 user) rather than being sent with null and refused
        "critical": ["visit_id", "patient_id", "consultant_id"],
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
DYNAMICS = "Microsoft Dynamics sync bookkeeping (what V2 sent to / failed to send to Dynamics) — not part of the record"
REPORT = "MOH register / report rows derived from the clinical and billing records — V3 builds its reports from its own data"

NOT_MIGRATED: dict[str, str] = {
    # V3 model exists, but it isn't this data's place
    "symptoms": SHARED, "evaluation_tags": SHARED,
    "evaluation_reference_range": SHARED, "evaluation_investigation_approval_statuses": SHARED,
    "users_permissions": SETUP, "activations": SETUP,
    "finance_account_groups": SETUP, "finance_account_types": SETUP,
    "settings_options": SETUP, "settings_settings": SETUP,
    "inventory_inventory_stocks": "V3 stock is per product + store (+ batch); V2 rows carry the product only",
    "inventory_customers": "V3 customer needs customer_no + name; V2 rows are patient-linked walk-ins",
    "inventory_batches": "V3 batch needs batch_no, product, cost; the one V2 row is a supplier delivery",
    "credit_notes": "V3 credit_note needs invoice_id, total_amount, reason; V2 notes are receipt-linked",
    "inpatient_visit_wards": "V2 rows are ward charges on a visit; V3 visit_ward is a bed allocation on an admission",
    "evaluation_sub_procedures": "no column in common with V3 subprocedures",
    "finance_payments_mpesa": NO_MODEL, "finance_payments_cash": NO_MODEL, "finance_payments_card": NO_MODEL,
    "finance_payments_pesa_pal_mpesa": NO_MODEL, "finance_payments_pesa_pal_card": NO_MODEL,
    # no V3 model at all
    "finance_invoice_items": NO_MODEL,
    "evaluation_result_details_reference_ranges": NO_MODEL, "evaluation_consumables": NO_MODEL,
    "evaluation_prescription_payments": NO_MODEL, "evaluation_dispensing_details": NO_MODEL,
    "inventory_inventory_stock_adjustments": NO_MODEL, "evaluation_investigation_approvals": NO_MODEL,
    "evaluation_pos_sale_details": NO_MODEL, "finance_evaluation_payment_details": NO_MODEL,
    "inventory_inventory_batch_product_sales": NO_MODEL,
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
    "inpatient_doctor_note_view_logs": LOG, "inpatient_nurse_notes_logs": LOG, "inpatient_prescription_logs": LOG,
    "outpatient_notes_history_views": LOG, "core_approval_action_logs": LOG,
    "dynamic_item_sents": DYNAMICS, "dynamics_failures": DYNAMICS, "dynamics_invoice_receipt_sents": DYNAMICS,
    "dynamics_invoices_sent": DYNAMICS, "dynamics_unsent_data_items": DYNAMICS,
    # MOH registers / report dumps
    "moh204as": REPORT, "moh204_bs": REPORT, "moh717_outpatients": REPORT, "evaluation_register_notes705s": REPORT,
    "registers_705_diseases": REPORT, "invoice_whole_dumps": REPORT, "revenue_dumps": REPORT,
    # organisation setup
    "settings_organization_table": SETUP, "settings_practice": SETUP, "settings_registration_field_settings": SETUP,
    "core_approval_categories": SETUP, "core_email_groups": SETUP,
    # payments: no V3 model, like the other finance_payments_* tables
    "finance_payments_cheque": NO_MODEL, "finance_payments_pesapal_card": NO_MODEL,
    "finance_payments_pesapal_mpesa": NO_MODEL, "payments_cheque": NO_MODEL,
    "petty_cash": "one 2017 setup row (amount 0.00, no fund name or custodian) — V3 petty cash is a named fund "
                  "with a custodian; the org sets up its own",
    "finance_petty_cash": "same single 2017 setup row as petty_cash",
    "settings_rebates": "BLOCKED ON V3 BACKEND — the gateway's `rebates` model is core_rebates, which has no "
                        "organization_id column although the gateway stamps one (every insert fails), and "
                        "core_rebates wants code + percentage while V2 rebates are name + amount; they belong in "
                        "inpatient inp_rebates (name, amount), which no gateway alias exposes",
    # inpatient (checked against the V3 models + migrations 2026-10-12)
    "inpatient_administer_drugs": "V3 drug_administration needs drug_name, dosage, route, frequency and "
                                  "scheduled_time (NOT NULL); V2 rows carry only prescription_id + an administered "
                                  "flag — and V3's model table doesn't match its migration (gateway reads 500)",
    "inpatient_blood_pressures": "V3 keeps BP in inp_vitals, which needs recorded_by (NOT NULL); V2 rows carry no user",
    "inpatient_feedings": "V2 rows are feeding-chart observations (temp, resp, urine, stool …) with no V3 column; "
                          "V3 feeding_plan is a diet plan",
    "partograph_vitals": "V3 PartographVitals has no migration (no table) — gateway reads 500",
    "report_on_labour": NO_MODEL, "summary_of_labour": NO_MODEL,
    "deposits": SETUP + " (V3 keeps the deposit on admission types)",
    "inpatient_insurance_maximum_amounts": NO_MODEL,
    "settings_evaluation_wards": SETUP,
    "inpatient_visit_charges": "V3 VisitCharge (inp_visit_charges) matches V2 almost 1:1 but no gateway alias "
                               "exposes it — needs one from the V3 backend",
    # inventory (checked against the V3 models + migrations 2026-10-12)
    "inventory_stocks": "V3 stock is per product + store (+ batch); V2 rows carry the product only",
    "inventory_batch": "V3 batch needs batch_no, store_id and cost_price; V2 rows have no store (0/1333) and a "
                       "batch number on only 113",
    "inventory_stock_adjustments": NO_MODEL,
    "inventory_store_stock_adjustments": "product_id and store_id empty on all 9,250 rows (only store_product_id, "
                                         "and store_products isn't migrated); same adjustments as the stock movements",
    "inventory_store_knock_offs": "one 2023 write-off with no store — a stock_movement in V3, follows inventory_stock_movements",
    "inventory_product_price": "V2 price history; V3 keeps the price on the product (carried from inventory_products) "
                               "and has no price-history model",
    "inventory_dispensing": "POS sale lines → V3 inv_sale_items, which the gateway doesn't expose",
    "inventory_evaluation_dispensing_details": "dispensing lines → V3 inv_sale_items, which the gateway doesn't "
                                              "expose (only the sale header is migrated)",
    "inventory_store_dispatched_orders": "V3 records dispatch as quantity_dispatched on the store order item — an "
                                         "update to that row, not a new one",
    "inventory_store_received_orders": "V3 records receipt as quantity_received on the store order item — an "
                                       "update to that row, not a new one",
    "inventory_store_prescriptions": "V2 pharmacy queue (prescription routed to a store); V3 has no store-prescription "
                                     "model — dispensing becomes a sale",
    "inventory_stock_takes": "3 abandoned 2023-24 stock takes (never completed); V3 stock_take has no tenant "
                             "column on the gateway",
    "inventory_stock_take_details": "the 1 line of an abandoned V2 stock take (see inventory_stock_takes)",
    # evaluation (checked against the V3 models + migrations 2026-10-12)
    "evaluation_procedure_tags": SHARED, "hemogram_titles": SHARED,
    "evaluation_subprocedures": "V3 SubProcedure's $fillable is only organization_id and its gateway alias has "
                                "no tenant column — shared lab config",
    "visit_symptoms": "V2 rows are IDSR case records (disease, county, list of V2 symptom ids); V3 visit_symptoms is "
                      "one row per symptom needing a V3 symptom_id from the shared symptoms table — no id map",
    "settings_evaluation_actions": SETUP,
    "evaluation_investigation_analytics_groups": "V3 result analytics groups have no gateway model; V2 holds one test row",
    "blocks": NO_MODEL, "sub_blocks": NO_MODEL,
    "own_prescriptions": "patient's own (brought-in) drugs — V3 prescriptions has no own-drug flag, so they would "
                         "become dispensable / billable prescriptions sharing the prescriptions id map; needs a decision",
    # finance / settings / users (checked against the V3 models + migrations 2026-10-12)
    "finance_credit_notes": "V3 credit_note needs total_amount (NOT NULL) and created_by; V2 notes carry no "
                            "amount (only 6 of 52 have a detail row) and point at receipts, which aren't migrated",
    "finance_credit_note_details": "child of finance_credit_notes, which isn't migrated (V3 credit_note_id is a required FK)",
    "finance_service_charges": "V3 service_charges needs service_name + a service_type enum; the 2 V2 rows (2019) "
                               "are an opaque service_code + amount, and cycle 'hours' isn't in V3's enum",
    "finance_patient_account_transactions": "V3 needs balance_before / balance_after (NOT NULL) and an enum "
                                            "transaction_type; V2 rows have neither and link V2 payments, which aren't migrated",
    "finance_change_insurances": NO_MODEL,
    "finance_client_payment_transactions": "V3 mpesa_transactions has no gateway model; V2 rows are M-Pesa prompts "
                                           "tied to invoices / payments, which aren't migrated",
    "finance_item_ledgers": "external ERP item-ledger import keyed by item no / location code text — not a V2 record",
    "split_bills": "V3 split_bills has no gateway model and no visit, amount or user columns",
    "settings_scheme_types": "no V3 table — V3 keeps the scheme type name on core_insurance_schemes.type",
    "settings_rooms": "no V3 room model; consultation rooms would be core service_destinations (see settings_destinations)",
    "settings_pos": SETUP + " (pos_configs is facility-specific — the gateway marks it 'no safe copy')",
    "user_specialities": SETUP + " (2 V2 rows, one a test entry; only users link to them)",
    "users_user_profiles": "profiles belong to the user accounts the V3 backend creates; user_profiles has no "
                           "gateway model and needs user_id, which waits for users",
    # theatre / dialysis
    # older KISUMU_CLEAN extract names
    "theatre_medic": PENDING_KEY,
    "theatre_theatre_before_anaesthesias": PENDING_KEY, "theatre_theatre_before_skin_incisions": PENDING_KEY,
    "theatre_theatre_payments": PENDING_KEY, "theatre_theatre_operation_pulses": PENDING_KEY,
    "theatre_theatre_operation_fluids": PENDING_KEY, "theatre_theatre_deposits": PENDING_KEY,
    "theatre_consumable_requirements": PENDING_KEY,
    "theatre_nurse_check": "the same 14 rows as theatre_nurse_checks (migrated from there)",
    # theatre-service columns V2 has no data for (checked 2026-10-13)
    "theatre_operation_investigations": "V3 needs investigation name, ordered_at and ordered_by (a user); "
                                        "V2 rows hold only a procedure id and type",
    "theatre_operation_fluids": "V3 needs fluid name, route and administered_by (a user); V2 has only a "
                                "quantity and time",
    "theatre_payments": "V3 needs received_by (a user), payment method and the booking; V2 rows link the "
                        "operation and invoice, with no cashier",
    "dialysis_patients": PENDING_KEY, "dialysis_chairs": PENDING_KEY, "dialysis_schedules": PENDING_KEY,
    "dialysis_machines": PENDING_KEY, "dialysis_wards": PENDING_KEY, "dialysis_ward_types": PENDING_KEY,
    "dialysis_sessions": PENDING_KEY, "dialysis_plants": PENDING_KEY, "dialysis_vascular_accesses": PENDING_KEY,
    "dialysis_coagulation_types": PENDING_KEY,
}


# Tables a hand-written mapping would post, but whose rows aren't real data —
# their NOT_MIGRATED reason wins over the mapping (an explicit list, so a table
# that is both mapped and in NOT_MIGRATED isn't silently switched off).
NOT_MIGRATED_DESPITE_MAPPING: frozenset[str] = frozenset({"petty_cash", "finance_petty_cash", "settings_rebates"})


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
    if kind == "fmt":                       # a string built from the row's own columns
        return lambda r: args[0].format_map({k: ("" if v is None else v) for k, v in r.items()})
    if kind == "join":
        return lambda r: " ".join(str(r.get(a)) for a in args if r.get(a) not in (None, "")) or None
    if kind == "mul":
        def mul(r):
            try:
                return round(float(r.get(args[0])) * float(r.get(args[1])), 2)
            except (TypeError, ValueError):
                return None
        return mul
    if kind == "yesno":                     # V2 checklist value → 1 / 0, else the default (None)
        unknown = args[1] if len(args) > 1 else None

        def yesno(r):
            v = str(r.get(args[0]) if r.get(args[0]) is not None else "").strip().lower()
            if v in ("", "null"):
                return unknown
            if v in ("on", "yes", "y", "true", "1", "checked", "done"):
                return 1
            if v in ("off", "no", "n", "non", "none", "false", "0", "nil"):
                return 0
            try:
                return 1 if float(v) > 0 else 0       # e.g. blood available "450" (ml)
            except ValueError:
                return unknown
        return yesno
    if kind == "if_empty":                  # a value only where V2's own column is empty
        col, value = args
        return lambda r: value if r.get(col) in (None, "", 0, "0") else None
    if kind == "number":                    # numeric V3 column: a V2 number, else null ("normal")
        def number(r):
            try:
                return float(str(r.get(args[0])).strip())
            except (TypeError, ValueError):
                return None
        return number
    if kind == "map":                       # value of one V2 column looked up in a dict
        col, table, default = args
        return lambda r: table.get(str(r.get(col) or "").strip().lower(), default)
    if kind == "nonneg":                    # a quantity V3 floors at 0 (CHECK >= 0)
        def nonneg(r):
            try:
                return max(0, int(float(r.get(args[0]))))
            except (TypeError, ValueError):
                return 0
        return nonneg
    if kind == "mul_nonneg":                # value of a quantity floored at 0
        def mul_nonneg(r):
            try:
                return round(max(0.0, float(r.get(args[0]))) * float(r.get(args[1]) or 0), 2)
            except (TypeError, ValueError):
                return 0
        return mul_nonneg
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


def _patient_dependants_sql(table: str):
    """V2 dependant rows only link two patients (patient_id = the holder,
    patient_dependant_id = the dependant, also a V2 patient); V3 needs the
    dependant's own name (full_name NOT NULL) — joined from the patient
    tables (both extracts, first match wins)."""
    def build(clean: str, cols_of, clean_set: set[str]) -> str | None:
        want = ("first_name", "middle_name", "last_name", "dob", "sex", "mobile", "id_no", "email")
        sources = [t for t in ("patients", "reception_patients")
                   if t in clean_set and {"id", "first_name"} <= cols_of(t)]
        if not sources or "patient_dependant_id" not in cols_of(table):
            return None
        joins = " ".join(f'LEFT JOIN {clean}.{t.upper()} p{i} ON TO_VARCHAR(p{i}."ID") = '
                         f'TO_VARCHAR(d."PATIENT_DEPENDANT_ID")' for i, t in enumerate(sources))
        picks = ", ".join(
            "COALESCE(" + ", ".join(f'p{i}."{c.upper()}"' for i, t in enumerate(sources) if c in cols_of(t))
            + f') AS "DEPENDANT_{c.upper()}"'
            for c in want if any(c in cols_of(t) for t in sources))
        return f"SELECT d.*, {picks} FROM {clean}.{table.upper()} d {joins}"
    return build


def _visit_destinations_sql(clean: str, cols_of, clean_set: set[str]) -> str | None:
    """137,031 kisumu_v3 destination rows carry neither the embedded
    destination object nor a department — only destination_id. Its name (the
    V3 department) is in V2's own settings_destinations: joined in as
    DESTINATION_SETTING_NAME."""
    if "settings_destinations" not in clean_set or "destination_id" not in cols_of("evaluation_visit_destinations"):
        return None
    if not {"id", "name"} <= cols_of("settings_destinations"):
        return None
    return (f'SELECT d.*, s."NAME" AS "DESTINATION_SETTING_NAME" FROM {clean}.EVALUATION_VISIT_DESTINATIONS d '
            f'LEFT JOIN {clean}.SETTINGS_DESTINATIONS s ON TO_VARCHAR(s."ID") = TO_VARCHAR(d."DESTINATION_ID")')


def _goods_received_sql(clean: str, cols_of, clean_set: set[str]) -> str | None:
    """V2 goods-received rows link only the purchase order; V3 GRNs need the
    store and supplier (NOT NULL, FK) — taken from that purchase order."""
    if "inventory_purchase_orders" not in clean_set or "purchase_order_id" not in cols_of("inventory_goods_received"):
        return None
    if not {"id", "store_id", "supplier_id"} <= cols_of("inventory_purchase_orders"):
        return None
    return (f'SELECT g.*, p."STORE_ID" AS "STORE_ID", p."SUPPLIER_ID" AS "SUPPLIER_ID" '
            f'FROM {clean}.INVENTORY_GOODS_RECEIVED g LEFT JOIN {clean}.INVENTORY_PURCHASE_ORDERS p '
            f'ON TO_VARCHAR(p."ID") = TO_VARCHAR(g."PURCHASE_ORDER_ID")')


def _join_name_sql(table: str, fk: str, parent: str, alias: str):
    """table rows + the parent's NAME as <alias> (V3 wants the name; V2 links the id)."""
    def build(clean: str, cols_of, clean_set: set[str]) -> str | None:
        if parent not in clean_set or fk not in cols_of(table) or not {"id", "name"} <= cols_of(parent):
            return None
        return (f'SELECT t.*, p."NAME" AS "{alias.upper()}" FROM {clean}.{table.upper()} t '
                f'LEFT JOIN {clean}.{parent.upper()} p ON TO_VARCHAR(p."ID") = TO_VARCHAR(t."{fk.upper()}")')
    return build


def _checklist_certifier_sql(table: str):
    """Surgical safety checklists: V3 requires certified_by, and no kisumu_v3
    checklist row records one. Agreed 2026-10-13: the operation's lead surgeon
    (its medic of type 'surgeon'; else any medic on the operation) stands in,
    joined as CERTIFIER_USER_ID — and the signature field says so."""
    def build(clean: str, cols_of, clean_set: set[str]) -> str | None:
        if not {"theatre_operation_medics", "theatre_medic_types"} <= clean_set or "operation_id" not in cols_of(table):
            return None
        return (
            f'SELECT t.*, COALESCE(s.uid, a.uid) AS "CERTIFIER_USER_ID" FROM {clean}.{table.upper()} t '
            f'LEFT JOIN (SELECT m."OPERATION_ID" op, MIN(m."USER_ID") uid FROM {clean}.THEATRE_OPERATION_MEDICS m '
            f'JOIN {clean}.THEATRE_MEDIC_TYPES mt ON TO_VARCHAR(mt."ID") = TO_VARCHAR(m."MEDIC_TYPE_ID") '
            f"WHERE LOWER(TRIM(mt.\"NAME\")) = 'surgeon' GROUP BY 1) s ON TO_VARCHAR(s.op) = TO_VARCHAR(t.\"OPERATION_ID\") "
            f'LEFT JOIN (SELECT "OPERATION_ID" op, MIN("USER_ID") uid FROM {clean}.THEATRE_OPERATION_MEDICS GROUP BY 1) a '
            f'ON TO_VARCHAR(a.op) = TO_VARCHAR(t."OPERATION_ID")')
    return build


def _result_ranges_sql(clean: str, cols_of, clean_set: set[str]) -> str | None:
    """Reference ranges of result details whose lab result V2 doesn't have
    (the orphan details skipped by snowflake_to_v3_migration) can never get a
    parent — read only the ranges whose detail has a V2 result."""
    need = {"evaluation_investigation_result_details", "evaluation_investigation_results"}
    if not need <= clean_set or "result_detail_id" not in cols_of("evaluation_result_details_ranges"):
        return None
    return (f'SELECT rg.* FROM {clean}.EVALUATION_RESULT_DETAILS_RANGES rg '
            f'JOIN {clean}.EVALUATION_INVESTIGATION_RESULT_DETAILS d ON TO_VARCHAR(d."ID") = TO_VARCHAR(rg."RESULT_DETAIL_ID") '
            f'JOIN {clean}.EVALUATION_INVESTIGATION_RESULTS r ON TO_VARCHAR(r."ID") = TO_VARCHAR(d."INVESTIGATION_RESULT_ID")')


def _critical_values_sql(clean: str, cols_of, clean_set: set[str]) -> str | None:
    """V2 sometimes stores the comparison itself as type ('>' with no lg_type);
    V3 wants type range / less_greater and the operator in lg_type."""
    if not {"type", "lg_type"} <= cols_of("evaluation_critical_values"):
        return None
    ops = "('<', '>', '<=', '>=', '==', '=')"
    return (f'SELECT c.* EXCLUDE ("TYPE", "LG_TYPE"), '
            f'IFF(TRIM(c."TYPE") IN {ops}, \'less_greater\', c."TYPE") AS "TYPE", '
            f'COALESCE(c."LG_TYPE", IFF(TRIM(c."TYPE") IN {ops}, TRIM(c."TYPE"), NULL)) AS "LG_TYPE" '
            f'FROM {clean}.EVALUATION_CRITICAL_VALUES c')


def _scheme_prices_sql(clean: str, cols_of, clean_set: set[str]) -> str | None:
    """Scheme prices of procedures V2 still has (2,638 of 2,929 kisumu_v3 rows
    point at procedures absent from V2 — they can never link), each with the
    procedure's latest cash price as STANDARD_PRICE (V3 NOT NULL)."""
    need = {"evaluation_procedures", "evaluation_procedure_prices"}
    if not need <= clean_set or "procedure_id" not in cols_of("settings_scheme_exclusions"):
        return None
    deleted = ' WHERE e."DELETED_AT" IS NULL' if "deleted_at" in cols_of("settings_scheme_exclusions") else ""
    return (f'SELECT e.*, COALESCE(pp."CASH_CHARGE", e."PRICE") AS "STANDARD_PRICE" '
            f'FROM {clean}.SETTINGS_SCHEME_EXCLUSIONS e '
            f'JOIN {clean}.EVALUATION_PROCEDURES p ON TO_VARCHAR(p."ID") = TO_VARCHAR(e."PROCEDURE_ID") '
            f'LEFT JOIN (SELECT "EVALUATION_PROCEDURE_ID", "CASH_CHARGE" FROM {clean}.EVALUATION_PROCEDURE_PRICES '
            f'QUALIFY ROW_NUMBER() OVER (PARTITION BY "EVALUATION_PROCEDURE_ID" ORDER BY "UPDATED_AT" DESC, "ID" DESC) = 1) pp '
            f'ON TO_VARCHAR(pp."EVALUATION_PROCEDURE_ID") = TO_VARCHAR(e."PROCEDURE_ID")' + deleted)


def _latest_store_price(clean: str, col: str, deleted: bool) -> str:
    """Each product's `col` from its most recently updated store row that has one."""
    live = ' AND "DELETED_AT" IS NULL' if deleted else ""
    return (f'SELECT "PRODUCT_ID", "{col}" AS "PRICE" FROM {clean}.INVENTORY_STORE_PRODUCTS '
            f'WHERE TRY_TO_DOUBLE(TO_VARCHAR("{col}")) > 0{live} '
            f'QUALIFY ROW_NUMBER() OVER (PARTITION BY "PRODUCT_ID" '
            f'ORDER BY "UPDATED_AT" DESC NULLS LAST, "ID" DESC) = 1')


def _product_prices_sql(clean: str, cols_of, clean_set: set[str]) -> str | None:
    """V2 prices products per store (inventory_store_products); its product
    rows hardly carry one (86 of 6,467 kisumu_v3 products). V3 prices the
    product itself and its stock has no price columns — agreed 2026-10-13:
    the most recently updated store row's selling / insurance price, else the
    product's own."""
    prices = {"selling_price", "insurance_price"}
    if "inventory_store_products" not in clean_set:
        return None
    sp = cols_of("inventory_store_products")
    if not (prices | {"product_id", "updated_at", "id"}) <= sp or not prices <= cols_of("inventory_products"):
        return None
    deleted = "deleted_at" in sp
    return (f'SELECT p.* EXCLUDE ("SELLING_PRICE", "INSURANCE_PRICE"), '
            f'COALESCE(s."PRICE", p."SELLING_PRICE") AS "SELLING_PRICE", '
            f'COALESCE(i."PRICE", p."INSURANCE_PRICE") AS "INSURANCE_PRICE" '
            f'FROM {clean}.INVENTORY_PRODUCTS p '
            f'LEFT JOIN ({_latest_store_price(clean, "SELLING_PRICE", deleted)}) s '
            f'ON TO_VARCHAR(s."PRODUCT_ID") = TO_VARCHAR(p."ID") '
            f'LEFT JOIN ({_latest_store_price(clean, "INSURANCE_PRICE", deleted)}) i '
            f'ON TO_VARCHAR(i."PRODUCT_ID") = TO_VARCHAR(p."ID")')


def _latest_procedure_price_sql(clean: str, cols_of, clean_set: set[str]) -> str | None:
    """V2 keeps every price a procedure ever had (12,813 rows, 1,715
    procedures); V3 holds one per org + procedure + facility (unique key), so
    whichever row landed first won — 115 procedures got an old price. Read
    only each procedure's most recently updated row."""
    cols = cols_of("evaluation_procedure_prices")
    if not {"evaluation_procedure_id", "updated_at", "id"} <= cols:
        return None
    return (f'SELECT * FROM {clean}.EVALUATION_PROCEDURE_PRICES QUALIFY ROW_NUMBER() OVER '
            f'(PARTITION BY "EVALUATION_PROCEDURE_ID" ORDER BY "UPDATED_AT" DESC NULLS LAST, "ID" DESC) = 1')


# table → builder(clean schema, cols_of(table) → lower-case column names, CLEAN
# object names) → SQL, or None to read the table as is
FETCH_SQL = {"evaluation_icd10_notes": _icd10_notes_sql,
             "evaluation_procedure_prices": _latest_procedure_price_sql,
             "inventory_products": _product_prices_sql,
             "evaluation_critical_values": _critical_values_sql,
             "settings_scheme_exclusions": _scheme_prices_sql,
             "evaluation_result_details_ranges": _result_ranges_sql,
             "theatre_before_anaesthesias": _checklist_certifier_sql("theatre_before_anaesthesias"),
             "theatre_before_skin_incisions": _checklist_certifier_sql("theatre_before_skin_incisions"),
             "theatre_before_leaving_theatres": _checklist_certifier_sql("theatre_before_leaving_theatres"),
             "theatre_operation_medics": _join_name_sql("theatre_operation_medics", "medic_type_id",
                                                        "theatre_medic_types", "medic_type_name"),
             "theatre_operation_procedures": _join_name_sql("theatre_operation_procedures", "procedure_id",
                                                            "evaluation_procedures", "procedure_name"),
             "inventory_goods_received": _goods_received_sql,
             "evaluation_visit_destinations": _visit_destinations_sql,
             "patient_dependants": _patient_dependants_sql("patient_dependants"),
             "reception_patient_dependants": _patient_dependants_sql("reception_patient_dependants")}


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
        elif t in MAPPINGS:
            # mapped, but this extract has no RAW rows naming its V2 class
            # (loaded straight into CLEAN), so discovery can't pick it up
            out[t] = "not migrated: no RAW rows (V2 class unknown) in this extract — mapped where RAW has it"
        else:
            out[t] = "UNACCOUNTED"
    return out
