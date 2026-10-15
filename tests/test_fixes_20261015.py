"""
Fixes from the kisumu_v3 → org 4 reload of 2026-10-15 (live V2 API), each tied to what V3 refused or
what came out wrong:

  * vitals: '36,6' / '.36.5' temperatures ("Incorrect decimal value"), height/weight 0 ("Division by
    zero" — V3 computes BMI), a value too large to encode; mapped tables (inpatient_vitals) get the rules
  * procedure categories: applies_to text ('Ward', 'NULL') → V3's integer module id
  * visit destinations: destination_id (→ V3 service destination) and destination_slug are kept
  * theatre schedules: scheduled_date from schedule_start_time
  * admissions by an external doctor only: system user + the doctor's name kept
  * required *_by users V3 doesn't have → system user; requisitions with no requestor → system user
  * visit wards: mapped (price × days on the visit's admission); daily-charge verifications: admission
    joined from the visit
  * product prices: V2's inventory_product_price list fills in after the store price
"""
from __future__ import annotations

import snowflake_to_v3_migration  # noqa: F401 — registers v3_extra_mappings' tables (xm.apply) on import
import v2_to_v3_api_migration as v2v3
import v3_extra_mappings as xm

ORG = {"organization_id": 4, "facility_id": 4, "application_id": 1}


def test_vital_reading_decimal_comma_zero_and_overflow():
    t, h, bp = v2v3._vital_reading("temperature"), v2v3._vital_reading("height"), v2v3._vital_reading("systolic_bp")
    assert t("36,6") == 36.6 and t(".36.5") == 36.5 and t("36.7C") == 36.7 and t("36") == 36
    assert h(0) is None and h("0") is None and h("170") == 170
    assert t("9" * 25) is None                                  # "Integer exceeds N-bit range"
    assert bp("120,80") == 120                                  # two readings, never 120.8


def test_mapped_inpatient_vitals_get_the_vital_rules():
    tk = xm.transform_key("inpatient_vitals")
    rules = v2v3._PER_KEY_COERCIONS[tk]
    assert {"temperature", "height", "weight"} <= set(rules)
    assert rules["temperature"]("36,6") == 36.6 and rules["height"](0) is None


def test_procedure_category_applies_to_text():
    c = v2v3._PER_KEY_COERCIONS["eval_procedure_category"]["applies_to"]
    assert (c("Ward"), c("NULL"), c(None), c("3"), c("Laboratory"), c("radiology")) == (None, None, None, 3, 3, 4)


def test_visit_destination_keeps_destination_and_slug():
    keep = v2v3._PER_KEY_KEEP_ONLY["evaluation_visit_destination"]
    assert {"destination_id", "destination_slug"} <= keep
    assert v2v3._FK_REMAP["evaluation_visit_destination"]["destination_id"] == "service_destinations"
    import snowflake_to_v3_migration as m
    assert "destination_id" in m._NULL_IF_UNRESOLVED["evaluation_visit_destination"]   # never the raw V2 id
    inj = v2v3._PER_KEY_INJECT["evaluation_visit_destination"]["destination_slug"]
    assert inj({"destination_setting_slug": "triage"}) == "triage" and inj({}) is None


def test_theatre_schedule_date_from_start_time():
    inj = v2v3._PER_KEY_INJECT["theatre_schedule"]["scheduled_date"]
    assert inj({"schedule_start_time": "2026-05-02 13:00:00"}) == "2026-05-02"
    assert inj({"schedule_start_time": None}) is None and inj({"schedule_start_time": "13:00"}) is None


def test_admission_by_external_doctor(monkeypatch):
    monkeypatch.setattr(v2v3, "_v3_user_id_for_v2", lambda *a, **k: None)
    inj = v2v3._PER_KEY_INJECT["inpatient_admission"]
    ext = {"id": 7372, "doctor_id": None, "external_doctor": "DR  BITTA"}
    assert inj["admitting_doctor_id"](ext) == 1 and inj["reason"](ext) == "External doctor: DR BITTA"
    assert inj["admitting_doctor_id"]({"doctor_id": None, "external_doctor": None}) is None


def test_required_users_fall_back_to_the_system_user(monkeypatch):
    monkeypatch.setattr(v2v3, "_v3_user_for_v2_id", lambda v: {1484: 77}.get(int(v)))
    assert v2v3._v3_user_or_system(1484) == 77 and v2v3._v3_user_or_system(9999) == 1
    assert v2v3._v3_user_or_system(None) is None
    assert v2v3._V3_NULL_DEFAULTS["requested_by"] == 1


def test_visit_wards_are_mapped_with_their_admission():
    spec = xm.MAPPINGS["inpatient_visit_wards"]
    assert spec["critical"] == ["admission_id", "ward_id", "allocated_by"]
    assert spec["renames"]["price"] == "daily_charge" and spec["renames"]["quantity"] == "days_stayed"
    sql = xm._visit_wards_sql("C", lambda t: {"id", "visit_id", "bed_id", "discharged_at"},
                              {"inpatient_admissions", "inpatient_visit_wards"})
    assert '"ADMISSION_ID"' in sql and "'discharged'" in sql


def test_daily_charges_admission_join_is_valid_sql():
    build = xm.FETCH_SQL["inpatient_daily_charges_verifications"]
    no_own = build("C", lambda t: {"id", "visit_id", "date", "user_id"}, {"inpatient_admissions"})
    assert 'a."ID" AS "ADMISSION_ID"' in no_own and "COALESCE(a." not in no_own      # 1-arg COALESCE fails
    own = build("C", lambda t: {"id", "visit_id", "admission_id"}, {"inpatient_admissions"})
    assert 'COALESCE(t."ADMISSION_ID", a."ID")' in own


def test_product_prices_use_the_v2_price_list():
    cols = {"inventory_store_products": {"selling_price", "insurance_price", "product_id", "updated_at", "id"},
            "inventory_products": {"selling_price", "insurance_price", "id"},
            "inventory_product_price": {"product", "price", "ins_price", "updated_at", "id"}}
    sql = xm._product_prices_sql("C", lambda t: cols.get(t, set()), set(cols))
    assert "INVENTORY_PRODUCT_PRICE" in sql
    assert 'COALESCE(s."PRICE", pl."PRICE", p."SELLING_PRICE")' in sql       # store first, then the list
    without = xm._product_prices_sql("C", lambda t: cols.get(t, set()), set(cols) - {"inventory_product_price"})
    assert "INVENTORY_PRODUCT_PRICE" not in without


def test_user_profiles_map_to_the_core_gateway_model():
    spec = xm.MAPPINGS["users_user_profiles"]
    assert (spec["alias"], spec["service"], spec["critical"]) == ("user_profile", "core", ["user_id"])
    tk = xm.transform_key("users_user_profiles")
    out = v2v3.transform_record({"id": 5, "user_id": 5, "first_name": "Carole", "last_name": None, "gender": "M",
                                 "dob": "", "date_of_employment": "0000-00-00", "title": "1"}, tk, ORG, "kisumu_v3")
    assert out["gender"] == "male" and out["last_name"] == "-"           # Laravel turns '' into null
    assert out["dob"] is None and out["date_of_employment"] is None


def test_v3_to_v3_links_profile_ids():
    import v3_to_v3_links as L
    assert L.link_target("core/user_profile", "user_id") == "core/users"
    assert L.link_target("core/user_profile", "speciality_id") == "core/specialties"
    assert L.link_target("core/user_profile", "employee_category_id") == "core/employee_categories"
    assert not L.NOT_IN_GATEWAY.get("core/user_profiles")
