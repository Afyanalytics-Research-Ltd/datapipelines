"""
V3 NOT NULL columns the kisumu_v3 V2 data leaves empty (2026-10-12 run):

  * visit destinations — the CLEAN view carries the destination as an object
    (no destination_name column): the department name comes from its "name",
    else V2's free-text department
  * next of kin — relationship (no V2 row has one) → "Not recorded in V2"
  * ICD-10 categories/subcategories — code from the code_from..code_to range
  * insurance schemes — code from the V2 id; a scheme whose company isn't in
    V3 yet is held back
"""
from __future__ import annotations

import snowflake_to_v3_migration as m
import v2_to_v3_api_migration as v2v3

ORG = {"organization_id": 4, "facility_id": 4, "application_id": 1}


def ns(v2_class):
    return v2v3.NAMESPACE_MAP[v2_class]["transform"]


class TestVisitDestination:
    def tf(self, monkeypatch, rec):
        monkeypatch.setattr(v2v3, "_v3_department_id_by_name", lambda n: {"pharmacy": 7, "nursing": 8}.get(
            str(n or "").lower()))
        return v2v3.transform_record({"id": 1, "visit": 5, "created_at": "2024-01-01", **rec},
                                     "evaluation_visit_destination", ORG, "kisumu_v3")

    def test_name_from_the_destination_object(self, monkeypatch):
        out = self.tf(monkeypatch, {"destination": {"id": 9, "name": "Pharmacy"}, "department": ""})
        assert out["department_name"] == "Pharmacy" and out["department_id"] == 7
        assert "destination" not in out

    def test_falls_back_to_department(self, monkeypatch):
        out = self.tf(monkeypatch, {"destination": None, "department": "Nursing"})
        assert out["department_name"] == "Nursing" and out["department_id"] == 8

    def test_flattened_destination_name_still_wins(self, monkeypatch):
        out = self.tf(monkeypatch, {"destination_name": "Pharmacy", "department": "Nursing"})
        assert out["department_name"] == "Pharmacy"

    def test_no_name_is_held_back(self, monkeypatch):
        assert self.tf(monkeypatch, {"destination": None, "department": ""}) is None


class _Cur:
    def __init__(self, cols, rows):
        self.cols, self.rows, self.sql, self.description = cols, rows, [], None

    def execute(self, sql):
        self.sql.append(sql)
        if "LIMIT 0" in sql:
            self.description = [(c.upper(),) for c in self.cols]
            return self
        self._out = next((v for k, v in self.rows.items() if f"DISTINCT {k}" in sql), [])
        return self

    def fetchall(self):
        return self._out


class _Conn:
    def __init__(self, cur):
        self.cur = cur

    def cursor(self):
        return self.cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_departments_read_from_the_columns_the_view_has(monkeypatch):
    cur = _Cur(["id", "destination", "department"], {
        "TRY_PARSE_JSON": [("Pharmacy",), ("Triage",)],
        "department::STRING": [("Pharmacy",), ("Nursing ",)],
    })
    monkeypatch.setattr(m, "_snowflake_connect", lambda: _Conn(cur))
    monkeypatch.setattr(v2v3, "reset_v3_department_cache", lambda: None)
    monkeypatch.setattr(v2v3, "v3_login_org_cfg", lambda: ORG)
    seen = []   # every name already in V3: nothing posted
    monkeypatch.setattr(v2v3, "_v3_department_id_by_name", lambda n: seen.append(n) or 1)
    m.ensure_departments_from_destinations("kisumu_v3")
    assert sorted(seen) == ["Nursing", "Pharmacy", "Triage"]
    assert not any("destination_name" in s for s in cur.sql)


def test_next_of_kin_relationship_placeholder():
    t = ns(r"Ignite\Reception\Entities\NextOfKin")
    out = v2v3.transform_record({"id": 35317, "patient_id": 233965, "first_name": "dancan",
                                 "relationship": None, "relationship_id": None}, t, ORG, "kisumu_v3")
    assert out["relationship"] == v2v3._NOT_RECORDED
    kept = v2v3.transform_record({"id": 1, "patient_id": 2, "relationship": "Spouse"}, t, ORG, "kisumu_v3")
    assert kept["relationship"] == "Spouse"


def test_icd10_code_from_range():
    for cls in ("Icd10Categories", "Icd10Subcategories"):
        t = ns(rf"Ignite\Evaluation\Entities\{cls}")
        out = v2v3.transform_record({"id": 1, "name": "Neoplasms", "code_from": "C00", "code_to": "D49"},
                                    t, ORG, "kisumu_v3")
        assert out["code"] == "C00-D49"
    single = v2v3.transform_record({"id": 2, "code_from": "A75", "code_to": "A75"}, t, ORG, "kisumu_v3")
    assert single["code"] == "A75"


def test_scheme_code_and_company_is_critical():
    out = v2v3.transform_record({"id": 1155, "name": "2032800", "code": None, "company": 366,
                                 "source_schema": "kisumu_v3"}, "settings_scheme", ORG, "kisumu_v3")
    assert out["code"] == "SCH-kisumu_v3-1155"
    assert out["company_id"] == 366
    assert "company_id" in m._CRITICAL_FK_FIELDS["settings_scheme"]
    real = v2v3.transform_record({"id": 1, "code": "NHIF-01", "company": 366}, "settings_scheme", ORG, "kisumu_v3")
    assert real["code"] == "NHIF-01"


# ── second pass (2026-10-12 afternoon run) ────────────────────────────────────

def test_icd10_description_from_name():
    out = v2v3.transform_record({"id": 6, "name": "Neoplasms", "code_from": "C00", "code_to": "D49"},
                                "eval_icd10_range", ORG, "kisumu_v3")
    assert out["description"] == "Neoplasms"


def test_text_flags_become_integers():
    out = v2v3.transform_record({"id": 841109, "allow_substitution": "false", "for_discharge": "TRUE"},
                                "evaluation_prescription", ORG, "kisumu_v3")
    assert {k: out[k] for k in ("allow_substitution", "for_discharge") if k in out} in (
        {"allow_substitution": 0, "for_discharge": 1}, {"allow_substitution": 0}, {"for_discharge": 1}, {})
    assert "false" not in out.values() and "TRUE" not in out.values()
    bed = v2v3.transform_record({"id": 15, "is_available": "true", "number": "B1"}, "inpatient_bed", ORG, "kisumu_v3")
    assert bed["is_available"] == 1
    gen = v2v3.transform_record({"id": 1, "flag": "false", "note": "true story"}, "generic", ORG, "kisumu_v3")
    assert gen == {"id": 1, "flag": 0, "note": "true story"}


def test_active_flag_text_false_is_inactive():
    # "false" used to be truthy → status "active"
    out = v2v3.transform_record({"id": 1, "name": "x", "active": "false"}, "settings_scheme", ORG, "kisumu_v3")
    assert out["status"] == "inactive"


def test_blank_unit_is_skipped():
    t = ns(r"Ignite\Inventory\Entities\Units")
    assert v2v3.transform_record({"id": 34696, "name": "", "symbol": ""}, t, ORG, "kisumu_v3") is None
    assert v2v3.transform_record({"id": 1, "name": "Tablet"}, t, ORG, "kisumu_v3")["name"] == "Tablet"


def test_gl_account_group_type_and_code():
    t = ns(r"Ignite\Finance\Entities\GlAccountGroups")
    out = v2v3.transform_record({"id": 5, "name": "Costs and Expenses", "type": 1}, t, ORG, "kisumu_v3")
    assert out["account_type_id"] == 1 and "type" not in out
    assert out["code"] == "COSTS_AND_EXPENSES"
    assert v2v3._FK_REMAP[t] == {"account_type_id": "gl_account_type"}
    assert m._CRITICAL_FK_FIELDS[t] == ["account_type_id"]


def test_examination_review_visit_is_remapped_and_critical():
    t = ns(r"Ignite\Evaluation\Entities\ExaminationReviews")
    assert v2v3._FK_REMAP[t] == {"visit_id": "visit"}
    assert m._CRITICAL_FK_FIELDS[t] == ["visit_id"]


def test_null_flag_defaults():
    assert v2v3._V3_NULL_DEFAULTS["appear_bold"] == 0
    assert v2v3._V3_NULL_DEFAULTS["use_bio_ref_flagging"] == 0


class _R:
    def __init__(self, status, body):
        import json
        self.status_code, self._b, self.text = status, body, json.dumps(body)
        self.ok = status < 400

    def json(self):
        return self._b

    def raise_for_status(self):
        pass


def _post(monkeypatch, responses):
    sent = []

    def fake(service, body, timeout=None):
        sent.append(dict(body["data"]))
        return responses.pop(0)
    monkeypatch.setattr(v2v3, "_gateway_post", fake)
    monkeypatch.setattr(v2v3, "_write_dead_letter", lambda *a, **k: None)
    monkeypatch.setattr(v2v3, "V3_POST_THROTTLE", 0)
    return sent


def test_422_insert_rejected_null_column_gets_default(monkeypatch):
    sent = _post(monkeypatch, [
        _R(422, {"success": False, "message": "insert rejected: Column 'appear_bold' cannot be null"}),
        _R(201, {"success": True, "data": {"id": 99}}),
    ])
    v2v3._post_to_v3_batch(r"App\Models\TemplateLab", ORG, {"id": 1534, "appear_bold": None},
                           alias_override="template_labs", service_override="evaluation")
    assert sent[1]["appear_bold"] == 0


def test_422_duplicate_code_retries_with_v2_id(monkeypatch):
    sent = _post(monkeypatch, [
        _R(422, {"success": False, "message": "insert rejected: Duplicate entry '4-ITM000321' for key "
                                              "'inv_products.inv_products_org_code_unique'"}),
        _R(201, {"success": True, "data": {"id": 99}}),
    ])
    v2v3._post_to_v3_batch(r"App\Models\Product", ORG, {"id": 37466, "code": "ITM000321"},
                           alias_override="product", service_override="inventory")
    assert [s["code"] for s in sent] == ["ITM000321", "ITM000321-37466"]


def test_422_other_validation_still_dead_letters(monkeypatch):
    import pytest
    _post(monkeypatch, [_R(422, {"success": False, "message": "The name field is required."})])
    with pytest.raises(v2v3.RecordDeadLettered):
        v2v3._post_to_v3_batch(r"App\Models\Unit", ORG, {"id": 1}, alias_override="unit",
                               service_override="inventory")


def test_user_keeps_username_and_email():
    out = v2v3.transform_record({"id": 1445, "username": "NYAMOHANGA", "email": "nyamopat@yahoo.com"},
                                "settings_user", ORG, "kisumu_v3")
    assert out["username"] == "NYAMOHANGA" and out["email"] == "nyamopat@yahoo.com"
    no_email = v2v3.transform_record({"id": 2, "username": "jdoe", "email": ""}, "settings_user", ORG, "kisumu_v3")
    assert no_email["email"] == "jdoe" and no_email["username"] == "jdoe"


def test_patient_document_file_size():
    out = v2v3.transform_record({"id": 7, "patient": 1, "file_name": "scan.pdf"},
                                "reception_patient_document", ORG, "kisumu_v3")
    assert out["file_size"] == 0


def test_user_gets_a_random_password_and_forced_reset(tmp_path, monkeypatch):
    a = v2v3.transform_record({"id": 1422, "username": "a", "email": "a@x"}, "settings_user", ORG, "kisumu_v3")
    b = v2v3.transform_record({"id": 1423, "username": "b", "email": "b@x"}, "settings_user", ORG, "kisumu_v3")
    assert len(a["password"]) >= 32 and a["password"] != b["password"]
    assert a["enforce_password_reset"] == 1
    # never written to the dead-letter file
    monkeypatch.setattr(v2v3, "DEAD_LETTER_FILE", tmp_path / "dl.jsonl")
    v2v3._write_dead_letter(r"App\Models\User", a, {"message": "x"})
    text = (tmp_path / "dl.jsonl").read_text()
    assert a["password"] not in text and "<redacted>" in text


def test_bed_number_from_number():
    out = v2v3.transform_record({"id": 17, "number": "008", "ward_id": 1, "bed_type_id": 2, "is_available": "true"},
                                "inpatient_bed", ORG, "kisumu_v3")
    assert out["bed_number"] == "008" and out["number"] == "008" and out["is_available"] == 1


def test_investigation_null_is_walkin_is_zero():
    out = v2v3.transform_record({"id": 727501, "visit": 1, "is_walkin": None}, "evaluation_investigation", ORG, "kisumu_v3")
    assert out is None or out["is_walkin"] == 0          # None = held back for its patient, not for is_walkin
    tk = v2v3._PER_KEY_COERCIONS["evaluation_investigation"]["is_walkin"]
    assert (tk(None), tk(""), tk(1), tk(0)) == (0, 0, 1, 0)
    assert v2v3._V3_NULL_DEFAULTS["is_walkin"] == 0


def test_purchase_order_required_columns_and_links():
    out = v2v3.transform_record({"id": 61, "source_schema": "kisumu_v3", "user": 12, "supplier_id": 3, "store_id": 1,
                                 "created_at": "2023-05-08 22:03:27", "status": "approved"},
                                "inventory_purchase_order", ORG, "kisumu_v3")
    assert out["po_number"] == "PO-kisumu_v3-61"
    assert out["order_date"] == "2023-05-08"
    assert out["created_by"] == 12 and "user_id" not in out
    fk = v2v3._FK_REMAP["inventory_purchase_order"]
    assert fk["supplier_id"] == "supplier" and fk["store_id"] == "store" and fk["created_by"] == "users"
    assert m._CRITICAL_FK_FIELDS["inventory_purchase_order"] == ["supplier_id", "store_id", "created_by"]
    assert m._NULL_IF_UNRESOLVED["inventory_purchase_order"] == {"approved_by"}


class TestTheatreChain:
    def test_booking_patient_from_visit_and_links(self, monkeypatch):
        monkeypatch.setattr(v2v3, "_visit_patient_map", {555: 777})
        out = v2v3.transform_record({"id": 99, "visit_id": 555, "operation_procedure": 12, "proc_cash_price": 100,
                                     "emergency": 0, "status_id": 2, "requested_by": 5},
                                    "theatre_booking", ORG, "kisumu_v3")
        assert out["patient_id"] == 777
        assert out["operation_procedure_id"] == 12 and out["procedure_cash_price"] == 100
        fk = v2v3._FK_REMAP["theatre_booking"]
        assert fk["visit_id"] == "visit" and fk["patient_id"] == "patient" and fk["requested_by"] == "users"
        assert set(m._CRITICAL_FK_FIELDS["theatre_booking"]) == {"visit_id", "patient_id", "status_id", "requested_by"}

    def test_schedule_waits_for_its_booking(self):
        t = ns(r"Ignite\Theatre\Entities\TheatreSchedules")
        assert v2v3._FK_REMAP[t]["booking_id"] == "theatre_booking"
        assert "booking_id" in m._CRITICAL_FK_FIELDS[t]

    def test_operation_patient_and_schedule(self, monkeypatch):
        monkeypatch.setattr(v2v3, "_visit_patient_map", {556: 778})
        t = ns(r"Ignite\Theatre\Entities\TheatreOperations")
        out = v2v3.transform_record({"id": 13, "visit_id": 556, "schedule_id": 9}, t, ORG, "kisumu_v3")
        assert out["patient_id"] == 778
        assert v2v3._FK_REMAP[t]["schedule_id"] == "theatre_schedule"
        assert m._CRITICAL_FK_FIELDS[t] == ["schedule_id", "patient_id"]


def test_prescription_return_approver_name_is_not_a_user_id():
    out = v2v3.transform_record({"id": 886337, "visit": 1, "return_approved_by": "Ms. Kerubo  Venah",
                                 "return_rejected_by": "12", "return_approved_at": "2024-01-02 10:00:00"},
                                "evaluation_prescription", ORG, "kisumu_v3")
    assert out["return_approved_by"] is None and out["return_rejected_by"] == 12
    assert out["return_approved_at"] == "2024-01-02 10:00:00"
    assert m._NULL_IF_UNRESOLVED["evaluation_prescription"] == {"return_approved_by", "return_rejected_by"}


def test_investigation_is_walkin_when_posted(monkeypatch):
    monkeypatch.setattr(v2v3, "_v3_patient_for_v2_visit", lambda vid: 4242)
    for val, want in ((None, 0), ("", 0), (1, 1), ("true", 1)):
        out = v2v3.transform_record({"id": 727510, "visit": 1, "is_walkin": val},
                                    "evaluation_investigation", ORG, "kisumu_v3")
        assert out["is_walkin"] == want


def test_sample_empty_status_is_collected():
    co = v2v3._PER_KEY_COERCIONS["evaluation_sample"]["status"]
    assert (co(""), co(None), co("  "), co("completed")) == ("collected", "collected", "collected", "completed")


def test_investigation_null_flags_get_v3_defaults(monkeypatch):
    monkeypatch.setattr(v2v3, "_v3_patient_for_v2_visit", lambda vid: 4242)
    flags = ("ordered", "on_credit", "is_walkin", "cancelled", "moved", "moved_from_chargesheet",
             "is_interim", "requires_preauth", "preauth_approved", "remove_from_report", "can_be_offered")
    out = v2v3.transform_record({"id": 575994, "visit": 1, **{f: None for f in flags}},
                                "evaluation_investigation", ORG, "kisumu_v3")
    assert {f: out[f] for f in flags} == {**{f: 0 for f in flags}, "can_be_offered": 1}
    kept = v2v3.transform_record({"id": 1, "visit": 1, "preauth_approved": "true", "can_be_offered": 0},
                                 "evaluation_investigation", ORG, "kisumu_v3")
    assert kept["preauth_approved"] == 1 and kept["can_be_offered"] == 0


class TestVitalsMultipleReadings:
    ROW = {"id": 208086, "visit": 336481, "user": 5, "created_at": "2025-11-10 10:10:43",
           "bp_systolic": "204            231    161", "bp_diastolic": "107           111       80",
           "pulse": "109", "respiration": "20", "temperature": "36.7", "oxygen": 97, "lmp": "2025-10-01"}

    def test_outpatient_first_reading_and_note(self, monkeypatch):
        monkeypatch.setattr(v2v3, "_visit_patient_map", {336481: 1})
        monkeypatch.setitem(v2v3._PER_KEY_COERCIONS["outpatient_vital"], "recorded_by", lambda v: v)
        out = v2v3.transform_record(dict(self.ROW), "outpatient_vital", ORG, "kisumu_v3")
        assert (out["bp_systolic"], out["bp_diastolic"], out["pulse"]) == (204, 107, 109)
        assert out["systolic_bp"] == 204 and out["blood_pressure"] == "204/107"
        assert out["nurse_notes"] == "Multiple readings in V2 — bp_systolic: 204 231 161; bp_diastolic: 107 111 80"
        assert out["lmp"] == "2025-10-01" and out["temperature"] == 36.7

    def test_inpatient_first_reading_and_note(self, monkeypatch):
        monkeypatch.setattr(v2v3, "_v2_admission_for_visit", lambda v: 9)
        monkeypatch.setitem(v2v3._PER_KEY_COERCIONS["inpatient_vital"], "recorded_by", lambda v: v)
        out = v2v3.transform_record({**self.ROW, "pulse": "83              77", "nurse_notes": "stable"},
                                    "inpatient_vital", ORG, "kisumu_v3")
        assert (out["systolic_bp"], out["pulse_rate"]) == (204, 83)
        assert out["notes"].startswith("stable\nMultiple readings in V2 — ") and "pulse_rate: 83 77" in out["notes"]

    def test_first_reading(self):
        f = v2v3._first_reading
        assert (f(""), f(None), f("N/A"), f("120/80"), f("36.7C"), f(98), f("72")) == (None, None, None, 120, 36.7, 98, 72)


def test_petty_cash_seed_row_is_not_posted():
    entries = [{"table": "petty_cash", "v3": r"App\Models\PettyCash", "transform": "generic"},
               {"table": "finance_invoice_dispatches", "v3": r"App\Models\X", "transform": "generic"}]
    out = {e["table"]: e for e in m.mark_skipped(entries)}
    assert out["petty_cash"]["v3"] is None and "2017 setup row" in out["petty_cash"]["skipped"]
    assert out["finance_invoice_dispatches"]["v3"] == r"App\Models\X"      # mapped tables untouched


class TestPatientDependants:
    def test_name_from_the_dependants_patient_record(self):
        out = v2v3.transform_record({"id": 1, "patient_id": 117685, "patient_dependant_id": 117469,
                                     "relationship_id": 254, "dependant_first_name": "Jane",
                                     "dependant_middle_name": "", "dependant_last_name": "Achieng ",
                                     "dependant_dob": "2015-03-01", "dependant_sex": "Female",
                                     "dependant_mobile": ""}, "reception_patient_dependant", ORG, "kisumu_v3")
        assert out == {"id": 1, "patient_id": 117685, "full_name": "Jane Achieng",
                       "relationship": v2v3._NOT_RECORDED, "dob": "2015-03-01", "gender": "female",
                       "mobile": None, "id_no": None, "email": None}

    def test_dependant_not_extracted_is_held_back(self):
        assert v2v3.transform_record({"id": 1, "patient_id": 117685, "patient_dependant_id": 117469},
                                     "reception_patient_dependant", ORG, "kisumu_v3") is None

    def test_fetch_sql_joins_both_patient_tables(self):
        import v3_extra_mappings as xm
        cols = {"patient_dependants": {"id", "patient_id", "patient_dependant_id"},
                "patients": {"id", "first_name", "last_name", "sex"},
                "reception_patients": {"id", "first_name", "middle_name", "last_name"}}
        sql = xm.FETCH_SQL["patient_dependants"]("C", lambda t: cols[t], set(cols))
        assert 'COALESCE(p0."FIRST_NAME", p1."FIRST_NAME") AS "DEPENDANT_FIRST_NAME"' in sql
        assert 'COALESCE(p1."MIDDLE_NAME") AS "DEPENDANT_MIDDLE_NAME"' in sql
        assert "LEFT JOIN C.PATIENTS p0" in sql and "LEFT JOIN C.RECEPTION_PATIENTS p1" in sql


def test_prescription_moved_from_chargesheet_message():
    msg = "Item removed from chargesheet by Mr. Charles  Byron on Sep 23, 2024 12:11:15 pm"
    out = v2v3.transform_record({"id": 846593, "visit": 1, "moved_from_chargesheet": msg, "notes": "BD x 5/7"},
                                "evaluation_prescription", ORG, "kisumu_v3")
    assert out["moved_from_chargesheet"] == 1
    assert out["notes"] == "BD x 5/7\nItem removed from chargesheet by Mr. Charles Byron on Sep 23, 2024 12:11:15 pm"
    plain = v2v3.transform_record({"id": 2, "visit": 1, "moved_from_chargesheet": "0"},
                                  "evaluation_prescription", ORG, "kisumu_v3")
    assert plain["moved_from_chargesheet"] == "0" and "notes" not in plain


def test_orphan_beds_are_skipped_not_posted():
    assert ("ward_id", "inpatient_wards", "id") in m._SKIP_IF_PARENT_NOT_IN_FACILITY["inpatient_bed"]
    assert m._CRITICAL_FK_FIELDS["inpatient_bed"] == ["ward_id", "bed_type_id"]


def test_rebates_wait_for_the_v3_backend():
    out = m.mark_skipped([{"table": "settings_rebates", "v3": r"App\Models\Rebate", "transform": "settings_rebate"}])
    assert out[0]["v3"] is None and out[0]["skipped"].startswith("BLOCKED ON V3 BACKEND")


def test_stock_from_store_products():
    import v3_extra_mappings as xm
    tk = xm.transform_key("inventory_store_products")
    assert v2v3.NAMESPACE_MAP[r"Ignite\Inventory\Entities\StoreProduct"]["transform"] == tk
    base = {"id": 1, "product_id": 34461, "store_id": 381, "unit_cost": 8.97, "lot_no": None,
            "selling_price": "0.00", "expiry_date": "0000-00-00"}
    out = v2v3.transform_record({**base, "quantity": 12}, tk, ORG, "kisumu_v3")
    assert out == {"id": 1, "product_id": 34461, "store_id": 381, "quantity": 12, "available_quantity": 12,
                   "reserved_quantity": 0, "average_cost": 8.97, "total_value": 107.64}
    neg = v2v3.transform_record({**base, "quantity": -5}, tk, ORG, "kisumu_v3")
    assert (neg["quantity"], neg["available_quantity"], neg["total_value"]) == (0, 0, 0)
    assert m._CRITICAL_FK_FIELDS[tk] == ["product_id", "store_id"]
