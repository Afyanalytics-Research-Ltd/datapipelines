from datetime import date

import load_portbell_to_snowflake as pb


def test_excel_dates_are_swapped_back_and_text_is_ddmm():
    assert pb.register_date("2026-01-05 00:00:00")[0] == date(2026, 5, 1)       # Excel read 01/05 as Jan 5
    assert pb.register_date("30/04/2026") == (date(2026, 4, 30), None)
    assert pb.register_date("  25/09/2026")[0] == date(2026, 9, 25)
    d, why = pb.register_date("23/82026")
    assert d == date(2026, 8, 23) and "malformed" in why
    assert pb.register_date(None) == (None, None) and pb.register_date("31/02/2026")[0] is None


def test_ages():
    assert pb.age("45") == (45.0, "45 years")
    assert pb.age("1.11") == (round(1 + 11 / 12, 3), "1 years 11 months")
    assert pb.age("3.6")[1] == "3 years 6 months"
    assert pb.age("8MNTHS") == (round(8 / 12, 3), "8 months")
    assert pb.age("4 MONTHS F ")[1] == "4 months"
    assert pb.age("6WKS")[1] == "6 weeks"
    assert pb.age("2.4YERS")[1] == "2 years 4 months"
    assert pb.age(None) == (None, None)


def test_phone_sex_kind_and_names():
    assert pb.phone("721481619") == "0721481619" and pb.phone("101738306") == "0101738306"
    assert pb.phone("721481619.0") == "0721481619" and pb.phone("11930629") is None
    assert pb.sex("f") == "female" and pb.sex("MALE") == "male" and pb.sex("") is None
    assert pb.visit_kind("N") == "NEW" and pb.visit_kind("REVIST") == "REVISIT" and pb.visit_kind(None) is None
    assert pb.name_of("PATIENCE  WAMAITHA ") == "PATIENCE WAMAITHA" and pb.name_of("4") is None
    assert pb.service_point("NURSING  STATION ") == "NURSING STATION" and pb.service_point("DENTAL UNIT") == "DENTAL"
    assert pb.diagnoses("URTI,GASTRITIS, ATHRITIS") == ["URTI", "GASTRITIS", "ATHRITIS"]
    assert pb.diagnoses("HUMAN BITE,. TRUMA FALL") == ["HUMAN BITE", "TRUMA FALL"]


def test_patient_numbers():
    assert pb.patient_no("O:001/26", "001/26", 0) == "001/26"
    assert pb.patient_no("U:001/26", "001/26", 0) == "U5-001/26"
    assert pb.patient_no("D:JANE DOE", None, 7) == "PBL-D0007"
    assert pb.patient_no("O:001/26#row40", "001/26", 0) == "001/26-R40"


def _over5(row, opd, name, when, phone=None):
    return {"_SOURCE_ROW": row, "DATE": when, "OPD_NO": opd, "NEW_REVISIT": "N", "PATIENT_NAME": name, "AGE": "40",
            "SEX": "F", "CONTACT": phone, "TEMP": "36.5", "BP": "120/80", "DIAGNOSIS": "UTI", "TREATMENT": "CIPRO",
            "MODE_OF_PAYMENT": "CASH"}


def _daily(row, when, name, opd=None, phone=None, point="CONSULTATION", info="UTI", amount="200", kind="REVIST"):
    return {"_SOURCE_ROW": row, "SERIAL_NO": "1", "DATE_OF_VISIT": when, "OPD_NO": opd, "PATIENT_NAME": name,
            "TYPE_OF_VISIT_NEW_RE_VISIT": kind, "AGE": "40", "SEX": "F", "CONTACT": phone,
            "SERVICE_DELIVERY_POINT": point, "TREATMENT_INFORMATION_DX": info, "AMOUNT_PAID": amount}


def test_cleaner_links_daily_lines_and_checks_totals():
    raw = {
        "OVER_5": [_over5(2, "001/26", "MARY WANJIKU", "2026-01-07 00:00:00", "721000001"),     # = 1 Jul
                   _over5(3, "002/26", None, "2/7/2026")],
        "UNDER_5": [{"_SOURCE_ROW": 2, "DATE": "2/7/2026", "OPD_NO": "001/26", "PATIENT_NAME": "BABY ONE", "AGE": "8MNTHS",
                     "SEX": "M", "PARENT_GUARDIAN_NAME": "MARY WANJIKU", "CONTACT": "721000001"}],
        "DAILY_ATTENDANCE": [
            _daily(2, "1/7/2026", "MARY WANJIKU", opd="001/26"),                 # same visit as the register row
            _daily(3, "1/7/2026", "MARY WANJIKU", point="LABORATORY", info="RBS", amount="100"),
            {"_SOURCE_ROW": 4, "SERIAL_NO": "TOTALS", "AMOUNT_PAID": "300", "TOTALS": "300"},
            _daily(5, "2/7/2026", "BABY ONE", opd="001/26", info="URTI"),       # OPD 001/26 → the under-5 child
            _daily(6, "2/7/2026", "JOHN DOE", opd="002/26"),                    # names the nameless register row
            _daily(7, "2/7/2026", "WALK IN", phone="722000000", point="NURSING STATION", info="BP MONITORING", amount="20"),
            _daily(8, None, "WALK IN", phone="722000000", point="NURSING STATION", info="DEPO", amount="#VALUE!"),
            _daily(9, "2/7/2026", None, point="NURSING STATION", info="BP MONITORING", amount="20"),
            {"_SOURCE_ROW": 10, "AMOUNT_PAID": "500"},                          # bare subtotal (clerk wrote 500, lines = 440)
        ],
        "CLINIC_BOOKINGS": [{"_SOURCE_ROW": 2, "OPD_NO": None, "NAME": "WALK IN", "CONTACT": "722000000",
                             "TYPE_OF_CLINICBOOKED": "DENTAL"}],
    }
    t = pb.Cleaner(raw).build()
    pats = {p["patient_key"]: p for p in t["PATIENTS"]}
    assert set(pats) == {"O:001/26", "O:002/26", "U:001/26", "D:WALK IN"}
    assert pats["O:002/26"]["full_name"] == "JOHN DOE" and pats["U:001/26"]["patient_no"] == "U5-001/26"
    assert pats["U:001/26"]["guardian_name"] == "MARY WANJIKU" and pats["O:001/26"]["phone"] == "0721000001"
    visits = {v["visit_key"]: v for v in t["VISITS"]}
    mary = visits["O:001/26@2026-07-01"]
    assert mary["visit_type"] == "NEW" and mary["amount_paid"] == 300 and mary["systolic_bp"] == 120
    assert mary["service_points"] == "CONSULTATION; LABORATORY" and mary["diagnosis"] == "UTI"
    assert visits["U:001/26@2026-07-02"]["diagnosis"] == "URTI"
    walk = visits["D:WALK IN@2026-07-02"]
    assert walk["visit_type"] == "REVISIT" and walk["amount_paid"] == 20      # '#VALUE!' line counts as no money
    unlinked = [a for a in t["ATTENDANCE"] if a["patient_key"] is None]
    assert len(unlinked) == 1 and unlinked[0]["payment_uuid"]                 # its payment is still posted
    totals = {x["source_row"]: x for x in t["DAILY_TOTALS"]}
    assert totals[4]["day"] == date(2026, 7, 1) and totals[4]["difference"] == 0
    assert totals[10]["computed_total"] == 440 and totals[10]["difference"] == 60
    assert t["CLINIC_BOOKINGS"][0]["patient_key"] == "D:WALK IN"
    issues = {(i["sheet"], i["source_row"], i["column_name"]) for i in t["DATA_ISSUES"]}
    assert ("OVER_5", 2, "DATE") in issues and ("DAILY_ATTENDANCE", 8, "AMOUNT_PAID") in issues
    assert ("DAILY_ATTENDANCE", 8, "DATE_OF_VISIT") in issues


def test_family_phone_needs_a_similar_name():
    raw = {"OVER_5": [_over5(2, "010/26", "EDITH WANGU", "2/7/2026", "717599134")],
           "DAILY_ATTENDANCE": [_daily(2, "3/7/2026", "ABIGAEL WAMBERE", phone="717599134"),
                                _daily(3, "3/7/2026", "EDYTH WANGU", phone="717599134")]}
    t = pb.Cleaner(raw).build()
    keys = [a["patient_key"] for a in t["ATTENDANCE"]]
    assert keys == ["D:ABIGAEL WAMBERE", "O:010/26"]


def test_uuids_are_stable():
    assert pb.gid("patient", "O:001/26") == pb.gid("patient", "O:001/26") != pb.gid("patient", "U:001/26")
