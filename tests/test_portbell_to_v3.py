import json
from datetime import date

import portbell_to_v3 as p

PATIENT = {"patient_uuid": "u-1", "patient_no": "001/26", "register": "over-5 register", "opd_no": "001/26",
           "first_name": "MARY", "middle_name": "WANJIKU", "last_name": "KANYORA", "sex": "female", "phone": "0721481619",
           "residence": "KIANYAGA", "national_id": "22408679", "occupation": "FARMING", "guardian_name": None,
           "age_text": "68 years", "age_recorded_on": date(2026, 4, 30), "dob_estimated": date(1958, 4, 30),
           "first_visit_date": date(2026, 4, 30), "last_visit_date": date(2026, 7, 1), "name_source": "over-5 register"}
VISIT = {"visit_key": "O:001/26@2026-04-30", "visit_uuid": "abcdef12-0000", "visit_date": date(2026, 4, 30),
         "visit_type": "NEW", "register": "over-5 register", "temperature": 36.5, "weight": None,
         "blood_pressure": "126/70", "systolic_bp": 126, "diastolic_bp": 70, "diagnosis": "URTI; GASTRITIS",
         "diagnosis_list": json.dumps(["URTI", "GASTRITIS"]), "treatment": "AMOXIL",
         "services": "CONSULTATION: URTI (KES 200)", "service_points": "CONSULTATION"}


def test_patient_payload():
    x = p.patient_payload(PATIENT, ["Clinic booking (no date in the register): DENTAL."])
    assert x["uuid"] == "u-1" and x["patient_no"] == "001/26" and x["dob"] == "1958-04-30"
    assert x["id_no"] == "22408679" and x["passport_no"] is None and x["registration_date"] == "2026-04-30"
    assert "OPD 001/26" in x["notes"] and "estimated" in x["notes"] and x["notes"].endswith("DENTAL.")
    assert x["created_at"] == "2026-04-30 00:00:00" and x["emergency_contact_name"] is None
    kid = p.patient_payload({**PATIENT, "national_id": "P00183272", "guardian_name": "NELLY NYAKIO"})
    assert kid["passport_no"] == "P00183272" and kid["id_no"] is None
    assert kid["emergency_contact_name"] == "NELLY NYAKIO" and kid["emergency_contact_phone"] == "0721481619"


def test_visit_vitals_and_note_payloads():
    v = p.visit_payload(VISIT, 77, 9)
    assert v["patient"] == 77 and v["clinic"] == 9 and v["status"] == "completed" and v["type_of_visit"] == "New"
    assert v["purpose"] == "CONSULTATION" and v["unique_id"] == "PBL-V-abcdef12" and v["vitals_recorded"] == 1
    vt = p.vitals_payload(VISIT, 5, 77)
    assert vt["systolic_bp"] == 126 and vt["temperature"] == 36.5 and vt["recorded_at"] == "2026-04-30 00:00:00"
    assert p.vitals_payload({**VISIT, "temperature": None, "blood_pressure": None}, 5, 77) is None
    n = p.doctor_note_payload(VISIT, 5, 77)
    assert n["assessment"] == "URTI; GASTRITIS" and n["primary_diagnosis"] == "URTI"
    assert json.loads(n["secondary_diagnoses"]) == ["GASTRITIS"] and n["treatment_plan"] == "AMOXIL"
    assert n["plan"] == "Treatment: AMOXIL\nServices: CONSULTATION: URTI (KES 200)"
    assert p.doctor_note_payload({**VISIT, "diagnosis": None, "treatment": None, "services": None}, 5, 77) is None


def test_payment_payload():
    line = {"payment_uuid": "pay-1", "amount_paid": 200.0, "source_row": 14, "visit_date": date(2026, 6, 24),
            "service_point": "CONSULTATION", "treatment_info": "HTN", "patient_name": "ANN NJERI"}
    x = p.payment_payload(line, 77, 1136)
    assert x["receipt_no"] == "PBL-R-14" and x["payment_type"] == "cash" and x["patient_id"] == 77
    assert x["processed_by"] == x["created_by"] == 1136 and x["payment_date"] == "2026-06-24"
    assert "not found" not in x["notes"]
    assert "no patient name" in p.payment_payload({**line, "patient_name": None}, None, 1)["notes"]
    assert p.payment_payload({**line, "amount_paid": 0}, 77, 1) is None
