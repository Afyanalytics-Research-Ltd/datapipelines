import medhanit_to_v3 as m
import medhanit_v1_to_snowflake as x


def test_gid_is_stable_and_per_table():
    assert m.gid("evaluation_visits", 102925) == m.gid("evaluation_visits", " 102925 ") != m.gid("reception_patients", 102925)


def test_results_pairs_reads_v1_result_json():
    raw = '[[843,"Loose, mucoid, not blood stained"],[867,"<p>No trophozoites<\\/p>"]]'
    assert m.results_pairs(raw) == [(843, "Loose, mucoid, not blood stained"), (867, "<p>No trophozoites</p>")]
    assert m.results_pairs('[[965,""]]') == [(965, "")]
    assert m.results_pairs('{"221": "Negative"}') == [(221, "Negative")]
    assert m.results_pairs("plain text result") == [(0, "plain text result")]
    assert m.results_pairs(None) == [] and m.results_pairs("") == []


def test_range_and_flag():
    assert m.range_text("2.7", "8.3") == "2.7 - 8.3" and m.range_text(None, "5") == "< 5" and m.range_text("", None) is None
    assert m.result_flag("9.1", "2.7", "8.3") == "H" and m.result_flag("1", "2.7", "8.3") == "L"
    assert m.result_flag("5", "2.7", "8.3") is None and m.result_flag("Negative", "1", "2") is None


def test_patient_no_prefix_and_duplicates():
    assert m.patient_no({"id": 226, "patient_no": "194"}, set()) == "MDH-194"
    assert m.patient_no({"id": 227, "patient_no": "194"}, {"194"}) == "MDH-194-227"
    assert m.patient_no({"id": 5, "patient_no": None}, set()) == "MDH-ID5"


def test_prior_load_numbering_and_name_key():
    assert m.prior_patient_no({"id": 226, "patient_no": "194"}) == "MH-194"
    assert m.prior_patient_no({"id": 45970, "patient_no": None}) == m.prior_patient_no({"id": 45970, "patient_no": " "}) == "MH-V2-45970"
    assert m.name_key("Total blood count (TBC)") == m.name_key("TOTAL BLOOD COUNT(TBC)") == "totalbloodcounttbc"
    assert m.name_key(None) == ""


def test_procedure_codes_never_collide():
    rows = [{"id": 1, "code": ""}, {"id": 587, "code": "P1"}, {"id": 9, "code": "CBC"}, {"id": 10, "code": "cbc"},
            {"id": 11, "code": "FBS"}]
    c = m.procedure_codes(rows)
    assert c["587"] == "MDH-P1" and c["1"] == "MDH-P1-2"          # V1 has a real code 'P1': the generated one moves
    assert c["11"] == "MDH-FBS" and (c["9"], c["10"]) == ("MDH-P9", "MDH-P10")   # a code two rows share isn't used
    assert len({v.upper() for v in c.values()}) == len(rows)


def test_patient_payload():
    r = {"id": 226, "patient_no": "194", "first_name": "Faith", "middle_name": "", "last_name": "Moraa", "dob": "1988-09-30",
         "sex": "female", "mobile": "712345678", "telephone": "", "email": "x", "id_no": "123", "status": 1,
         "created_at": "2017-09-17 18:19:47", "updated_at": "2020-03-30 16:29:52", "deleted_at": None}
    p = m.patient_payload(r, "MDH-194", {"first_name": "John", "last_name": "Moraa", "mobile": "0722000000", "relationship": "Brother"})
    assert p["uuid"] == m.gid("reception_patients", 226) and p["patient_no"] == "MDH-194"
    assert (p["first_name"], p["middle_name"], p["last_name"]) == ("Faith", None, "Moraa")
    assert p["gender"] == "female" and p["dob"] == "1988-09-30" and p["mobile"] == "0712345678" and p["email"] is None
    assert p["emergency_contact_name"] == "John Moraa" and p["emergency_contact_phone"] == "0722000000"
    assert p["registration_date"] == "2017-09-17" and p["status"] == "active" and "V1 patient number: 194" in p["notes"]


def test_doctor_note_skips_empty_and_maps_fields():
    empty = {"id": 1, "presenting_complaints": "", "past_medical_history": "", "examination": "", "diagnosis": "",
             "investigations": "", "treatment_plan": "", "office_treatment": None}
    assert m.doctor_note_payload(empty, 1, 2, 3) is None
    p = m.doctor_note_payload({**empty, "presenting_complaints": "<p>Fever</p>", "d1": "B54", "created_at": "2026-08-14 18:19:27"}, 10, 20, 5)
    assert p["presenting_complaints"] == p["history_of_present_illness"] == "Fever"
    assert p["secondary_diagnosis_1"] == "B54" and p["visit_id"] == p["visit"] == 10 and p["doctor_id"] == 5


def test_visit_and_destination_payloads():
    v = m.visit_payload({"id": 102925, "payment_mode": "insurance", "created_at": "2026-08-14 18:15:57"}, 7, 20, 3, 9)
    assert v["unique_id"] == "MDH-102925" and v["clinic"] == 20 and v["payment_mode"] == "insurance" and v["scheme"] == 9
    d = m.destination_payload({"id": 1, "department": "Nursing", "checkout": 0, "created_at": "2026-08-14 18:15:57"}, 7, 305, "Nursing", 3)
    assert d["destination_id"] == 305 and d["status"] == "pending" and d["checkout"] == 0
    assert m.destination_payload({"id": 1, "department": "Doctor", "checkout": 1}, 7, 300, "Doctor Consultation", 3)["status"] == "completed"


def test_reference_range_words_become_text():
    r = m.reference_range_payload({"id": 1, "procedure": 925, "type": "range", "lower": "2.7", "upper": "8.3"}, 11, "Urea")
    assert r["lower"] == "2.7" and r["upper"] == "8.3" and r["type"] == "range"
    t = m.reference_range_payload({"id": 2, "lower": "Post menopausal 10.87", "upper": "58.64"}, 11, "FSH")
    assert t["type"] == "text" and t["text_reference"] == "Post menopausal 10.87 – 58.64" and "lower" not in t


def test_payment_payload_splits_modes():
    p = m.payment_payload({"id": 6137, "receipt": "210826170417", "amount": "2000.00", "created_at": "2026-08-21 17:04:17"},
                          {"cash": 1500.0, "mpesa": 500.0}, (1, "Faith Moraa", "MDH-194"), 7, 3, "Aurelia Tremblay")
    assert p["receipt"] == "MDH-210826170417" and p["cash_amount"] == 1500 and p["mpesa_amount"] == 500
    assert p["payment_mode"] == "cash" and p["patient_no"] == "MDH-194" and p["receipt_date"] == "2026-08-21"
    assert m.payment_payload({"id": 1, "amount": "50", "created_at": "2026-08-21"}, {}, None, None, 3, None)["cash_amount"] == 50


def test_ciphertext_detection():
    enc = [{"first_name": "CwDx2DVxu+KDrg0wr6PXSw==", "last_name": "pyplEk63wUPeLUxeUf6yOQ=="}]
    assert x.looks_encrypted(enc, ("first_name", "last_name"))
    assert not x.looks_encrypted([{"first_name": "Faith", "last_name": "Moraa"}], ("first_name", "last_name"))
    assert not x.looks_encrypted([{"first_name": None}], ("first_name",))
