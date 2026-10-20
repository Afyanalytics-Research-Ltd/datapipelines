import gyno_to_v3 as g


def test_gid_is_stable_and_per_table():
    assert g.gid("patients", 5) == g.gid("patients", "5") != g.gid("visits", 5)


def test_dates_and_stamps():
    assert g.day("0000-00-00") is None and g.day("") is None and g.day("2024-13-01") is None
    assert g.day("2026-10-20 08:12:09") == "2026-10-20"
    assert g.stamp("2026-10-20", "9:57:23") == "2026-10-20 09:57:23"
    assert g.stamp("2026-10-20 14:05:00") == "2026-10-20 14:05:00"
    assert g.stamp("2026-10-20") == "2026-10-20 00:00:00" and g.stamp("0000-00-00", "10:00") is None


def test_split_name_and_phone():
    assert g.split_name("ELIZABETH WANJIRU KAMAU") == ("ELIZABETH", "WANJIRU", "KAMAU")
    assert g.split_name("MARY  ATIENO") == ("MARY", None, "ATIENO")
    assert g.split_name("ESTHER") == ("ESTHER", None, "ESTHER")
    assert g.phone("#0712 345 678") == "0712345678" and g.phone("") is None


def test_patient_payload():
    p = g.patient_payload({"id": 24605, "full_names": "Jane Wanjiku Doe", "file_no": "", "dateofbirth": "0000-00-00",
                           "gender": "Female", "tel": "#0712345678", "email": "none", "reg_date": "2026-10-20",
                           "paymenttype": "Cash", "updated_on": "2026-10-20"})
    assert p["patient_no"] == "GYN-24605" and p["uuid"] == g.gid("patients", 24605)
    assert (p["first_name"], p["middle_name"], p["last_name"]) == ("Jane", "Wanjiku", "Doe")
    assert p["dob"] is None and p["gender"] == "female" and p["mobile"] == "0712345678" and p["email"] is None
    assert p["created_at"] == "2026-10-20 00:00:00" and "Gyno file no" not in p["notes"]
    assert "Gyno file no: 7/22" in g.patient_payload({"id": 1, "full_names": "A B", "file_no": "7/22"})["notes"]


def test_visit_payload_status_and_bookings():
    r = {"id": 9, "dateofvisit": "2025-01-02", "timeofvisit": "10:30:00", "status": "hold", "is_insurance": "Yes",
         "is_inpatient": "No", "pstatus": "Review"}
    p = g.visit_payload(r, 77, "2026-10-20")
    assert p["status"] == "completed" and p["patient"] == 77 and p["payment_mode"] == "insurance"
    assert p["created_at"] == "2025-01-02 10:30:00" and p["unique_id"] == "GYN-V-9"
    assert g.visit_payload({**r, "dateofvisit": "2026-10-20"}, 77, "2026-10-20")["status"] == "waiting"
    assert g.visit_payload({**r, "status": "booking", "dateofvisit": "2026-12-05"}, 77, "2026-10-20") is None


def test_vitals_payload_parses_bp_and_numbers():
    v = g.vitals_payload({"id": 3, "pressure": "120/80 mmHg", "weight": "64 kg", "temprature": "36.5", "po2": "98%",
                          "height": "", "lmp": "2026-09-01", "triage_date": "2026-10-01 09:00:00"}, 10, 20)
    assert (v["systolic_bp"], v["diastolic_bp"], v["weight"], v["temperature"], v["oxygen_saturation"]) == (120, 80, 64.0, 36.5, 98)
    assert v["height"] is None and v["lmp"] == "2026-09-01" and v["visit_id"] == 10 and v["patient_id"] == 20


def test_doctor_note_payload_spreads_fields():
    n = g.doctor_note_payload({"id": 7, "notes": "Pain lower abdomen", "eprescription": "Paracetamol", "lmp": "2026-09-01",
                               "due_date": "2027-06-08", "noofweeks": "6", "nextappointment": "0000-00-00",
                               "updated_on": "2026-10-01 10:00:00"}, 1, 2, ("Dr. A", "Gynaecology"))
    assert n[g.NOTE_FIELD] == "Pain lower abdomen" and n["treatment_plan"] == "Paracetamol"
    assert "LMP: 2026-09-01" in n["remarks"] and "EDD: 2027-06-08" in n["remarks"] and n["next_visit_date"] is None
    assert n["doctor_name"] == "Dr. A" and n["visit_id"] == 1 and n["patient_id"] == 2


def test_payment_type_mapping():
    assert g.payment_type("MPESA (Githunguri)", "No") == "mpesa" and g.payment_type("Cash", "No") == "cash"
    assert g.payment_type("VISA", "No") == "card" and g.payment_type("Direct Bank Deposit ", "No") == "bank_transfer"
    assert g.payment_type("SHA-SHIF", "No") == "insurance" and g.payment_type("CIC", "Yes") == "insurance"
    assert g.payment_type(None, None) == "other"


def test_payment_payload():
    p = g.payment_payload({"id": 120690, "amount": "1000", "date_of_transaction": "2026-10-20 10:43:04",
                           "descrpt": "UJKK0B5934-JANE", "invoiceno": "120972/2026"}, 55, ("MPESA", "No"), 1132)
    assert p["receipt_no"] == "GYN-R-120690" and p["payment_type"] == "mpesa" and p["amount"] == 1000.0
    assert p["payment_date"] == "2026-10-20" and p["reference_no"] == "120972/2026" and p["processed_by"] == 1132
    assert g.payment_payload({"id": 1, "amount": "5"}, None, None, 1) is None


def test_prescription_and_investigation_payloads():
    rx = g.prescription_payload({"id": 4, "quantity": "2", "unitpharmacycost": "50", "status": "Yes",
                                 "updated_on": "2026-01-01 08:00:00"}, 9, 300, "AMOXIL", 1132)
    assert rx["dispensed"] == 1 and rx["dispensed_quantity"] == 2.0 and rx["drug"] == 300 and rx["status"] == "dispensed"
    inv = g.investigation_payload("u", 9, 8, type_="laboratory", procedure=5, quantity=None, price="800", amount=None,
                                  when="2026-01-01 08:00:00", result="Hb 12")
    assert inv["price"] == 800.0 and inv["amount"] == 800.0 and inv["quantity"] == 1 and inv["result"] == {"text": "Hb 12"}


def test_nurse_note_and_appointment_skip_empty():
    assert g.nurse_note_payload({"id": 1, "cardex": " ", "updated_on": "2026-01-01"}, 9, 1) is None
    assert g.appointment_payload({"id": 1, "booking_date": "0000-00-00"}, 5) is None
    a = g.appointment_payload({"id": 2, "booking_date": "2026-11-01", "is_antenatal": "Yes"}, 5)
    assert a["appointment_no"] == "GYN-A-2" and a["reason"] == "Antenatal" and a["appointment_time"] == "09:00:00"


def test_user_payload_never_mails_and_resets():
    u = g.user_payload({"id": 12, "username": "jdoe", "email": "j@x.com"})
    assert u["username"] == "gyno.jdoe" and u["email"] == "j@x.com.gyno-12.invalid" and u["enforce_password_reset"] == 1
    assert g.user_payload({"id": 13, "username": "nomail", "email": ""})["email"] == "nomail@gyno.migrated.gyno-13.invalid"


def test_invoice_payload_status():
    base = {"id": 1, "dateofvisit": "2026-01-02", "totalpayment": "1000", "totalpaid": "1000", "balance": "0"}
    assert g.invoice_payload(base, 5, 6)["status"] == "paid"
    assert g.invoice_payload({**base, "totalpaid": "400", "balance": "600"}, 5, 6)["status"] == "partially_paid"
    assert g.invoice_payload({**base, "totalpaid": "0", "balance": "1000"}, 5, 6)["status"] == "pending"
    assert g.invoice_payload({**base, "dateofvisit": "0000-00-00"}, 5, 6) is None


def test_document_payload_uses_manifest_size():
    d = g.document_payload("externaltests", {"id": 3, "document": "1717495761.pdf", "document_title": "Scan",
                                             "updated_on": "2026-01-02"}, 9, {"1717495761.pdf": "258498"},
                           title_col="document_title", file_col="document", doc_type="lab_result")
    assert d["file_size"] == 258498 and d["file_type"] == "application/pdf" and d["file_path"] == "gyno-s3/1717495761.pdf"
    assert g.document_payload("externaltests", {"id": 4, "document": ""}, 9, {}, title_col="document_title",
                              file_col="document", doc_type="lab_result") is None


def test_insurer_and_movement():
    assert g.is_insurer("SHA - PUBLIC OFFICER", "Yes") and g.is_insurer("NHIF NATIONAL SCHEME", "No")
    assert not g.is_insurer("MPESA", "No")
    assert g.movement_payload("u", 1, 2, 3, type_="adjustment", quantity="0") is None
    m = g.movement_payload("u", 1, 2, 3, type_="issue", quantity="5", unit_cost="10", incoming=False)
    assert m["from_store_id"] == 2 and m["to_store_id"] is None and m["total_value"] == 50.0


def test_activity_payload_keeps_every_field():
    a = g.activity_payload("sendsms", {"id": 7, "phone": "0712", "themessage": "Hi"}, "SMS to 0712: Hi", None)
    assert a["log_name"] == "gyno_sendsms" and a["properties"]["themessage"] == "Hi" and a["uuid"] == g.gid("sendsms", 7)


def test_visit_route_and_destination_rows():
    r = g.visit_route("Senior Review", has_vitals=True, has_lab=True, has_ultra=False, has_rx=True, inpatient=False)
    assert r == ["triage", "doctor", "laboratory", "pharmacy"]
    assert g.visit_route("Walkin", has_vitals=False, has_lab=False, has_ultra=True, has_rx=False, inpatient=True) == \
        ["general-opd", "ultrasound", "inpatient"]
    dests = {"triage": (156, "Triage"), "doctor": (137, "Doctor Consultation"), "pharmacy": (152, "Pharmacy")}
    ev, rc = g.destination_payloads(9, 77, "2025-01-02 10:00:00", True, ["triage", "doctor", "laboratory", "pharmacy"], dests)
    assert [e["destination_id"] for e in ev] == [156, 137, 152]          # laboratory missing in V3 → skipped
    assert all(e["status"] == "completed" and e["checkout"] == 1 and e["visit_id"] == 77 for e in ev)
    assert [x["sequence"] for x in rc] == [0, 1, 3] and rc[1]["destination_slug"] == "doctor"
    ev2, _ = g.destination_payloads(9, 77, "2026-10-20 10:00:00", False, ["doctor"], dests)
    assert ev2[0]["status"] == "pending" and ev2[0]["finish_at"] is None
    assert ev[0]["uuid"] == g.destination_payloads(9, 77, None, False, ["triage"], dests)[0][0]["uuid"]   # stable per visit + stop


def test_visit_payload_sets_clinic():
    assert g.visit_payload({"id": 1, "dateofvisit": "2025-01-02"}, 5, "2026-10-20", 8)["clinic"] == 8


def test_product_names_and_prices():
    p = g.product_payload({"id": 2, "title": "Methyldopa 250mg tabs", "details": "100s", "cost": "10.0",
                           "buying_price": "6", "insurancecost": None}, 1, 1)
    assert (p["selling_price"], p["insurance_price"], p["cost_price"], p["trade_price"]) == (10.0, 10.0, 6.0, 6.0)
    assert p["generic_name"] == "Methyldopa 250mg tabs" and p["is_active"] is True
    junk = g.product_payload({"id": 9, "title": "0", "details": "null", "cost": "0.0"}, 1, 1)
    assert junk["generic_name"] == "Unnamed item 9" and junk["is_active"] is False and junk["description"] is None
    assert g.product_payload({"id": 3, "title": "X", "cost": "100", "insurancecost": "150"}, 1, 1)["insurance_price"] == 150.0


def test_receipt_payload():
    r = g.receipt_payload({"id": 3, "amount": "2000", "date_of_transaction": "2020-03-30 10:58:27", "descrpt": "OCU44QSW04"},
                          55, ("VISA", "No"), 1132, payment_id=None, invoice_id=9)
    assert r["receipt_no"] == "GYN-R-3" and r["amount"] == 2000.0 and r["receipt_date"] == "2020-03-30"
    assert r["payment_method"] == "card" and r["status"] == "valid" and r["issued_by"] == 1132 and r["invoice_id"] == 9
    d = g.receipt_payload({"id": 4, "amount": "500", "updated_on": "2021-01-01 09:00:00"}, 5, None, 1, payment_id=7,
                          invoice_id=None, deposit=True)
    assert d["receipt_no"] == "GYN-D-4" and d["receipt_type"] == "deposit" and d["payment_id"] == 7
