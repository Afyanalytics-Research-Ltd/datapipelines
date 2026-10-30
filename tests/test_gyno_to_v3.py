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
    assert n["presenting_complaints"] == n["history_of_present_illness"] == "Pain lower abdomen" and n["treatment_plan"] == "Paracetamol"
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
    assert (p["selling_price"], p["insurance_price"], p["cost_price"], p["trade_price"]) == (10.0, 13.0, 6.0, 6.0)
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


def test_procedure_price_payload():
    lab = g.procedure_price_payload("laboratory", {"id": 4, "cost": "800", "insurance_cost": "1000"}, 77, 8)
    assert (lab["evaluation_procedure_id"], lab["cash_charge"], lab["insurance_charge"], lab["facility_id"]) == (77, 800.0, 1000.0, 8)
    svc = g.procedure_price_payload("otherservices", {"id": 5, "cost": "300.0", "insurancecost": None}, 78, 8)
    assert svc["insurance_charge"] == 300.0 and svc["status"] == 1
    assert lab["uuid"] != g.procedure_price_payload("otherservices", {"id": 4, "cost": "1"}, 1, 8)["uuid"]


def test_stock_row_carries_quantity_on_hand():
    s = g.stock_row_payload("u", 1, 89, quantity="700", unit_cost="6", selling="10.0", insurance=None)
    assert (s["quantity"], s["available_quantity"], s["average_cost"], s["total_value"]) == (700, 700, 6.0, 4200.0)
    assert s["selling_price"] == 10.0 and s["insurance_price"] == 10.0
    assert g.stock_row_payload("u", 1, 89, quantity="-3", unit_cost="5", selling="1")["quantity"] == 0


def test_person_name_and_profile():
    assert g.person_name("Dr Salome Waweru") == ("Dr", "Salome", None, "Waweru")
    assert g.person_name("Dr.Shiphrah Kuria") == ("Dr", "Shiphrah", None, "Kuria")
    assert g.person_name("DR. JOY W. KURIA") == ("Dr", "JOY", "W.", "KURIA")
    assert g.person_name("Nurse") == (None, "Nurse", None, "Nurse") and g.person_name("") == (None, "Staff", None, "Member")
    p = g.user_profile_payload({"id": 149, "name": "Dr. Evelyn Njagi", "description": "Doctor", "email": "e@x.com"}, 1196)
    assert (p["user_id"], p["first_name"], p["last_name"], p["staff_number"]) == (1196, "Evelyn", "Njagi", "GYN-U-149")
    assert p["job_description"] == "Dr Doctor" and p["work_email"] == "e@x.com"


def test_profile_title_codes():
    assert g.user_profile_payload({"id": 1, "name": "Dr Salome Waweru"}, 5)["title"] == 4
    assert "title" not in g.user_profile_payload({"id": 2, "name": "Jane Wanjiru"}, 6)


def test_rx_dosing_and_fix():
    assert g.rx_dosing(" 1 od") == ("1 od", "once a day (OD)")
    assert g.rx_dosing(" STAT") == ("STAT", "immediately (STAT)")
    assert g.rx_dosing("1 tab bd x5/7") == ("1 tab bd x5/7", "twice a day (BD)")
    assert g.rx_dosing(" ") == (None, None) and g.rx_dosing("apply thinly")[1] is None
    f = g.rx_fix_payload({"notes": "1 od"}, (89, "Main Store"), 55)
    assert f == {"status": "active", "dosage": "1 od", "frequency": None, "store_id": 89,     # the Dose column reads method
                 "store_name": "Main Store", "admission_id": 55, "take": "1", "method": 5}
    assert g.rx_dose("1 tab bd x5/7")["duration"] == "5"
    assert "admission_id" not in g.rx_fix_payload({"notes": ""}, (89, "Main Store"), None)


def test_admission_status():
    base = {"id": 7, "dateofvisit": "2025-03-01", "timeofvisit": "10:00:00", "status": "hold", "check_out": "2025-03-04"}
    a = g.admission_payload(base, 1, 2, type_id=3, doctor=4, discharge="2025-03-03 12:00:00", today="2026-10-21")
    assert a["status"] == "discharged" and a["discharge_date"] == "2025-03-03 12:00:00" and a["admission_number"] == "GYN-ADM-7"
    old = g.admission_payload(base, 1, 2, type_id=3, doctor=4, discharge=None, today="2026-10-21")
    assert old["status"] == "discharged" and old["discharge_date"] == "2025-03-04 00:00:00"
    now = g.admission_payload({**base, "dateofvisit": "2026-10-19"}, 1, 2, type_id=3, doctor=4, discharge=None, today="2026-10-21")
    assert now["status"] == "admitted" and now["discharge_date"] is None
    assert g.admission_payload({**base, "dateofvisit": "0000-00-00"}, 1, 2, type_id=3, doctor=4, discharge=None, today="2026-10-21") is None


def test_po_history_payload():
    p = g.po_history_payload({"lpostatus": "Full Delivery", "subtotal": "12000", "vat": "1920", "grandtotal": "13920",
                              "totalpayment": "0"}, 13920.0)
    assert p == {"status": "received", "subtotal": 12000.0, "tax_amount": 1920.0, "total_amount": 13920.0,
                 "paid_amount": 13920.0, "payment_status": "paid"}
    assert g.po_history_payload({"lpostatus": "Canceled", "grandtotal": "500"})["status"] == "cancelled"
    assert g.po_history_payload({"lpostatus": "Partial Delivery", "grandtotal": "1000", "totalpayment": "400"})["payment_status"] == "partial"
    assert g.po_history_payload({"lpostatus": "FirstApproval", "grandtotal": "10"})["status"] == "pending_approval"


def test_theatre_booking_payload():
    st = {"booked": 10, "completed": 14}
    up = g.theatre_booking_payload({"id": 1, "surgeryprocedure": "ECS Private", "surgeryappointment": "2026-11-04",
                                    "updated_on": "2026-10-20 08:00:00"}, 5, 6, status=st, requested_by=7, today="2026-10-21")
    assert (up["status_id"], up["preferred_date"], up["reason"], up["requested_by"]) == (10, "2026-11-04", "ECS Private", 7)
    past = g.theatre_booking_payload({"id": 2, "surgeryprocedure": "TAH", "surgeryappointment": "2025-02-01"}, 5, 6,
                                     status=st, requested_by=7, today="2026-10-21")
    assert past["status_id"] == 14
    assert g.theatre_booking_payload({"id": 3, "surgeryprocedure": "", "surgeryappointment": "0000-00-00"}, 5, 6,
                                     status=st, requested_by=7, today="2026-10-21") is None


def test_evaluation_payment_payload():
    r = {"id": 120690, "amount": "1000", "date_of_transaction": "2026-10-20 10:43:04", "descrpt": "UJKK0B5934-JANE",
         "invoiceno": "120972/2026", "updated_by": "Ann Kariuki"}
    p = g.evaluation_payment_payload(r, patient=(55, "JANE DOE", "GYN-9"), visit_id=7, invoice_id=8, ptype=("MPESA", "No"),
                                     cashier=1150, insurer=None)
    assert p["receipt"] == "120972/2026" and p["payment_mode"] == "mpesa" and p["mpesa_amount"] == 1000.0
    assert p["cash_amount"] == 0.0 and p["amount"] == 1000.0 and p["patient"] == 55 and p["patient_no"] == "GYN-9"
    assert p["created_at"] == "2026-10-20 10:43:04" and p["receipt_date"] == "2026-10-20" and p["transaction_id"] == "UJKK0B5934-JANE"
    ins = g.evaluation_payment_payload({**r, "invoiceno": None}, patient=None, visit_id=None, invoice_id=None,
                                       ptype=("CIC", "Yes"), cashier=1, insurer=33)
    assert ins["company"] == 33 and ins["receipt"] == "GYN-R-120690" and all(ins[c] == 0.0 for c in g.MODE_COLUMN.values())
    d = g.evaluation_payment_payload({"id": 4, "amount": "500", "updated_on": "2021-01-01 09:00:00"}, patient=None, visit_id=None,
                                     invoice_id=None, ptype=("Cash", "No"), cashier=1, insurer=None, deposit=True)
    assert d["receipt"] == "GYN-D-4" and d["cash_amount"] == 500.0
    det = g.evaluation_payment_detail_payload(r, 99, visit_id=7, invoice_id=8, method="MPESA")
    assert det["payment"] == 99 and det["amount"] == 1000.0 and det["name"] == "UJKK0B5934-JANE"


def test_fp_item_mapping():
    assert g.fp_item("Microgynon Fe (pack)") == (["combined_oral_pills"], [])
    assert g.fp_item("Microlut pills  0.03mg") == (["progestin_only_pills"], [])
    assert g.fp_item("Depo injection") == (["dmpa_im"], []) and g.fp_item("Primolut Depo") is None
    assert g.fp_item("Mirena Subsidized") == (["iucd_hormonal"], [])
    assert g.fp_item("Multiload  IUD Insertion") == (["iucd_non_hormonal"], [])
    assert g.fp_item("Lost IUD removal") == ([], ["iucd"]) and g.fp_item("Jadelle Removal") == ([], ["implant"])
    assert g.fp_item("Implanon Insertion") == (["implant_1_rod"], []) and g.fp_item("BTL") == (["btl"], [])
    assert g.fp_item("POSTINOR -2 TABS") == (["emergency_contraceptive_pill"], [])
    assert g.fp_item("IUD SET") is None and g.fp_item("Paracetamol") is None


def test_fp_service_payload():
    p = g.fp_service_payload(5, 6, 77, "2024-01-02", [(["iucd_hormonal"], []), ([], ["implant"])], ["Mirena", "Jadelle Removal"],
                             revisit=True, reinsertion=True)
    assert p["methods"] == ["iucd_hormonal"] and p["removals"] == ["implant"] and p["client_type"] == "revisit"
    assert p["insertion_type"] == "reinsertion" and p["notes"].startswith("Gyno visit 77:")
    pills = g.fp_service_payload(5, 6, 78, "2024-01-02", [(["combined_oral_pills"], [])], ["Microgynon"], revisit=False, reinsertion=True)
    assert pills["insertion_type"] is None and pills["client_type"] == "new"
    btl = g.fp_service_payload(5, 6, 79, "2024-01-02", [(["btl"], [])], ["BTL"], revisit=True, reinsertion=True)
    assert btl["insertion_type"] == "first_insertion"


def test_blood_group_and_pregnancy():
    assert g.blood_group("O POSITIVE") == ("O", "+") and g.blood_group("o+") == ("O", "+")
    assert g.blood_group("B NEGATIVE") == ("B", "-") and g.blood_group("AB") == ("AB", None)
    assert g.blood_group("A +ve") == ("A", "+") and g.blood_group("") == (None, None)
    assert g.current_pregnancy({"due_date": "2027-03-01", "first_day_of_last_lmp": "2026-05-25"}, "2026-10-22") == ("2026-05-25", "2027-03-01")
    assert g.current_pregnancy({"due_date": "2027-03-01", "first_day_of_last_lmp": ""}, "2026-10-22") == ("2026-05-25", "2027-03-01")
    assert g.current_pregnancy({"due_date": "2026-10-01"}, "2026-10-22") is None
    assert g.current_pregnancy({"due_date": "2028-01-01"}, "2026-10-22") is None


def test_immunization_payload():
    p = g.immunization_payload("pharmacy_records", {"id": 9, "quantity": "1.0", "status": "No", "updated_by": "Nurse A",
                                                    "updated_on": "2024-03-04 10:00:00"}, 11, 12, "Tetanus vaccine 10ml", 1)
    assert p["uuid"] == g.gid("immunization-pharmacy_records", 9) and p["visit_id"] == 11 and p["patient_id"] == 12
    assert p["administration_date"] == "2024-03-04" and "not marked dispensed" in p["notes"]
    assert g.VACCINE_RE.search("TETANUS VACCINE 10ML") and not g.VACCINE_RE.search("HEPARIN INJ")


def test_drug_insurance_price():
    assert g.drug_insurance_price("100") == 133.0 and g.drug_insurance_price("150", "") == 200.0   # 199.5 → 200
    assert g.drug_insurance_price("100", "180") == 180.0 and g.drug_insurance_price("0") is None
    p = g.product_payload({"id": 1, "title": "Panadol", "cost": "50", "buying_price": "20"}, 1, 1)
    assert p["selling_price"] == 50.0 and p["insurance_price"] == 67.0   # 66.5 → 67


def test_rx_dose():
    assert g.rx_dose(" 1 BD FOR 10/7") == {"frequency": None, "take": "1", "method": 1, "duration": "10", "time_measure": 1}
    assert g.rx_dose("120 mg od for 5/7 then Prn") == {"frequency": None, "take": "120", "unit": "mg", "method": 5,
                                                       "duration": "5", "time_measure": 1}
    assert g.rx_dose("1tab tds 10/7")["take"] == "1" and g.rx_dose("1tab tds 10/7")["method"] == 2
    assert g.rx_dose("0.5mls im stat") == {"frequency": None, "take": "0.5", "unit": "ml", "method": 4, "whereto": 10}
    assert g.rx_dose(" insert pv nocte") == {"frequency": None, "method": 23, "whereto": 21}
    assert g.rx_dose("1 bd* 1/52 ")["time_measure"] == 2 and g.rx_dose(" 1 bd 1/12")["time_measure"] == 3
    assert g.rx_dose(" one tab bd")["take"] == "1" and g.rx_dose(" 1 tab od for 30 days")["duration"] == "30"
    assert g.rx_dose(" partner rx") == {"frequency": None} and g.rx_dose("") == {}


def test_rx_dose_multiline_is_notes_only():
    d = g.rx_dose(" cefasyn 1 bd 5/7\r\nnuclin v 1 od 3/7\r\npartner sec/azm")
    assert d and all(v is None for v in d.values()) and "method" in d


def test_core_insurer_match_and_scheme_type():
    core = {"social health authority (sha)": 1, "aar insurance": 2, "cic insurance": 3, "apa insurance": 4, "jubilee insurance": 5}
    assert g.core_insurer_match("SHA-SHIF", core) == 1 and g.core_insurer_match("SHIF", core) == 1
    assert g.core_insurer_match("SHA - MWALIMU MEDICAL", core) == 1 and g.core_insurer_match("AAR INSURANCE", core) == 2
    assert g.core_insurer_match("Cooperative Insurance", core) == 3 and g.core_insurer_match("APA", core) == 4
    assert g.core_insurer_match("NHIF NATIONAL SCHEME", core) is None and g.core_insurer_match("PACIS INSURANCE", core) is None
    assert g.core_insurer_match("CO-OP BANK", core) is None
    assert g.scheme_type("SHA - PUBLIC OFFICER") == "government" and g.scheme_type("NHIF CIVIL SERVANT") == "government"
    assert g.scheme_type("MINET") == "private"


def test_rx_dose_spelled_out_frequency():
    assert g.rx_dose("take one tablet twice daily for 7 days") == {"frequency": None, "take": "1", "method": 1,
                                                                   "duration": "7", "time_measure": 1}
    assert g.rx_dose("take one tablet three times daily for 5 days")["method"] == 2
    assert g.rx_dose("1 tab once daily")["method"] == 5 and g.rx_dose("take one tablet stat")["method"] == 4
    assert g.rx_dose("2 tabs at night")["method"] == 23 and g.rx_dose("1 when needed")["method"] == 19


def test_lab_catalogue_mapping_keys():
    assert g.lab_key("Full Haemogram (Cbc/Fbc)") == "full haemogram cbc fbc" and g.LAB_CATALOGUE_TO_GYNO["full haemogram cbc fbc"] == 5
    assert g.lab_key("Thyroid Profile, Free T3 T4 .TSH") in g.LAB_CATALOGUE_TO_GYNO
    assert g.lab_key("Hiv I&Ii Antibody Test") in g.LAB_CATALOGUE_TO_GYNO and g.lab_key("Pct") in g.LAB_CATALOGUE_SKIP


def test_consumables():
    assert g.is_consumable("Disposable Speculum") and g.is_consumable("Giving Set") and g.is_consumable("Cannular")
    assert not g.is_consumable("OG Kit Tabs") and not g.is_consumable("Triokit 4S") and not g.is_consumable("Azithromycin 500mg")
    assert not g.is_consumable(None) and g.is_consumable("Gauzes(Sterile)") and g.is_consumable("Blood GIving Set")
    p = g.consumable_payload({"id": 7, "quantity": "2", "unitpharmacycost": "150", "totalcost": "300", "status": "Yes",
                              "updated_by": "Nurse A", "updated_on": "2024-05-01 10:00:00"}, 11, 22, 33, "Giving Set")
    assert p["uuid"] == g.gid("consumable-pharmacy_records", 7) and (p["visit_id"], p["product_id"], p["user_id"]) == (11, 22, 33)
    assert p["quantity"] == 2.0 and p["amount"] == 300.0 and p["status"] == "administered" and p["bill"] == 1
