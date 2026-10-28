import eagle_to_v3 as e


def test_gid_is_stable_and_per_table():
    assert e.gid("visit", "ENH/OP/000027/2024") == e.gid("visit", " ENH/OP/000027/2024 ") != e.gid("patient", "ENH/OP/000027/2024")


def test_day_reads_iso_and_month_first():
    assert e.day("2024-03-14") == e.day("2024-03-14 13:10:38") == e.day("3/14/2024") == "2024-03-14"
    assert e.day("10/31/2023") == "2023-10-31" and e.day("31/3/2024") == "2024-03-31"
    assert e.day("0000-00-00") is None and e.day("2/30/2024") is None and e.day("") is None and e.day("4") is None


def test_stamp():
    assert e.stamp("2024-03-03", "03:46:41") == "2024-03-03 03:46:41"
    assert e.stamp("2024-03-14 13:10:38") == "2024-03-14 13:10:38"
    assert e.stamp("3/14/2024", "11:23:32") == "2024-03-14 11:23:32" and e.stamp("2024-03-14", "3:12") == "2024-03-14 03:12:00"
    assert e.stamp("2024-03-14") == "2024-03-14 00:00:00" and e.stamp(None, "10:00") is None


def test_clean_drops_patcom_placeholders():
    assert e.clean("Select Marital Status") is None and e.clean("-Select Citizenship-") is None
    assert e.clean("NULL") is None and e.clean(" x ") == "x" and e.clean(b"admit ") == "admit"


def test_strip_html():
    assert e.strip_html("<p>DYSPHAGIA</p>\n\n<p>RT SIDED CHEST PAINS</p>\n\n<p>&nbsp;</p>\n") == "DYSPHAGIA\nRT SIDED CHEST PAINS"
    assert e.strip_html("<p>FRFJ&amp;GH</p>") == "FRFJ&GH" and e.strip_html("<p>&nbsp;</p>") is None


def test_phone_restores_leading_zero():
    assert e.phone("715817687") == "0715817687" and e.phone("0707758660") == "0707758660"
    assert e.phone("0") is None and e.phone("") is None


def test_names():
    assert e.split_name("Deborah ", "Wambui", "Kamande\t") == ("Deborah", "Wambui", "Kamande")
    assert e.split_name("Susan", "Akinyi", "") == ("Susan", None, "Akinyi")
    assert e.split_name("", "", "Otieno") == ("Otieno", None, "Otieno")
    assert e.person_name("Dr Peter Wanyama Situma") == ("Dr", "Peter", "Wanyama", "Situma")
    assert e.person_name("dr.roy") == ("Dr", "roy", None, "roy")


def test_patient_payload():
    r = {"customercode": "ENH-095451", "customername": "Wambui", "customermiddlename": "Ochieng", "customerlastname": "Amber",
         "gender": "Female", "birth_date": "10/31/2023", "registrationdate": "3/14/2024", "registrationtime": "11:23:32",
         "mobilenumber": "715817687", "maritalstatus": "Select Marital Status", "maintype": "Insurance",
         "subtype": "Nhif Insuarnce", "accountname": "NHIF NATIONAL SCHEME", "memberno": "M1", "kinname": "ochieng",
         "kincontactnumber": "729977436", "kinrelation": "Father", "updatedtime": "2024-03-14 11:26:43"}
    p = e.patient_payload(r, "Main")
    assert p["patient_no"] == "ENH-095451" and p["uuid"] == e.gid("patient", "ENH-095451")
    assert (p["first_name"], p["middle_name"], p["last_name"]) == ("Wambui", "Ochieng", "Amber")
    assert p["dob"] == "2023-10-31" and p["gender"] == "female" and p["mobile"] == "0715817687"
    assert p["marital_status"] is None and p["insurance_status"] == "insured" and p["insurance_policy_no"] == "M1"
    assert p["registration_date"] == "2024-03-14" and p["created_at"] == "2024-03-14 11:23:32"
    assert "Branch: Main" in p["notes"] and p["emergency_contact_phone"] == "0729977436"


def test_visit_payload():
    r = {"visitcode": "ENH/OP/000027/2024", "consultationdate": "2024-03-03", "consultationtime": "03:46:41",
         "paymenttypename": "Insurance", "department": "General OPD", "consultationtype": "consultation fee",
         "patientfirstname": "Ramesh", "patientmiddlename": "M", "patientlastname": "Murugwe"}
    p = e.visit_payload(r, 5, inpatient=False, clinic=14, doctor=9)
    assert p["unique_id"] == "ENH/OP/000027/2024" and p["patient"] == 5 and p["clinic"] == 14 and p["user"] == 9
    assert p["payment_mode"] == "insurance" and p["created_at"] == "2024-03-03 03:46:41" and p["inpatient"] == 0


def test_vitals_payload():
    v = e.vitals_payload({"vitalid": 1, "height": "172", "weight": "67", "bpsystolic": "120", "bpdiastolic": "80",
                          "celsius": "36.6", "spo2": "98", "dm": "Checked", "hypertension": "on", "bmi": "22.647376",
                          "vitaldate": "2024-01-20", "vitaltime": "12:06:24"}, 10, 20)
    assert v["blood_pressure"] == "120/80" and v["oxygen_saturation"] == 98 and v["bmi"] == 22.65
    assert v["chronic_illnesses"] == "Diabetes, Hypertension" and v["recorded_at"] == "2024-01-20 12:06:24"


def test_consultation_note_merges_history():
    h = {"id": 1, "dm": 1, "hypertension": 0, "complaints": "<p>cough</p>", "drug_allergy": "PCN", "para": "2", "gravida": "3"}
    n = e.consultation_note_payload({"consultationid": 2, "consultation": "<p>DRY COUGH</p>", "diagnosis": "Cellulitis",
                                     "code": "L0390", "consultingdoctor": "dr.roy", "timestamp": "2024-03-14 14:29:06"}, 1, 2, h)
    assert n["presenting_complaints"] == "DRY COUGH" and n["diagnosis"] == "Cellulitis [L0390]"
    assert n["past_medical_history"] == "Diabetes" and n["allergy_history"] == "Drug: PCN" and n["chief_complaints"] == "cough"
    assert "Para: 2" in n["remarks"] and n["created_at"] == "2024-03-14 14:29:06"


def test_lab_line_and_flag():
    r = {"parameter": "HB", "value": "9.1", "units": "g/dl", "low": "11.5", "high": "16.5"}
    assert e.lab_line(r) == "HB: 9.1 g/dl (11.5 - 16.5)" and e.lab_flag(r) == "L"
    assert e.lab_flag({"value": "negative", "low": "0", "high": "0"}) is None
    assert e.lab_line({"testname": "VDRL Test", "value": "negative", "low": "0", "high": "0"}) == "VDRL Test: negative"


def test_pay_mode():
    assert [e.pay_mode(x) for x in ("MPESA", "M-PESA", "CASH", "Visa Card", "CHEQUE", "Bank Transfer", "NHIF", "x")] == \
        ["mpesa", "mpesa", "cash", "card", "cheque", "bank_transfer", "insurance", "other"]


def test_evaluation_payment_payload_puts_amount_in_mode_column():
    p = e.evaluation_payment_payload("u", "BILL-NO-1", amount=1500, mode_text="MPESA", when="2024-05-18 17:30:39",
                                     patient=(1, "A B", "ENH-1"), visit_id=3, cashier=4, cashier_name="tessy", reference="QX1")
    assert p["mpesa_amount"] == 1500 and p["cash_amount"] == 0 and p["receipt_date"] == "2024-05-18" and p["patient_no"] == "ENH-1"
    assert e.evaluation_payment_payload("u", "r", amount=None, mode_text="CASH", when="2024-01-01", patient=None,
                                        visit_id=None, cashier=1, cashier_name=None) is None


def test_prescription_payload():
    r = {"pharmbillid": 2, "itemname": "ABZ SUSPENSION 10ML", "dose": "1", "dosemeasur": "Amp", "freq": "bd", "days": "3",
         "quantity": 3, "route": "Oral", "rate": "304.8", "issued": "YES", "paymentstatus": "InvoiceRaised",
         "timeupdated": "2024-03-04 12:32:15"}
    p = e.prescription_payload(r, 7, 99, (5, "Pharmacy"), None, 3)
    assert p["status"] == "active" and p["dispensed"] == 1 and p["dispensed_quantity"] == 3 and p["drug"] == 99
    assert p["frequency"] == "twice a day (BD)" and p["dosage"] == "1 Amp" and p["duration"] == 3 and p["invoiced"] == 1
