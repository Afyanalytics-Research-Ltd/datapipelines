"""dedup.py matching rules on hand-made records shaped like different hospitals' data. No Snowflake."""
from __future__ import annotations

import random

import pytest

import dedup as d
import dedup_rules as dr


def _patient(id, first="", middle="", last="", dob="", gender="female", mobile="", reg="2025-01-01 09:00:00", **kw):
    return {"id": str(id), "first_name": first, "middle_name": middle, "last_name": last, "dob": dob,
            "gender": gender, "mobile": mobile, "registration_date": reg, **kw}


def _run_patients(rows, **kw):
    cols = set().union(*(r.keys() for r in rows))
    return d.dedupe_table("patients", rows, cols, dr.rules_for("patients"), **kw)


def _merged(out) -> set[frozenset]:
    return {frozenset((l, s)) for l, s, _, _ in out.crosswalk if l != s}


def _reviewed(out) -> set[frozenset]:
    return {frozenset((a, b)) for a, b, _, _ in out.review}


# ─── normalisation ────────────────────────────────────────────────────────

def test_phone_formats_are_one_number_and_junk_is_dropped():
    assert d.norm_phone("+254 712 345 678") == d.norm_phone("0712345678") == d.norm_phone("712-345-678")
    assert d.norm_phone("07") is None and d.norm_phone("0700000000") is None


def test_name_words_ignore_order_case_titles_and_punctuation():
    assert d.name_tokens("Mrs. WANJIRU,", "mary") == d.name_tokens("Mary", "Wanjiru")


def test_names_relation():
    assert d.names_relation(("mary", "wanjiru"), ("mary", "wanjiru")) == "equal"
    assert d.names_relation(("mary", "wanjiru"), ("kamau", "mary", "wanjiru")) == "compatible"
    assert d.names_relation(("mary", "wanjiru"), ("mary", "wanjru")) == "compatible"       # typo
    assert d.names_relation(("faith", "wanjiru"), ("hope", "wanjiru")) is None              # twins
    assert d.names_relation(("mary",), ("mary",)) is None                                   # one word isn't a name


def test_dates_and_gender():
    assert d.norm_dob("0000-00-00") is None and d.norm_dob("1890-05-05") is None
    assert d.norm_dob("1990-05-17 00:00:00").isoformat() == "1990-05-17"
    assert d.norm_gender("F") == d.norm_gender("Female") == "f"


# ─── patients ─────────────────────────────────────────────────────────────

def test_double_registration_minutes_apart_merges():
    out = _run_patients([_patient(1, "Alfred", "Munngai", "Thumbi", reg="2026-08-03 16:32:30"),
                         _patient(2, "alfred", "munngai", "thumbi", reg="2026-08-03 16:32:31")])
    assert _merged(out) == {frozenset(("2", "1"))}
    assert out.crosswalk == [("2", "1", "entry", 0)]


def test_same_name_and_exact_dob_merges_whatever_the_name_order():
    out = _run_patients([_patient(1, "Mary", "", "Wanjiru", dob="1990-05-17", reg="2024-01-01"),
                         _patient(2, "Wanjiru", "", "Mary", dob="1990-05-17", reg="2025-06-01")])
    assert _merged(out) == {frozenset(("1", "2"))}


def test_typo_with_same_dob_goes_to_review_not_merge():
    out = _run_patients([_patient(1, "Mary", "", "Wanjiru", dob="1990-05-17", reg="2024-01-01"),
                         _patient(2, "Mary", "", "Wanjru", dob="1990-05-17", reg="2025-06-01")])
    assert not _merged(out) and _reviewed(out) == {frozenset(("1", "2"))}


@pytest.mark.parametrize("b", [
    _patient(2, "Mary", "", "Wanjiru", dob="1985-02-02", reg="2025-06-01"),                 # other dob
    _patient(2, "Mary", "", "Wanjiru", dob="1990-05-17", gender="male", reg="2025-06-01"),  # other gender
])
def test_conflicting_evidence_blocks_any_match(b):
    out = _run_patients([_patient(1, "Mary", "", "Wanjiru", dob="1990-05-17", reg="2024-01-01"), b])
    assert not _merged(out) and not _reviewed(out)


def test_twins_and_families_sharing_a_phone_are_not_matched():
    rows = [_patient(1, "Faith", "", "Wanjiru", dob="2015-03-03", mobile="0712345678"),
            _patient(2, "Hope", "", "Wanjiru", dob="2015-03-03", mobile="0712345678"),
            _patient(3, "Peter", "", "Kamau", dob="1980-01-09", gender="male", mobile="0712345678")]
    out = _run_patients(rows)
    assert not _merged(out) and not _reviewed(out)


def test_common_name_without_other_evidence_is_left_alone():
    rows = [_patient(i, "Mary", "", "Wanjiku", reg=f"202{i % 5}-0{1 + i % 8}-01") for i in range(1, 8)]
    out = _run_patients(rows)
    assert not _merged(out) and not _reviewed(out)


def test_rare_name_and_phone_without_dob_merges():
    out = _run_patients([_patient(1, "Ann", "Muthoni", "Ngai", mobile="0783069639", reg="2025-01-01"),
                         _patient(2, "Ann", "Muthoni", "Ngai", mobile="+254783069639", reg="2025-09-06")])
    assert _merged(out) == {frozenset(("1", "2"))}


def test_common_name_and_phone_without_dob_is_review():
    rows = [_patient(1, "Ann", "Muthoni", "Ngai", mobile="0783069639", reg="2025-01-01"),
            _patient(2, "Ann", "Muthoni", "Ngai", mobile="+254783069639", reg="2025-09-06"),
            _patient(3, "Ann", "Muthoni", "Ngai", mobile="0722000111", reg="2025-03-03")]
    out = _run_patients(rows)
    assert _reviewed(out) == {frozenset(("1", "2"))} and not _merged(out)


def test_near_name_with_exact_dob_and_phone_merges():
    out = _run_patients([_patient(1, "Mary", "Wangeci", "Mathenge", mobile="0721329525", dob="1984-03-12", reg="2024-01-01"),
                         _patient(2, "Mary", "Wagneci", "Mathenge", mobile="0721 329 525", dob="1984-03-12", reg="2024-06-01")])
    assert _merged(out) == {frozenset(("1", "2"))}


def test_family_on_one_phone_is_not_merged():
    out = _run_patients([_patient(1, "Mary", "Wanjiru", "Kamau", mobile="0721329525", dob="1970-03-12", reg="2024-01-01"),
                         _patient(2, "", "Wanjiru", "Kamau", mobile="0721329525", reg="2024-06-01")])
    assert not _merged(out)


def test_burst_is_timed_from_the_entry_before():
    rows = [{"id": str(i), "patient_id": "9", "department_id": "2", "created_at": f"2026-01-01 10:0{m}:00"}
            for i, m in ((1, 0), (2, 1), (3, 3))]
    out = d.dedupe_table("visits", rows, set(rows[0]), dr.rules_for("visits"))
    assert set(out.survivor_of) == {"2", "3"}


def test_blank_optional_link_still_matches():
    rows = [{"id": str(i), "patient_id": "9", "amount": "500", "payment_type": "cash", "payment_date": t}
            for i, t in ((1, "2026-01-01 10:00:00"), (2, "2026-01-01 10:00:30"))]
    out = d.dedupe_table("payments", rows, set(rows[0]) | {"invoice_id"}, dr.rules_for("payments"))
    assert out.survivor_of == {"2": "1"}


def test_identical_note_text_merges_and_repeated_line_is_not_reviewed():
    note = "cough for 3 days, no fever, chest clear"
    rows = [{"id": "1", "visit_id": "3", "chief_complaints": note}, {"id": "2", "visit_id": "3", "chief_complaints": note}]
    out = d.dedupe_table("doctor_notes", rows, set(rows[0]), dr.rules_for("doctor_notes"))
    assert out.survivor_of == {"2": "1"}
    rows = [{"id": str(i), "invoice_id": "7", "item_name": "Paracetamol", "unit_price": "10", "created_at": t}
            for i, t in ((1, "2026-01-01 10:00:00"), (2, "2026-01-01 10:06:00"))]
    out = d.dedupe_table("invoice_items", rows, set(rows[0]), dr.rules_for("invoice_items"))
    assert not out.survivor_of and not out.review


def test_strong_id_merges_when_names_fit_and_blocks_when_ids_differ():
    rows = [_patient(1, "John", "", "Otieno", national_id="12345678", reg="2024-01-01"),
            _patient(2, "John", "Ochieng", "Otieno", national_id="12345678", reg="2025-01-01"),
            _patient(3, "John", "", "Otieno", national_id="87654321", reg="2024-01-01 09:01:00")]
    out = _run_patients(rows)
    assert _merged(out) == {frozenset(("1", "2"))}
    assert "3" not in {i for p in _merged(out) for i in p}      # different id number: never the same person


def test_id_number_shared_by_many_names_is_a_placeholder():
    rows = [_patient(i, f"Name{chr(97 + i)}x", "", f"Other{chr(97 + i)}y", national_id="99999999".replace("9", "1") + "23")
            for i in range(5)]
    out = _run_patients(rows)
    assert not _merged(out) and not _reviewed(out)


def test_walk_in_placeholder_records_are_never_merged():
    rows = [_patient(1, "Walk", "", "In", reg="2025-01-01 09:00:00"),
            _patient(2, "walk", "", "in", reg="2025-01-01 09:00:05")]
    assert not _merged(_run_patients(rows))


def test_estimated_dob_only_counts_for_the_year():
    rows = [_patient(1, "Agnes", "", "Wanjiku", dob="1973-01-01", reg="2024-01-01"),
            _patient(2, "Agnes", "", "Wanjiku", dob="1973-01-01", reg="2025-06-01")]
    out = _run_patients(rows)
    assert not _merged(out) and _reviewed(out) == {frozenset(("1", "2"))}


def test_full_name_only_shape_works():
    rows = [{"id": "10", "name": "Grace Achieng Odhiambo", "sex": "F", "date_of_birth": "1992-07-04"},
            {"id": "11", "name": "ODHIAMBO Grace Achieng", "sex": "female", "date_of_birth": "1992-07-04"}]
    out = _run_patients(rows)
    assert _merged(out) == {frozenset(("10", "11"))}


def test_chain_cannot_join_records_that_conflict():
    rows = [_patient(1, "Mary", "", "Wanjiru", dob="1990-05-17", reg="2025-01-01 09:00:00"),
            _patient(2, "Mary", "", "Wanjiru", dob="", reg="2025-01-01 09:01:00"),
            _patient(3, "Mary", "", "Wanjiru", dob="1985-02-02", reg="2025-01-01 09:02:00")]
    out = _run_patients(rows)
    clusters = {}
    for l, s, _, _ in out.crosswalk:
        clusters.setdefault(s, {s}).add(l)
    assert not any({"1", "3"} <= c for c in clusters.values())


def test_reviewer_decisions_are_applied():
    rows = [_patient(1, "Mary", "", "Wanjiru", dob="1990-05-17", reg="2024-01-01"),
            _patient(2, "Mary", "", "Wanjru", dob="1990-05-17", reg="2025-06-01"),
            _patient(3, "Peter", "", "Kamau", dob="1990-05-17", gender="male", reg="2024-01-01"),
            _patient(4, "Peter", "", "Kamau", dob="1990-05-17", gender="male", reg="2024-01-02")]
    decisions = {("patients", frozenset(("1", "2"))): "same", ("patients", frozenset(("3", "4"))): "different"}
    out = _run_patients(rows, decisions=decisions)
    assert _merged(out) == {frozenset(("1", "2"))} and not _reviewed(out)


def test_survivor_is_the_earliest_and_result_is_order_independent():
    rows = [_patient(i, "Mary", "", "Wanjiru", dob="1990-05-17", reg=f"2025-0{i}-01") for i in range(1, 6)]
    first = _run_patients(rows)
    shuffled = rows[:]
    random.Random(4).shuffle(shuffled)
    assert _run_patients(shuffled).crosswalk == first.crosswalk
    assert {s for _, s, _, _ in first.crosswalk} == {"1"}


# ─── events and lines ─────────────────────────────────────────────────────

def _visit(id, patient, at, dept="1", **kw):
    return {"id": str(id), "patient_id": str(patient), "department_id": dept, "visit_date": at, **kw}


def test_visit_double_clicks_merge_and_real_repeat_visits_stay():
    rows = [_visit(1, 7, "2026-01-15 09:00:00"), _visit(2, 7, "2026-01-15 09:00:40"),
            _visit(3, 7, "2026-01-15 09:01:30"),                     # still within 120 s of the first
            _visit(4, 7, "2026-01-15 14:00:00"),                     # afternoon visit: real
            _visit(5, 7, "2026-01-15 09:00:10", dept="2")]           # another department: real
    out = d.dedupe_table("visits", rows, set(rows[0]), dr.rules_for("visits"))
    assert {l for l, s, _, _ in out.crosswalk} == {"2", "3"}
    assert all(s == "1" for _, s, _, _ in out.crosswalk)


def test_visit_minutes_apart_is_review():
    rows = [_visit(1, 7, "2026-01-15 09:00:00"), _visit(2, 7, "2026-01-15 09:08:00")]
    out = d.dedupe_table("visits", rows, set(rows[0]), dr.rules_for("visits"))
    assert not out.crosswalk and _reviewed(out) == {frozenset(("1", "2"))}


def test_stuck_submit_collapses_and_same_drug_hours_later_stays():
    rows = [{"id": str(i), "visit_id": "9549", "drug_name": "Zycet 10mg OD", "created_at": f"2025-12-06 10:18:{i % 60:02d}"}
            for i in range(170)]                                       # more lines than any placeholder threshold
    rows += [{"id": str(1000 + i), "visit_id": str(i), "drug_name": "Amoxil", "created_at": "2025-12-06 09:00:00"}
             for i in range(200)]
    rows.append({"id": "9999", "visit_id": "9549", "drug_name": "Zycet 10mg OD", "created_at": "2025-12-06 15:00:00"})
    out = d.dedupe_table("prescriptions", rows, set(rows[0]), dr.rules_for("prescriptions"))
    assert out.merged_away == 169 and "9999" not in out.survivor_of


def test_lines_without_time_are_never_merged_automatically():
    rows = [{"id": "1", "invoice_id": "5", "item_name": "Paracetamol", "unit_price": "10"},
            {"id": "2", "invoice_id": "5", "item_name": "Paracetamol", "unit_price": "10"}]
    out = d.dedupe_table("invoice_items", rows, set(rows[0]), dr.rules_for("invoice_items"))
    assert not out.survivor_of and _reviewed(out) == {frozenset(("1", "2"))}


def test_children_of_duplicate_visits_are_compared_on_the_survivor():
    tables = {
        "patients": [_patient(1, "Mary", "", "Wanjiru", dob="1990-05-17")],
        "visits": [_visit(10, 1, "2026-01-15 09:00:00"), _visit(11, 1, "2026-01-15 09:00:30")],
        "prescriptions": [{"id": "100", "visit_id": "10", "drug_name": "Amoxil", "created_at": "2026-01-15 09:05:00"},
                          {"id": "101", "visit_id": "11", "drug_name": "Amoxil", "created_at": "2026-01-15 09:05:20"}],
    }
    cols = {t: set().union(*(r.keys() for r in rows)) for t, rows in tables.items()}
    assert d.table_order(cols) == ["patients", "visits", "prescriptions"]
    res = d.run_engine(tables, cols)
    assert res["visits"].survivor_of == {"11": "10"}
    assert res["prescriptions"].survivor_of == {"101": "100"}


def test_lines_on_each_copy_of_a_merged_parent_merge_but_lines_on_one_parent_stay():
    tables = {
        "invoices": [{"id": "1", "patient_id": "7", "invoice_date": "2026-01-15 09:00:00"},
                     {"id": "2", "patient_id": "7", "invoice_date": "2026-01-15 09:00:20"}],
        "invoice_items": [{"id": "10", "invoice_id": "1", "item_name": "Paracetamol", "unit_price": "10"},
                          {"id": "11", "invoice_id": "1", "item_name": "Paracetamol", "unit_price": "10"},
                          {"id": "12", "invoice_id": "2", "item_name": "Paracetamol", "unit_price": "10"}],
    }
    cols = {t: set().union(*(r.keys() for r in rows)) for t, rows in tables.items()}
    res = d.run_engine(tables, cols)
    assert res["invoices"].survivor_of == {"2": "1"}
    items = res["invoice_items"]
    assert items.survivor_of == {"12": "10"}                   # the copy on the merged invoice
    assert _reviewed(items) == {frozenset(("10", "11"))}       # two units on one invoice: maybe real


def test_rule_missing_what_was_entered_never_merges():
    rows = [{"id": "1", "visit_id": "3", "order_date": "2026-01-01 10:00:00", "result": "neg"},   # no test name
            {"id": "2", "visit_id": "3", "order_date": "2026-01-01 10:00:05", "result": "neg"}]
    out = d.dedupe_table("investigations", rows, set(rows[0]), dr.rules_for("investigations"))
    assert not out.survivor_of and _reviewed(out) == {frozenset(("1", "2"))}


def test_retaken_vitals_are_neither_merged_nor_reviewed():
    rows = [{"id": "1", "visit_id": "3", "recorded_at": "2026-01-01 10:00:00", "weight": "60", "temperature": "36.5"},
            {"id": "2", "visit_id": "3", "recorded_at": "2026-01-01 10:00:30", "weight": "60", "temperature": "38.9"}]
    out = d.dedupe_table("vitals", rows, set(rows[0]), dr.rules_for("vitals"))
    assert not out.survivor_of and not out.review


def test_entries_under_a_shared_placeholder_parent_are_not_matched():
    rows = [_visit(i, 1 + i % 150, f"2026-01-01 08:{i % 60:02d}:00") for i in range(300)]   # 150 people, 2 visits each
    otc = [_visit(1000 + i, 3004, f"2026-01-02 09:00:{i % 60:02d}") for i in range(400)]     # one "PHARMACY -OTC"
    rows += otc + [_visit(5000, 9, "2026-01-03 10:00:00"), _visit(5001, 9, "2026-01-03 10:00:30")]
    out = d.dedupe_table("visits", rows, set(rows[0]), dr.rules_for("visits"))
    assert out.survivor_of.get("5001") == "5000"
    assert not any(int(l) >= 1000 and int(l) < 5000 for l in out.survivor_of)


def test_pharmacy_otc_patient_is_a_placeholder():
    rows = [_patient(1, "PHARMACY", "-OTC", ""), _patient(2, "Pharmacy", "OTC", "", reg="2025-01-01 09:00:30")]
    assert not _merged(_run_patients(rows))


def test_prescription_dose_changed_a_minute_later_migrates_as_is():
    rows = [{"id": "1", "visit_id": "3", "drug_name": "Amoxil", "dose": "250mg", "created_at": "2026-01-01 10:00:00"},
            {"id": "2", "visit_id": "3", "drug_name": "Amoxil", "dose": "500mg", "created_at": "2026-01-01 10:01:00"}]
    out = d.dedupe_table("prescriptions", rows, set(rows[0]), dr.rules_for("prescriptions"))
    assert not out.survivor_of and not out.review


def test_date_only_times_are_not_read_as_seconds_apart():
    rows = [{"id": str(i), "visit_id": "3", "drug_name": "Amoxil", "prescription_date": "2026-01-01"} for i in (1, 2)]
    out = d.dedupe_table("prescriptions", rows, set(rows[0]), dr.rules_for("prescriptions"))
    assert not out.survivor_of and _reviewed(out) == {frozenset(("1", "2"))}


def test_without_what_was_entered_only_identical_content_is_reviewed():
    rows = [{"id": "1", "visit_id": "3", "order_date": "2026-01-01 10:00:00", "result": "positive"},
            {"id": "2", "visit_id": "3", "order_date": "2026-01-01 10:00:05", "result": "positive"},
            {"id": "3", "visit_id": "3", "order_date": "2026-01-01 10:00:09"}]
    out = d.dedupe_table("investigations", rows, set(rows[0]), dr.rules_for("investigations"))
    assert not out.survivor_of and _reviewed(out) == {frozenset(("1", "2"))}


def test_shared_employee_number_with_other_names_and_emails_is_not_a_match():
    rows = [{"id": "12", "name": "Doctor Wema", "email": "doctor@x.org", "employee_number": "13"},
            {"id": "13", "name": "Nurse Wema", "email": "nurse@x.org", "employee_number": "13"}]
    out = d.dedupe_table("users", rows, set(rows[0]), dr.rules_for("users"))
    assert not out.survivor_of and not out.review


def test_catalogue_without_price_column_is_review_only():
    rows = [{"id": "1", "name": "HBA1C", "departmentcode": "Lab"}, {"id": "2", "name": "hba1c", "departmentcode": "Lab"}]
    out = d.dedupe_table("procedures", rows, set(rows[0]), dr.rules_for("procedures"))
    assert not out.survivor_of and _reviewed(out) == {frozenset(("1", "2"))}


# ─── catalogue, copies, overrides ─────────────────────────────────────────

def test_catalogue_names_merge_unless_strength_differs_and_price_sends_to_review():
    rows = [{"id": "1", "name": "H. Pylori Ag", "category": "lab", "price": "500"},
            {"id": "2", "name": "h pylori ag", "category": "lab", "price": "500"},
            {"id": "3", "name": "HBA1C", "category": "lab", "price": "1500"},
            {"id": "4", "name": "hba1c", "category": "lab", "price": "1800"}]
    out = d.dedupe_table("procedures", rows, set(rows[0]), dr.rules_for("procedures"))
    assert out.survivor_of == {"2": "1"} and _reviewed(out) == {frozenset(("3", "4"))}
    drugs = [{"id": "1", "name": "Amoxil", "strength": "250mg"}, {"id": "2", "name": "Amoxil", "strength": "500mg"}]
    assert not d.dedupe_table("inv_products", drugs, set(drugs[0]), dr.rules_for("inv_products")).survivor_of


def test_load_and_version_copies():
    recs = [{"id": 1, "name": "A", "updated_at": "2025-01-01"}, {"id": 1, "name": "A", "updated_at": "2025-01-01"},
            {"id": 2, "name": "B", "updated_at": "2025-01-01"}, {"id": 2, "name": "B2", "updated_at": "2025-03-01"}]
    out = d.dedupe_records("some_unruled_table", recs)
    assert sorted(out.crosswalk) == [("1", "1", "load", 1), ("2", "2", "version", 1)]
    assert {r["name"] for r in out.kept} == {"A", "B2"}
    assert out.rows_in == 4 and out.rows_out == 2


def test_mappings_override_changes_or_disables_a_rule():
    over = {"patients": {"burst_seconds": 5, "strong_ids": ["file_no"]}, "invoice_items": {"disabled": True}}
    assert dr.rules_for("patients", over)["burst_seconds"] == 5
    assert dr.rules_for("patients", over)["strong_ids"] == ("file_no",)
    assert dr.rules_for("invoice_items", over) is None
    assert dr.rules_for("patients")["burst_seconds"] == 600          # defaults untouched


def test_missing_fields_degrade_instead_of_failing():
    rows = [{"id": "1", "last_name": "Kamau"}, {"id": "2", "last_name": "Kamau"}]   # no first name, dob, phone …
    out = _run_patients(rows)
    assert not out.crosswalk
    assert not d.dedupe_table("visits", [{"id": "1"}], {"id"}, dr.rules_for("visits")).crosswalk


class _Cursor:
    """Answers the CROSSWALK existence check, then the facility's row count."""
    def __init__(self, *counts):
        self.counts = list(counts)

    def execute(self, *a):
        self.current = self.counts.pop(0)

    def fetchone(self):
        return (self.current,)


COLS = [("ID", "NUMBER"), ("PATIENT_ID", "NUMBER"), ("DEPARTMENT_ID", "STRING"), ("META", None), ("NAME", "STRING")]


def test_crosswalk_select_drops_losers_and_moves_links():
    sql = " ".join(d.crosswalk_select(_Cursor(1, 5), "wema_medical", "visits", "SELECT 1", COLS).split())
    assert sql.startswith('SELECT f.SOURCE_SCHEMA, f."ID", COALESCE(TRY_CAST(l0.SURVIVOR_ID AS NUMBER), '
                          'f."PATIENT_ID") AS "PATIENT_ID", COALESCE(TRY_CAST(l1.SURVIVOR_ID AS STRING), '
                          'f."DEPARTMENT_ID") AS "DEPARTMENT_ID", f."META", f."NAME" FROM (SELECT 1) f')
    assert "FACILITY = 'WEMA_MEDICAL'" in sql and "V3_TABLE IN ('patients')" in sql
    assert "V3_TABLE IN ('core_departments', 'departments')" in sql
    assert "CASE V3_TABLE WHEN 'core_departments' THEN 0 WHEN 'departments' THEN 1 END" in sql
    assert "V3_TABLE IN ('visits')" in sql and 'gone ON gone.LOSER_ID = TO_VARCHAR(f."ID")' in sql
    assert sql.endswith("WHERE gone.LOSER_ID IS NULL")


def test_crosswalk_select_is_the_plain_query_without_a_dedup_run():
    assert d.crosswalk_select(_Cursor(0), "wema_medical", "visits", "SELECT 1", COLS) == "SELECT 1"
    assert d.crosswalk_select(_Cursor(1, 0), "wema_medical", "visits", "SELECT 1", COLS) == "SELECT 1"
    assert d.crosswalk_select(_Cursor(1, 5), "w", "t", "SELECT 1", [("NAME", "STRING")]) == "SELECT 1"


def test_facility_of_strips_raw():
    assert d.facility_of("HOSPITALS.WEMA_MEDICAL_RAW") == "WEMA_MEDICAL"


def test_raw_tables_takes_rule_tables_and_reuses_the_flattens_fields(monkeypatch):
    import flatten_jsons_schemas as fl
    discovered = []
    monkeypatch.setattr(fl, "get_source_tables", lambda cur, raw: ["patients", "visits", "audit_log"])
    monkeypatch.setattr(fl, "discover_fields", lambda cur, raw, t: discovered.append(t) or [("id", "INTEGER")])
    monkeypatch.setattr(fl, "expand_objects", lambda cur, raw, t, f: [(n, n, ty) for n, ty in f])
    out = d.raw_tables(None, "R", fields={"patients": [["id", "id", "INTEGER"], ["dob", "dob", "VARCHAR"]]})
    assert set(out) == {"patients", "visits"} and discovered == ["visits"]
    assert out["patients"][1] == {"id": ("ID", "NUMBER"), "dob": ("DOB", "STRING")}
    assert set(d.raw_tables(None, "R", tables=["audit_log"])) == {"audit_log"}
