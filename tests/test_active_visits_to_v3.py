"""active_visits_to_v3: which V2 visits count as active, and which rows of a table belong to them."""
from __future__ import annotations

import pytest

from active_visits_to_v3 import is_active, keys_for, row_filter

CUT = "2026-10-20 15:00:00"
KW = {"cutoff": CUT, "open_admission": {"1"}, "open_destination": {"2", "3"}}


def test_open_admission_is_active_whatever_its_age():
    old_inpatient = {"id": 1, "created_at": "2025-04-11 16:04:17"}
    assert is_active(old_inpatient, "open_admission", **KW)
    assert is_active(old_inpatient, "default", **KW)
    assert not is_active(old_inpatient, "recent", **KW)


def test_recent_rules():
    seen_today = {"id": 2, "created_at": "2026-10-21 09:00:00"}
    left_today = {"id": 4, "created_at": "2026-10-21T10:00:00", "status": "billed"}      # all destinations checked out
    last_week = {"id": 3, "created_at": "2026-10-14 09:00:00"}
    assert is_active(seen_today, "recent_open_destination", **KW) and is_active(seen_today, "default", **KW)
    assert is_active(left_today, "recent", **KW)
    assert not is_active(left_today, "recent_open_destination", **KW) and not is_active(left_today, "default", **KW)
    assert not is_active(left_today, "recent_unbilled", **KW) and is_active(seen_today, "recent_unbilled", **KW)
    assert not is_active(last_week, "default", **KW)          # an open destination, but too old


def test_deleted_visit_never_active_and_unknown_rule_fails():
    assert not is_active({"id": 1, "created_at": "2026-10-21", "deleted_at": "2026-10-21"}, "default", **KW)
    with pytest.raises(ValueError):
        is_active({"id": 1}, "closed", **KW)


def test_keys_follow_visits_to_patients_schemes_and_insurers():
    visits = [{"id": 10, "patient": 7, "scheme": 100}, {"id": 11, "patient": 8, "scheme": None}]
    patient_schemes = [{"patient": 8, "scheme": 200}, {"patient": 99, "scheme": 300}]
    schemes = {"100": {"company": 5}, "200": {"company": 6}, "300": {"company": 7}}
    k = keys_for(visits, patient_schemes, schemes)
    assert k == {"visit": {"10", "11"}, "patient": {"7", "8"}, "scheme": {"100", "200"}, "insurance": {"5", "6"}}


def test_row_filter_matches_any_link_column_as_text():
    keep = row_filter({"10"}, ("visit", "visit_id"))
    assert keep({"visit": 10}) and keep({"visit_id": "10"}) and keep({"VISIT": 10.0})
    assert not keep({"visit": 11}) and not keep({"visit_id": None}) and not keep({})


def test_receipts_found_through_details_invoices_or_the_patient_after_the_visit():
    from active_visits_to_v3 import receipt_ids
    visits = [{"id": 10, "patient": 7, "created_at": "2026-10-21 09:00:00"}]
    details = [{"visit": 10, "payment": 501}, {"visit": 99, "payment": 502}]
    receipts = [
        {"id": 503, "invoice_id": 900, "patient": 1, "created_at": "2025-01-01"},     # pays the visit's invoice
        {"id": 504, "invoice_id": None, "patient": 7, "created_at": "2026-10-21 10:00:00"},   # patient, after the visit
        {"id": 505, "invoice_id": None, "patient": 7, "created_at": "2026-10-01 10:00:00"},   # patient, before it
        {"id": 506, "invoice_id": 901, "patient": 7, "created_at": "2026-10-21 11:00:00"},    # another invoice
        {"id": 507, "invoice_id": None, "patient": 7, "created_at": "2026-10-21 12:00:00", "deleted_at": "x"},
    ]
    assert receipt_ids(visits, details, {"900"}, receipts) == {"501", "503", "504"}
