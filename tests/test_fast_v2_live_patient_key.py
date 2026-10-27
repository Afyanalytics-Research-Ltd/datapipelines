"""fast_v2_live_to_v3.CONTENT_KEY["patient"]: a V2 patient must still match its org 284 row after the
2026-10-26 renumbering (kisumu_v3-[orgN-]<no>-<id> → PAT<no>), or v2_delta_to_v3 inserts duplicates."""
import pytest

fl = pytest.importorskip("fast_v2_live_to_v3")
key = fl.CONTENT_KEY["patient"]


@pytest.mark.parametrize("value, expected", [
    ("kisumu_v3-13634-245664", "13634"),
    ("kisumu_v3-org4-13634-245664", "13634"),
    ("kisumu_v3-org284-59670-285647", "59670"),
    ("afya_api_auth-org7-47193-12", "47193"),
    ("PAT13634", "13634"),
    ("pat13634", "13634"),
    ("  PAT13634  ", "13634"),
    ("PAT1", "1"),
    ("PAT12345678", "12345678"),            # 8 digits: still a V2 number
])
def test_v2_number_extracted(value, expected):
    assert fl._patient_no_key(value) == expected


@pytest.mark.parametrize("value", [
    "PAT2026290695",                        # V3's own number (year + 6 digits)
    "PAT2026058690",
    "PAT13634-DUP",                         # the extra copy of a duplicated patient
    "PAT13634-DUP2",
    "PAT2026-9",
    "13634",                                # a bare number is not a migration format
    "kisumu_v3-13634",                      # no V2 id part
])
def test_other_formats_only_match_themselves(value):
    assert fl._patient_no_key(value) == " ".join(value.split()).lower()


def test_empty_values():
    assert fl._patient_no_key(None) == ""
    assert fl._patient_no_key("") == ""


def test_migration_payload_matches_renumbered_row():
    payload = {"patient_no": "kisumu_v3-org284-59670-285647"}
    assert key(payload) == key({"patient_no": "PAT59670"})
    assert key(payload) == key({"patient_no": "kisumu_v3-59670-285647"})


def test_payload_never_matches_dup_copy_or_v3_number():
    payload = {"patient_no": "kisumu_v3-org284-13634-245664"}
    assert key(payload) != key({"patient_no": "PAT13634-DUP"})
    assert key({"patient_no": "kisumu_v3-2026290695-1"}) != key({"patient_no": "PAT2026290695"})


def test_different_patients_stay_different():
    assert key({"patient_no": "PAT13634"}) != key({"patient_no": "PAT13635"})
    assert key({"patient_no": "kisumu_v3-1-10"}) != key({"patient_no": "PAT10"})
