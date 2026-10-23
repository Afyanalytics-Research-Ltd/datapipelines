"""load_missing_records: CSV parsing, value conversion, outcome classification, posting."""
import load_missing_records as lmr


def test_parse_csv_reads_the_exporters_format():
    text = 'a,b,c\n"x","say ""hi""",NULL\n"multi\nline","",NULL\n'
    assert lmr.parse_csv(text) == [["a", "b", "c"], ["x", 'say "hi"', None], ["multi\nline", "", None]]


def test_to_value():
    assert lmr.to_value(None) is None
    assert lmr.to_value("NULL") is None                 # V2's literal text in an integer column
    assert lmr.to_value('{"a": 1}') == {"a": 1}
    assert lmr.to_value("[1, 2]") == [1, 2]
    assert lmr.to_value("{not json") == "{not json"
    assert lmr.to_value("12.5") == "12.5"


def test_classify_v3_responses():
    assert lmr.classify(201, "") == "inserted"
    assert lmr.classify(422, "insert rejected: Duplicate entry 'ADM-9787' for key 'x'") == "duplicate"
    assert lmr.classify(422, "insert rejected: Cannot add or update a child row: a foreign key constraint fails") == "parent missing"
    assert lmr.classify(422, "insert rejected: Column 'admission_id' cannot be null") == "parent missing"
    assert lmr.classify(422, "insert rejected: Out of range value for column 'bp_diastolic'") == "rejected"


class _Resp:
    def __init__(self, status, text=""):
        self.status_code, self.text, self.ok = status, text, status < 300


class _Client:
    def __init__(self):
        self.sent = []

    def insert(self, svc, alias, rec, match_on=None):
        self.sent.append((rec, match_on))
        if rec["uuid"] == "dup":
            return _Resp(422, "insert rejected: Duplicate entry 'X' for key 'u'")
        if rec["uuid"] == "bad":
            return _Resp(422, "insert rejected: Data too long for column 'postal_code'")
        return _Resp(201)


def test_load_rows_counts_outcomes_and_drops_the_org_column():
    c = _Client()
    counts, misses = lmr.load_rows(c, "finance", "invoice", ["uuid", "organization_id", "amount"],
                                   [["u1", "4", "10"], ["dup", "4", "5"], ["bad", "4", None]], workers=2)
    assert counts == {"inserted": 1, "duplicate": 1, "parent missing": 0, "rejected": 1, "error": 0}
    assert all("organization_id" not in rec and m == "uuid" for rec, m in c.sent)
    assert {m["_outcome"] for m in misses} == {"duplicate", "rejected"}
    assert "postal_code" in lmr.misses_csv(misses)


def test_every_table_has_a_tier():
    assert lmr.tier_of("evaluation_age_groups") == 1 and lmr.tier_of("inp_admissions") == 2
    assert lmr.tier_of("receipts") == 3
