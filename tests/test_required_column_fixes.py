"""
V3 NOT NULL columns the kisumu_v3 V2 data leaves empty (2026-10-12 run):

  * visit destinations — the CLEAN view carries the destination as an object
    (no destination_name column): the department name comes from its "name",
    else V2's free-text department
  * next of kin — relationship (no V2 row has one) → "Not recorded in V2"
  * ICD-10 categories/subcategories — code from the code_from..code_to range
  * insurance schemes — code from the V2 id; a scheme whose company isn't in
    V3 yet is held back
"""
from __future__ import annotations

import snowflake_to_v3_migration as m
import v2_to_v3_api_migration as v2v3

ORG = {"organization_id": 4, "facility_id": 4, "application_id": 1}


def ns(v2_class):
    return v2v3.NAMESPACE_MAP[v2_class]["transform"]


class TestVisitDestination:
    def tf(self, monkeypatch, rec):
        monkeypatch.setattr(v2v3, "_v3_department_id_by_name", lambda n: {"pharmacy": 7, "nursing": 8}.get(
            str(n or "").lower()))
        return v2v3.transform_record({"id": 1, "visit": 5, "created_at": "2024-01-01", **rec},
                                     "evaluation_visit_destination", ORG, "kisumu_v3")

    def test_name_from_the_destination_object(self, monkeypatch):
        out = self.tf(monkeypatch, {"destination": {"id": 9, "name": "Pharmacy"}, "department": ""})
        assert out["department_name"] == "Pharmacy" and out["department_id"] == 7
        assert "destination" not in out

    def test_falls_back_to_department(self, monkeypatch):
        out = self.tf(monkeypatch, {"destination": None, "department": "Nursing"})
        assert out["department_name"] == "Nursing" and out["department_id"] == 8

    def test_flattened_destination_name_still_wins(self, monkeypatch):
        out = self.tf(monkeypatch, {"destination_name": "Pharmacy", "department": "Nursing"})
        assert out["department_name"] == "Pharmacy"

    def test_no_name_is_held_back(self, monkeypatch):
        assert self.tf(monkeypatch, {"destination": None, "department": ""}) is None


class _Cur:
    def __init__(self, cols, rows):
        self.cols, self.rows, self.sql, self.description = cols, rows, [], None

    def execute(self, sql):
        self.sql.append(sql)
        if "LIMIT 0" in sql:
            self.description = [(c.upper(),) for c in self.cols]
            return self
        self._out = next((v for k, v in self.rows.items() if f"DISTINCT {k}" in sql), [])
        return self

    def fetchall(self):
        return self._out


class _Conn:
    def __init__(self, cur):
        self.cur = cur

    def cursor(self):
        return self.cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_departments_read_from_the_columns_the_view_has(monkeypatch):
    cur = _Cur(["id", "destination", "department"], {
        "TRY_PARSE_JSON": [("Pharmacy",), ("Triage",)],
        "department::STRING": [("Pharmacy",), ("Nursing ",)],
    })
    monkeypatch.setattr(m, "_snowflake_connect", lambda: _Conn(cur))
    monkeypatch.setattr(v2v3, "reset_v3_department_cache", lambda: None)
    monkeypatch.setattr(v2v3, "v3_login_org_cfg", lambda: ORG)
    seen = []   # every name already in V3: nothing posted
    monkeypatch.setattr(v2v3, "_v3_department_id_by_name", lambda n: seen.append(n) or 1)
    m.ensure_departments_from_destinations("kisumu_v3")
    assert sorted(seen) == ["Nursing", "Pharmacy", "Triage"]
    assert not any("destination_name" in s for s in cur.sql)


def test_next_of_kin_relationship_placeholder():
    t = ns(r"Ignite\Reception\Entities\NextOfKin")
    out = v2v3.transform_record({"id": 35317, "patient_id": 233965, "first_name": "dancan",
                                 "relationship": None, "relationship_id": None}, t, ORG, "kisumu_v3")
    assert out["relationship"] == v2v3._NOT_RECORDED
    kept = v2v3.transform_record({"id": 1, "patient_id": 2, "relationship": "Spouse"}, t, ORG, "kisumu_v3")
    assert kept["relationship"] == "Spouse"


def test_icd10_code_from_range():
    for cls in ("Icd10Categories", "Icd10Subcategories"):
        t = ns(rf"Ignite\Evaluation\Entities\{cls}")
        out = v2v3.transform_record({"id": 1, "name": "Neoplasms", "code_from": "C00", "code_to": "D49"},
                                    t, ORG, "kisumu_v3")
        assert out["code"] == "C00-D49"
    single = v2v3.transform_record({"id": 2, "code_from": "A75", "code_to": "A75"}, t, ORG, "kisumu_v3")
    assert single["code"] == "A75"


def test_scheme_code_and_company_is_critical():
    out = v2v3.transform_record({"id": 1155, "name": "2032800", "code": None, "company": 366,
                                 "source_schema": "kisumu_v3"}, "settings_scheme", ORG, "kisumu_v3")
    assert out["code"] == "SCH-kisumu_v3-1155"
    assert out["company_id"] == 366
    assert "company_id" in m._CRITICAL_FK_FIELDS["settings_scheme"]
    real = v2v3.transform_record({"id": 1, "code": "NHIF-01", "company": 366}, "settings_scheme", ORG, "kisumu_v3")
    assert real["code"] == "NHIF-01"
