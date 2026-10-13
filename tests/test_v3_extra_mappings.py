"""
v3_extra_mappings.py — the mappings added for whole-schema migrations, and the
guard that every table in a facility's CLEAN schema is accounted for.

  * every KISUMU_CLEAN table (snapshot in fixtures/kisumu_clean_tables.json)
    is migrated, a duplicate of one that is, or not migrated with a reason —
    none UNACCOUNTED. Adding a table to CLEAN without deciding its fate fails
    here (refresh the fixture with the live listing: DAG mode=list_tables).
  * each mapping is internally consistent (links / injections / renames land
    on posted columns, critical links have a parent)
  * apply() only adds — hand-written mappings are never replaced
  * real-shaped rows transform into exactly the V3 payload expected
  * duplicates are skipped by the migration, not just reported
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import snowflake_to_v3_migration as m
import v2_to_v3_api_migration as v2v3
import v3_extra_mappings as xm

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "kisumu_clean_tables.json").read_text())["tables"]
ORG = {"organization_id": 168, "facility_id": 1, "application_id": 1}


def entries_from_fixture():
    """What discover_tables() would return for the snapshot (RAW tables only)."""
    out = []
    for t, x in FIXTURE.items():
        ns = x["namespace"]
        if ns is None:
            continue
        mp = v2v3.NAMESPACE_MAP.get(ns) or {}
        out.append({"table": t, "namespace": ns, "v3": mp.get("v3"), "transform": mp.get("transform")})
    return m.mark_skipped(out)


class TestKisumuCoverage:
    def test_every_clean_table_is_accounted_for(self):
        cov = xm.coverage(entries_from_fixture(), FIXTURE)
        gaps = sorted(t for t, s in cov.items() if s == "UNACCOUNTED")
        assert gaps == [], f"decide these tables' fate in v3_extra_mappings.py: {gaps}"

    def test_counts(self):
        cov = xm.coverage(entries_from_fixture(), FIXTURE)
        kinds = {s.split(":")[0].split(" of ")[0] for s in cov.values()}
        assert kinds == {"migrated", "duplicate", "not migrated"}
        assert sum(s == "migrated" for s in cov.values()) >= 104

    def test_every_new_mapping_for_a_kisumu_clean_table_migrates(self):
        cov = xm.coverage(entries_from_fixture(), FIXTURE)
        in_clean = [t for t in xm.MAPPINGS if t in FIXTURE]
        assert len(in_clean) >= 19                     # the rest come from other extracts (queue)
        for t in in_clean:
            if FIXTURE[t]["namespace"] is None:        # CLEAN-only here: discovery can't see it
                assert cov.get(t, "").startswith("not migrated: no RAW rows"), t
            else:
                assert cov.get(t) == "migrated", t

    def test_no_table_has_two_fates(self):
        assert not set(xm.MAPPINGS) & set(xm.NOT_MIGRATED)
        assert not set(xm.MAPPINGS) & set(xm.DUPLICATES)
        assert not set(xm.DUPLICATES) & set(xm.NOT_MIGRATED)

    def test_duplicates_point_at_a_table_with_a_fate(self):
        cov = xm.coverage(entries_from_fixture(), FIXTURE)
        for dup, canonical in xm.DUPLICATES.items():
            assert canonical in FIXTURE, (dup, canonical)
            assert cov[canonical] != "UNACCOUNTED" and not cov[canonical].startswith("duplicate"), canonical


class TestMappingSpecs:
    @pytest.mark.parametrize("table", sorted(xm.MAPPINGS))
    def test_consistent(self, table):
        s = xm.MAPPINGS[table]
        keep = set(s["keep"])
        assert s["alias"] and s["service"] and s["v3"].startswith("App\\Models\\")
        assert set(s.get("fk", {})) <= keep, "links must be posted columns"
        assert set(s.get("critical", [])) <= set(s.get("fk", {})), "a critical link needs a parent alias"
        assert set(s.get("null_if_unresolved", [])) <= set(s.get("fk", {}))
        assert set(s.get("inject", {})) <= keep
        for src, dst in (s.get("renames") or {}).items():
            assert dst in keep or dst.startswith("v2_"), (src, dst)
        assert not {"id", "organization_id", "uuid"} & keep

    def test_user_links_wait_for_users_never_a_stand_in(self):
        for t, s in xm.MAPPINGS.items():
            for col, alias in (s.get("fk") or {}).items():
                if alias == "users":
                    assert col in s.get("critical", []) or col in s.get("null_if_unresolved", []), (t, col)
            assert "@migrating_user" not in json.dumps(s.get("inject", {}), default=str)


class TestApply:
    def test_applied_at_import(self):
        for t, s in xm.MAPPINGS.items():
            tk = xm.transform_key(t)
            assert v2v3.NAMESPACE_MAP[s["namespace"]] == {"v3": s["v3"], "transform": tk}
            assert v2v3._PER_KEY_KEEP_ONLY[tk] == set(s["keep"])
            assert m._ALIAS_OVERRIDE[tk] == s["alias"] and m._SERVICE_OVERRIDE[tk] == s["service"]
            assert tk in v2v3._PER_KEY_RENAMES        # else transform_record treats it as "generic"

    def test_never_replaces_a_hand_written_mapping(self, monkeypatch):
        ns = xm.MAPPINGS["inpatient_charges"]["namespace"]
        monkeypatch.setitem(v2v3.NAMESPACE_MAP, ns, {"v3": "App\\Models\\HandMade", "transform": "hand"})
        assert xm.apply(v2v3, m) == 0                  # everything already present
        assert v2v3.NAMESPACE_MAP[ns]["transform"] == "hand"

    def test_namespace_alias_reuses_the_existing_transform(self):
        assert v2v3.NAMESPACE_MAP[r"Ignite\Reception\Entities\PatientInsurance"] == \
            v2v3.NAMESPACE_MAP[r"Ignite\Reception\Entities\PatientSchemes"]


def transform(table, row):
    return v2v3.transform_record(dict(row), xm.transform_key(table), ORG, "kisumu")


class TestTransforms:
    def test_inpatient_note(self, monkeypatch):
        monkeypatch.setattr(v2v3, "_visit_admission_map", {309272: 7275})
        row = {"id": 6135, "user": 0, "user_id": 1503, "type": 0, "admission_id": 0, "note_pic": None,
               "title": "Ward round", "notes": "Patient stable", "visit_id": 309272,
               "created_at": "2024-09-05 06:58:14", "updated_at": "2024-09-05 06:58:14", "source_schema": "x"}
        out = transform("inpatient_notes", row)
        assert out == {"id": 6135, "admission_id": 7275, "visit_id": 309272, "title": "Ward round",
                       "note_type": "general_note", "note_date": "2024-09-05 06:58:14",
                       "note_content": "Patient stable", "created_by": 1503}

    def test_inpatient_vitals(self):
        row = {"id": 1902, "admission_id": 7275, "visit_id": 308750, "user_id": 1507, "bp_systolic": 101,
               "bp_diastolic": 52, "pulse": 98, "respiration": 20, "oxygen": 96, "temperature": "37.9",
               "date_recorded": "2024-06-01", "time_recorded": "06:28", "created_at": "2024-06-01 06:29:52",
               "temperature_location": "Axillary", "allergies": None}
        out = transform("inpatient_vitals", row)
        assert out == {"id": 1902, "admission_id": 7275, "recorded_by": 1507, "systolic_bp": 101,
                       "diastolic_bp": 52, "pulse_rate": 98, "respiratory_rate": 20, "oxygen_saturation": 96,
                       "temperature": "37.9", "recorded_date": "2024-06-01", "recorded_time": "06:28",
                       "recorded_at": "2024-06-01 06:29:52"}

    @pytest.mark.parametrize("flags,status", [({"cancelled": 1, "order_dispatched": 1}, "cancelled"),
                                              ({"cancelled": 0, "order_dispatched": 1}, "dispatched"),
                                              ({"cancelled": 0, "order_dispatched": None}, "pending")])
    def test_store_order(self, flags, status):
        row = {"id": 36876, "requesting_store": 2, "dispatching_store": 1, "status": 1, "ordered_by": 723,
               "dispatched_by": None, "delivery_date": "2023-05-05", "created_at": "2023-05-05 12:16:35", **flags}
        out = transform("inventory_internal_order", row)
        assert out["status"] == status and "v2_status" not in out
        assert out["order_number"] == "IO-36876" and out["order_date"] == "2023-05-05 12:16:35"
        assert (out["requesting_store_id"], out["supplying_store_id"], out["ordered_by"]) == (2, 1, 723)

    def test_store_order_item(self):
        out = transform("inventory_internal_order_details",
                        {"id": 1, "store_product_id": 29, "quantity": 200, "internal_order": 36876, "product_id": 29})
        assert out == {"id": 1, "store_order_id": 36876, "product_id": 29, "quantity_requested": 200}

    def test_goods_received_line_total(self):
        out = transform("inventory_goods_received_details",
                        {"id": 3, "product_id": 9, "received_quantity": 4, "receiving_price": "2.5",
                         "goods_receipt_id": 11, "ordered_quantity": 4, "unit_cost": 2.5, "pack_size": 1,
                         "bonus": 0, "discount": 0, "tax": 0, "invoice_id": 5})
        assert out["line_total"] == 10.0 and out["grn_id"] == 11 and "invoice_id" not in out

    def test_treatment_location_code(self):
        out = transform("evaluation_treatment_locations", {"id": 1, "name": "outpatient", "description": None})
        assert out["code"] == "OUTPATIENT"

    def test_extractor_columns_never_posted(self):
        out = transform("inpatient_charges", {"id": 43, "name": "Admission Fee", "cost": "2800.00", "type": "once",
                                              "associated_procedure": "Admission Fee", "source_schema": "s",
                                              "page": 1, "namespace": "n", "created_at": "c"})
        assert set(out) == {"id", "name", "cost", "type", "associated_procedure"}


class TestSkipping:
    def test_duplicates_are_not_migrated(self):
        es = m.mark_skipped([
            {"table": "reception_patient_insurance", "v3": "App\\Models\\PatientInsurance", "transform": "t"},
            {"table": "patient_insurance", "v3": "App\\Models\\PatientInsurance", "transform": "t"},
            {"table": "users_permissions", "v3": None, "transform": None},
            {"table": "brand_new_table", "v3": None, "transform": None},
        ])
        assert es[0]["v3"] is None and es[0]["skipped"] == "duplicate of patient_insurance"
        assert es[1]["v3"] and "skipped" not in es[1]
        assert es[2]["skipped"] == xm.SETUP
        assert es[3]["skipped"] is None                     # → UNACCOUNTED in the listing

    def test_coverage_flags_unknown_tables(self):
        cov = xm.coverage([{"table": "a", "v3": "X", "transform": "x"}], ["a", "b", "users_permissions"])
        assert cov == {"a": "migrated", "b": "UNACCOUNTED", "users_permissions": f"not migrated: {xm.SETUP}"}


# ─── twins / history / no-CLEAN-view ─────────────────────────────────────

class TestTwins:
    def test_pairs_are_clean_tables_with_one_fate(self):
        for t, twin in xm.TWINS.items():
            assert t in FIXTURE and twin in FIXTURE and t != twin, (t, twin)
            assert t not in xm.DUPLICATES and t not in xm.NOT_MIGRATED, t
            assert twin not in xm.TWINS, "a twin must be migrated whole"

    def test_twins_are_both_migrated(self):
        cov = xm.coverage(entries_from_fixture(), FIXTURE)
        for t, twin in xm.TWINS.items():
            assert cov[t] == "migrated" and cov[twin] == "migrated", (t, twin)

    def test_reader_skips_ids_the_twin_has(self, monkeypatch):
        monkeypatch.setattr(m, "clean_tables", lambda cur, f: ["admissions", "inpatient_admissions"])
        monkeypatch.setattr(m, "sf_schema", lambda f, layer: "DB.K_CLEAN")
        sql = m._without_twin_rows(None, "k", "admissions", "SELECT * FROM DB.K_CLEAN.admissions")
        assert sql == ('SELECT * FROM (SELECT * FROM DB.K_CLEAN.admissions) WHERE TO_VARCHAR("ID") NOT IN '
                       '(SELECT TO_VARCHAR("ID") FROM DB.K_CLEAN.INPATIENT_ADMISSIONS WHERE "ID" IS NOT NULL)')

    def test_no_twin_in_this_facility_reads_everything(self, monkeypatch):
        monkeypatch.setattr(m, "clean_tables", lambda cur, f: ["admissions"])
        assert m._without_twin_rows(None, "k", "admissions", "BASE") == "BASE"
        assert m._without_twin_rows(None, "k", "evaluation_prescriptions", "BASE") == "BASE"


class TestSelection:
    @pytest.fixture
    def select(self, monkeypatch):
        entries = [
            {"table": "patients", "v3": r"App\Models\Patient", "transform": "reception_patient"},
            {"table": "evaluation_prescriptions", "v3": r"App\Models\Prescription", "transform": "evaluation_prescription"},
            {"table": "visit_purposes", "v3": None, "transform": None},
            {"table": "mapped_but_no_view", "v3": r"App\Models\Unit", "transform": "inventory_unit"},
        ]

        class Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def cursor(self):
                return type("C", (), {"close": lambda s: None})()
        monkeypatch.setattr(m, "_snowflake_connect", Conn)
        monkeypatch.setattr(m, "discover_tables", lambda cur, f: [dict(e) for e in entries])
        monkeypatch.setattr(m, "clean_tables", lambda cur, f: ["patients", "evaluation_prescriptions"])
        monkeypatch.setattr(m, "OLD_SYSTEM_HISTORY_TABLES", {"evaluation_prescriptions": ("x", "y")})
        return lambda **kw: m._select_entries("k", None, None, set(), **kw)

    def test_history_left_out_by_default(self, select):
        runnable, unmapped, _ = select()
        assert [e["table"] for e in runnable] == ["patients"]

    def test_include_history(self, select):
        runnable, _, _ = select(include_history=True)
        assert sorted(e["table"] for e in runnable) == ["evaluation_prescriptions", "patients"]

    def test_no_clean_view_is_skipped_with_reason(self, select):
        _, unmapped, _ = select()
        reasons = {e["table"]: e.get("skipped") for e in unmapped}
        assert reasons == {"visit_purposes": "no CLEAN view — nothing to migrate",
                           "mapped_but_no_view": "no CLEAN view — nothing to migrate"}
