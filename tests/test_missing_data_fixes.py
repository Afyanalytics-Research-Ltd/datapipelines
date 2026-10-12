"""
Fixes for migrating the records V3 org 4 (kisumu_v3) is missing:

  * table_set=queue — any V2 table extracted by name from the V2 extraction
    queue (exact namespaces)
  * duplicates are only skipped when the canonical copy is in the same
    facility's CLEAN (for kisumu_v3, reception_patient_schemes is the only copy)
  * a V2 id already in the model's id map counts as migrated — re-extracted
    tables under new names aren't posted again — except for models fed by
    two different V2 tables (vital)
  * uuid registration reads a parent's twin (the re-extract carries the uuids)
  * diagnoses (ICD10 notes) map to V3 evaluation_diagnosis, the diagnosis text
    joined from the ICD10 variation
"""
from __future__ import annotations

import pytest

import facility_to_snowflake_fast_resume as loader
import snowflake_to_v3_migration as m
import v2_to_v3_api_migration as v2v3
import v3_extra_mappings as xm

ORG = {"organization_id": 4, "facility_id": 4, "application_id": 1}


class TestQueueTableSet:
    def test_reads_the_queue(self):
        rows = loader.read_extraction_queue()
        tables = {r["table"]: r for r in rows}
        assert len(rows) > 400
        assert tables["evaluation_visit_consultants"]["namespace"] == r"Ignite\Evaluation\Entities\VisitConsultants"
        assert tables["users_permissions"]["namespace"] == ""                     # no model
        tiers = [int(r["step"]) for r in rows]
        assert tiers == sorted(tiers)                                              # load order

    def test_jobs_for_named_tables(self, monkeypatch):
        monkeypatch.setattr(loader, "facility_config", lambda f: {"base_url": "https://x", "db": "kisumu_db"})
        monkeypatch.setattr(loader, "get_watermark", lambda key: None)
        jobs = loader.build_jobs_for_facility("kisumu_v3", only_tables={"evaluation_visit_consultants",
                                                                       "reception_patient_schemes",
                                                                       "users_permissions"}, table_set="queue")
        assert {j["table"]: j["namespace"] for j in jobs} == {
            "evaluation_visit_consultants": r"Ignite\Evaluation\Entities\VisitConsultants",
            "reception_patient_schemes": r"Ignite\Reception\Entities\PatientInsurance",
        }                                                                          # users_permissions: no namespace
        assert all(j["database"] == "kisumu_db" for j in jobs)

    def test_every_missing_table_on_munius_list_resolves_to_a_mapping(self):
        """The tables V3 org 4 is missing, as the migration's discovery would resolve them."""
        queue = {r["table"]: r for r in loader.read_extraction_queue()}
        wanted = ["evaluation_visit_consultants", "reception_patient_schemes", "evaluation_icd10_notes",
                  "evaluation_procedure_prices", "evaluation_refraction", "evaluation_icd10_types",
                  "evaluation_sample_types", "evaluation_units", "evaluation_templates_lab",
                  "evaluation_critical_values", "settings_schemes", "settings_insurance", "evaluation_age_groups",
                  "users", "reception_patient_documents", "inventory_store_departments", "finance_payment_modes",
                  "inventory_suppliers", "reception_appointment_categories", "settings_treatment_actions"]
        for t in wanted:
            q = queue[t]
            cands = m._candidate_namespaces(q["module"], t) + [q["namespace"]]
            assert any(c in v2v3.NAMESPACE_MAP for c in cands), t


class TestDuplicatesPerFacility:
    def test_skipped_only_when_the_canonical_copy_is_there(self):
        e = lambda: [{"table": "reception_patient_schemes", "v3": r"App\Models\PatientInsurance",   # noqa: E731
                      "transform": "reception_patient_scheme"}]
        kisumu = m.mark_skipped(e(), {"reception_patient_schemes", "patient_insurance"})
        kisumu_v3 = m.mark_skipped(e(), {"reception_patient_schemes", "evaluation_visits"})
        assert kisumu[0]["v3"] is None and kisumu[0]["skipped"] == "duplicate of patient_insurance"
        assert kisumu_v3[0]["v3"] and "skipped" not in kisumu_v3[0]

    def test_coverage_uses_the_same_rule(self):
        cov = xm.coverage([{"table": "reception_patient_schemes", "v3": "X", "transform": "t"}],
                          ["reception_patient_schemes"])
        assert cov == {"reception_patient_schemes": "migrated"}


class TestAlreadyMigratedById:
    @pytest.fixture(autouse=True)
    def state(self, monkeypatch):
        monkeypatch.setattr(v2v3, "_inserted_ids", {})
        monkeypatch.setattr(v2v3, "_id_map", {"admission": {2116: 501, "9999": 502}, "vital": {7: 70}})
        monkeypatch.setattr(m, "_single_source_cache", {})

    def test_v2_id_in_the_id_map_is_skipped(self):
        assert m._already_inserted("kisumu_v3|sf:inpatient_admissions", {"id": 2116, "uuid": "u"}, "admission")
        assert m._already_inserted("kisumu_v3|sf:inpatient_admissions", {"id": "9999", "uuid": "u"}, "admission")
        assert not m._already_inserted("kisumu_v3|sf:inpatient_admissions", {"id": 1, "uuid": "u"}, "admission")

    def test_not_for_models_fed_by_two_v2_tables(self):
        assert not m._single_source("vital")                     # evaluation + inpatient vitals
        assert not m._already_inserted("kisumu_v3|sf:inpatient_vitals", {"id": 7, "uuid": "u"}, "vital")

    def test_single_source_models(self):
        assert m._single_source("admission")
        assert m._single_source("diagnoses")


class TestTwinUuids:
    def test_parent_tables_include_twins(self, monkeypatch):
        monkeypatch.setattr(m, "_alias_to_table", {"patient": "patients", "visits": "visits",
                                                   "admission": "inpatient_admissions"})
        tables = m._parent_tables("gen_evaluation_visit_consultants", r"App\Models\VisitConsultant")
        assert {"patients", "reception_patients", "visits", "evaluation_visits",
                "inpatient_admissions", "admissions"} <= tables


class TestDiagnoses:
    def test_transform(self, monkeypatch):
        monkeypatch.setattr(v2v3, "_visit_patient_map", {308666: 232035})
        row = {"id": 67107, "visit_id": 308666, "code": "", "created_at": "2024-04-18 11:08:49",
               "updated_at": "2024-04-18 11:08:49", "variation_id": None, "visit": None, "user_id": 1503,
               "uuid": "14ff863a", "variation_code": "A20.9", "variation_name": "Plague, unspecified",
               "variation_uuid": "82a56163"}
        out = v2v3.transform_record(row, xm.transform_key("evaluation_icd10_notes"), ORG, "kisumu_v3")
        assert out == {"id": 67107, "uuid": "14ff863a", "visit_id": 308666, "patient_id": 232035,
                       "diagnosis_text": "A20.9 Plague, unspecified", "diagnosis_date": "2024-04-18 11:08:49",
                       "recorded_by": 1503}

    def test_code_when_no_variation(self, monkeypatch):
        monkeypatch.setattr(v2v3, "_visit_patient_map", {})
        out = v2v3.transform_record({"id": 1, "visit_id": 5, "patient_id": 9, "code": "B33.3"},
                                    xm.transform_key("evaluation_icd10_notes"), ORG, "k")
        assert out["diagnosis_text"] == "B33.3" and out["patient_id"] == 9

    def test_both_namespaces_map(self):
        assert v2v3.NAMESPACE_MAP[r"Ignite\Evaluation\Entities\Icd10Note"]["transform"] == "gen_evaluation_icd10_notes"
        assert v2v3.NAMESPACE_MAP[r"Ignite\Evaluation\Entities\Icd10Notes"]["transform"] == "gen_evaluation_icd10_notes"

    @pytest.mark.parametrize("notes,variations,join", [
        ({"id", "variation_id", "variation_uuid"}, {"id", "uuid", "code", "name"},
         'TO_VARCHAR(v."ID") = TO_VARCHAR(n."VARIATION_ID") OR v."UUID" = n."VARIATION_UUID"'),
        ({"id", "variation_id"}, {"id", "code", "name"}, 'TO_VARCHAR(v."ID") = TO_VARCHAR(n."VARIATION_ID")'),
    ])
    def test_join_sql(self, notes, variations, join):
        cols = {"evaluation_icd10_notes": notes, "evaluation_icd10_variations": variations}
        sql = xm.FETCH_SQL["evaluation_icd10_notes"]("DB.K_CLEAN", cols.get, set(cols))
        assert sql == ('SELECT n.*, v."CODE" AS "VARIATION_CODE", v."NAME" AS "VARIATION_NAME" '
                       'FROM DB.K_CLEAN.EVALUATION_ICD10_NOTES n LEFT JOIN DB.K_CLEAN.EVALUATION_ICD10_VARIATIONS v ON '
                       + join)

    def test_no_variations_table_reads_notes_as_is(self):
        assert xm.FETCH_SQL["evaluation_icd10_notes"]("S", lambda t: set(), {"evaluation_icd10_notes"}) is None


class TestStoreDepartments:
    def test_transform(self):
        out = v2v3.transform_record({"id": 3, "name": "Pharmacy", "description": "", "created_at": "c", "uuid": "u"},
                                    xm.transform_key("inventory_store_departments"), ORG, "k")
        assert out == {"id": 3, "uuid": "u", "name": "Pharmacy", "description": ""}
        assert m._ALIAS_OVERRIDE["gen_inventory_store_departments"] == "departments"
        assert m._SERVICE_OVERRIDE["gen_inventory_store_departments"] == "core"


class TestVisitNeverExtracted:
    def test_visit_linked_mappings_skip_rows_whose_visit_was_never_extracted(self):
        for t in ("evaluation_icd10_notes", "evaluation_refraction", "patient_management",
                  "evaluation_visit_consultants"):
            assert m._SKIP_IF_PARENT_NOT_IN_FACILITY[xm.transform_key(t)] == [("visit_id", "visits", "id")], t

    def test_not_for_mappings_without_a_required_visit(self):
        assert xm.transform_key("inpatient_notes") not in m._SKIP_IF_PARENT_NOT_IN_FACILITY


class _Resp:
    def __init__(self, body):
        self.body = body

    def json(self):
        return self.body


class TestModelNotOnThisSite:
    """A sheet table the V2 site has no model for (e.g. inp_blood_transfusions
    on level6) is skipped — not a failed task."""

    @pytest.fixture
    def job(self, tmp_path, monkeypatch):
        monkeypatch.setattr(loader, "PAGE_STATE_DIR", tmp_path)
        return {"facility": "kisumu_v3", "module": "Inpatient", "table": "inp_blood_transfusions",
                "namespace": r"Ignite\Inpatient\Entities\BloodTransfusions", "database": "db",
                "updated_since": None, "limit": 500}

    def test_keyset_all_404_on_first_request_is_skipped(self, job, monkeypatch, caplog):
        def all_404(url, headers, bodies, **kw):
            raise loader.NamespaceNotFound("All fallback request bodies returned 404")
        monkeypatch.setattr(loader, "post_with_retry_and_fallback", all_404)
        for dry in (True, False):
            assert loader._extract_keyset(job, "run", dry, None, url="u", headers={}, session=None) is None
        assert "not on this V2 site" in caplog.text

    def test_keyset_404_mid_walk_still_fails(self, job, monkeypatch):
        calls = {"n": 0}

        def first_ok_then_404(url, headers, bodies, **kw):
            calls["n"] += 1
            if calls["n"] == 1:
                return _Resp({"data": [{"id": 1}], "pagination": {"has_more_pages": True, "next_after_id": 1}}), bodies[0]
            raise loader.NamespaceNotFound("All fallback request bodies returned 404")
        monkeypatch.setattr(loader, "post_with_retry_and_fallback", first_ok_then_404)
        monkeypatch.setattr(loader, "_flush_spool", lambda *a, **k: None)
        with pytest.raises(RuntimeError, match="keyset fetch after id 1 failed"):
            loader._extract_keyset(job, "run", False, None, url="u", headers={}, session=None)

    def test_resumed_namespace_404_still_fails(self, job, monkeypatch):
        state_path, _ = loader._page_state_paths(job)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text('{"mode": "keyset", "after_id": 0, "batch": 0, "loaded_pages": [], "s3_keys": [], '
                              '"rows_loaded": 0, "namespace": "Ignite\\\\Inpatient\\\\Entities\\\\BloodTransfusion"}')

        def all_404(url, headers, bodies, **kw):
            raise loader.NamespaceNotFound("All fallback request bodies returned 404")
        monkeypatch.setattr(loader, "post_with_retry_and_fallback", all_404)
        monkeypatch.setattr(loader, "_flush_spool", lambda *a, **k: None)
        with pytest.raises(RuntimeError):
            loader._extract_keyset(job, "run", False, None, url="u", headers={}, session=None)

    def test_page_mode_all_404_is_skipped(self, job, monkeypatch):
        monkeypatch.setattr(loader, "KEYSET_TABLES", {"some_other_table"})
        monkeypatch.setattr(loader, "facility_config", lambda f: {"base_url": "https://x", "db": "db"})
        monkeypatch.setattr(loader, "_facility_session", lambda f: None)
        monkeypatch.setattr(loader, "_facility_token", lambda f: "t")

        def all_404(url, headers, bodies, **kw):
            raise loader.NamespaceNotFound("All fallback request bodies returned 404")
        monkeypatch.setattr(loader, "post_with_retry_and_fallback", all_404)
        assert loader.extract_one_model(job, run_id="r", dry_run=False) is None

    def test_still_an_exception_for_old_callers(self):
        assert issubclass(loader.NamespaceNotFound, Exception)
