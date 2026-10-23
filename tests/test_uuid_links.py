"""
The V2 uuid layer (2026-10): links arrive as `<column>_uuid` with the integer
column nulled. Covers

  * snowflake_to_v3_migration.restore_int_links — integers put back from the
    uuids, person uuid for patients, the `invoiced` flag, critical links held
  * the id↔uuid registry (in memory, on disk, reloaded) and
    ensure_uuids_loaded reading parents from CLEAN
  * _with_stable_uuid preferring a V3 uuid already paired with the V2 id
  * _already_inserted recognising records migrated before uuids (by V2 id)
  * post_table_to_v3 never posting V2 link uuids to V3
  * pair_v3_uuids.pair
  * the loader's keyset default and namespace fallback

Row shapes are the live level6.collabmed.net ones (2026-10-10). No network,
no Snowflake.
"""
from __future__ import annotations

import json
from collections import Counter
from unittest import mock

import pytest

import facility_to_snowflake_fast_resume as loader
import pair_v3_uuids
import snowflake_to_v3_migration as m
import v2_to_v3_api_migration as v2v3

P_ROW, P_PERSON = "b12d4e2e-0000-0000-0000-00000000000a", "e831a6cd-0000-0000-0000-00000000000b"
V_ROW = "3feb8752-0000-0000-0000-00000000000c"
STORE = "a51d748c-0000-0000-0000-00000000000d"
INVOICE = "f51ddc6d-0000-0000-0000-00000000000e"


@pytest.fixture
def reg(tmp_path, monkeypatch):
    """A clean registry in a tmp state file, with a patient, a visit and a store."""
    monkeypatch.setattr(m, "ID_TO_UUID_FILE", tmp_path / ".migration_id_to_uuid.json")
    monkeypatch.setattr(m, "_id_to_uuid", {})
    monkeypatch.setattr(m, "_alias_to_table", {})
    monkeypatch.setattr(m, "_uuid_to_id", {})
    monkeypatch.setattr(m, "_uuids_loaded", set())
    m._register_table("patient", "patients",
                      [{"id": 232035, "uuid": P_ROW.upper(), "patient_uuid": P_PERSON}])
    m._register_table("visits", "visits", [{"id": 325013, "uuid": V_ROW, "patient_uuid": P_PERSON}])
    m._register_table("store", "inventory_stores", [{"id": 7, "uuid": STORE}])
    return tmp_path


def visit_row(**kw):   # evaluation_visits as the gateway returns it now
    return {"id": 325013, "uuid": V_ROW, "patient": None, "patient_uuid": P_PERSON,
            "reception_patient_uuid": P_ROW, "clinic": None, "clinic_uuid": "8a1c1d5b-unknown", **kw}


# ─── restore_int_links ───────────────────────────────────────────────────

class TestRestore:
    def test_visit_patient_from_person_uuid(self, reg):
        rows, held, miss = m.restore_int_links([visit_row()], "reception_visit")
        assert rows[0]["patient"] == 232035 and held == 0
        assert rows[0]["clinic"] is None and miss == Counter({"clinic": 1})   # unknown parent: stays null

    def test_child_visit_link(self, reg):
        rx = {"id": 841050, "uuid": "rx", "visit": None, "visit_uuid": V_ROW.upper(),
              "store_id": None, "store_uuid": STORE, "patient_uuid": P_PERSON}
        [out], held, _ = m.restore_int_links([rx], "evaluation_prescription")
        assert (out["visit"], out["store_id"]) == (325013, 7)
        assert "patient" not in out and "patient_id" not in out      # no column → none invented

    def test_visit_id_column(self, reg):
        res = {"id": 158, "uuid": "r", "investigation": None, "investigation_uuid": None,
               "visit_id": None, "visit_uuid": V_ROW}
        [out], _, _ = m.restore_int_links([res], "evaluation_inv_result")
        assert out["visit_id"] == 325013 and out["investigation"] is None

    def test_invoiced_flag(self, reg):
        rows = [{"id": 1, "uuid": "a", "invoiced": None, "invoiced_uuid": INVOICE, "visit": None, "visit_uuid": V_ROW},
                {"id": 2, "uuid": "b", "invoiced": 0, "invoiced_uuid": None, "visit": None, "visit_uuid": V_ROW}]
        out, _, _ = m.restore_int_links(rows, "evaluation_investigation")
        assert [r["invoiced"] for r in out] == [1, 0]

    def test_integer_still_set_is_left_alone(self, reg):
        [out], _, miss = m.restore_int_links([visit_row(patient=999)], "reception_visit")
        assert out["patient"] == 999 and "patient" not in miss

    def test_old_extract_untouched(self, reg):
        old = {"id": 5, "visit": 42, "drug": 9}
        [out], held, miss = m.restore_int_links([dict(old)], "evaluation_prescription")
        assert out == old and held == 0 and not miss

    def test_critical_unresolved_is_held(self, reg):
        rx = {"id": 1, "uuid": "x", "visit": None, "visit_uuid": "not-registered"}
        rows, held, miss = m.restore_int_links([rx], "evaluation_prescription")   # visit_id is critical
        assert rows == [] and held == 1 and miss == Counter({"visit": 1})

    def test_critical_override(self, reg):
        vt = {"id": 1, "uuid": "x", "visit": None, "visit_uuid": "not-registered"}
        assert m.restore_int_links([dict(vt)], "inpatient_vital")[1] == 0              # admission_id only
        assert m.restore_int_links([dict(vt)], "inpatient_vital", critical={"visit_id"})[1] == 1

    def test_non_critical_unresolved_posts_with_null(self, reg):
        [out], held, _ = m.restore_int_links([visit_row()], "reception_visit")
        assert held == 0 and out["clinic"] is None


# ─── registry ────────────────────────────────────────────────────────────

class TestRegistry:
    def test_persisted_and_reloaded(self, reg, monkeypatch):
        on_disk = json.loads((reg / ".migration_id_to_uuid.json").read_text())
        assert on_disk["id_to_uuid"]["visits"] == {"325013": V_ROW}
        assert on_disk["person_uuid_to_id"] == {P_PERSON: 232035}
        assert on_disk["alias_to_table"]["patient"] == "patients"
        monkeypatch.setattr(m, "_uuid_to_id", {})
        m._load_id_to_uuid()
        assert m._uuid_to_id[V_ROW] == 325013
        assert m._uuid_to_id[P_ROW] == 232035 and m._uuid_to_id[P_PERSON] == 232035

    def test_existing_file_contents_survive(self, reg, monkeypatch):
        f = reg / ".migration_id_to_uuid.json"
        data = json.loads(f.read_text())
        data["id_to_uuid"]["other"] = {"1": "u-other"}
        data["something_else"] = 1
        f.write_text(json.dumps(data))
        m._register_table("store", "inventory_stores", [{"id": 8, "uuid": "s8"}])
        after = json.loads(f.read_text())
        assert after["id_to_uuid"]["other"] == {"1": "u-other"} and after["something_else"] == 1
        assert after["id_to_uuid"]["inventory_stores"] == {"7": STORE, "8": "s8"}

    def test_parent_tables(self, reg):
        assert m._parent_tables("evaluation_prescription", r"App\Models\Prescription") >= {"visits", "patients"}
        assert "inventory_stores" in m._parent_tables("inventory_dispensing", r"App\Models\Sale")

    def test_ensure_uuids_loaded_reads_clean(self, reg, monkeypatch):
        cur = mock.MagicMock()
        cur.description = [("ID",), ("UUID",), ("PATIENT_UUID",), ("NAME",)]
        cur.fetchall.return_value = [(1, "U1", "P1"), ("2", "U2", None)]
        conn = mock.MagicMock()
        conn.__enter__.return_value.cursor.return_value = cur
        monkeypatch.setattr(m, "_snowflake_connect", lambda: conn)
        monkeypatch.setattr(m, "sf_schema", lambda f, layer: "HOSPITALS.X_CLEAN")
        m.ensure_uuids_loaded("x", {"patients", "visits"})
        sqls = [c.args[0] for c in cur.execute.call_args_list]
        assert "SELECT id, uuid, patient_uuid FROM HOSPITALS.X_CLEAN.PATIENTS WHERE uuid IS NOT NULL" in sqls
        assert "SELECT id, uuid FROM HOSPITALS.X_CLEAN.VISITS WHERE uuid IS NOT NULL" in sqls
        assert m._uuid_to_id["u1"] == 1 and m._uuid_to_id["u2"] == 2 and m._uuid_to_id["p1"] == 1
        cur.execute.reset_mock()
        m.ensure_uuids_loaded("x", {"patients"})          # once per process
        cur.execute.assert_not_called()

    def test_ensure_skips_tables_without_uuid(self, reg, monkeypatch):
        cur = mock.MagicMock()
        cur.description = [("ID",), ("NAME",)]
        conn = mock.MagicMock()
        conn.__enter__.return_value.cursor.return_value = cur
        monkeypatch.setattr(m, "_snowflake_connect", lambda: conn)
        monkeypatch.setattr(m, "sf_schema", lambda f, layer: "S")
        m.ensure_uuids_loaded("x", {"old_table"})
        assert cur.execute.call_count == 1                   # just the LIMIT 0 probe

    def test_ensure_survives_snowflake_error(self, reg, monkeypatch):
        def boom():
            raise RuntimeError("no such table")
        monkeypatch.setattr(m, "_snowflake_connect", boom)
        m.ensure_uuids_loaded("x", {"gone"})                 # warns, doesn't raise


# ─── stable uuid / already inserted / payload ────────────────────────────

class TestPosting:
    @pytest.fixture
    def known(self, monkeypatch):
        monkeypatch.setattr(m, "_known_v3_uuids", {"visits": {"325013": "a2eb-v3-own"}})
        monkeypatch.setattr(m, "_model_has_uuid", lambda alias, svc: True)

    def test_known_v3_uuid_wins_over_v2_uuid(self, known):
        out = m._with_stable_uuid({"uuid": V_ROW}, facility="k", alias="visits", rec_key=V_ROW,
                                  transform_key="reception_visit", v2_id=325013)
        assert out["uuid"] == "a2eb-v3-own"

    def test_unknown_keeps_v2_uuid(self, known):
        out = m._with_stable_uuid({"uuid": V_ROW}, facility="k", alias="visits", rec_key=V_ROW,
                                  transform_key="reception_visit", v2_id=1)
        assert out["uuid"] == V_ROW

    def test_no_uuid_gets_stable_uuid5(self, known):
        a = m._with_stable_uuid({}, facility="k", alias="visits", rec_key=5, transform_key="reception_visit", v2_id=5)
        b = m._with_stable_uuid({}, facility="k", alias="visits", rec_key=5, transform_key="reception_visit", v2_id=5)
        assert a["uuid"] == b["uuid"] and len(a["uuid"]) == 36

    def test_match_on_override_untouched(self, known, monkeypatch):
        monkeypatch.setattr(m, "_known_v3_uuids", {"patient": {"1": "v3"}})
        out = m._with_stable_uuid({"uuid": P_ROW}, facility="k", alias="patient", rec_key=P_ROW,
                                  transform_key="reception_patient", v2_id=1)
        assert out["uuid"] == P_ROW

    def test_already_inserted_by_v2_id(self, monkeypatch):
        monkeypatch.setattr(v2v3, "_inserted_ids", {"k|sf:visits": {325013, "abc"}})
        assert m._already_inserted("k|sf:visits", {"id": "325013", "uuid": V_ROW})
        assert m._already_inserted("k|sf:visits", {"id": 325013, "uuid": V_ROW})
        assert m._already_inserted("k|sf:visits", {"uuid": "abc"})
        assert not m._already_inserted("k|sf:visits", {"id": 1, "uuid": V_ROW})
        assert not m._already_inserted("k|sf:visits", {"id": None, "uuid": V_ROW})

    def test_v2_link_uuids_never_posted(self, known, monkeypatch):
        sent = []
        monkeypatch.setattr(v2v3, "_post_to_v3_batch", lambda ns, cfg, payload, **kw: sent.append(payload) or 11)
        monkeypatch.setattr(v2v3, "_inserted_ids", {})
        monkeypatch.setattr(v2v3, "_mark_record_inserted", lambda *a: None)
        monkeypatch.setattr(v2v3, "_flush_record_progress", lambda: None)
        monkeypatch.setattr(m, "_store_uuid_mapping", lambda *a: None)
        monkeypatch.setattr(m, "_remap_fks_via_uuid", lambda rec, tk, ns: (dict(rec), []))
        rec = {"id": 325013, "uuid": V_ROW, "patient_id": 380565, "patient_uuid": P_PERSON,
               "clinic_uuid": "c", "batch_uuid": "keep-me"}
        problems = m.post_table_to_v3(r"App\Models\Visit", {}, [rec], transform_key="reception_visit",
                                      alias="visits", job_key="k|sf:visits", dry_run=False)
        assert problems == 0
        assert sent == [{"id": 325013, "uuid": "a2eb-v3-own", "patient": 380565, "batch_uuid": "keep-me"}]


# ─── pair_v3_uuids ───────────────────────────────────────────────────────

class TestPair:
    def test_pairs_v2_ids_with_v3_uuids(self, tmp_path, monkeypatch):
        monkeypatch.setattr(v2v3, "STATE_ROOT", tmp_path)
        d = tmp_path / "fac"
        d.mkdir()
        (d / ".migration_id_map.json").write_text(json.dumps({
            "visits": {"1": 101, "2": 102, "3": 999, "some-uuid": 103},
            "prescriptions": {"5": 205}, "nothing_here": {}, "hidden": {"9": 1}}))
        (d / ".migration_v3_uuid.json").write_text(json.dumps({"visits": {"1": "OLD"}, "keep": {"4": "x"}}))
        monkeypatch.setattr(v2v3, "_alias_to_service", {"visits": "evaluation", "prescriptions": "evaluation"})
        rows = {"visits": [{"id": 101, "uuid": "v-101"}, {"id": 102, "uuid": "v-102"}, {"id": 103, "uuid": "v-103"}],
                "prescriptions": [{"id": 205, "uuid": None}]}
        rep = pair_v3_uuids.pair("fac", read=lambda svc, alias: rows[alias])
        assert rep["visits"] == {"pairs": 2, "added": 1, "changed": 1, "v3_missing": 1}
        assert rep["prescriptions"] == {"skipped": "V3 rows have no uuid column"}
        assert rep["hidden"] == {"skipped": "not exposed by the V3 gateway"}
        assert rep["nothing_here"]["skipped"].startswith("no V2-id")
        out = json.loads((d / ".migration_v3_uuid.json").read_text())
        assert out["visits"] == {"1": "v-101", "2": "v-102"} and out["keep"] == {"4": "x"}
        assert list(d.glob(".migration_v3_uuid.json.bak_pair_*"))

    def test_dry_run_writes_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(v2v3, "STATE_ROOT", tmp_path)
        (tmp_path / "fac").mkdir()
        (tmp_path / "fac" / ".migration_id_map.json").write_text(json.dumps({"visits": {"1": 101}}))
        monkeypatch.setattr(v2v3, "_alias_to_service", {"visits": "evaluation"})
        pair_v3_uuids.pair("fac", dry_run=True, read=lambda s, a: [{"id": 101, "uuid": "u"}])
        assert not (tmp_path / "fac" / ".migration_v3_uuid.json").exists()


# ─── loader: keyset everywhere ───────────────────────────────────────────

class Resp:
    def __init__(self, body):
        self.body = body

    def json(self):
        return self.body


class TestKeyset:
    def test_default_is_every_table(self):
        assert loader._use_keyset("anything")

    def test_env_list_limits_it(self, monkeypatch):
        monkeypatch.setattr(loader, "KEYSET_TABLES", {"users"})
        assert loader._use_keyset("users") and not loader._use_keyset("visits")

    @pytest.fixture
    def job(self, tmp_path, monkeypatch):
        monkeypatch.setattr(loader, "PAGE_STATE_DIR", tmp_path)
        return {"facility": "f", "module": "Evaluation", "table": "evaluation_visits",
                "namespace": "Ignite\\Evaluation\\Entities\\Visits", "database": "db",
                "updated_since": None, "limit": None}

    def test_namespace_fallback_found_once_then_reused(self, job, monkeypatch):
        calls = []
        pages = iter([
            {"data": [{"id": 1}], "pagination": {"has_more_pages": True, "next_after_id": 1}},
            {"data": [{"id": 2}], "pagination": {"has_more_pages": False, "next_after_id": 2}},
        ])

        def fake(url, headers, bodies, **kw):
            calls.append([b["namespace"] for b in bodies])
            used = next(b for b in bodies if b["namespace"].endswith("\\Visit"))
            return Resp(next(pages)), used
        monkeypatch.setattr(loader, "post_with_retry_and_fallback", fake)
        loader._extract_keyset(job, "run", True, None, url="u", headers={}, session=None)
        assert calls[0][0] == "Ignite\\Evaluation\\Entities\\Visits" and "Ignite\\Evaluation\\Entities\\Visit" in calls[0]
        assert len(calls[0]) == len(set(calls[0])) > 1
        assert calls[1] == ["Ignite\\Evaluation\\Entities\\Visit"]

    def test_leftover_page_mode_state_is_discarded(self, job, monkeypatch, caplog):
        state_path, spool = loader._page_state_paths(job)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps({"loaded_pages": [1, 2, 3], "last_page": 9, "s3_keys": [], "rows_loaded": 0}))
        spool.mkdir(parents=True, exist_ok=True)
        (spool / "page_00004.jsonl.gz").write_bytes(b"x")
        monkeypatch.setattr(loader, "post_with_retry_and_fallback",
                            lambda url, headers, bodies, **kw: (Resp({"data": [], "pagination": {}}), bodies[0]))
        monkeypatch.setattr(loader, "_flush_spool", lambda *a, **k: None)
        loader._extract_keyset(job, "run", False, None, url="u", headers={}, session=None)
        assert "discarding page-mode resume state" in caplog.text
        assert not (spool / "page_00004.jsonl.gz").exists()
        # an empty table finishes and clears its state; any state left must be keyset
        if state_path.exists():
            assert json.loads(state_path.read_text())["mode"] == "keyset"
