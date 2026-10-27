"""
tiered_ingestion.py: bronze → silver → gold promotion, lineage, load modes,
quarantine, resume, and the Snowflake store's SQL. Everything runs against
MemoryTierStore or a fake Snowflake client — no network, no credentials.
"""
from __future__ import annotations

import json

import pytest

import tiered_ingestion as ti
from from_json_mappings_to_snowflake import Mappings

MAPPINGS = {
    "connection": {"id": 1, "name": "test hospital"},
    "tables": [
        {"source table": "patients", "v3 model": "App\\Models\\Reception\\Patient",
         "v3 table": "patients", "v3 service": "reception-service"},
        {"source table": "services", "v3 model": "App\\Models\\Inventory\\Product",
         "v3 table": "inv_products", "v3 service": "inventory-service",
         "where": {"column": "type", "in": ["drug"]}},
        {"source table": "services", "v3 model": "App\\Models\\PatientEvaluation\\Procedure",
         "v3 table": "procedures", "v3 service": "patient-evaluation-service",
         "where": {"column": "type", "in": ["procedure"]}},
    ],
    "fields": [
        {"table": "patients", "v3_table": "patients", "model": "Patient", "column": "fname", "field": "first_name"},
        {"table": "patients", "v3_table": "patients", "model": "Patient", "column": "sex", "field": "gender",
         "rule": {"map": {"m": "male", "f": "female"}}},
        {"table": "services", "v3_table": "inv_products", "model": "Product", "column": "name", "field": "name"},
        {"table": "services", "v3_table": "procedures", "model": "Procedure", "column": "name", "field": "name"},
    ],
}

PATIENTS = [{"id": 1, "fname": "Akinyi", "sex": "F", "password": "x"},
            {"id": 2, "fname": "Otieno", "sex": "m"}]
SERVICES = [{"id": 10, "name": "Paracetamol", "type": "drug"},
            {"id": 11, "name": "X-Ray", "type": "procedure"}]


@pytest.fixture
def spec(tmp_path):
    path = tmp_path / "mappings.json"
    path.write_text(json.dumps(MAPPINGS))
    return Mappings.load(path)


@pytest.fixture
def store():
    return ti.MemoryTierStore()


@pytest.fixture
def tiered(spec, store):
    return ti.TieredIngestion("test_hospital", store, mappings=spec)


@pytest.fixture
def stage_registry():
    """Register test stages without leaking them into other tests."""
    saved = {t: list(s) for t, s in ti._STAGES.items()}
    yield ti.stage
    ti._STAGES.clear()
    ti._STAGES.update(saved)


class TestTiers:
    def test_order(self):
        assert ti.TIERS == ("bronze", "silver", "gold")
        assert ti.next_tier("bronze") == "silver"
        assert ti.next_tier("silver") == "gold"
        assert ti.next_tier("gold") is None

    def test_row_hash_ignores_key_order(self):
        assert ti.row_hash({"a": 1, "b": 2}) == ti.row_hash({"b": 2, "a": 1})
        assert ti.row_hash({"a": 1}) != ti.row_hash({"a": 2})


class TestIngest:
    def test_through_gold(self, tiered, store):
        batches = tiered.ingest("patients", PATIENTS, "R1")
        assert [b.tier for b in batches] == ["bronze", "silver", "gold"]
        assert all(b.status == "done" for b in batches)
        assert [b.rows_out for b in batches] == [2, 2, 2]

        gold = {r["source_pk"]: r for r in store.tables["gold"]}
        assert gold["1"]["payload"] == {"id": 1, "first_name": "Akinyi", "gender": "female"}
        assert gold["2"]["payload"]["gender"] == "male"
        assert gold["1"]["v3_table"] == "patients"
        assert gold["1"]["module"] == "reception-service"
        assert batches[-1].targets == "patients"

    def test_bronze_keeps_source_shape_without_secrets(self, tiered, store):
        tiered.ingest("patients", PATIENTS, "R1", through="bronze")
        row = store.tables["bronze"][0]
        assert row["payload"] == {"id": 1, "fname": "Akinyi", "sex": "F"}
        assert store.tables["silver"] == [] and store.tables["gold"] == []

    def test_lineage_chain(self, tiered, store):
        bronze, silver, gold = tiered.ingest("patients", PATIENTS, "R1")
        assert bronze.batch_id == "R1.bronze.patients" and bronze.parent_batch_id is None
        assert silver.parent_batch_id == bronze.batch_id
        assert gold.parent_batch_id == silver.batch_id
        g = store.tables["gold"][0]
        assert (g["batch_id"], g["parent_batch_id"], g["source_table"]) == (gold.batch_id, silver.batch_id, "patients")
        assert store.control[gold.batch_id].status == "done"

    def test_one_source_table_fans_out_to_several_v3_tables(self, tiered, store):
        gold = tiered.ingest("services", SERVICES, "R1")[-1]
        assert gold.targets == "inv_products,procedures"
        by_table = {r["v3_table"]: r["payload"]["name"] for r in store.tables["gold"]}
        assert by_table == {"inv_products": "Paracetamol", "procedures": "X-Ray"}

    def test_rows_without_pk_get_a_hash_key(self, tiered, store):
        tiered.ingest("patients", [{"fname": "NoId"}], "R1", through="bronze")
        assert store.tables["bronze"][0]["source_pk"].startswith("hash:")

    def test_bad_mode(self, tiered):
        with pytest.raises(ValueError, match="mode"):
            tiered.ingest("patients", PATIENTS, "R1", mode="sideways")


class TestLoadModes:
    def test_rerun_same_run_id_is_idempotent(self, tiered, store):
        tiered.ingest("patients", PATIENTS, "R1")
        tiered.ingest("patients", PATIENTS, "R1")
        assert {t: len(rows) for t, rows in store.tables.items()} == {"bronze": 2, "silver": 2, "gold": 2}

    def test_full_replaces_older_batches_but_bronze_keeps_history(self, tiered, store):
        tiered.ingest("patients", PATIENTS, "R1")
        tiered.ingest("patients", PATIENTS[:1], "R2")
        assert len(store.tables["bronze"]) == 3
        assert [r["source_pk"] for r in store.tables["silver"]] == ["1"]
        assert [r["source_pk"] for r in store.tables["gold"]] == ["1"]

    def test_full_only_touches_its_own_source_table(self, tiered, store):
        tiered.ingest("patients", PATIENTS, "R1")
        tiered.ingest("services", SERVICES, "R2")
        assert sorted(r["source_table"] for r in store.tables["gold"]) == ["patients"] * 2 + ["services"] * 2

    def test_incremental_replaces_matching_keys_only(self, tiered, store):
        tiered.ingest("patients", PATIENTS, "R1")
        tiered.ingest("patients", [{"id": 2, "fname": "Otieno J.", "sex": "m"}], "R2", mode="incremental")
        gold = {r["source_pk"]: r["payload"]["first_name"] for r in store.tables["gold"]}
        assert gold == {"1": "Akinyi", "2": "Otieno J."}


class TestStages:
    def test_registered_stages_run_in_order_and_quarantine(self, tiered, store, stage_registry):
        calls = []

        @stage_registry(ti.SILVER, "second", order=20)
        def second(rows, ctx):
            calls.append("second")
            return rows

        @stage_registry(ti.SILVER, "first", order=10)
        def first(rows, ctx):
            calls.append("first")
            assert ctx.tier == "silver" and ctx.source_table == "patients"
            kept = [r for r in rows if r["payload"].get("sex")]
            return kept, [ti.Reject(r, "no sex") for r in rows if not r["payload"].get("sex")]

        silver = tiered.ingest("patients", PATIENTS + [{"id": 3, "fname": "Unknown"}], "R1")[1]
        assert calls == ["first", "second"]
        assert (silver.rows_in, silver.rows_out, silver.rows_rejected) == (3, 2, 1)
        q = store.quarantined["silver"][0]
        assert (q["stage"], q["reason"], q["source_pk"]) == ("first", "no sex", "3")
        assert len(store.tables["gold"]) == 2

    def test_stage_changes_reach_gold_and_rehash(self, tiered, store, stage_registry):
        @stage_registry(ti.SILVER, "upper")
        def upper(rows, ctx):
            for r in rows:
                r["payload"]["fname"] = r["payload"]["fname"].upper()
            return rows

        bronze, silver, _ = tiered.ingest("patients", PATIENTS, "R1")
        assert store.tables["gold"][0]["payload"]["first_name"] == "AKINYI"
        assert store.tables["bronze"][0]["payload"]["fname"] == "Akinyi"
        assert store.tables["silver"][0]["row_hash"] != store.tables["bronze"][0]["row_hash"]

    def test_reregistering_a_name_replaces_it(self, stage_registry):
        stage_registry(ti.SILVER, "x", order=1)(lambda rows, ctx: rows)
        stage_registry(ti.SILVER, "x", order=2)(lambda rows, ctx: rows)
        assert [(s.name, s.order) for s in ti.stages(ti.SILVER)] == [("x", 2)]

    def test_no_stages_on_bronze(self):
        with pytest.raises(ValueError):
            ti.stage(ti.BRONZE, "nope")

    def test_failing_stage_marks_batch_failed(self, tiered, store, stage_registry):
        @stage_registry(ti.GOLD, "boom", order=50)
        def boom(rows, ctx):
            raise RuntimeError("target schema mismatch")

        with pytest.raises(RuntimeError):
            tiered.ingest("patients", PATIENTS, "R1")
        gold = store.control["R1.gold.patients"]
        assert gold.status == "failed" and "target schema mismatch" in gold.error
        assert store.control["R1.silver.patients"].status == "done"
        assert store.tables["gold"] == []

    def test_gold_needs_mappings(self, store):
        with pytest.raises(RuntimeError, match="mappings"):
            ti.TieredIngestion("x", store).ingest("patients", PATIENTS, "R1")


class TestResume:
    def test_resume_finishes_interrupted_promotion(self, tiered, store, stage_registry):
        @stage_registry(ti.GOLD, "flaky", order=50)
        def flaky(rows, ctx):
            raise RuntimeError("connection reset")

        with pytest.raises(RuntimeError):
            tiered.ingest("patients", PATIENTS, "R1")
        assert [b.batch_id for b in tiered.pending()] == ["R1.silver.patients"]

        ti._STAGES[ti.GOLD] = [s for s in ti._STAGES[ti.GOLD] if s.name != "flaky"]
        resumed = tiered.resume()
        assert [(b.batch_id, b.status) for b in resumed] == [("R1.gold.patients", "done")]
        assert len(store.tables["gold"]) == 2
        assert tiered.pending() == []

    def test_resume_from_bronze_reads_back(self, tiered, store):
        tiered.ingest("patients", PATIENTS, "R1", through="bronze")
        resumed = tiered.resume()
        assert [b.tier for b in resumed] == ["silver", "gold"]
        assert len(store.tables["gold"]) == 2

    def test_resume_respects_through(self, tiered, store):
        tiered.ingest("patients", PATIENTS, "R1", through="bronze")
        assert [b.tier for b in tiered.resume(through="silver")] == ["silver"]
        assert store.tables["gold"] == []

    def test_resume_skips_batches_older_than_the_latest_run(self, tiered, store):
        tiered.ingest("patients", PATIENTS, "R1", through="bronze")
        tiered.ingest("patients", PATIENTS[:1], "R2")
        assert tiered.pending() == []
        assert [r["source_pk"] for r in store.tables["gold"]] == ["1"]

    def test_promote_refuses_unfinished_parent(self, tiered):
        parent = ti.BatchRecord("R1.bronze.patients", "bronze", "patients", "R1", "full", status="failed")
        with pytest.raises(ValueError, match="not done"):
            tiered.promote(parent)


class FakeSnowflake:
    def __init__(self):
        self.sql: list[str] = []

    def execute(self, sql, label=None):
        self.sql.append(" ".join(sql.split()))
        return {"rowcount": 0}


class TestSnowflakeStore:
    @pytest.fixture
    def sf_store(self, monkeypatch):
        s = ti.SnowflakeTierStore("Test Hospital")
        s._sf = FakeSnowflake()
        self.copied = []
        monkeypatch.setattr(s, "_copy", lambda target, cols, key, lines, label: self.copied.append(
            (target, cols, key, lines)))
        return s

    def test_names(self, sf_store):
        assert sf_store.table("bronze") == "HOSPITALS.TEST_HOSPITAL_BRONZE.RECORDS"
        assert sf_store.table("silver") == "HOSPITALS.TEST_HOSPITAL_SILVER.RECORDS"
        assert sf_store.table("gold") == "HOSPITALS.TEST_HOSPITAL_GOLD.EVENTS_RAW"
        assert sf_store.control == "HOSPITALS.TEST_HOSPITAL_CONTROL.TIER_BATCHES"

    def test_ensure_creates_every_tier(self, sf_store):
        sf_store.ensure()
        sql = "\n".join(sf_store._sf.sql)
        for schema in ("BRONZE", "SILVER", "GOLD", "CONTROL"):
            assert f"CREATE SCHEMA IF NOT EXISTS HOSPITALS.TEST_HOSPITAL_{schema}" in sql
        assert "TEST_HOSPITAL_GOLD.QUARANTINE" in sql
        # gold keeps the EVENTS_RAW columns the flattening step reads
        assert "TEST_HOSPITAL_GOLD.EVENTS_RAW (facility_id STRING, ingested_at TIMESTAMP_TZ" in sql

    def test_bronze_write_appends(self, sf_store):
        b = ti.BatchRecord("R1.bronze.patients", "bronze", "patients", "R1", "full")
        sf_store.write(b, [{"batch_id": b.batch_id, "source_table": "patients", "source_pk": "1",
                            "row_hash": "h", "payload": {"id": 1}}])
        assert sf_store._sf.sql == ["DELETE FROM HOSPITALS.TEST_HOSPITAL_BRONZE.RECORDS "
                                    "WHERE batch_id = 'R1.bronze.patients'"]
        target, cols, key, lines = self.copied[0]
        assert key == "raw/tiers/test_hospital/tier=bronze/table=patients/R1.jsonl.gz"
        assert set(lines[0]) == set(cols) and lines[0]["payload"] == {"id": 1}

    def test_gold_full_write_replaces_by_origin_table(self, sf_store):
        b = ti.BatchRecord("R1.gold.patients", "gold", "patients", "R1", "full", parent_batch_id="R1.silver.patients")
        sf_store.write(b, [{"batch_id": b.batch_id, "parent_batch_id": b.parent_batch_id, "source_table": "patients",
                            "source_pk": "1", "v3_table": "patients", "namespace": "N", "module": "m",
                            "payload": {"id": 1}}])
        assert ("DELETE FROM HOSPITALS.TEST_HOSPITAL_GOLD.EVENTS_RAW t WHERE t.origin_table = 'patients' "
                "AND t.batch_id <> 'R1.gold.patients'") in sf_store._sf.sql
        line = self.copied[0][3][0]
        assert (line["source_table"], line["origin_table"], line["facility_id"]) == ("patients", "patients",
                                                                                     "test_hospital")

    def test_silver_incremental_write_merges_on_key(self, sf_store):
        b = ti.BatchRecord("R2.silver.patients", "silver", "patients", "R2", "incremental")
        sf_store.write(b, [])
        merge = sf_store._sf.sql[-1]
        assert "USING (SELECT DISTINCT source_pk FROM HOSPITALS.TEST_HOSPITAL_SILVER.RECORDS" in merge
        assert "t.source_pk = n.source_pk AND t.source_table = 'patients'" in merge
        assert self.copied == []

    def test_record_batch_escapes_errors(self, sf_store):
        b = ti.BatchRecord("R1.gold.x", "gold", "x", "R1", "full", status="failed", error="it's broken")
        sf_store.record_batch(b)
        assert "error = 'it''s broken'" in sf_store._sf.sql[0]
