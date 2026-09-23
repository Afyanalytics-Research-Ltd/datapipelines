"""
Deep coverage for dags/orthopedic_v2_clean_pipeline.py: DAG task-graph shape,
the governance_gate compliance gate (approved/bypassed/unapproved split, the
zero-approved-models hard failure, and the ORTHOPEDIC_V2_FORCE_UNAPPROVED
dangerous override), the _column_expr de-identification rules (HASH/SHA2
pseudonymization, note-like-field email/phone REGEXP_REPLACE redaction, KEEP
passthrough), and the per-model MERGE/QUALIFY dedup vs. append_only
TRUNCATE+INSERT SQL builder in _clean_one_model_sql.

Fixtures below are modeled on the real shape of the repo-root
pii_classification_v2.json (models -> fields -> {category, action}, plus
per-model approved/bypass/append_only/primary_key).
"""
from __future__ import annotations

import json
import logging
import re

import pytest

from tests.helpers import load_dag_module

MODULE_NAME = "orthopedic_v2_clean_pipeline"


@pytest.fixture
def module():
    return load_dag_module(MODULE_NAME)


class FakeSnowflakeClient:
    """Records every sf.execute(...) call instead of touching real Snowflake."""

    def __init__(self):
        self.calls: list[dict] = []

    def execute(self, sql: str, label: str | None = None) -> dict:
        self.calls.append({"sql": sql, "label": label})
        return {"rowcount": 1, "sfqid": "fake-sfqid"}

    def call(self, prefix: str) -> dict:
        return next(c for c in self.calls if (c["label"] or "").startswith(prefix))

    def has_call(self, prefix: str) -> bool:
        return any((c["label"] or "").startswith(prefix) for c in self.calls)


class _NullSFContext:
    """Stand-in for `with SnowflakeClient() as sf:` when the test patches
    _clean_one_model_sql itself and doesn't care what `sf` actually is."""

    def __enter__(self):
        return None

    def __exit__(self, *exc):
        return False


def _classification(models: dict) -> str:
    return json.dumps({"generated_at": "2026-07-08T12:05:51Z", "models": models})


# A realistic mix of approved / bypassed / unapproved models, modeled on the
# real pii_classification_v2.json shape.
REALISTIC_MODELS = {
    "admnotes": {
        "namespace": "App\\Models\\Admonotes",
        "fields": {
            "id": {"category": "SYSTEM_META", "action": "KEEP"},
            "pid": {"category": "SYSTEM_META", "action": "KEEP"},
            "description": {"category": "CLINICAL_CONTENT", "action": "KEEP"},
            "notes": {"category": "CLINICAL_CONTENT", "action": "KEEP"},
            "username": {"category": "DIRECT_IDENTIFIER", "action": "HASH"},
        },
        "approved": True,
        "bypass": False,
        "append_only": True,
    },
    "cadex": {
        "namespace": "App\\Models\\Cadex",
        "fields": {
            "id": {"category": "SYSTEM_META", "action": "KEEP"},
            "patname": {"category": "DIRECT_IDENTIFIER", "action": "HASH"},
            "fullname": {"category": "STAFF_IDENTIFIER", "action": "HASH"},
        },
        "approved": True,
        "bypass": False,
        "primary_key": "id",
    },
    "unreviewed_model": {
        "namespace": "App\\Models\\Unreviewed",
        "fields": {
            "id": {"category": "SYSTEM_META", "action": "KEEP"},
            "ssn": {"category": "DIRECT_IDENTIFIER", "action": "HASH"},
        },
        "approved": False,
        "bypass": False,
    },
    "legacy_dump": {
        "namespace": "App\\Models\\LegacyDump",
        "fields": {
            "id": {"category": "SYSTEM_META", "action": "KEEP"},
            "patname": {"category": "DIRECT_IDENTIFIER", "action": "HASH"},
        },
        "approved": False,
        "bypass": True,
    },
}


# ─── DAG structural shape ─────────────────────────────────────────────────

def test_dag_id_schedule_tags(module):
    dag = module.dag
    assert dag.dag_id == "orthopedic_v2_clean_pipeline"
    assert dag.schedule == "@daily"
    assert dag.catchup is False
    assert set(dag.tags) == {"orthopedic", "v2", "transform", "clean", "governance"}


def test_dag_params_documented_defaults(module):
    dag = module.dag
    assert dag.params == {"models": [], "full_refresh": False}


def test_task_chain_ensure_schema_then_gate_then_mapped_clean(module):
    dag = module.dag
    assert set(dag.task_dict) == {"ensure_clean_schema", "governance_gate", "clean_one_model"}

    t_schema = dag.task_dict["ensure_clean_schema"]
    t_gate = dag.task_dict["governance_gate"]
    t_clean = dag.task_dict["clean_one_model"]

    assert t_schema.upstream_task_ids == set()
    assert t_gate.upstream_task_ids == {"ensure_clean_schema"}
    assert t_clean.upstream_task_ids == {"governance_gate"}

    assert t_clean.is_mapped is True
    # dynamically mapped from governance_gate's output (the approved/bypassed job list)
    assert t_clean.mapped_kwargs["op_kwargs"].task is t_gate


# ─── governance_gate: missing / unparseable Variable ──────────────────────

def test_governance_gate_raises_when_variable_missing(module):
    with pytest.raises(RuntimeError, match=module.CLASSIFICATION_VARIABLE):
        module.governance_gate()


def test_governance_gate_raises_when_variable_unparseable(module, set_variables):
    set_variables(**{module.CLASSIFICATION_VARIABLE: "{not valid json"})
    with pytest.raises(RuntimeError, match="Could not parse"):
        module.governance_gate()


def test_governance_gate_raises_when_zero_approved_and_zero_bypassed(module, set_variables):
    models = {
        "unreviewed_model": {
            "fields": {"id": {"category": "SYSTEM_META", "action": "KEEP"}},
            "approved": False,
            "bypass": False,
        },
        "another_unreviewed": {
            "fields": {"id": {"category": "SYSTEM_META", "action": "KEEP"}},
            # no "approved"/"bypass" keys at all
        },
    }
    set_variables(**{module.CLASSIFICATION_VARIABLE: _classification(models)})
    with pytest.raises(RuntimeError, match="No approved models"):
        module.governance_gate()


# ─── governance_gate: realistic approved/bypassed/unapproved split ────────

def test_governance_gate_includes_only_approved_and_bypassed(module, set_variables):
    set_variables(**{module.CLASSIFICATION_VARIABLE: _classification(REALISTIC_MODELS)})
    jobs = module.governance_gate()
    tables = {j["table"] for j in jobs}

    assert tables == {"admnotes", "cadex", "legacy_dump"}
    assert "unreviewed_model" not in tables


def test_governance_gate_marks_bypass_flag_correctly_per_job(module, set_variables):
    set_variables(**{module.CLASSIFICATION_VARIABLE: _classification(REALISTIC_MODELS)})
    jobs = {j["table"]: j for j in module.governance_gate()}

    # legacy_dump: bypass=True, approved=False -> job bypass flag True
    assert jobs["legacy_dump"]["bypass"] is True
    # admnotes/cadex: approved=True -> not a bypass job, even though bypass key is False
    assert jobs["admnotes"]["bypass"] is False
    assert jobs["cadex"]["bypass"] is False


def test_governance_gate_model_cls_passed_through_unchanged(module, set_variables):
    set_variables(**{module.CLASSIFICATION_VARIABLE: _classification(REALISTIC_MODELS)})
    jobs = {j["table"]: j for j in module.governance_gate()}
    assert jobs["cadex"]["model_cls"] == REALISTIC_MODELS["cadex"]


# ─── governance_gate: models DAG param restricts scope ────────────────────

def test_models_param_restricts_scope_to_requested_tables(module, set_variables):
    set_variables(**{module.CLASSIFICATION_VARIABLE: _classification(REALISTIC_MODELS)})
    jobs = module.governance_gate(params={"models": ["admnotes"]})
    assert {j["table"] for j in jobs} == {"admnotes"}


def test_models_param_with_no_matching_tables_raises(module, set_variables):
    set_variables(**{module.CLASSIFICATION_VARIABLE: _classification(REALISTIC_MODELS)})
    with pytest.raises(RuntimeError, match="No matching tables"):
        module.governance_gate(params={"models": ["does_not_exist"]})


# ─── governance_gate: bypass models processed without approval ────────────

def test_bypass_model_processed_without_approved_true(module, set_variables):
    models = {
        "bypass_only": {
            "fields": {"id": {"category": "SYSTEM_META", "action": "KEEP"}},
            "approved": False,
            "bypass": True,
        },
    }
    set_variables(**{module.CLASSIFICATION_VARIABLE: _classification(models)})
    jobs = module.governance_gate()
    assert len(jobs) == 1
    assert jobs[0]["table"] == "bypass_only"
    assert jobs[0]["bypass"] is True


# ─── governance_gate: ORTHOPEDIC_V2_FORCE_UNAPPROVED override ─────────────

def test_force_unapproved_overrides_and_includes_unapproved_models(module, set_variables):
    set_variables(**{
        module.CLASSIFICATION_VARIABLE: _classification(REALISTIC_MODELS),
        module.FORCE_UNAPPROVED_VARIABLE: "true",
    })
    jobs = module.governance_gate()
    tables = {j["table"] for j in jobs}
    # with the override, the previously-excluded unreviewed_model is now included
    assert tables == {"admnotes", "cadex", "legacy_dump", "unreviewed_model"}


@pytest.mark.xfail(
    strict=True,
    reason=(
        "BUG found in dags/orthopedic_v2_clean_pipeline.py governance_gate(): the "
        "module docstring promises 'every use [of ORTHOPEDIC_V2_FORCE_UNAPPROVED] is "
        "logged loudly', but the `if force_unapproved and unapproved:` guard is dead "
        "code. `approved` is computed as "
        "`{t: m for t, m in models.items() if m.get('approved') or m.get('bypass') or "
        "force_unapproved}` -- i.e. force_unapproved already folds every model into "
        "`approved` -- and then `unapproved = set(models) - set(approved)` is computed "
        "AFTER that fold, so `unapproved` is always the empty set whenever "
        "force_unapproved is True, regardless of how many models were actually "
        "unapproved. The dangerous-override warning can therefore never fire. This "
        "test intentionally documents the currently-broken behavior (see report)."
    ),
)
def test_force_unapproved_override_is_logged_loudly(module, set_variables, caplog):
    set_variables(**{
        module.CLASSIFICATION_VARIABLE: _classification(REALISTIC_MODELS),
        module.FORCE_UNAPPROVED_VARIABLE: "true",
    })
    with caplog.at_level(logging.WARNING):
        module.governance_gate()
    assert any(
        "bypasses the governance gate" in record.getMessage()
        for record in caplog.records
    ), "expected a loud warning log when ORTHOPEDIC_V2_FORCE_UNAPPROVED overrides unapproved models"


# ─── _column_expr: HASH / SHA2 pseudonymization ────────────────────────────

def test_column_expr_hash_action_wraps_in_sha2(module):
    expr, col = module._column_expr(
        "patname", {"category": "DIRECT_IDENTIFIER", "action": "HASH"}
    )
    assert col == "patname_hash"
    assert "SHA2(" in expr
    assert "256" in expr
    assert 'payload:"patname"' in expr
    assert expr.endswith('AS "patname_hash"')


def test_column_expr_hash_handles_null_and_empty_string(module):
    expr, _ = module._column_expr(
        "patname", {"category": "DIRECT_IDENTIFIER", "action": "HASH"}
    )
    assert "IS NULL" in expr
    assert "::STRING = ''" in expr
    assert "THEN NULL" in expr


def test_column_expr_bypass_forces_passthrough_even_for_hash_field(module):
    # bypass=True (model-level "bypass": true) must flatten unmasked, even
    # for a field that would otherwise be a DIRECT_IDENTIFIER HASH.
    expr, col = module._column_expr(
        "patname", {"category": "DIRECT_IDENTIFIER", "action": "HASH"}, bypass=True
    )
    assert col == "patname"
    assert "SHA2" not in expr
    assert 'payload:"patname"::STRING AS "patname"' == expr


# ─── _column_expr: note-like CLINICAL_CONTENT fields -> email/phone redaction ─

def test_column_expr_note_like_field_uses_regexp_replace(module):
    expr, col = module._column_expr(
        "notes", {"category": "CLINICAL_CONTENT", "action": "KEEP"}
    )
    assert col == "notes"
    assert "REGEXP_REPLACE" in expr
    assert "[REDACTED_EMAIL]" in expr
    assert "[REDACTED_PHONE]" in expr


@pytest.mark.parametrize(
    "field_name",
    ["notes", "history", "progress_note", "impression", "remarks", "COMMENTS", "Patient_Complaint"],
)
def test_column_expr_note_like_hint_matching_is_case_insensitive_substring(module, field_name):
    expr, _ = module._column_expr(
        field_name, {"category": "CLINICAL_CONTENT", "action": "KEEP"}
    )
    assert "REGEXP_REPLACE" in expr


def test_column_expr_non_note_like_clinical_content_is_plain_passthrough(module):
    expr, col = module._column_expr(
        "description", {"category": "CLINICAL_CONTENT", "action": "KEEP"}
    )
    assert col == "description"
    assert "REGEXP_REPLACE" not in expr
    assert expr == 'payload:"description"::STRING AS "description"'


def test_column_expr_keep_field_passes_through_unmodified(module):
    expr, col = module._column_expr(
        "pid", {"category": "SYSTEM_META", "action": "KEEP"}
    )
    assert col == "pid"
    assert "SHA2" not in expr
    assert "REGEXP_REPLACE" not in expr
    assert expr == 'payload:"pid"::STRING AS "pid"'


def test_note_like_redaction_regex_actually_strips_email_and_contiguous_phone(module):
    """Pull the literal regex patterns baked into the generated SQL string
    and exercise them directly against realistic PII strings, the same way
    Snowflake's REGEXP_REPLACE would evaluate them."""
    expr, _ = module._column_expr(
        "notes", {"category": "CLINICAL_CONTENT", "action": "KEEP"}
    )
    assert f"'{module._EMAIL_RX}'" in expr
    assert f"'{module._PHONE_RX}'" in expr

    sample = "Reach patient at foo@bar.com or +254712345678 for follow-up."
    redacted = re.sub(module._EMAIL_RX, "[REDACTED_EMAIL]", sample)
    redacted = re.sub(module._PHONE_RX, "[REDACTED_PHONE]", redacted)

    assert "foo@bar.com" not in redacted
    assert "+254712345678" not in redacted
    assert "[REDACTED_EMAIL]" in redacted
    assert "[REDACTED_PHONE]" in redacted


@pytest.mark.xfail(
    strict=True,
    reason=(
        "PII LEAK RISK found in dags/orthopedic_v2_clean_pipeline.py: _PHONE_RX = "
        r"r'([+]254|0)[0-9]{9}|[+]?[0-9]{10,13}' only matches CONTIGUOUS digit runs. "
        "A hand-typed phone number using separators -- e.g. '0712-345-678' -- is NOT "
        "matched by either alternative (the hyphens break the [0-9]{9} / [0-9]{10,13} "
        "runs), so it passes through the note-like defense-in-depth redaction pass "
        "completely unredacted into HOSPITALS.ORTHOPEDIC_CLEAN_V2. This test documents "
        "that gap directly against the real regex (see report)."
    ),
)
def test_note_like_redaction_regex_strips_hyphenated_phone_numbers(module):
    sample = "Call back on 0712-345-678 please."
    redacted = re.sub(module._PHONE_RX, "[REDACTED_PHONE]", sample)
    assert "0712-345-678" not in redacted


# ─── _quote_ident / _table_fqn ─────────────────────────────────────────────

def test_quote_ident_escapes_embedded_double_quotes(module):
    assert module._quote_ident('foo"bar') == '"foo""bar"'
    assert module._quote_ident("plain") == '"plain"'


def test_table_fqn_uppercases_table_name(module):
    fqn = module._table_fqn(module.SF_RAW_SCHEMA, "admnotes")
    assert fqn == f"{module.SF_DB}.ORTHOPEDIC_RAW_V2.ADMNOTES"


# ─── ensure_clean_table ────────────────────────────────────────────────────

def test_ensure_clean_table_creates_and_evolves_schema(module):
    sf = FakeSnowflakeClient()
    module.ensure_clean_table(sf, "admnotes", ["id", "description", "notes"])

    create_call = sf.call("ensure_clean:")
    assert "CREATE TABLE IF NOT EXISTS" in create_call["sql"]
    assert '"id" STRING' in create_call["sql"]
    assert "_clean_processed_at" in create_call["sql"]

    evolve_labels = [c["label"] for c in sf.calls if c["label"].startswith("evolve:")]
    assert evolve_labels == ["evolve:admnotes.id", "evolve:admnotes.description", "evolve:admnotes.notes"]
    for c in sf.calls:
        if c["label"].startswith("evolve:"):
            assert "ALTER TABLE" in c["sql"]
            assert "ADD COLUMN IF NOT EXISTS" in c["sql"]


# ─── _clean_one_model_sql: append_only vs MERGE/QUALIFY dedup ─────────────

def test_no_fields_short_circuits_as_skipped(module):
    sf = FakeSnowflakeClient()
    result = module._clean_one_model_sql(
        sf, "empty_model", {"fields": {}}, since="1970-01-01T00:00:00Z", full_refresh=False
    )
    assert result == {"table": "empty_model", "status": "skipped_no_fields"}
    assert sf.calls == []


def test_append_only_full_refresh_truncates_then_inserts(module):
    sf = FakeSnowflakeClient()
    model_cls = {
        "append_only": True,
        "fields": {
            "id": {"category": "SYSTEM_META", "action": "KEEP"},
            "description": {"category": "CLINICAL_CONTENT", "action": "KEEP"},
        },
    }
    result = module._clean_one_model_sql(
        sf, "admnotes", model_cls, since="1970-01-01T00:00:00Z", full_refresh=True
    )
    assert result["status"] == "ok"
    assert sf.has_call("truncate:")
    assert sf.has_call("append:")
    assert not sf.has_call("merge:")

    truncate_call = sf.call("truncate:")
    assert "TRUNCATE TABLE IF EXISTS" in truncate_call["sql"]

    insert_call = sf.call("append:")
    assert "INSERT INTO" in insert_call["sql"]
    assert "MERGE" not in insert_call["sql"]
    assert "QUALIFY" not in insert_call["sql"]
    # incremental WHERE clause must be absent on a full refresh
    assert "WHERE _ingested_at" not in insert_call["sql"]


def test_append_only_incremental_does_not_truncate_and_filters_by_watermark(module):
    sf = FakeSnowflakeClient()
    model_cls = {
        "append_only": True,
        "fields": {"id": {"category": "SYSTEM_META", "action": "KEEP"}},
    }
    result = module._clean_one_model_sql(
        sf, "admnotes", model_cls, since="2024-06-01T00:00:00Z", full_refresh=False
    )
    assert result["status"] == "ok"
    assert not sf.has_call("truncate:")
    assert sf.has_call("append:")

    insert_call = sf.call("append:")
    assert "WHERE _ingested_at > '2024-06-01T00:00:00Z'" in insert_call["sql"]


def test_default_merge_path_uses_qualify_row_number_dedup_on_primary_key(module):
    sf = FakeSnowflakeClient()
    model_cls = {
        "primary_key": "pid",
        "fields": {
            "pid": {"category": "SYSTEM_META", "action": "KEEP"},
            "patname": {"category": "DIRECT_IDENTIFIER", "action": "HASH"},
        },
    }
    result = module._clean_one_model_sql(
        sf, "cadex", model_cls, since="2024-01-01T00:00:00Z", full_refresh=False
    )
    assert result["status"] == "ok"
    assert not sf.has_call("truncate:")
    assert not sf.has_call("append:")
    assert sf.has_call("merge:")

    merge_call = sf.call("merge:")
    sql = merge_call["sql"]
    assert "MERGE INTO" in sql
    assert "QUALIFY ROW_NUMBER() OVER (" in sql
    # dedup partitions by the model's classified primary key
    assert 'PARTITION BY payload:"pid"' in sql
    # the MERGE ON-clause keys on that same clean column
    assert 'ON tgt."pid" = src."pid"' in sql
    assert "WHERE _ingested_at > '2024-01-01T00:00:00Z'" in sql


def test_default_merge_path_full_refresh_has_no_where_but_still_merges(module):
    sf = FakeSnowflakeClient()
    model_cls = {
        "primary_key": "id",
        "fields": {"id": {"category": "SYSTEM_META", "action": "KEEP"}},
    }
    module._clean_one_model_sql(
        sf, "cadex", model_cls, since="1970-01-01T00:00:00Z", full_refresh=True
    )
    assert not sf.has_call("truncate:")
    merge_call = sf.call("merge:")
    assert "WHERE _ingested_at" not in merge_call["sql"]
    assert "QUALIFY ROW_NUMBER()" in merge_call["sql"]


def test_merge_path_auto_injects_missing_primary_key_field(module):
    """If the classifier folded the PK into SYSTEM_META and it's not a
    distinct 'fields' entry, _clean_one_model_sql must still project +
    dedup + merge on it rather than silently dropping the key."""
    sf = FakeSnowflakeClient()
    model_cls = {
        "primary_key": "pid",
        "fields": {
            "patname": {"category": "DIRECT_IDENTIFIER", "action": "HASH"},
        },
    }
    module._clean_one_model_sql(
        sf, "cadex", model_cls, since="2024-01-01T00:00:00Z", full_refresh=False
    )
    merge_call = sf.call("merge:")
    assert '"pid"' in merge_call["sql"]
    assert 'ON tgt."pid" = src."pid"' in merge_call["sql"]


def test_merge_path_uses_default_primary_key_when_unspecified(module):
    sf = FakeSnowflakeClient()
    model_cls = {
        # no "primary_key" key at all -> DEFAULT_PRIMARY_KEY ("id")
        "fields": {"id": {"category": "SYSTEM_META", "action": "KEEP"}},
    }
    module._clean_one_model_sql(
        sf, "some_table", model_cls, since="2024-01-01T00:00:00Z", full_refresh=False
    )
    merge_call = sf.call("merge:")
    assert 'ON tgt."id" = src."id"' in merge_call["sql"]
    assert module.DEFAULT_PRIMARY_KEY == "id"


def test_bypass_flag_propagates_through_clean_one_model_sql_to_column_expr(module):
    """A model with bypass=True must flatten unmasked end-to-end, including
    through the MERGE SQL builder, not just _column_expr in isolation."""
    sf = FakeSnowflakeClient()
    model_cls = {
        "primary_key": "id",
        "fields": {
            "id": {"category": "SYSTEM_META", "action": "KEEP"},
            "patname": {"category": "DIRECT_IDENTIFIER", "action": "HASH"},
        },
    }
    module._clean_one_model_sql(
        sf, "legacy_dump", model_cls, since="2024-01-01T00:00:00Z", full_refresh=False, bypass=True
    )
    merge_call = sf.call("merge:")
    assert "SHA2" not in merge_call["sql"]
    assert '"patname"' in merge_call["sql"]
    assert '"patname_hash"' not in merge_call["sql"]


# ─── clean_one_model task wiring: watermark + full_refresh + bypass ───────

def test_clean_one_model_task_uses_stored_watermark_when_not_full_refresh(module, monkeypatch, set_variables):
    captured = {}

    def fake_sql(sf, table, model_cls, *, since, full_refresh, bypass=False):
        captured.update(table=table, since=since, full_refresh=full_refresh, bypass=bypass)
        return {"table": table, "status": "ok"}

    monkeypatch.setattr(module, "_clean_one_model_sql", fake_sql)
    monkeypatch.setattr(module, "SnowflakeClient", lambda *a, **k: _NullSFContext())
    set_variables(**{"orthopedic_v2_clean_wm__admnotes": "2024-05-01T00:00:00Z"})

    result = module.clean_one_model("admnotes", {"fields": {}}, False, params={"full_refresh": False})

    assert result == {"table": "admnotes", "status": "ok"}
    assert captured == {
        "table": "admnotes", "since": "2024-05-01T00:00:00Z", "full_refresh": False, "bypass": False,
    }


def test_clean_one_model_task_full_refresh_ignores_watermark(module, monkeypatch, set_variables):
    captured = {}

    def fake_sql(sf, table, model_cls, *, since, full_refresh, bypass=False):
        captured.update(since=since, full_refresh=full_refresh)
        return {"table": table, "status": "ok"}

    monkeypatch.setattr(module, "_clean_one_model_sql", fake_sql)
    monkeypatch.setattr(module, "SnowflakeClient", lambda *a, **k: _NullSFContext())
    set_variables(**{"orthopedic_v2_clean_wm__admnotes": "2024-05-01T00:00:00Z"})

    module.clean_one_model("admnotes", {"fields": {}}, False, params={"full_refresh": True})

    assert captured["since"] == "1970-01-01T00:00:00Z"
    assert captured["full_refresh"] is True


def test_clean_one_model_task_propagates_bypass_flag(module, monkeypatch):
    captured = {}

    def fake_sql(sf, table, model_cls, *, since, full_refresh, bypass=False):
        captured["bypass"] = bypass
        return {"table": table, "status": "ok"}

    monkeypatch.setattr(module, "_clean_one_model_sql", fake_sql)
    monkeypatch.setattr(module, "SnowflakeClient", lambda *a, **k: _NullSFContext())

    module.clean_one_model("legacy_dump", {"fields": {}}, True, params={})

    assert captured["bypass"] is True
