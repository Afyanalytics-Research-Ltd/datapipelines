# dags/orthopedic_v2_clean_pipeline.py
"""
orthopedic_v2_clean_pipeline — HOSPITALS.ORTHOPEDIC_RAW_V2 → HOSPITALS.ORTHOPEDIC_CLEAN_V2

Flattens, de-identifies, and deduplicates the 19 raw model tables landed by
orthopedic_v2_raw_pipeline into an analyst-facing CLEAN schema. This is the
Airflow port of the standalone orthopedic_v2_clean_pipeline.py script; the
flatten/mask/dedup SQL below is copied verbatim from that script (it is
compliance-sensitive — do not "simplify" the masking expressions).

WHAT THIS DAG DOES
  1. governance_gate reads the governance-approved PII classification (see
     GOVERNANCE below) and refuses to touch any model that isn't
     "approved": true, unless the model itself has "bypass": true, or the
     ORTHOPEDIC_V2_FORCE_UNAPPROVED Variable is set to "true" (dangerous —
     see below). If ZERO models are approved/bypassed, this task raises and
     the DAG run fails outright — the compliance gate is never silently
     skipped.
  2. For each approved (or bypassed) model, clean_one_model:
       a. Ensures HOSPITALS.ORTHOPEDIC_CLEAN_V2.<table> exists with one
          column per classified field (adding new columns as schema evolves).
       b. Reads new/changed rows from HOSPITALS.ORTHOPEDIC_RAW_V2.<table>
          (payload VARIANT) since the per-table clean watermark.
       c. DIRECT_IDENTIFIER fields -> SHA2-256 pseudonymized into
          "<field>_hash" (irreversible; the same source value always hashes
          the same way, so joins/counts across records for the same person
          still work without exposing the identifier itself).
       d. Free-text CLINICAL_CONTENT fields that commonly carry hand-typed
          identifiers (notes, history, impression, progress, remarks, ...)
          get a defense-in-depth REGEXP_REPLACE pass that redacts embedded
          phone numbers / emails, on top of keeping the clinical content.
       e. QUASI_IDENTIFIER / STAFF_IDENTIFIER / SYSTEM_META / other
          CLINICAL_CONTENT fields are kept as-is.
       f. QUALIFY ROW_NUMBER() ... = 1 picks only the LATEST raw snapshot per
          business key (id) before merging, and the MERGE itself is keyed on
          that same id — so re-running never creates duplicate rows in the
          clean table, and reprocessing after a correction upstream simply
          updates the existing row instead of appending a new one.
  3. Advances a per-table Airflow Variable watermark only after that table's
     MERGE/INSERT succeeds.

GOVERNANCE (design note — read before changing this DAG)
  The standalone script gates on a local file, pii_classification_v2.json,
  written/updated by orthopedic_v2_schema_discovery.py (NOT converted to a
  DAG — it stays a manual, human-in-the-loop script; see its own docstring)
  and then hand-reviewed by a human governance owner who flips "approved":
  true per model (or sets "bypass": true as a per-model escape hatch).

  Only ./dags is mounted into the Airflow containers (see docker-compose.yaml
  volumes), so this DAG cannot read that repo-root file at run time. Instead,
  the governance artifact lives in the Airflow Variable
  ORTHOPEDIC_V2_PII_CLASSIFICATION, which must be populated/updated by a
  human as follows:
    1. Run orthopedic_v2_schema_discovery.py locally (or wherever it has
       network access to the gateway API).
    2. Review pii_classification_v2.json / governance_report_v2.md, correct
       any misclassified fields, mark models "approved": true (or "bypass":
       true) per the same rules documented in that script.
    3. Paste the full resulting JSON document as the value of the
       ORTHOPEDIC_V2_PII_CLASSIFICATION Airflow Variable.
  governance_gate re-derives the exact same approved/unapproved/bypassed
  split the standalone script computed from the file, every run, and FAILS
  the DAG run (raises) if the Variable is missing/unparseable/empty or if no
  model in it is approved or bypassed — the compliance check is never
  bypassed silently, only ever via the explicit, logged
  ORTHOPEDIC_V2_FORCE_UNAPPROVED override below.

  - HOSPITALS.ORTHOPEDIC_CLEAN_V2 is the layer meant for analyst/BI access.
  - Direct identifiers never appear in plaintext in this schema — UNLESS a
    model has "bypass": true in the classification Variable. Per-model escape
    hatch for when "approved": true isn't set yet but you want that model's
    data flowing into CLEAN_V2 now anyway: every field is flattened straight
    from RAW, unmasked, including fields that would otherwise be HASHed as
    DIRECT_IDENTIFIER. Ignored if the model also has "approved": true
    (approved models always get proper per-field masking).
  - "append_only": true on a model skips the id-based dedup/MERGE entirely
    and just INSERTs matching rows straight from RAW — for tables where no
    field is actually unique per record, so there's no reliable key to merge
    on. full_refresh (DAG param) truncates first (else every rerun would
    duplicate all rows); incremental runs only ever append rows newer than
    the watermark.
  - patient_id-style foreign keys are NOT hashed here (they're the join key
    across clean tables) — but note that a facility_id + patient_id + a
    quasi-identifier (e.g. DOB) is, in aggregate, a linkage risk. Recommend
    Snowflake row/column access policies on ORTHOPEDIC_CLEAN_V2 in addition
    to this DAG's masking, per your org's data governance policy.

AIRFLOW VARIABLES REQUIRED
  ORTHOPEDIC_V2_PII_CLASSIFICATION   JSON document — see GOVERNANCE above.
                                      Same shape as pii_classification_v2.json
                                      ({"models": {<table>: {"fields": {...},
                                      "approved": bool, "bypass": bool,
                                      "append_only": bool, "primary_key": str}}}).

AIRFLOW VARIABLES OPTIONAL
  ORTHOPEDIC_V2_FORCE_UNAPPROVED     "true"/"false" (default "false"). DANGEROUS:
                                      process models even if approved=false.
                                      Still masks per each field's classified
                                      action — set "bypass": true per-model in
                                      the classification instead to flatten
                                      unmasked. Every use is logged loudly.
  orthopedic_v2_clean_wm__<table>    Per-table watermark, managed automatically
                                      by this DAG — do not set by hand.

DAG PARAMS (override at manual trigger time)
  models         list[str] | []   Restrict to specific tables. Empty = all
                                   approved/bypassed models.
  full_refresh   bool             Reprocess all raw history instead of just
                                   rows newer than the watermark. Default False.

ENV VARS — same Snowflake vars as orthopedic_v2_raw_pipeline
  SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
  SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD (provided to the Airflow
  containers via the compose env_file — no .env loading needed here).

Runs @daily, intended to run after orthopedic_v2_raw_pipeline has landed
that day's RAW rows (trigger manually / chain via a sensor or TriggerDagRun
operator if strict ordering is required; not wired up here to keep this DAG's
own diff self-contained).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import snowflake.connector

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

DAG_ID = "orthopedic_v2_clean_pipeline"

# ─── CONFIG ──────────────────────────────────────────────────────────────────

SF_DB           = os.getenv("SNOWFLAKE_DATABASE", "HOSPITALS")
SF_RAW_SCHEMA   = "ORTHOPEDIC_RAW_V2"
SF_CLEAN_SCHEMA = "ORTHOPEDIC_CLEAN_V2"

CLASSIFICATION_VARIABLE = "ORTHOPEDIC_V2_PII_CLASSIFICATION"
FORCE_UNAPPROVED_VARIABLE = "ORTHOPEDIC_V2_FORCE_UNAPPROVED"

# Fields commonly holding hand-typed free text where a phone/email could be
# embedded even though the column itself is clinical content, not a contact
# field. Matched against the field name (case-insensitive substring).
NOTE_LIKE_FIELD_HINTS = (
    "note", "impression", "history", "progress", "remark", "comment",
    "complaint", "finding", "instruction", "summary",
)

# Regexes used for the defense-in-depth in-text redaction pass.
# NOTE: deliberately backslash-free. A backslash-escaped version of these
# (e.g. \+, \., \b) broke in transit to Snowflake — REGEXP_REPLACE received
# the pattern with backslashes silently stripped, turning "\+?" into a bare
# "+?" with nothing to repeat ("no argument for repetition operator: +").
# Character classes give the same matching power without needing any
# backslash escapes, so there's nothing left that can be mangled.
_EMAIL_RX = r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}'
_PHONE_RX = r'([+]254|0)[0-9]{9}|[+]?[0-9]{10,13}'

# Default assumed primary key per model (Laravel convention). Override per
# model via `"primary_key": "..."` inside the classification Variable if a
# table's real PK differs.
DEFAULT_PRIMARY_KEY = "id"

# ─── SNOWFLAKE CLIENT (same shape as the standalone v2 scripts) ─────────────

class SnowflakeClient:
    def __init__(self, schema: str = SF_CLEAN_SCHEMA):
        user      = os.getenv("SNOWFLAKE_USER", "").strip()
        account   = os.getenv("SNOWFLAKE_ACCOUNT", "").strip()
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "").strip()
        database  = os.getenv("SNOWFLAKE_DATABASE", SF_DB).strip()
        key_path  = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()
        password  = os.getenv("SNOWFLAKE_PASSWORD", "").strip()

        for name, val in [
            ("SNOWFLAKE_USER", user), ("SNOWFLAKE_ACCOUNT", account),
            ("SNOWFLAKE_WAREHOUSE", warehouse), ("SNOWFLAKE_DATABASE", database),
        ]:
            if not val:
                raise RuntimeError(f"Missing env var {name}.")
        if not key_path and not password:
            raise RuntimeError(
                "Snowflake auth: set SNOWFLAKE_PRIVATE_KEY_PATH (key-pair) "
                "or SNOWFLAKE_PASSWORD."
            )

        kwargs: dict = dict(user=user, account=account, warehouse=warehouse,
                            database=database, schema=schema)
        if key_path:
            if not Path(key_path).exists():
                raise RuntimeError(f"Snowflake private key not found: {key_path}")
            kwargs["private_key_file"] = key_path
        else:
            kwargs["password"] = password

        try:
            self._conn = snowflake.connector.connect(**kwargs)
        except Exception as e:
            raise RuntimeError(f"Snowflake connection failed — account={account} user={user}. Cause: {e}") from e

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    @contextmanager
    def _cursor(self):
        cur = self._conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def execute(self, sql: str, label: str | None = None) -> dict:
        label = label or f"x:{hashlib.md5(sql.encode()).hexdigest()[:8]}"
        log.info("▶ %-35s | %s…", label, " ".join(sql.split())[:140])
        t0 = time.perf_counter()
        try:
            with self._cursor() as cur:
                cur.execute(sql)
                rowcount, sfqid = cur.rowcount, cur.sfqid
            log.info("✓ %-35s rowcount=%s · %.2fs", label, rowcount, time.perf_counter() - t0)
            return {"rowcount": rowcount, "sfqid": sfqid}
        except Exception as e:
            log.exception("✗ %-35s FAILED · %s", label, e)
            raise

    def __enter__(self): return self
    def __exit__(self, *_): self.close()

# ─── COLUMN-EXPRESSION BUILDER (copied verbatim from the standalone script) ──

def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def _column_expr(field: str, info: dict, bypass: bool = False) -> tuple[str, str]:
    """Return (select_expr, clean_column_name) for one classified field.

    bypass=True forces a raw passthrough regardless of the field's classified
    action/category — used for models with "bypass": true in the
    classification, which flatten straight into CLEAN_V2 unmasked (including
    DIRECT_IDENTIFIER fields that would otherwise be hashed)."""
    action   = "KEEP" if bypass else info.get("action", "HASH")
    category = info.get("category", "UNKNOWN")
    src      = f'payload:{_quote_ident(field)}'

    if action == "HASH":
        col = f"{field}_hash"
        expr = (
            f"CASE WHEN {src} IS NULL OR {src}::STRING = '' THEN NULL "
            f"ELSE SHA2({src}::STRING, 256) END AS {_quote_ident(col)}"
        )
        return expr, col

    col = field
    is_note_like = (not bypass) and category == "CLINICAL_CONTENT" and any(
        hint in field.lower() for hint in NOTE_LIKE_FIELD_HINTS
    )
    if is_note_like:
        # Defense-in-depth: keep the clinical text, but scrub embedded
        # emails/phone numbers a staff member may have hand-typed into it.
        expr = (
            f"REGEXP_REPLACE(REGEXP_REPLACE({src}::STRING, "
            f"'{_EMAIL_RX}', '[REDACTED_EMAIL]'), "
            f"'{_PHONE_RX}', '[REDACTED_PHONE]') AS {_quote_ident(col)}"
        )
    else:
        expr = f"{src}::STRING AS {_quote_ident(col)}"
    return expr, col

# ─── SCHEMA BOOTSTRAP (CLEAN SIDE) ────────────────────────────────────────────

def _table_fqn(schema: str, table: str) -> str:
    return f"{SF_DB}.{schema}.{table.upper()}"

def ensure_clean_table(sf: SnowflakeClient, table: str, columns: list[str]) -> None:
    fqn = _table_fqn(SF_CLEAN_SCHEMA, table)
    col_defs = ",\n                ".join(f"{_quote_ident(c)} STRING" for c in columns)
    sf.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            {col_defs},
            _source_run_id      VARCHAR,
            _raw_ingested_at    TIMESTAMP_TZ,
            _clean_processed_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """,
        label=f"ensure_clean:{table}",
    )
    # Schema evolution: add any newly-classified columns to an existing table.
    for c in columns:
        try:
            sf.execute(
                f'ALTER TABLE {fqn} ADD COLUMN IF NOT EXISTS {_quote_ident(c)} STRING;',
                label=f"evolve:{table}.{c}",
            )
        except Exception as e:
            log.warning("Could not add column %s to %s (may already exist): %s", c, fqn, e)

# ─── WATERMARKS (Airflow Variable, replaces .orthopedic_v2_clean_watermarks.json) ─

def _wm_key(table: str) -> str:
    return f"orthopedic_v2_clean_wm__{table}"

def get_watermark(table: str, default: str = "1970-01-01T00:00:00Z") -> str:
    return Variable.get(_wm_key(table), default_var=default)

def set_watermark(table: str, ts_iso: str) -> None:
    Variable.set(_wm_key(table), ts_iso)
    log.info("Clean watermark [%s] → %s", table, ts_iso)

# ─── CLEAN ONE MODEL (flatten + mask + dedup MERGE) ──────────────────────────

def _clean_one_model_sql(
    sf: SnowflakeClient,
    table: str,
    model_cls: dict,
    *,
    since: str,
    full_refresh: bool,
    bypass: bool = False,
) -> dict:
    fields: dict = model_cls.get("fields", {})
    if not fields:
        return {"table": table, "status": "skipped_no_fields"}

    append_only = bool(model_cls.get("append_only"))

    if not append_only:
        primary_key = model_cls.get("primary_key", DEFAULT_PRIMARY_KEY)
        if primary_key not in fields:
            # Always project the PK even if the classifier didn't see it as a
            # distinct "field" entry (e.g. it was folded under SYSTEM_META already).
            fields = {primary_key: {"category": "SYSTEM_META", "action": "KEEP"}, **fields}

    select_exprs, clean_columns = [], []
    pk_clean_col = None
    for fname, info in fields.items():
        expr, col = _column_expr(fname, info, bypass=bypass)
        select_exprs.append(expr)
        clean_columns.append(col)
        if not append_only and fname == primary_key:
            pk_clean_col = col  # PK is never hashed in practice, but stay generic

    raw_fqn   = _table_fqn(SF_RAW_SCHEMA, table)
    clean_fqn = _table_fqn(SF_CLEAN_SCHEMA, table)

    ensure_clean_table(sf, table, clean_columns)

    where_clause = "" if full_refresh else f"WHERE _ingested_at > '{since}'"

    insert_cols_sql = ", ".join(_quote_ident(c) for c in clean_columns) + \
        ", _source_run_id, _raw_ingested_at"

    if append_only:
        # No field is reliably unique per record, so there's no key to
        # dedup/merge on — just append. full_refresh truncates first so
        # re-running doesn't duplicate every row; incremental runs only ever
        # select rows newer than the watermark, so appending is safe there
        # without truncating.
        insert_sql = f"""
        INSERT INTO {clean_fqn} ({insert_cols_sql})
        SELECT
          {", ".join(select_exprs)},
          _run_id      AS _source_run_id,
          _ingested_at AS _raw_ingested_at
        FROM {raw_fqn}
        {where_clause};
        """
        if full_refresh:
            sf.execute(f"TRUNCATE TABLE IF EXISTS {clean_fqn};", label=f"truncate:{table}")
        result = sf.execute(insert_sql, label=f"append:{table}")
        set_watermark(table, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
        return {"table": table, "status": "ok", "rowcount": result.get("rowcount")}

    src_sql = f"""
    (
      SELECT
        {", ".join(select_exprs)},
        _run_id           AS _source_run_id,
        _ingested_at      AS _raw_ingested_at
      FROM {raw_fqn}
      {where_clause}
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payload:{_quote_ident(primary_key)}
        ORDER BY _ingested_at DESC
      ) = 1
    )
    """

    update_set = ", ".join(
        f"tgt.{_quote_ident(c)} = src.{_quote_ident(c)}" for c in clean_columns if c != pk_clean_col
    )
    update_set += (
        f", tgt._source_run_id = src._source_run_id"
        f", tgt._raw_ingested_at = src._raw_ingested_at"
        f", tgt._clean_processed_at = CURRENT_TIMESTAMP()"
    )
    # NOTE: _source_run_id / _raw_ingested_at were created UNQUOTED in
    # ensure_clean_table (so Snowflake normalized them to uppercase). They
    # must stay unquoted here too — quoting them would make Snowflake look
    # for a case-sensitive "_source_run_id" column that doesn't exist and
    # fail with "invalid identifier". Only the user-defined clean_columns
    # (arbitrary/mixed-case field names) need _quote_ident.
    insert_vals_sql = ", ".join(f"src.{_quote_ident(c)}" for c in clean_columns) + \
        ", src._source_run_id, src._raw_ingested_at"

    merge_sql = f"""
    MERGE INTO {clean_fqn} AS tgt
    USING {src_sql} AS src
    ON tgt.{_quote_ident(pk_clean_col)} = src.{_quote_ident(pk_clean_col)}
    WHEN MATCHED THEN UPDATE SET {update_set}
    WHEN NOT MATCHED THEN INSERT ({insert_cols_sql})
      VALUES ({insert_vals_sql});
    """

    result = sf.execute(merge_sql, label=f"merge:{table}")
    set_watermark(table, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    return {"table": table, "status": "ok", "rowcount": result.get("rowcount")}

# ─── DAG TASK CALLABLES ───────────────────────────────────────────────────────

def ensure_clean_schema(**context) -> None:
    with SnowflakeClient() as sf:
        sf.execute(f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_CLEAN_SCHEMA};", label="ensure_clean_schema")
    log.info("Ensured schema %s.%s", SF_DB, SF_CLEAN_SCHEMA)


def governance_gate(**context) -> list[dict]:
    """The compliance gate. Reads the human-approved PII classification from
    the ORTHOPEDIC_V2_PII_CLASSIFICATION Variable, re-derives the same
    approved/unapproved/bypassed split the standalone script computed from
    pii_classification_v2.json, and FAILS the DAG run (raises) if the
    Variable is missing/unparseable or if no model in it is approved or
    bypassed. See the module docstring GOVERNANCE section for the full design
    rationale and how a human populates this Variable."""
    raw = Variable.get(CLASSIFICATION_VARIABLE, default_var=None)
    if not raw:
        raise RuntimeError(
            f"Airflow Variable {CLASSIFICATION_VARIABLE} not found. This is the "
            "governance gate — run orthopedic_v2_schema_discovery.py, review the "
            "suggested field classification, mark models \"approved\": true, then "
            "paste the resulting JSON into that Variable before this DAG can run."
        )
    try:
        classification = json.loads(raw)
    except Exception as e:
        raise RuntimeError(f"Could not parse {CLASSIFICATION_VARIABLE} as JSON: {e}") from e

    models: dict = classification.get("models", {})

    params = context.get("params") or {}
    only_tables = params.get("models") or []
    if only_tables:
        wanted = {t.lower() for t in only_tables}
        models = {t: m for t, m in models.items() if t in wanted}
        if not models:
            raise RuntimeError(f"No matching tables in {CLASSIFICATION_VARIABLE} for params.models={only_tables}")

    force_unapproved = Variable.get(FORCE_UNAPPROVED_VARIABLE, default_var="false").strip().lower() == "true"

    approved   = {t: m for t, m in models.items() if m.get("approved") or m.get("bypass") or force_unapproved}
    bypassed   = sorted(t for t, m in approved.items() if m.get("bypass") and not m.get("approved"))
    unapproved = sorted(set(models) - set(approved))

    if unapproved:
        log.warning(
            "SKIPPING %d unapproved model(s) (set \"approved\": true, or \"bypass\": true to flatten "
            "unmasked, in %s after review): %s",
            len(unapproved), CLASSIFICATION_VARIABLE, ", ".join(unapproved),
        )
    if bypassed:
        log.warning(
            "BYPASS: %d model(s) have \"bypass\": true — flattening RAW straight into CLEAN_V2 "
            "UNMASKED (identities included, no hashing, no redaction) despite not being approved: %s",
            len(bypassed), ", ".join(bypassed),
        )
    if force_unapproved and unapproved:
        log.warning(
            "%s=true: processing %d otherwise-unapproved model(s) anyway. "
            "This bypasses the governance gate.", FORCE_UNAPPROVED_VARIABLE, len(unapproved),
        )

    if not approved:
        raise RuntimeError(
            "No approved models to process. Run orthopedic_v2_schema_discovery.py, "
            f"review and update {CLASSIFICATION_VARIABLE}, approve models (or set "
            "\"bypass\": true on specific ones), then re-run this DAG."
        )

    log.info("Governance gate passed — %d model(s) cleared to process.", len(approved))

    jobs = []
    for table, model_cls in approved.items():
        model_bypass = bool(model_cls.get("bypass")) and not model_cls.get("approved")
        jobs.append({"table": table, "model_cls": model_cls, "bypass": model_bypass})
    return jobs


def clean_one_model(table: str, model_cls: dict, bypass: bool, **context) -> dict:
    params = context.get("params") or {}
    full_refresh = bool(params.get("full_refresh", False))
    since = "1970-01-01T00:00:00Z" if full_refresh else get_watermark(table)

    with SnowflakeClient() as sf:
        return _clean_one_model_sql(
            sf, table, model_cls,
            since=since, full_refresh=full_refresh, bypass=bypass,
        )

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────

default_args = {
    "owner":       "airflow",
    "start_date":  datetime(2025, 1, 1),
    "retries":     2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    max_active_tasks=8,
    tags=["orthopedic", "v2", "transform", "clean", "governance"],
    params={"models": [], "full_refresh": False},
) as dag:

    t_schema = PythonOperator(
        task_id="ensure_clean_schema",
        python_callable=ensure_clean_schema,
    )
    t_gate = PythonOperator(
        task_id="governance_gate",
        python_callable=governance_gate,
    )
    t_clean = PythonOperator.partial(
        task_id="clean_one_model",
        python_callable=clean_one_model,
    ).expand(op_kwargs=t_gate.output)

    t_schema >> t_gate >> t_clean
