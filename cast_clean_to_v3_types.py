#!/usr/bin/env python3
"""
cast_clean_to_v3_types.py - standardise, cast and check columns against V3's types.

It is meant to be called while <FACILITY>_CLEAN is being built (by the flatten step), so CLEAN holds the
final, V3-typed data. It changes nothing in CLEAN itself: it reads, checks, reports and FAILS. What it reports
(and the state of a run) is stored in Snowflake, in HOSPITALS.V3_CAST_AUDIT (see AUDIT STORE below).
Today it reads the existing CLEAN tables so it can be tested before it is hooked into the flatten step.

The plan (in order):
  0. choose_tables      which tables to run on (from the flatten DAG's conf, or CLEAN itself)
  1. get_v3_structure   V3 column types / lengths / nullability (v3-schema.json), or provisional guesses
  2. standardise        '34,5' -> '34.5', blanks and 'N/A' -> empty, dates into one format
  3. compare_and_cast   cast each TEXT column to its V3 type; fill defaults; find records with errors
  4. verify             per column: values before / after, failed casts, defaulted, records with errors
  5. fail               a table with any record with an error FAILS (TypingErrors); other tables carry on

USAGE
  # check one table of WEMA_MEDICAL (read-only, nothing is created):
  python cast_clean_to_v3_types.py --facility WEMA_MEDICAL --table vitals

  # every CLEAN table of the facility:
  python cast_clean_to_v3_types.py --facility WEMA_MEDICAL

  # just see the SQL it would run:
  python cast_clean_to_v3_types.py --facility WEMA_MEDICAL --table vitals --print-sql

OUTPUT, in Snowflake, schema HOSPITALS.V3_CAST_AUDIT (created on first use; set V3_AUDIT_SCHEMA to use another):
  RUNS           one row per run: facility, schema pair, start/finish, status, RAW row count, dedup status
  TABLE_STATE    one row per table per run: ok / failed / running / pending, the reason, the object built
  COLUMN_REPORT  one row per column: what was converted, defaulted, filled, failed
  ERRORS         records with errors: table, record id, why, original values. A table with errors makes the script fail.
  CHANGES        audit list: every value changed or left empty, and every record skipped (a record it points to is
                 missing), with the original, the new value and what happened
  LOG            what the run printed, line by line
Every row has the RUN_ID. ERRORS, CHANGES, COLUMN_REPORT and LOG hold the original values of real records, so they are
deleted after V3_AUDIT_KEEP_DAYS (default 90). No files are written. Needs a role that can CREATE SCHEMA in HOSPITALS
(or an existing schema it can write to).

V3 STRUCTURE: dags/jsons/v3-schema.json (or --structure <path>), the AI-mapper export (tables, defaults, models.required).
It knows each table's service, nullability, defaults, enum values and required columns.

WITHOUT a structure file the target types are GUESSES from column names (id, *_id, *_at, *_date,
is_*, amounts, vitals). Every report row says "guess" and the high-failure warning is
the way to spot a wrong guess. Do not point the migration at guessed output.

ENV: SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
     SNOWFLAKE_SCHEMA, SNOWFLAKE_PRIVATE_KEY_PATH  (same .env as the rest of the repo)
     V3_SCHEMA_PATH   (optional) full path of the V3 structure file
     V3_MAPPINGS_DIR  (optional) folder of the <facility>_data_mappings.json files
     Not set: the first dags/jsons folder that has the file is used (next to this script, /opt/airflow/pipelines/dags/jsons,
     /opt/airflow/dags/jsons, ./dags/jsons). A path set in the environment that does not exist is an error.
"""

from __future__ import annotations

import argparse
import contextlib
import json
from datetime import date, datetime, timedelta, timezone
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

SF_DB = os.getenv("TYPED_SF_DB", "HOSPITALS")      # same database snowflake_to_v3_migration reads
CLEAN_SUFFIX = "CLEAN"
SOURCE_OVERRIDE: dict[tuple[str, str], str] = {}   # (FACILITY, table) -> SQL name to read instead of <FACILITY>_CLEAN.<table> (set by the flatten step)
MAX_CASCADE = 30000                     # skipped ids passed on to the tables that point to them
ERROR_SAMPLE_LIMIT = 5000               # records with errors or skipped listed per table
CHANGE_LOG_LIMIT = 200000               # changed values listed per table

# ── settings you may want to change ──────────────────────────────────────────

# There is no day-first or month-first default. A slash date like 06/07/2024 is read in the order the column's own other
# dates prove (a part above 12: 15/06/2024 means day-first). A column that proves nothing has every such ambiguous date
# reported as an error, instead of being guessed.
NUMBER_DATE_YEARS = (1990, 2050)      # an Excel or Unix number is read as a date only if it falls in these years; otherwise it is an error

# text that means "nothing here" — becomes empty before any cast
NULL_TOKENS = ("", "null", "none", "n/a", "na", "nan", "undefined", "-", "--")

# Defaults allowed for a column V3 says cannot be empty. Taken from _V3_NULL_DEFAULTS in
# v2_to_v3_api_migration.py, WITHOUT the ones that hide missing medical or money data
# (payment_method "cash", admitting_doctor_id 1, admission_diagnosis "Not specified",
# recorded_by 1, exemption_amount 0). A record missing one of those is reported as an error instead.
DEFAULTS: dict[str, object] = {
    "created_by": 1, "updated_by": 1,
    "currency": "KES", "invoice_type": "standard", "credit_note_status": "none",
    "is_a_split": 0, "appear_bold": 0, "use_bio_ref_flagging": 0, "has_quick_results": 0,
    "queued": 0, "tobe_contacted": 0, "is_walkin": 0, "preauth_approved": 0,
    "requires_preauth": 0, "requested_by": 1,
    "item_name": "Not given",             # a label only; the line (and its amount) is kept instead of being lost
}

# V3's structure file says these columns may be empty, but V3 rejected empty values for them before
# (seen in v2_to_v3_api_migration's error log), so they always get their default.
ALWAYS_DEFAULT = {"credit_note_status", "invoice_type", "is_a_split"}

def _plain(x: str) -> str:
    """SQL: the text with tabs, line breaks and non-breaking spaces turned into ordinary spaces (so TRIM removes them)."""
    return f"TRANSLATE({x}, CHR(9) || CHR(10) || CHR(13) || CHR(160), '    ')"


def _blank(x: str) -> str:
    """SQL test: the value is empty (NULL or blank, including tabs and non-breaking spaces)."""
    return f"({x} IS NULL OR TRIM({_plain(x)}) = '')"


def _two_words(x: str) -> str:
    """SQL test: the value has two or more words."""
    return f"REGEXP_COUNT(TRIM({x}), '[^ ]+') >= 2"


# How a facility's staff type slash dates (3/12/24). Used ONLY for a date column whose own values do not prove the order
# (no slash date with a part above 12). A column whose values prove an order, or mix both, keeps what its data shows.
# FACILITY_DATE_ORDER: one convention for every date column of a facility. COLUMN_DATE_ORDER: one column; it wins over the facility.
# Every entry is an ASSUMPTION until someone at the facility or the team confirms it. Each use is printed and shows in the report as "DMY (assumed)".
FACILITY_DATE_ORDER = {
    "WEMA_MEDICAL": "DMY",   # PENDING CONFIRMATION: Kenyan day-first convention; first seen on vitals.LMP "3/12/24"
}
COLUMN_DATE_ORDER: dict[tuple[str, str, str], str] = {}   # e.g. {("WEMA_MEDICAL", "vitals", "lmp"): "DMY"}


# Fill an empty value from other columns of the same record. (table, column) -> (columns the rule uses, SQL builder).
# patients: the old system left one name field empty and put the full name in another one.
FALLBACK_RULES = {
    ("patients", "first_name"): (("first_name", "middle_name", "last_name"), lambda c:
        f"CASE WHEN {_blank(c['first_name'])} AND {_two_words(c['last_name'])} "
        f"THEN SPLIT_PART(TRIM({c['last_name']}), ' ', 1) "
        f"WHEN {_blank(c['first_name'])} AND {_blank(c['last_name'])} AND {_two_words(c['middle_name'])} "
        f"THEN SPLIT_PART(TRIM({c['middle_name']}), ' ', 1) "
        f"WHEN {_blank(c['last_name'])} AND {_blank(c['middle_name'])} AND {_two_words(c['first_name'])} "
        f"THEN SPLIT_PART(TRIM({c['first_name']}), ' ', 1) ELSE {c['first_name']} END"),
    ("patients", "last_name"): (("first_name", "middle_name", "last_name"), lambda c:
        f"CASE WHEN {_blank(c['first_name'])} AND {_two_words(c['last_name'])} "
        f"THEN TRIM(REGEXP_REPLACE(TRIM({c['last_name']}), '^[^ ]+ +', '')) "
        f"WHEN {_blank(c['first_name'])} AND {_blank(c['last_name'])} AND {_two_words(c['middle_name'])} "
        f"THEN TRIM(REGEXP_REPLACE(TRIM({c['middle_name']}), '^[^ ]+ +', '')) "
        f"WHEN {_blank(c['last_name'])} AND NOT {_blank(c['middle_name'])} THEN TRIM({c['middle_name']}) "
        f"WHEN {_blank(c['last_name'])} AND {_blank(c['middle_name'])} AND {_two_words(c['first_name'])} "
        f"THEN TRIM(REGEXP_REPLACE(TRIM({c['first_name']}), '^[^ ]+ +', '')) "
        f"ELSE {c['last_name']} END"),
    ("patients", "middle_name"): (("last_name", "middle_name"), lambda c:
        f"CASE WHEN {_blank(c['last_name'])} AND NOT {_blank(c['middle_name'])} THEN NULL ELSE {c['middle_name']} END"),
}

# Names that other records link to by id (and patient names). An empty one gets a label so the record, and the links to it, survive.
TABLE_DEFAULTS = {("core_departments", "name"): "Not given", ("inp_wards", "name"): "Not given",
                  ("procedures", "name"): "Not given", ("inv_products", "generic_name"): "Not given",
                  ("patients", "first_name"): "Not given", ("patients", "last_name"): "Not given",
                  ("user_profiles", "last_name"): "Not given",        # e.g. the "Administrator" account: one word, no last name
                  # Allergy and diagnosis: kept with an honest "Not specified" instead of losing the record (losing an allergy is worse
                  # than a vague entry). A column listed here is allowed a default even though its name looks medical (see NEVER_DEFAULT_RE).
                  # PENDING CONFIRMATION from the V3 owner: that nothing in V3 (alerts, filters by status) treats this text as a real value.
                  ("evaluation_allergies", "allergen_text"): "Not specified",
                  ("evaluation_diagnosis", "status"): "Not specified"}

# Tables the type check leaves alone: they are built as before (not checked, not rebuilt as typed tables). Not migrated.
# PENDING CONFIRMATION from the V3 owner: the organization and facility already exist in V3 (every V3 table carries
# organization_id / facility_id). This does not stop snowflake_to_v3_migration from sending them; that is decided there.
NOT_TYPE_CHECKED = {"core_organizations", "core_facilities"}

# A record with an empty value here is left out and listed (a skip), not an error: V3 needs the value, nothing can fill it, and
# the original record (notes included) stays in the changes list. (table, column)
SKIP_WHEN_EMPTY = {("evaluation_visit_consultants", "consultant_id")}

# A user column ("created_by", "user"...) may hold the person's username instead of an id. It is replaced by the id of the user
# with that username (RAW's users table), and the username is kept in a new column <COLUMN>_NAME. No user, or several
# users, with that username is an error: it is never guessed.
USER_COLUMN_RE = re.compile(r"(_by|^user)$", re.I)

# An allowed-word column whose value is not one of V3's words can be worked out from other columns of the record:
# (table, column) -> (columns the rule needs, builder). The original is kept in <COLUMN>_NAME. Nothing worked out = an error.
STORE_TYPE_WORDS = (("pharmacy", ("pharm", "dispens")), ("laboratory", ("lab",)), ("ward", ("ward",)),
                    ("theater", ("theat", "operating")), ("consumable", ("consum",)), ("main", ("main",)),
                    ("department", ("consult", "psych", "dental", "nurs", "opd", "clinic", "department")))


def _store_type_sql(c: dict, spelled: str, allowed: str) -> str:
    """inv_stores.type from the store's name (Silverwood stores 1 in the type column and the real information in the name), then is_main_store."""
    t, n = f"LOWER(TRIM({c['type']}))", f"LOWER(TRIM({c['name']}))"
    whens = [f"WHEN {t} IN ({allowed}) THEN {spelled}"]
    for word, keys in STORE_TYPE_WORDS:
        test = " OR ".join(f"{n} RLIKE {lit('(.*[^a-z])?' + k + '.*')}" for k in keys)
        whens.append(f"WHEN {test} THEN {lit(word)}")
    if "is_main_store" in c:
        whens.append(f"WHEN LOWER(TRIM({c['is_main_store']})) IN ('1', 'true', 'yes', 'y') THEN 'main'")
    return "CASE " + " ".join(whens) + " ELSE NULL END"


DERIVED_ENUMS = {("inv_stores", "type"): (("name",), _store_type_sql)}

# A "<something>_by" user column that V3 cannot leave empty gets user 1. One V3 allows to be empty stays empty
# (approved_by, cancelled_by, deleted_by... being empty means it did not happen).
BY_RE = re.compile(r"_by$", re.I)

# A number V3 cannot leave empty gets 0 when empty, except ids (0 would link to nothing) and medical values.
ID_RE = re.compile(r"(^id$|_id$)", re.I)
MEDICAL_RE = re.compile(r"(temperature|pulse|weight|height|bmi|bp\b|systolic|diastolic|saturation|spo2|respiratory|"
                        r"glucose|sugar|diagnos|dose|dosage|result|allerg|medic|prescri)", re.I)

# A value that cannot fit V3 is left empty (and listed in the changes file) instead of failing the table.
# (table, column) -> only for columns V3 allows to be empty.
EMPTY_WHEN_UNFIT = {("vitals", "temperature")}

# A patient_id can be taken from, and checked against, a parent record: (column that points to it, parent table).
PARENTS = (("visit_id", "visits"), ("invoice_id", "invoices"))

# Medical and money columns: NEVER given a made-up default, and a cast failure on them
# holds the record back instead of silently emptying a clinical or financial value.
NEVER_DEFAULT_RE = re.compile(
    r"(amount|price|cost|total|balance|fee|charge|paid|payment|temperature|pulse|weight|height|"
    r"bmi|bp\b|systolic|diastolic|saturation|spo2|respiratory|glucose|sugar|diagnos|dose|dosage|"
    r"result|allerg|medic|prescri)", re.I)

# vitals V3 stores as decimal(6,2) (see _DECIMAL_VITALS in v2_to_v3_api_migration.py)
DECIMAL_VITALS = {"temperature", "weight", "height", "bmi", "head_circumference", "muac",
                  "random_blood_sugar", "fasting_blood_sugar", "blood_sugar", "waist", "hip",
                  "body_fat", "muscle_mass", "bone_mass", "body_water", "visceral_fat",
                  "intracular_pressure"}

WARN_FAILURE_SHARE = 0.05    # more than 5% of values failing = the target type is probably wrong
TEXT_TYPES = {"TEXT", "VARCHAR", "STRING", "CHAR", "CHARACTER"}
# CLEAN columns that already have a real type: read as text and checked against V3 like any other column
TYPED_OK = {"NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "REAL",
            "FLOAT4", "FLOAT8", "DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "DATETIME", "BOOLEAN"}
DEFAULT_TEXT_LENGTH = 16777216


# ── small helpers ────────────────────────────────────────────────────────────

def q(name: str) -> str:
    """Puts quotes around a Snowflake column or table name."""
    return '"' + name.replace('"', '""') + '"'


def sf_schema(facility: str, layer: str) -> str:
    """Full name of a schema, for example HOSPITALS.WEMA_MEDICAL_CLEAN."""
    return f"{SF_DB}.{facility.upper()}_{layer}"


def lit(value) -> str:
    """Turns a Python value into SQL text (numbers stay numbers, text gets quotes)."""
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _connect():
    """Connects to Snowflake with the key-pair login from the .env file."""
    import snowflake.connector
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env", override=False)
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER", "").strip(),
        account=os.getenv("SNOWFLAKE_ACCOUNT", "").strip(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "").strip(),
        database=os.getenv("SNOWFLAKE_DATABASE", "").strip(),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC").strip(),
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip(),
        client_session_keep_alive=True,
    )


# ── 0. choose which tables to run on ─────────────────────────────────────────

def choose_tables(cur, facility: str, tables: list[str] | None = None,
                  conf: dict | None = None, skip: list[str] | None = None) -> list[str]:
    """Decides which tables to check: the ones named, else the flatten DAG's list, else every table in CLEAN; minus any skipped."""
    cur.execute(f"""SELECT table_name FROM {SF_DB}.INFORMATION_SCHEMA.TABLES
                    WHERE table_schema = %s ORDER BY table_name""",
                (f"{facility.upper()}_{CLEAN_SUFFIX}",))
    existing = {r[0].lower(): r[0] for r in cur.fetchall()}
    if tables:
        wanted = [t.lower() for t in tables]
    elif conf and conf.get("tables") and facility.lower() in {f.lower() for f in conf.get("facilities", [])}:
        wanted = [t.lower() for t in conf["tables"]]
    else:
        wanted = sorted(existing)
    missing = [t for t in wanted if t not in existing]
    if missing:
        print(f"  not in {facility.upper()}_{CLEAN_SUFFIX}, skipped: {', '.join(missing)}")
    skipped = {s.lower() for s in (skip or [])}
    return [existing[t] for t in wanted if t in existing and t not in skipped]


# ── 1. get the V3 structure ──────────────────────────────────────────────────

@dataclass
class Target:
    """What one column must look like in V3: its type, size, whether it can be empty, and the allowed values."""

    kind: str = "keep"                 # keep | int | decimal | float | flag | date | timestamp | text
    p: int | None = None               # decimal precision
    s: int | None = None               # decimal scale
    length: int | None = None          # text maximum length
    nullable: bool | None = None       # None = unknown
    v3_default: str | None = None
    enum_values: list | None = None    # allowed values when V3 declares the column as an enum
    source: str = "guess"              # 'v3', 'name-rule', 'guess' or 'not in V3'
    lo: int | None = None              # smallest whole number V3's column holds
    hi: int | None = None              # largest whole number V3's column holds

    def label(self) -> str:
        """Short text for the report, such as 'decimal(6,2)' or 'text(50)'."""
        if self.kind == "decimal":
            return f"decimal({self.p},{self.s})"
        if self.kind == "text" and self.length:
            return f"text({self.length})"
        return self.kind


def target_from_v3_type(ty: str) -> Target:
    """Turns V3's type text (such as decimal(10,2) or varchar(255)) into a Target."""
    ty = (ty or "").strip()
    m = re.match(r"([a-zA-Z]+)\s*(?:\((.*)\))?", ty)
    dt, args = (m.group(1).lower(), m.group(2) or "") if m else (ty.lower(), "")
    parts = [x.strip() for x in args.split(",")] if args else []
    to_int = lambda v: int(v) if str(v or "").strip().isdigit() else None

    if dt == "tinyint" and parts[:1] == ["1"] or dt in ("bit", "bool", "boolean"):
        return Target("flag", source="v3")             # tinyint(1) is how MySQL stores yes/no
    if dt in ("int", "integer", "bigint", "smallint", "mediumint", "tinyint", "number"):
        t = Target("int", source="v3")
        bits = {"tinyint": 8, "smallint": 16, "mediumint": 24, "int": 32, "integer": 32, "bigint": 64}.get(dt)
        if bits:
            t.lo, t.hi = (0, 2 ** bits - 1) if "unsigned" in ty.lower() else (-2 ** (bits - 1), 2 ** (bits - 1) - 1)
        return t
    if dt in ("decimal", "numeric"):
        return Target("decimal", p=to_int(parts[0] if parts else None) or 18,
                      s=to_int(parts[1] if len(parts) > 1 else None) or 0, source="v3")
    if dt in ("float", "double", "real", "float4", "float8"):
        return Target("float", source="v3")
    if dt == "date":
        return Target("date", source="v3")
    if dt in ("datetime", "timestamp") or dt.startswith("timestamp"):
        return Target("timestamp", source="v3")
    if dt == "enum":                                    # keep the allowed words
        t = Target("text", source="v3")
        t.enum_values = [v.replace("''", "'") for v in re.findall(r"'((?:[^']|'')*)'", args)]
        return t
    if dt in ("varchar", "char", "text", "longtext", "mediumtext", "tinytext", "string"):
        return Target("text", length=to_int(parts[0]) if dt in ("varchar", "char") and parts else None, source="v3")
    return Target("keep", source="v3")                  # json, blob, ... - leave as is


# columns V3 fills itself or this pipeline sets later - never a reason to hold a record back
SYSTEM_COLUMNS = {"id", "uuid", "organization_id", "facility_id", "created_at", "updated_at", "deleted_at"}


class V3Structure:
    """Everything V3 expects (types, whether empty is allowed, defaults, required columns), read from v3-schema.json."""

    def __init__(self) -> None:
        """Starts empty; get_v3_structure fills it."""
        self.cols: dict[tuple[str, str], dict[str, Target]] = {}
        self.required: dict[tuple[str, str], set] = {}      # columns the V3 model says must be sent
        self.fks: dict[tuple[str, str], list] = {}          # foreign keys: column -> parent table
        self.by_table: dict[str, list] = {}                 # table -> services that have it
        self.source = ""

    def __bool__(self) -> bool:
        """True once a structure has been loaded."""
        return bool(self.cols)

    def key(self, table: str, service: str | None):
        """Finds which V3 service and table a CLEAN table is; nothing if it is unknown or the name is in several services."""
        t = table.lower()
        if service and (service, t) in self.cols:
            return (service, t)
        services = self.by_table.get(t, [])
        return (services[0], t) if len(services) == 1 else None

    def target(self, table: str, col: str, service: str | None) -> Target | None:
        """The Target for one column of one table; nothing if V3 does not have it."""
        k = self.key(table, service)
        return self.cols[k].get(col.lower()) if k else None

    def name_rule(self, col: str) -> Target | None:
        """A type for a column name when every V3 table that has it agrees (3 tables or more); used when the table is not found."""
        if not hasattr(self, "_names"):
            seen: dict[str, dict] = {}
            for cols in self.cols.values():
                for c, t in cols.items():
                    seen.setdefault(c, {}).setdefault(t.label(), [t, 0])[1] += 1
            self._names = {c: next(iter(v.values()))[0] for c, v in seen.items()
                           if len(v) == 1 and sum(n for _, n in v.values()) >= 3
                           and next(iter(v.values()))[0].kind not in ("text", "keep")}
        t = self._names.get(col.lower())
        return Target(t.kind, p=t.p, s=t.s, source="name-rule") if t else None

    def required_set(self, table: str, service: str | None) -> set:
        """Columns that need a value: V3 lists them as required, or NOT NULL with no V3 default (system columns left out)."""
        k = self.key(table, service)
        if not k:
            return set()
        need = set(self.required.get(k, set()))
        need |= {c for c, t in self.cols[k].items() if t.nullable is False and not t.v3_default}
        return need - SYSTEM_COLUMNS


# WHERE THE JSON FILES ARE. Both can be set from the environment, as full paths, so a server never has to guess:
#   V3_SCHEMA_PATH   the V3 structure file, e.g. /opt/airflow/pipelines/dags/jsons/v3-schema.json
#   V3_MAPPINGS_DIR  the folder holding <facility>_data_mappings.json, e.g. /opt/airflow/pipelines/dags/jsons
# Not set = the first of these folders that has the file (so it is found wherever the repo is deployed):
#   1. dags/jsons next to this script          (a checkout, or the repo mounted anywhere)
#   2. /opt/airflow/pipelines/dags/jsons       (the repo mounted by docker-compose on the Airflow server)
#   3. /opt/airflow/dags/jsons                 (the dags folder mounted by docker-compose)
#   4. dags/jsons under the current folder
# A path that IS set in the environment but is wrong is an error, never a silent fallback.
V3_SCHEMA_ENV, V3_MAPPINGS_ENV = "V3_SCHEMA_PATH", "V3_MAPPINGS_DIR"


def json_dirs() -> list[Path]:
    """The folders searched for the JSON files when no environment path is set, in order."""
    return [Path(__file__).resolve().parent / "dags" / "jsons", Path("/opt/airflow/pipelines/dags/jsons"),
            Path("/opt/airflow/dags/jsons"), Path.cwd() / "dags" / "jsons"]


def _find(name: str) -> tuple[Path, list[Path]]:
    """The first existing <folder>/<name>, and every path tried (the first one is returned when none exists, for messages)."""
    tried = [d / name for d in json_dirs()]
    return next((t for t in tried if t.is_file()), tried[0]), tried


def v3_schema_path() -> Path:
    """The V3 structure file this script will read: $V3_SCHEMA_PATH, else the first dags/jsons folder that has v3-schema.json."""
    env = os.getenv(V3_SCHEMA_ENV, "").strip()
    return Path(env) if env else _find("v3-schema.json")[0]


def v3_schema_tried() -> str:
    """Every place the V3 structure file is looked for, as text for an error message."""
    env = os.getenv(V3_SCHEMA_ENV, "").strip()
    return env if env else ", ".join(str(t) for t in _find("v3-schema.json")[1])


def mapping_path(facility: str) -> Path:
    """The mapping file of a facility: in $V3_MAPPINGS_DIR, else in the first dags/jsons folder that has it."""
    name = f"{facility.lower()}_data_mappings.json"
    env = os.getenv(V3_MAPPINGS_ENV, "").strip()
    return Path(env) / name if env else _find(name)[0]


def get_v3_structure(path: str | None = None) -> V3Structure:
    """Reads the V3 structure file into a V3Structure. Path order: the argument, $V3_SCHEMA_PATH, dags/jsons/v3-schema.json
    next to this script. A path that was given (argument or environment) but does not exist raises FileNotFoundError;
    only the default location missing gives an empty structure (the script then GUESSES types)."""
    v = V3Structure()
    given = path or os.getenv(V3_SCHEMA_ENV, "").strip()
    if given and not Path(given).is_file():
        raise FileNotFoundError(f"V3 structure file not found: {given}" +
                                (f" (set by ${V3_SCHEMA_ENV})" if not path else ""))
    default = v3_schema_path()
    path = given or (str(default) if default.is_file() else None)
    if not path:
        return v
    with open(path, encoding="utf-8-sig") as fh:
        d = json.load(fh)
    v.source = path
    defaults = d.get("defaults", {})
    for key, tbl in d.get("tables", {}).items():
        service, table = key.split("/", 1)
        has_default = set(defaults.get(key, []))
        cols: dict[str, Target] = {}
        for col, ty in tbl.get("columns", {}).items():
            t = target_from_v3_type(ty)
            t.nullable = bool(tbl.get("nullable", {}).get(col, True))
            t.v3_default = "(V3 fills it)" if col in has_default else None
            cols[col.lower()] = t
        v.cols[(service, table.lower())] = cols
        v.fks[(service, table.lower())] = tbl.get("fks", [])
        v.by_table.setdefault(table.lower(), []).append(service)
    for model in d.get("models", {}).values():
        name = (model.get("table") or {}).get("name")
        if name and model.get("service") and isinstance(model.get("required"), list):
            v.required.setdefault((model["service"], name.lower()), set()).update(c.lower() for c in model["required"])
    return v


def load_mapping(facility: str, path: str | None = None) -> tuple[dict[str, str], set]:
    """Reads the mapping JSON: which V3 service each V3 table belongs to, and which (table, field) are not plain copies of a source column."""
    env_dir = os.getenv(V3_MAPPINGS_ENV, "").strip()
    if env_dir and not path and not Path(env_dir).is_dir():
        raise FileNotFoundError(f"mapping folder not found: {env_dir} (set by ${V3_MAPPINGS_ENV})")
    path = Path(path) if path else mapping_path(facility)
    if not path.is_file():
        print(f"Mapping file: none found at {path} - tables are matched to V3 by name only")
        return {}, set()
    doc = json.loads(path.read_text(encoding="utf-8-sig"))
    mapped = {str(t["v3 table"]).lower(): t["v3 service"] for t in doc.get("tables", [])
              if t.get("v3 table") and t.get("v3 service")}
    derived = {(str(f["v3_table"]).lower(), str(f["field"]).lower()) for f in doc.get("fields", [])
               if f.get("kind") not in (None, "", "copied")}
    print(f"Mapping file: {len(mapped)} V3 tables from {path}")
    return mapped, derived


def guess_target(col: str) -> Target:
    """Last resort: a type guessed from the column name alone."""
    c = col.lower()
    if c in ("uuid", "source_schema") or c.endswith("_uuid"):
        return Target("keep")
    if c == "id" or c.endswith("_id"):
        return Target("int")
    if c.startswith(("is_", "has_", "requires_")):
        return Target("flag")
    if c.endswith(("_at", "_time")):
        return Target("timestamp")
    if c.endswith("_date") or c in ("dob", "date_of_birth", "birthdate"):
        return Target("date")
    if c in DECIMAL_VITALS:
        return Target("decimal", p=6, s=2)
    if re.search(r"(amount|price|cost|total|balance|fee|charge|paid|subtotal|tax|discount)$", c):
        return Target("decimal", p=18, s=2)
    return Target("keep")


# ── 2. standardise (SQL expressions) ─────────────────────────────────────────

def clean_sql(col: str, expr: str | None = None) -> str:
    """SQL that trims spaces and turns blanks and 'N/A'-style text into empty. expr = SQL to clean instead of the column."""
    x = f"TRIM({_plain(expr or q(col))})"
    tokens = ", ".join(lit(t) for t in NULL_TOKENS)
    return f"CASE WHEN LOWER({x}) IN ({tokens}) THEN NULL ELSE {x} END"


def _fix_comma(e: str) -> str:
    """SQL that turns '34,5' into '34.5'. Used for decimal and float columns only."""
    return f"CASE WHEN {e} RLIKE '^-?[0-9]+,[0-9]{{1,2}}$' THEN REPLACE({e}, ',', '.') ELSE {e} END"


def _fix_number(e: str) -> str:
    """SQL that turns '1,350', '1 350' and 'KES 2,500.50' into '1350' and '2500.50' (currency code and thousands separators removed)."""
    t = f"REGEXP_REPLACE(REGEXP_REPLACE(UPPER({e}), '^(KES|KSH|KSHS|USD|[$]) *', ''), ' *(KES|KSH|KSHS)$', '')"
    return f"IFF({t} RLIKE '^-?[0-9]{{1,3}}([, ][0-9]{{3}})+([.][0-9]+)?$', REPLACE(REPLACE({t}, ',', ''), ' ', ''), {t})"


# ── 3. cast (SQL expressions, applied to the cleaned value) ──────────────────

_MONTHS = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")


def _words(c: str) -> str:
    """SQL: the value in lower case with every separator turned into one space ('15-Jun-2024' becomes '15 jun 2024')."""
    return f"TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER({c}), '([0-9])(st|nd|rd|th)([^a-z]|$)', '\\\\1\\\\3'), '[^0-9a-z]+', ' '))"


_MONTH_WORDS = {"jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3, "apr": 4, "april": 4, "may": 5,
                "jun": 6, "june": 6, "jul": 7, "july": 7, "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
                "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12}


def _month_text(t: str) -> str:
    """SQL: a month as text, from a number or from a whole month name or abbreviation ('jun', 'june', 'sept'); anything else is empty."""
    whens = " ".join(f"WHEN '{w}' THEN '{n}'" for w, n in _MONTH_WORDS.items())
    return f"CASE WHEN {t} RLIKE '[0-9]+' THEN {t} ELSE CASE {t} {whens} END END"


def _year_text(t: str) -> str:
    """SQL: a year as 4-digit text; a 2-digit year up to next year's last two digits is 20xx, the rest 19xx."""
    return (f"IFF(LENGTH({t}) = 2, IFF(TRY_TO_NUMBER({t}, 38, 0) <= MOD(YEAR(CURRENT_DATE()), 100) + 1, '20', '19') "
            f"|| {t}, {t})")


def _make_date(y: str, m: str, d: str) -> str:
    """SQL: a date from year, month and day text; empty when that day does not exist."""
    return (f"TRY_TO_DATE(LPAD({y}, 4, '0') || '-' || LPAD({m}, 2, '0') || '-' || LPAD({d}, 2, '0'), 'YYYY-MM-DD')")


_EXCEL, _EPOCH_S, _EPOCH_MS = "^[0-9]{5}([.][0-9]+)?$", "^[0-9]{10}$", "^[0-9]{13}$"
_EPOCH = f"{_EPOCH_S}|{_EPOCH_MS}"
_NUMBER_KINDS = {"EXCEL": _EXCEL, "EPOCH": _EPOCH}     # a number is read as a date only in a column that has no other date format


def _number_stamp(c: str) -> str:
    """SQL: a number that is an Excel day count (5 digits, from 1899-12-30), or Unix seconds or milliseconds (10 or 13 digits, UTC), as a timestamp."""
    first, last = date(NUMBER_DATE_YEARS[0], 1, 1), date(NUMBER_DATE_YEARS[1], 12, 31)
    xl_lo, xl_hi = (first - date(1899, 12, 30)).days, (last - date(1899, 12, 30)).days + 1
    ep_lo, ep_hi = (first - date(1970, 1, 1)).days * 86400, ((last - date(1970, 1, 1)).days + 1) * 86400
    n = f"TRY_TO_DOUBLE({c})"
    return (f"CASE WHEN {c} RLIKE '{_EXCEL}' AND {n} >= {xl_lo} AND {n} < {xl_hi} "
            f"THEN DATEADD(second, ROUND({n} * 86400, 0), TO_TIMESTAMP_NTZ('1899-12-30 00:00:00')) "
            f"WHEN {c} RLIKE '{_EPOCH_S}' AND {n} >= {ep_lo} AND {n} < {ep_hi} "
            f"THEN DATEADD(second, TRY_TO_NUMBER({c}, 38, 0), TO_TIMESTAMP_NTZ('1970-01-01 00:00:00')) "
            f"WHEN {c} RLIKE '{_EPOCH_MS}' AND {n} >= {ep_lo * 1000} AND {n} < {ep_hi * 1000} "
            f"THEN DATEADD(second, TRY_TO_NUMBER({c}, 38, 0) / 1000, TO_TIMESTAMP_NTZ('1970-01-01 00:00:00')) END")


def _date_sql(c: str, day_first: bool, numbers: str = "", strict: bool = False) -> str:
    """SQL for a date written as 2024-06-15, 2024/06/15, 15/06/2024, 15-06-24, 15.06.2024, 15 Jun 2024, 15th June 2024, June 15 2024, 20240615, an Excel day count or Unix time."""
    n = _words(c)
    p = lambda k: f"SPLIT_PART({n}, ' ', {k})"
    day, month = (p(1), p(2)) if day_first else (p(2), p(1))
    if strict:      # the column mixes both orders: a value is read by its own proof (a part above 12 cannot be a month)
        a, b = (f"TRY_TO_NUMBER({p(k)}, 38, 0)" for k in (1, 2))
        day, month = (f"IFF({b} > 12, {p(2)}, {p(1)})", f"IFF({b} > 12, {p(1)}, {p(2)})") if day_first else \
                     (f"IFF({a} > 12, {p(1)}, {p(2)})", f"IFF({a} > 12, {p(2)}, {p(1)})")
    yy, tail = "([0-9]{2}|[0-9]{4})", "( .*)?"
    return (f"CASE WHEN {c} RLIKE '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}.*' THEN TRY_TO_DATE(LEFT({c}, 10), 'YYYY-MM-DD') "
            f"WHEN {n} RLIKE '[0-9]{{4}} [0-9]{{1,2}} [0-9]{{1,2}}{tail}' THEN {_make_date(p(1), _month_text(p(2)), p(3))} "
            f"WHEN {n} RLIKE '[0-9]{{1,2}} [0-9]{{1,2}} {yy}{tail}' THEN {_make_date(_year_text(p(3)), _month_text(month), day)} "
            f"WHEN {n} RLIKE '[0-9]{{1,2}} [a-z]{{3,9}} {yy}{tail}' THEN {_make_date(_year_text(p(3)), _month_text(p(2)), p(1))} "
            f"WHEN {n} RLIKE '[a-z]{{3,9}} [0-9]{{1,2}} {yy}{tail}' THEN {_make_date(_year_text(p(3)), _month_text(p(1)), p(2))} "
            f"WHEN {c} RLIKE '^[0-9]{{8}}$' THEN TRY_TO_DATE({c}, 'YYYYMMDD') "
            + (f"WHEN {c} RLIKE '{_NUMBER_KINDS[numbers]}' THEN CAST({_number_stamp(c)} AS DATE) " if numbers else "") + "END")


_TIME = "[0-9]{1,2}:[0-9]{2}(:[0-9]{2})?( ?[aApP][mM])?"
_ZONE = ".*[0-9]:[0-9]{2}.*([zZ]|[+-][0-9]{2}(:?[0-9]{2})?)$"       # a time followed by Z or +03:00 / -0500
TIME_ZONE = "Africa/Nairobi"


def _time_text(c: str) -> str:
    """SQL: the time of day in the value ('2:05pm' becomes '2:05 PM'), empty when there is none."""
    return f"NULLIF(UPPER(REGEXP_REPLACE(REGEXP_SUBSTR({c}, '{_TIME}'), ' ?([aApP][mM])$', ' \\\\1')), '')"


def _date_text(c: str) -> str:
    """SQL: the value without its time of day, which leaves the date part."""
    return f"TRIM(REGEXP_REPLACE({c}, 'T?{_TIME}.*$', ''))"


def _timestamp_sql(c: str, day_first: bool, numbers: str = "", d: str | None = None, tm: str | None = None,
                   strict: bool = False) -> str:
    """SQL for a date and time: ISO, any date format above followed by 24-hour or AM/PM time, or a time zone (Z, +03:00) converted to Nairobi time; a date alone becomes midnight."""
    d, tm = d or _date_text(c), tm or _time_text(c)
    first = f"WHEN {c} RLIKE '{_NUMBER_KINDS[numbers]}' THEN {_number_stamp(c)} " if numbers else ""
    day = f"TO_VARCHAR({_date_sql(d, day_first, strict=strict)})"
    both = f"{day} || ' ' || {tm}"
    return (f"CASE {first}WHEN {c} RLIKE '{_ZONE}' THEN TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE('{TIME_ZONE}', TRY_TO_TIMESTAMP_TZ({c}))) "
            f"WHEN {tm} IS NULL THEN TRY_TO_TIMESTAMP_NTZ({day}, 'YYYY-MM-DD') "
            f"ELSE COALESCE(IFF({c} RLIKE '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}[ T].*', TRY_TO_TIMESTAMP_NTZ({c}), NULL), "
            f"TRY_TO_TIMESTAMP_NTZ({both}), TRY_TO_TIMESTAMP_NTZ({both}, 'YYYY-MM-DD HH12:MI AM'), "
            f"TRY_TO_TIMESTAMP_NTZ({both}, 'YYYY-MM-DD HH12:MI:SS AM')) END")


def cast_sql(kind: str, c: str, t: Target, day_first: bool = True, numbers: str = "",
             d: str | None = None, tm: str | None = None, strict: bool = False) -> str:
    """SQL that converts a tidied value to its V3 type. A value that does not fit becomes empty and is counted."""
    if kind == "int":
        fits = f" AND TRY_TO_NUMBER({c}, 38, 0) BETWEEN {t.lo} AND {t.hi}" if t.lo is not None else ""
        return f"CASE WHEN {c} RLIKE '^-?[0-9]+([.]0+)?$'{fits} THEN TRY_TO_NUMBER({c}, 38, 0) END"
    if kind == "decimal":
        return f"CASE WHEN {c} RLIKE '^-?[0-9]+([.][0-9]+)?$' THEN TRY_TO_NUMBER({c}, {t.p}, {t.s}) END"
    if kind == "float":
        return f"CASE WHEN {c} RLIKE '^-?[0-9]+([.][0-9]+)?$' THEN TRY_TO_DOUBLE({c}) END"
    if kind == "flag":
        return (f"CASE LOWER({c}) WHEN 'true' THEN 1 WHEN 'yes' THEN 1 WHEN 'y' THEN 1 WHEN '1' THEN 1 "
                f"WHEN 'false' THEN 0 WHEN 'no' THEN 0 WHEN 'n' THEN 0 WHEN '0' THEN 0 END")
    if kind == "date":
        return _date_sql(c, day_first, numbers, strict)
    if kind == "timestamp":
        return _timestamp_sql(c, day_first, numbers, d, tm, strict)
    raise ValueError(kind)


# ── the plan for one table ───────────────────────────────────────────────────

@dataclass
class ColPlan:
    """What this script will do to one column: its V3 target, the SQL to tidy and convert it, its default, and which checks apply."""

    name: str
    src_type: str
    target: Target
    converted: bool = False            # TEXT cast to another type
    limited: bool = False              # TEXT with a V3 maximum length
    clean: str | None = None           # standardised value (expression), converted columns only
    cast: str | None = None            # cast of the standardised value
    final: str = ""                    # the finished value (converted, with any default)
    default: object | None = None
    required: bool = False             # V3 says it cannot be empty and has no default of its own
    protected: bool = False            # medical / money — never defaulted; a failed conversion is an error
    enum_values: list | None = None    # V3 allows only these values; anything else is an error
    fallback: bool = False             # an empty value is filled from other columns or another table
    fill_test: str | None = None       # SQL: true for the records whose value was filled that way (for the report)
    parents: list = field(default_factory=list)   # parent records this value is filled from and checked against
    own: str = ""                      # the tidied value before anything is filled in from a parent
    day_first: bool = True             # how this date column's 12/03/2024 is read when its order is proven month-first or day-first
    date_order: str = ""               # "DMY", "MDY", "MIXED", or "EXCEL" / "EPOCH" (the column has numbers of that kind and no other date format) when the column's own values show it
    date_assumed: bool = False         # the order comes from FACILITY_DATE_ORDER / COLUMN_DATE_ORDER, not from the column's own values
    strict: bool = False               # a date is read by its own proof; a date that could be either order is an error (columns that mix both orders, or prove neither)
    zero_default: bool = False         # the default is the general 0 for numbers, not a listed default
    garbage_only: bool = False         # column may be empty in V3: only a value that cannot be read gets the listed default
    id_required: bool = False          # the record's own id: a record without one is skipped
    typed: bool = False                # the CLEAN column already has a real type; it is read as text first
    fail_is_error: bool = False        # a value that cannot be converted is an error (not replaced by a listed default)
    empty_if_unfit: bool = False       # a value that cannot be converted is left empty and listed instead of failing the table
    link: dict | None = None           # the parent table this column points to; a record whose parent is missing is skipped
    spelled: bool = False              # allowed-word column: a word differing only in capitals is written as V3 spells it
    derived: bool = False              # allowed-word column worked out from other columns when its own value is not an allowed word
    change_label: str = ""             # what the changes list says for a value this rule changed
    user_lookup: dict | None = None    # a user column: text is replaced by the id of the user with that username
    skip_if_empty: bool = False        # an empty value leaves the record out (SKIP_WHEN_EMPTY)


def _parent_sql(name: str, ptr: str, info: dict) -> dict:
    """SQL pieces for one parent: the single value it gives, 
    how many different values it has, how many parent rows match."""
    val = f"TRIM(x.{info['value']})"
    where = f"FROM {info['table']} x WHERE TRIM(x.{info['id']}) = TRIM(t.{ptr})"
    return {"value": f"(SELECT IFF(COUNT(DISTINCT {val}) = 1, MAX({val}), NULL) {where})",
            "count": f"(SELECT COUNT(DISTINCT {val}) {where})"}


def plan_columns(table: str, columns: list[dict], structure: V3Structure, service: str | None = None,
                 parents: dict | None = None, date_orders: dict | None = None,
                 links: dict | None = None, users: dict | None = None) -> list[ColPlan]:
    """Builds one ColPlan per column of a table: finds its V3 type, then sets up the conversion, 
    length check, allowed-words check, default and parent checks. users = {"table", "id", "username"} (SQL names): where a user column's text is looked up."""
    plans: list[ColPlan] = []
    must_have = structure.required_set(table, service)
    quoted = {c["name"].lower(): q(c["name"]) for c in columns}
    for col in columns:
        name, src = col["name"], str(col["type"]).upper()
        tgt = structure.target(table, name, service)                 # 1. exact: V3 table + column
        if tgt is None:
            known = bool(structure) and structure.key(table, service) is not None
            if known:
                tgt = Target("keep", source="not in V3")
            else:                                                     # table unknown: 2. general name rule, 3. guess
                tgt = (structure.name_rule(name) if structure else None) or guess_target(name)
        p = ColPlan(name=name, src_type=src, target=tgt, final=q(name))
        p.protected = ((bool(NEVER_DEFAULT_RE.search(name)) or name.lower() == "payment_method")
                       and (table.lower(), name.lower()) not in TABLE_DEFAULTS)
        p.required = tgt.source == "v3" and name.lower() in must_have
        p.typed = (src not in TEXT_TYPES and src in TYPED_OK and tgt.kind in ("int", "decimal", "float", "flag", "date", "timestamp"))
        is_text = src in TEXT_TYPES or p.typed
        if is_text and tgt.kind == "text" and tgt.enum_values:
            p.enum_values = tgt.enum_values
            words = " ".join(f"WHEN {lit(v.lower())} THEN {lit(v)}" for v in tgt.enum_values)
            p.final, p.spelled = f"CASE LOWER(TRIM({q(name)})) {words} ELSE {q(name)} END", True
            drv = DERIVED_ENUMS.get((table.lower(), name.lower()))
            if drv and all(n in quoted for n in drv[0]):
                allowed_words = ", ".join(lit(v.lower()) for v in tgt.enum_values)
                p.final, p.derived, p.fallback = drv[1](quoted, p.final, allowed_words), True, True
                p.fill_test, p.change_label = f"({p.final}) IS DISTINCT FROM {q(name)}", "worked out from the other columns of the record"
        if links and tgt.source == "v3" and name.lower() in links:
            p.link = links[name.lower()]
        if is_text and tgt.kind in ("int", "decimal", "float", "flag", "date", "timestamp"):
            p.converted = True
            p.clean = clean_sql(name)
            if tgt.kind in ("int", "decimal", "float"):
                p.clean = _fix_number(p.clean)       # '1,350' -> '1350', 'KES 2,500' -> '2500'
            if tgt.kind in ("decimal", "float"):
                p.clean = _fix_comma(p.clean)        # '34,5' -> '34.5', once, in the standardise step
            if tgt.kind in ("date", "timestamp"):
                p.date_order = (date_orders or {}).get(name.lower(), "")
                p.day_first = p.date_order != "MDY"
                p.strict = p.date_order in ("MIXED", "")
            if (users and tgt.kind == "int" and tgt.source == "v3" and USER_COLUMN_RE.search(name) and table.lower() != "users"):
                raw = f"TRIM(t.{q(name)})"
                # one object {username: id} built once (it does not refer to the row being read, which Snowflake needs here);
                # a username shared by two different users is left out, so it stays an error instead of being guessed
                umap = (f"(SELECT OBJECT_AGG(k, v) FROM (SELECT LOWER(TRIM(x.{users['username']})) AS k, TO_VARIANT(MAX(TRIM(x.{users['id']}))) AS v "
                        f"FROM {users['table']} x WHERE TRIM(x.{users['username']}) <> '' AND TRIM(x.{users['id']}) <> '' "
                        f"GROUP BY 1 HAVING COUNT(DISTINCT TRIM(x.{users['id']})) = 1))")
                raw = f"TRIM(t.{q(name)})"
                lookup = f"IFF({raw} <> '' AND TRY_TO_NUMBER({raw}, 38, 0) IS NULL, GET({umap}, LOWER({raw}))::STRING, NULL)"
                p.own, p.clean, p.user_lookup = clean_sql(name), f"COALESCE({lookup}, {p.clean})", users     # own = the text as typed (not upper-cased)
            if parents and name.lower() == "patient_id" and tgt.kind == "int":
                for ptr, ptable in PARENTS:        # the patient is also known from the visit or invoice this record belongs to
                    if ptr in quoted and ptable != table.lower() and ptable in parents:
                        p.parents.append({"pointer": ptr, "column": quoted[ptr], "table": ptable, **_parent_sql(name, quoted[ptr], parents[ptable])})
                if p.parents:
                    p.own = p.clean
                    p.clean = f"COALESCE({p.own}, {', '.join(clean_sql(name, pa['value']) for pa in p.parents)})"
                    p.fallback, p.fill_test = True, f"({p.own}) IS NULL AND {q(name + '__C')} IS NOT NULL"
            helper = q(name + "__C")
            p.cast = cast_sql(tgt.kind, helper, tgt, p.day_first, p.date_order if p.date_order in _NUMBER_KINDS else "",
                              q(name + "__D"), q(name + "__T"), p.strict)
            p.final = p.cast
        elif is_text and tgt.kind == "text" and tgt.length and tgt.length < DEFAULT_TEXT_LENGTH:
            p.limited = True      # value is kept as is; a value over V3's limit is an error
        rule = FALLBACK_RULES.get((table.lower(), name.lower()))
        if rule and tgt.source == "v3" and all(n in quoted for n in rule[0]):
            p.final, p.fallback = rule[1](quoted), True
            p.fill_test = f"({p.final}) IS DISTINCT FROM {q(name)}"
        # default when V3 knows the column and it cannot be empty (even if V3 has a default of its own,
        # V3 rejected empty values for those before), or when it is on the always-default list
        default = DEFAULTS.get(name.lower(), TABLE_DEFAULTS.get((table.lower(), name.lower())))
        by_rule = default is None and tgt.source == "v3" and tgt.kind == "int" and bool(BY_RE.search(name))
        if by_rule:
            default = 1
        zero_rule = (tgt.source == "v3" and tgt.kind in ("int", "decimal", "float") and tgt.nullable is False
                     and not ID_RE.search(name) and not MEDICAL_RE.search(name) and not BY_RE.search(name))
        from_zero = default is None and zero_rule
        if from_zero:
            default = 0
        if (default is not None and tgt.source == "v3" and (not p.protected or zero_rule)
                and (tgt.nullable is False or p.required or name.lower() in ALWAYS_DEFAULT)):
            p.default, p.zero_default = default, from_zero
            base = f"NULLIF(TRIM({p.final}), '')" if tgt.kind == "text" and is_text else p.final   # blank text counts as empty
            p.final = f"COALESCE({base}, {lit(p.default)})"
        elif (default is not None and not by_rule and p.converted and tgt.source == "v3" and name.lower() in DEFAULTS
              and not p.protected):       # the column may be empty: only an unreadable value gets the default
            p.default, p.garbage_only = default, True
            p.final = f"IFF({q(name + '__C')} IS NOT NULL AND ({p.cast}) IS NULL, {lit(default)}, {p.cast})"
        p.id_required = name.lower() == "id" and tgt.source == "v3"
        p.empty_if_unfit = p.converted and (table.lower(), name.lower()) in EMPTY_WHEN_UNFIT and tgt.nullable is not False
        p.fail_is_error = (p.converted and not p.empty_if_unfit
                           and not (p.default is not None and not p.zero_default))
        if p.user_lookup:
            p.fail_is_error = True        # a username nobody has is an error, never replaced by user 1
        p.skip_if_empty = (table.lower(), name.lower()) in SKIP_WHEN_EMPTY and tgt.source == "v3"
        plans.append(p)
    return plans


def _enum_bad(p: ColPlan, allowed: str) -> str:
    """SQL test: a value is present but is not one of the words V3 allows (capital letters ignored)."""
    c = q(p.name)
    if p.derived:
        return f"{c} IS NOT NULL AND TRIM({c}) <> '' AND ({p.final}) IS NULL"
    return f"{c} IS NOT NULL AND TRIM({c}) <> '' AND LOWER(TRIM({c})) NOT IN ({allowed})"


def _missing_test(p: ColPlan) -> str:
    """SQL test: the value is empty (NULL or blank)."""
    if p.derived:
        return f"{_blank(q(p.name))} AND ({p.final}) IS NULL"
    if p.converted:
        return f"{q(p.name + '__C')} IS NULL"
    return _blank(f"TO_VARCHAR({p.final})")


def _cast_failed_reason(p: ColPlan) -> str:
    """SQL: the error text for a value that could not be converted, naming the column, the value and the type."""
    helper, label = q(p.name + "__C"), p.target.label()
    if p.target.source in ("guess", "name-rule"):
        label += " [type " + ("guessed from the column name" if p.target.source == "guess" else "taken from V3's other tables with this column name") + "; this table is not in V3]"
    if p.target.kind == "decimal":
        verb = f"IFF({helper} RLIKE '^-?[0-9]+([.][0-9]+)?$', ' does not fit ', ' is not a valid ')"
    elif p.target.kind == "int" and p.target.lo is not None:
        verb = f"IFF({helper} RLIKE '^-?[0-9]+([.]0+)?$', ' is outside the range of ', ' cannot be converted to ')"
        label = f"{label} ({p.target.lo} to {p.target.hi})"
    else:
        verb = "' cannot be converted to '"
    note = (" (this column has both day-first and month-first dates)" if p.strict and p.date_order == "MIXED" else
            " (no date in this column shows whether it is day-first or month-first)" if p.strict and p.target.kind in ("date", "timestamp") else "")
    tail = lit(label + note)
    if p.user_lookup:
        verb = f"IFF(TRY_TO_NUMBER({helper}, 38, 0) IS NULL, ' is not a user id, and no single user has that username', {verb})"
        tail = f"IFF(TRY_TO_NUMBER({helper}, 38, 0) IS NULL, '', {lit(label)})"
    if p.target.kind in ("date", "timestamp") and p.date_order in _NUMBER_KINDS:
        tail = (f"{lit(label + note)} || IFF({helper} RLIKE '{_NUMBER_KINDS[p.date_order]}', "
                f"{lit(' (a number is read as a date only in the years %d to %d)' % NUMBER_DATE_YEARS)}, '')")
    if p.target.kind in ("date", "timestamp") and p.date_order not in _NUMBER_KINDS:
        tail = (f"{lit(label + note)} || IFF({helper} RLIKE '{_EXCEL}|{_EPOCH}', "
                f"' (a number is read as a date only when the column has no other date format)', '')")
    return (f"IFF({helper} IS NOT NULL AND ({p.cast}) IS NULL, {lit(p.name + ': ')} || '\"' || {q(p.name)} || '\"' || "
            f"{verb} || {tail}, NULL)")


def _parent_reasons(p: ColPlan) -> list[str]:
    """SQL error texts for a patient_id against its parents: different patient, several patients, or no such parent."""
    out, own = [], q(p.name + "__O")
    for i, pa in enumerate(p.parents):
        value = q(f"{p.name}__P{i}")
        out.append(f"IFF({own} IS NOT NULL AND {value} IS NOT NULL AND TRY_TO_NUMBER({own}, 38, 0) <> TRY_TO_NUMBER(TRIM({value}), 38, 0), "
                   f"{lit(p.name + ' ')} || {own} || {lit(' does not match the patient of ' + pa['pointer'] + ' ')} || "
                   f"{pa['column']} || ' (patient ' || {value} || ')', NULL)")
        out.append(f"IFF({q(f'{p.name}__N{i}')} > 1, {lit(pa['pointer'] + ' ')} || {pa['column']} || "
                   f"' belongs to ' || TO_VARCHAR({q(f'{p.name}__N{i}')}) || {lit(' different patients in ' + pa['table'])}, NULL)")
    return out


def _join_reasons(parts: list[str]) -> str | None:
    """SQL that joins the reason texts of one record into a single text; empty when there are none."""
    if not parts:
        return None
    return f"NULLIF(ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT({', '.join(parts)}), '; '), '')"


def error_reason_sql(plans: list[ColPlan]) -> str | None:
    """SQL that lists why a record has an error (required value empty, too long, word not allowed, value would not convert). Empty = no error."""
    parts = []
    for p in plans:
        if p.required and p.default is None and not p.parents:
            parts.append(f"IFF({_missing_test(p)}, {lit(p.name + ' (required, missing)')}, NULL)")
        if p.limited:
            parts.append(f"IFF(LENGTH({q(p.name)}) > {p.target.length}, {lit(p.name + ' (too long, max ' + str(p.target.length) + ')')}, NULL)")
        if p.enum_values:
            allowed = ", ".join(lit(v.lower()) for v in p.enum_values)
            hint = " and the other columns did not show one" if p.derived else ""
            parts.append(f"IFF({_enum_bad(p, allowed)}, {lit(p.name + ': ')} || '\"' || TRIM({q(p.name)}) || '\"' || "
                         f"{lit(' is not an allowed value' + hint + ' (allowed: ' + ', '.join(p.enum_values) + ')')}, NULL)")
        if p.fail_is_error:
            parts.append(_cast_failed_reason(p))
        if p.strict and p.target.kind in ("date", "timestamp"):
            n = _words(q(p.name + "__C"))
            first, second = (f"TRY_TO_NUMBER(SPLIT_PART({n}, ' ', {k}), 38, 0)" for k in (1, 2))
            parts.append(f"IFF({n} RLIKE '[0-9]{{1,2}} [0-9]{{1,2}} ([0-9]{{2}}|[0-9]{{4}})( .*)?' AND {first} <= 12 AND {second} <= 12 "
                         f"AND {first} <> {second}, {lit(p.name + ': ')} || '\"' || {q(p.name)} || '\"' || "
                         f"' could be day-first or month-first, and " + ("this column has both" if p.date_order == "MIXED" else "no other date in this column shows which") + "', NULL)")
    return _join_reasons(parts)


def skip_reason_sql(plans: list[ColPlan]) -> str | None:
    """SQL that lists why a record is left out: a record it points to (visit, invoice, patient...) does not exist, or its patient disagrees with its visit. Empty = the record is kept."""
    parts = []
    for p in plans:
        if p.id_required:
            empty = f"{q(p.name + '__C')} IS NULL" if p.converted else _blank(q(p.name))
            parts.append(f"IFF({empty}, 'id is empty and cannot be filled', NULL)")
        if p.skip_if_empty:
            parts.append(f"IFF({_missing_test(p)}, {lit(p.name + ' is empty, and V3 needs it')}, NULL)")
        if p.link:
            value = q(p.name)
            why = f"{p.link['table']}{' (or that record was skipped)' if p.link.get('excluded') else ''}"
            parts.append(f"IFF({q(p.name + '__L')} = 0 AND NOT {_blank(value)}, {lit(p.name + ' ')} || TO_VARCHAR({value}) || "
                         f"IFF(TRY_TO_NUMBER(TRIM(TO_VARCHAR({value})), 38, 0) IS NULL, "
                         f"{lit(' is not a number, so it matches no record in ' + why)}, {lit(' not found in ' + why)}), NULL)")
        if p.parents:
            if p.required and p.default is None:
                parts.append(f"IFF({_missing_test(p)}, {lit(p.name + ' (missing, and not found through the visit or invoice)')}, NULL)")
            parts += _parent_reasons(p)
    return _join_reasons(parts)


def _source_cte(facility: str, table: str, plans: list[ColPlan]) -> str:
    """First part of every query: reads the CLEAN table and adds a tidied copy (<COLUMN>__C) of each column to be converted."""
    items = [f"{p.clean} AS {q(p.name + '__C')}" for p in plans if p.converted]
    for p in plans:
        if p.converted and p.target.kind == "timestamp":
            items += [f"{_date_text(p.clean)} AS {q(p.name + '__D')}", f"{_time_text(p.clean)} AS {q(p.name + '__T')}"]
    for p in plans:
        if p.user_lookup and not p.parents:
            items.append(f"{p.own} AS {q(p.name + '__O')}")
        if p.parents:
            items.append(f"{p.own} AS {q(p.name + '__O')}")
            for i, pa in enumerate(p.parents):
                items += [f"{pa['value']} AS {q(f'{p.name}__P{i}')}", f"{pa['count']} AS {q(f'{p.name}__N{i}')}"]
        if p.link:
            gone = p.link.get("excluded") or []
            skipped = f" AND {_norm('x.' + p.link['id'])} NOT IN ({', '.join(lit(i) for i in gone)})" if gone else ""
            items.append(f"(SELECT COUNT(*) FROM {p.link['parent']} x WHERE {_norm('x.' + p.link['id'])} = {_norm('t.' + q(p.name))}"
                         f"{skipped}) AS {q(p.name + '__L')}")
    helpers = ", ".join(items)
    src = SOURCE_OVERRIDE.get((facility.upper(), table.lower())) or f"{sf_schema(facility, CLEAN_SUFFIX)}.{q(table)}"
    typed = [p for p in plans if p.typed]
    if typed:
        src = (f"(SELECT * EXCLUDE ({', '.join(q(p.name) for p in typed)}), "
               f"{', '.join(f'TO_VARCHAR({q(p.name)}) AS {q(p.name)}' for p in typed)} FROM {src})")
    return f"WITH s AS (SELECT t.*{', ' + helpers if helpers else ''} FROM {src} t)"


def _norm(x: str) -> str:
    """SQL: an id as comparable text, so 001503, 1503 and 1503.0 are the same id."""
    return f"COALESCE(TO_VARCHAR(TRY_TO_NUMBER(TRIM(TO_VARCHAR({x})), 38, 0)), TRIM(TO_VARCHAR({x})))"


def _record_id(plans: list[ColPlan]) -> str:
    """SQL for the record's id as text (NULL when the table has no id column)."""
    id_col = next((p.name for p in plans if p.name.lower() == "id"), None)
    return f"TO_VARCHAR({q(id_col)})" if id_col else "NULL"


def original_text_columns(plans: list[ColPlan]) -> list[str]:
    """Extra columns <COLUMN>_NAME that keep the text a value was worked out from (a username, a store type), empty when the
    value was already right. Not made when the table already has a column of that name."""
    have, out = {p.name.lower() for p in plans}, []
    for p in plans:
        name = p.name + ("_NAME" if p.name.isupper() else "_name")
        if name.lower() in have:
            continue
        if p.user_lookup:
            o = q(p.name + "__O")
            out.append(f"IFF({o} IS NOT NULL AND TRY_TO_NUMBER({o}, 38, 0) IS NULL, {o}, NULL) AS {q(name)}")
        elif p.derived:
            o = f"TRIM({q(p.name)})"
            out.append(f"IFF({o} <> '' AND LOWER({o}) <> LOWER({p.final}), {o}, NULL) AS {q(name)}")
    return out


def typed_select_sql(facility: str, table: str, plans: list[ColPlan]) -> str:
    """SQL for the finished data of one table: every column converted to its V3 type, defaults filled in, skipped records left out."""
    items = [f"{p.final} AS {q(p.name)}" if p.final != q(p.name) else q(p.name) for p in plans]
    items += original_text_columns(plans)
    skip = skip_reason_sql(plans)
    where = f"\nWHERE ({skip}) IS NULL" if skip else ""
    return f"{_source_cte(facility, table, plans)}\nSELECT {', '.join(items)}\nFROM s{where}"


def _listing_sql(facility: str, table: str, plans: list[ColPlan], reason: str | None, extra: str, limit: int) -> str | None:
    """SQL listing the records that have a reason: record id, the reason, and their original values."""
    if not reason:
        return None
    pairs = ", ".join(f"{lit(p.name)}, {q(p.name)}" for p in plans)
    return (f"{_source_cte(facility, table, plans)}\n"
            f"SELECT {_record_id(plans)}, {reason}, TO_VARCHAR(OBJECT_CONSTRUCT_KEEP_NULL({pairs}))\n"
            f"FROM s\nWHERE ({reason}) IS NOT NULL{extra}\nORDER BY 1, 2\nLIMIT {int(limit)}")


def error_sample_sql(facility: str, table: str, plans: list[ColPlan], limit: int) -> str | None:
    """SQL for the records with an error (records that are skipped are not listed here)."""
    skip = skip_reason_sql(plans)
    return _listing_sql(facility, table, plans, error_reason_sql(plans), f" AND ({skip}) IS NULL" if skip else "", limit)


def skipped_sample_sql(facility: str, table: str, plans: list[ColPlan], limit: int) -> str | None:
    """SQL for the records left out because their patient cannot be found through the visit or invoice."""
    return _listing_sql(facility, table, plans, skip_reason_sql(plans), "", limit)


def change_sql(facility: str, table: str, plans: list[ColPlan], limit: int) -> str | None:
    """SQL listing every value changed in the records that are kept: record id, column, original value, new value, what happened."""
    skip = skip_reason_sql(plans)
    parts = []
    for p in plans:
        if not (p.converted or p.default is not None or p.fallback or p.spelled):
            continue
        orig, new = f"TRIM(TO_VARCHAR({q(p.name)}))", f"TO_VARCHAR({p.final})"
        blank = f"({orig} IS NULL OR {orig} = '')"
        kind = p.target.kind if p.converted else "text"
        if kind in ("int", "decimal", "float"):
            differs, label = f"ABS({p.final} - TRY_TO_DOUBLE({orig})) > 0.000001 OR TRY_TO_DOUBLE({orig}) IS NULL", "number changed (comma or rounding)"
        elif kind == "flag":
            if p.typed and p.src_type == "BOOLEAN":
                continue
            differs, label = f"{orig} NOT IN ('0', '1')", "yes/no converted to 1/0"
        elif kind == "date":
            differs, label = f"NOT ({orig} RLIKE '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}.*')", "date written in another format"
        elif kind == "timestamp":
            differs = (f"NOT ({orig} RLIKE '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}([ T][0-9]{{2}}:[0-9]{{2}}(:[0-9]{{2}}([.][0-9]+)?)?)?') "
                       f"OR {orig} RLIKE '{_ZONE}'")
            label = "date written in another format"
        else:
            differs, label = f"{new} IS DISTINCT FROM {orig}", "changed"
        if p.change_label:
            label = p.change_label
        elif p.user_lookup:
            label = "username replaced by the user's id"
        elif p.fallback:
            label = "rearranged from other columns"
        elif p.spelled:
            label = "spelling changed to the word V3 allows"
        if p.default is None:
            filled = lit("filled from other columns")
        elif p.fallback:
            filled = f"IFF({new} = {lit(str(p.default))}, 'default applied', 'filled from other columns')"
        else:
            filled = lit("default applied")
        if p.converted and p.default is not None:
            unreadable = f"WHEN {q(p.name + '__C')} IS NOT NULL AND {p.cast} IS NULL THEN '{'unreadable value: error, table fails' if p.fail_is_error else 'unreadable value replaced by default'}' "
        else:
            unreadable = ""
        placeholder = f"WHEN {q(p.name + '__C')} IS NULL THEN 'placeholder text (n/a, -, null) treated as empty' " if p.converted else ""
        emptied = ("moved to another column" if p.fallback and not p.converted
                   else "unreadable value: error, table fails" if p.fail_is_error
                   else "value could not be converted, left empty")
        label_sql = (f"IFF({orig} RLIKE '{_ZONE}', {lit('time zone converted to ' + TIME_ZONE.split('/')[1] + ' time')}, {lit(label)})"
                     if kind == "timestamp" else lit(label))
        what = (f"CASE WHEN {blank} THEN {filled} {unreadable}{placeholder}WHEN {p.final} IS NULL THEN "
                f"{lit(emptied)} ELSE {label_sql} END")
        where = f"(({blank} AND NULLIF({new}, '') IS NOT NULL) OR (NOT {blank} AND ({p.final} IS NULL OR {differs})))"
        if p.spelled and not (p.converted or p.default is not None or p.fallback):
            where = f"(NOT {blank} AND ({p.final} IS NULL OR {differs}))"
        if skip:
            where += f" AND ({skip}) IS NULL"
        parts.append(f"SELECT {_record_id(plans)} AS RECORD_ID, {lit(p.name)} AS COLUMN_NAME, {orig} AS ORIGINAL, "
                     f"{new} AS NEW_VALUE, {what} AS WHAT_HAPPENED FROM s WHERE {where}")
    if not parts:
        return None
    return (f"{_source_cte(facility, table, plans)}\nSELECT * FROM (\n" + "\nUNION ALL\n".join(parts) +
            f"\n)\nORDER BY RECORD_ID, COLUMN_NAME\nLIMIT {int(limit)}")


def stats_sql(facility: str, table: str, plans: list[ColPlan]) -> tuple[str, list[tuple[int, ColPlan]]]:
    """One read-only query that counts, per column, the values before and after conversion, and the records with errors."""
    changed = [(i, p) for i, p in enumerate(plans) if p.converted or p.limited or p.enum_values or p.fallback or p.default is not None]
    sel = ["COUNT(*) AS ROWS_N"]
    for i, p in changed:
        if p.converted:
            sel += [f"COUNT({q(p.name + '__C')}) AS B{i}", f"COUNT({p.cast}) AS C{i}", f"COUNT({p.final}) AS F{i}"]
            if p.target.kind == "decimal":
                sel.append(f"COUNT_IF({q(p.name + '__C')} RLIKE '^-?[0-9]+[.][0-9]{{{p.target.s + 1},}}$') AS R{i}")
        elif p.limited:
            sel += [f"COUNT({q(p.name)}) AS B{i}", f"COUNT({p.final}) AS F{i}",
                    f"COUNT_IF(LENGTH({q(p.name)}) > {p.target.length}) AS L{i}"]
        elif p.enum_values:   # enum-only: how many values V3 would not accept
            allowed = ", ".join(lit(v.lower()) for v in p.enum_values)
            sel += [f"COUNT({q(p.name)}) AS B{i}", f"COUNT_IF({_enum_bad(p, allowed)}) AS E{i}"]
        else:
            sel.append(f"COUNT({q(p.name)}) AS B{i}")
        if p.default is not None and not p.converted:    # text column: records that were empty and got the default
            sel.append(f"COUNT_IF(NULLIF(TRIM({q(p.name)}), '') IS NULL) AS D{i}")
        if p.fill_test:   # how many records got this value from other columns or another table
            sel.append(f"COUNT_IF({p.fill_test}) AS FB{i}")
    reason, skip = error_reason_sql(plans), skip_reason_sql(plans)
    if reason:
        sel.append(f"COUNT_IF(({reason}) IS NOT NULL{f' AND ({skip}) IS NULL' if skip else ''}) AS HELD_N")
    if skip:
        sel.append(f"COUNT_IF(({skip}) IS NOT NULL) AS SKIP_N")
    return f"{_source_cte(facility, table, plans)}\nSELECT {', '.join(sel)}\nFROM s", changed


# ── 4. verify ────────────────────────────────────────────────────────────────

def verify(cur, facility: str, table: str, plans: list[ColPlan], route: str = "unknown") -> list[dict]:
    """Runs stats_sql and turns the counts into report rows (converted, failed, defaulted, rounded, too long, records with errors) with a status."""
    sql, changed = stats_sql(facility, table, plans)
    if not changed and not any(p.required or p.link or p.parents or p.id_required for p in plans):
        return []
    cur.execute(sql)
    names = [d[0] for d in cur.description]
    row = dict(zip(names, cur.fetchone()))
    rows_n, held, skipped = row["ROWS_N"], row.get("HELD_N", 0) or 0, row.get("SKIP_N", 0) or 0
    report = []
    for i, p in changed:
        before = row[f"B{i}"]
        if p.converted:
            converted, final = row[f"C{i}"], row[f"F{i}"]
            failed, defaulted = before - converted, final - converted
            rounded, too_long = row.get(f"R{i}", 0) or 0, 0
        elif p.limited:
            converted, final, failed, defaulted, rounded = before, row[f"F{i}"], 0, row.get(f"D{i}", 0) or 0, 0
            too_long = row[f"L{i}"]
        else:
            converted, failed, defaulted, rounded, too_long = before, 0, row.get(f"D{i}", 0) or 0, 0, 0
        bad_enum = row.get(f"E{i}", 0) or 0
        filled = row.get(f"FB{i}", 0) or 0
        examples = ""
        if p.converted and failed:       # which original values could not be converted
            cur.execute(f"{_source_cte(facility, table, plans)}\nSELECT DISTINCT {q(p.name)} FROM s "
                        f"WHERE {q(p.name + '__C')} IS NOT NULL AND ({p.cast}) IS NULL LIMIT 5")
            examples = " | ".join(str(r[0]) for r in cur.fetchall())
        share = failed / before if before else 0.0
        status = "OK"
        if bad_enum:
            status = f"NOT ALLOWED ({bad_enum})"
        if too_long:
            status = "TOO LONG"
        if rounded:
            status = "ROUNDED"
        if share > WARN_FAILURE_SHARE:
            status = "WARN: many failures" + (f" (type is a {p.target.source})" if p.target.source in ("guess", "name-rule") else "")
        if failed and p.empty_if_unfit:
            status = f"SET EMPTY ({failed})"
        if failed and p.fail_is_error and held:
            status = "ERROR: value could not be converted"
        if p.date_order == "MIXED":
            status = "ERROR: mixed day/month order" if failed else "MIXED day/month order"
        report.append({"table": table, "route": route, "column": p.name, "from": p.src_type,
                       "to": p.target.label() if not p.enum_values or p.converted or p.limited else "enum", "type_source": p.target.source, "rows": rows_n,
                       "values_before": before, "converted": converted, "failed": failed,
                       "defaulted": defaulted, "filled_from_other_columns": filled, "rounded": rounded,
                       "too_long": too_long, "date_order": p.date_order + (" (assumed)" if p.date_assumed else ""),
                       "records_with_errors": held, "records_skipped": skipped, "failed_examples": examples,
                       "status": status})
    if not report and (held or skipped):
        report.append({"table": table, "route": route, "column": "(required columns)", "from": "", "to": "",
                       "type_source": "v3", "rows": rows_n, "values_before": "", "converted": "", "failed": "",
                       "defaulted": "", "filled_from_other_columns": "", "rounded": "", "too_long": "",
                       "date_order": "", "records_with_errors": held, "records_skipped": skipped, "failed_examples": "",
                       "status": "ERROR" if held else "SKIPPED"})
    return report


def absent_rows(table: str, route: str, columns: list[dict], structure: V3Structure, service: str | None) -> list[dict]:
    """Report rows for columns V3 requires that CLEAN does not have at all (nothing can be filled in here; the mapping needs them)."""
    if not structure or structure.key(table, service) is None:
        return []
    have = {c["name"].lower() for c in columns}
    keys = ("from", "to", "rows", "values_before", "converted", "failed", "defaulted", "filled_from_other_columns",
            "rounded", "too_long", "date_order", "records_with_errors", "records_skipped", "failed_examples")
    return [{"table": table, "route": route, "column": c, "type_source": "v3", **{k: "" for k in keys},
             "status": "V3 REQUIRES THIS COLUMN - NOT IN CLEAN"}
            for c in sorted(structure.required_set(table, service) - have)]


def print_report(rows: list[dict]) -> None:
    """Prints the report rows as a table in the terminal."""
    if not rows:
        print("  nothing to convert (no TEXT column with a different target type)")
        return
    head = f"  {'column':<30}{'to':<16}{'src':<6}{'before':>8}{'ok':>8}{'failed':>8}{'default':>8}{'filled':>8}  status"
    print(head)
    for r in sorted(rows, key=lambda r: -r["failed"] if isinstance(r["failed"], int) else 0):
        print(f"  {r['column'][:29]:<30}{r['to']:<16}{r['type_source']:<6}{r['values_before']!s:>8}"
              f"{r['converted']!s:>8}{r['failed']!s:>8}{r['defaulted']!s:>8}{r['filled_from_other_columns']!s:>8}  {r['status']}")
        if r["failed_examples"]:
            print(f"      values that could not be converted in {r['column']}: {r['failed_examples']}")
    held = max((r["records_with_errors"] or 0) for r in rows)
    if held:
        print(f"  records with errors: {held} of {rows[0]['rows']}")
    skipped = max((r.get("records_skipped") or 0) for r in rows)
    if skipped:
        print(f"  records skipped (left out of the migration): {skipped} of {rows[0]['rows']}")


# ── the run ──────────────────────────────────────────────────────────────────

def read_columns(cur, facility: str, tables: list[str] | None = None) -> dict[str, list[dict]]:
    """Reads the column names and types of the CLEAN tables from Snowflake (every table when none are given)."""
    marks = ", ".join(["%s"] * len(tables or []))
    cur.execute(f"""SELECT table_name, column_name, data_type, is_nullable
                    FROM {SF_DB}.INFORMATION_SCHEMA.COLUMNS
                    WHERE table_schema = %s{f' AND table_name IN ({marks})' if tables else ''}
                    ORDER BY table_name, ordinal_position""",
                (f"{facility.upper()}_{CLEAN_SUFFIX}", *(tables or [])))
    out: dict[str, list[dict]] = {}
    for t, c, dt, nl in cur.fetchall():
        out.setdefault(t, []).append({"name": c, "type": dt, "nullable": nl == "YES"})
    return out


def find_parents(cur, facility: str) -> dict:
    """Finds the parent tables in CLEAN (visits, invoices) with their id column and patient column, so patient_id can be filled and checked."""
    names = sorted({t for _, t in PARENTS})
    cur.execute(f"""SELECT table_name, column_name FROM {SF_DB}.INFORMATION_SCHEMA.COLUMNS
                    WHERE table_schema = %s AND LOWER(table_name) IN ({', '.join(['%s'] * len(names))})""",
                (f"{facility.upper()}_{CLEAN_SUFFIX}", *names))
    found: dict[str, dict] = {}
    for table, column in cur.fetchall():
        found.setdefault(table.lower(), {"name": table, "cols": {}})["cols"][column.lower()] = column
    out = {}
    for key, info in found.items():
        patient = info["cols"].get("patient_id") or info["cols"].get("patient")
        if "id" in info["cols"] and patient:
            out[key] = {"table": f"{sf_schema(facility, CLEAN_SUFFIX)}.{q(info['name'])}",
                        "id": q(info["cols"]["id"]), "value": q(patient)}
    return out


def find_users(all_cols: dict, facility: str) -> dict | None:
    """The CLEAN users table with its id and username columns, for looking up a user column's text; None when there is no such table."""
    for table, cols in all_cols.items():
        if table.lower() == "users":
            names = {c["name"].lower(): c["name"] for c in cols}
            if "id" in names and "username" in names:
                return {"table": f"{sf_schema(facility, CLEAN_SUFFIX)}.{q(table)}", "id": q(names["id"]), "username": q(names["username"])}
    return None


def is_user_column(name: str) -> bool:
    """True for a column that holds a user: created_by, recorded_by, user..."""
    return bool(USER_COLUMN_RE.search(name))


def link_candidates(structure: V3Structure, table: str, service: str | None, columns: list[dict], derived: set) -> dict[str, str]:
    """The columns of a table that may point to another table, with that table's name (V3 foreign keys, or <name>_id pointing to <name>s).
    Columns the mapping builds from other columns are left out: their value is not the parent's id."""
    key = structure.key(table, service) if structure else None
    if not key:
        return {}
    fk_parent = {f["column"].lower(): f["table"].lower() for f in structure.fks.get(key, [])}
    out = {}
    for col in columns:
        name = col["name"].lower()
        if name == "id" or (table.lower(), name) in derived:
            continue
        parent = fk_parent.get(name) or (name[:-3] + "s" if name.endswith("_id") else None)
        if parent:
            out[name] = parent
    return out


def find_links(structure: V3Structure, table: str, service: str | None, columns: list[dict],
               all_cols: dict, facility: str, derived: set) -> dict:
    """Finds the columns of a table that point to another CLEAN table (see link_candidates) whose table exists and has an id column."""
    tables = {t.lower(): t for t in all_cols}
    out = {}
    for name, parent in link_candidates(structure, table, service, columns, derived).items():
        pcols = {c["name"].lower(): c["name"] for c in all_cols.get(tables.get(parent), [])} if parent in tables else {}
        if "id" in pcols:
            out[name] = {"table": parent, "parent": f"{sf_schema(facility, CLEAN_SUFFIX)}.{q(tables[parent])}", "id": q(pcols["id"]),
                         "excluded": []}
    return out


def detect_date_orders(cur, facility: str, table: str, plans: list[ColPlan]) -> dict:
    """Decides, per date column, whether 12/03/2024 is day-first or month-first from its own values; a column with both is MIXED."""
    cols = [p for p in plans if p.converted and p.target.kind in ("date", "timestamp")]
    if not cols:
        return {}
    sel = []
    for i, p in enumerate(cols):
        n = _words(q(p.name + "__C"))
        gate = f"{n} RLIKE '[0-9]{{1,2}} [0-9]{{1,2}} ([0-9]{{2}}|[0-9]{{4}})( .*)?'"
        sel += [f"COUNT_IF({gate} AND TRY_TO_NUMBER(SPLIT_PART({n}, ' ', {k}), 38, 0) > 12) AS X{i}{k}" for k in (1, 2)]
        h = q(p.name + "__C")
        sel += [f"COUNT_IF({h} RLIKE '{_EXCEL}') AS XL{i}", f"COUNT_IF({h} RLIKE '{_EPOCH}') AS EP{i}",
                f"COUNT_IF((LOWER({h}) RLIKE '.*[0-9]+[-/.: ][0-9a-z]+.*|[0-9]{{8}}|.*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec).*') "
                f"AND NOT ({h} RLIKE '{_EXCEL}')) AS DL{i}"]
    cur.execute(f"{_source_cte(facility, table, plans)}\nSELECT {', '.join(sel)}\nFROM s")
    row = cur.fetchone()
    out = {}
    for i, p in enumerate(cols):
        first, second, excel, epoch, dated = row[5 * i:5 * i + 5]
        out[p.name.lower()] = ("EXCEL" if excel and not epoch and not dated else "EPOCH" if epoch and not excel and not dated else
                               "MIXED" if first and second else "DMY" if first else "MDY" if second else "")
    return out


def apply_date_conventions(facility: str, table: str, plans: list[ColPlan], orders: dict) -> tuple[dict, dict]:
    """Fills the date columns whose order the data does not prove from COLUMN_DATE_ORDER, then FACILITY_DATE_ORDER.
    Returns (orders, assumed) where assumed = {column: order}. The data always wins: a column that proves an order or mixes both is left alone."""
    fac, out, assumed = facility.upper(), dict(orders), {}
    for p in plans:
        if not (p.converted and p.target.kind in ("date", "timestamp")):
            continue
        n = p.name.lower()
        want = COLUMN_DATE_ORDER.get((fac, table.lower(), n)) or FACILITY_DATE_ORDER.get(fac)
        if not want:
            continue
        found = out.get(n, "")
        if not found:
            out[n], assumed[n] = want, want
        elif found in ("DMY", "MDY", "MIXED") and found != want:
            print(f"   {n}: the column's own dates show {found}; the {fac} convention ({want}) is not used for it")
    return out, assumed


def compare_and_cast(cur, facility: str, table: str, columns: list[dict], structure: V3Structure,
                     *, print_sql: bool, route: str, service: str | None = None,
                     parents: dict | None = None, links: dict | None = None,
                     skipped_out: dict | None = None, plans_out: list | None = None,
                     users: dict | None = None) -> tuple[list[dict], list[dict], list[dict]]:
    """Checks one table: plans it, counts what converts and what fails, and lists the errors and, for the audit, every changed value and skipped record. Creates nothing in Snowflake."""
    if structure and structure.key(table, service) is None:
        print(f"\n>> {table}: not found in the V3 structure, or the name exists in several V3 services "
              f"and the mapping file does not say which - using GUESSED types for this table")
    plans = plan_columns(table, columns, structure, service, parents, None, links, users)
    if not print_sql:
        orders = detect_date_orders(cur, facility, table, plans)
        orders, assumed = apply_date_conventions(facility, table, plans, orders)
        if any(orders.values()):
            plans = plan_columns(table, columns, structure, service, parents, orders, links, users)
            for p in plans:
                p.date_assumed = p.name.lower() in assumed
            for name, order in orders.items():
                if order in ("DMY", "MDY") and name in assumed:
                    print(f"   {name}: dates are read {'day-first' if order == 'DMY' else 'month-first'} by the {facility.upper()} convention "
                          f"(ASSUMED - no date in this column proves it)")
                elif order:
                    print(f"   {name}: " + {"DMY": "dates are read day-first", "MDY": "dates are read month-first", "EXCEL": "numbers are read as Excel day numbers",
                                 "EPOCH": "numbers are read as Unix time (UTC)"}.get(order, "the column MIXES both orders: each date is read by its own proof, and a date that could be either is an error"))
    if plans_out is not None:
        plans_out[:] = plans
    extra = [p.name for p in plans if p.target.source == "not in V3"]
    if extra:
        print(f"\n>> {table}: {len(extra)} column(s) V3 does not have (not sent as such): "
              f"{', '.join(extra[:12])}{' ...' if len(extra) > 12 else ''}")
    changed = [p for p in plans if p.converted or p.limited or p.enum_values or p.fallback or p.default is not None]
    reason = error_reason_sql(plans)
    print(f"\n>> {facility.upper()}.{table}  [{route}]  {len(plans)} columns, {len(changed)} to convert"
          f"{', required columns checked' if reason else ''}")
    if print_sql:
        for sql in (typed_select_sql(facility, table, plans), error_sample_sql(facility, table, plans, ERROR_SAMPLE_LIMIT),
                    skipped_sample_sql(facility, table, plans, ERROR_SAMPLE_LIMIT),
                    change_sql(facility, table, plans, CHANGE_LOG_LIMIT)):
            if sql:
                print(sql + "\n")
        return [], [], []
    report = verify(cur, facility, table, plans, route)
    report += absent_rows(table, route, columns, structure, service)
    print_report(report)
    errors, changes = [], []
    id_plan = next((p for p in plans if p.name.lower() == "id"), None)
    if skipped_out is not None and id_plan and any(r.get("records_skipped") for r in report):
        cur.execute(f"{_source_cte(facility, table, plans)}\nSELECT DISTINCT {_norm(q(id_plan.name))} FROM s "
                    f"WHERE ({skip_reason_sql(plans)}) IS NOT NULL AND {q(id_plan.name)} IS NOT NULL LIMIT {MAX_CASCADE + 1}")
        ids = [r[0] for r in cur.fetchall()]
        if len(ids) > MAX_CASCADE:
            print(f"  WARNING: more than {MAX_CASCADE} skipped ids in {table}; records that point to the rest are not skipped")
        skipped_out[table.lower()] = ids[:MAX_CASCADE]
    if any(r["records_with_errors"] for r in report):
        cur.execute(error_sample_sql(facility, table, plans, ERROR_SAMPLE_LIMIT))
        errors = [{"table": table, "record_id": rid, "error": why, "original_values": row}
                  for rid, why, row in cur.fetchall()]
        total = max(int(r["records_with_errors"] or 0) for r in report)
        if total > len(errors):
            errors.append({"table": table, "record_id": "(LIST CUT OFF)", "original_values": "",
                           "error": f"only the first {len(errors)} of {total} records with errors are listed"})
    if any(r.get("records_skipped") for r in report):
        cur.execute(skipped_sample_sql(facility, table, plans, ERROR_SAMPLE_LIMIT))
        changes = [{"table": table, "record_id": rid, "column": "(whole record)", "original": row,
                    "new_value": "skipped, not migrated", "what_happened": why} for rid, why, row in cur.fetchall()]
        groups: dict[str, int] = {}
        for c in changes:           # why records were skipped, grouped (ids and values taken out), biggest first
            key = re.sub(r"\b\d+\b", "#", re.sub(r'"[^"]*"', '"..."', c["what_happened"]))
            groups[key] = groups.get(key, 0) + 1
        print("  skipped because:")
        for key, n in sorted(groups.items(), key=lambda kv: -kv[1])[:6]:
            print(f"    {n:>6}  {key}")
        print(f"    (every skipped record, with its original values, is in the audit CHANGES list: what_happened / new_value = 'skipped, not migrated')")
        total = max(int(r.get("records_skipped") or 0) for r in report)
        if total > len(changes):
            changes.append({"table": table, "record_id": "(LIST CUT OFF)", "column": "(whole record)", "original": "",
                            "new_value": "", "what_happened": f"only the first {len(changes)} of {total} skipped records are listed"})
    sql = change_sql(facility, table, plans, CHANGE_LOG_LIMIT)
    if sql:
        cur.execute(sql)
        values = [{"table": table, "record_id": rid, "column": col, "original": old, "new_value": new, "what_happened": what}
                  for rid, col, old, new, what in cur.fetchall()]
        print(f"  values changed in kept records: {len(values)}{' (list is cut off at the limit)' if len(values) >= CHANGE_LOG_LIMIT else ''}")
        if len(values) >= CHANGE_LOG_LIMIT:
            values.append({"table": table, "record_id": "(LIST CUT OFF)", "column": "", "original": "", "new_value": "",
                           "what_happened": f"only the first {CHANGE_LOG_LIMIT} changed values of this table are listed; more exist"})
        changes += values
    return report, errors, changes


class TypingErrors(Exception):
    """Raised when a table has records with errors. .tables = {table: number of records}. One task per table means only that table fails."""

    def __init__(self, message: str, tables: dict[str, int]):
        """Keeps the failed tables and their error counts."""
        super().__init__(message)
        self.tables = tables


def parents_first(tables: list[str], links: dict) -> list[str]:
    """Orders tables so a table comes after the tables its records point to (a loop is broken at the first table left)."""
    left, done, out = list(tables), set(), []
    names = {t.lower() for t in tables}
    while left:
        ready = [t for t in left if all(l["table"] in done or l["table"] not in names or l["table"] == t.lower()
                                        for l in links[t].values())] or [left[0]]
        for t in ready:
            left.remove(t)
            done.add(t.lower())
            out.append(t)
    return out


# AUDIT STORE (Snowflake)
#
# Everything a run reports is stored in AUDIT_SCHEMA instead of files (files vanish with an Airflow container, and are hard to
# search). Tables, all keyed by RUN_ID (+ FACILITY):
#   RUNS, TABLE_STATE   the run and the state of each table (this is what --retry-failed reads)
#   COLUMN_REPORT, ERRORS, CHANGES, LOG   what the check found / did / printed
# A write that fails stops the run with a clear message, before the table is built (the LOG alone is best effort).
# The rows that hold real values (all but RUNS and TABLE_STATE) are deleted after V3_AUDIT_KEEP_DAYS; RUNS and TABLE_STATE
# rows older than that go too, except the latest run of each schema (a retry needs it).
AUDIT_SCHEMA_ENV = "V3_AUDIT_SCHEMA"
DEFAULT_AUDIT_SCHEMA = "HOSPITALS.V3_CAST_AUDIT"
AUDIT_KEEP_DAYS_ENV = "V3_AUDIT_KEEP_DAYS"
DEFAULT_AUDIT_KEEP_DAYS = 90
_AUDIT_TABLES = {
    "RUNS": "RUN_ID STRING, FACILITY STRING, RAW_SCHEMA STRING, CLEAN_SCHEMA STRING, SOURCE STRING, STARTED_AT TIMESTAMP_TZ, "
            "FINISHED_AT TIMESTAMP_TZ, STATUS STRING, RAW_ROWS NUMBER, DEDUP_STATUS STRING",
    "TABLE_STATE": "RUN_ID STRING, FACILITY STRING, TABLE_NAME STRING, STATUS STRING, OBJECT_TYPE STRING, ERROR STRING, UPDATED_AT TIMESTAMP_TZ",
    "COLUMN_REPORT": "RUN_ID STRING, FACILITY STRING, TABLE_NAME STRING, ROUTE STRING, COLUMN_NAME STRING, SOURCE_TYPE STRING, TARGET_TYPE STRING, "
                     "TYPE_SOURCE STRING, ROWS_IN NUMBER, VALUES_BEFORE NUMBER, CONVERTED NUMBER, FAILED NUMBER, DEFAULTED NUMBER, "
                     "FILLED_FROM_OTHER_COLUMNS NUMBER, ROUNDED NUMBER, TOO_LONG NUMBER, DATE_ORDER STRING, RECORDS_WITH_ERRORS NUMBER, "
                     "RECORDS_SKIPPED NUMBER, FAILED_EXAMPLES STRING, STATUS STRING, LOGGED_AT TIMESTAMP_TZ",
    "ERRORS": "RUN_ID STRING, FACILITY STRING, TABLE_NAME STRING, RECORD_ID STRING, ERROR STRING, ORIGINAL_VALUES STRING, LOGGED_AT TIMESTAMP_TZ",
    "CHANGES": "RUN_ID STRING, FACILITY STRING, TABLE_NAME STRING, RECORD_ID STRING, COLUMN_NAME STRING, ORIGINAL STRING, NEW_VALUE STRING, "
               "WHAT_HAPPENED STRING, LOGGED_AT TIMESTAMP_TZ",
    "LOG": "RUN_ID STRING, FACILITY STRING, TABLE_NAME STRING, LOGGED_AT TIMESTAMP_TZ, LINE STRING",
}
_AUDIT_READY: set[str] = set()
_INSERT_CHUNK = 2000


def audit_schema() -> str:
    """The schema the audit tables live in: $V3_AUDIT_SCHEMA, else HOSPITALS.V3_CAST_AUDIT."""
    return (os.getenv(AUDIT_SCHEMA_ENV, "").strip() or DEFAULT_AUDIT_SCHEMA).upper()


def audit_keep_days() -> int:
    """How many days the rows with real values are kept (0 = never delete)."""
    try:
        return max(0, int(os.getenv(AUDIT_KEEP_DAYS_ENV, "").strip() or DEFAULT_AUDIT_KEEP_DAYS))
    except ValueError:
        raise ValueError(f"{AUDIT_KEEP_DAYS_ENV} must be a whole number of days") from None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _num(v):
    """A report number for the NUMBER columns ('' or None -> NULL)."""
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


class _Tee:
    """Passes everything printed on to the real stdout and keeps the finished lines for Audit.flush_log()."""

    def __init__(self, real, audit):
        self.real, self.audit, self.partial = real, audit, ""

    def write(self, text):
        self.real.write(text)
        self.partial += text
        *lines, self.partial = self.partial.split("\n")
        for line in lines:
            self.audit.log_line(line)
        return len(text)

    def flush(self):
        self.real.flush()

    def __getattr__(self, name):
        return getattr(self.real, name)


class Audit:
    """One run's connection to the audit tables: stores the reports, the state and the log. `source` is 'manual' (python flatten_jsons_schemas.py),
    'airflow' (a single table run by the DAG) or 'standalone' (this script on its own)."""

    def __init__(self, cur, facility: str, raw_schema: str | None = None, clean_schema: str | None = None,
                 run_id: str | None = None, source: str = "manual", schema: str | None = None):
        self.cur, self.facility, self.raw_schema, self.clean_schema = cur, facility.upper(), raw_schema, clean_schema
        self.source = source
        self.schema = (schema or audit_schema()).upper()
        self.run_id = run_id or _now().strftime("%Y%m%dT%H%M%S") + ("" if source == "manual" else "-" + uuid.uuid4().hex[:4])
        self.started_at = _now()
        self.current_table: str | None = None
        self._lines: list[tuple] = []

    # setup
    def ensure(self) -> None:
        """Creates the schema and tables the first time (once per process)."""
        if self.schema in _AUDIT_READY:
            return
        try:
            self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            for name, cols in _AUDIT_TABLES.items():
                self.cur.execute(f"CREATE TABLE IF NOT EXISTS {self.schema}.{name} ({cols})")
        except Exception as e:                 # noqa: BLE001
            raise RuntimeError(f"Cannot create or use the audit schema {self.schema}: {e}. The role needs CREATE SCHEMA on the database "
                               f"(or an existing schema it can write to; set {AUDIT_SCHEMA_ENV}). Nothing was changed in CLEAN.") from e
        _AUDIT_READY.add(self.schema)

    def _write(self, statements: list[tuple]) -> None:
        """Runs the statements as one transaction: all or nothing."""
        self.ensure()
        self.cur.execute("BEGIN")
        try:
            for sql, params in statements:
                if isinstance(params, list):
                    for i in range(0, len(params), _INSERT_CHUNK):
                        self.cur.executemany(sql, params[i:i + _INSERT_CHUNK])
                else:
                    self.cur.execute(sql, params)
            self.cur.execute("COMMIT")
        except Exception as e:                 # noqa: BLE001
            try:
                self.cur.execute("ROLLBACK")
            except Exception:                  # noqa: BLE001
                pass
            raise RuntimeError(f"Could not write to the audit schema {self.schema}: {e}. Nothing was changed in CLEAN.") from e

    # runs and table state
    def save_run(self, status: str = "running", raw_rows: int | None = None, dedup: str = "pending", finished: bool = False) -> None:
        """Writes (replaces) this run's row in RUNS."""
        t = f"{self.schema}.RUNS"
        self._write([
            (f"DELETE FROM {t} WHERE RUN_ID = %s AND FACILITY = %s", (self.run_id, self.facility)),
            (f"INSERT INTO {t} (RUN_ID, FACILITY, RAW_SCHEMA, CLEAN_SCHEMA, SOURCE, STARTED_AT, FINISHED_AT, STATUS, RAW_ROWS, DEDUP_STATUS) "
             f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
             (self.run_id, self.facility, self.raw_schema, self.clean_schema, self.source, self.started_at, _now() if finished else None,
              status, raw_rows, dedup))])

    def save_table(self, table: str, status: str, object_type: str | None = None, error: str | None = None) -> None:
        """Writes (replaces) one table's row in TABLE_STATE."""
        t = f"{self.schema}.TABLE_STATE"
        self._write([
            (f"DELETE FROM {t} WHERE RUN_ID = %s AND FACILITY = %s AND TABLE_NAME = %s", (self.run_id, self.facility, table)),
            (f"INSERT INTO {t} (RUN_ID, FACILITY, TABLE_NAME, STATUS, OBJECT_TYPE, ERROR, UPDATED_AT) VALUES (%s, %s, %s, %s, %s, %s, %s)",
             (self.run_id, self.facility, table, status, object_type, error, _now()))])

    def save_pending(self, tables: list[str]) -> None:
        """Writes a `pending` row for each table not yet in TABLE_STATE for this run (one transaction)."""
        if not tables:
            return
        t = f"{self.schema}.TABLE_STATE"
        now = _now()
        self._write([
            (f"DELETE FROM {t} WHERE RUN_ID = %s AND FACILITY = %s AND STATUS = 'pending'", (self.run_id, self.facility)),
            (f"INSERT INTO {t} (RUN_ID, FACILITY, TABLE_NAME, STATUS, OBJECT_TYPE, ERROR, UPDATED_AT) VALUES (%s, %s, %s, %s, %s, %s, %s)",
             [(self.run_id, self.facility, x, "pending", None, None, now) for x in tables])])

    def load_latest(self) -> dict | None:
        """The state of the latest run by hand of this RAW schema, shaped like the in-memory run state, or None when there is none."""
        self.ensure()
        self.cur.execute(f"SELECT RUN_ID, STARTED_AT, RAW_ROWS, DEDUP_STATUS FROM {self.schema}.RUNS "
                         f"WHERE RAW_SCHEMA = %s AND SOURCE = 'manual' ORDER BY STARTED_AT DESC LIMIT 1", (self.raw_schema,))
        row = self.cur.fetchone()
        if not row:
            return None
        run_id, started, raw_rows, dedup = row
        self.cur.execute(f"SELECT TABLE_NAME, STATUS, OBJECT_TYPE, ERROR, UPDATED_AT FROM {self.schema}.TABLE_STATE "
                         f"WHERE RUN_ID = %s AND FACILITY = %s", (run_id, self.facility))
        tables = {}
        for name, status, obj, error, updated in self.cur.fetchall():
            info = {"status": status}
            if updated:
                info["updated_at"] = updated.isoformat(timespec="seconds") if hasattr(updated, "isoformat") else str(updated)
            if obj:
                info["object"] = obj
            if error:
                info["error"] = error
            tables[name] = info
        self.run_id = run_id
        if isinstance(started, datetime):
            self.started_at = started
        return {"version": 2, "raw_schema": self.raw_schema, "run_id": run_id,
                "started_at": started.isoformat(timespec="seconds") if hasattr(started, "isoformat") else str(started),
                "raw_rows": raw_rows, "dedup": dedup or "pending", "tables": tables}

    # what the check found
    def save_reports(self, rows: list[dict], errors: list[dict], changes: list[dict]) -> dict:
        """Stores the column report, the errors and the changes of the tables in them. A table checked again in the same run
        replaces its own earlier rows. Returns how many rows went where."""
        tables = sorted({r["table"] for r in rows} | {e["table"] for e in errors} | {c["table"] for c in changes})
        now, rid, fac, sch = _now(), self.run_id, self.facility, self.schema
        statements = []
        for name in ("COLUMN_REPORT", "ERRORS", "CHANGES"):
            for t in tables:
                statements.append((f"DELETE FROM {sch}.{name} WHERE RUN_ID = %s AND FACILITY = %s AND TABLE_NAME = %s", (rid, fac, t)))
        if rows:
            statements.append((
                f"INSERT INTO {sch}.COLUMN_REPORT (RUN_ID, FACILITY, TABLE_NAME, ROUTE, COLUMN_NAME, SOURCE_TYPE, TARGET_TYPE, TYPE_SOURCE, ROWS_IN, "
                f"VALUES_BEFORE, CONVERTED, FAILED, DEFAULTED, FILLED_FROM_OTHER_COLUMNS, ROUNDED, TOO_LONG, DATE_ORDER, RECORDS_WITH_ERRORS, "
                f"RECORDS_SKIPPED, FAILED_EXAMPLES, STATUS, LOGGED_AT) VALUES ({', '.join(['%s'] * 22)})",
                [(rid, fac, r["table"], r.get("route"), r.get("column"), r.get("from"), r.get("to"), r.get("type_source"), _num(r.get("rows")),
                  _num(r.get("values_before")), _num(r.get("converted")), _num(r.get("failed")), _num(r.get("defaulted")),
                  _num(r.get("filled_from_other_columns")), _num(r.get("rounded")), _num(r.get("too_long")), r.get("date_order"),
                  _num(r.get("records_with_errors")), _num(r.get("records_skipped")), r.get("failed_examples"), r.get("status"), now)
                 for r in rows]))
        if errors:
            statements.append((f"INSERT INTO {sch}.ERRORS (RUN_ID, FACILITY, TABLE_NAME, RECORD_ID, ERROR, ORIGINAL_VALUES, LOGGED_AT) "
                               f"VALUES (%s, %s, %s, %s, %s, %s, %s)",
                               [(rid, fac, e["table"], str(e.get("record_id")), e.get("error"), e.get("original_values"), now) for e in errors]))
        if changes:
            statements.append((f"INSERT INTO {sch}.CHANGES (RUN_ID, FACILITY, TABLE_NAME, RECORD_ID, COLUMN_NAME, ORIGINAL, NEW_VALUE, WHAT_HAPPENED, LOGGED_AT) "
                               f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               [(rid, fac, c["table"], str(c.get("record_id")), c.get("column"), c.get("original"), c.get("new_value"),
                                 c.get("what_happened"), now) for c in changes]))
        if statements:
            self._write(statements)
        out = {"report": len(rows), "errors": len(errors), "changes": len(changes)}
        print(f"Stored in {self.schema} (run {rid}): {out['report']} report rows, {out['errors']} errors, {out['changes']} changes/skips")
        if any(c.get("new_value") == "skipped, not migrated" for c in changes):
            print(f"  Why records were skipped: SELECT * FROM {sch}.CHANGES WHERE RUN_ID = '{rid}' AND FACILITY = '{fac}' "
                  f"AND NEW_VALUE = 'skipped, not migrated'")
        return out

    # the log
    def log_line(self, line: str) -> None:
        if line.strip():
            self._lines.append((self.run_id, self.facility, self.current_table, _now(), line[:4000]))

    def flush_log(self) -> None:
        """Writes the printed lines kept so far. Best effort: a failure here is only a warning (the lines are still on the console)."""
        lines, self._lines = self._lines, []
        if not lines:
            return
        try:
            self._write([(f"INSERT INTO {self.schema}.LOG (RUN_ID, FACILITY, TABLE_NAME, LOGGED_AT, LINE) VALUES (%s, %s, %s, %s, %s)", lines)])
        except Exception as e:                 # noqa: BLE001
            sys.__stderr__.write(f"WARNING: the log could not be stored in {self.schema}.LOG: {e}\n")

    @contextlib.contextmanager
    def capture_log(self):
        """Keeps what is printed inside the block (and still prints it); call flush_log() to store it."""
        real = sys.stdout
        tee = _Tee(real, self)
        sys.stdout = tee
        try:
            yield self
        finally:
            sys.stdout = real
            if tee.partial:
                self.log_line(tee.partial)
            self.flush_log()

    # clean-up
    def prune(self, days: int | None = None) -> None:
        """Deletes rows older than the keep period (see AUDIT STORE). Best effort: a failure is only a warning."""
        days = audit_keep_days() if days is None else days
        if not days:
            return
        sch, cutoff = self.schema, _now() - timedelta(days=days)
        old_runs = (f"SELECT RUN_ID, FACILITY FROM {sch}.RUNS WHERE STARTED_AT < %s AND (RUN_ID, FACILITY) NOT IN "
                    f"(SELECT MAX_BY(RUN_ID, STARTED_AT), FACILITY FROM {sch}.RUNS GROUP BY COALESCE(RAW_SCHEMA, FACILITY), FACILITY)")
        try:
            stmts = [(f"DELETE FROM {sch}.{name} WHERE LOGGED_AT < %s", (cutoff,)) for name in ("COLUMN_REPORT", "ERRORS", "CHANGES", "LOG")]
            stmts.append((f"DELETE FROM {sch}.TABLE_STATE WHERE (RUN_ID, FACILITY) IN ({old_runs})", (cutoff,)))
            stmts.append((f"DELETE FROM {sch}.RUNS WHERE STARTED_AT < %s AND (RUN_ID, FACILITY) NOT IN "
                          f"(SELECT MAX_BY(RUN_ID, STARTED_AT), FACILITY FROM {sch}.RUNS GROUP BY COALESCE(RAW_SCHEMA, FACILITY), FACILITY)", (cutoff,)))
            self._write(stmts)
        except Exception as e:                 # noqa: BLE001
            print(f"WARNING: old audit rows were not cleaned up: {e}")


def check_table_for_flatten(cur, facility: str, table: str, source: str, columns: list[dict], service: str, *,
                            structure: V3Structure, derived: set | frozenset = frozenset(), raw_parents: dict | None = None,
                            audit: Audit | None = None, users: dict | None = None) -> dict:
    """Used by the flatten step. Checks one table that has been read, as text, into the temporary table `source`
    (columns = its names and types: TEXT, or VARIANT for nested values). Reads only; creates nothing.
    raw_parents = {parent table (lower case): {"table": SQL name, "id": quoted id column, "value": quoted patient column or None}}.
    users = {"table": SQL name, "id": quoted id column, "username": quoted username column}: where a user column's text (created_by = 'admin') is looked up.
    Returns {"ok", "select_sql" (the finished typed SELECT, set only when ok), "failed_records", "skipped_records", "errors", "changes"}.
    The report, errors and changes are stored in the audit schema first (`audit`; a single-table Audit is made when none is given),
    so a table is never built unless its findings are on record."""
    raw_parents = raw_parents or {}
    parents = {t: {k: v for k, v in info.items() if k in ("table", "id", "value")}
               for t, info in raw_parents.items() if info.get("value") and t in {p for _, p in PARENTS}}
    links = {}
    for col, l in find_links(structure, table, service, columns, {t: [{"name": "id"}] for t in raw_parents}, facility, derived).items():
        links[col] = {**l, "parent": raw_parents[l["table"]]["table"], "id": raw_parents[l["table"]]["id"]}
    SOURCE_OVERRIDE[(facility.upper(), table.lower())] = source
    plans: list[ColPlan] = []
    try:
        rows, errors, changes = compare_and_cast(cur, facility, table, columns, structure, print_sql=False, route="mapping",
                                                 service=service, parents=parents, links=links, plans_out=plans, users=users)
        failed = max([int(r.get("records_with_errors") or 0) for r in rows] or [0])
        skipped = max([int(r.get("records_skipped") or 0) for r in rows] or [0])
        select_sql = None if failed else typed_select_sql(facility, table, plans)
    finally:
        SOURCE_OVERRIDE.pop((facility.upper(), table.lower()), None)
    if audit is None:
        audit = Audit(cur, facility, source="standalone")
        audit.save_run("single table", finished=True)
    audit.save_reports(rows, errors, changes)
    return {"ok": not failed, "select_sql": select_sql, "failed_records": failed, "skipped_records": skipped,
            "errors": errors, "changes": changes}


def run(facility: str, tables: list[str] | None = None, conf: dict | None = None,
        structure_path: str | None = None, skip: list[str] | None = None,
        mapping_path: str | None = None, print_sql: bool = False) -> list[dict]:
    """Entry point for the command line and the flatten step: checks every table given, stores what it found in the audit
    schema (nothing is stored with print_sql), then raises TypingErrors if any has errors."""
    structure = get_v3_structure(structure_path)
    print(f"V3 structure: {len(structure.cols)} tables loaded from {structure.source}" if structure else
          "V3 structure: NOT FOUND - target types are GUESSES from column names (verified = false)")
    mapped, derived = load_mapping(facility, mapping_path)
    conn = _connect()
    all_rows: list[dict] = []
    all_errors: list[dict] = []
    all_changes: list[dict] = []
    try:
        cur = conn.cursor()
        audit = None if print_sql else Audit(cur, facility, source="standalone")
        if audit:
            audit.save_run()
        with audit.capture_log() if audit else contextlib.nullcontext():
            chosen = choose_tables(cur, facility, tables, conf, skip)
            left_out = [t for t in chosen if t.lower() in NOT_TYPE_CHECKED]
            if left_out:
                print(f"  left out of the type check (not migrated): {', '.join(left_out)}")
            chosen = [t for t in chosen if t.lower() not in NOT_TYPE_CHECKED]
            if not chosen:
                print("No tables to process.")
                if audit:
                    audit.save_run("nothing to do", finished=True)
                return []
            all_cols = read_columns(cur, facility)
            cols = {t: all_cols[t] for t in chosen if t in all_cols}
            parents = find_parents(cur, facility)
            users = find_users(all_cols, facility)
            links_by = {t: find_links(structure, t, mapped.get(t.lower()), cols[t], all_cols, facility, derived) for t in chosen if t in cols}
            skipped: dict[str, list] = {}
            for table in parents_first(list(links_by), links_by):
                if audit:
                    audit.current_table = table
                service = mapped.get(table.lower())               # the mapping file names the V3 table and service
                links = {c: {**l, "excluded": skipped.get(l["table"], [])} for c, l in links_by[table].items()}
                rows, errors, changes = compare_and_cast(
                    cur, facility, table, cols[table], structure, print_sql=print_sql,
                    route="mapping" if service else "not in mapping", service=service, parents=parents,
                    links=links, skipped_out=skipped, users=users)
                all_rows += rows
                all_errors += errors
                all_changes += changes
            if audit:
                audit.current_table = None
                audit.save_reports(all_rows, all_errors, all_changes)
        # a table with even one error FAILS: it must not be migrated. Other tables are not affected.
        failed: dict[str, int] = {}
        for r in all_rows:
            if r.get("records_with_errors"):
                failed[r["table"]] = max(failed.get(r["table"], 0), int(r["records_with_errors"]))
        if audit:
            audit.save_run(f"failed: {len(failed)} table(s)" if failed else "ok", finished=True)
            audit.prune()
        cur.close()
    finally:
        conn.close()
    if failed:
        listing = ", ".join(f"{t} ({n} record{'s' if n != 1 else ''})" for t, n in failed.items())
        raise TypingErrors(f"{len(failed)} table(s) have errors and must not be migrated: {listing}", failed)
    return all_rows


def main() -> None:
    """Command line: reads the options, runs, and exits with an error if any table has errors."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser(description="Check that a facility's CLEAN tables fit V3's types: convert, default, and list the records with errors.")
    ap.add_argument("--facility", "-f", required=True, help="e.g. WEMA_MEDICAL (reads WEMA_MEDICAL_CLEAN)")
    ap.add_argument("--table", "-t", nargs="+", help="only these tables (default: every table in CLEAN)")
    ap.add_argument("--skip", nargs="+", default=[], help="tables to leave out, e.g. ones already migrated")
    ap.add_argument("--structure", help="V3's structure (default: dags/jsons/v3-schema.json)")
    ap.add_argument("--mapping", help="the mapping JSON (default: dags/jsons/<facility>_data_mappings.json)")
    ap.add_argument("--print-sql", action="store_true", help="print the SQL and stop (reads column names only)")
    a = ap.parse_args()
    try:
        run(a.facility, tables=a.table, structure_path=a.structure, skip=a.skip,
            mapping_path=a.mapping, print_sql=a.print_sql)
    except TypingErrors as e:
        print(f"\nFAILED: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
