#!/usr/bin/env python3
"""
cast_clean_to_v3_types.py - standardise, cast and check columns against V3's types.

It is meant to be called while <FACILITY>_CLEAN is being built (by the flatten step), so CLEAN holds the
final, V3-typed data. It creates NO new schema, view or table. It only reads, checks, reports and FAILS.
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

OUTPUT in output/typing/ (all start with <FACILITY>_<time>):
  .csv           one row per column: what was converted, defaulted, filled, failed
  _errors.csv    records with errors: table, record id, why, original values. A table with errors makes the script fail.
  _changes.csv   audit list: every value changed or left empty, and every record skipped (a record it points to is
                 missing), with the original, the new value and what happened

V3 STRUCTURE: dags/jsons/v3-schema.json (or --structure <path>), the AI-mapper export (tables, defaults, models.required).
It knows each table's service, nullability, defaults, enum values and required columns.

WITHOUT a structure file the target types are GUESSES from column names (id, *_id, *_at, *_date,
is_*, amounts, vitals). Every report row says "guess" and the high-failure warning is
the way to spot a wrong guess. Do not point the migration at guessed output.

ENV: SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
     SNOWFLAKE_SCHEMA, SNOWFLAKE_PRIVATE_KEY_PATH  (same .env as the rest of the repo)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

SF_DB = os.getenv("TYPED_SF_DB", "HOSPITALS")      # same database snowflake_to_v3_migration reads
CLEAN_SUFFIX = "CLEAN"
ERROR_SAMPLE_LIMIT = 5000               # records with errors or skipped listed per table
CHANGE_LOG_LIMIT = 200000               # changed values listed per table

# ── settings you may want to change ──────────────────────────────────────────

# DD/MM/YYYY (Kenya) is read day-first. Set False to read MM/DD/YYYY instead.
DAY_FIRST = True

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

def _blank(x: str) -> str:
    """SQL test: the value is empty (NULL or blank)."""
    return f"({x} IS NULL OR TRIM({x}) = '')"


def _two_words(x: str) -> str:
    """SQL test: the value has two or more words."""
    return f"REGEXP_COUNT(TRIM({x}), '[^ ]+') >= 2"


# Fill an empty value from other columns of the same record. (table, column) -> (columns the rule uses, SQL builder).
# patients: the old system left one name field empty and put the full name in another one.
FALLBACK_RULES = {
    ("patients", "first_name"): (("first_name", "middle_name", "last_name"), lambda c:
        f"CASE WHEN {_blank(c['first_name'])} AND {_two_words(c['last_name'])} "
        f"THEN SPLIT_PART(TRIM({c['last_name']}), ' ', 1) "
        f"WHEN {_blank(c['last_name'])} AND {_blank(c['middle_name'])} AND {_two_words(c['first_name'])} "
        f"THEN SPLIT_PART(TRIM({c['first_name']}), ' ', 1) ELSE {c['first_name']} END"),
    ("patients", "last_name"): (("first_name", "middle_name", "last_name"), lambda c:
        f"CASE WHEN {_blank(c['first_name'])} AND {_two_words(c['last_name'])} "
        f"THEN TRIM(REGEXP_REPLACE(TRIM({c['last_name']}), '^[^ ]+ +', '')) "
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
                  ("patients", "first_name"): "Not given", ("patients", "last_name"): "Not given"}

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
        return Target("int", source="v3")
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


def get_v3_structure(path: str | None = None) -> V3Structure:
    """Reads v3-schema.json into a V3Structure. No path = dags/jsons/v3-schema.json next to this script; no file at all = an empty structure."""
    v = V3Structure()
    default = Path(__file__).resolve().parent / "dags" / "jsons" / "v3-schema.json"
    path = path or (str(default) if default.is_file() else None)
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
    path = Path(path) if path else Path(__file__).resolve().parent / "dags" / "jsons" / f"{facility.lower()}_data_mappings.json"
    if not path.is_file():
        print("Mapping file: none found - tables are matched to V3 by name only")
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
    x = expr or q(col)
    tokens = ", ".join(lit(t) for t in NULL_TOKENS)
    return f"CASE WHEN LOWER(TRIM({x})) IN ({tokens}) THEN NULL ELSE TRIM({x}) END"


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


def _month_text(t: str) -> str:
    """SQL: a month as text, from a number or from a name ('jun', 'june')."""
    whens = " ".join(f"WHEN '{m}' THEN '{i}'" for i, m in enumerate(_MONTHS, 1))
    return f"CASE WHEN {t} RLIKE '[a-z]+' THEN CASE LEFT({t}, 3) {whens} END ELSE {t} END"


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
    return (f"CASE WHEN {c} RLIKE '{_EXCEL}' THEN DATEADD(second, ROUND(TRY_TO_DOUBLE({c}) * 86400, 0), TO_TIMESTAMP_NTZ('1899-12-30 00:00:00')) "
            f"WHEN {c} RLIKE '{_EPOCH_S}' THEN DATEADD(second, TRY_TO_NUMBER({c}, 38, 0), TO_TIMESTAMP_NTZ('1970-01-01 00:00:00')) "
            f"WHEN {c} RLIKE '{_EPOCH_MS}' THEN DATEADD(second, TRY_TO_NUMBER({c}, 38, 0) / 1000, TO_TIMESTAMP_NTZ('1970-01-01 00:00:00')) END")


def _date_sql(c: str, day_first: bool, numbers: str = "") -> str:
    """SQL for a date written as 2024-06-15, 2024/06/15, 15/06/2024, 15-06-24, 15.06.2024, 15 Jun 2024, 15th June 2024, June 15 2024, 20240615, an Excel day count or Unix time."""
    n = _words(c)
    p = lambda k: f"SPLIT_PART({n}, ' ', {k})"
    day, month = (p(1), p(2)) if day_first else (p(2), p(1))
    yy, tail = "([0-9]{2}|[0-9]{4})", "( .*)?"
    return (f"CASE WHEN {c} RLIKE '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}.*' THEN TRY_TO_DATE(LEFT({c}, 10), 'YYYY-MM-DD') "
            f"WHEN {n} RLIKE '[0-9]{{4}} [0-9]{{1,2}} [0-9]{{1,2}}{tail}' THEN {_make_date(p(1), _month_text(p(2)), p(3))} "
            f"WHEN {n} RLIKE '[0-9]{{1,2}} [0-9]{{1,2}} {yy}{tail}' THEN {_make_date(_year_text(p(3)), _month_text(month), day)} "
            f"WHEN {n} RLIKE '[0-9]{{1,2}} [a-z]{{3,9}} {yy}{tail}' THEN {_make_date(_year_text(p(3)), _month_text(p(2)), p(1))} "
            f"WHEN {n} RLIKE '[a-z]{{3,9}} [0-9]{{1,2}} {yy}{tail}' THEN {_make_date(_year_text(p(3)), _month_text(p(1)), p(2))} "
            f"WHEN {c} RLIKE '^[0-9]{{8}}$' THEN TRY_TO_DATE({c}, 'YYYYMMDD') "
            + (f"WHEN {c} RLIKE '{_NUMBER_KINDS[numbers]}' THEN CAST({_number_stamp(c)} AS DATE) " if numbers else "") + "END")


def _timestamp_sql(c: str, day_first: bool, numbers: str = "") -> str:
    """SQL for a date and time: ISO, or 15/06/2024 14:30[:00] with 24-hour or AM/PM time; a date alone becomes midnight."""
    d = f"TO_VARCHAR({_date_sql(c, day_first)})"
    first = f"WHEN {c} RLIKE '{_NUMBER_KINDS[numbers]}' THEN {_number_stamp(c)} " if numbers else ""
    tm = f"TRIM(REGEXP_REPLACE({c}, '^[^ ]*', ''))"
    both = f"{d} || ' ' || {tm}"
    return (f"CASE {first}"
            f"WHEN {c} RLIKE '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}([ T].*)?$' THEN TRY_TO_TIMESTAMP_NTZ({c}) "
            f"WHEN {c} RLIKE '[0-9]{{1,2}}[-/.][0-9]{{1,2}}[-/.]([0-9]{{2}}|[0-9]{{4}})( .*)?' THEN COALESCE("
            f"TRY_TO_TIMESTAMP_NTZ({both}), TRY_TO_TIMESTAMP_NTZ({both}, 'YYYY-MM-DD HH12:MI AM'), "
            f"TRY_TO_TIMESTAMP_NTZ({both}, 'YYYY-MM-DD HH12:MI:SS AM'), "
            f"IFF({tm} = '', TRY_TO_TIMESTAMP_NTZ({d}, 'YYYY-MM-DD'), NULL)) "
            f"ELSE IFF({c} RLIKE '.*:.*', NULL, CAST({_date_sql(c, day_first)} AS TIMESTAMP_NTZ)) END")


def cast_sql(kind: str, c: str, t: Target, day_first: bool = DAY_FIRST, numbers: str = "") -> str:
    """SQL that converts a tidied value to its V3 type. A value that does not fit becomes empty and is counted."""
    if kind == "int":
        return f"CASE WHEN {c} RLIKE '^-?[0-9]+([.]0+)?$' THEN TRY_TO_NUMBER({c}, 38, 0) END"
    if kind == "decimal":
        return f"CASE WHEN {c} RLIKE '^-?[0-9]+([.][0-9]+)?$' THEN TRY_TO_NUMBER({c}, {t.p}, {t.s}) END"
    if kind == "float":
        return f"CASE WHEN {c} RLIKE '^-?[0-9]+([.][0-9]+)?$' THEN TRY_TO_DOUBLE({c}) END"
    if kind == "flag":
        return (f"CASE LOWER({c}) WHEN 'true' THEN 1 WHEN 'yes' THEN 1 WHEN 'y' THEN 1 WHEN '1' THEN 1 "
                f"WHEN 'false' THEN 0 WHEN 'no' THEN 0 WHEN 'n' THEN 0 WHEN '0' THEN 0 END")
    if kind == "date":
        return _date_sql(c, day_first, numbers)
    if kind == "timestamp":
        return _timestamp_sql(c, day_first, numbers)
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
    day_first: bool = DAY_FIRST        # how this date column's 12/03/2024 is read
    date_order: str = ""               # "DMY", "MDY", "MIXED", or "EXCEL" / "EPOCH" (the column has numbers of that kind and no other date format) when the column's own values show it
    strict: bool = False               # the column mixes day-first and month-first dates
    zero_default: bool = False         # the default is the general 0 for numbers, not a listed default
    fail_is_error: bool = False        # a value that cannot be converted is an error (not replaced by a listed default)
    empty_if_unfit: bool = False       # a value that cannot be converted is left empty and listed instead of failing the table
    link: dict | None = None           # the parent table this column points to; a record whose parent is missing is skipped
    spelled: bool = False              # allowed-word column: a word differing only in capitals is written as V3 spells it


def _parent_sql(name: str, ptr: str, info: dict) -> dict:
    """SQL pieces for one parent: the single value it gives, how many different values it has, how many parent rows match."""
    val = f"TRIM(x.{info['value']})"
    where = f"FROM {info['table']} x WHERE TRIM(x.{info['id']}) = TRIM(t.{ptr})"
    return {"value": f"(SELECT IFF(COUNT(DISTINCT {val}) = 1, MAX({val}), NULL) {where})",
            "count": f"(SELECT COUNT(DISTINCT {val}) {where})"}


def plan_columns(table: str, columns: list[dict], structure: V3Structure, service: str | None = None,
                 parents: dict | None = None, date_orders: dict | None = None,
                 links: dict | None = None) -> list[ColPlan]:
    """Builds one ColPlan per column of a table: finds its V3 type, then sets up the conversion, length check, allowed-words check, default and parent checks."""
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
        p.protected = bool(NEVER_DEFAULT_RE.search(name)) or name.lower() == "payment_method"
        p.required = tgt.source == "v3" and name.lower() in must_have
        is_text = src in TEXT_TYPES
        if is_text and tgt.kind == "text" and tgt.enum_values:
            p.enum_values = tgt.enum_values
            words = " ".join(f"WHEN {lit(v.lower())} THEN {lit(v)}" for v in tgt.enum_values)
            p.final, p.spelled = f"CASE LOWER(TRIM({q(name)})) {words} ELSE {q(name)} END", True
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
                p.day_first = DAY_FIRST if p.date_order not in ("DMY", "MDY") else p.date_order == "DMY"
                p.strict = p.date_order == "MIXED"
            if parents and name.lower() == "patient_id" and tgt.kind == "int":
                for ptr, ptable in PARENTS:        # the patient is also known from the visit or invoice this record belongs to
                    if ptr in quoted and ptable != table.lower() and ptable in parents:
                        p.parents.append({"pointer": ptr, "column": quoted[ptr], "table": ptable, **_parent_sql(name, quoted[ptr], parents[ptable])})
                if p.parents:
                    p.own = p.clean
                    p.clean = f"COALESCE({p.own}, {', '.join(clean_sql(name, pa['value']) for pa in p.parents)})"
                    p.fallback, p.fill_test = True, f"({p.own}) IS NULL AND {q(name + '__C')} IS NOT NULL"
            helper = q(name + "__C")
            p.cast = cast_sql(tgt.kind, helper, tgt, p.day_first, p.date_order if p.date_order in _NUMBER_KINDS else "")
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
        zero_rule = (tgt.source == "v3" and tgt.kind in ("int", "decimal", "float") and tgt.nullable is False
                     and not ID_RE.search(name) and not MEDICAL_RE.search(name))
        from_zero = default is None and zero_rule
        if from_zero:
            default = 0
        if (default is not None and tgt.source == "v3" and (not p.protected or zero_rule)
                and (tgt.nullable is False or p.required or name.lower() in ALWAYS_DEFAULT)):
            p.default, p.zero_default = default, from_zero
            base = f"NULLIF(TRIM({p.final}), '')" if tgt.kind == "text" and is_text else p.final   # blank text counts as empty
            p.final = f"COALESCE({base}, {lit(p.default)})"
        p.empty_if_unfit = p.converted and (table.lower(), name.lower()) in EMPTY_WHEN_UNFIT and tgt.nullable is not False
        p.fail_is_error = (p.converted and not p.empty_if_unfit
                           and not (p.default is not None and not p.zero_default))
        plans.append(p)
    return plans


def _enum_bad(p: ColPlan, allowed: str) -> str:
    """SQL test: a value is present but is not one of the words V3 allows (capital letters ignored)."""
    c = q(p.name)
    return f"{c} IS NOT NULL AND TRIM({c}) <> '' AND LOWER(TRIM({c})) NOT IN ({allowed})"


def _missing_test(p: ColPlan) -> str:
    """SQL test: the value is empty (NULL or blank)."""
    if p.converted:
        return f"{q(p.name + '__C')} IS NULL"
    return f"({p.final} IS NULL OR TRIM(TO_VARCHAR({p.final})) = '')"


def _cast_failed_reason(p: ColPlan) -> str:
    """SQL: the error text for a value that could not be converted, naming the column, the value and the type."""
    helper, label = q(p.name + "__C"), p.target.label()
    if p.target.kind == "decimal":
        verb = f"IFF({helper} RLIKE '^-?[0-9]+([.][0-9]+)?$', ' does not fit ', ' is not a valid ')"
    else:
        verb = "' cannot be converted to '"
    note = " (this column has both day-first and month-first dates)" if p.strict and p.date_order == "MIXED" else ""
    tail = lit(label + note)
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
            parts.append(f"IFF({_enum_bad(p, allowed)}, {lit(p.name + ' (not an allowed value)')}, NULL)")
        if p.fail_is_error:
            parts.append(_cast_failed_reason(p))
    return _join_reasons(parts)


def skip_reason_sql(plans: list[ColPlan]) -> str | None:
    """SQL that lists why a record is left out: a record it points to (visit, invoice, patient...) does not exist, or its patient disagrees with its visit. Empty = the record is kept."""
    parts = []
    for p in plans:
        if p.link:
            value = q(p.name)
            parts.append(f"IFF({q(p.name + '__L')} = 0 AND NOT {_blank(value)}, {lit(p.name + ' ')} || {value} || "
                         f"{lit(' not found in ' + p.link['table'])}, NULL)")
        if p.parents:
            if p.required and p.default is None:
                parts.append(f"IFF({_missing_test(p)}, {lit(p.name + ' (missing, and not found through the visit or invoice)')}, NULL)")
            parts += _parent_reasons(p)
    return _join_reasons(parts)


def _source_cte(facility: str, table: str, plans: list[ColPlan]) -> str:
    """First part of every query: reads the CLEAN table and adds a tidied copy (<COLUMN>__C) of each column to be converted."""
    items = [f"{p.clean} AS {q(p.name + '__C')}" for p in plans if p.converted]
    for p in plans:
        if p.parents:
            items.append(f"{p.own} AS {q(p.name + '__O')}")
            for i, pa in enumerate(p.parents):
                items += [f"{pa['value']} AS {q(f'{p.name}__P{i}')}", f"{pa['count']} AS {q(f'{p.name}__N{i}')}"]
        if p.link:
            items.append(f"(SELECT COUNT(*) FROM {p.link['parent']} x WHERE TRIM(x.{p.link['id']}) = TRIM(t.{q(p.name)})) "
                         f"AS {q(p.name + '__L')}")
    helpers = ", ".join(items)
    src = f"{sf_schema(facility, CLEAN_SUFFIX)}.{q(table)}"
    return f"WITH s AS (SELECT t.*{', ' + helpers if helpers else ''} FROM {src} t)"


def _record_id(plans: list[ColPlan]) -> str:
    """SQL for the record's id as text (NULL when the table has no id column)."""
    id_col = next((p.name for p in plans if p.name.lower() == "id"), None)
    return f"TO_VARCHAR({q(id_col)})" if id_col else "NULL"


def typed_select_sql(facility: str, table: str, plans: list[ColPlan]) -> str:
    """SQL for the finished data of one table: every column converted to its V3 type, defaults filled in, skipped records left out."""
    items = [f"{p.final} AS {q(p.name)}" if p.final != q(p.name) else q(p.name) for p in plans]
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
            f"FROM s\nWHERE ({reason}) IS NOT NULL{extra}\nLIMIT {int(limit)}")


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
            differs, label = f"{orig} NOT IN ('0', '1')", "yes/no converted to 1/0"
        elif kind in ("date", "timestamp"):
            differs, label = f"NOT ({orig} RLIKE '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}.*')", "date written in another format"
        else:
            differs, label = f"{new} IS DISTINCT FROM {orig}", "changed"
        if p.fallback:
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
            unreadable = f"WHEN {p.cast} IS NULL THEN '{'unreadable value: error, table fails' if p.fail_is_error else 'unreadable value replaced by default'}' "
        else:
            unreadable = ""
        placeholder = f"WHEN {q(p.name + '__C')} IS NULL THEN 'placeholder text (n/a, -, null) treated as empty' " if p.converted else ""
        emptied = ("moved to another column" if p.fallback and not p.converted
                   else "unreadable value: error, table fails" if p.fail_is_error
                   else "value could not be converted, left empty")
        what = (f"CASE WHEN {blank} THEN {filled} {unreadable}{placeholder}WHEN {p.final} IS NULL THEN "
                f"{lit(emptied)} ELSE {lit(label)} END")
        where = f"(({blank} AND {new} IS NOT NULL) OR (NOT {blank} AND ({p.final} IS NULL OR {differs})))"
        if p.spelled and not (p.converted or p.default is not None or p.fallback):
            where = f"(NOT {blank} AND ({p.final} IS NULL OR {differs}))"
        if skip:
            where += f" AND ({skip}) IS NULL"
        parts.append(f"SELECT {_record_id(plans)} AS RECORD_ID, {lit(p.name)} AS COLUMN_NAME, {orig} AS ORIGINAL, "
                     f"{new} AS NEW_VALUE, {what} AS WHAT_HAPPENED FROM s WHERE {where}")
    if not parts:
        return None
    return f"{_source_cte(facility, table, plans)}\n" + "\nUNION ALL\n".join(parts) + f"\nLIMIT {int(limit)}"


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
    if not changed and not any(p.required or p.link or p.parents for p in plans):
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
                       "too_long": too_long, "date_order": p.date_order,
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
        print(f"  records skipped (a record they point to is missing): {skipped} of {rows[0]['rows']}")


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


def find_links(structure: V3Structure, table: str, service: str | None, columns: list[dict],
               all_cols: dict, facility: str, derived: set) -> dict:
    """Finds the columns of a table that point to another CLEAN table (V3 foreign keys, or <name>_id pointing to the table <name>s).
    Columns the mapping builds from other columns are left out: their CLEAN value is not the parent's id."""
    key = structure.key(table, service) if structure else None
    if not key:
        return {}
    fk_parent = {f["column"].lower(): f["table"].lower() for f in structure.fks.get(key, [])}
    tables = {t.lower(): t for t in all_cols}
    out = {}
    for col in columns:
        name = col["name"].lower()
        if name == "id" or (table.lower(), name) in derived:
            continue
        parent = fk_parent.get(name) or (name[:-3] + "s" if name.endswith("_id") else None)
        pcols = {c["name"].lower(): c["name"] for c in all_cols.get(tables.get(parent), [])} if parent in tables else {}
        if "id" in pcols:
            out[name] = {"table": parent, "parent": f"{sf_schema(facility, CLEAN_SUFFIX)}.{q(tables[parent])}", "id": q(pcols["id"])}
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


def compare_and_cast(cur, facility: str, table: str, columns: list[dict], structure: V3Structure,
                     *, print_sql: bool, route: str, service: str | None = None,
                     parents: dict | None = None, links: dict | None = None) -> tuple[list[dict], list[dict], list[dict]]:
    """Checks one table: plans it, counts what converts and what fails, and lists the errors and, for the audit, every changed value and skipped record. Creates nothing in Snowflake."""
    if structure and structure.key(table, service) is None:
        print(f"\n>> {table}: not found in the V3 structure, or the name exists in several V3 services "
              f"and the mapping file does not say which - using GUESSED types for this table")
    plans = plan_columns(table, columns, structure, service, parents, None, links)
    if not print_sql:
        orders = detect_date_orders(cur, facility, table, plans)
        if any(orders.values()):
            plans = plan_columns(table, columns, structure, service, parents, orders, links)
            for name, order in orders.items():
                if order:
                    print(f"   {name}: " + {"DMY": "dates are read day-first", "MDY": "dates are read month-first", "EXCEL": "numbers are read as Excel day numbers",
                                 "EPOCH": "numbers are read as Unix time (UTC)"}.get(order, "DAY-FIRST, but the column MIXES both orders"))
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
    if any(r["records_with_errors"] for r in report):
        cur.execute(error_sample_sql(facility, table, plans, ERROR_SAMPLE_LIMIT))
        errors = [{"table": table, "record_id": rid, "error": why, "original_values": row}
                  for rid, why, row in cur.fetchall()]
    if any(r.get("records_skipped") for r in report):
        cur.execute(skipped_sample_sql(facility, table, plans, ERROR_SAMPLE_LIMIT))
        changes = [{"table": table, "record_id": rid, "column": "(whole record)", "original": row,
                    "new_value": "skipped, not migrated", "what_happened": why} for rid, why, row in cur.fetchall()]
    sql = change_sql(facility, table, plans, CHANGE_LOG_LIMIT)
    if sql:
        cur.execute(sql)
        values = [{"table": table, "record_id": rid, "column": col, "original": old, "new_value": new, "what_happened": what}
                  for rid, col, old, new, what in cur.fetchall()]
        print(f"  values changed in kept records: {len(values)}{' (list is cut off at the limit)' if len(values) >= CHANGE_LOG_LIMIT else ''}")
        changes += values
    return report, errors, changes


class TypingErrors(Exception):
    """Raised when a table has records with errors. .tables = {table: number of records}. One task per table means only that table fails."""

    def __init__(self, message: str, tables: dict[str, int]):
        """Keeps the failed tables and their error counts."""
        super().__init__(message)
        self.tables = tables


def run(facility: str, tables: list[str] | None = None, conf: dict | None = None,
        structure_path: str | None = None, skip: list[str] | None = None,
        mapping_path: str | None = None, print_sql: bool = False, report_dir: str | None = None) -> list[dict]:
    """Entry point for the command line and the flatten step: checks every table given, then raises TypingErrors if any has errors."""
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
        chosen = choose_tables(cur, facility, tables, conf, skip)
        if not chosen:
            print("No tables to process.")
            return []
        all_cols = read_columns(cur, facility)
        cols = {t: all_cols[t] for t in chosen if t in all_cols}
        parents = find_parents(cur, facility)
        for table in chosen:
            if table not in cols:
                continue
            service = mapped.get(table.lower())               # the mapping file names the V3 table and service
            rows, errors, changes = compare_and_cast(
                cur, facility, table, cols[table], structure, print_sql=print_sql,
                route="mapping" if service else "not in mapping", service=service, parents=parents,
                links=find_links(structure, table, service, cols[table], all_cols, facility, derived))
            all_rows += rows
            all_errors += errors
            all_changes += changes
        cur.close()
    finally:
        conn.close()
    stamp = time.strftime('%Y%m%dT%H%M%S')
    out_dir = Path(report_dir) if report_dir else Path(__file__).resolve().parent / "output" / "typing"
    for name, data in ((f"{facility.upper()}_{stamp}.csv", all_rows), (f"{facility.upper()}_{stamp}_errors.csv", all_errors),
                       (f"{facility.upper()}_{stamp}_changes.csv", all_changes)):
        if data:
            out_dir.mkdir(parents=True, exist_ok=True)
            with open(out_dir / name, "w", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=list(data[0].keys()))
                w.writeheader()
                w.writerows(data)
            kind = "Errors" if "_errors" in name else "Changes and skipped records" if "_changes" in name else "Report"
            print(f"{kind} saved: {out_dir / name}")
    # a table with even one error FAILS: it must not be migrated. Other tables are not affected.
    failed: dict[str, int] = {}
    for r in all_rows:
        if r.get("records_with_errors"):
            failed[r["table"]] = max(failed.get(r["table"], 0), int(r["records_with_errors"]))
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
    ap.add_argument("--report-dir", help="where to save the CSV report (default: output/typing)")
    a = ap.parse_args()
    try:
        run(a.facility, tables=a.table, structure_path=a.structure, skip=a.skip,
            mapping_path=a.mapping, print_sql=a.print_sql, report_dir=a.report_dir)
    except TypingErrors as e:
        print(f"\nFAILED: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
