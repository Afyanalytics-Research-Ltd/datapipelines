"""
One-off load: Portbell Medical Centre's MOH 204 A register (an Excel workbook, not an HMS) -> Snowflake.

Source: "PORTBELL MEDICAL CENTREMOH 204 A REGISTER_decrypted.xlsx" (received 2026-10-20), kept out of git at
downloads/portbell/moh204a_register.xlsx. Four sheets, 30 Apr – 18 Oct 2026:

  OVER 5 / UNDER 5     the registration registers: one row per new patient (OPD no, age, sex, residence, ID, phone,
                       occupation / guardian, temp, weight, BP, diagnosis, treatment, mode of payment). Each has its
                       OWN OPD series, both starting at 001/26, so an OPD number alone doesn't name a patient.
  DAILY ATTENDANCE     every attendance from 23 Jun 2026 (new and revisits, nursing / lab / dental walk-ins): service
                       delivery point, treatment / Dx, amount paid; each day closes with a TOTALS row.
  CLINIC BOOKINGS      3 bookings, no dates.

RAW   HOSPITALS.PORTBELL_RAW.<sheet>: every non-empty row, every cell as the text Excel holds (an Excel date cell
      as 'YYYY-MM-DD HH:MM:SS', everything else as typed), plus _SOURCE_ROW / _SHEET / _SOURCE_FILE /
      _FILE_SHA256 / _LOADED_AT. Nothing is fixed here.
CLEAN HOSPITALS.PORTBELL_CLEAN, built from the RAW text read back from Snowflake (so it can be rebuilt any time):
      PATIENTS, VISITS (one per patient per day), ATTENDANCE (daily register lines), DAILY_TOTALS (each TOTALS row
      against the sum of its day), CLINIC_BOOKINGS, DATA_ISSUES (every value that was fixed, guessed or dropped).

Cleaning rules (each applied value is logged in DATA_ISSUES):
  * Dates: the clerk typed dd/mm/yyyy. Excel turned those with a day <= 12 into real dates read as mm/dd, so every
    Excel date cell has its month and day swapped back (2026-01-05 -> 1 May 2026); typed text is read as dd/mm/yyyy
    ('23/82026' -> 23 Aug). A missing date takes the row above's (the registers are in date order).
  * Ages: whole numbers are years; a decimal under 18 is years.months as clinics write it (1.11 = 1 y 11 m,
    3.6 = 3 y 6 m); '8MNTHS' / '6WKS' / '2.4YERS' are parsed. DOB is estimated as visit date minus age.
  * Phones: Excel dropped the leading 0 (721481619 -> 0721481619); other lengths are kept only in RAW.
  * Patients: register rows are patients (key O:<opd> / U:<opd>). A daily line goes, in this order, to: the patient
    of its OPD number in whichever register has the closer name; a register patient of the same name; a register
    patient with the same phone AND a similar name (families share phones); the one register patient whose name
    contains it (or is contained in it) with no conflicting phone; else a daily-only patient — the same one again
    for the same phone + similar name or the same name, a new one for the same name with a different phone. Lines
    with no name stay unlinked (their payment still counts in the day's total). ATTENDANCE.MATCH_METHOD says which.
  * TOTALS rows (and the bare subtotal rows without serial / date / name) are not attendances; they are checked
    against the day's lines in DAILY_TOTALS.

Every CLEAN row carries the uuid5 portbell_to_v3.py posts it with, so a re-run updates V3 instead of duplicating.

Airflow: dags/portbell_moh204a_dag.py (load → V3 → reconcile). By hand, inside the Airflow worker:
    docker exec -w /opt/airflow/pipelines datapipelines-airflow-worker-1 python load_portbell_to_snowflake.py
    ... python load_portbell_to_snowflake.py --dry-run     # parse + clean locally, print the summary, write nothing
"""
from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import logging
import re
import sys
import uuid
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger("portbell_load")

DEFAULT_XLSX = Path(__file__).resolve().parent / "downloads" / "portbell" / "moh204a_register.xlsx"
SF_DB = "HOSPITALS"
RAW_SCHEMA, CLEAN_SCHEMA = "PORTBELL_RAW", "PORTBELL_CLEAN"
NS = uuid.UUID("9a7c1f52-3e4b-4d1a-8f60-2b5d7e0c4a93")      # never change: V3 rows are matched by these uuids
SHEETS = {"OVER 5": "OVER_5", "UNDER 5": "UNDER_5", "DAILY ATTENDANCE": "DAILY_ATTENDANCE",
          "CLINIC BOOKINGS": "CLINIC_BOOKINGS"}
META = ("_SOURCE_ROW", "_SHEET", "_SOURCE_FILE", "_FILE_SHA256", "_LOADED_AT")


# ── RAW: the workbook as text ──────────────────────────────────────────────
def col_name(header, index: int) -> str:
    if header is None or not str(header).strip():
        letters, n = "", index + 1
        while n:
            n, r = divmod(n - 1, 26)
            letters = chr(65 + r) + letters
        return f"COL_{letters}"
    return re.sub(r"[^A-Z0-9]+", "_", str(header).upper()).strip("_")


def raw_text(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    s = str(v)
    return s if s.strip() else None


def read_workbook(path: Path) -> dict[str, tuple[list[str], list[dict]]]:
    """{RAW table: (columns, rows)}; rows are {column: text} + _SOURCE_ROW / _SHEET, empty rows and columns dropped."""
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    out = {}
    for ws in wb.worksheets:
        table = SHEETS.get(ws.title.strip())
        if table is None:
            raise ValueError(f"unexpected sheet {ws.title!r} — add it to SHEETS")
        rows = list(ws.iter_rows(values_only=True))
        header, body = rows[0], rows[1:]
        width = max(len(header), *(len(r) for r in body)) if body else len(header)
        used = [j for j in range(width)
                if (j < len(header) and header[j] is not None) or any(j < len(r) and raw_text(r[j]) for r in body)]
        cols, seen = [], Counter()
        for j in used:
            c = col_name(header[j] if j < len(header) else None, j)
            seen[c] += 1
            cols.append(c if seen[c] == 1 else f"{c}_{seen[c]}")
        recs = []
        for i, r in enumerate(body, start=2):
            vals = [raw_text(r[j]) if j < len(r) else None for j in used]
            if any(vals):
                recs.append({**dict(zip(cols, vals)), "_SOURCE_ROW": i, "_SHEET": ws.title})
        out[table] = (cols, recs)
    return out


# ── CLEAN: pure helpers (tests/test_portbell.py) ───────────────────────────
def gid(kind: str, key) -> str:
    return str(uuid.uuid5(NS, f"portbell/{kind}/{key}"))


def clean(v) -> str | None:
    s = " ".join(str(v).split()) if v is not None else ""
    return s or None


def name_of(v) -> str | None:
    """A patient name, upper-cased with single spaces; None when empty or not a name (a stray number)."""
    s = clean(v)
    if not s or not re.search(r"[A-Za-z]", s):
        return None
    return s.upper()


def register_date(v) -> tuple[date | None, str | None]:
    """(real date, issue). Excel date cells are dd/mm read as mm/dd: swap back. Text is dd/mm/yyyy."""
    s = re.sub(r"\s", "", v or "")
    if not s:
        return None, None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})\d{2}:\d{2}:\d{2}$", s)
    if m:
        y, mo, d = int(m[1]), int(m[2]), int(m[3])
        if d <= 12:
            return date(y, d, mo), "Excel read dd/mm as mm/dd: month and day swapped back"
        return date(y, mo, d), None
    m = re.match(r"^(\d{1,2})/(\d{1,2})/?(\d{4})$", s)
    if m:
        try:
            d = date(int(m[3]), int(m[2]), int(m[1]))
        except ValueError:
            return None, f"impossible date {v!r}"
        return d, (f"malformed date {v!r} read as dd/mm/yyyy" if s.count("/") != 2 else None)
    return None, f"unparsable date {v!r}"


def number(v) -> float | None:
    s = clean(v)
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def phone(v) -> str | None:
    s = clean(v) or ""
    d = re.sub(r"\D", "", s[:-2] if re.fullmatch(r"\d+\.0", s) else s)
    if len(d) == 9 and d[0] in "17":
        return "0" + d
    if len(d) == 10 and d[0] == "0":
        return d
    if len(d) == 12 and d.startswith("254"):
        return "0" + d[3:]
    return None


def age(v) -> tuple[float | None, str | None]:
    """(age in years, readable age). Decimals under 18 are years.months (1.11 = 1 y 11 m)."""
    s = (clean(v) or "").upper()
    if not s:
        return None, None
    m = re.match(r"^(\d+)\s*(?:WK|WKS|WEEK|WEEKS)\b", s)
    if m:
        return round(int(m[1]) * 7 / 365.25, 3), f"{int(m[1])} weeks"
    m = re.match(r"^(\d+)\s*(?:M|MN|MNTH|MNTHS|MONTH|MONTHS|MTHS)\b", s)
    if m:
        return round(int(m[1]) / 12, 3), f"{int(m[1])} months"
    m = re.match(r"^(\d+)(?:\.(\d+))?\s*(?:Y|YR|YRS|YEARS?|YERS)?\b", s)
    if not m:
        return None, None
    y, frac = int(m[1]), m[2]
    if not frac or int(frac) == 0:
        return float(y), f"{y} years"
    if y < 18 and int(frac) <= 11:
        mo = int(frac)
        return round(y + mo / 12, 3), f"{y} years {mo} months"
    val = float(f"{y}.{frac}")
    return val, f"{val:g} years"


def sex(v) -> str | None:
    s = (clean(v) or "").upper()
    return {"F": "female", "FEMALE": "female", "M": "male", "MALE": "male"}.get(s)


def visit_kind(v) -> str | None:
    s = (clean(v) or "").upper().replace("-", "").replace(" ", "")
    if s in ("N", "NEW"):
        return "NEW"
    if s.startswith("RE") or s == "R":
        return "REVISIT"
    return None


SERVICE_POINTS = {"NURSING STATION": "NURSING STATION", "CONSULTATION": "CONSULTATION", "LABORATORY": "LABORATORY",
                  "LABORATORY SERVICES": "LABORATORY", "DENTAL CLINIC": "DENTAL", "DENTAL UNIT": "DENTAL"}
OCCUPATIONS = {"B.LADY": "BUSINESS LADY", "B. LADY": "BUSINESS LADY", "B.MAN": "BUSINESSMAN", "B. MAN": "BUSINESSMAN",
               "C.LABOURER": "CASUAL LABOURER", "C.LABOR": "CASUAL LABOURER", "C. LABOURER": "CASUAL LABOURER",
               "FARMER": "FARMING"}


def service_point(v) -> str | None:
    s = (clean(v) or "").upper()
    return SERVICE_POINTS.get(s, s or None)


def occupation(v) -> str | None:
    s = (clean(v) or "").upper()
    return OCCUPATIONS.get(s, s or None)


def opd(v) -> str | None:
    s = re.sub(r"\s", "", clean(v) or "")
    return s or None


def similarity(a: str | None, b: str | None) -> float:
    """0..1: the better of character similarity and the share of the shorter name's words found in the other."""
    if not a or not b:
        return 0.0
    ta, tb = set(a.split()), set(b.split())
    return max(difflib.SequenceMatcher(None, a, b).ratio(), len(ta & tb) / min(len(ta), len(tb)))


def split_name(full: str | None) -> tuple[str, str | None, str]:
    parts = (full or "UNKNOWN PATIENT").split()
    if len(parts) == 1:
        return parts[0], None, parts[0]
    return parts[0], " ".join(parts[1:-1]) or None, parts[-1]


def diagnoses(text: str | None) -> list[str]:
    """'URTI,GASTRITIS, ATHRITIS' / 'TONSILITIS,. URTI' -> ['URTI', 'GASTRITIS', 'ATHRITIS']."""
    return [p for p in (x.strip(" .") for x in re.split(r"[,;/]|\.\s|,\.", text or "")) if p]


def patient_no(key: str, opd_no: str | None, daily_seq: int) -> str:
    """The V3 patient number: the clinic's OPD number (under-5s prefixed U5-, their series overlaps the over-5s')."""
    base, _, dup = key.partition("#row")
    prefix, rest = base.split(":", 1)
    if prefix == "D":
        return f"PBL-D{daily_seq:04d}"
    if not opd_no:
        return f"PBL-{prefix}-{rest.upper()}"
    no = opd_no if prefix == "O" else f"U5-{opd_no}"
    return f"{no}-R{dup}" if dup else no


def join_unique(values) -> str | None:
    out = []
    for v in values:
        if v and v not in out:
            out.append(v)
    return "; ".join(out) or None


# ── CLEAN: the model ───────────────────────────────────────────────────────
class Cleaner:
    MATCH_THRESHOLD = 0.5            # OPD number given: the name only has to be recognisably the same
    PHONE_NAME_THRESHOLD = 0.7       # no OPD number: same phone and a clearly similar name

    def __init__(self, raw: dict[str, list[dict]]):
        self.raw = raw
        self.daily_people: list[dict] = []
        self.issues: list[dict] = []
        self.patients: dict[str, dict] = {}
        self.visits: dict[tuple[str, date], dict] = {}
        self.attendance: list[dict] = []
        self.totals: list[dict] = []
        self.bookings: list[dict] = []

    def issue(self, sheet, row, column, raw_value, problem, action):
        self.issues.append({"sheet": sheet, "source_row": row, "column_name": column, "raw_value": raw_value,
                            "issue": problem, "action": action})

    def dated(self, sheet: str, rows: list[dict], col: str) -> list[tuple[dict, date | None, bool]]:
        out, last = [], None
        for r in rows:
            d, why = register_date(r.get(col))
            if why:
                self.issue(sheet, r["_SOURCE_ROW"], col, r.get(col), why, f"set to {d}" if d else "left empty")
            imputed = False
            if d is None and last is not None:
                d, imputed = last, True
            if d is not None:
                last = d
            out.append((r, d, imputed))
        return out

    # registers → patients + their registration visit
    def registers(self):
        for table, prefix, label in (("OVER_5", "O", "over-5 register"), ("UNDER_5", "U", "under-5 register")):
            for r, d, imputed in self.dated(table, self.raw.get(table, []), "DATE"):
                no = opd(r.get("OPD_NO"))
                key = f"{prefix}:{no}" if no else f"{prefix}:row{r['_SOURCE_ROW']}"
                if imputed:
                    self.issue(table, r["_SOURCE_ROW"], "DATE", None, "date missing", f"took the row above's: {d}")
                if key in self.patients:
                    self.issue(table, r["_SOURCE_ROW"], "OPD_NO", no, "OPD number repeats in the register",
                               "kept as a second patient")
                    key = f"{key}#row{r['_SOURCE_ROW']}"
                yrs, age_txt = age(r.get("AGE"))
                nm = name_of(r.get("PATIENT_NAME"))
                if not nm:
                    self.issue(table, r["_SOURCE_ROW"], "PATIENT_NAME", r.get("PATIENT_NAME"), "no patient name",
                               "named from the daily register if an attendance has this OPD number, else 'UNKNOWN PATIENT'")
                ph = phone(r.get("CONTACT"))
                if clean(r.get("CONTACT")) and not ph:
                    self.issue(table, r["_SOURCE_ROW"], "CONTACT", r.get("CONTACT"), "not a Kenyan phone number", "left empty")
                ident = clean(r.get("ID_NO"))
                self.patients[key] = {
                    "patient_key": key, "register": label, "opd_no": no, "full_name": nm, "name_source": label if nm else None,
                    "sex": sex(r.get("SEX")), "age_years": yrs, "age_text": age_txt, "age_on": d,
                    "residence": clean(r.get("RESIDENCE")), "national_id": ident, "phone": ph,
                    "occupation": occupation(r.get("OCCUPATION")), "guardian_name": name_of(r.get("PARENT_GUARDIAN_NAME")),
                    "source_rows": [f"{table}:{r['_SOURCE_ROW']}"]}
                if d is None:
                    self.issue(table, r["_SOURCE_ROW"], "DATE", None, "no date and nothing above", "registration visit dropped")
                    continue
                bp = clean(r.get("BP"))
                m = re.match(r"^(\d{2,3})/(\d{2,3})$", bp or "")
                v = self.visit(key, d, imputed)
                v.update({"register": label, "temperature": number(r.get("TEMP")), "weight": number(r.get("WEIGHT")),
                          "blood_pressure": bp, "systolic_bp": int(m[1]) if m else None,
                          "diastolic_bp": int(m[2]) if m else None, "diagnosis": clean(r.get("DIAGNOSIS")),
                          "treatment": clean(r.get("TREATMENT")), "remarks": clean(r.get("REMARKS")),
                          "payment_mode": clean(r.get("MODE_OF_PAYMENT")), "age_text": age_txt})
                v["kinds"].add(visit_kind(r.get("NEW_REVISIT")) or "NEW")
                v["source_rows"].append(f"{table}:{r['_SOURCE_ROW']}")

    def visit(self, patient_key: str | None, d: date, imputed: bool = False) -> dict:
        k = (patient_key, d)
        if k not in self.visits:
            self.visits[k] = {"visit_key": f"{patient_key}@{d.isoformat()}", "patient_key": patient_key,
                              "visit_date": d, "date_imputed": imputed, "kinds": set(), "source_rows": [],
                              "register": None, "lines": []}
        return self.visits[k]

    # daily attendance → lines linked to patients / visits
    def resolve(self, r: dict, nm: str | None, ph: str | None) -> tuple[str | None, str]:
        no = opd(r.get("OPD_NO"))
        if no:
            best, score = None, 0.0
            for prefix in ("O", "U"):
                p = self.patients.get(f"{prefix}:{no}")
                if p is None:
                    continue
                s = 1.0 if (nm and not p["full_name"]) else similarity(nm, p["full_name"])
                if s > score:
                    best, score = p, s
            if best and score >= self.MATCH_THRESHOLD:
                if nm and not best["full_name"]:
                    best["full_name"], best["name_source"] = nm, "daily attendance (register row had no name)"
                return best["patient_key"], f"OPD number + name ({score:.2f})"
        if not nm:
            return None, "no name: unlinked"
        registered = [p for p in self.patients.values() if not p["patient_key"].startswith("D:")]
        same = [p for p in registered if p["full_name"] == nm]
        if len(same) > 1 and ph:
            same = [p for p in same if p["phone"] == ph] or same
        if same:
            return same[0]["patient_key"], "name" + (f" (ambiguous: {len(same)})" if len(same) > 1 else "")
        # families share a phone, so a phone only counts together with a similar name
        by_phone = sorted(((similarity(nm, p["full_name"]), p) for p in registered if ph and p["phone"] == ph),
                          key=lambda x: -x[0])
        if by_phone and by_phone[0][0] >= self.PHONE_NAME_THRESHOLD:
            return by_phone[0][1]["patient_key"], f"phone + similar name ({by_phone[0][0]:.2f})"
        words = set(nm.split())
        subset = [p for p in registered if p["full_name"] and len(words) >= 2 and len(set(p["full_name"].split())) >= 2
                  and (words <= set(p["full_name"].split()) or set(p["full_name"].split()) <= words)
                  and not (ph and p["phone"] and p["phone"] != ph)]
        if len(subset) == 1:
            return subset[0]["patient_key"], "name contained in register name (no phone conflict)"
        return self.daily_person(nm, ph), "daily-only patient"

    def daily_person(self, nm: str, ph: str | None) -> str:
        """A person seen only in the daily register: same phone + similar name, or same name without a conflicting
        phone, is the same person; one name with different phones is different people."""
        people = self.daily_people
        if ph:
            close = sorted(((similarity(nm, p["name"]), p) for p in people if ph in p["phones"]), key=lambda x: -x[0])
            if close and close[0][0] >= self.PHONE_NAME_THRESHOLD:
                return close[0][1]["key"]
        for p in people:
            if p["name"] == nm and (not ph or not p["phones"] or ph in p["phones"]):
                if ph:
                    p["phones"].add(ph)
                return p["key"]
        key = f"D:{nm}" if not any(p["key"] == f"D:{nm}" for p in people) else f"D:{nm}|{ph or len(people)}"
        people.append({"key": key, "name": nm, "phones": {ph} if ph else set()})
        return key

    def daily(self):
        rows = self.raw.get("DAILY_ATTENDANCE", [])
        last_day = None
        dated = self.dated("DAILY_ATTENDANCE", rows, "DATE_OF_VISIT")
        lines = []
        for r, d, imputed in dated:
            serial = clean(r.get("SERIAL_NO"))
            nm = name_of(r.get("PATIENT_NAME"))
            is_total = bool(serial and "TOTAL" in serial.upper()) or not any(
                clean(r.get(c)) for c in ("SERIAL_NO", "DATE_OF_VISIT", "PATIENT_NAME", "SERVICE_DELIVERY_POINT",
                                          "TYPE_OF_VISIT_NEW_RE_VISIT", "TREATMENT_INFORMATION_DX", "AGE"))
            if is_total:
                recorded = number(r.get("TOTALS")) if number(r.get("TOTALS")) is not None else number(r.get("AMOUNT_PAID"))
                self.totals.append({"source_row": r["_SOURCE_ROW"], "day": last_day, "recorded_total": recorded,
                                    "label": serial or "(bare subtotal)"})
                continue
            if clean(r.get("DATE_OF_VISIT")):
                last_day = d
            elif d is not None:
                self.issue("DAILY_ATTENDANCE", r["_SOURCE_ROW"], "DATE_OF_VISIT", None, "date missing", f"took the row above's: {d}")
            amount_raw = clean(r.get("AMOUNT_PAID"))
            amount = number(amount_raw)
            if amount_raw and amount is None:
                self.issue("DAILY_ATTENDANCE", r["_SOURCE_ROW"], "AMOUNT_PAID", amount_raw, "not a number", "amount left empty")
            if clean(r.get("PATIENT_NAME")) and not nm:
                self.issue("DAILY_ATTENDANCE", r["_SOURCE_ROW"], "PATIENT_NAME", r.get("PATIENT_NAME"),
                           "not a name (a number in the name column)", "line kept without a patient")
            info = clean(r.get("TREATMENT_INFORMATION_DX"))
            ph = phone(r.get("CONTACT"))
            if info and not re.search(r"[A-Za-z]", info):
                self.issue("DAILY_ATTENDANCE", r["_SOURCE_ROW"], "TREATMENT_INFORMATION_DX", info,
                           "a number in the treatment column (looks like a phone)", "moved to the phone" if phone(info) else "dropped")
                ph, info = ph or phone(info), None
            for c, v in r.items():
                if c.startswith("COL_") and clean(v):
                    self.issue("DAILY_ATTENDANCE", r["_SOURCE_ROW"], c, v, "value outside the register's columns", "kept in RAW only")
            if d is None:
                self.issue("DAILY_ATTENDANCE", r["_SOURCE_ROW"], "DATE_OF_VISIT", None, "no date and nothing above", "line dropped")
                continue
            yrs, age_txt = age(r.get("AGE"))
            line = {"source_row": r["_SOURCE_ROW"], "visit_date": d, "date_imputed": imputed, "serial_no": serial,
                    "opd_no": opd(r.get("OPD_NO")), "visit_type": visit_kind(r.get("TYPE_OF_VISIT_NEW_RE_VISIT")),
                    "patient_name": nm, "age_years": yrs, "age_text": age_txt, "sex": sex(r.get("SEX")), "phone": ph,
                    "service_point": service_point(r.get("SERVICE_DELIVERY_POINT")), "treatment_info": info,
                    "amount_paid": amount, "amount_raw": amount_raw}
            line["patient_key"], line["match_method"] = self.resolve(r, nm, ph)
            lines.append(line)
        for line in lines:
            k = line["patient_key"]
            if k and k.startswith("D:") and k not in self.patients:
                self.patients[k] = {"patient_key": k, "register": "daily attendance only", "opd_no": line["opd_no"],
                                    "full_name": line["patient_name"], "name_source": "daily attendance", "sex": line["sex"],
                                    "age_years": line["age_years"], "age_text": line["age_text"], "age_on": line["visit_date"],
                                    "residence": None, "national_id": None, "phone": line["phone"], "occupation": None,
                                    "guardian_name": None, "source_rows": []}
            p = self.patients.get(k)
            if p is not None:
                p["source_rows"].append(f"DAILY_ATTENDANCE:{line['source_row']}")
                for f in ("phone", "sex"):
                    p[f] = p[f] or line[f]
                if p["age_years"] is None and line["age_years"] is not None:
                    p["age_years"], p["age_text"], p["age_on"] = line["age_years"], line["age_text"], line["visit_date"]
                v = self.visit(k, line["visit_date"], line["date_imputed"])
                if line["visit_type"]:
                    v["kinds"].add(line["visit_type"])
                v["source_rows"].append(f"DAILY_ATTENDANCE:{line['source_row']}")
                v["lines"].append(line)
                line["visit_key"] = v["visit_key"]
            else:
                line["visit_key"] = None
            line["payment_uuid"] = gid("payment", f"DAILY_ATTENDANCE:{line['source_row']}") if line["amount_paid"] else None
            self.attendance.append(line)
        # TOTALS rows vs the day's lines
        by_day = Counter()
        for line in self.attendance:
            by_day[line["visit_date"]] += line["amount_paid"] or 0
        for t in self.totals:
            t["computed_total"] = by_day.get(t["day"], 0) if t["day"] else None
            t["difference"] = (None if t["recorded_total"] is None or t["computed_total"] is None
                               else round(t["recorded_total"] - t["computed_total"], 2))

    def clinic_bookings(self):
        for r in self.raw.get("CLINIC_BOOKINGS", []):
            nm = name_of(r.get("NAME"))
            no = opd(r.get("OPD_NO"))
            ph = phone(r.get("CONTACT"))
            key, how = self.resolve({"OPD_NO": no}, nm, ph) if nm else (None, "no name")
            if key and key.startswith("D:") and key not in self.patients:
                # a daily-only person may have been split by phone number
                split = [k for k in self.patients if k.startswith(key + "|")]
                key = next((k for k in split if self.patients[k]["phone"] == ph), split[0] if split else None)
                how = "daily-only patient" if key else "not in any register"
            yrs, age_txt = age(r.get("AGE"))
            self.bookings.append({"source_row": r["_SOURCE_ROW"], "booking_uuid": gid("clinic_booking", r["_SOURCE_ROW"]),
                                  "opd_no": no, "patient_name": nm, "age_years": yrs, "age_text": age_txt,
                                  "sex": sex(r.get("SEX")), "phone": phone(r.get("CONTACT")),
                                  "clinic_type": clean(r.get("TYPE_OF_CLINICBOOKED")),
                                  "consultant_name": clean(r.get("CONSULTANT_NAME")), "patient_key": key, "match_method": how})

    def build(self) -> dict[str, list[dict]]:
        self.registers()
        self.daily()
        self.clinic_bookings()
        # patient numbers: the clinic's OPD number where it has one
        daily_no = 0
        visits_of = defaultdict(list)
        for v in self.visits.values():
            visits_of[v["patient_key"]].append(v["visit_date"])
        patients = []
        for key in sorted(self.patients, key=lambda k: ({"O": 0, "U": 1, "D": 2}[k[0]], min(visits_of.get(k) or [date.max]), k)):
            p = self.patients[key]
            daily_no += key.startswith("D:")
            no = patient_no(key, p["opd_no"] if not key.startswith("D:") else None, daily_no)
            first, middle, last = split_name(p["full_name"])
            dob = (p["age_on"] - timedelta(days=round(p["age_years"] * 365.25))
                   if p["age_years"] is not None and p["age_on"] else None)
            seen = sorted(visits_of.get(key) or [])
            patients.append({"patient_key": key, "patient_uuid": gid("patient", key), "patient_no": no,
                             "register": p["register"], "opd_no": p["opd_no"], "full_name": p["full_name"] or "UNKNOWN PATIENT",
                             "first_name": first, "middle_name": middle, "last_name": last, "name_source": p["name_source"],
                             "sex": p["sex"], "age_years": p["age_years"], "age_text": p["age_text"], "age_recorded_on": p["age_on"],
                             "dob_estimated": dob, "residence": p["residence"], "national_id": p["national_id"],
                             "phone": p["phone"], "occupation": p["occupation"], "guardian_name": p["guardian_name"],
                             "first_visit_date": seen[0] if seen else None, "last_visit_date": seen[-1] if seen else None,
                             "visit_count": len(seen), "source_rows": ", ".join(p["source_rows"])})
        visits = []
        for v in sorted(self.visits.values(), key=lambda v: (v["visit_date"], v["visit_key"])):
            lines = v["lines"]
            points = join_unique(l["service_point"] for l in lines)
            services = join_unique(f"{l['service_point'] or 'UNSPECIFIED'}: {l['treatment_info'] or '-'}"
                                   + (f" (KES {l['amount_paid']:g})" if l["amount_paid"] else "") for l in lines)
            consult_dx = join_unique(l["treatment_info"] for l in lines if l["service_point"] == "CONSULTATION")
            dx = join_unique([v.get("diagnosis"), consult_dx])
            paid = sum(l["amount_paid"] or 0 for l in lines)
            visits.append({"visit_key": v["visit_key"], "visit_uuid": gid("visit", v["visit_key"]),
                           "patient_key": v["patient_key"], "patient_uuid": gid("patient", v["patient_key"]),
                           "visit_date": v["visit_date"], "date_imputed": v["date_imputed"],
                           "visit_type": "NEW" if "NEW" in v["kinds"] else ("REVISIT" if v["kinds"] else None),
                           "register": v["register"], "temperature": v.get("temperature"), "weight": v.get("weight"),
                           "blood_pressure": v.get("blood_pressure"), "systolic_bp": v.get("systolic_bp"),
                           "diastolic_bp": v.get("diastolic_bp"), "diagnosis": dx,
                           "diagnosis_list": json.dumps(diagnoses(dx)) if dx else None,
                           "treatment": v.get("treatment"), "service_points": points, "services": services,
                           "amount_paid": paid if lines else None, "payment_mode": v.get("payment_mode"),
                           "source_rows": ", ".join(v["source_rows"])})
        attendance = [{**{k: l[k] for k in ("source_row", "visit_date", "date_imputed", "serial_no", "opd_no", "visit_type",
                                              "patient_name", "age_years", "age_text", "sex", "phone", "service_point",
                                              "treatment_info", "amount_paid", "amount_raw", "patient_key", "match_method",
                                              "visit_key", "payment_uuid")},
                       "patient_uuid": gid("patient", l["patient_key"]) if l["patient_key"] else None,
                       "visit_uuid": gid("visit", l["visit_key"]) if l["visit_key"] else None} for l in self.attendance]
        totals = [{"source_row": t["source_row"], "label": t["label"], "day": t["day"], "recorded_total": t["recorded_total"],
                   "computed_total": t["computed_total"], "difference": t["difference"]} for t in self.totals]
        bookings = [{**b, "patient_uuid": gid("patient", b["patient_key"]) if b["patient_key"] else None} for b in self.bookings]
        return {"PATIENTS": patients, "VISITS": visits, "ATTENDANCE": attendance, "DAILY_TOTALS": totals,
                "CLINIC_BOOKINGS": bookings, "DATA_ISSUES": self.issues}


CLEAN_TYPES = {
    "PATIENTS": {"age_years": "FLOAT", "age_recorded_on": "DATE", "dob_estimated": "DATE", "first_visit_date": "DATE",
                 "last_visit_date": "DATE", "visit_count": "INTEGER"},
    "VISITS": {"visit_date": "DATE", "date_imputed": "BOOLEAN", "temperature": "FLOAT", "weight": "FLOAT",
               "systolic_bp": "INTEGER", "diastolic_bp": "INTEGER", "amount_paid": "FLOAT"},
    "ATTENDANCE": {"source_row": "INTEGER", "visit_date": "DATE", "date_imputed": "BOOLEAN", "age_years": "FLOAT",
                   "amount_paid": "FLOAT"},
    "DAILY_TOTALS": {"source_row": "INTEGER", "day": "DATE", "recorded_total": "FLOAT", "computed_total": "FLOAT",
                     "difference": "FLOAT"},
    "CLINIC_BOOKINGS": {"source_row": "INTEGER", "age_years": "FLOAT"},
    "DATA_ISSUES": {"source_row": "INTEGER"},
}


# ── Snowflake ──────────────────────────────────────────────────────────────
def replace_table(cur, fqn: str, columns: list[tuple[str, str]], rows: list[tuple]) -> None:
    cur.execute(f"CREATE OR REPLACE TABLE {fqn} (" + ", ".join(f'"{c}" {t}' for c, t in columns) + ")")
    if rows:
        cur.executemany(f"INSERT INTO {fqn} (" + ", ".join(f'"{c}"' for c, _ in columns) + ") VALUES ("
                        + ", ".join(["%s"] * len(columns)) + ")", rows)
    cur.execute(f"SELECT COUNT(*) FROM {fqn}")
    n = cur.fetchone()[0]
    if n != len(rows):
        raise RuntimeError(f"{fqn}: wrote {len(rows)} rows, table has {n}")


def load_raw(cur, sheets: dict, source: Path, sha: str) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for table, (cols, rows) in sheets.items():
        columns = [(c, "VARCHAR") for c in cols] + [("_SOURCE_ROW", "INTEGER"), ("_SHEET", "VARCHAR"),
                                                    ("_SOURCE_FILE", "VARCHAR"), ("_FILE_SHA256", "VARCHAR"),
                                                    ("_LOADED_AT", "TIMESTAMP_NTZ")]
        replace_table(cur, f"{SF_DB}.{RAW_SCHEMA}.{table}", columns,
                      [tuple(r.get(c) for c in cols) + (r["_SOURCE_ROW"], r["_SHEET"], source.name, sha, now) for r in rows])
        log.info("  RAW   %-18s %5d rows", table, len(rows))


def read_raw(cur) -> dict[str, list[dict]]:
    out = {}
    for table in SHEETS.values():
        cur.execute(f'SELECT * FROM {SF_DB}.{RAW_SCHEMA}.{table} ORDER BY "_SOURCE_ROW"')
        cols = [c[0] for c in cur.description]
        out[table] = [dict(zip(cols, r)) for r in cur.fetchall()]
    return out


def load_clean(cur, tables: dict[str, list[dict]]) -> None:
    for table, rows in tables.items():
        if not rows:
            log.warning("  CLEAN %-18s empty", table)
        cols = list(rows[0]) if rows else ["source_row"]
        types = CLEAN_TYPES.get(table, {})
        columns = [(c.upper(), types.get(c, "VARCHAR")) for c in cols]
        replace_table(cur, f"{SF_DB}.{CLEAN_SCHEMA}.{table}", columns, [tuple(r.get(c) for c in cols) for r in rows])
        log.info("  CLEAN %-18s %5d rows", table, len(rows))


def summary(tables: dict[str, list[dict]]) -> dict:
    p, v, a, t = tables["PATIENTS"], tables["VISITS"], tables["ATTENDANCE"], tables["DAILY_TOTALS"]
    return {"patients": len(p), "patients_by_register": dict(Counter(x["register"] for x in p)),
            "visits": len(v), "visits_with_vitals": sum(1 for x in v if x["temperature"] or x["weight"] or x["blood_pressure"]),
            "attendance_lines": len(a), "attendance_match": dict(Counter(x["match_method"].split(" (")[0] for x in a)),
            "payments": sum(1 for x in a if x["payment_uuid"]), "amount_paid": sum(x["amount_paid"] or 0 for x in a),
            "totals_rows": len(t), "totals_mismatched": sum(1 for x in t if x["difference"] not in (None, 0)),
            "clinic_bookings": len(tables["CLINIC_BOOKINGS"]), "data_issues": dict(Counter(x["issue"].split(":")[0].split(" '")[0]
                                                                                           for x in tables["DATA_ISSUES"]))}


def run(xlsx: str | Path = DEFAULT_XLSX, *, dry_run: bool = False) -> dict:
    """Workbook → RAW → CLEAN (dry_run: parse and clean locally, write nothing). Returns the summary.
    Called by main() and dags/portbell_moh204a_dag.py (which has already set up the pipelines dir)."""
    path = Path(xlsx)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    sheets = read_workbook(path)
    log.info("%s (sha256 %s…): %s", path.name, sha[:12], {t: len(r) for t, (_, r) in sheets.items()})
    if dry_run:
        tables = Cleaner({t: rows for t, (_, rows) in sheets.items()}).build()
        return {"file": path.name, "sha256": sha, "dry_run": True, **summary(tables)}
    import snowflake_to_v3_migration as s2v3
    sf = s2v3._snowflake_connect()
    try:
        cur = sf.cursor()
        for schema in (RAW_SCHEMA, CLEAN_SCHEMA):
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{schema}")
        load_raw(cur, sheets, path, sha)
        raw = read_raw(cur)                      # CLEAN is built from what RAW holds, not from the workbook
        tables = Cleaner(raw).build()
        load_clean(cur, tables)
    finally:
        sf.close()
    out = {"file": path.name, "sha256": sha, "dry_run": False, **summary(tables)}
    log.info("summary: %s", json.dumps(out, default=str))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Portbell MOH 204 A register -> Snowflake PORTBELL_RAW / PORTBELL_CLEAN")
    ap.add_argument("xlsx", nargs="?", default=str(DEFAULT_XLSX))
    ap.add_argument("--dry-run", action="store_true", help="parse and clean locally; write nothing")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    if not a.dry_run:
        sys.path.insert(0, "/opt/airflow/dags")
        from pipelines_common import use_pipelines_dir
        use_pipelines_dir([])
    print(json.dumps(run(a.xlsx, dry_run=a.dry_run), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
