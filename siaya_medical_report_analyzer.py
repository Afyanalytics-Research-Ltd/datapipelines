#!/usr/bin/env python3
"""
siaya_medical_report_analyzer.py

Deep analysis of HOSPITALS.SIAYA_MEDICAL_CAMP.SIAYA_MEDICAL_CAMP_COMPLETE_DATA.

That table merges two record families fed by siaya_v2_visits_to_snowflake.py /
siaya_v3_visits_to_snowflake.py (RECORD_TYPE = V2_EMR_VISIT / V3_EMR_VISIT —
adult camp visits with structured vitals, chief complaints, systemic exam
fields, etc.) and a neonatal / PDF-extracted family sharing this table's
schema (BABY_NAME, GESTATIONAL_AGE, APGAR_SCORES, DELIVERY_DETAILS, …). Both
families are unified into one report:

    record_type, patient_id, patient_name / baby_name, phone_number,
    sex, age / age_or_day_of_life, date_of_birth, camp_file_number,
    patient_id_file_no, address, visit_date / document_date, time_in,
    time_out, attending_clinician / clinician_name, emergency_contact,
    blood_pressure, heart_rate, oxygen_saturation, temperature,
    respiratory_rate, weight, height, body_mass_index, blood_sugar,
    nurses_triage_notes, chief_complaints, history_of_present_illness,
    past_medical_surgical_history, known_allergies, current_medications,
    general_appearance, systemic_exam_cvs/resp/git/cns/msk_skin,
    provisional_diagnosis / diagnoses, lab_imaging_investigations_ordered /
    investigations, treatment_plan_prescriptions / treatments,
    follow_up_referral_notes, doctors_notes / clinical_summary, lab_results,
    raw_notes, document_type, gestational_age, birth_weight, current_weight,
    delivery_details, apgar_scores, feeding_and_support, discharge_plan

Produces a multi-page PDF report containing:
  * Cover & executive summary
  * Dataset overview and data-quality / missingness
  * Record-type mix (V2 vs V3 vs any future neonatal/PDF source)
  * Patient demographics (sex, age, gestational age, birth weight)
  * Visit/document-date distribution
  * Diagnosis frequency, category buckets, and co-occurrence heatmap
  * Treatment / medication frequency and antibiotic share
  * Investigation & lab-results patterns
  * Structured vitals (BP, HR, RR, Temp, SpO2, weight, height, BMI, sugar)
  * Chief complaints / triage keyword analysis
  * Clinician activity
  * Text-completeness / length analysis
  * Appendix with representative full records

Usage:
    # Pull directly from Snowflake (uses the same .env as the ingestion scripts)
    python siaya_medical_report_analyzer.py -o report.pdf
    python siaya_medical_report_analyzer.py --since 2026-01-01 --record-type V3_EMR_VISIT

    # Or analyze a CSV export of the table (e.g. `SELECT * FROM ...`)
    python siaya_medical_report_analyzer.py INPUT.csv -o report.pdf

ENV VARS (.env, same as siaya_v2/v3_visits_to_snowflake.py)
  SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_WAREHOUSE / SNOWFLAKE_ROLE
  SNOWFLAKE_PRIVATE_KEY_PATH  (or SNOWFLAKE_PASSWORD)
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
import textwrap
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# ---------------------------------------------------------------------------
# Snowflake target (mirrors siaya_v2/v3_visits_to_snowflake.py)
# ---------------------------------------------------------------------------
TARGET_DB     = "HOSPITALS"
TARGET_SCHEMA = "SIAYA_MEDICAL_CAMP"
TARGET_TABLE  = "SIAYA_MEDICAL_CAMP_COMPLETE_DATA"

# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor":   "white",
    "axes.edgecolor":   "#333333",
    "axes.linewidth":   0.8,
    "axes.titlesize":   14,
    "axes.titleweight": "bold",
    "axes.labelsize":   11,
    "axes.spines.top":  False,
    "axes.spines.right": False,
    "axes.grid":        True,
    "grid.alpha":       0.3,
    "grid.linestyle":   "--",
    "font.family":      "DejaVu Sans",
    "font.size":        10,
    "xtick.labelsize":  9,
    "ytick.labelsize":  9,
    "legend.fontsize":  9,
    "figure.titlesize": 16,
    "figure.titleweight": "bold",
})

PALETTE = ["#2E86AB", "#A23B72", "#F18F01", "#C73E1D", "#3B7A57",
           "#6A4C93", "#1F77B4", "#FF7F0E", "#2CA02C", "#D62728"]

LIST_COLUMNS = ["diagnoses", "treatments", "investigations"]
ALL_TEXT_COLUMNS = [
    "chief_complaints", "history_of_present_illness",
    "past_medical_surgical_history", "general_appearance",
    "delivery_details", "_clinical_text", "_examination_text",
    "feeding_and_support", "discharge_plan", "raw_notes",
]

# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------
def safe_parse_list(x):
    """Turn a value into a list of discrete items.

    Handles both a Python-literal list-string (from PDF-extraction sources)
    and a "; "-joined plain string (from the V2/V3 EMR loaders, e.g.
    PROVISIONAL_DIAGNOSIS / TREATMENT_PLAN_PRESCRIPTIONS).
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return []
    if isinstance(x, list):
        return x
    s = str(x).strip()
    if not s:
        return []
    try:
        v = ast.literal_eval(s)
        if isinstance(v, (list, tuple, set)):
            return [str(item) for item in v]
        return [str(v)]
    except Exception:
        parts = [p.strip() for p in re.split(r"[;\n]+", s) if p.strip()]
        return parts if parts else [s]


def parse_weight_kg(x):
    if x is None or pd.isna(x):
        return None
    s = str(x).lower().replace(",", ".")
    m = re.search(r"(\d+\.?\d*)\s*kg", s)
    if m:
        return float(m.group(1))
    m = re.search(r"(\d+\.?\d*)\s*g(?:ms?)?\b", s)
    if m:
        return float(m.group(1)) / 1000.0
    m = re.search(r"(\d+\.?\d*)", s)
    if m:
        v = float(m.group(1))
        if v < 10:
            return v
        if v > 100:
            return v / 1000.0
    return None


def parse_gestational_weeks(x):
    if x is None or pd.isna(x):
        return None
    s = str(x).lower()
    m = re.search(r"(\d{1,2})\s*(?:weeks|wks)", s)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d{1,2})\s*[+/]\s*40", s)
    if m:
        return int(m.group(1))
    m = re.search(r"^(\d{1,2})\b", s)
    if m:
        return int(m.group(1))
    return None


def parse_age(x):
    """Parse a free-text age (e.g. 'day 3', '45 yrs') into (unit, value)."""
    if x is None or pd.isna(x):
        return (None, None)
    s = str(x).lower()
    for pat, unit in [
        (r"(\d+)\s*yrs?\b",   "years"),
        (r"(\d+)\s*years?\b", "years"),
        (r"day\s*(\d+)",      "days"),
        (r"(\d+)\s*days?\b",  "days"),
        (r"(\d+)\s*months?\b","months"),
        (r"(\d+)\s*weeks?\b", "weeks"),
    ]:
        m = re.search(pat, s)
        if m:
            return (unit, int(m.group(1)))
    m = re.match(r"^\s*(\d+)\s*$", s)
    if m:
        return ("years", int(m.group(1)))
    return (None, None)


def parse_first_number(x):
    """Pull the first numeric token out of an already-formatted structured
    field, e.g. '72 bpm' -> 72.0, '98 %' -> 98.0, '5.6 mmol/L' -> 5.6."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    m = re.search(r"(\d+\.?\d*)", str(x))
    return float(m.group(1)) if m else None


def parse_bp(x):
    """Split a formatted 'SBP/DBP mmHg' string into (sbp, dbp)."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return (None, None)
    m = re.search(r"(\d{2,3})\s*/\s*(\d{2,3})", str(x))
    if m:
        return (float(m.group(1)), float(m.group(2)))
    return (None, None)


def parse_vitals_freetext(text):
    """Fallback: pull numeric vitals out of a free-text vitals/exam string,
    for record types that only populate the free-text VITALS column."""
    out = {}
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return out
    s = str(text).lower().replace("²", "2")
    m = re.search(r"(\d{2,3})\s*/\s*(\d{2,3})\s*mmhg", s)
    if not m:
        m = re.search(r"\bbp\s*[:\-]?\s*(\d{2,3})\s*/\s*(\d{2,3})", s)
    if m:
        out["SBP"] = int(m.group(1))
        out["DBP"] = int(m.group(2))
    m = re.search(r"\b(?:pr|hr|pulse)\s*[:\-]?\s*(\d{2,3})\b", s)
    if m:
        v = int(m.group(1))
        if 20 < v < 250:
            out["HR"] = v
    m = re.search(r"\brr\s*[:\-]?\s*(\d{1,3})\b", s)
    if m:
        v = int(m.group(1))
        if 0 < v < 100:
            out["RR"] = v
    m = re.search(r"(\d{2}\.?\d?)\s*°?\s*c\b", s)
    if m:
        try:
            t = float(m.group(1))
            if 30 < t < 45:
                out["Temp"] = t
        except ValueError:
            pass
    m = re.search(r"sp\s*o\s*2\s*[:\-]?\s*(\d{2,3})", s)
    if m:
        v = int(m.group(1))
        if 50 <= v <= 100:
            out["SpO2"] = v
    return out


def parse_labs(items):
    """Pull WBC / Hb / PLT / Hct / Glucose / CRP / TSB out of investigation
    or lab-result strings."""
    out = defaultdict(list)
    patterns = [
        ("WBC",     r"\bwbc\s*[:\-]?\s*(\d+\.?\d*)"),
        ("Hb",      r"\bhb\s*[:\-]?\s*(\d+\.?\d*)"),
        ("PLT",     r"\bplt\s*[:\-]?\s*(\d+\.?\d*)"),
        ("Hct",     r"\bhct\s*[:\-]?\s*(\d+\.?\d*)"),
        ("Glucose", r"\bglucose\s*[:\-]?\s*(\d+\.?\d*)"),
        ("CRP",     r"\bcrp\s*[:\-]?\s*(\d+\.?\d*)"),
        ("TSB",     r"\btsb[^\d]{0,10}(\d+\.?\d*)"),
    ]
    for item in items or []:
        s = str(item).lower()
        for key, pat in patterns:
            for m in re.finditer(pat, s):
                try:
                    out[key].append(float(m.group(1)))
                except ValueError:
                    pass
    return dict(out)


_DATE_MIN = pd.Timestamp("2000-01-01")
_DATE_MAX = pd.Timestamp.now() + pd.Timedelta(days=365)


def parse_date(x):
    """Parse a free-text date, rejecting anything that isn't a plausible
    record date. Dirty source data can contain malformed/ambiguous strings
    (e.g. '3.15.2026', stray IDs, OCR noise) that dateutil's fallback parser
    will happily turn into a nonsense date decades or centuries off — which
    then crashes matplotlib's date axis. Anything outside [2000, now+1y] is
    treated as unparseable."""
    if x is None or pd.isna(x):
        return pd.NaT
    if isinstance(x, (pd.Timestamp, datetime)):
        result = x
    else:
        s = str(x).strip()
        result = None
        for fmt in ("%Y-%m-%d", "%d/%m/%y", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y",
                    "%m.%d.%Y", "%d.%m.%Y"):
            try:
                result = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
        if result is None:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                result = pd.to_datetime(s, errors="coerce", dayfirst=True)

    if result is None or pd.isna(result):
        return pd.NaT
    ts = pd.Timestamp(result)
    if ts < _DATE_MIN or ts > _DATE_MAX:
        return pd.NaT
    return ts


def normalize_sex(x):
    if x is None or pd.isna(x):
        return None
    s = str(x).strip().lower()
    if not s:
        return None
    if s.startswith("f"):
        return "Female"
    if s.startswith("m"):
        return "Male"
    return s.title()


def normalize_clinician(name):
    if name is None or pd.isna(name):
        return []
    parts = re.split(r"[;,/]| and ", str(name))
    out = []
    for p in parts:
        c = re.sub(r"\([^)]*\)", "", p).strip()
        c = re.sub(r"^(?:dr\.?|prof\.?|mr\.?|ms\.?|mrs\.?)\s*", "", c, flags=re.I).strip()
        if c and len(c) > 1 and not c.lower().startswith("signature"):
            out.append(c.title())
    return out


def first_non_null(*vals):
    for v in vals:
        if v is None:
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


# ---------------------------------------------------------------------------
# Data cleaning
# ---------------------------------------------------------------------------
NULL_TOKENS = {
    "", "n/a", "na", "n\\a", "none", "null", "nil", "-", "--", "---",
    "unknown", "unk", "not available", "not applicable", "nan", "tbd",
    "pending", "n.a", "n.a.", "#n/a", ".", "?",
}

# (lo, hi) plausibility bounds for extracted structured vitals / labs — values
# outside these are almost always OCR/extraction noise, not real readings.
VITAL_RANGES = {
    "_v_SBP": (40, 300), "_v_DBP": (20, 200), "_v_HR": (20, 250),
    "_v_RR": (5, 100), "_v_Temp": (25, 45), "_v_SpO2": (50, 100),
    "_v_Weight": (0.3, 300), "_v_Height": (20, 250), "_v_BMI": (8, 80),
    "_v_BloodSugar": (0.5, 40),
}
LAB_RANGES = {
    "WBC": (0.1, 100), "Hb": (2, 25), "PLT": (1, 1500), "Hct": (5, 70),
    "Glucose": (0.5, 40), "CRP": (0, 500), "TSB": (0, 600),
}


def clean_text_cell(x):
    """Normalize whitespace and turn placeholder null-tokens into real NaN."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    if not s:
        return None
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    if s.strip(" .").lower() in NULL_TOKENS:
        return None
    return s


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Strip/normalize text, collapse placeholder nulls, and drop duplicate
    rows before any analysis runs on the raw Snowflake/CSV export."""
    df = df.copy()
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].apply(clean_text_cell)

    # Only drop EXACT full-row duplicates. SOURCE_FILE is not a safe
    # per-record key in general — for bulk/PDF-extraction loads it can be
    # the shared originating filename across many distinct patient rows,
    # so deduping on it alone would silently collapse legitimate records.
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    if dropped:
        print(f"Data cleaning: dropped {dropped} exact-duplicate row(s) "
              f"({before} -> {len(df)})", file=sys.stderr)

    return df.reset_index(drop=True)


def clean_list_items(lst):
    """Strip stray punctuation/whitespace and drop case-insensitive dupes
    within a single record's diagnoses/treatments/investigations list."""
    seen = set()
    out = []
    for item in lst:
        s = re.sub(r"\s+", " ", str(item).strip()).strip(" .;,:")
        if not s or s.lower() in NULL_TOKENS:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
    return out


def clip_range(series, lo, hi):
    s = pd.to_numeric(series, errors="coerce")
    return s.where((s >= lo) & (s <= hi))


# ---------------------------------------------------------------------------
# Buckets & keyword helpers
# ---------------------------------------------------------------------------
STOPWORDS = set("""
the of and to a in is on for with at by from as was were be been being are an or
that this these those it its but not no if then so than which who whom whose what
when where why how all any both each few more most other some such nor too very
can will just don should now patient pt has have had do does did doing here there
will would could should may might must shall about above after again against had
has have her his him she they them their our your you i we us also via mins min
day days week weeks year years post via during while including including following
""".split())


def tokenize(text):
    if text is None or pd.isna(text):
        return []
    return re.findall(r"[A-Za-z][A-Za-z\-']{2,}", str(text).lower())


def keyword_counts(series, min_len=4, top=25):
    c = Counter()
    for v in series.dropna():
        for tok in tokenize(v):
            if len(tok) >= min_len and tok not in STOPWORDS:
                c[tok] += 1
    return c.most_common(top)


def list_value_counts(lists, top=25):
    c = Counter()
    for lst in lists:
        for item in lst:
            key = str(item).strip()
            if key:
                c[key] += 1
    return c.most_common(top)


DIAG_BUCKETS = [
    ("Prematurity / LBW",         ["prematur", "preterm", "low birth", "lbw", "<2 kg", "vlbw"]),
    ("Jaundice",                   ["jaundice", "icter"]),
    ("Sepsis / Infection",         ["sepsis", "infect", "pneumon", "meningit"]),
    ("Respiratory",                ["rds", "respir", "asphyx", "meconium", "apnea", "asthma", "uri"]),
    ("Anemia",                     ["anemi", "anaemia"]),
    ("Malaria",                    ["malaria"]),
    ("Diabetes / Metabolic",       ["diabet", "hyperglyc", "hypoglyc"]),
    ("Hypertensive disorders",     ["preeclamp", "eclamp", "hypertens", "pih"]),
    ("Cardiac",                    ["cardiac", "heart", "pda", "asd", "vsd"]),
    ("Neurological",               ["seizur", "encephalo", "hie"]),
    ("GI / Feeding",               ["nec", "gastro", "feeding intoler", "vomit", "diarrh"]),
    ("Musculoskeletal / Skin",     ["arthrit", "rash", "dermat", "wound", "ulcer"]),
]


def bucket_diagnosis(d):
    s = str(d).lower()
    for label, terms in DIAG_BUCKETS:
        if any(t in s for t in terms):
            return label
    return "Other"


ANTIBIOTIC_HINTS = [
    "amoxic", "ampicill", "ceftri", "cefotax", "cefurox", "gentamic",
    "metronid", "doxycyc", "azithro", "clindamy", "vancomy", "meropen",
    "ciproflox", "penicill", "erythromy",
]

def is_antibiotic(s):
    s = str(s).lower()
    return any(h in s for h in ANTIBIOTIC_HINTS)


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------
def page_text(pdf, title, body_lines, *, fontsize=10, wrap=110):
    """Render a text-only page."""
    fig = plt.figure(figsize=(8.5, 11))
    ax = fig.add_subplot(111)
    ax.axis("off")
    ax.text(0.05, 0.96, title, fontsize=18, fontweight="bold",
            transform=ax.transAxes, color="#1F2D3D", va="top")
    ax.hlines(0.935, 0.05, 0.95, transform=ax.transAxes,
              color="#2E86AB", linewidth=1.5)
    y = 0.905
    for line in body_lines:
        if not line:
            y -= 0.012
            continue
        wrapped = textwrap.wrap(line, width=wrap) if not line.startswith("    ") else [line]
        for w in wrapped:
            ax.text(0.05, y, w, fontsize=fontsize, family="DejaVu Sans",
                    transform=ax.transAxes, va="top", color="#222222")
            y -= 0.018
            if y < 0.05:
                pdf.savefig(fig, bbox_inches="tight")
                plt.close(fig)
                fig = plt.figure(figsize=(8.5, 11))
                ax = fig.add_subplot(111)
                ax.axis("off")
                y = 0.96
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def hbar(ax, labels, values, color=None, title="", xlabel="Count"):
    color = color or PALETTE[0]
    y = np.arange(len(labels))
    ax.barh(y, values, color=color, edgecolor="white")
    ax.set_yticks(y)
    ax.set_yticklabels([textwrap.shorten(l, 60, placeholder="…") for l in labels])
    ax.invert_yaxis()
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    for i, v in enumerate(values):
        ax.text(v, i, f" {v}", va="center", fontsize=8, color="#333333")


def empty_axis(ax, message):
    ax.axis("off")
    ax.text(0.5, 0.5, message, ha="center", va="center",
            fontsize=11, color="#888888", style="italic",
            transform=ax.transAxes)


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------
def build_report(df: pd.DataFrame, pdf_path: Path) -> None:
    # ---- Normalize columns from the Snowflake table (upper) or a CSV export ----
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # ---- Clean dirty source data before any analysis (placeholder nulls,
    #      whitespace noise, duplicate rows from stale exports/reruns) ----
    df = clean_dataframe(df)
    n = len(df)

    # ---- Cross-family coalescing ----
    # V2/V3 EMR-visit rows duplicate the neonatal-shaped columns (e.g.
    # DIAGNOSES == PROVISIONAL_DIAGNOSIS); a hypothetical PDF-extracted
    # neonatal row might only populate one side — coalesce, don't union.
    def col(name):
        return df[name] if name in df.columns else pd.Series([None] * n, index=df.index)

    df["_subject_name"] = [first_non_null(a, b) for a, b in zip(col("patient_name"), col("baby_name"))]
    df["_identifier"] = [first_non_null(a, b, c) for a, b, c in
                          zip(col("camp_file_number"), col("patient_id_file_no"), col("patient_id"))]
    df["_clinical_text"] = [first_non_null(a, b) for a, b in zip(col("clinical_summary"), col("doctors_notes"))]
    df["_clinician_raw"] = [first_non_null(a, b) for a, b in zip(col("clinician_name"), col("attending_clinician"))]
    df["_diagnoses_raw"] = [first_non_null(a, b) for a, b in zip(col("diagnoses"), col("provisional_diagnosis"))]
    df["_treatments_raw"] = [first_non_null(a, b) for a, b in zip(col("treatments"), col("treatment_plan_prescriptions"))]
    df["_investigations_raw"] = [first_non_null(a, b, c) for a, b, c in
                                  zip(col("investigations"), col("lab_imaging_investigations_ordered"), col("lab_results"))]

    systemic_exam_joined = df.apply(
        lambda r: "; ".join(str(r[c]) for c in
                             ("systemic_exam_cvs", "systemic_exam_resp", "systemic_exam_git",
                              "systemic_exam_cns", "systemic_exam_msk_skin")
                             if c in df.columns and pd.notna(r[c])),
        axis=1,
    )
    df["_examination_text"] = [first_non_null(a, b) for a, b in zip(col("examination_findings"), systemic_exam_joined)]

    raw_date = [first_non_null(a, b) for a, b in zip(col("visit_date"), col("document_date"))]
    df["_doc_date"] = pd.Series(raw_date, index=df.index).apply(parse_date)

    # ---- Derived demographic / neonatal columns ----
    df["_birth_weight_kg"] = clip_range(col("birth_weight").apply(parse_weight_kg), 0.2, 6.5)
    df["_current_weight_kg"] = clip_range(col("current_weight").apply(parse_weight_kg), 0.2, 300)
    df["_ga_weeks"] = clip_range(col("gestational_age").apply(parse_gestational_weeks), 20, 44)
    raw_age = [first_non_null(a, b) for a, b in zip(col("age_or_day_of_life"), col("age"))]
    df["_age_tuple"] = pd.Series(raw_age, index=df.index).apply(parse_age)
    df["_age_unit"] = df["_age_tuple"].apply(lambda t: t[0])
    age_bounds = {"years": (0, 120), "months": (0, 24), "weeks": (0, 52), "days": (0, 31)}
    def _clip_age(t):
        unit, val = t
        if unit is None or val is None:
            return None
        lo, hi = age_bounds.get(unit, (0, 1000))
        return val if lo <= val <= hi else None
    df["_age_value"] = df["_age_tuple"].apply(_clip_age)
    df["_sex_norm"] = col("sex").apply(normalize_sex)

    # ---- Structured vitals (already-formatted columns), with a free-text
    #      VITALS-column fallback for rows that don't populate them ----
    bp_pairs = col("blood_pressure").apply(parse_bp)
    df["_v_SBP"] = bp_pairs.apply(lambda t: t[0])
    df["_v_DBP"] = bp_pairs.apply(lambda t: t[1])
    df["_v_HR"] = col("heart_rate").apply(parse_first_number)
    df["_v_RR"] = col("respiratory_rate").apply(parse_first_number)
    df["_v_Temp"] = col("temperature").apply(parse_first_number)
    df["_v_SpO2"] = col("oxygen_saturation").apply(parse_first_number)
    df["_v_Weight"] = col("weight").apply(parse_first_number)
    df["_v_Height"] = col("height").apply(parse_first_number)
    df["_v_BMI"] = col("body_mass_index").apply(parse_first_number)
    df["_v_BloodSugar"] = col("blood_sugar").apply(parse_first_number)

    fallback_vitals = df.apply(
        lambda r: parse_vitals_freetext(r.get("vitals")) if "vitals" in df.columns else {},
        axis=1,
    )
    for k, target in (("SBP", "_v_SBP"), ("DBP", "_v_DBP"), ("HR", "_v_HR"),
                      ("RR", "_v_RR"), ("Temp", "_v_Temp"), ("SpO2", "_v_SpO2")):
        df[target] = [existing if pd.notna(existing) else fb.get(k)
                      for existing, fb in zip(df[target], fallback_vitals)]

    # drop implausible extracted vitals (OCR / free-text noise)
    for key, (lo, hi) in VITAL_RANGES.items():
        df[key] = clip_range(df[key], lo, hi)

    # ---- Diagnoses / treatments / investigations parsed to lists ----
    df["_diagnoses_list"] = df["_diagnoses_raw"].apply(safe_parse_list).apply(clean_list_items)
    df["_treatments_list"] = df["_treatments_raw"].apply(safe_parse_list).apply(clean_list_items)
    df["_investigations_list"] = df["_investigations_raw"].apply(safe_parse_list).apply(clean_list_items)
    df["_n_diagnoses"] = df["_diagnoses_list"].apply(len)
    df["_n_treatments"] = df["_treatments_list"].apply(len)
    df["_n_investigations"] = df["_investigations_list"].apply(len)

    # ---- Lab values pulled from investigations + lab_results text ----
    df["_labs_parsed"] = df["_investigations_list"].apply(parse_labs)
    for k in ("WBC", "Hb", "PLT", "Hct", "Glucose", "CRP", "TSB"):
        vals = df["_labs_parsed"].apply(lambda d: np.mean(d[k]) if d.get(k) else np.nan)
        lo, hi = LAB_RANGES[k]
        df[f"_lab_{k}"] = clip_range(vals, lo, hi)

    # ---- Clinicians ----
    df["_clinicians"] = df["_clinician_raw"].apply(normalize_clinician)

    # ---- Text completeness ----
    for c in ALL_TEXT_COLUMNS:
        if c in df.columns:
            df[f"_len_{c}"] = df[c].fillna("").astype(str).str.len()

    # ---- Render PDF ----
    with PdfPages(pdf_path) as pdf:
        render_cover(pdf, df)
        render_overview(pdf, df)
        render_record_types(pdf, df)
        render_demographics(pdf, df)
        render_visits(pdf, df)
        render_diagnoses(pdf, df)
        render_treatments(pdf, df)
        render_investigations(pdf, df)
        render_vitals(pdf, df)
        render_labs(pdf, df)
        render_chief_complaints(pdf, df)
        render_clinicians(pdf, df)
        render_text_completeness(pdf, df)
        render_keyword_analysis(pdf, df)
        render_appendix(pdf, df)


# ---------------------------------------------------------------------------
# Individual pages
# ---------------------------------------------------------------------------
def render_cover(pdf, df):
    fig = plt.figure(figsize=(8.5, 11))
    ax = fig.add_subplot(111)
    ax.axis("off")
    ax.text(0.5, 0.78, "Siaya Medical Camp",
            ha="center", fontsize=34, fontweight="bold", color="#1F2D3D",
            transform=ax.transAxes)
    ax.text(0.5, 0.72, "Deep Analysis Report",
            ha="center", fontsize=22, color="#2E86AB",
            transform=ax.transAxes)
    ax.hlines(0.66, 0.2, 0.8, transform=ax.transAxes,
              color="#2E86AB", linewidth=2)

    n = len(df)
    dx_total = sum(df["_n_diagnoses"]) if "_n_diagnoses" in df else 0
    tx_total = sum(df["_n_treatments"]) if "_n_treatments" in df else 0
    dates = df["_doc_date"].dropna() if "_doc_date" in df else pd.Series(dtype="datetime64[ns]")
    date_range = (f"{dates.min():%Y-%m-%d} → {dates.max():%Y-%m-%d}"
                  if len(dates) else "No parseable dates")
    record_types = ", ".join(f"{k} ({v})" for k, v in
                              df["record_type"].fillna("Unknown").value_counts().items()) \
                   if "record_type" in df else "—"

    summary = [
        f"Records analyzed:         {n}",
        f"Record types:              {record_types}",
        f"Total diagnoses recorded:  {dx_total}",
        f"Total treatments recorded: {tx_total}",
        f"Visit/document date range: {date_range}",
        "",
        f"Generated: {datetime.now():%Y-%m-%d %H:%M}",
    ]
    ax.text(0.5, 0.55, "\n".join(summary),
            ha="center", fontsize=12, family="DejaVu Sans Mono",
            transform=ax.transAxes, color="#333333")
    ax.text(0.5, 0.08, "Confidential — for internal clinical review",
            ha="center", fontsize=9, style="italic", color="#888888",
            transform=ax.transAxes)
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_overview(pdf, df):
    n = len(df)
    raw_cols = [c for c in df.columns if not c.startswith("_")]
    missing = df[raw_cols].isna().sum().sort_values(ascending=False)
    missing_pct = (missing / n * 100).round(1) if n else missing
    completeness = (100 - missing_pct)

    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(11, 8.5), gridspec_kw={"width_ratios": [1, 1.4]})
    fig.suptitle("Dataset Overview & Data Quality", y=0.98)

    ax_left.axis("off")
    kpi_rows = [
        ("Records",              f"{n}"),
        ("Columns (raw)",        f"{len(raw_cols)}"),
        ("Mean diagnoses / rec", f"{df['_n_diagnoses'].mean():.1f}" if n else "—"),
        ("Mean treatments / rec",f"{df['_n_treatments'].mean():.1f}" if n else "—"),
        ("Mean tests / rec",     f"{df['_n_investigations'].mean():.1f}" if n else "—"),
        ("Median age (yrs)",     f"{df.loc[df['_age_unit']=='years','_age_value'].median():.0f}"
                                  if (df["_age_unit"] == "years").any() else "—"),
        ("Median GA (weeks)",    f"{df['_ga_weeks'].median():.0f}" if df["_ga_weeks"].notna().any() else "—"),
        ("Distinct clinicians",  f"{len(set(c for lst in df['_clinicians'] for c in lst))}" if "_clinicians" in df else "—"),
    ]
    y = 0.92
    for label, val in kpi_rows:
        ax_left.text(0.05, y, label, fontsize=11, color="#555555", transform=ax_left.transAxes)
        ax_left.text(0.55, y, val, fontsize=13, fontweight="bold", color="#1F2D3D", transform=ax_left.transAxes)
        y -= 0.07

    completeness.sort_values(inplace=True)
    if completeness.empty:
        empty_axis(ax_right, "No columns")
    else:
        colors = ["#C73E1D" if v < 50 else "#F18F01" if v < 80 else "#3B7A57" for v in completeness.values]
        ax_right.barh(np.arange(len(completeness)), completeness.values, color=colors, edgecolor="white")
        ax_right.set_yticks(np.arange(len(completeness)))
        ax_right.set_yticklabels(completeness.index, fontsize=7)
        ax_right.set_xlim(0, 105)
        ax_right.set_xlabel("Completeness (%)")
        ax_right.set_title("Column completeness")
        for i, v in enumerate(completeness.values):
            ax_right.text(v + 1, i, f"{v:.0f}%", va="center", fontsize=6, color="#555555")

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_record_types(pdf, df):
    """New page (no equivalent in the neonatal-only analyzer): this table
    merges multiple ingestion sources (V2/V3 EMR camp visits today, possibly
    PDF-extracted neonatal records in future), so surface the mix explicitly."""
    fig, axes = plt.subplots(1, 2, figsize=(11, 6.5))
    fig.suptitle("Record Type Mix", y=0.98)

    ax = axes[0]
    if "record_type" not in df or df["record_type"].dropna().empty:
        empty_axis(ax, "No record_type column")
    else:
        counts = df["record_type"].fillna("Unknown").value_counts()
        ax.pie(counts.values, labels=counts.index, autopct="%1.0f%%",
               colors=PALETTE[:len(counts)], startangle=90,
               wedgeprops={"edgecolor": "white", "linewidth": 1.5})
        ax.set_title("Records by source")

    ax = axes[1]
    has_camp = df["camp_file_number"].notna().sum() if "camp_file_number" in df else 0
    has_neonatal = df["baby_name"].notna().sum() if "baby_name" in df else 0
    has_emr_visit = df["visit_date"].notna().sum() if "visit_date" in df else 0
    labels = ["Has camp_file_number", "Has baby_name", "Has visit_date"]
    values = [has_camp, has_neonatal, has_emr_visit]
    if sum(values) == 0:
        empty_axis(ax, "No family-identifying fields populated")
    else:
        hbar(ax, labels, values, color=PALETTE[4], title="Record-family field coverage")

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_demographics(pdf, df):
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("Patient Demographics", y=0.98)

    ax = axes[0, 0]
    sex_counts = df["_sex_norm"].value_counts(dropna=False).rename(index={np.nan: "Unknown"})
    if sex_counts.sum() == 0:
        empty_axis(ax, "No sex data")
    else:
        ax.pie(sex_counts.values, labels=sex_counts.index, autopct="%1.0f%%",
               colors=PALETTE[:len(sex_counts)], startangle=90,
               wedgeprops={"edgecolor": "white", "linewidth": 1.5})
        ax.set_title("Sex distribution")

    ax = axes[0, 1]
    ages = df.loc[df["_age_unit"] == "years", "_age_value"].dropna()
    if ages.empty:
        empty_axis(ax, "No age (years) extracted")
    else:
        ax.hist(ages, bins=min(15, max(3, int(ages.nunique()))), color=PALETTE[1], edgecolor="white")
        ax.set_xlabel("Age (years)")
        ax.set_ylabel("Records")
        ax.set_title("Age distribution")

    ax = axes[1, 0]
    ga = df["_ga_weeks"].dropna()
    bw = df["_birth_weight_kg"].dropna()
    if ga.empty and bw.empty:
        empty_axis(ax, "No neonatal gestational-age / birth-weight data")
    elif not ga.empty:
        ax.hist(ga, bins=range(20, 43, 2), color=PALETTE[2], edgecolor="white")
        ax.axvline(37, color="#C73E1D", linestyle="--", label="Term (37w)")
        ax.set_xlabel("Gestational age (weeks)")
        ax.set_ylabel("Records")
        ax.set_title("Gestational age distribution")
        ax.legend()
    else:
        ax.hist(bw, bins=10, color=PALETTE[2], edgecolor="white")
        ax.axvline(2.5, color="#C73E1D", linestyle="--", label="LBW < 2.5 kg")
        ax.set_xlabel("Birth weight (kg)")
        ax.set_ylabel("Records")
        ax.set_title("Birth weight distribution")
        ax.legend()

    ax = axes[1, 1]
    unit_counts = df["_age_unit"].value_counts(dropna=False).rename(index={np.nan: "Unknown"})
    if unit_counts.sum() == 0:
        empty_axis(ax, "No age info")
    else:
        ax.bar(unit_counts.index, unit_counts.values, color=PALETTE[3], edgecolor="white")
        ax.set_title("Age expressed as…")
        ax.set_ylabel("Records")

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_visits(pdf, df):
    fig, axes = plt.subplots(2, 1, figsize=(11, 8.5))
    fig.suptitle("Visits / Documents", y=0.98)

    ax = axes[0]
    dt_col = first_non_null_col(df, "document_type", "record_type")
    if dt_col is None:
        empty_axis(ax, "No document_type / record_type column")
    else:
        dt = dt_col.fillna("Unknown").value_counts().head(15)
        hbar(ax, list(dt.index), list(dt.values), color=PALETTE[0], title="Document / record types")

    ax = axes[1]
    dates = df["_doc_date"].dropna()
    if dates.empty:
        empty_axis(ax, "No parseable visit/document dates")
    else:
        span_days = (dates.max() - dates.min()).days
        if span_days > 400:
            freq, label, width = "M", "Records per month", 25
        elif span_days > 60:
            freq, label, width = "W", "Records per week", 6
        else:
            freq, label, width = "D", "Records per day", 0.8
        ts = dates.dt.to_period(freq).value_counts().sort_index()
        x = ts.index.to_timestamp()
        ax.bar(x, ts.values, width=width, color=PALETTE[1], edgecolor="white")
        ax.set_title(label)
        ax.set_ylabel("Count")
        locator = mdates.AutoDateLocator(minticks=5, maxticks=12)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def first_non_null_col(df, *names):
    cols = [df[n] for n in names if n in df.columns]
    if not cols:
        return None
    out = cols[0].copy()
    for c in cols[1:]:
        out = out.where(out.notna(), c)
    return out


def render_diagnoses(pdf, df):
    fig, axes = plt.subplots(2, 1, figsize=(11, 8.5))
    fig.suptitle("Diagnoses", y=0.98)

    items = list_value_counts(df["_diagnoses_list"], top=15)
    ax = axes[0]
    if not items:
        empty_axis(ax, "No diagnoses extracted")
    else:
        labels, vals = zip(*items)
        hbar(ax, list(labels), list(vals), color=PALETTE[0], title="Top diagnoses (verbatim)")

    ax = axes[1]
    bucket_counter = Counter()
    for lst in df["_diagnoses_list"]:
        for d in lst:
            bucket_counter[bucket_diagnosis(d)] += 1
    if not bucket_counter:
        empty_axis(ax, "No diagnoses to bucket")
    else:
        items = sorted(bucket_counter.items(), key=lambda kv: -kv[1])
        labels, vals = zip(*items)
        hbar(ax, list(labels), list(vals), color=PALETTE[2], title="Diagnoses by category")

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)

    if len(df) > 1:
        diag_sets = [set(bucket_diagnosis(d) for d in lst) for lst in df["_diagnoses_list"] if lst]
        categories = sorted({b for s in diag_sets for b in s})
        if categories:
            mat = np.zeros((len(categories), len(categories)), dtype=int)
            for s in diag_sets:
                for a in s:
                    for b in s:
                        mat[categories.index(a)][categories.index(b)] += 1
            fig, ax = plt.subplots(figsize=(8.5, 7.5))
            im = ax.imshow(mat, cmap="Blues")
            ax.set_xticks(range(len(categories)))
            ax.set_yticks(range(len(categories)))
            ax.set_xticklabels(categories, rotation=45, ha="right", fontsize=8)
            ax.set_yticklabels(categories, fontsize=8)
            ax.set_title("Diagnosis category co-occurrence", pad=14)
            for i in range(len(categories)):
                for j in range(len(categories)):
                    ax.text(j, i, mat[i, j], ha="center", va="center",
                            color="white" if mat[i, j] > mat.max() / 2 else "#333333",
                            fontsize=8)
            fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="Co-occurrences")
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)


def render_treatments(pdf, df):
    fig, axes = plt.subplots(2, 1, figsize=(11, 8.5))
    fig.suptitle("Treatments & Medications", y=0.98)

    items = list_value_counts(df["_treatments_list"], top=15)
    ax = axes[0]
    if not items:
        empty_axis(ax, "No treatments extracted")
    else:
        labels, vals = zip(*items)
        hbar(ax, list(labels), list(vals), color=PALETTE[5], title="Top treatments (verbatim)")

    ax = axes[1]
    abx, non_abx = 0, 0
    for lst in df["_treatments_list"]:
        for t in lst:
            if is_antibiotic(t):
                abx += 1
            else:
                non_abx += 1
    total = abx + non_abx
    if total == 0:
        empty_axis(ax, "No treatment items to classify")
    else:
        ax.pie([abx, non_abx], labels=[f"Antibiotic ({abx})", f"Other ({non_abx})"],
               colors=[PALETTE[3], PALETTE[6]], startangle=90, autopct="%1.0f%%",
               wedgeprops={"edgecolor": "white", "linewidth": 1.5})
        ax.set_title("Antibiotic vs. other treatments")

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_investigations(pdf, df):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.suptitle("Investigations", y=0.98)
    items = list_value_counts(df["_investigations_list"], top=20)
    if not items:
        empty_axis(ax, "No investigations extracted")
    else:
        labels, vals = zip(*items)
        hbar(ax, list(labels), list(vals), color=PALETTE[4], title="Top investigations (verbatim)")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_vitals(pdf, df):
    fig, axes = plt.subplots(3, 3, figsize=(12, 11))
    fig.suptitle("Vitals (structured columns, free-text fallback)", y=0.985)
    plots = [
        ("_v_SBP",  "Systolic BP\n(mmHg)",  PALETTE[0]),
        ("_v_DBP",  "Diastolic BP\n(mmHg)", PALETTE[0]),
        ("_v_HR",   "Heart Rate\n(bpm)",    PALETTE[1]),
        ("_v_RR",   "Respiratory Rate",     PALETTE[2]),
        ("_v_Temp", "Temperature\n(°C)",    PALETTE[3]),
        ("_v_SpO2", "SpO₂ (%)",             PALETTE[5]),
        ("_v_Weight", "Weight (kg)",        PALETTE[6]),
        ("_v_Height", "Height (cm)",        PALETTE[7]),
        ("_v_BMI",  "Body Mass Index",      PALETTE[8]),
    ]
    for ax, (key, label, color) in zip(axes.flat, plots):
        flat_label = label.replace("\n", " ")
        s = pd.to_numeric(df[key], errors="coerce").dropna()
        if s.empty:
            empty_axis(ax, f"No {flat_label} data")
            continue
        if len(s) == 1:
            ax.bar([0], [s.iloc[0]], color=color, edgecolor="white")
            ax.set_xticks([])
            ax.set_title(f"{label}\n(single value: {s.iloc[0]:.1f})", fontsize=9)
        else:
            ax.hist(s, bins=min(15, max(3, len(s) // 2)), color=color, edgecolor="white")
            ax.set_title(f"{label}\n(n={len(s)}, μ={s.mean():.1f})", fontsize=9)
        ax.set_ylabel("Records", fontsize=8)
        ax.tick_params(axis="both", labelsize=7)
    fig.subplots_adjust(hspace=0.6, wspace=0.35, top=0.90, bottom=0.05, left=0.06, right=0.98)
    pdf.savefig(fig)
    plt.close(fig)


def _plot_lab_hist(ax, s, label, color, fontsize=9):
    if len(s) == 1:
        ax.bar([0], [s.iloc[0]], color=color, edgecolor="white")
        ax.set_xticks([])
        ax.set_title(f"{label}\n(single value: {s.iloc[0]:.2f})", fontsize=fontsize)
    else:
        ax.hist(s, bins=min(15, max(3, len(s) // 2)), color=color, edgecolor="white")
        ax.set_title(f"{label}\n(n={len(s)}, μ={s.mean():.2f}, median={s.median():.2f})", fontsize=fontsize)
    ax.set_ylabel("Records", fontsize=8)
    ax.tick_params(axis="both", labelsize=7)


def render_labs(pdf, df):
    """Blood sugar is a structured field extracted for nearly every record;
    the other lab values (WBC/Hb/PLT/…) only surface when free-text
    investigations/lab_results happen to mention them, so they're usually
    sparse. Give blood sugar a large, dedicated panel and only spend space
    on the others when they actually have data."""
    keys = ("WBC", "Hb", "PLT", "Hct", "Glucose", "CRP", "TSB")
    series_map = {k: df[f"_lab_{k}"].dropna() for k in keys}
    blood_sugar = df["_v_BloodSugar"].dropna()
    populated = [k for k in keys if not series_map[k].empty]

    fig = plt.figure(figsize=(12, 11))
    fig.suptitle("Blood Sugar & Lab Values", y=0.98)
    gs = fig.add_gridspec(3, 4, height_ratios=[1.6, 1, 1], hspace=0.65, wspace=0.4,
                           top=0.90, bottom=0.06, left=0.07, right=0.97)

    ax_main = fig.add_subplot(gs[0, :])
    if blood_sugar.empty:
        empty_axis(ax_main, "No blood sugar data")
    else:
        _plot_lab_hist(ax_main, blood_sugar, "Blood Sugar (mmol/L)", PALETTE[9], fontsize=14)
        ax_main.set_xlabel("mmol/L")
        ax_main.set_ylabel("Records")
        ax_main.tick_params(axis="both", labelsize=9)

    slots = [(r, c) for r in (1, 2) for c in range(4)]
    if not populated:
        ax_note = fig.add_subplot(gs[1:, :])
        empty_axis(
            ax_note,
            "No WBC / Hb / PLT / Hct / Glucose / CRP / TSB values could be\n"
            "extracted from investigations / lab_results free text",
        )
    else:
        for (r, c), key in zip(slots, populated):
            ax = fig.add_subplot(gs[r, c])
            _plot_lab_hist(ax, series_map[key], key, PALETTE[7])
        for (r, c) in slots[len(populated):]:
            fig.add_subplot(gs[r, c]).axis("off")

    pdf.savefig(fig)
    plt.close(fig)


def render_chief_complaints(pdf, df):
    """New page: general camp visits carry structured triage/complaint text
    that the neonatal-only analyzer had no equivalent column for."""
    if "chief_complaints" not in df.columns:
        return
    items = keyword_counts(df["chief_complaints"], min_len=4, top=20)
    if not items:
        return
    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.suptitle("Chief Complaints — Keyword Analysis", y=0.98)
    labels, vals = zip(*items)
    hbar(ax, list(labels), list(vals), color=PALETTE[3],
         title="Most frequent chief-complaint words (length ≥ 4)")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_clinicians(pdf, df):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.suptitle("Clinician activity", y=0.98)
    counter = Counter()
    for lst in df["_clinicians"]:
        for c in lst:
            counter[c] += 1
    items = counter.most_common(20)
    if not items:
        empty_axis(ax, "No clinician names extracted")
    else:
        labels, vals = zip(*items)
        hbar(ax, list(labels), list(vals), color=PALETTE[6],
             title="Records authored / attended (top 20)")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_text_completeness(pdf, df):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.suptitle("Free-text field length distribution", y=0.98)
    cols = [c for c in ALL_TEXT_COLUMNS if f"_len_{c}" in df.columns and df[f"_len_{c}"].sum() > 0]
    if not cols:
        empty_axis(ax, "No text columns")
    else:
        data = [df[f"_len_{c}"].values for c in cols]
        bp = ax.boxplot(data, tick_labels=cols, patch_artist=True, vert=False)
        for patch, color in zip(bp["boxes"], PALETTE * 4):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        ax.set_xlabel("Characters")
        ax.set_title("Free-text field length per record")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_keyword_analysis(pdf, df):
    items = keyword_counts(df["_clinical_text"], top=30)
    if not items:
        return
    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.suptitle("Top keywords in clinical summary / doctors' notes", y=0.98)
    labels, vals = zip(*items)
    hbar(ax, list(labels), list(vals), color=PALETTE[8],
         title="Most frequent meaningful words (length ≥ 4)")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_appendix(pdf, df):
    page_text(pdf, "Appendix — Representative Records", [
        f"Showing up to 3 records from the dataset (n={len(df)}).",
        "",
    ])
    for i, (_, row) in enumerate(df.head(3).iterrows(), 1):
        subject = row.get("_subject_name") or row.get("source_file") or "?"
        lines = [f"Record {i}: {subject}", ""]
        for col in ("record_type", "source_file", "_identifier", "sex", "age",
                    "date_of_birth", "gestational_age", "age_or_day_of_life",
                    "birth_weight", "current_weight", "_clinician_raw"):
            if col in row.index:
                val = row[col] if pd.notna(row[col]) else "—"
                lines.append(f"  {col.lstrip('_'):24s}: {val}")
        lines.append("")
        for label, lst_col in (("diagnoses", "_diagnoses_list"),
                                ("treatments", "_treatments_list"),
                                ("investigations", "_investigations_list")):
            lst = row.get(lst_col) or []
            lines.append(f"  {label} ({len(lst)}):")
            for it in lst[:10]:
                lines.append(f"      • {it}")
            if len(lst) > 10:
                lines.append(f"      … (+{len(lst)-10} more)")
            lines.append("")
        for label, txt_col in (("clinical_text", "_clinical_text"),
                                ("discharge_plan", "discharge_plan")):
            val = row.get(txt_col)
            if pd.notna(val):
                lines.append(f"  {label}:")
                lines.extend(f"      {w}" for w in textwrap.wrap(str(val), width=100)[:8])
                lines.append("")
        page_text(pdf, f"Record {i}", lines, fontsize=9, wrap=100)


# ---------------------------------------------------------------------------
# Snowflake loading
# ---------------------------------------------------------------------------
def _snowflake_connect():
    import os
    import snowflake.connector

    kwargs: dict[str, Any] = dict(
        user=os.getenv("SNOWFLAKE_USER", "").strip(),
        account=os.getenv("SNOWFLAKE_ACCOUNT", "").strip(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "").strip(),
        role=os.getenv("SNOWFLAKE_ROLE", "").strip() or None,
        database=TARGET_DB,
        schema=TARGET_SCHEMA,
    )
    pk_path = (os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or "").strip()
    pwd     = (os.getenv("SNOWFLAKE_PASSWORD") or "").strip()
    if pk_path:
        kwargs["private_key_file"] = pk_path
    elif pwd:
        kwargs["password"] = pwd
    else:
        raise RuntimeError("Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD in .env")
    return snowflake.connector.connect(**kwargs)


def fetch_from_snowflake(since: str | None, record_type: str | None, limit: int | None) -> pd.DataFrame:
    conn = _snowflake_connect()
    try:
        where, params = [], []
        if since:
            where.append("(VISIT_DATE >= %s OR DOCUMENT_DATE >= %s)")
            params += [since, since]
        if record_type:
            where.append("RECORD_TYPE = %s")
            params.append(record_type)
        sql = f"SELECT * FROM {TARGET_DB}.{TARGET_SCHEMA}.{TARGET_TABLE}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        if limit:
            sql += f" LIMIT {int(limit)}"
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1] if __doc__ else "")
    ap.add_argument("input_csv", type=Path, nargs="?", default=None,
                     help="Optional CSV export of the table; omit to query Snowflake directly")
    ap.add_argument("-o", "--output", type=Path, default=Path("siaya_medical_camp_report.pdf"),
                    help="Output PDF path (default: siaya_medical_camp_report.pdf)")
    ap.add_argument("--since", default=None, help="only rows with VISIT_DATE/DOCUMENT_DATE on/after this date (YYYY-MM-DD)")
    ap.add_argument("--record-type", default=None, help="filter to a single RECORD_TYPE (e.g. V3_EMR_VISIT)")
    ap.add_argument("--limit", type=int, default=None, help="cap number of rows fetched from Snowflake")
    args = ap.parse_args()

    if args.input_csv is not None:
        if not args.input_csv.exists():
            print(f"ERROR: input not found: {args.input_csv}", file=sys.stderr)
            sys.exit(1)
        df = pd.read_csv(args.input_csv)
        print(f"Loaded {len(df)} rows × {len(df.columns)} columns from {args.input_csv}")
    else:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parent / ".env", override=False)
        df = fetch_from_snowflake(args.since, args.record_type, args.limit)
        print(f"Loaded {len(df)} rows × {len(df.columns)} columns from "
              f"{TARGET_DB}.{TARGET_SCHEMA}.{TARGET_TABLE}")

    if df.empty:
        print("WARNING: no rows to analyze", file=sys.stderr)

    build_report(df, args.output)
    print(f"Wrote PDF report → {args.output}")


if __name__ == "__main__":
    main()
