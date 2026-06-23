#!/usr/bin/env python3
"""
medical_records_analyzer.py

Deep analysis of an extracted medical-records CSV (one row per document) with
columns like:
    filename, document_type, document_date, baby_name, sex, gestational_age,
    age_or_day_of_life, birth_weight, current_weight, delivery_details,
    apgar_scores, diagnoses, clinical_summary, vitals, examination_findings,
    investigations, treatments, feeding_and_support, discharge_plan,
    clinician_name, raw_notes

Produces a multi-page PDF report containing:
  * Cover & executive summary
  * Dataset overview and data-quality / missingness
  * Patient demographics (sex, gestational age, birth weight, age)
  * Document-type and document-date distributions
  * Diagnosis frequency, category buckets, and co-occurrence heatmap
  * Treatment / medication frequency and antibiotic share
  * Investigation patterns and extracted lab values (WBC, Hb, PLT, …)
  * Vitals extracted from free-text (BP, HR, RR, Temp, SpO2)
  * Clinician activity
  * Text-completeness / length analysis
  * Top-keyword analysis of clinical summaries
  * Appendix with representative full records

Usage:
    python medical_records_analyzer.py INPUT.csv -o report.pdf
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

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

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
    "delivery_details", "clinical_summary", "vitals", "examination_findings",
    "feeding_and_support", "discharge_plan", "raw_notes",
]

# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------
def safe_parse_list(x):
    """Turn a Python-literal list-string back into a Python list."""
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
        return [s]


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
    return (None, None)


def parse_vitals(text):
    """Pull numeric vitals out of a free-text vitals/exam string."""
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
    """Pull WBC / Hb / PLT / Hct / Glucose out of investigation strings."""
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


def parse_date(x):
    if x is None or pd.isna(x):
        return pd.NaT
    s = str(x).strip()
    for fmt in ("%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return pd.to_datetime(s, errors="coerce", dayfirst=True)


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


def list_value_counts(df, col, top=25):
    c = Counter()
    for v in df[col].apply(safe_parse_list):
        for item in v:
            key = str(item).strip()
            if key:
                c[key] += 1
    return c.most_common(top)


DIAG_BUCKETS = [
    ("Prematurity / LBW",         ["prematur", "preterm", "low birth", "lbw", "<2 kg", "vlbw"]),
    ("Jaundice",                   ["jaundice", "icter"]),
    ("Sepsis / Infection",         ["sepsis", "infect", "pneumon", "meningit"]),
    ("Respiratory",                ["rds", "respir", "asphyx", "meconium", "apnea"]),
    ("Anemia",                     ["anemi", "anaemia"]),
    ("Miscarriage / Abortion",     ["miscarriage", "abortion"]),
    ("Hypertensive disorders",     ["preeclamp", "eclamp", "hypertens", "pih"]),
    ("Cardiac",                    ["cardiac", "heart", "pda", "asd", "vsd"]),
    ("Neurological",               ["seizur", "encephalo", "hie"]),
    ("GI / Feeding",               ["nec", "gastro", "feeding intoler", "vomit"]),
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
    n = len(df)

    # ---- Derived columns ----
    df = df.copy()
    df["_birth_weight_kg"] = df["birth_weight"].apply(parse_weight_kg)
    df["_current_weight_kg"] = df["current_weight"].apply(parse_weight_kg) if "current_weight" in df else np.nan
    df["_ga_weeks"] = df["gestational_age"].apply(parse_gestational_weeks)
    df["_age_tuple"] = df["age_or_day_of_life"].apply(parse_age)
    df["_age_unit"] = df["_age_tuple"].apply(lambda t: t[0])
    df["_age_value"] = df["_age_tuple"].apply(lambda t: t[1])
    df["_sex_norm"] = df["sex"].apply(normalize_sex)
    df["_doc_date"] = df["document_date"].apply(parse_date)

    # vitals extracted from BOTH the vitals column and examination_findings
    def extract_all_vitals(row):
        merged = {}
        for col in ("vitals", "examination_findings", "raw_notes"):
            if col in row and pd.notna(row[col]):
                merged.update(parse_vitals(row[col]))
        return merged
    df["_vitals_parsed"] = df.apply(extract_all_vitals, axis=1)
    for k in ("SBP", "DBP", "HR", "RR", "Temp", "SpO2"):
        df[f"_v_{k}"] = df["_vitals_parsed"].apply(lambda d: d.get(k))

    # diagnoses / treatments / investigations parsed
    df["_diagnoses_list"]     = df["diagnoses"].apply(safe_parse_list) if "diagnoses" in df else [[]] * n
    df["_treatments_list"]    = df["treatments"].apply(safe_parse_list) if "treatments" in df else [[]] * n
    df["_investigations_list"] = df["investigations"].apply(safe_parse_list) if "investigations" in df else [[]] * n
    df["_n_diagnoses"]     = df["_diagnoses_list"].apply(len)
    df["_n_treatments"]    = df["_treatments_list"].apply(len)
    df["_n_investigations"] = df["_investigations_list"].apply(len)

    # lab values
    df["_labs_parsed"] = df["_investigations_list"].apply(parse_labs)
    for k in ("WBC", "Hb", "PLT", "Hct", "Glucose", "CRP", "TSB"):
        df[f"_lab_{k}"] = df["_labs_parsed"].apply(lambda d: np.mean(d[k]) if d.get(k) else np.nan)

    # clinicians
    df["_clinicians"] = df["clinician_name"].apply(normalize_clinician) if "clinician_name" in df else [[]] * n

    # text completeness
    for c in ALL_TEXT_COLUMNS:
        if c in df:
            df[f"_len_{c}"] = df[c].fillna("").astype(str).str.len()

    # ---- Render PDF ----
    with PdfPages(pdf_path) as pdf:
        render_cover(pdf, df)
        render_overview(pdf, df)
        render_demographics(pdf, df)
        render_documents(pdf, df)
        render_diagnoses(pdf, df)
        render_treatments(pdf, df)
        render_investigations(pdf, df)
        render_vitals(pdf, df)
        render_labs(pdf, df)
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
    ax.text(0.5, 0.78, "Medical Records",
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

    summary = [
        f"Records analyzed:        {n}",
        f"Total diagnoses recorded: {dx_total}",
        f"Total treatments recorded: {tx_total}",
        f"Document date range:      {date_range}",
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
    missing = df[[c for c in df.columns if not c.startswith("_")]].isna().sum().sort_values(ascending=False)
    missing_pct = (missing / n * 100).round(1)
    completeness = (100 - missing_pct)

    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(11, 8.5), gridspec_kw={"width_ratios": [1, 1.4]})
    fig.suptitle("Dataset Overview & Data Quality", y=0.98)

    # KPIs on the left
    ax_left.axis("off")
    kpi_rows = [
        ("Records",              f"{n}"),
        ("Columns (raw)",        f"{sum(1 for c in df.columns if not c.startswith('_'))}"),
        ("Mean diagnoses / pt",  f"{df['_n_diagnoses'].mean():.1f}" if n else "—"),
        ("Mean treatments / pt", f"{df['_n_treatments'].mean():.1f}" if n else "—"),
        ("Mean tests / pt",      f"{df['_n_investigations'].mean():.1f}" if n else "—"),
        ("Median GA (weeks)",    f"{df['_ga_weeks'].median():.0f}" if df['_ga_weeks'].notna().any() else "—"),
        ("Median birth wt (kg)", f"{df['_birth_weight_kg'].median():.2f}" if df['_birth_weight_kg'].notna().any() else "—"),
        ("Distinct clinicians",  f"{len(set(c for lst in df['_clinicians'] for c in lst))}" if "_clinicians" in df else "—"),
    ]
    y = 0.92
    for label, val in kpi_rows:
        ax_left.text(0.05, y, label, fontsize=11, color="#555555", transform=ax_left.transAxes)
        ax_left.text(0.55, y, val, fontsize=13, fontweight="bold", color="#1F2D3D", transform=ax_left.transAxes)
        y -= 0.07

    # Completeness on the right
    completeness.sort_values(inplace=True)
    colors = ["#C73E1D" if v < 50 else "#F18F01" if v < 80 else "#3B7A57" for v in completeness.values]
    ax_right.barh(np.arange(len(completeness)), completeness.values, color=colors, edgecolor="white")
    ax_right.set_yticks(np.arange(len(completeness)))
    ax_right.set_yticklabels(completeness.index, fontsize=8)
    ax_right.set_xlim(0, 105)
    ax_right.set_xlabel("Completeness (%)")
    ax_right.set_title("Column completeness")
    for i, v in enumerate(completeness.values):
        ax_right.text(v + 1, i, f"{v:.0f}%", va="center", fontsize=7, color="#555555")

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_demographics(pdf, df):
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("Patient Demographics", y=0.98)

    # Sex
    ax = axes[0, 0]
    sex_counts = df["_sex_norm"].value_counts(dropna=False).rename(index={np.nan: "Unknown"})
    if sex_counts.sum() == 0:
        empty_axis(ax, "No sex data")
    else:
        ax.pie(sex_counts.values, labels=sex_counts.index, autopct="%1.0f%%",
               colors=PALETTE[:len(sex_counts)], startangle=90,
               wedgeprops={"edgecolor": "white", "linewidth": 1.5})
        ax.set_title("Sex distribution")

    # Gestational age
    ax = axes[0, 1]
    ga = df["_ga_weeks"].dropna()
    if ga.empty:
        empty_axis(ax, "No gestational age extracted")
    else:
        ax.hist(ga, bins=range(20, 43, 2), color=PALETTE[1], edgecolor="white")
        ax.axvline(37, color="#C73E1D", linestyle="--", label="Term (37w)")
        ax.set_xlabel("Gestational age (weeks)")
        ax.set_ylabel("Patients")
        ax.set_title("Gestational age distribution")
        ax.legend()

    # Birth weight
    ax = axes[1, 0]
    bw = df["_birth_weight_kg"].dropna()
    if bw.empty:
        empty_axis(ax, "No birth weight extracted")
    else:
        ax.hist(bw, bins=10, color=PALETTE[2], edgecolor="white")
        ax.axvline(2.5, color="#C73E1D", linestyle="--", label="LBW < 2.5 kg")
        ax.set_xlabel("Birth weight (kg)")
        ax.set_ylabel("Patients")
        ax.set_title("Birth weight distribution")
        ax.legend()

    # Age unit breakdown
    ax = axes[1, 1]
    unit_counts = df["_age_unit"].value_counts(dropna=False).rename(index={np.nan: "Unknown"})
    if unit_counts.sum() == 0:
        empty_axis(ax, "No age info")
    else:
        ax.bar(unit_counts.index, unit_counts.values, color=PALETTE[3], edgecolor="white")
        ax.set_title("Age expressed as…")
        ax.set_ylabel("Patients")

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_documents(pdf, df):
    fig, axes = plt.subplots(2, 1, figsize=(11, 8.5))
    fig.suptitle("Documents", y=0.98)

    ax = axes[0]
    if "document_type" in df:
        dt = df["document_type"].fillna("Unknown").value_counts().head(15)
        hbar(ax, list(dt.index), list(dt.values), color=PALETTE[0], title="Document types")
    else:
        empty_axis(ax, "No document_type column")

    ax = axes[1]
    dates = df["_doc_date"].dropna()
    if dates.empty:
        empty_axis(ax, "No parseable document dates")
    else:
        ts = dates.dt.to_period("D").value_counts().sort_index()
        ax.bar(ts.index.astype(str), ts.values, color=PALETTE[1], edgecolor="white")
        ax.set_title("Documents per day")
        ax.set_ylabel("Count")
        ax.tick_params(axis="x", rotation=45)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_diagnoses(pdf, df):
    fig, axes = plt.subplots(2, 1, figsize=(11, 8.5))
    fig.suptitle("Diagnoses", y=0.98)

    # Top raw diagnoses
    items = list_value_counts(df, "diagnoses", top=15) if "diagnoses" in df else []
    ax = axes[0]
    if not items:
        empty_axis(ax, "No diagnoses extracted")
    else:
        labels, vals = zip(*items)
        hbar(ax, list(labels), list(vals), color=PALETTE[0], title="Top diagnoses (verbatim)")

    # Bucketed
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

    # Co-occurrence heatmap (if >1 row)
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

    items = list_value_counts(df, "treatments", top=15) if "treatments" in df else []
    ax = axes[0]
    if not items:
        empty_axis(ax, "No treatments extracted")
    else:
        labels, vals = zip(*items)
        hbar(ax, list(labels), list(vals), color=PALETTE[5], title="Top treatments (verbatim)")

    # Antibiotic share
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
    items = list_value_counts(df, "investigations", top=20) if "investigations" in df else []
    if not items:
        empty_axis(ax, "No investigations extracted")
    else:
        labels, vals = zip(*items)
        hbar(ax, list(labels), list(vals), color=PALETTE[4], title="Top investigations (verbatim)")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_vitals(pdf, df):
    fig, axes = plt.subplots(2, 3, figsize=(11, 8.5))
    fig.suptitle("Vitals extracted from free-text", y=0.98)
    plots = [
        ("SBP",  "Systolic BP (mmHg)",   PALETTE[0]),
        ("DBP",  "Diastolic BP (mmHg)",  PALETTE[0]),
        ("HR",   "Heart Rate (bpm)",     PALETTE[1]),
        ("RR",   "Respiratory Rate",     PALETTE[2]),
        ("Temp", "Temperature (°C)",     PALETTE[3]),
        ("SpO2", "SpO₂ (%)",             PALETTE[5]),
    ]
    for ax, (key, label, color) in zip(axes.flat, plots):
        s = df[f"_v_{key}"].dropna()
        if s.empty:
            empty_axis(ax, f"No {key} data")
            continue
        if len(s) == 1:
            ax.bar([0], [s.iloc[0]], color=color, edgecolor="white")
            ax.set_xticks([0])
            ax.set_xticklabels([s.index[0] if isinstance(s.index[0], str) else "value"])
            ax.set_title(f"{label} (single value: {s.iloc[0]:.1f})", fontsize=10)
        else:
            ax.hist(s, bins=min(15, max(3, len(s) // 2)), color=color, edgecolor="white")
            ax.set_title(f"{label} (n={len(s)}, μ={s.mean():.1f})", fontsize=10)
        ax.set_xlabel(label)
        ax.set_ylabel("Records")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_labs(pdf, df):
    keys = ("WBC", "Hb", "PLT", "Hct", "Glucose", "CRP", "TSB")
    fig, axes = plt.subplots(3, 3, figsize=(11, 9.5))
    fig.suptitle("Lab values extracted from investigations", y=0.99)
    for ax, key in zip(axes.flat, keys):
        s = df[f"_lab_{key}"].dropna()
        if s.empty:
            empty_axis(ax, f"No {key}")
            continue
        if len(s) == 1:
            ax.bar([0], [s.iloc[0]], color=PALETTE[7], edgecolor="white")
            ax.set_xticks([])
            ax.set_title(f"{key} (single value: {s.iloc[0]:.2f})", fontsize=10)
        else:
            ax.hist(s, bins=min(10, max(3, len(s) // 2)), color=PALETTE[7], edgecolor="white")
            ax.set_title(f"{key} (n={len(s)}, μ={s.mean():.2f})", fontsize=10)
        ax.set_ylabel("Records")
    # blank the unused axes
    for ax in axes.flat[len(keys):]:
        empty_axis(ax, "")
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
             title="Documents authored / signed (top 20)")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_text_completeness(pdf, df):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.suptitle("Free-text field length distribution", y=0.98)
    cols = [c for c in ALL_TEXT_COLUMNS if f"_len_{c}" in df.columns]
    if not cols:
        empty_axis(ax, "No text columns")
    else:
        data = [df[f"_len_{c}"].values for c in cols]
        bp = ax.boxplot(data, labels=cols, patch_artist=True, vert=False)
        for patch, color in zip(bp["boxes"], PALETTE * 4):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        ax.set_xlabel("Characters")
        ax.set_title("Free-text field length per record")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_keyword_analysis(pdf, df):
    if "clinical_summary" not in df:
        return
    items = keyword_counts(df["clinical_summary"], top=30)
    if not items:
        return
    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.suptitle("Top keywords in clinical_summary", y=0.98)
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
        lines = [f"Record {i}: {row.get('filename', '?')}", ""]
        for col in ("document_type", "document_date", "baby_name", "sex",
                    "gestational_age", "age_or_day_of_life", "birth_weight",
                    "current_weight", "clinician_name"):
            if col in row:
                val = row[col] if pd.notna(row[col]) else "—"
                lines.append(f"  {col:24s}: {val}")
        lines.append("")
        for col in ("diagnoses", "treatments", "investigations"):
            if col in row:
                lst = safe_parse_list(row[col])
                lines.append(f"  {col} ({len(lst)}):")
                for it in lst[:10]:
                    lines.append(f"      • {it}")
                if len(lst) > 10:
                    lines.append(f"      … (+{len(lst)-10} more)")
                lines.append("")
        for col in ("clinical_summary", "discharge_plan"):
            if col in row and pd.notna(row[col]):
                lines.append(f"  {col}:")
                lines.extend(f"      {w}" for w in textwrap.wrap(str(row[col]), width=100)[:8])
                lines.append("")
        page_text(pdf, f"Record {i}", lines, fontsize=9, wrap=100)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1] if __doc__ else "")
    ap.add_argument("input_csv", type=Path, help="CSV file produced by the extraction pipeline")
    ap.add_argument("-o", "--output", type=Path, default=Path("medical_records_report.pdf"),
                    help="Output PDF path (default: medical_records_report.pdf)")
    args = ap.parse_args()

    if not args.input_csv.exists():
        print(f"ERROR: input not found: {args.input_csv}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(args.input_csv)
    print(f"Loaded {len(df)} rows × {len(df.columns)} columns from {args.input_csv}")
    build_report(df, args.output)
    print(f"Wrote PDF report → {args.output}")


if __name__ == "__main__":
    main()