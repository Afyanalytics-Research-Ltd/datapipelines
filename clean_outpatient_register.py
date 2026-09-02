#!/usr/bin/env python3
"""
clean_outpatient_register.py

Clean up a text-extracted "OUTPATIENT REGISTER WITH DISEASES" PDF and
produce a tidy CSV with columns: PNO, FULL_NAMES, SEX, AGE, RESIDENCE, ATT_DIAGNOSIS.

SOURCE FORMAT
-------------
The register prints one record per line, in this column order:

    PNO. FULL NAMES SEX ATT. AGE RESIDENCE DIAGNOSIS

which comes out of extraction as, e.g.:

    0020/2025 JOHN RANGON MR M 60 Yrs. MESWO : MESWO - Upper respiratory tract

i.e.  PNO/YEAR  NAME  [TITLE]  SEX  [REV]  AGE  RESIDENCE : ATT  -  DIAGNOSIS

KNOWN DATA QUIRKS (inherent to the source file, not fixable by this script)
----------------------------------------------------------------------------
1. DIAGNOSIS TRUNCATION: the register's own diagnosis column is narrow, so
   long diagnosis names are cut off mid-word (e.g. "Upper respiratory tract"
   is missing "infection"). This is baked into the source PDF itself -- the
   full text was never printed, so it cannot be recovered here.

2. PAGE-BREAK TRUNCATION: occasionally the last record on a page is cut off
   before the " - DIAGNOSIS" part is printed at all (rare). These rows are
   kept with a blank diagnosis (flagged as "[UNKNOWN - ATT]") and counted in
   the run summary rather than silently dropped.

3. DUPLICATE-SPAM BLOCKS: some blocks repeat the exact same record (same
   PNO + same diagnosis) many times in a row, sometimes across dozens of
   pages -- clearly a source artifact, not real repeat visits. These are
   deduplicated by (PNO, diagnosis, name, age, sex). Records that share a
   PNO but have DIFFERENT diagnoses (real comorbidities recorded on the
   same visit) are kept as separate rows.

OUTPUT SCHEMA NOTE
-------------------
The source table's own header is "... AGE RESIDENCE DIAGNOSIS", with ATT
(a short attending/facility code) as its own column. Since the requested
output has no separate ATT column, ATT_DIAGNOSIS is built as:

    "<diagnosis> [<att_code>]"

Edit `format_att_diagnosis()` below if you'd rather drop the ATT code and
just keep the diagnosis text.

USAGE
-----
    python clean_outpatient_register.py input.pdf -o output.csv
    python clean_outpatient_register.py input.txt -o output.csv   # plain text also accepted

    # Merge a new source's records into an already-cleaned CSV, skipping any
    # record that duplicates one already in it (same PNO, diagnosis, name,
    # age, sex). Writes a .bak backup of the existing file before overwriting.
    python clean_outpatient_register.py new_source.pdf --merge-into outpatient_register_cleaned.csv

Requires `pdfplumber` for PDF input (`pip install pdfplumber`).
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

TITLE_TOKENS = {
    "MR", "MRS", "MISS", "MSS", "MS", "DR", "BBY", "BABY", "MNR", "MTR",
}

BOILERPLATE_PATTERNS = [
    re.compile(r"^\s*OUTPATIENT REGISTER WITH DISEASES", re.I),
    re.compile(r"^\s*Page\s+\d+\s+of\s+\d+\s+BETWEEN", re.I),
    re.compile(r"^\s*BETWEEN\s+\d{2}/\d{2}/\d{4}\s+AND\s+\d{2}/\d{2}/\d{4}", re.I),
    re.compile(r"^\s*PNO\.?\s*FULL NAMES", re.I),
    re.compile(r"^\s*\d{2}/\d{2}/\d{4}\s*(\.\.\.\s*Continued)?\s*$", re.I),
    re.compile(r"^\s*SUMMARY FOR\s+\d{2}/\d{2}/\d{4}", re.I),
    re.compile(r"^\s*FIRST ATTENDANCES", re.I),
    re.compile(r"^\s*REATTENDANCES", re.I),
    re.compile(r"^\s*TOTAL\s*\.*\s*\d*\s*$", re.I),
    re.compile(r"^\s*MALES\s*\.*.*FEMALES", re.I),
    re.compile(r"^\s*[\d\s.]+$"),  # stray tally lines (digits/dots only)
]

RECORD_RE = re.compile(
    r"^(?P<pno>\d{3,6}/\d{4})\s+"
    r"(?P<namepart>.+?)\s+"
    r"(?P<sex>[MF])\s+"
    r"(?:REV\s+)?"
    r"(?P<age>\d+\s*Yrs\.?(?:\s*\d+\s*Mon\.?)?|\d+\s*Mon\.?|\d+\s*Ds)\s+"
    r"(?P<residence>.+?)\s*:\s*(?P<att>.+?)"
    r"(?:\s*-\s*(?P<diagnosis>.*))?\s*$"
)


def extract_text(path: Path) -> str:
    if path.suffix.lower() == ".pdf":
        try:
            import pdfplumber
        except ImportError:
            sys.exit(
                "pdfplumber is required to read PDF input.\n"
                "Install it with:  pip install pdfplumber"
            )
        pages = []
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                pages.append(page.extract_text() or "")
        return "\n".join(pages)
    return path.read_text(encoding="utf-8", errors="replace")


def is_boilerplate(line: str) -> bool:
    return any(p.match(line) for p in BOILERPLATE_PATTERNS)


def clean_name(raw_name: str) -> str:
    tokens = [t.strip(" ,.") for t in raw_name.split()]
    tokens = [t for t in tokens if t and t.upper() not in TITLE_TOKENS]
    return " ".join(tokens)


def clean_age(age_text: str) -> str:
    age_text = re.sub(r"\.", "", age_text)
    age_text = re.sub(r"\s+", " ", age_text).strip()
    return age_text


def format_att_diagnosis(diagnosis: str, att: str) -> str:
    diagnosis = (diagnosis or "").strip(" ,.-")
    if not diagnosis:
        return f"[UNKNOWN - {att}]" if att else "UNKNOWN"
    if not att:
        return diagnosis
    return f"{diagnosis} [{att}]"


def parse_line(line: str) -> dict | None:
    m = RECORD_RE.match(line.strip())
    if not m:
        return None
    return {
        "pno": m.group("pno"),
        "full_names": clean_name(m.group("namepart")),
        "sex": m.group("sex"),
        "age": clean_age(m.group("age")),
        "residence": m.group("residence").strip(" ,."),
        "att": m.group("att").strip(" ,."),
        "diagnosis": (m.group("diagnosis") or "").strip(" ,.-"),
    }


def parse_records(text: str) -> tuple[list[dict], int]:
    records = []
    unmatched = 0
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or is_boilerplate(line):
            continue
        record = parse_line(line)
        if record is None:
            unmatched += 1
            continue
        records.append(record)
    return records, unmatched


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()


def dedupe_key(r: dict) -> tuple:
    """Identity key for a record: same patient number, diagnosis, name, age
    and sex. Records sharing a PNO with a genuinely different diagnosis
    (real comorbidities) are NOT considered duplicates."""
    return (
        str(r["pno"]).strip(),
        _norm(r["diagnosis"]),
        _norm(r["full_names"]),
        str(r["sex"]).strip().upper(),
        _norm(r["age"]),
    )


def dedupe(records: list[dict]) -> tuple[list[dict], int]:
    """Drop exact duplicates (same PNO, diagnosis, name, age, sex) -- see
    quirk #3 above."""
    seen = set()
    out = []
    dropped = 0
    for r in records:
        key = dedupe_key(r)
        if key in seen:
            dropped += 1
            continue
        seen.add(key)
        out.append(r)
    return out, dropped


def extract_diagnosis_from_combined(att_diagnosis: str) -> str:
    """Recover the raw diagnosis text from a previously-written
    "<diagnosis> [<att>]" ATT_DIAGNOSIS cell, for building a dedupe key
    against records loaded back in from an existing output CSV."""
    m = re.match(r"^(.*?)(?:\s*\[[^\[\]]*\])?$", att_diagnosis.strip())
    return m.group(1).strip() if m else att_diagnosis.strip()


def load_existing_csv(path: Path) -> list[dict]:
    records = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append({
                "pno": row["PNO"],
                "full_names": row["FULL_NAMES"],
                "sex": row["SEX"],
                "age": row["AGE"],
                "residence": row["RESIDENCE"],
                "diagnosis": extract_diagnosis_from_combined(row["ATT_DIAGNOSIS"]),
                "att": "",
                "_combined_override": row["ATT_DIAGNOSIS"],
            })
    return records


def merge_records(existing: list[dict], new: list[dict]) -> tuple[list[dict], int]:
    """Union existing (already-established) records with newly parsed ones,
    dropping any new record that duplicates an existing one by dedupe_key."""
    seen = {dedupe_key(r) for r in existing}
    merged = list(existing)
    skipped = 0
    for r in new:
        key = dedupe_key(r)
        if key in seen:
            skipped += 1
            continue
        seen.add(key)
        merged.append(r)
    return merged, skipped


def write_csv(records: list[dict], out_path: Path) -> None:
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["PNO", "FULL_NAMES", "SEX", "AGE", "RESIDENCE", "ATT_DIAGNOSIS"])
        for r in records:
            att_diagnosis = r.get("_combined_override") or format_att_diagnosis(r["diagnosis"], r["att"])
            writer.writerow([
                r["pno"],
                r["full_names"],
                r["sex"],
                r["age"],
                r["residence"],
                att_diagnosis,
            ])


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[3] if __doc__ else "")
    ap.add_argument("input", type=Path, help="Source register (.pdf or .txt)")
    ap.add_argument("-o", "--output", type=Path, default=None,
                     help="Output CSV path (default: outpatient_register_cleaned.csv, "
                          "or --merge-into's path if given)")
    ap.add_argument("--merge-into", type=Path, default=None,
                     help="Existing cleaned CSV to merge these records into. Duplicates "
                          "(same PNO, diagnosis, name, age, sex) already present are skipped. "
                          "A .bak backup of this file is written before it's overwritten.")
    args = ap.parse_args()

    if not args.input.exists():
        sys.exit(f"ERROR: input not found: {args.input}")

    output = args.output or args.merge_into or Path("outpatient_register_cleaned.csv")

    text = extract_text(args.input)
    records, unmatched = parse_records(text)
    total_parsed = len(records)
    records, dropped = dedupe(records)

    if args.merge_into:
        if not args.merge_into.exists():
            sys.exit(f"ERROR: --merge-into file not found: {args.merge_into}")
        existing = load_existing_csv(args.merge_into)
        backup = args.merge_into.with_suffix(args.merge_into.suffix + ".bak")
        backup.write_bytes(args.merge_into.read_bytes())
        merged, cross_dupes = merge_records(existing, records)
        write_csv(merged, output)
        blank_diagnosis = sum(1 for r in merged if not r["diagnosis"])

        print(f"Parsed {total_parsed} record rows from new source "
              f"({unmatched} lines could not be parsed and were skipped).")
        print(f"Dropped {dropped} exact-duplicate rows within the new source.")
        print(f"Existing records loaded from {args.merge_into}: {len(existing)}")
        print(f"Skipped {cross_dupes} new rows that duplicated an existing record "
              f"(same PNO, diagnosis, name, age, sex).")
        print(f"Backed up original to {backup}")
        if blank_diagnosis:
            print(f"WARNING: {blank_diagnosis} merged rows have no diagnosis text.")
        print(f"Wrote {len(merged)} merged rows to {output}")
        return

    blank_diagnosis = sum(1 for r in records if not r["diagnosis"])
    write_csv(records, output)

    print(f"Parsed {total_parsed} record rows ({unmatched} lines could not be parsed and were skipped).")
    print(f"Dropped {dropped} exact-duplicate rows.")
    if blank_diagnosis:
        print(f"WARNING: {blank_diagnosis} rows have no diagnosis text "
              f"(likely cut off at a page break in the source PDF) -- flagged as [UNKNOWN - ATT].")
    print(f"Wrote {len(records)} rows to {output}")


if __name__ == "__main__":
    main()
