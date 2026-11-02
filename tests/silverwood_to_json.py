#converting silverwood patient list pdf to json
# header row: # Patient No Patient Name Gender DOB Age Contact Email Residence Know About Us Registered By Reg. Date

#import libraries
import json
import re
from pathlib import Path

import pdfplumber


from pathlib import Path

# tests/silverwood_to_json.py -> project root is one level up
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

INPUT_PDF = DATA_DIR / "Patient_List (1).pdf"  
OUTPUT_JSON = DATA_DIR / "silverwood_patients.json"

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ROWNO_RE = re.compile(r"^\d+\.$")
PATIENT_NO_RE = re.compile(r"^[A-Z]+-\d+$")

HEADER_MAP = [  # (header word, field name)
    ("Gender", "gender"),
    ("DOB", "dob"),
    ("Age", "age"),
    ("Contact", "contact"),
    ("Email", "email"),
    ("Residence", "residence"),
    ("Know", "know_about_us"),
    ("Registered", "registered_by"),
    ("Reg.", "reg_date"),
]


def find_columns(words):
    """Return (header_top, [(field, x_start), ...]) from the header row."""
    hdr_top = next(w["top"] for w in words if w["text"] == "#")
    hdr = sorted(
        (w for w in words if abs(w["top"] - hdr_top) < 3), key=lambda w: w["x0"]
    )
    patient_words = [w for w in hdr if w["text"] == "Patient"]
    cols = [
        ("row_no", next(w["x0"] for w in hdr if w["text"] == "#")),
        ("patient_no", patient_words[0]["x0"]),
        ("patient_name", patient_words[1]["x0"]),
    ]
    for text, field in HEADER_MAP:
        cols.append((field, next(w["x0"] for w in hdr if w["text"] == text)))
    return hdr_top, sorted(cols, key=lambda c: c[1])


def column_of(word, cols):
    reg_by_start = dict(cols)["registered_by"]
    # Reg date is right-aligned, so it starts left of its header
    if DATE_RE.match(word["text"]) and word["x0"] > reg_by_start:
        return "reg_date"
    field = cols[0][0]
    for name, start in cols:
        if word["x0"] >= start - 4:
            field = name
    return field


def parse_page(page, cols=None):
    words = page.extract_words(keep_blank_chars=False, use_text_flow=False)
    hdr_top, page_cols = find_columns(words)
    cols = cols or page_cols
    starts = dict(cols)

    # Row anchors: "80.", "81.", ... in the first column
    anchors = sorted(
        (
            w for w in words
            if ROWNO_RE.match(w["text"])
            and w["top"] > hdr_top
            and w["x0"] < starts["patient_no"] - 2
        ),
        key=lambda w: w["top"],
    )
    if not anchors:
        return []

    tops = [a["top"] for a in anchors]
    pitch = (
        sorted(b - a for a, b in zip(tops, tops[1:]))[len(tops) // 2]
        if len(tops) > 1 else 40
    )
    max_dist = pitch * 0.6

    rows = [{"row_no": int(a["text"].rstrip(".")), "_cells": {}} for a in anchors]

    for w in words:
        if w["top"] <= hdr_top + 3 or w in anchors:
            continue
        field = column_of(w, cols)
        if field in ("row_no", "age"):  # age wraps across rows; derive from DOB instead
            continue
        idx = min(range(len(anchors)), key=lambda i: abs(anchors[i]["top"] - w["top"]))
        if abs(anchors[idx]["top"] - w["top"]) > max_dist:
            continue  # footer / page number etc.
        rows[idx]["_cells"].setdefault(field, []).append(w)

    records = []
    for r in rows:
        rec = {"row_no": r["row_no"]}
        for field in [
            "patient_no", "patient_name", "gender", "dob", "contact",
            "email", "residence", "know_about_us", "registered_by", "reg_date",
        ]:
            ws = sorted(r["_cells"].get(field, []), key=lambda w: (round(w["top"]), w["x0"]))
            rec[field] = " ".join(w["text"] for w in ws).strip() or None
        if rec["contact"]:
            rec["contact"] = re.sub(r"[\s-]", "", rec["contact"])
        records.append(rec)
    return records


def main():
    all_records = []
    with pdfplumber.open(INPUT_PDF) as pdf:
        cols = None
        for i, page in enumerate(pdf.pages, start=1):
            try:
                words = page.extract_words()
                _, cols_here = find_columns(words)
                cols = cols or cols_here
            except StopIteration:
                pass  # page without a header, reuse columns from earlier page
            recs = parse_page(page, cols)
            print(f"Page {i}: {len(recs)} rows")
            all_records.extend(recs)

    # Sanity checks
    bad = [r for r in all_records if not r["patient_no"] or not PATIENT_NO_RE.match(r["patient_no"])]
    if bad:
        print(f"WARNING: {len(bad)} rows with invalid patient_no, check: {bad[:3]}")
    dupes = len(all_records) - len({r["patient_no"] for r in all_records})
    if dupes:
        print(f"WARNING: {dupes} duplicate patient numbers")

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(all_records)} records to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()