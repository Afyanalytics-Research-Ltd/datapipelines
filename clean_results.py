import ast
import re
import pandas as pd

INPUT_FILE = "results_motherfransisca_flat.csv"
OUTPUT_FILE = "results_motherfransisca_cleaned.csv"

# Values that mean "no data" but were serialized as literal strings
NULL_LIKE = {"", "null", "none", "nan", "[]", "{}", "n/a", "na"}


def parse_stringified_literal(value):
    """Turn "['Goitre']" or "[{'references': [...], 'value': 'X'}]" into readable text."""
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not (stripped.startswith("[") or stripped.startswith("{")):
        return value
    try:
        parsed = ast.literal_eval(stripped)
    except (ValueError, SyntaxError):
        return value

    if isinstance(parsed, dict):
        parsed = [parsed]
    if isinstance(parsed, list):
        if not parsed:
            return None
        parts = []
        for item in parsed:
            if isinstance(item, dict):
                if "value" in item:
                    parts.append(str(item["value"]))
            else:
                parts.append(str(item))
        return "; ".join(p for p in parts if p) or None
    return value


def clean_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, str):
        value = value.strip()
        if value.lower() in NULL_LIKE:
            return None
        value = parse_stringified_literal(value)
        if isinstance(value, str):
            value = value.strip()
            value = value if value and value.lower() not in NULL_LIKE else None
    return value


def main():
    df = pd.read_csv(INPUT_FILE, dtype=str)

    # Keep filename + core extraction.* fields; drop the provenance/reference
    # columns (extraction_metadata.*) and pipeline bookkeeping (job_id,
    # duration_ms, credit_usage, org_id, version, ...).
    keep_cols = ["filename"] + [
        c for c in df.columns
        if c.startswith("extraction.") and not c.startswith("extraction_metadata.")
    ]
    # Surface data-quality flags if present, for QA purposes.
    for qa_col in ("metadata.schema_violation_error", "metadata.warnings"):
        if qa_col in df.columns:
            keep_cols.append(qa_col)

    df = df[keep_cols]

    # Normalize null-like placeholders and unwrap stringified lists/dicts.
    df = df.map(clean_value)

    # NOTE: extraction.document_date is left as cleaned text rather than
    # parsed to datetime — the source dates are inconsistent OCR output
    # (e.g. "25/7/26.", "25.7-26", "2026-07-26 15:21") and can't be
    # reliably normalized without guessing.

    # Drop exact duplicate rows and rows with no extraction data at all.
    df = df.drop_duplicates()
    data_cols = [c for c in df.columns if c != "filename"]
    df = df.dropna(subset=data_cols, how="all")

    # extraction.document_type -> extraction_document_type
    df.columns = [re.sub(r"\.", "_", c) for c in df.columns]

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Cleaned {len(df)} rows -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
