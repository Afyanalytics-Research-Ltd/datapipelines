import json
import pandas as pd

INPUT = "results.csv"
OUTPUT = "results_flat.csv"

df = pd.read_csv(INPUT)

# Parse the JSON strings into Python dicts
parsed = df["extracted_json"].apply(json.loads)

# Expand each dict into its own columns
expanded = pd.json_normalize(parsed)

# Stringify list/dict cells so they render as the literal `['a', 'b']` form
# in CSV (matches your target output).
for col in expanded.columns:
    expanded[col] = expanded[col].apply(
        lambda v: str(v) if isinstance(v, (list, dict)) else v
    )

# Put filename first, then the expanded columns
out = pd.concat([df[["filename"]].reset_index(drop=True),
                 expanded.reset_index(drop=True)], axis=1)

out.to_csv(OUTPUT, index=False)
print(f"Wrote {len(out)} rows × {len(out.columns)} cols to {OUTPUT}")