import pandas as pd
from pathlib import Path

# Folder containing your CSV files
CSV_FOLDER = Path("/opt/softwares/automations_and_data_pipelines/data/csvs/patient_history_bahari_medical")

# Output file
OUTPUT_FILE = "/opt/softwares/automations_and_data_pipelines/data/csvs/patient_history_bahari_medical/combined.csv"

# List all .csv files
csv_files = list(CSV_FOLDER.glob("*.csv"))

if not csv_files:
    print("No CSV files found.")
    exit()

print(f"Found {len(csv_files)} CSV files. Combining...")

# Read and combine
df_list = []
for file in csv_files:
    try:
        df = pd.read_csv(file)
        df_list.append(df)
        print(f"Loaded: {file.name}")
    except Exception as e:
        print(f"Error reading {file.name}: {e}")

# Concatenate all dataframes
combined_df = pd.concat(df_list, ignore_index=True)

# Remove duplicate rows (optional)
combined_df = combined_df.drop_duplicates()

# Save result
combined_df.to_csv(OUTPUT_FILE, index=False)

print(f"Combined CSV saved to: {OUTPUT_FILE}")
print(f"Total rows: {combined_df.shape[0]}")
