from pathlib import Path
from datetime import datetime

root_folder = Path("/media/martin/NO NAME/Ultrasound reports/")

# Collect all files (excluding ~$ temp files)
path_extension = ".doc"
input_files = [
    f for f in root_folder.rglob(f"*{path_extension}")
    if not f.name.startswith("~$")
]

# Create a unique filename using timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = Path(f"file_list_{timestamp}.txt")

# Write results to the text file
with open(output_file, "w", encoding="utf-8") as f:
    for path in input_files:
        f.write(str(path) + "\n")

print(f"Saved {len(input_files)} file paths to {output_file}")
