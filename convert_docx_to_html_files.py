from pathlib import Path
import mammoth

# ---- 1. Read paths from the TXT file ----
txt_file = Path("/opt/softwares/automations_and_data_pipelines/file_list_20251212_092317.txt")  # ← change to your actual file name

with open(txt_file, "r", encoding="utf-8") as f:
    docx_paths = [Path(line.strip()) for line in f if line.strip()]

# ---- 2. Create output folder ----
output_folder = Path("data/html_outputs")
output_folder.mkdir(exist_ok=True)

# ---- 3. Convert each DOCX to HTML ----
for docx_path in docx_paths:
    if not docx_path.exists():
        print(f"[SKIPPED] File not found → {docx_path}")
        continue

    try:
        with open(docx_path, "rb") as docx_file:
            result = mammoth.convert_to_html(docx_file)
            html = result.value

        # Output filename = same as DOCX but .html
        output_file = output_folder / (docx_path.stem + ".html")

        with open(output_file, "w", encoding="utf-8") as html_file:
            html_file.write(html)

        print(f"[DONE] {docx_path} → {output_file}")

    except Exception as e:
        print(f"[ERROR] Could not convert {docx_path}: {e}")
