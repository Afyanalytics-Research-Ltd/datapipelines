from pathlib import Path
import pdfplumber  # pip install pdfplumber

# ---- 1. Read paths from the TXT file ----
txt_file = Path("/opt/softwares/automations_and_data_pipelines/file_list_20251211_174207.txt")  # your PDF list file

with open(txt_file, "r", encoding="utf-8") as f:
    pdf_paths = [Path(line.strip()) for line in f if line.strip()]

# ---- 2. Create output folder ----
output_folder = Path("data/text_outputs")
output_folder.mkdir(parents=True, exist_ok=True)

# ---- 3. Convert each PDF to TXT ----
for pdf_path in pdf_paths:
    if not pdf_path.exists():
        print(f"[SKIPPED] File not found → {pdf_path}")
        continue

    try:
        text_content = ""
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text_content += page.extract_text() + "\n"

        # Output filename = same as PDF but .txt
        output_file = output_folder / (pdf_path.stem + ".txt")

        with open(output_file, "w", encoding="utf-8") as txt_file:
            txt_file.write(text_content)

        print(f"[DONE] {pdf_path} → {output_file}")

    except Exception as e:
        print(f"[ERROR] Could not convert {pdf_path}: {e}")
