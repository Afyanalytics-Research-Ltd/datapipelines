"""
Batch anonymize PDFs using LandingAI ADE Parse:
- Loops through all PDF files in a folder
- Calls LandingAI parse endpoint to extract text
- Anonymizes "IP NO: ..." and "NAME: ..." using HMAC-SHA256 (small cryptography)
- Writes an anonymized PDF per input file (text-based PDF)

Requirements:
  pip install requests python-dotenv reportlab

Env (.env):
  VA_API_KEY=xxxxxxxxxxxxxxxx
  # Optional (recommended):
  ANON_SALT=some-long-random-secret-string
"""

import os
import re
import json
import time
import hmac
import hashlib
from pathlib import Path

import requests
from dotenv import load_dotenv
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm


# ----------------------------
# Config
# ----------------------------
load_dotenv()

VA_API_KEY = os.getenv("VA_API_KEY")
ANON_SALT = os.getenv("ANON_SALT", "CHANGE_ME_TO_A_RANDOM_SECRET_SALT")

# Folder where all PDFs lie (your path)
INPUT_DIR = Path(r"/mnt/c/Users/luthe/Documents/afyadocs/")
OUTPUT_DIR = INPUT_DIR / "_anonymized_out"

LANDING_PARSE_URL = "https://api.va.landing.ai/v1/ade/parse"
MODEL_NAME = "dpt-2"

REQUEST_TIMEOUT = 120  # seconds
SLEEP_BETWEEN_CALLS = 0.4  # be gentle to API


# ----------------------------
# Helpers: crypto anonymization
# ----------------------------
def _hmac_token(value: str, prefix: str, length: int = 10) -> str:
    """
    Small cryptography: HMAC-SHA256(salt, value) -> short token.
    Deterministic: same input -> same output (good for linking across docs).
    """
    msg = value.strip().encode("utf-8")
    key = ANON_SALT.encode("utf-8")
    digest = hmac.new(key, msg, hashlib.sha256).hexdigest()
    return f"{prefix}-{digest[:length].upper()}"


def anonymize_text(text: str) -> str:
    """
    Replaces:
      IP NO: <value>  -> IP NO: IP-XXXXXXXXXX
      NAME: <value>   -> NAME: NM-XXXXXXXXXX

    Handles mild formatting differences (spaces, casing).
    """
    # Capture until end-of-line or double-space-ish break; tune as needed
    ip_pattern = re.compile(r"(?im)\bIP\s*NO\s*:\s*([^\n\r]+)")
    name_pattern = re.compile(r"(?im)\bNAME\s*:\s*([^\n\r]+)")

    def ip_repl(m):
        original = m.group(1).strip()
        token = _hmac_token(original, "IP", length=10)
        return f"IP NO: {token}"

    def name_repl(m):
        original = m.group(1).strip()
        token = _hmac_token(original, "NM", length=10)
        return f"NAME: {token}"

    text = ip_pattern.sub(ip_repl, text)
    text = name_pattern.sub(name_repl, text)
    return text


# ----------------------------
# LandingAI Parse extraction
# ----------------------------
def parse_pdf_with_landingai(pdf_file: Path) -> dict:
    if not VA_API_KEY:
        raise RuntimeError("VA_API_KEY is missing. Put it in your .env file.")

    headers = {"Authorization": f"Basic {VA_API_KEY}"}

    with pdf_file.open("rb") as f:
        resp = requests.post(
            url=LANDING_PARSE_URL,
            headers=headers,
            files=[("document", (pdf_file.name, f, "application/pdf"))],
            data={"model": MODEL_NAME},
            timeout=REQUEST_TIMEOUT,
        )

    # Raise on HTTP errors but keep body for debugging
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(
            f"LandingAI parse failed for {pdf_file.name}: {resp.status_code}\n{resp.text[:2000]}"
        ) from e

    return resp.json()


def extract_text(parse_json: dict) -> str:
    """
    LandingAI response shapes can vary by model/version.
    This tries common locations; if it can't, it falls back to a JSON pretty dump.
    """
    # 1) If API returns a direct text field
    for key in ("text", "document_text", "content"):
        if isinstance(parse_json.get(key), str) and parse_json[key].strip():
            return parse_json[key]

    # 2) Pages list with text fields
    pages = parse_json.get("pages")
    if isinstance(pages, list) and pages:
        chunks = []
        for p in pages:
            if isinstance(p, dict):
                if isinstance(p.get("text"), str):
                    chunks.append(p["text"])
                elif isinstance(p.get("content"), str):
                    chunks.append(p["content"])
                # Sometimes blocks/lines carry text
                elif isinstance(p.get("blocks"), list):
                    for b in p["blocks"]:
                        if isinstance(b, dict) and isinstance(b.get("text"), str):
                            chunks.append(b["text"])
                elif isinstance(p.get("lines"), list):
                    for ln in p["lines"]:
                        if isinstance(ln, dict) and isinstance(ln.get("text"), str):
                            chunks.append(ln["text"])
        joined = "\n\n".join([c for c in chunks if c and c.strip()])
        if joined.strip():
            return joined

    # 3) Generic fallback: search for all dict entries named "text"
    chunks = []

    def walk(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "text" and isinstance(v, str) and v.strip():
                    chunks.append(v)
                else:
                    walk(v)
        elif isinstance(obj, list):
            for it in obj:
                walk(it)

    walk(parse_json)
    if chunks:
        return "\n".join(chunks)

    # 4) Last resort: dump JSON (so you still get *something* in output)
    return json.dumps(parse_json, ensure_ascii=False, indent=2)


# ----------------------------
# PDF writer (text-based)
# ----------------------------
def write_text_pdf(out_path: Path, title: str, text: str) -> None:
    """
    Creates a simple, readable PDF containing the anonymized text.
    (This does not preserve original PDF layout.)
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    c = canvas.Canvas(str(out_path), pagesize=A4)
    width, height = A4

    left_margin = 15 * mm
    right_margin = 15 * mm
    top_margin = 15 * mm
    bottom_margin = 15 * mm

    y = height - top_margin

    # Title
    c.setFont("Helvetica-Bold", 12)
    c.drawString(left_margin, y, title)
    y -= 10 * mm

    # Body
    c.setFont("Helvetica", 10)
    line_height = 5 * mm

    max_width = width - left_margin - right_margin

    # Simple word-wrap
    for paragraph in text.splitlines():
        if not paragraph.strip():
            y -= line_height  # blank line
            if y < bottom_margin:
                c.showPage()
                c.setFont("Helvetica", 10)
                y = height - top_margin
            continue

        words = paragraph.split(" ")
        line = ""
        for w in words:
            candidate = (line + " " + w).strip()
            if c.stringWidth(candidate, "Helvetica", 10) <= max_width:
                line = candidate
            else:
                # draw current line
                c.drawString(left_margin, y, line)
                y -= line_height
                if y < bottom_margin:
                    c.showPage()
                    c.setFont("Helvetica", 10)
                    y = height - top_margin
                line = w

        if line:
            c.drawString(left_margin, y, line)
            y -= line_height
            if y < bottom_margin:
                c.showPage()
                c.setFont("Helvetica", 10)
                y = height - top_margin

    c.save()


# ----------------------------
# Main loop
# ----------------------------
def main():
    if not INPUT_DIR.exists():
        raise FileNotFoundError(f"Input folder does not exist: {INPUT_DIR}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pdf_files = sorted([p for p in INPUT_DIR.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"])
    if not pdf_files:
        print(f"No PDF files found in: {INPUT_DIR}")
        return

    print(f"Found {len(pdf_files)} PDFs in {INPUT_DIR}")
    print(f"Output folder: {OUTPUT_DIR}")

    for i, pdf in enumerate(pdf_files, start=1):
        if i == 2: break
        try:
            print(f"\n[{i}/{len(pdf_files)}] Parsing: {pdf.name}")
            parsed = parse_pdf_with_landingai(pdf)
            raw_text = extract_text(parsed)

            anon_text = anonymize_text(raw_text)

            out_pdf = OUTPUT_DIR / f"{pdf.stem}__ANON.pdf"
            write_text_pdf(out_pdf, title=f"Anonymized: {pdf.name}", text=anon_text)

            print(f"✅ Saved: {out_pdf}")

        except Exception as e:
            print(f"❌ Failed: {pdf.name}\n   {e}")

        time.sleep(SLEEP_BETWEEN_CALLS)


if __name__ == "__main__":
    main()