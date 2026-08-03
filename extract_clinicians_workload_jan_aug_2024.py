#!/usr/bin/env python3
"""
Extract the "CLINICIANS WORK LOAD - (PATIENTS SEEN)" table from
2024-2025report.pdf and write out just the name + Jan 2024..Aug 2024
columns (page 1 headers, page 1/4/7/10/13 data rows) to CSV.
"""
import csv
import re
import sys

import pdfplumber

SOURCE_PDF = "C:/Users/luthe/Downloads/2024-2025report.pdf"
OUTPUT_CSV = "2024_clinicians_workload_Jan-Aug2024.csv"

COLUMNS = [
    "name", "Jan 2024", "Feb 2024", "Mar 2024", "Apr 2024",
    "May 2024", "Jun 2024", "Jul 2024", "Aug 2024",
]

# Data rows for Jan-Aug 2024 live on these pages (1-indexed); the other
# pages only carry later months / totals for the same clinicians.
DATA_PAGES = [1, 4, 7, 10, 13]

ROW_RE = re.compile(r"^(?P<name>[A-Za-z][A-Za-z0-9.\-]*)\s+(?P<rest>[\d,\s]+)$")


def parse_page(page):
    rows = []
    text = page.extract_text() or ""
    for line in text.splitlines():
        line = line.strip()
        m = ROW_RE.match(line)
        if not m:
            continue
        name = m.group("name")
        nums = [n.replace(",", "") for n in m.group("rest").split()]
        if len(nums) < 8:
            continue
        rows.append([name] + nums[:8])
    return rows


def main():
    with pdfplumber.open(SOURCE_PDF) as pdf:
        all_rows = []
        for page_num in DATA_PAGES:
            page = pdf.pages[page_num - 1]
            all_rows.extend(parse_page(page))

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        writer.writerows(all_rows)

    print(f"Wrote {len(all_rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    sys.exit(main())
