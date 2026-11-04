"""
One-off load: Aroha Cancer Centre's oncology price list (an Excel sheet, not an HMS) -> Snowflake.

Source: "Oncology Sha  Cash prices - Aroha cancer center.xlsx" (received 2026-11-03), kept out of git at
downloads/aroha/aroha_prices.xlsx. One sheet: DRUG NAME, STRENGTH, CASH PRICES, SHA PRICES, Buying Price (122 rows,
oncology + supportive drugs; no stock quantities).

RAW   HOSPITALS.AROHA_RAW.PRICE_LIST: every non-empty row, every cell as the text Excel holds, plus _SOURCE_ROW /
      _SHEET / _SOURCE_FILE / _FILE_SHA256 / _LOADED_AT. Nothing is fixed here.
CLEAN HOSPITALS.AROHA_CLEAN, built from the RAW text read back from Snowflake:
      PRODUCTS (one per drug + strength + unit, with the uuid5 aroha_to_v3.py posts it with) and DATA_ISSUES
      (every value that was fixed, guessed or dropped).

Cleaning rules (each applied value is logged in DATA_ISSUES):
  * Prices: the leading number of the cell ('30/TAB', '108 per tab', '30 P/TAB' -> 30). Text naming a tablet or
    ampoule makes that the product's unit, so the price is per tablet / ampoule. A cell with two numbers
    ('64500/41500', '100/Ampoule50') keeps the one a duplicate row agrees with, else the first.
  * Units: per-tablet / per-ampoule text above; else CREAM -> Tube; ZOLADEX (implant) -> Piece; PACK / 10'S / 21s /
    P.O / oral-only drugs -> Pack (Tablet when priced under 100); everything else (infusions, injections) -> Vial.
  * Duplicates: rows with the same drug + strength + unit are one product; prices both rows agree on win, otherwise
    the later row's (CETUXIMAB 100MG appears twice).
  * Missing cash price: the product keeps no cash price (V3 then shows 0), it is never copied from SHA.
  * Drug names are kept as the centre spells them (only spaces trimmed).

Airflow-free; by hand, inside the Airflow worker:
    docker exec -w /opt/airflow/pipelines datapipelines-airflow-worker-1 python load_aroha_to_snowflake.py
    ... python load_aroha_to_snowflake.py --dry-run     # parse + clean locally, print the summary, write nothing
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import uuid
from collections import Counter
from pathlib import Path

from load_portbell_to_snowflake import col_name, raw_text, replace_table

log = logging.getLogger("aroha_load")

DEFAULT_XLSX = Path(__file__).resolve().parent / "downloads" / "aroha" / "aroha_prices.xlsx"
SF_DB = "HOSPITALS"
RAW_SCHEMA, CLEAN_SCHEMA = "AROHA_RAW", "AROHA_CLEAN"
RAW_TABLE = "PRICE_LIST"
NS = uuid.UUID("5d2e8b47-91c3-4f0a-b6d8-3a7e1c9f0b26")      # never change: V3 rows are matched by these uuids
ORAL = re.compile(r"\bP\.?O\b|PACK|\d+\s*'?S\b|\bTAB|ABRATERONE|ANASTROZOLE|BECALATUMIDE|CASODEX|OLAPARIB|PAZOPANIB", re.I)
INJECTION = re.compile(r"\bINJ|\bI\.?V\b", re.I)
IMPLANT = re.compile(r"ZOLADEX", re.I)


def gid(kind: str, key) -> str:
    return str(uuid.uuid5(NS, f"aroha/{kind}/{key}"))


# ── RAW: the sheet as text ─────────────────────────────────────────────────
def read_workbook(path: Path) -> tuple[list[str], list[dict]]:
    """(columns, rows) of the one sheet; the header is the first row with DRUG NAME, empty rows/columns dropped."""
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]
    rows = list(ws.iter_rows(values_only=True))
    h = next(i for i, r in enumerate(rows) if r and str(r[0] or "").strip().upper() == "DRUG NAME")
    header, body = rows[h], rows[h + 1:]
    width = max(len(r) for r in rows)
    used = [j for j in range(width)
            if (j < len(header) and header[j] is not None) or any(j < len(r) and raw_text(r[j]) and str(r[j]).strip() for r in body)]
    cols = [col_name(header[j] if j < len(header) else None, j) for j in used]
    recs = []
    for i, r in enumerate(body, start=h + 2):
        vals = [raw_text(r[j]) if j < len(r) else None for j in used]
        if any(v and v.strip() for v in vals):
            recs.append({**dict(zip(cols, vals)), "_SOURCE_ROW": i, "_SHEET": ws.title})
    return cols, recs


# ── CLEAN: pure helpers ────────────────────────────────────────────────────
def clean(v) -> str | None:
    s = re.sub(r"\s+", " ", str(v)).strip() if v is not None else ""
    return s or None


def price(v) -> tuple[list[float], str | None]:
    """(numbers in the cell, unit the text names: 'Tablet' / 'Ampoule' / None)."""
    s = clean(v)
    if s is None:
        return [], None
    nums = [float(n) for n in re.findall(r"\d+(?:\.\d+)?", s)]
    unit = "Tablet" if re.search(r"tab", s, re.I) else "Ampoule" if re.search(r"amp", s, re.I) else None
    return nums, unit


def unit_for(name: str, priced_per: str | None, top_price: float | None) -> str:
    if priced_per:
        return priced_per
    if re.search(r"CREAM", name, re.I):
        return "Tube"
    if IMPLANT.search(name):
        return "Piece"
    if ORAL.search(name) and not INJECTION.search(name):
        return "Tablet" if top_price is not None and top_price < 100 else "Pack"     # BUSCOPAN P.O at 15 is per tablet
    return "Vial"


class Cleaner:
    def __init__(self, raw: list[dict]):
        self.raw = raw
        self.issues: list[dict] = []

    def issue(self, row, column, raw_value, problem, action):
        self.issues.append({"source_row": row, "column": column, "raw_value": None if raw_value is None else str(raw_value),
                            "issue": problem, "action": action})

    def build(self) -> dict[str, list[dict]]:
        lines = []
        for r in self.raw:
            row, name = r["_SOURCE_ROW"], clean(r.get("DRUG_NAME"))
            if not name:
                self.issue(row, "DRUG_NAME", None, "row without a drug name", "dropped")
                continue
            strength = clean(r.get("STRENGTH"))
            cells = {c: (r.get(c), *price(r.get(c))) for c in ("CASH_PRICES", "SHA_PRICES", "BUYING_PRICE")}
            per = next((u for c, (_, _, u) in cells.items() if u and c != "BUYING_PRICE"), None)
            top = max((n for c in ("CASH_PRICES", "SHA_PRICES") for n in cells[c][1]), default=None)
            lines.append({"row": row, "name": name, "strength": strength, "cells": cells,
                          "unit": unit_for(name, per, top), "key": f"{name.upper()}|{(strength or '').upper()}"})
        groups: dict[str, list[dict]] = {}
        for ln in lines:
            groups.setdefault(f"{ln['key']}|{ln['unit']}", []).append(ln)

        products = []
        for n, (key, grp) in enumerate(groups.items(), start=1):
            first, rows = grp[0], [g["row"] for g in grp]
            if len(grp) > 1:
                self.issue(rows[-1], "DRUG_NAME", first["name"], f"duplicate of row(s) {rows[:-1]}", "merged into one product")
            vals = {}
            for c in ("CASH_PRICES", "SHA_PRICES", "BUYING_PRICE"):
                seen = [(g["row"], g["cells"][c]) for g in grp if g["cells"][c][1]]
                singles = Counter(nums[0] for _, (_, nums, _) in seen if len(nums) == 1)
                pick = None
                for row, (raw, nums, _) in seen:
                    if len(nums) == 1:
                        pick = nums[0]
                    else:
                        agreed = [x for x in nums if x in singles]
                        choice = agreed[0] if agreed else nums[0]
                        self.issue(row, c, raw, "cell holds several numbers",
                                   f"kept {choice:g}" + (" (matches the duplicate row)" if agreed else " (the first)"))
                        pick = pick if pick is not None and agreed and pick == choice else choice
                if len(singles) > 1:
                    self.issue(rows[-1], c, json.dumps([raw for _, (raw, _, _) in seen]), "duplicate rows disagree",
                               f"kept {pick:g}")
                for row, (raw, nums, _) in seen:
                    if len(nums) == 1 and raw is not None and not re.fullmatch(r"\s*\d+(\.\d+)?\s*", str(raw)):
                        self.issue(row, c, raw, "price written as text", f"read as {nums[0]:g}")
                vals[c] = pick
            if vals["CASH_PRICES"] is None:
                self.issue(rows[0], "CASH_PRICES", None, "no cash price", "cash price left empty (not copied from SHA)")
            if vals["SHA_PRICES"] is None:
                self.issue(rows[0], "SHA_PRICES", None, "no SHA price", "SHA price left empty")
            if vals["BUYING_PRICE"] is None:
                self.issue(rows[0], "BUYING_PRICE", None, "no buying price", "cost left empty")
            elif vals["CASH_PRICES"] and vals["BUYING_PRICE"] > vals["CASH_PRICES"]:
                self.issue(rows[0], "BUYING_PRICE", vals["BUYING_PRICE"],
                           f"buying price above cash price {vals['CASH_PRICES']:g} (probably per pack vs per {first['unit'].lower()})",
                           "kept as given")
            same_name = [k for k in groups if k.split("|")[:2] == key.split("|")[:2]]
            squash = lambda s: re.sub(r"\s+", "", (s or "").upper())
            label = first["name"] + (f" {first['strength']}" if first["strength"]
                                     and squash(first["strength"]) not in squash(first["name"]) else "")
            if len(same_name) > 1:
                label += f" ({first['unit']})"
                self.issue(rows[0], "DRUG_NAME", first["name"], f"same drug and strength sold as {len(same_name)} units",
                           f"named '{label}'")
            products.append({
                "product_uuid": gid("product", key), "stock_uuid": gid("stock", key), "code": f"ARC-{n:03d}",
                "generic_name": label, "drug_name": first["name"], "strength": first["strength"], "unit": first["unit"],
                "cash_price": vals["CASH_PRICES"], "sha_price": vals["SHA_PRICES"], "buying_price": vals["BUYING_PRICE"],
                "source_rows": ",".join(map(str, rows))})
        return {"PRODUCTS": products, "DATA_ISSUES": self.issues}


CLEAN_TYPES = {"PRODUCTS": {"cash_price": "FLOAT", "sha_price": "FLOAT", "buying_price": "FLOAT"},
               "DATA_ISSUES": {"source_row": "INTEGER"}}


# ── Snowflake ──────────────────────────────────────────────────────────────
def summary(tables: dict[str, list[dict]]) -> dict:
    p = tables["PRODUCTS"]
    return {"products": len(p), "units": dict(Counter(x["unit"] for x in p)),
            "without_cash_price": sum(1 for x in p if x["cash_price"] is None),
            "without_sha_price": sum(1 for x in p if x["sha_price"] is None),
            "data_issues": dict(Counter(x["issue"].split(" (")[0].split(" row")[0] for x in tables["DATA_ISSUES"]))}


def run(xlsx: str | Path = DEFAULT_XLSX, *, dry_run: bool = False) -> dict:
    path = Path(xlsx)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    cols, rows = read_workbook(path)
    log.info("%s (sha256 %s…): %d rows, columns %s", path.name, sha[:12], len(rows), cols)
    if dry_run:
        tables = Cleaner(rows).build()
        return {"file": path.name, "sha256": sha, "dry_run": True, **summary(tables), "issues": tables["DATA_ISSUES"]}
    from datetime import datetime, timezone
    import snowflake_to_v3_migration as s2v3
    sf = s2v3._snowflake_connect()
    try:
        cur = sf.cursor()
        for schema in (RAW_SCHEMA, CLEAN_SCHEMA):
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{schema}")
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        columns = [(c, "VARCHAR") for c in cols] + [("_SOURCE_ROW", "INTEGER"), ("_SHEET", "VARCHAR"),
                                                    ("_SOURCE_FILE", "VARCHAR"), ("_FILE_SHA256", "VARCHAR"),
                                                    ("_LOADED_AT", "TIMESTAMP_NTZ")]
        replace_table(cur, f"{SF_DB}.{RAW_SCHEMA}.{RAW_TABLE}", columns,
                      [tuple(r.get(c) for c in cols) + (r["_SOURCE_ROW"], r["_SHEET"], path.name, sha, now) for r in rows])
        log.info("  RAW   %-12s %4d rows", RAW_TABLE, len(rows))
        cur.execute(f'SELECT * FROM {SF_DB}.{RAW_SCHEMA}.{RAW_TABLE} ORDER BY "_SOURCE_ROW"')
        names = [c[0] for c in cur.description]
        tables = Cleaner([dict(zip(names, r)) for r in cur.fetchall()]).build()   # CLEAN from RAW, not the workbook
        for table, trows in tables.items():
            tcols = list(trows[0]) if trows else ["source_row"]
            types = CLEAN_TYPES.get(table, {})
            replace_table(cur, f"{SF_DB}.{CLEAN_SCHEMA}.{table}", [(c.upper(), types.get(c, "VARCHAR")) for c in tcols],
                          [tuple(r.get(c) for c in tcols) for r in trows])
            log.info("  CLEAN %-12s %4d rows", table, len(trows))
    finally:
        sf.close()
    out = {"file": path.name, "sha256": sha, "dry_run": False, **summary(tables)}
    log.info("summary: %s", json.dumps(out, default=str))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Aroha oncology price list -> Snowflake AROHA_RAW / AROHA_CLEAN")
    ap.add_argument("xlsx", nargs="?", default=str(DEFAULT_XLSX))
    ap.add_argument("--dry-run", action="store_true", help="parse and clean locally; write nothing")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    if not a.dry_run:
        sys.path.insert(0, "/opt/airflow/dags")
        from pipelines_common import use_pipelines_dir
        use_pipelines_dir([])
    print(json.dumps(run(a.xlsx, dry_run=a.dry_run), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
