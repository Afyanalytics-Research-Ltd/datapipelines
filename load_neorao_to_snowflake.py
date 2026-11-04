"""
One-off load: Neorao Community Medical Centre's pharmacy inventory export (a CSV, not an HMS) -> Snowflake.

Source: "01-11-2026 Inventory (03-10-2026 to 01-11-2026).csv" (received 2026-11-04), kept out of git at
downloads/neorao/neorao_inventory_2026-11-01.csv. Columns: Facility_Name, Product, API, Tags, Volume, Retail_Price,
Cost_Price, In_Stock, Availability, Expiry, Total_Cost, Sales_Value (660 rows, stock as at 01-11-2026).

RAW   HOSPITALS.NEORAO_RAW.INVENTORY: every row, every cell as text (the unnamed 13th column too), plus _SOURCE_ROW /
      _SOURCE_FILE / _FILE_SHA256 / _LOADED_AT. Nothing is fixed here.
CLEAN HOSPITALS.NEORAO_CLEAN, built from the RAW text read back from Snowflake:
      PRODUCTS (one per row, with the uuid5s neorao_to_v3.py posts with), SERVICES (rows that are services, not stock;
      not loaded to V3) and DATA_ISSUES (every value that was fixed, guessed or dropped).

Cleaning rules (each applied value is logged in DATA_ISSUES):
  * A row whose Product held an unquoted comma spills one column to the right: the two halves are re-joined.
  * Name: Product as the centre has it (spaces trimmed, a trailing '/' dropped). 'Generic ...' products are named by
    their API ('Generic Adult G21' + Infusion IV Giving Set -> 'Infusion IV Giving Set Adult G21'); Volume is
    appended when not already in the name; names still shared by several rows get their Tags in brackets.
  * Retail_Price -> selling and insurance price, Cost_Price -> cost / trade price, In_Stock -> opening quantity.
  * Services (consultation fees, BP / sugar readings, wound dressing / suturing, implant removal, home-based care)
    go to SERVICES: their In_Stock (100-990) is a placeholder, not stock.
  * Category: tests -> Laboratory, supplies/devices -> Medical Supplies, else Pharmaceuticals. Unit from the
    Volume / name (Bottle, Tube, Vial, Ampoule, Tablet, Capsule, Sachet, Piece).
  * No expiry dates in the export: opening batches carry none.

Airflow-free; by hand, inside the Airflow worker:
    docker exec -w /opt/airflow/pipelines datapipelines-airflow-worker-1 python load_neorao_to_snowflake.py
    ... python load_neorao_to_snowflake.py --dry-run     # parse + clean locally, print the summary, write nothing
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import re
import sys
import uuid
from collections import Counter
from pathlib import Path

from load_portbell_to_snowflake import col_name, replace_table

log = logging.getLogger("neorao_load")

DEFAULT_CSV = Path(__file__).resolve().parent / "downloads" / "neorao" / "neorao_inventory_2026-11-01.csv"
SF_DB = "HOSPITALS"
RAW_SCHEMA, CLEAN_SCHEMA = "NEORAO_RAW", "NEORAO_CLEAN"
RAW_TABLE = "INVENTORY"
NS = uuid.UUID("8c1f4e2a-6b3d-4d7e-9a05-2f7b1c8e4d63")      # never change: V3 rows are matched by these uuids
COLS = ["FACILITY_NAME", "PRODUCT", "API", "TAGS", "VOLUME", "RETAIL_PRICE", "COST_PRICE", "IN_STOCK", "AVAILABILITY",
        "EXPIRY", "TOTAL_COST", "SALES_VALUE"]

SERVICE = re.compile(r"Consultation Fee|Blood Pressure\s+Reading|Blood Sugar Reading|Wound Dressing|Wound Stitching|"
                     r"Implant Removal|Home Based Care", re.I)
LAB = re.compile(r"^Test:|Test Strips|Lancets", re.I)
SUPPLY = re.compile(r"Syringe|Needle|Gauze|Swab|Giving Set|Cannula|Blade|Glove|Catheter|Bandage|Plaster|Cotton|Diaper|"
                    r"Spirit|Condom|Suture|Bag - Urine|Mask|Sanitary|Glucometer|Blood Pressure Machine|Envelope|"
                    r"Wound Dressing|Lubricating|Insecticide|Soap|Drinking Water|Mosquito|Chlorine", re.I)


def gid(kind: str, key) -> str:
    return str(uuid.uuid5(NS, f"neorao/{kind}/{key}"))


def clean(v) -> str | None:
    s = re.sub(r"\s+", " ", str(v)).strip() if v is not None else ""
    return s if s and s != "-" else None


def num(v) -> float | None:
    s = clean(v)
    return float(s.replace(",", "")) if s and re.fullmatch(r"-?[\d,]*\.?\d+", s) else None


squash = lambda s: re.sub(r"[^A-Z0-9.%]+", "", (s or "").upper())


# ── RAW: the file as text ──────────────────────────────────────────────────
def read_csv(path: Path) -> tuple[list[str], list[dict]]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    width = max(len(r) for r in rows)
    cols = [col_name(rows[0][j] if j < len(rows[0]) else None, j) for j in range(width)]
    recs = []
    for i, r in enumerate(rows[1:], start=2):
        vals = [(r[j] if r[j].strip() else None) if j < len(r) else None for j in range(width)]
        if any(vals):
            recs.append({**dict(zip(cols, vals)), "_SOURCE_ROW": i})
    return cols, recs


# ── CLEAN ──────────────────────────────────────────────────────────────────
def strength_of(product: str) -> str | None:
    m = re.search(r"((?:\s*/?\s*\d[\d.]*\s*(?:MG|MCG|G|GM|KG|ML|L|IU|%W/V|%W/W|%V/V|%W|%V|%)(?:/\s*[\d.]*\s*ML)?)+)\s*$",
                  product, re.I)
    return clean(m.group(1).lstrip(" /")) if m else None


def unit_for(category: str, name: str, api: str, tags: str, volume: str | None) -> str:
    text = f"{name} {api} {tags}"
    if category != "Pharmaceuticals":
        return "Bottle" if volume and re.search(r"\d\s*(ML|L|LTR)S?$", volume, re.I) else "Piece"
    if re.search(r"\bamp(oule|ule)?s?\b", text, re.I):
        return "Ampoule"
    if re.search(r"inhaler|evohaler|rotacap", text, re.I):
        return "Piece"
    if re.search(r"\binj|vial|vaccine|injection water|insulin|benzathine", text, re.I):
        return "Vial"
    if re.search(r"sachet|powder|\bORS\b|oral rehydration", text, re.I):
        return "Sachet"
    if volume and re.search(r"\d\s*(ML|L|LTR)S?$", volume, re.I) or re.search(r"\bI\.?V\b|syrup|suspension", text, re.I):
        return "Bottle"
    if volume and re.search(r"\d\s*(GM|G|KG)$", volume, re.I) or re.search(r"cream|gel|ointment|oint\b", text, re.I):
        return "Tube"
    if re.search(r"\bcaps?\b|capsule", text, re.I):
        return "Capsule"
    if re.search(r"suppositor|pessar|lozenge|condom|implant|tampon", text, re.I):
        return "Piece"
    return "Tablet"


class Cleaner:
    def __init__(self, raw: list[dict]):
        self.raw = raw
        self.issues: list[dict] = []

    def issue(self, row, column, raw_value, problem, action):
        self.issues.append({"source_row": row, "column": column, "raw_value": None if raw_value is None else str(raw_value),
                            "issue": problem, "action": action})

    def build(self) -> dict[str, list[dict]]:
        lines, services = [], []
        for r in self.raw:
            row, extra = r["_SOURCE_ROW"], [k for k in r if k.startswith("COL_") and r[k] is not None]
            vals = [r.get(c) for c in COLS] + [r[k] for k in extra]
            if extra:                                   # 'clozolecanison, theoclotri 1%W/W' split by its comma
                joined = f"{vals[1]}, {vals[2]}"
                self.issue(row, "PRODUCT", joined, "unquoted comma in the product name shifted the row one column",
                           "joined the two halves and shifted the rest back")
                vals = [vals[0], joined] + vals[3:]
            v = dict(zip(COLS, vals))
            product = (clean(v["PRODUCT"]) or "").rstrip(" /")
            api, tags, volume = clean(v["API"]) or "", clean(v["TAGS"]) or "", clean(v["VOLUME"])
            retail, cost, qty = num(v["RETAIL_PRICE"]), num(v["COST_PRICE"]), num(v["IN_STOCK"])
            if clean(v["EXPIRY"]):
                self.issue(row, "EXPIRY", v["EXPIRY"], "expiry given but not loaded", "ignored")
            if SERVICE.search(api):
                services.append({"source_row": row, "product": product, "api": api, "tags": tags, "retail_price": retail,
                                 "cost_price": cost, "in_stock": qty})
                self.issue(row, "API", api, f"a service, not a stock item (In_Stock {v['IN_STOCK']} is a placeholder)",
                           "kept in SERVICES, not loaded as a product")
                continue
            name = product
            if not product or re.match(r"generic\b", product, re.I):
                name = clean(f"{api} {re.sub(r'(?i)^generic', '', product)}") or api
                self.issue(row, "PRODUCT", product, "product named only 'Generic'", f"named by its API: '{name}'")
            if volume and squash(volume) not in squash(name):
                name = f"{name} {volume}"
            if retail is None:
                self.issue(row, "RETAIL_PRICE", v["RETAIL_PRICE"], "no retail price", "selling price left empty")
            if cost is not None and retail is not None and cost > retail:
                self.issue(row, "COST_PRICE", cost, f"cost above retail {retail:g}", "kept as given")
            if qty is None or qty < 0:
                self.issue(row, "IN_STOCK", v["IN_STOCK"], "stock not a non-negative number", "opening stock 0")
                qty = 0
            category = "Laboratory" if LAB.search(api) else "Medical Supplies" if SUPPLY.search(api) else "Pharmaceuticals"
            lines.append({"row": row, "name": name, "product": product, "api": api, "tags": tags, "volume": volume,
                          "retail": retail, "cost": cost, "qty": qty, "category": category,
                          "unit": unit_for(category, name, api, tags, volume)})

        names = Counter(ln["name"].upper() for ln in lines)
        products = []
        for n, ln in enumerate(lines, start=1):
            label = ln["name"]
            if names[label.upper()] > 1:
                label = f"{label} ({ln['tags'] or 'row ' + str(ln['row'])})"
                self.issue(ln["row"], "PRODUCT", ln["product"], f"name shared by {names[ln['name'].upper()]} rows",
                           f"named '{label}'")
            key = label.upper()          # the name is unique per row; a re-export keeps the same uuids
            products.append({
                "product_uuid": gid("product", key), "stock_uuid": gid("stock", key), "batch_uuid": gid("batch", key),
                "code": f"NEO-{n:03d}", "generic_name": label, "source_product": ln["product"], "api": ln["api"],
                "tags": ln["tags"], "volume": ln["volume"], "strength": strength_of(ln["product"]),
                "category": ln["category"], "unit": ln["unit"], "retail_price": ln["retail"], "cost_price": ln["cost"],
                "quantity": ln["qty"], "source_row": ln["row"]})
        dup = [k for k, c in Counter(p["generic_name"].upper() for p in products).items() if c > 1]
        if dup:
            raise ValueError(f"product names still not unique: {dup}")
        return {"PRODUCTS": products, "SERVICES": services, "DATA_ISSUES": self.issues}


CLEAN_TYPES = {"PRODUCTS": {"retail_price": "FLOAT", "cost_price": "FLOAT", "quantity": "FLOAT", "source_row": "INTEGER"},
               "SERVICES": {"retail_price": "FLOAT", "cost_price": "FLOAT", "in_stock": "FLOAT", "source_row": "INTEGER"},
               "DATA_ISSUES": {"source_row": "INTEGER"}}


# ── Snowflake ──────────────────────────────────────────────────────────────
def summary(tables: dict[str, list[dict]]) -> dict:
    p = tables["PRODUCTS"]
    return {"products": len(p), "services_not_loaded": len(tables["SERVICES"]),
            "in_stock": sum(1 for x in p if x["quantity"] > 0), "units_total": sum(x["quantity"] for x in p),
            "stock_cost_value": round(sum(x["quantity"] * (x["cost_price"] or 0) for x in p), 2),
            "categories": dict(Counter(x["category"] for x in p)), "units": dict(Counter(x["unit"] for x in p)),
            "data_issues": dict(Counter(x["issue"].split(" (")[0].split(":")[0] for x in tables["DATA_ISSUES"]))}


def run(path: str | Path = DEFAULT_CSV, *, dry_run: bool = False) -> dict:
    path = Path(path)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    cols, rows = read_csv(path)
    log.info("%s (sha256 %s…): %d rows, columns %s", path.name, sha[:12], len(rows), cols)
    if dry_run:
        tables = Cleaner(rows).build()
        return {"file": path.name, "sha256": sha, "dry_run": True, **summary(tables), "tables": tables}
    from datetime import datetime, timezone
    import snowflake_to_v3_migration as s2v3
    sf = s2v3._snowflake_connect()
    try:
        cur = sf.cursor()
        for schema in (RAW_SCHEMA, CLEAN_SCHEMA):
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{schema}")
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        columns = [(c, "VARCHAR") for c in cols] + [("_SOURCE_ROW", "INTEGER"), ("_SOURCE_FILE", "VARCHAR"),
                                                    ("_FILE_SHA256", "VARCHAR"), ("_LOADED_AT", "TIMESTAMP_NTZ")]
        replace_table(cur, f"{SF_DB}.{RAW_SCHEMA}.{RAW_TABLE}", columns,
                      [tuple(r.get(c) for c in cols) + (r["_SOURCE_ROW"], path.name, sha, now) for r in rows])
        log.info("  RAW   %-12s %4d rows", RAW_TABLE, len(rows))
        cur.execute(f'SELECT * FROM {SF_DB}.{RAW_SCHEMA}.{RAW_TABLE} ORDER BY "_SOURCE_ROW"')
        names = [c[0] for c in cur.description]
        tables = Cleaner([dict(zip(names, r)) for r in cur.fetchall()]).build()   # CLEAN from RAW, not the file
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
    ap = argparse.ArgumentParser(description="Neorao inventory CSV -> Snowflake NEORAO_RAW / NEORAO_CLEAN")
    ap.add_argument("csv", nargs="?", default=str(DEFAULT_CSV))
    ap.add_argument("--dry-run", action="store_true", help="parse and clean locally; write nothing")
    ap.add_argument("--dump", help="with --dry-run: write the CLEAN tables to this JSON file")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    if not a.dry_run:
        sys.path.insert(0, "/opt/airflow/dags")
        from pipelines_common import use_pipelines_dir
        use_pipelines_dir([])
    out = run(a.csv, dry_run=a.dry_run)
    if a.dump and "tables" in out:
        Path(a.dump).write_text(json.dumps(out["tables"], indent=1, default=str))
    out.pop("tables", None)
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
