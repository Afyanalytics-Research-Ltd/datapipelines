"""
Aroha Cancer Centre price list: Snowflake HOSPITALS.AROHA_CLEAN.PRODUCTS -> V3 inventory products.

Run load_aroha_to_snowflake.py first. Targets (credentials in .env):
  aroha        migrate platform org 18 / facility 22 (blank org: the store, category and units are created here)
  aroha_prod   production org 145 / facility 143

Each product: generic_name '<drug> <strength>', strength, code/item_no ARC-nnn, cash -> selling_price, SHA ->
insurance_price, buying -> cost_price / trade_price, plus a zero stock row in the pharmacy store (the POS and search
lists are built from stock rows). No quantities in the source, so no batches.

A target product with the same name (case / spacing ignored) is reused: its empty prices are filled, the ones it
already has are left alone. Everything carries the CLEAN uuid5s, so a re-run updates instead of duplicating.

By hand, inside the Airflow worker:
    docker exec -w /opt/airflow/pipelines datapipelines-airflow-worker-1 python aroha_to_v3.py --target aroha --dry-run
    ... python aroha_to_v3.py --target aroha [--limit 3]
    ... python aroha_to_v3.py --target aroha --verify          # read everything back and compare with CLEAN
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/opt/airflow/dags")
from load_aroha_to_snowflake import CLEAN_SCHEMA, SF_DB, gid  # noqa: E402

log = logging.getLogger("aroha_to_v3")

SOURCE = "Aroha oncology cash/SHA price list (2026-11-03)"
CATEGORY = "Pharmaceuticals"
UNITS = ("Tablet", "Vial", "Ampoule", "Tube", "Pack", "Piece")
STORE = "Pharmacy"
PRICE_COLS = {"selling_price": "cash_price", "insurance_price": "sha_price", "cost_price": "buying_price",
              "trade_price": "buying_price"}
norm = lambda s: re.sub(r"[^a-z0-9]+", " ", str(s or "").lower()).strip()


def blank(v) -> bool:
    return v is None or float(v or 0) == 0


class Run:
    def __init__(self, target: str, dry_run: bool, limit: int, workers: int, store_id: int | None):
        from pipelines_common import use_pipelines_dir
        use_pipelines_dir([target])
        from v3_to_v3_client import V3Client
        import snowflake_to_v3_migration as s2v3
        self.dry_run, self.limit, self.workers, self.store_id = dry_run, limit, workers, store_id
        self.c = V3Client(target)
        self.c.send_facility_header = True          # stores/stock otherwise land under facility NULL
        self.c.login()
        log.info("%r user %s", self.c, self.c.user_id)
        sf = s2v3._snowflake_connect()
        try:
            cur = sf.cursor()
            cur.execute(f"SELECT * FROM {SF_DB}.{CLEAN_SCHEMA}.PRODUCTS ORDER BY CODE")
            cols = [c[0].lower() for c in cur.description]
            self.src = [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            sf.close()
        log.info("CLEAN products: %d", len(self.src))

    def ins(self, alias: str, data: dict) -> int:
        r = self.c.insert("inventory", alias, data, match_on="uuid")
        body = r.json() if r.content else {}
        if not r.ok or body.get("success") is False:
            raise RuntimeError(f"{alias} {data.get('uuid')}: {r.status_code} {r.text[:300]}")
        return body["data"]["id"]

    def existing(self, alias: str) -> list[dict]:
        return [r for r in self.c.read_all("inventory", alias, workers=4) if not r.get("deleted_at")]

    # ── reference rows ──
    def refs(self) -> dict:
        if self.dry_run:
            return {"category": 0, "units": {u: 0 for u in UNITS}, "store": self.store_id or 0}
        cats = {norm(c["name"]): c["id"] for c in self.existing("product_category")}
        units = {norm(u["name"]): u["id"] for u in self.existing("unit")}
        cat = cats.get(norm(CATEGORY)) or self.ins("product_category", {
            "uuid": gid("category", CATEGORY), "name": CATEGORY, "code": "PHARM", "is_drug": 1, "is_consumable": 0})
        unit_ids = {u: units.get(norm(u)) or self.ins("unit", {"uuid": gid("unit", u), "name": u}) for u in UNITS}
        store = self.store_id or self.ins("store", {
            "uuid": gid("store", STORE), "name": STORE, "code": "PHARMACY", "type": "pharmacy", "is_main_store": 1,
            "is_active": 1, "can_order_from_suppliers": 1, "can_update_product_prices": 1, "facility_id": self.c.facility_id,
            "description": "Aroha Cancer Centre pharmacy"})
        out = {"category": cat, "units": unit_ids, "store": store}
        log.info("refs %s", out)
        return out

    def plan(self, refs: dict) -> list[dict]:
        by_name: dict[str, list[dict]] = {}
        if not self.dry_run or self.c.organization_id:
            try:
                for p in self.existing("product"):
                    by_name.setdefault(norm(p.get("generic_name") or p.get("name")), []).append(p)
            except Exception as e:                     # a blank or unreadable org: everything is new
                log.warning("could not read existing products: %s", e)
        out = []
        for s in self.src:
            prices = {k: s[v] for k, v in PRICE_COLS.items() if s[v] is not None}
            hits = [p for p in by_name.get(norm(s["generic_name"]), []) if p.get("uuid") != s["product_uuid"]]
            if len(hits) == 1:
                h = hits[0]
                fill = {k: v for k, v in prices.items() if blank(h.get(k))}
                out.append({"code": s["code"], "reuse": h["id"], "product": {"uuid": h["uuid"], **fill} if fill else None,
                            "stock_uuid": s["stock_uuid"]})
                continue
            out.append({"code": s["code"], "reuse": None, "stock_uuid": s["stock_uuid"], "product": {
                "uuid": s["product_uuid"], "generic_name": s["generic_name"], "strength": s["strength"],
                "code": s["code"], "item_no": s["code"], "category_id": refs["category"], "unit_id": refs["units"][s["unit"]],
                **prices, "is_active": 1, "facility_id": self.c.facility_id,
                "description": f"{SOURCE}; source row(s) {s['source_rows']}"
                               + ("" if s["cash_price"] is not None else "; no cash price on the list")}})
        return out

    def load(self) -> dict:
        refs = self.refs()
        plan = self.plan(refs)[: self.limit or None]
        res = Counter(new=sum(1 for p in plan if not p["reuse"]), reused=sum(1 for p in plan if p["reuse"]),
                      reused_price_fills=sum(1 for p in plan if p["reuse"] and p["product"]))
        if self.dry_run:
            for p in plan[:3]:
                log.info("  dry-run %s", json.dumps(p, default=str))
            return dict(res)
        lock = threading.Lock()

        def one(p):
            try:
                pid = p["reuse"]
                if p["product"]:
                    pid = self.ins("product", p["product"])
                self.ins("stock", {"uuid": p["stock_uuid"], "product_id": pid, "store_id": refs["store"], "quantity": 0,
                                   "available_quantity": 0, "facility_id": self.c.facility_id})
                with lock:
                    res["ok"] += 1
            except Exception as e:
                with lock:
                    res["failed"] += 1
                    log.error("  %s: %s", p["code"], e)
        with ThreadPoolExecutor(self.workers) as ex:
            list(ex.map(one, plan))
        return dict(res)

    def verify(self) -> dict:
        prods = {p.get("uuid"): p for p in self.existing("product")}
        stock = {s.get("uuid"): s for s in self.existing("stock")}
        problems = []
        for s in self.src:
            p = prods.get(s["product_uuid"])
            if p is None:
                problems.append((s["code"], "product missing (or reused under another uuid)"))
                continue
            for k, v in PRICE_COLS.items():
                if s[v] is not None and abs(float(p.get(k) or 0) - float(s[v])) > 0.005:
                    problems.append((s["code"], f"{k} {p.get(k)} != {s[v]}"))
            if p.get("generic_name") != s["generic_name"]:
                problems.append((s["code"], f"name {p.get('generic_name')!r} != {s['generic_name']!r}"))
            if p.get("facility_id") not in (None, self.c.facility_id):
                problems.append((s["code"], f"facility {p.get('facility_id')}"))
            st = stock.get(s["stock_uuid"])
            if st is None or st.get("product_id") != p["id"]:
                problems.append((s["code"], "stock row missing"))
        for code, why in problems[:30]:
            log.error("  %s: %s", code, why)
        return {"source": len(self.src), "products_found": sum(1 for s in self.src if s["product_uuid"] in prods),
                "stock_rows_found": sum(1 for s in self.src if s["stock_uuid"] in stock), "problems": len(problems)}


def main() -> int:
    ap = argparse.ArgumentParser(description="AROHA_CLEAN.PRODUCTS -> V3 inventory")
    ap.add_argument("--target", choices=("aroha", "aroha_prod"), required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verify", action="store_true", help="read back and compare with CLEAN; write nothing")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--store-id", type=int, help="existing store to stock into (default: create/reuse 'Pharmacy')")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    r = Run(a.target, a.dry_run, a.limit, a.workers, a.store_id)
    print(json.dumps(r.verify() if a.verify else r.load(), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
