"""
Neorao Community Medical Centre inventory: Snowflake HOSPITALS.NEORAO_CLEAN.PRODUCTS -> V3 inventory.

Run load_neorao_to_snowflake.py first. Targets (credentials in .env):
  neorao        migrate platform org 19 / facility 23 (blank org: store, categories and units are created here)
  neorao_prod   production org 338 / facility 346 (template catalogue + 'Pharmacy' store 876 already there)

Each product: generic_name, strength, code/item_no NEO-nnn, Retail -> selling + insurance price, Cost -> cost + trade
price. In-stock items get an opening batch OPENING-NEO-nnn (no expiry: the export has none) and a batched stock row in
the pharmacy store for the 01-11-2026 quantity; out-of-stock items get a zero stock row so they show in the store.

A target product with the same name (CLEAN name, the centre's Product or its Tags; case / punctuation ignored) is
reused when exactly one matches and no other row claims it. Its prices are set from the sheet (prod's catalogue
prices are template prices nobody at the centre set), unless staff edited the product after STAFF_EDIT_AFTER, in
which case only empty prices are filled. Products staff have already stocked in the store get no opening stock.
Everything carries the CLEAN uuid5s, so a re-run updates instead of duplicating.

By hand, inside the Airflow worker:
    docker exec -w /opt/airflow/pipelines datapipelines-airflow-worker-1 python neorao_to_v3.py --target neorao --dry-run
    ... python neorao_to_v3.py --target neorao [--limit 3]
    ... python neorao_to_v3.py --target neorao --verify          # read everything back and compare with CLEAN
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
from load_neorao_to_snowflake import CLEAN_SCHEMA, SF_DB, gid  # noqa: E402

log = logging.getLogger("neorao_to_v3")

SOURCE = "Neorao inventory export as at 01-11-2026"
STORE = "Pharmacy"
STORE_IDS = {"neorao_prod": 876}                     # existing 'Pharmacy' store in prod
STAFF_EDIT_AFTER = "2026-11-01"                      # prod catalogue untouched since its copy (latest edit 2026-10-28)
CAT_CODES = {"Pharmaceuticals": "PHARM", "Medical Supplies": "MEDSUP", "Laboratory": "LAB"}
PRICE_COLS = {"selling_price": "retail_price", "insurance_price": "retail_price", "cost_price": "cost_price",
              "trade_price": "cost_price"}
norm = lambda s: re.sub(r"[^a-z0-9]+", " ", str(s or "").lower()).strip()
num = lambda v: float(v or 0)


class Run:
    def __init__(self, target: str, dry_run: bool, limit: int, workers: int, store_id: int | None):
        from pipelines_common import use_pipelines_dir
        use_pipelines_dir([target])
        from v3_to_v3_client import V3Client
        import snowflake_to_v3_migration as s2v3
        self.dry_run, self.limit, self.workers = dry_run, limit, workers
        self.known: dict[str, dict[str, int]] = {}          # filled by plan(): uuid -> id for product / batch / stock
        self.store_id = store_id or STORE_IDS.get(target)
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
        """Upsert by uuid. For product / batch / stock the uuid -> id map read in plan() decides: a known uuid is updated
        by id, an unknown one plain-inserted. (match_on=uuid is unindexed server-side: >120 s per call on prod's 27k
        products, so nginx 502s.) Small reference tables still use match_on."""
        known = self.known.get(alias)
        if known is not None:
            u = data.get("uuid")
            if u in known:
                rid = known[u]
                r = self.c.post("inventory", {"action": "update", "model": alias, "destination_tenant_id": self.c.organization_id,
                                              "ids": [rid], "data": {k: v for k, v in data.items() if k != "uuid"}}, timeout=120)
                if not r.ok or (r.json() if r.content else {}).get("success") is False:
                    raise RuntimeError(f"{alias} update {rid}: {r.status_code} {r.text[:300]}")
                return rid
            r = self.c.insert("inventory", alias, data, match_on=None)
        else:
            r = self.c.insert("inventory", alias, data, match_on="uuid")
        body = r.json() if r.content else {}
        if not r.ok or body.get("success") is False:
            raise RuntimeError(f"{alias} {data.get('uuid')}: {r.status_code} {r.text[:300]}")
        if known is not None:
            known[data["uuid"]] = body["data"]["id"]
        return body["data"]["id"]

    def existing(self, alias: str) -> list[dict]:
        return [r for r in self.c.read_all("inventory", alias, workers=8) if not r.get("deleted_at")]

    # ── reference rows ──
    def refs(self) -> dict:
        cats_needed = sorted({s["category"] for s in self.src})
        units_needed = sorted({s["unit"] for s in self.src})
        if self.dry_run:
            return {"categories": {c: 0 for c in cats_needed}, "units": {u: 0 for u in units_needed},
                    "store": self.store_id or 0}
        cats = {norm(c["name"]): c["id"] for c in self.existing("product_category")}
        units = {norm(u["name"]): u["id"] for u in self.existing("unit")}
        cat_ids = {c: cats.get(norm(c)) or self.ins("product_category", {
            "uuid": gid("category", c), "name": c, "code": CAT_CODES[c], "is_drug": int(c == "Pharmaceuticals"),
            "is_consumable": int(c != "Pharmaceuticals")}) for c in cats_needed}
        unit_ids = {u: units.get(norm(u)) or self.ins("unit", {"uuid": gid("unit", u), "name": u}) for u in units_needed}
        store = self.store_id or self.ins("store", {
            "uuid": gid("store", STORE), "name": STORE, "code": "PHARMACY", "type": "pharmacy", "is_main_store": 1,
            "is_active": 1, "can_order_from_suppliers": 1, "can_update_product_prices": 1, "facility_id": self.c.facility_id,
            "description": "Neorao Community Medical Centre pharmacy"})
        out = {"categories": cat_ids, "units": unit_ids, "store": store}
        log.info("refs %s", out)
        return out

    def plan(self, refs: dict) -> list[dict]:
        prods, stock = [], []
        if not self.dry_run or self.c.organization_id:
            prods, stock = self.existing("product"), self.existing("stock")
            self.known = {"product": {p["uuid"]: p["id"] for p in prods if p.get("uuid")},
                          "stock": {s["uuid"]: s["id"] for s in stock if s.get("uuid")},
                          "batch": {b["uuid"]: b["id"] for b in self.existing("batch") if b.get("uuid")}}
        store = refs["store"]
        ours = {s["product_uuid"] for s in self.src}
        our_stock = {s["stock_uuid"] for s in self.src}
        staff_stocked = {s["product_id"] for s in stock if s.get("store_id") == store and num(s.get("quantity")) > 0
                         and s.get("uuid") not in our_stock}
        by_name: dict[str, list[dict]] = {}
        for p in prods:
            if p.get("uuid") not in ours:
                by_name.setdefault(norm(p.get("generic_name")), []).append(p)
        cand = {}
        for s in self.src:
            hits = {h["id"]: h for k in (s["generic_name"], s["source_product"], s["tags"]) if norm(k)
                    for h in by_name.get(norm(k), [])}
            cand[s["code"]] = list(hits.values())
        claims = Counter(h["id"] for hs in cand.values() if len(hs) == 1 for h in hs)

        out = []
        for s in self.src:
            prices = {k: s[v] for k, v in PRICE_COLS.items() if s[v] is not None}
            qty = num(s["quantity"])
            hs = cand[s["code"]]
            hit = hs[0] if len(hs) == 1 and claims[hs[0]["id"]] == 1 else None
            job = {"code": s["code"], "s": s, "qty": qty, "reuse": None, "product": None, "stocked_by_staff": False}
            if hit:
                staff_edit = str(hit.get("updated_at") or "") >= STAFF_EDIT_AFTER
                set_ = {k: v for k, v in prices.items()
                        if (not staff_edit or not num(hit.get(k))) and abs(num(hit.get(k)) - float(v)) > 0.005}
                job.update(reuse=hit["id"], reuse_name=hit.get("generic_name"),
                           product={"uuid": hit["uuid"], **set_} if set_ else None,
                           stocked_by_staff=hit["id"] in staff_stocked)
                if job["stocked_by_staff"]:
                    job["qty"] = 0
            else:
                desc = f"{SOURCE}; row {s['source_row']}; API: {s['api'] or '-'}" + (f"; tags: {s['tags']}" if s["tags"] else "")
                job["product"] = {
                    "uuid": s["product_uuid"], "generic_name": s["generic_name"], "strength": s["strength"],
                    "code": s["code"], "item_no": s["code"], "category_id": refs["categories"][s["category"]],
                    "unit_id": refs["units"][s["unit"]], **prices, "is_active": 1, "facility_id": self.c.facility_id,
                    "description": desc}
            out.append(job)
        return out

    def load(self) -> dict:
        refs = self.refs()
        plan = self.plan(refs)[: self.limit or None]
        res = Counter(new=sum(1 for p in plan if not p["reuse"]), reused=sum(1 for p in plan if p["reuse"]),
                      reused_price_updates=sum(1 for p in plan if p["reuse"] and p["product"]),
                      reused_staff_stocked=sum(1 for p in plan if p["stocked_by_staff"]),
                      batches=sum(1 for p in plan if p["qty"] > 0), units=sum(p["qty"] for p in plan))
        for p in plan:
            if p["reuse"]:
                log.info("  reuse %s %-45s -> %s %r%s", p["code"], p["s"]["generic_name"][:45], p["reuse"],
                         p["reuse_name"], " (staff-stocked)" if p["stocked_by_staff"] else "")
        if self.dry_run:
            for p in plan[:3]:
                log.info("  dry-run %s", json.dumps({k: v for k, v in p.items() if k != "s"}, default=str))
            return dict(res)
        lock = threading.Lock()
        store, fac = refs["store"], self.c.facility_id

        def one(p):
            s = p["s"]
            try:
                pid = p["reuse"]
                if p["product"]:
                    pid = self.ins("product", p["product"])
                if p["qty"] > 0:
                    cost, sell = num(s["cost_price"]), num(s["retail_price"])
                    bid = self.ins("batch", {
                        "uuid": s["batch_uuid"], "batch_no": f"OPENING-{s['code']}", "product_id": pid, "store_id": store,
                        "cost_price": cost, "selling_price": sell, "quantity_received": p["qty"],
                        "quantity_available": p["qty"], "status": "active", "notes": f"Opening stock: {SOURCE}",
                        "facility_id": fac})
                    self.ins("stock", {"uuid": s["stock_uuid"], "product_id": pid, "store_id": store, "batch_id": bid,
                                       "quantity": p["qty"], "available_quantity": p["qty"], "average_cost": cost,
                                       "total_value": round(p["qty"] * cost, 2), "facility_id": fac})
                elif not p["reuse"]:                  # a zero stock row makes a new product show in the store
                    self.ins("stock", {"uuid": s["stock_uuid"], "product_id": pid, "store_id": store, "quantity": 0,
                                       "available_quantity": 0, "facility_id": fac})
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
        refs = {"store": self.store_id}
        if not self.store_id:
            refs["store"] = next((s["id"] for s in self.existing("store") if s.get("uuid") == gid("store", STORE)), None)
        self.dry_run = False
        plan = {p["code"]: p for p in self.plan({**refs, "categories": _Any(), "units": _Any()})}
        prods = {p["id"]: p for p in self.existing("product")}
        by_uuid = {p.get("uuid"): p for p in prods.values()}
        batches = {b.get("uuid"): b for b in self.existing("batch")}
        stock = {s.get("uuid"): s for s in self.existing("stock")}
        problems, found = [], Counter()
        for s in self.src:
            p = by_uuid.get(s["product_uuid"])
            j = plan[s["code"]]
            if p is None and j["reuse"]:
                p = prods.get(j["reuse"])
            if p is None:
                problems.append((s["code"], "product missing"))
                continue
            found["products"] += 1
            for k, v in PRICE_COLS.items():           # a staff-edited reused product shows here too: look before fixing
                if s[v] is not None and abs(num(p.get(k)) - float(s[v])) > 0.005:
                    problems.append((s["code"], f"{k} {p.get(k)} != {s[v]}"))
            if p.get("uuid") == s["product_uuid"] and p.get("generic_name") != s["generic_name"]:
                problems.append((s["code"], f"name {p.get('generic_name')!r} != {s['generic_name']!r}"))
            qty = num(s["quantity"])
            st = stock.get(s["stock_uuid"])
            if qty > 0 and not j["stocked_by_staff"]:
                b = batches.get(s["batch_uuid"])
                if b is None or b.get("product_id") != p["id"] or num(b.get("quantity_available")) != qty:
                    problems.append((s["code"], f"batch {b and b.get('quantity_available')} != {qty}"))
                else:
                    found["batches"] += 1
                if st is None or st.get("product_id") != p["id"] or num(st.get("quantity")) != qty \
                        or st.get("batch_id") != (b or {}).get("id") or st.get("store_id") != refs["store"]:
                    problems.append((s["code"], f"stock {st and st.get('quantity')} != {qty}"))
                else:
                    found["stock_rows"] += 1
                    found["units"] += qty
            elif p.get("uuid") == s["product_uuid"]:
                if st is None or num(st.get("quantity")) != 0:
                    problems.append((s["code"], "zero stock row missing"))
                else:
                    found["stock_rows"] += 1
        for code, why in problems[:40]:
            log.error("  %s: %s", code, why)
        return {"source": len(self.src), **found, "problems": len(problems)}


class _Any(dict):
    def __missing__(self, key):
        return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="NEORAO_CLEAN.PRODUCTS -> V3 inventory")
    ap.add_argument("--target", choices=("neorao", "neorao_prod"), required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verify", action="store_true", help="read back and compare with CLEAN; write nothing")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--store-id", type=int, help="existing store to stock into (default: prod 876 / create 'Pharmacy')")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    r = Run(a.target, a.dry_run, a.limit, a.workers, a.store_id)
    print(json.dumps(r.verify() if a.verify else r.load(), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
