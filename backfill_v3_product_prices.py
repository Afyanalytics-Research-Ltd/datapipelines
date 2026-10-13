"""
backfill_v3_product_prices.py — set selling / insurance prices on V3 products
already migrated without them.

V2 prices products per store (inventory_store_products); V3 prices the
product (its stock has no price columns), so the first loads posted products
with V2's mostly-empty product-level prices. New loads now read the price
from the most recently updated store row (v3_extra_mappings._product_prices_sql);
this applies the same prices to the products already in V3 — matched through
the migration's id map, updated in place by uuid (only the two price columns
are sent). Only products whose V3 price differs are posted.

    python backfill_v3_product_prices.py -f kisumu_v3 --dry-run
    python backfill_v3_product_prices.py -f kisumu_v3 --limit 3
    python backfill_v3_product_prices.py -f kisumu_v3
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor

import snowflake_to_v3_migration as s2v3
import v2_to_v3_api_migration as v2v3
import v3_extra_mappings as xm

log = logging.getLogger("backfill_prices")
PRICES = ("selling_price", "insurance_price")


def _num(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None


def v2_prices(facility: str) -> dict[str, dict]:
    """V2 product id → {selling_price, insurance_price} (store price rule)."""
    clean = s2v3.sf_schema(facility, "CLEAN")
    with s2v3._snowflake_connect() as conn:
        cur = conn.cursor()
        sql = xm._product_prices_sql(clean, lambda t: s2v3.clean_columns(cur, facility, t),
                                     set(s2v3.clean_tables(cur, facility)))
        if not sql:
            raise SystemExit("inventory_store_products / price columns not in CLEAN — nothing to backfill")
        cur.execute(f'SELECT "ID", "SELLING_PRICE", "INSURANCE_PRICE" FROM ({sql}) '
                    f'QUALIFY ROW_NUMBER() OVER (PARTITION BY "ID" ORDER BY "UPDATED_AT" DESC NULLS LAST) = 1')
        return {str(i): {"selling_price": _num(s), "insurance_price": _num(p)} for i, s, p in cur.fetchall()}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-f", "--facility", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="post at most N products (canary)")
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")

    v2v3.set_v3_target_facility(a.facility)
    v2v3.use_state_dir(a.facility)
    org = v2v3.v3_login_org_cfg()
    prices = v2_prices(a.facility)
    idm = json.loads(v2v3.ID_MAP_FILE.read_text()).get("product", {}) if v2v3.ID_MAP_FILE.exists() else {}
    v3 = {r["id"]: r for r in v2v3._fetch_v3_records("product", org, service_name="inventory")}

    todo, unmapped, same = [], 0, 0
    for v2_id, want in prices.items():
        if not any(want.values()):
            continue
        row = v3.get(idm.get(v2_id))
        if row is None or not row.get("uuid"):
            unmapped += 1
            continue
        patch = {k: v for k, v in want.items() if v is not None and _num(row.get(k)) != v}
        if not patch:
            same += 1
            continue
        todo.append((v2_id, row, patch))
    log.info("V2 products priced: %d | already right in V3: %d | not in V3 / unmapped: %d | to update: %d",
             sum(any(v.values()) for v in prices.values()), same, unmapped, len(todo))
    if a.limit:
        todo = todo[:a.limit]
    if a.dry_run:
        for v2_id, row, patch in todo[:10]:
            log.info("  would set V3 product %s (V2 %s) %s — now %s", row["id"], v2_id, patch,
                     {k: row.get(k) for k in PRICES})
        return 0

    failed = []

    def post(item):
        v2_id, row, patch = item
        body = {"action": "insert", "model": "product", "destination_tenant_id": org["organization_id"],
                "match_on": "uuid", "data": {"uuid": row["uuid"], **patch}}
        r = v2v3._gateway_post("inventory", body, timeout=120)
        if not r.ok:
            failed.append((row["id"], r.status_code, r.text[:300]))

    with ThreadPoolExecutor(a.workers) as pool:
        list(pool.map(post, todo))
    for f in failed[:20]:
        log.error("  V3 product %s: %s %s", *f)
    log.info("updated %d, failed %d", len(todo) - len(failed), len(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
