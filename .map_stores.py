"""Idempotent: map kisumu_v3's 26 V2 stores to the stores already in V3 org 4
(by unique name+code — V2 stores have no uuid) and record them as migrated."""
import collections
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
import snowflake_to_v3_migration as s
import v2_to_v3_api_migration as v

v.set_v3_target_facility("kisumu_v3")
org = v.v3_login_org_cfg()
v._fetch_available_models()
v3 = v._fetch_v3_records("store", org)
with s._snowflake_connect() as c:
    cur = c.cursor()
    cur.execute("SELECT id, name, code FROM KISUMU_V3_CLEAN.INVENTORY_STORES")
    src = cur.fetchall()
key = lambda name, code: (str(name or "").strip().lower(), str(code or "").strip().lower())
v3_by = collections.defaultdict(list)
for r in v3:
    v3_by[key(r.get("name"), r.get("code"))].append(r["id"])
src_ct = collections.Counter(key(n, cd) for _, n, cd in src)
pairs = [(int(i), v3_by[key(n, cd)][0]) for i, n, cd in src
         if len(v3_by.get(key(n, cd), [])) == 1 and src_ct[key(n, cd)] == 1]
v._load_id_map()
v._store_id_mappings("store", pairs)
v._load_record_progress()
for v2_id, _ in pairs:
    v._mark_record_inserted("kisumu_v3|sf:inventory_stores", v2_id)
v._flush_record_progress()
logging.info("STORES MAPPED %d/%d (store 382 -> %s)", len(pairs), len(src), dict(pairs).get(382))
