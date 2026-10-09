"""Read-only: silverwood patients — extraction API vs Snowflake vs V3. Run inside the Airflow container."""
import json, logging, sys, time
sys.path.insert(0, "/opt/airflow/dags")
logging.basicConfig(level=logging.WARNING)
from pipelines_common import use_pipelines_dir
use_pipelines_dir(["silverwood"])
import v2_to_v3_api_migration as v2v3
import snowflake_to_v3_migration as m
from from_json_mappings_to_snowflake import ExtractionClient, Mappings, DEFAULT_MAPPINGS


def retry(fn, what):
    for i in range(4):
        try:
            return fn()
        except Exception as e:
            print(f"  {what} attempt {i + 1}: {str(e)[:140]}")
            time.sleep(15)
    return None


# 1. extraction API
spec = Mappings.load(DEFAULT_MAPPINGS)
mapping = [(f["column"], f["field"]) for f in next(x for x in spec.models if x.source_table == "person").fields]
got = retry(lambda: ExtractionClient(spec.connection_id).fetch_page("person", 1), "extraction login/read")
api = {}
if got:
    rows = got[0]
    keys = set().union(*(r.keys() for r in rows))
    filled = sorted({k for r in rows for k, v in r.items() if v not in (None, "")})
    api = {str(r.get("id")): r for r in rows}
    print(f"EXTRACTION person: {len(rows)} rows · non-empty keys: {filled}")
    print("  mapping check (source column → V3 field: column returned? / value present?):")
    for col, fld in mapping:
        print(f"    {col:20s} → {fld:18s} {col in keys!s:5s} {any(r.get(col) not in (None, '') for r in rows)!s:5s}"
              f"  (as V3 name: {any(r.get(fld) not in (None, '') for r in rows)})")

# 2. Snowflake
with m._snowflake_connect() as conn:
    cur = conn.cursor()
    cur.execute("SELECT * FROM SILVERWOOD_CLEAN.PATIENTS")
    cols = [d[0].lower() for d in cur.description]
    sf = {str(r[cols.index("id")]): dict(zip(cols, r)) for r in cur.fetchall()}
print(f"\nSNOWFLAKE patients: {len(sf)} rows")

# 3. V3
v2v3.use_state_dir("silverwood")
v2v3.set_v3_target_facility("silverwood")
org = retry(v2v3.v3_login_org_cfg, "V3 login")
v3 = []
if org:
    print(f"\nV3 login ok: org {org.get('organization_id')} facility {org.get('facility_id')}")
    v3 = v2v3._fetch_v3_records("patient", org, service_name="reception")
    print(f"V3 patients in org: {len(v3)}")
    if v3:
        cols = sorted(v3[0])
        print("  filled:", {c: sum(1 for x in v3 if x.get(c) not in (None, "")) for c in cols
                            if any(x.get(c) not in (None, "") for x in v3)})

# 4. the same patients side by side
uuid_map = {}
p = v2v3.ID_MAP_FILE.with_name(".migration_v3_uuid.json")
idmap = json.loads(v2v3.ID_MAP_FILE.read_text()).get("patient", {}) if v2v3.ID_MAP_FILE.exists() else {}
print(f"\nlocal silverwood id map for patient: {len(idmap)} entries ({v2v3.ID_MAP_FILE})")
by_name = {}
for x in v3:
    by_name.setdefault((str(x.get("first_name") or "").lower(), str(x.get("last_name") or "").lower()), []).append(x)
FIELDS = ["patient_no", "first_name", "middle_name", "last_name", "dob", "gender", "mobile", "id_no",
          "marital_status", "nationality", "religion", "occupation", "registration_date", "facility_id", "created_by"]
for sid in sorted(sf, key=lambda s: int(s) if s.isdigit() else 0)[:5]:
    s = sf[sid]
    match = by_name.get((str(s.get("first_name") or "").lower(), str(s.get("last_name") or "").lower()), [])
    print(f"\n source id {sid}: {s.get('first_name')} {s.get('last_name')} — V3 matches by name: {len(match)}")
    a = api.get(sid, {})
    for f in FIELDS:
        print(f"   {f:18s} api={str(a.get(f))[:22]:24s} snowflake={str(s.get(f))[:22]:24s} "
              f"v3={str(match[0].get(f))[:22] if match else '—'}")
