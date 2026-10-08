"""Read-only: org-4 counts + a sample row for reception vs evaluation visit tables."""
import json, logging
import v2_to_v3_api_migration as v2v3

logging.getLogger().setLevel(logging.ERROR)
v2v3.set_v3_target_facility("kisumu_v3")
v2v3.v3_login_org_cfg()
models = v2v3._fetch_available_models()
print("reception aliases:", sorted(a for a, s in v2v3._alias_to_service.items() if s == "reception"))
for svc, alias in [("reception", "visit_destination"), ("reception", "visit_consultant"), ("reception", "visit_precharge"), ("reception", "patient"), ("evaluation", "visit_destination"), ("evaluation", "destinations")]:
                   ("reception", "checkin"), ("evaluation", "visits"), ("evaluation", "visit_destinations")]:
    try:
        r = v2v3._gateway_post(svc, {"action": "read", "model": alias, "source_tenant_id": 4, "per_page": 1}, timeout=60)
        b = r.json()
        row = (b.get("data") or [{}])[0]
        print(f"{svc:10s} {alias:20s} HTTP {r.status_code} org4={b.get('meta', {}).get('total')} "
              f"all_orgs={b.get('meta', {}).get('table_total')} cols={sorted(row)[:45]}")
    except Exception as e:
        print(svc, alias, "ERR", e)
