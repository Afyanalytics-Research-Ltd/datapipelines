import logging, json; logging.disable(logging.WARNING)
import facility_to_snowflake_fast_resume as f, v2_to_v3_api_migration as v
FAC = "kisumu_v3"; cfg = f.FACILITIES[FAC]; url = f"{cfg['base_url'].rstrip('/')}/api/finance/access/data/point"
s = f._facility_session(FAC); h = {"Authorization": f"Bearer {f._facility_token(FAC)}"}
for mod, cls in (("Users", "User"), ("Users", "Users"), ("Core", "User")):
    ns = chr(92).join(["Ignite", mod, "Entities", cls])
    r = s.post(url, headers=h, json={"namespace": ns, "action": "get", "database": cfg["db"], "after_id": 0, "per_page": 100}, timeout=120)
    try: p = r.json()
    except Exception: p = {}
    rows = f._extract_rows(p) if isinstance(p, dict) else []
    print(f"V2 {mod}.{cls} keyset: {r.status_code} rows={len(rows)} pag={p.get('pagination') if isinstance(p, dict) else None}")
    if rows:
        print("   fields:", sorted(rows[0])[:40]); print("   sample:", {k: rows[0].get(k) for k in ("id", "username", "email", "employee_number", "active")}); break
v.set_v3_target_facility(FAC); v.v3_login_org_cfg(); v._fetch_available_models()
print("V3 users meta:", v._gateway_model_meta.get("users"))
d = v._gateway_post("core", {"action": "describe", "model": "users", "destination_tenant_id": 4}).json().get("data", {})
print("V3 users columns:", d.get("copy_safe"), "| excluded:", d.get("excluded"))
