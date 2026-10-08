"""Read-only: org-4 V3 row counts for the link tables, twice, 60s apart."""
import logging, time
import v2_to_v3_api_migration as v2v3

logging.getLogger().setLevel(logging.ERROR)
v2v3.set_v3_target_facility("kisumu_v3")
v2v3.v3_login_org_cfg()
ALIASES = ["visits", "prescriptions", "investigations", "doctor_notes", "vitals", "visit_destinations"]


def counts():
    out = {}
    for a in ALIASES:
        r = v2v3._gateway_post("evaluation", {"action": "read", "model": a, "source_tenant_id": 4, "per_page": 1}, timeout=60)
        out[a] = r.json().get("meta", {}).get("total") if r.ok else r.status_code
    return out


a = counts(); print(time.strftime("%H:%M:%S"), a)
time.sleep(60)
b = counts(); print(time.strftime("%H:%M:%S"), b)
print("growth per minute:", {k: (b[k] - a[k]) if isinstance(a[k], int) and isinstance(b[k], int) else "?" for k in a})
