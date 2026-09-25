"""
Count records in V3 per model via the migration gateway's read action (read-only),
alongside what v2_to_v3_api_migration.py believes it has posted.

Usage:
  python count_v3_records.py                       # patient, visit
  python count_v3_records.py patient visit invoice
  python count_v3_records.py --facility afya_api_auth patient
"""
import argparse
import json

import v2_to_v3_api_migration as m


def v3_count(alias: str, org_cfg: dict) -> tuple[int | None, str]:
    """Total rows for one model. Uses the pagination total when the gateway
    returns one; otherwise pages through (per_page=500) and counts."""
    service = m._alias_to_service.get(alias, "core")
    base = {"action": "read", "model": alias, "source_tenant_id": org_cfg.get("organization_id")}

    r = m._gateway_post(service, {**base, "per_page": 1, "page": 1}, timeout=60)
    if not r.ok:
        return None, f"{r.status_code} {r.text[:200]}"
    payload = r.json()
    for meta in (payload.get("pagination"), payload.get("meta"),
                 payload.get("data") if isinstance(payload.get("data"), dict) else None, payload):
        if isinstance(meta, dict) and isinstance(meta.get("total"), int):
            return meta["total"], "pagination total"

    total, page = 0, 1
    while True:
        r = m._gateway_post(service, {**base, "per_page": 500, "page": page}, timeout=120)
        if not r.ok:
            return None, f"{r.status_code} on page {page}"
        rows = r.json().get("data") or []
        if isinstance(rows, dict):
            rows = rows.get("data") or []
        total += len(rows)
        if len(rows) < 500:
            return total, f"counted over {page} page(s)"
        page += 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("aliases", nargs="*", default=["patient", "visit"])
    ap.add_argument("--facility", default="afya_api_auth")
    args = ap.parse_args()

    m._fetch_available_models()  # builds alias → service routing
    org_cfg = m.FACILITY_V3_CONFIG.get(args.facility, {})

    progress = json.loads(m.RECORD_PROGRESS_FILE.read_text()) if m.RECORD_PROGRESS_FILE.exists() else {}
    posted = {}
    for job_key, ids in progress.items():
        v3_ns = m.NAMESPACE_MAP.get(job_key.split("|", 1)[-1], {}).get("v3")
        if v3_ns:
            posted[m._v3_alias(v3_ns)] = posted.get(m._v3_alias(v3_ns), 0) + len(ids)

    print(f"\n{'model':20s} {'rows in V3':>12s} {'posted by script':>17s}  source")
    for alias in args.aliases:
        n, how = v3_count(alias, org_cfg)
        print(f"{alias:20s} {n if n is not None else 'ERROR':>12} {posted.get(alias, 0):>17}  {how}")


if __name__ == "__main__":
    main()
