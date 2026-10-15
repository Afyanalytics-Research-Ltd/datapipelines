"""
load_scheme_tables.py — V2 insurance setup + procedure prices → V3, for one facility (2026-10-14).

  settings_insurance          → core insurance_companies      (prerequisite: core schemes need a core company)
  settings_schemes            → core insurance_schemes
  settings_scheme_exclusions  → core scheme_pricing, core scheme_exclusions, finance insurance_scheme_pricing
  evaluation_procedure_prices → evaluation procedure_prices   (each procedure's latest price — V3 holds one)

Every link is resolved from what V3 holds right now, not from the local id map
(the local .migration_state is older than the server's):
  * core company / scheme   — the rows this script posted, by uuid
  * finance scheme          — its code SCH-<facility>-<V2 id>
  * procedure               — sync_id_map's match by code (V2 repeats some codes)

Each row posts with the migration's stable uuid (uuid5 of facility/alias/V2 id,
as snowflake_to_v3_migration does) and match_on uuid, so a re-run — or the
pipeline posting the same row later — updates it in place, never a copy.

V2 "exclusions" are really per-scheme procedure prices (3 of 2,929 excluded).
In core scheme_exclusions an active row excludes the procedure from the
scheme, so only the excluded ones are active; the rest are kept inactive with
their V2 price in the description.

    python load_scheme_tables.py --dry-run
    python load_scheme_tables.py --limit 3
    python load_scheme_tables.py
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "dags"))
sys.path.insert(0, "/opt/airflow/dags")

log = logging.getLogger("load_scheme_tables")

STEPS = ["insurance_companies", "insurance_schemes", "placeholder_procedures", "scheme_pricing",
         "scheme_exclusions", "insurance_scheme_pricing", "procedure_prices"]
SERVICE = {"insurance_companies": "core", "insurance_schemes": "core", "scheme_pricing": "core",
           "placeholder_procedures": "evaluation", "procedures": "evaluation",
           "scheme_exclusions": "core", "insurance_scheme_pricing": "finance", "procedure_prices": "evaluation",
           "insurance_scheme": "finance"}
PRICE_FIELDS = ["cash_charge", "insurance_charge", "charge_insurance", "precharge", "status", "variable_pricing",
                "requires_preauth", "preauth_cash", "preauth_insurance", "preauth_specific_insurance",
                "preauth_specific_scheme"]


def blank(v):
    return None if v is None or (isinstance(v, str) and not v.strip()) else v


def flag(v) -> int:
    return int(str(v).strip().lower() in ("1", "true"))


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class Loader:
    def __init__(self, facility: str, dry_run: bool, limit: int, workers: int):
        from pipelines_common import use_pipelines_dir
        use_pipelines_dir([facility])
        import snowflake_to_v3_migration as s2v3
        import v2_to_v3_api_migration as v2v3
        self.s2v3, self.m = s2v3, v2v3
        self.facility, self.dry_run, self.limit, self.workers = facility, dry_run, limit, workers
        v2v3.set_v3_target_facility(facility)
        v2v3._fetch_available_models()
        self.org = v2v3.FACILITY_V3_CONFIG[facility]
        login = v2v3.v3_login_org_cfg().get("organization_id")
        if login != self.org.get("organization_id"):
            raise SystemExit(f"V3 login is org {login}, {facility} is org {self.org.get('organization_id')}")
        self.clean = s2v3.sf_schema(facility, "CLEAN")
        self.cur = s2v3._snowflake_connect().cursor()
        self.counts: dict[str, dict[str, int]] = {}

    # ── reads ──────────────────────────────────────────────────────────────
    def sf(self, sql: str) -> list[dict]:
        self.cur.execute(sql)
        cols = [d[0].lower() for d in self.cur.description]
        return [dict(zip(cols, r)) for r in self.cur.fetchall()]

    def v3(self, alias: str) -> list[dict]:
        return self.m._fetch_v3_records(alias, self.org, service_name=SERVICE.get(alias, "core")) or []

    def uid(self, alias: str, v2_id) -> str:
        return str(uuid.uuid5(self.s2v3._STABLE_UUID_NS, f"{self.facility}/{alias}/{v2_id}"))

    def by_uuid(self, alias: str) -> dict[str, int]:
        return {str(r["uuid"]).lower(): r["id"] for r in self.v3(alias) if r.get("uuid")}

    def procedure_map(self) -> dict[int, int]:
        """V2 procedure id → V3 procedure id, through sync_id_map's code matching."""
        v2 = self.sf(f"SELECT * FROM {self.clean}.EVALUATION_PROCEDURES")
        found: dict[int, int] = {}
        m = self.m
        saved = (m.extract_v2_records, m._store_id_mapping, m.v2_facility_config)
        m.extract_v2_records = lambda job: (v2, [])
        m._store_id_mapping = lambda alias, k, v: found.__setitem__(k, v)
        m.v2_facility_config = lambda f: {"db": "snowflake"}
        try:
            m.sync_id_map("procedures", r"Ignite\Evaluation\Entities\Procedures", self.facility, match_field="code")
        finally:
            m.extract_v2_records, m._store_id_mapping, m.v2_facility_config = saved
        out = {int(k): int(v) for k, v in found.items() if str(k).isdigit()}
        log.info("procedures: %d of %d V2 procedures matched to V3", len(out), len(v2))
        return out

    # ── posting ────────────────────────────────────────────────────────────
    def post(self, alias: str, rows: list[tuple[object, dict]]) -> dict[object, int]:
        return self.post_as(alias, alias, rows)

    def post_as(self, alias: str, label: str, rows: list[tuple[object, dict]]) -> dict[object, int]:
        """rows: (V2 id, payload with uuid). Returns V2 id → V3 id for those posted."""
        c = self.counts.setdefault(label, {"rows": len(rows), "posted": 0, "failed": 0})
        if self.limit:
            rows = rows[: self.limit]
        if self.dry_run:
            for v2_id, data in rows[:2]:
                log.info("  [dry-run] %s V2 %s → %s", alias, v2_id, data)
            return {}
        out: dict[object, int] = {}

        def one(item):
            v2_id, data = item
            try:
                v3_id = self.m._post_to_v3_batch(alias, self.org, data, alias_override=alias,
                                                 service_override=SERVICE[alias])
                return v2_id, v3_id
            except Exception as e:   # dead-lettered (logged by the poster) or network
                log.error("  %s V2 %s failed: %s", alias, v2_id, e)
                return v2_id, None

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            for v2_id, v3_id in pool.map(one, rows):
                if v3_id:
                    out[v2_id] = int(v3_id)
                    c["posted"] += 1
                else:
                    c["failed"] += 1
        log.info("%s: %d posted, %d failed (of %d)", alias, c["posted"], c["failed"], len(rows))
        return out

    def ids_after(self, alias: str, v2_ids) -> dict[object, int]:
        """V2 id → V3 id for rows of this script's, from V3 itself (covers earlier runs)."""
        have = self.by_uuid(alias)
        return {i: have[self.uid(alias, i)] for i in v2_ids if self.uid(alias, i) in have}

    # ── steps ──────────────────────────────────────────────────────────────
    def run(self, steps: list[str]) -> None:
        f = self.facility
        companies = self.sf(f"SELECT * FROM {self.clean}.SETTINGS_INSURANCE")
        schemes = self.sf(f"SELECT * FROM {self.clean}.SETTINGS_SCHEMES")
        excl = self.sf(
            f'SELECT e.*, pp."CASH_CHARGE" AS "STANDARD_PRICE", p."CODE" AS "PROC_CODE", p."NAME" AS "PROC_NAME" '
            f'FROM {self.clean}.SETTINGS_SCHEME_EXCLUSIONS e '
            f'LEFT JOIN {self.clean}.EVALUATION_PROCEDURES p ON TO_VARCHAR(p."ID") = TO_VARCHAR(e."PROCEDURE_ID") '
            f'LEFT JOIN (SELECT "EVALUATION_PROCEDURE_ID", "CASH_CHARGE" FROM {self.clean}.EVALUATION_PROCEDURE_PRICES '
            f'QUALIFY ROW_NUMBER() OVER (PARTITION BY "EVALUATION_PROCEDURE_ID" ORDER BY "UPDATED_AT" DESC NULLS LAST, '
            f'"ID" DESC) = 1) pp ON TO_VARCHAR(pp."EVALUATION_PROCEDURE_ID") = TO_VARCHAR(e."PROCEDURE_ID") '
            f'WHERE e."DELETED_AT" IS NULL')
        log.info("V2 %s: %d companies, %d schemes, %d scheme exclusions", f, len(companies), len(schemes), len(excl))
        procs = self.procedure_map() if {"scheme_pricing", "insurance_scheme_pricing", "procedure_prices"} & set(steps) else {}

        # core insurance companies
        if "insurance_companies" in steps:
            rows = [(c["id"], {
                "uuid": self.uid("insurance_companies", c["id"]),
                "name": blank(c.get("name")) or f"Insurance company {c['id']}",
                "code": f"INS-{f}-{c['id']}",
                "address": blank(c.get("address")), "city": blank(c.get("town")),
                "postal_code": blank(c.get("post_code")), "phone": blank(c.get("telephone")),
                "mobile_number": blank(c.get("mobile")), "email": blank(c.get("email")),
                "kra_pin": blank(c.get("kra_pin")), "is_corporate": flag(c.get("is_corporate")),
                "procuring_officer_name": blank(c.get("procuring_officer_name")),
                "building": blank(c.get("building")),
            }) for c in companies]
            self.post("insurance_companies", rows)
        core_company = self.ids_after("insurance_companies", [c["id"] for c in companies])

        # core insurance schemes
        if "insurance_schemes" in steps:
            rows, held = [], 0
            for s in schemes:
                company = core_company.get(s.get("company"))
                if not company and not self.dry_run:
                    held += 1
                    continue
                gender = str(s.get("gender") or "").strip().lower()
                rows.append((s["id"], {
                    "uuid": self.uid("insurance_schemes", s["id"]),
                    "company_id": company, "name": blank(s.get("name")) or f"Scheme {s['id']}",
                    "code": f"SCH-{f}-{s['id']}",
                    "status": "active" if flag(s.get("active")) else "inactive",
                    "amount": num(s.get("amount")), "capitation_copay_amount": num(s.get("capitation_copay_amount")),
                    "attention": blank(s.get("attention")), "smart_enabled": flag(s.get("smart")),
                    "age_restricted": flag(s.get("age_restricted")), "age_range": blank(s.get("age_range")),
                    "gender_restricted": flag(s.get("gender_restricted")),
                    "gender": gender if gender in ("male", "female") else None,
                    "policy_number_compulsory": flag(s.get("policy_number_compulsory")),
                    "effective_date": blank(str(s.get("effective_date") or ""))[:10] if blank(s.get("effective_date")) else None,
                    "expiry_date": blank(str(s.get("expiry_date") or ""))[:10] if blank(s.get("expiry_date")) else None,
                }))
            if held:
                log.warning("insurance_schemes: %d held — their company isn't in core V3", held)
            self.post("insurance_schemes", rows)
        core_scheme = self.ids_after("insurance_schemes", [s["id"] for s in schemes])
        fin_scheme = {}
        for r in sorted(self.v3("insurance_scheme"), key=lambda x: int(x["id"])):
            code = str(r.get("code") or "")
            if code.startswith(f"SCH-{f}-") and code[len(f"SCH-{f}-"):].isdigit():
                fin_scheme.setdefault(int(code[len(f"SCH-{f}-"):]), r["id"])

        # Scheme prices whose procedure is gone from V2 itself (2,638 kisumu_v3
        # rows, 10 procedures — not in the V2 API either): agreed 2026-10-14 to
        # link them to inactive placeholder procedures. Same uuid the pipeline
        # would give the real V2 procedure, so it would replace the placeholder.
        missing: dict[int, int] = {}
        for e in excl:
            pid = int(e["procedure_id"]) if e.get("procedure_id") is not None else None
            if pid is not None and pid not in procs:
                missing[pid] = missing.get(pid, 0) + 1
        if missing and "placeholder_procedures" in steps:
            self.counts["placeholder_procedures"] = {"rows": len(missing), "posted": 0, "failed": 0}
            placeholders = [(pid, {
                "uuid": self.uid("procedures", pid),
                "name": f"Deleted V2 procedure {pid}", "code": f"V2-DELETED-{pid}", "active": 0,
                "description": f"Placeholder: V2 procedure {pid} no longer exists in V2 ({f}); it holds "
                               f"{n} scheme prices from V2 settings_scheme_exclusions."})
                for pid, n in sorted(missing.items())]
            got = self.post_as("procedures", "placeholder_procedures", placeholders)
            have = self.by_uuid("procedures")
            for pid in missing:
                v3_id = got.get(pid) or have.get(self.uid("procedures", pid))
                if v3_id:
                    procs[pid] = int(v3_id)

        def price_rows(alias, scheme_map, build):
            rows, held = [], {"scheme": 0, "procedure": 0}
            for e in excl:
                scheme = scheme_map.get(e.get("scheme_id"))
                proc = procs.get(int(e["procedure_id"])) if e.get("procedure_id") is not None else None
                # a dry run hasn't created the core schemes yet: count, don't hold
                if not scheme and not (self.dry_run and scheme_map is not fin_scheme):
                    held["scheme"] += 1
                    if not self.dry_run:
                        continue
                if not proc and alias != "scheme_exclusions":
                    held["procedure"] += 1
                    if not self.dry_run:
                        continue
                rows.append((e["id"], {"uuid": self.uid(alias, e["id"]), **build(e, scheme, proc)}))
            if any(held.values()):
                log.warning("%s: held back — no V3 scheme: %d, no V3 procedure: %d", alias, held["scheme"],
                            held["procedure"])
            return rows

        def prices(e):
            std, price = num(e.get("standard_price")), num(e.get("price"))
            return (std if std is not None else price or 0), (price if price is not None else std or 0)

        if "scheme_pricing" in steps:
            self.post("scheme_pricing", price_rows("scheme_pricing", core_scheme, lambda e, sc, pr: {
                "scheme_id": sc, "priceable_type": "procedure", "priceable_id": pr,
                "standard_price": prices(e)[0], "scheme_price": prices(e)[1],
                "is_covered": 0 if flag(e.get("excluded")) else 1, "status": "active"}))

        if "scheme_exclusions" in steps:
            self.post("scheme_exclusions", price_rows("scheme_exclusions", core_scheme, lambda e, sc, pr: {
                "scheme_id": sc, "exclusion_type": "procedure",
                "exclusion_code": blank(e.get("proc_code")) or f"V2-DELETED-{e.get('procedure_id')}",
                "exclusion_name": blank(e.get("proc_name")) or f"Deleted V2 procedure {e.get('procedure_id')}",
                "status": "active" if flag(e.get("excluded")) else "inactive",
                "description": (f"V2 scheme price {prices(e)[1]:g}" + (" — excluded in V2" if flag(e.get("excluded"))
                                                                        else " — not excluded in V2"))}))

        if "insurance_scheme_pricing" in steps:
            self.post("insurance_scheme_pricing", price_rows("insurance_scheme_pricing", fin_scheme,
                                                             lambda e, sc, pr: {
                "scheme_id": sc, "item_id": pr, "item_type": "procedure",
                "standard_price": prices(e)[0], "scheme_price": prices(e)[1],
                "is_covered": 0 if flag(e.get("excluded")) else 1,
                "requires_preauth": flag(e.get("requires_preauth"))}))

        if "procedure_prices" in steps:
            latest = self.sf(f'SELECT * FROM {self.clean}.EVALUATION_PROCEDURE_PRICES QUALIFY ROW_NUMBER() OVER '
                             f'(PARTITION BY "EVALUATION_PROCEDURE_ID" ORDER BY "UPDATED_AT" DESC NULLS LAST, '
                             f'"ID" DESC) = 1')
            # V3 holds one price per org + procedure + facility: a procedure that
            # already has one is updated through that row's uuid, not duplicated
            existing = {}
            for r in sorted(self.v3("procedure_prices"), key=lambda x: int(x["id"])):
                if r.get("uuid") and r.get("evaluation_procedure_id"):
                    existing.setdefault(int(r["evaluation_procedure_id"]), str(r["uuid"]))
            rows, held = [], 0
            for p in latest:
                proc = procs.get(int(p["evaluation_procedure_id"])) if p.get("evaluation_procedure_id") else None
                if not proc and not self.dry_run:
                    held += 1
                    continue
                data = {k: p.get(k) for k in PRICE_FIELDS if k in p}
                for k, v in data.items():
                    if isinstance(v, str) and v.strip().lower() in ("true", "false"):
                        data[k] = flag(v)
                rows.append((p["id"], {"uuid": existing.get(proc) or self.uid("procedure_prices", p["id"]),
                                       "evaluation_procedure_id": proc, **data}))
            if held:
                log.warning("procedure_prices: %d held — procedure not in V3", held)
            self.post("procedure_prices", rows)

        for alias in steps:
            alias = "procedures" if alias == "placeholder_procedures" else alias
            log.info("V3 %s now: %s", alias, len(self.v3(alias)))
        for alias, c in self.counts.items():
            log.info("summary %-26s to post %-5d posted %-5d failed %d", alias, c["rows"], c["posted"], c["failed"])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-f", "--facility", default="kisumu_v3")
    ap.add_argument("--steps", nargs="*", default=STEPS, choices=STEPS)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="post at most N rows per table (canary)")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    Loader(a.facility, a.dry_run, a.limit, a.workers).run(a.steps)


if __name__ == "__main__":
    main()
