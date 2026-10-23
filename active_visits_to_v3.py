"""
active_visits_to_v3.py — send the V2 visits that are still ACTIVE (patient still in the facility) to a V3 org,
with everything that hangs off them: the patient and their insurance, the bill (investigations, prescriptions,
dispensing, consumables, ward stays, theatre), invoices and receipts. Built for the Kisumu cut-over: V2 kept
running after the migration, so the visits open at switch-over must carry on in V3 (production org 284).

  which visits   active_visits(): one rule, chosen with --rule (see RULES). V2 has no "visit closed" flag
                 that Kisumu fills — evaluation_visits.status is NULL or 'billed', and visit_destinations
                 .checkout stays 0 on ~90 % of rows even years later — so a visit is active when it has an
                 open admission (no discharged_at) or it is recent (created in the last --days days, V2
                 local time) and still has a destination nobody checked out of. Every rule's count is
                 reported with the run.
  rows           per table, only the rows of those visits (by visit id; patients / patient schemes by the
                 visits' patients; schemes / insurance companies by the schemes they use): the Snowflake
                 snapshot's rows plus the live V2 API's rows past it (the API pages by id and ignores
                 updated_since, so reading only its tail keeps this to minutes). Rows from before the
                 snapshot carry the snapshot's values, not edits made in V2 since.
  posting        v2_delta_to_v3's loader: the V2 load's transform rules and stable uuids, links chained
                 V2 → V2-load org → target; a record the target has (uuid / chained id map / content) is
                 UPDATED in place, anything else INSERTED — whatever its age (these visits can predate any
                 cut-off date). Its own delta state: .migration_state/v2active_<source>_to_<target>/.
  then           v3_org_corrections for the same visits: evaluation visit destinations and consumables (the
                 tables the V2 load has no model for), and admission states (admitted / discharged + ward stay).
  not sent       tables of the list the V2 load has no V3 model / gateway alias for are listed in the report
                 with the reason (invoice items, payment details, cash / M-Pesa / card / cheque, visit charges …).

    python active_visits_to_v3.py --target org284 --dry-run                       # rule counts + plan, posts nothing
    python active_visits_to_v3.py --target org284 --rule open_admission --dry-run
    python active_visits_to_v3.py --target org284 --days 2                        # the real thing

Writes output/active_visits/<source>-to-<target>-<rule>-<time>.csv (per table: rows, updated, inserted, held,
failed, why) and .json (the rule counts, the visit ids, the corrections' counts, the tables not sent).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "dags"))

log = logging.getLogger("active_visits_to_v3")

NAIROBI = ZoneInfo("Africa/Nairobi")             # V2 stores local wall time

RULES = {
    "default": "an open admission, or created in the last N days with a destination not checked out",
    "open_admission": "the visit has a V2 admission with no discharged_at (inpatients still in)",
    "recent": "created in the last N days",
    "recent_open_destination": "created in the last N days with at least one destination not checked out",
    "recent_unbilled": "created in the last N days and visit status not 'billed'",
}

# The list asked for (2026-10-21), in the V2 load's parent-first order. Each table: how its rows are tied to
# the active visits — (key set, the row's columns that hold it). evaluation_visits / reception_patients /
# evaluation_investigations are the twins of visits / patients / investigations (same V2 classes, the load
# posts them under the main name); reception_patient_schemes is loaded as patient_insurance.
TABLES: list[tuple[str, str, tuple[str, ...]]] = [
    ("settings_insurance", "insurance", ("id",)),
    ("settings_schemes", "scheme", ("id",)),
    ("patients", "patient", ("id",)),
    ("patient_insurance", "patient", ("patient", "patient_id")),
    ("visits", "visit", ("id",)),
    ("inpatient_admissions", "visit", ("visit_id", "visit")),
    ("investigations", "visit", ("visit", "visit_id")),
    ("evaluation_prescriptions", "visit", ("visit", "visit_id")),
    ("inventory_evaluation_dispensing", "visit", ("visit", "visit_id")),
    # theatre chain booking → schedule → operation: a schedule carries only its booking (bookings_of())
    ("theatre_bookings", "visit", ("visit_id", "visit")),
    ("theatre_schedules", "booking", ("booking_id",)),
    ("theatre_operations", "visit", ("visit_id", "visit")),
    ("inpatient_visit_wards", "visit", ("visit_id", "visit")),
    ("finance_invoices", "visit", ("visit", "visit_id")),
    # a V2 receipt doesn't carry its visit (evaluation_payments.visit is empty): receipt_ids() finds them
    ("finance_evaluation_payments", "receipt", ("id",)),
]
COVERED_ELSEWHERE = {
    "evaluation_visits": "sent as its twin `visits` (same V2 class)",
    "reception_patients": "sent as its twin `patients` (same V2 class)",
    "evaluation_investigations": "sent as its twin `investigations` (same V2 class)",
    "reception_patient_schemes": "sent as `patient_insurance` (the V2 load's name for it)",
    "evaluation_visit_destinations": "sent by v3_org_corrections eval_visit_destinations (the frontend reads the "
                                     "evaluation service's copy)",
    "evaluation_consumables": "sent by v3_org_corrections eval_consumables",
}
ASKED = ["evaluation_visits", "evaluation_visit_destinations", "reception_patients", "reception_patient_schemes",
         "settings_schemes", "settings_insurance", "evaluation_investigations", "evaluation_prescriptions",
         "inventory_evaluation_dispensing", "evaluation_consumables", "inpatient_visit_charges",
         "inpatient_charge_sheet", "inpatient_visit_wards", "theatre_operations", "theatre_payments",
         "finance_removed_bills", "finance_waivers", "finance_copay", "finance_change_insurances",
         "finance_split_insurances", "finance_split_insurance_items", "finance_invoices", "finance_invoice_items",
         "finance_invoice_payments", "finance_evaluation_payments", "finance_evaluation_payment_details",
         "finance_payments_cash", "finance_payments_mpesa", "finance_payments_card", "finance_payments_cheque"]
CORRECTION_STEPS = ["eval_visit_destinations", "eval_consumables", "admission_discharges"]
VISIT_NS = r"Ignite\Evaluation\Entities\Visit"
VD_NS = r"Ignite\Evaluation\Entities\VisitDestinations"
ADMISSION_NS = r"Ignite\Inpatient\Entities\Admission"


# ─── which visits are active (pure: tests/test_active_visits_to_v3.py) ─────────────────────────────

def _local(v) -> str:
    """A V2 timestamp as sortable local text ('2026-10-21 15:03:25'); '' when empty."""
    return str(v or "").replace("T", " ")[:19]


def _open(flag) -> bool:
    return str(flag).strip().lower() in ("0", "false", "")


def is_active(visit: dict, rule: str, *, cutoff: str, open_admission: set[str], open_destination: set[str]) -> bool:
    """The rule that decides whether a V2 visit is still active. `cutoff`: local time; `open_admission`
    / `open_destination`: V2 visit ids (text) with an admission not discharged / a destination not
    checked out."""
    vid = str(visit.get("id"))
    if visit.get("deleted_at"):
        return False
    recent = _local(visit.get("created_at")) >= cutoff
    if rule == "open_admission":
        return vid in open_admission
    if rule == "recent":
        return recent
    if rule == "recent_open_destination":
        return recent and vid in open_destination
    if rule == "recent_unbilled":
        return recent and str(visit.get("status") or "").lower() != "billed"
    if rule == "default":
        return vid in open_admission or (recent and vid in open_destination)
    raise ValueError(f"unknown rule {rule!r}; rules are {sorted(RULES)}")


def keys_for(visits: list[dict], patient_schemes: list[dict], schemes: dict[str, dict]) -> dict[str, set[str]]:
    """The key sets the tables are filtered by: visit ids, their patients, the schemes the visits or the
    patients' scheme rows use, and those schemes' insurance companies."""
    vis = {str(v["id"]) for v in visits}
    pat = {str(v["patient"]) for v in visits if v.get("patient") not in (None, "")}
    sch = {str(v["scheme"]) for v in visits if v.get("scheme") not in (None, "")}
    sch |= {str(r.get("scheme")) for r in patient_schemes
            if str(r.get("patient") or r.get("patient_id")) in pat and r.get("scheme") not in (None, "")}
    ins = {str(schemes[s].get("company")) for s in sch if s in schemes and schemes[s].get("company") not in (None, "")}
    return {"visit": vis, "patient": pat, "scheme": sch, "insurance": ins}


def receipt_ids(visits: list[dict], details: list[dict], invoice_ids: set[str], receipts: list[dict]) -> set[str]:
    """The V2 receipts (finance_evaluation_payments) of the active visits. V2 leaves a receipt's visit empty, so:
    the receipts its payment-detail lines name for those visits, the receipts paying their invoices, and —
    for receipts with neither — the visit's patient's receipts dated on/after that patient's active visit."""
    vis = {str(v["id"]) for v in visits}
    since = {}
    for v in visits:
        p = str(v.get("patient") or "")
        if p:
            since[p] = min(since.get(p, "9999"), _local(v.get("created_at")))
    out = {str(d.get("payment")) for d in details if str(d.get("visit")) in vis and d.get("payment") not in (None, "")}
    for r in receipts:
        if r.get("deleted_at"):
            continue
        rid, inv, pat = str(r["id"]), str(r.get("invoice_id") or ""), str(r.get("patient") or "")
        if inv and inv in invoice_ids:
            out.add(rid)
        elif not inv and str(r.get("visit") or "") in vis:
            out.add(rid)
        elif not inv and pat in since and _local(r.get("created_at")) >= since[pat]:
            out.add(rid)
    return out


def row_filter(keys: set[str], cols: tuple[str, ...]):
    """A row passes when any of `cols` holds one of `keys` (ids compared as text)."""
    def keep(r: dict) -> bool:
        for c in cols:
            v = r.get(c, r.get(c.upper()))
            if v not in (None, "") and str(v).split(".")[0] in keys:
                return True
        return False
    return keep


# ─── V2 side ───────────────────────────────────────────────────────────────────────────────────────

def _sf_rows(loader, sql: str, cols: list[str]) -> list[dict]:
    return [dict(zip(cols, r)) for r in loader.sf(sql)]


def _in(ids) -> str:
    ids = sorted({int(i) for i in ids if str(i).isdigit()})
    return "(" + ",".join(map(str, ids or [-1])) + ")"


def gather(loader, days: int, now: datetime) -> dict:
    """The candidate visits (recent ones and those with an open admission), their destinations and the live
    admissions — snapshot + live API tail."""
    import snowflake_to_v3_migration as s2v3
    clean = s2v3.sf_schema(loader.facility, "CLEAN")
    cutoff = (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    widest = (now - timedelta(days=max(days, 14))).strftime("%Y-%m-%d %H:%M:%S")   # for the counts table
    adm = [a for a in loader.api_tail(ADMISSION_NS, 0) if not a.get("deleted_at")]
    open_adm = {str(a.get("visit_id")) for a in adm if not a.get("discharged_at")}
    vcols = ["id", "patient", "scheme", "status", "created_at", "deleted_at", "unique_id"]
    snap_max = loader.snapshot_max_id("evaluation_visits")
    visits = {str(v["id"]): v for v in _sf_rows(
        loader, f'SELECT {", ".join(chr(34) + c.upper() + chr(34) for c in vcols)} FROM {clean}.EVALUATION_VISITS '
                f'WHERE "CREATED_AT" >= \'{widest}\' OR "ID" IN {_in(open_adm)}', vcols)}
    for v in loader.api_tail(VISIT_NS, snap_max):
        visits[str(v["id"])] = v
    for vid in open_adm - set(visits):          # an open admission whose visit the snapshot and tail lack
        log.warning("open admission on V2 visit %s, which V2 doesn't list — left out", vid)
    dcols = ["id", "visit", "checkout", "deleted_at"]
    vd_max = loader.snapshot_max_id("evaluation_visit_destinations")
    dest = _sf_rows(loader, f'SELECT "ID", "VISIT", "CHECKOUT", "DELETED_AT" FROM {clean}.EVALUATION_VISIT_DESTINATIONS '
                            f'WHERE "VISIT" IN {_in(visits)}', dcols)
    latest = {str(d["id"]): d for d in dest}
    for d in loader.api_tail(VD_NS, vd_max):
        if str(d.get("visit")) in visits:
            latest[str(d["id"])] = d
    open_dest = {str(d["visit"]) for d in latest.values() if not d.get("deleted_at") and _open(d.get("checkout"))}
    return {"visits": visits, "open_admission": open_adm, "open_destination": open_dest, "cutoff": cutoff,
            "snapshot_max_visit": snap_max}


def rule_counts(g: dict, now: datetime, days_list=(1, 2, 3, 7, 14)) -> dict:
    out = {}
    for rule in RULES:
        for d in days_list if rule != "open_admission" else (0,):
            cutoff = (now - timedelta(days=d)).strftime("%Y-%m-%d %H:%M:%S")
            n = sum(is_active(v, rule, cutoff=cutoff, open_admission=g["open_admission"],
                              open_destination=g["open_destination"]) for v in g["visits"].values())
            out[f"{rule}" + (f" ({d}d)" if rule != "open_admission" else "")] = n
    return out


def patient_schemes_and_schemes(loader, patients: set[str]) -> tuple[list[dict], dict[str, dict]]:
    import snowflake_to_v3_migration as s2v3
    clean = s2v3.sf_schema(loader.facility, "CLEAN")
    ps = _sf_rows(loader, f'SELECT "ID", "PATIENT", "SCHEME", "DELETED_AT" FROM {clean}.RECEPTION_PATIENT_SCHEMES '
                          f'WHERE "PATIENT" IN {_in(patients)}', ["id", "patient", "scheme", "deleted_at"])
    ns = loader.namespace("reception_patient_schemes") or loader.namespace("patient_insurance")
    if ns:
        ps += [r for r in loader.api_tail(ns, loader.snapshot_max_id("reception_patient_schemes"))
               if str(r.get("patient")) in patients]
    schemes = {str(i): {"id": i, "company": c} for i, c in loader.sf(f'SELECT "ID", "COMPANY" FROM {clean}.SETTINGS_SCHEMES')}
    sns = loader.namespace("settings_schemes")
    if sns:
        for r in loader.api_tail(sns, loader.snapshot_max_id("settings_schemes")):
            schemes[str(r["id"])] = r
    return ps, schemes


def receipts_of(loader, visits: list[dict]) -> set[str]:
    """receipt_ids() fed from the snapshot + the live API's tail."""
    import snowflake_to_v3_migration as s2v3
    clean = s2v3.sf_schema(loader.facility, "CLEAN")
    vids, pats = _in(v["id"] for v in visits), _in(v.get("patient") for v in visits)
    details = _sf_rows(loader, f'SELECT "VISIT", "PAYMENT" FROM {clean}.FINANCE_EVALUATION_PAYMENT_DETAILS '
                               f'WHERE "VISIT" IN {vids}', ["visit", "payment"])
    dns = loader.namespace("finance_evaluation_payment_details")
    if dns:
        details += loader.api_tail(dns, loader.snapshot_max_id("finance_evaluation_payment_details"))
    vset = {str(v["id"]) for v in visits}
    inv = {str(i) for (i,) in loader.sf(f'SELECT "ID" FROM {clean}.FINANCE_INVOICES WHERE "VISIT" IN {vids}')}
    inv |= {str(r["id"]) for r in loader.api_tail(loader.namespace("finance_invoices"),
                                                  loader.snapshot_max_id("finance_invoices"))
            if str(r.get("visit")) in vset}
    rcols = ["id", "visit", "patient", "invoice_id", "created_at", "deleted_at"]
    receipts = _sf_rows(loader, f'SELECT {", ".join(chr(34) + c.upper() + chr(34) for c in rcols)} FROM '
                                f'{clean}.FINANCE_EVALUATION_PAYMENTS WHERE "PATIENT" IN {pats} OR "INVOICE_ID" IN {_in(inv)}', rcols)
    receipts += loader.api_tail(loader.namespace("finance_evaluation_payments"),
                                loader.snapshot_max_id("finance_evaluation_payments"))
    return receipt_ids(visits, details, inv, receipts)


def bookings_of(loader, visits: list[dict]) -> set[str]:
    """The V2 theatre bookings of the active visits: the snapshot's + the live API's tail."""
    import snowflake_to_v3_migration as s2v3
    clean = s2v3.sf_schema(loader.facility, "CLEAN")
    vset = {str(v["id"]) for v in visits}
    out = {str(i) for (i,) in loader.sf(f'SELECT "ID" FROM {clean}.THEATRE_BOOKINGS WHERE "VISIT_ID" IN {_in(vset)}')}
    out |= {str(r["id"]) for r in loader.api_tail(loader.namespace("theatre_bookings"),
                                                  loader.snapshot_max_id("theatre_bookings"))
            if str(r.get("visit_id") or r.get("visit")) in vset}
    return out


def prescription_stores_of(loader, visits: list[dict]) -> dict[str, str]:
    """V2 prescription id → V2 store id for the active visits (snapshot + live tail): the store a
    dispensing is sold from (V3 inv_sales.store_id is NOT NULL; V2 dispensing has no store)."""
    import snowflake_to_v3_migration as s2v3
    clean = s2v3.sf_schema(loader.facility, "CLEAN")
    vset = {str(v["id"]) for v in visits}
    out = {str(i): str(st) for i, st in loader.sf(f'SELECT "ID", "STORE_ID" FROM {clean}.EVALUATION_PRESCRIPTIONS '
                                                   f'WHERE "VISIT" IN {_in(vset)} AND "STORE_ID" IS NOT NULL')}
    out.update({str(r["id"]): str(r["store_id"]) for r in loader.api_tail(
        loader.namespace("evaluation_prescriptions"), loader.snapshot_max_id("evaluation_prescriptions"))
        if str(r.get("visit") or r.get("visit_id")) in vset and r.get("store_id") not in (None, "", 0, "0")})
    return out


def not_sent(loader, source: str) -> dict[str, str]:
    """The asked-for tables this run doesn't send, and why (the V2 load's own reasons)."""
    import snowflake_to_v3_migration as s2v3
    sent = {t for t, _, _ in TABLES}
    rest = [t for t in ASKED if t not in sent and t not in COVERED_ELSEWHERE]
    run, unmapped, no_insert = s2v3._select_entries(source, rest, None, set(loader.m._gateway_model_meta))
    why = {e["table"]: e.get("skipped") or "no V3 model" for e in unmapped}
    why.update({e["table"]: f"V3 model {e.get('v3')} exists but the gateway can't insert it (no alias)" for e in no_insert})
    why.update({e["table"]: "mapped by the V2 load but not in this run's list — add it to TABLES" for e in run})
    for t in rest:
        why.setdefault(t, "not in the V2 snapshot: never extracted from V2 (empty or not in this V2 database)")
    return why


# ─── the run ───────────────────────────────────────────────────────────────────────────────────────

def make_active_loader(source: str, target: str, workers: int, page_workers: int, dry_run: bool, delta_root: Path):
    import snowflake_to_v3_migration as s2v3
    import v2_delta_to_v3 as d
    loader = d.make_loader(source, target, "2000-01-01", False, workers, page_workers, dry_run, delta_root)
    base = type(loader)

    class ActiveLoader(base):
        scope: dict[str, tuple[set[str], tuple[str, ...]]] = {}

        def api_tail(self, ns, after_id):
            rows = super().api_tail(ns, after_id)
            f = getattr(self, "_tail_filter", None)
            if f and ns == f[0]:
                rows = [r for r in rows if f[1](r)]
                self._seen += len(rows)
            return rows

        def capture(self, table):
            """The base loader's snapshot + tail read (not the delta's whole-table one), each row kept only
            when it belongs to an active visit."""
            keys, cols = self.scope[table]
            keep = row_filter(keys, cols)
            real_chunks = s2v3.iter_clean_chunks

            def scoped(facility, tbl, chunk_rows=s2v3.CHUNK_ROWS):
                for ch in real_chunks(facility, tbl, chunk_rows):
                    ch = [r for r in ch if keep({str(k).lower(): v for k, v in r.items()})]
                    if ch:
                        self._seen += len(ch)
                        yield ch
            self._seen, self._tail_filter = 0, (self.namespace(table), keep)
            saved = (self.since, self.live)
            self.since, self.live = None, False
            s2v3.iter_clean_chunks = scoped
            try:
                out = super().capture(table)
            finally:
                s2v3.iter_clean_chunks = real_chunks
                self.since, self.live = saved
                self._tail_filter = None
            self._live_rows = self._seen
            return out

    loader.__class__ = ActiveLoader
    # plan_delta inserts an unmatched record only when its created_at ≥ since; some transforms (receipts) carry
    # no created_at, and an active visit's rows may be of any age — "" lets every unmatched one through (the
    # uuid / chained id map / content matches still come first)
    loader.since = ""
    return loader


def corrections(target: str, source: str, visits: dict[str, dict], dry_run: bool, steps: list[str], workers: int) -> dict:
    """v3_org_corrections for the active visits only: its V2 visit map narrowed to them."""
    import v3_org_corrections as oc
    x = oc.Ctx(target, source, dry_run, workers)
    x._v2_visit_uid = {vid: v.get("unique_id") for vid, v in visits.items()}
    out = {}
    for s in steps:
        t0 = time.time()
        try:
            out[s] = {"eval_visit_destinations": lambda: oc.eval_visit_destinations(x, True, 0),
                      "eval_consumables": lambda: oc.eval_consumables(x, 0),
                      "admission_discharges": lambda: oc.admission_discharges(x)}[s]()
        except Exception as e:
            log.exception("%s failed", s)
            out[s] = {"error": f"{type(e).__name__}: {str(e)[:300]}"}
        out[s]["seconds"] = round(time.time() - t0)
        # the steps count every V2 row they skip as "visit not in the org" — here that's every inactive visit
        out[s].pop("visit not in the org", None)
    return out


def run(source: str, target: str, rule: str = "default", days: int = 1, workers: int = 16, page_workers: int = 16,
        dry_run: bool = False, corrections_steps: list[str] | None = None, out: Path | None = None,
        now: datetime | None = None, tables: list[str] | None = None) -> Path:
    import v2_delta_to_v3 as d
    if rule not in RULES:
        raise SystemExit(f"--rule must be one of {sorted(RULES)}")
    now = now or datetime.now(NAIROBI).replace(tzinfo=None)
    delta_root = ROOT / ".migration_state" / f"v2active_{source}_to_{target}"
    loader = make_active_loader(source, target, workers, page_workers, dry_run, delta_root)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    out = out or ROOT / "output" / "active_visits" / f"{source}-to-{target}-{rule}-{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    loader.report_path = out.with_suffix(".plan.json")
    t0 = time.time()
    g = gather(loader, days, now)
    counts = rule_counts(g, now)
    active = {vid: v for vid, v in g["visits"].items()
              if is_active(v, rule, cutoff=g["cutoff"], open_admission=g["open_admission"],
                           open_destination=g["open_destination"])}
    ps, schemes = patient_schemes_and_schemes(loader, {str(v["patient"]) for v in active.values() if v.get("patient")})
    keys = keys_for(list(active.values()), ps, schemes)
    keys["receipt"] = receipts_of(loader, list(active.values()))
    keys["booking"] = bookings_of(loader, list(active.values()))
    loader.m._prescription_store.update(prescription_stores_of(loader, list(active.values())))
    loader.scope = {t: (keys[k], cols) for t, k, cols in TABLES}
    todo = [t for t, _, _ in TABLES if not tables or t in tables]
    log.info("active visits (%s, %d day(s), as of %s): %d — %s; keys: %s", rule, days, now, len(active),
             RULES[rule], {k: len(v) for k, v in keys.items()})
    for table in todo:                               # parents first: children link to the parents posted before them
        loader.run([table])
        if not dry_run:
            loader.persist_new_ids()
        d.write_csv(loader.report, out, f"active visits: {rule}", "dry run" if dry_run else "posted")
    fixes = corrections(target, source, active, dry_run, CORRECTION_STEPS if corrections_steps is None
                        else corrections_steps, workers)
    summary = {"source": source, "target": target, "rule": rule, "rule_means": RULES[rule], "days": days,
               "as_of_v2_local": str(now), "dry_run": dry_run, "rule_counts": counts,
               "active_visits": len(active), "keys": {k: len(v) for k, v in keys.items()},
               "visit_ids": sorted(int(i) for i in active), "corrections": fixes,
               "covered_elsewhere": COVERED_ELSEWHERE, "not_sent": not_sent(loader, source),
               "minutes": round((time.time() - t0) / 60, 1), "csv": str(out)}
    out.with_suffix(".json").write_text(json.dumps(summary, indent=2, default=str))
    log.info("done in %.0f min: %s", (time.time() - t0) / 60, out)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", default="kisumu_v3")
    ap.add_argument("--target", required=True)
    ap.add_argument("--rule", default="default", choices=sorted(RULES), help="which visits count as active")
    ap.add_argument("--days", type=int, default=1, help="the recent rules: created in the last N days")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--page-workers", type=int, default=16)
    ap.add_argument("--tables", nargs="*", default=None, help=f"only these of {[t for t, _, _ in TABLES]}")
    ap.add_argument("--no-corrections", action="store_true", help="skip visit destinations / consumables / discharges")
    ap.add_argument("--dry-run", action="store_true", help="read + transform + decide; post nothing")
    ap.add_argument("--out", type=Path, default=None)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run(a.source, a.target, a.rule, a.days, a.workers, a.page_workers, a.dry_run,
              [] if a.no_corrections else None, a.out, tables=a.tables))
    return 0


if __name__ == "__main__":
    sys.exit(main())
