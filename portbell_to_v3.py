"""
portbell_to_v3.py — Portbell Medical Centre's MOH 204 A register → V3 migrate platform, org 7 / facility 9 (target key
"portbell", AFYA_PORTBELL_* in .env). Reads HOSPITALS.PORTBELL_CLEAN (see load_portbell_to_snowflake.py) and posts
through the model gateway, in order:

  patients      PATIENTS → reception/patient. patient_no = the clinic's OPD number (under-5s "U5-<opd>", their series
                overlaps the over-5s'; people only in the daily register "PBL-D<n>"). DOB estimated from the age at
                the visit (said so in notes); the under-5 guardian → emergency contact.
  visits        VISITS (one per patient per day) → evaluation/visits, all completed (the register ends 18 Oct 2026).
  vitals        temperature / weight / BP from the registers → evaluation/vitals.
  doctor_notes  diagnosis → assessment / primary + secondary diagnoses; treatment and the daily register's services
                (service point, Dx / procedure, amount) → plan → evaluation/doctor_notes.
  payments      each daily-register line with money → finance/payment (cash: the registers record CASH for every
                patient; the daily sheet has no payment column). Lines with no name are posted without a patient so
                the days still add up.
  (bookings)    CLINIC BOOKINGS have no dates, so they can't be appointments; each goes into its patient's notes in
                the patients step (the gateway's activity_log is read-only).

Every row is posted with the uuid5 the CLEAN layer gave it (match_on=uuid), so a re-run updates instead of
duplicating. --limit N posts the first N per step (canary); --dry-run posts nothing; --reconcile compares V3 with
CLEAN afterwards. Airflow: dags/portbell_moh204a_dag.py. By hand, inside the Airflow worker:

    docker exec -w /opt/airflow/pipelines datapipelines-airflow-worker-1 python portbell_to_v3.py --dry-run --limit 2
    ... python portbell_to_v3.py --steps patients --limit 3
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

sys.path.insert(0, "/opt/airflow/dags")
from load_portbell_to_snowflake import gid, join_unique  # noqa: E402

log = logging.getLogger("portbell_to_v3")

TARGET = "portbell"
SF = "HOSPITALS.PORTBELL_CLEAN"
STEPS = ("patients", "visits", "vitals", "doctor_notes", "payments")
SOURCE = "Portbell MOH 204A register"


# ── payloads (tests/test_portbell_to_v3.py) ────────────────────────────────
def iso(d) -> str | None:
    return d.isoformat() if isinstance(d, date) else (str(d)[:10] if d else None)


def stamp(d) -> str | None:
    return f"{iso(d)} 00:00:00" if iso(d) else None


def booking_note(r: dict) -> str:
    return (f"Clinic booking (no date in the register): {r.get('clinic_type') or 'clinic'}"
            + (f" with {r['consultant_name']}" if r.get("consultant_name") else "") + ".")


def patient_payload(r: dict, bookings: list[str] = ()) -> dict:
    ident = r.get("national_id")
    passport = bool(ident and any(ch.isalpha() for ch in ident))
    guardian = r.get("guardian_name")
    age_note = (f"Age {r['age_text']} recorded on {iso(r['age_recorded_on'])}; date of birth estimated from it."
                if r.get("age_text") and r.get("dob_estimated") else None)
    where = f"{r['register']}, OPD {r['opd_no']}" if r.get("opd_no") else r["register"]
    return {"uuid": r["patient_uuid"], "patient_no": r["patient_no"], "first_name": r["first_name"],
            "middle_name": r.get("middle_name"), "last_name": r["last_name"], "dob": iso(r.get("dob_estimated")),
            "gender": r.get("sex"), "occupation": r.get("occupation"), "mobile": r.get("phone"),
            "address": r.get("residence"), "id_no": None if passport else ident, "passport_no": ident if passport else None,
            "emergency_contact_name": guardian, "emergency_contact_relationship": "Parent/Guardian" if guardian else None,
            "emergency_contact_phone": r.get("phone") if guardian else None,
            "registration_date": iso(r.get("first_visit_date")), "registration_type": "new", "status": "active",
            "notes": " ".join(x for x in (f"Imported from the {SOURCE} ({where}).", age_note,
                                          "Name taken from the daily attendance register." if (r.get("name_source") or "").startswith("daily attendance (") else None,
                                          *bookings) if x),
            "created_at": stamp(r.get("first_visit_date")), "updated_at": stamp(r.get("last_visit_date") or r.get("first_visit_date"))}


def visit_payload(r: dict, patient_id: int, clinic: int | None) -> dict:
    kind = {"NEW": "New", "REVISIT": "Revisit"}.get(r.get("visit_type") or "")
    purpose = r.get("service_points") or ("OPD registration" if r.get("register") else None)
    return {"uuid": r["visit_uuid"], "patient": patient_id, "clinic": clinic, "inpatient": 0, "purpose": purpose,
            "payment_mode": "cash", "status": "completed", "unique_id": f"PBL-V-{r['visit_uuid'][:8]}",
            "vitals_recorded": 1 if any(r.get(k) is not None for k in ("temperature", "weight", "blood_pressure")) else 0,
            "type_of_visit": kind, "created_at": stamp(r["visit_date"]), "updated_at": stamp(r["visit_date"])}


def vitals_payload(r: dict, visit_id: int, patient_id: int) -> dict | None:
    if not any(r.get(k) is not None for k in ("temperature", "weight", "blood_pressure")):
        return None
    return {"uuid": gid("vitals", r["visit_key"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            "temperature": r.get("temperature"), "weight": r.get("weight"), "blood_pressure": r.get("blood_pressure"),
            "systolic_bp": r.get("systolic_bp"), "diastolic_bp": r.get("diastolic_bp"),
            "recorded_by_name": SOURCE, "recorded_at": stamp(r["visit_date"]),
            "created_at": stamp(r["visit_date"]), "updated_at": stamp(r["visit_date"])}


def doctor_note_payload(r: dict, visit_id: int, patient_id: int) -> dict | None:
    dx, rx, services = r.get("diagnosis"), r.get("treatment"), r.get("services")
    if not (dx or rx or services):
        return None
    listed = json.loads(r["diagnosis_list"]) if r.get("diagnosis_list") else []
    plan = "\n".join(x for x in (f"Treatment: {rx}" if rx else None, f"Services: {services}" if services else None) if x)
    return {"uuid": gid("doctor_note", r["visit_key"]), "visit_id": visit_id, "visit": visit_id, "patient_id": patient_id,
            "assessment": dx, "primary_diagnosis": listed[0][:255] if listed else None,
            "secondary_diagnoses": json.dumps(listed[1:]) if len(listed) > 1 else None,
            "plan": plan or None, "treatment_plan": rx, "status": "final", "doctor_name": None,
            "created_at": stamp(r["visit_date"]), "updated_at": stamp(r["visit_date"])}


def payment_payload(r: dict, patient_id: int | None, user_id: int) -> dict | None:
    if not r.get("payment_uuid") or not r.get("amount_paid"):
        return None
    what = join_unique([r.get("service_point"), r.get("treatment_info")])
    return {"uuid": r["payment_uuid"], "receipt_no": f"PBL-R-{r['source_row']}", "payment_type": "cash",
            "payment_method": "CASH", "patient_id": patient_id, "amount": r["amount_paid"],
            "payment_date": iso(r["visit_date"]), "paid_at": stamp(r["visit_date"]),
            "reference": (what or "")[:100] or None,
            "notes": f"{SOURCE}, daily attendance row {r['source_row']}: {what or '-'}"
                     + ("" if patient_id else f" (patient {r['patient_name']} not found in V3)" if r.get("patient_name")
                        else " (no patient name in the register)"),
            "status": "completed", "is_deposit": 0, "processed_by": user_id, "created_by": user_id,
            "created_at": stamp(r["visit_date"]), "updated_at": stamp(r["visit_date"])}


# ── runner ─────────────────────────────────────────────────────────────────
class Run:
    def __init__(self, dry_run: bool, limit: int, workers: int, only_missing: bool = False):
        from pipelines_common import use_pipelines_dir
        use_pipelines_dir([TARGET])
        from v3_to_v3_client import V3Client
        import snowflake_to_v3_migration as s2v3
        self.dry_run, self.limit, self.workers, self.only_missing = dry_run, limit, workers, only_missing
        self.c = V3Client(TARGET)
        self.c.login()
        self.sf = s2v3._snowflake_connect()

    def rows(self, table: str, order: str) -> list[dict]:
        cur = self.sf.cursor()
        cur.execute(f"SELECT * FROM {SF}.{table} ORDER BY {order}")
        cols = [c[0].lower() for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def ids_by_uuid(self, svc: str, alias: str) -> dict[str, int]:
        return {str(r.get("uuid")).lower(): r["id"] for r in self.c.read_all(svc, alias, workers=8) if r.get("uuid")}

    def post(self, svc: str, alias: str, payloads: list[dict]) -> Counter:
        skipped = 0
        if self.only_missing:
            have = set(self.ids_by_uuid(svc, alias))
            before = len(payloads)
            payloads = [p for p in payloads if p["uuid"] not in have]
            skipped = before - len(payloads)
        if self.limit:
            payloads = payloads[:self.limit]
        if self.dry_run:
            for p in payloads[:2]:
                log.info("  dry-run %s/%s: %s", svc, alias, json.dumps(p, default=str)[:700])
            return Counter(would_post=len(payloads))
        res, lock = Counter(), threading.Lock()

        def one(p):
            r = self.c.insert(svc, alias, p, match_on="uuid")
            ok = r.ok and (r.json() or {}).get("success", True)
            with lock:
                res["ok" if ok else "failed"] += 1
                if not ok and res["failed"] <= 5:
                    log.error("  %s/%s %s: %s %s", svc, alias, p.get("uuid"), r.status_code, r.text[:300])
        with ThreadPoolExecutor(self.workers) as ex:
            list(ex.map(one, payloads))
        if skipped:
            res["already in V3 (skipped)"] = skipped
        return res

    def patient_ids(self) -> dict[str, int]:
        return self.ids_by_uuid("reception", "patient")

    def visit_ids(self) -> dict[str, tuple[int, int]]:
        if not hasattr(self, "_visits"):
            self._visits = {str(v.get("uuid")).lower(): (v["id"], v.get("patient"))
                            for v in self.c.read_all("evaluation", "visits", workers=8)}
        return self._visits

    def patients(self) -> dict:
        src = self.rows("PATIENTS", "PATIENT_NO")
        booked = {}
        for b in self.rows("CLINIC_BOOKINGS", "SOURCE_ROW"):
            if b.get("patient_uuid"):
                booked.setdefault(b["patient_uuid"], []).append(booking_note(b))
        return {"source": len(src), "with clinic booking": sum(1 for r in src if r["patient_uuid"] in booked),
                **self.post("reception", "patient", [patient_payload(r, booked.get(r["patient_uuid"], [])) for r in src])}

    def visits(self) -> dict:
        pat, src, payloads, why = self.patient_ids(), self.rows("VISITS", "VISIT_DATE, VISIT_KEY"), [], Counter()
        for r in src:
            pid = pat.get(r["patient_uuid"])
            if pid is None:
                why["patient not in V3"] += 1
                continue
            payloads.append(visit_payload(r, pid, self.c.facility_id))
        return {"source": len(src), **why, **self.post("evaluation", "visits", payloads)}

    def per_visit(self, svc: str, alias: str, build) -> dict:
        vis, src, payloads, why = self.visit_ids(), self.rows("VISITS", "VISIT_DATE, VISIT_KEY"), [], Counter()
        for r in src:
            v = vis.get(r["visit_uuid"])
            if v is None:
                why["visit not in V3"] += 1
                continue
            p = build(r, *v)
            if p is None:
                why["nothing to load"] += 1
                continue
            payloads.append(p)
        return {"source": len(src), **why, **self.post(svc, alias, payloads)}

    def vitals(self) -> dict:
        return self.per_visit("evaluation", "vitals", vitals_payload)

    def doctor_notes(self) -> dict:
        return self.per_visit("evaluation", "doctor_notes", doctor_note_payload)

    def payments(self) -> dict:
        pat, src, payloads, why = self.patient_ids(), self.rows("ATTENDANCE", "SOURCE_ROW"), [], Counter()
        for r in src:
            pid = pat.get(r["patient_uuid"]) if r.get("patient_uuid") else None
            p = payment_payload(r, pid, self.c.user_id)
            if p is None:
                why["no money on the line"] += 1
                continue
            why["no patient (posted unlinked)"] += pid is None
            payloads.append(p)
        return {"source": len(src), **why, **self.post("finance", "payment", payloads)}


    # ── reconcile: V3 org against PORTBELL_CLEAN (read-only) ──
    def reconcile(self) -> dict:
        """Counts, duplicate / missing uuids, broken links and money, V3 vs CLEAN. `problems` empty = all good."""
        def uuids(table, col):
            return {r[col.lower()] for r in self.rows(table, col) if r.get(col.lower())}
        want = {"reception/patient": uuids("PATIENTS", "PATIENT_UUID"), "evaluation/visits": uuids("VISITS", "VISIT_UUID"),
                "finance/payment": uuids("ATTENDANCE", "PAYMENT_UUID")}
        visits = self.rows("VISITS", "VISIT_KEY")
        want["evaluation/vitals"] = {vitals_payload(v, 0, 0)["uuid"] for v in visits if vitals_payload(v, 0, 0)}
        want["evaluation/doctor_notes"] = {doctor_note_payload(v, 0, 0)["uuid"] for v in visits if doctor_note_payload(v, 0, 0)}
        out, problems, got = {"models": {}}, [], {}
        for m, expected in want.items():
            svc, alias = m.split("/")
            got[m] = [r for r in self.c.read_all(svc, alias, workers=8)
                      if str(r.get("uuid")).lower() in expected]          # only Portbell's rows
            seen = Counter(str(r.get("uuid")).lower() for r in got[m])
            row = {"expected": len(expected), "in_v3": len(seen), "missing": len(expected - set(seen)),
                   "duplicates": sum(n - 1 for n in seen.values() if n > 1)}
            out["models"][m] = row
            if row["missing"] or row["duplicates"]:
                problems.append(f"{m}: {row['missing']} missing, {row['duplicates']} duplicated")
        pids = {p["id"] for p in got["reception/patient"]}
        vids = {v["id"] for v in got["evaluation/visits"]}
        links = {"visits without their patient": sum(1 for v in got["evaluation/visits"] if v.get("patient") not in pids),
                 "vitals without their visit": sum(1 for r in got["evaluation/vitals"] if r.get("visit_id") not in vids),
                 "notes without their visit": sum(1 for r in got["evaluation/doctor_notes"] if r.get("visit_id") not in vids)}
        out["broken_links"] = links
        problems += [f"{k}: {n}" for k, n in links.items() if n]
        lines = self.rows("ATTENDANCE", "SOURCE_ROW")
        v3_total = round(sum(float(p.get("amount") or 0) for p in got["finance/payment"]), 2)
        clean_total = round(sum(r["amount_paid"] or 0 for r in lines if r.get("payment_uuid")), 2)
        named = {r["payment_uuid"] for r in lines if r.get("payment_uuid") and r.get("patient_uuid")}
        out["payments"] = {"v3_total": v3_total, "clean_total": clean_total,
                           "named_but_unlinked": sum(1 for p in got["finance/payment"]
                                                     if str(p.get("uuid")).lower() in named and not p.get("patient_id"))}
        if v3_total != clean_total:
            problems.append(f"payments total {v3_total} in V3 vs {clean_total} in CLEAN")
        if out["payments"]["named_but_unlinked"]:
            problems.append(f"{out['payments']['named_but_unlinked']} payments of a named patient have no patient_id")
        out["problems"] = problems
        return out


def run(steps=STEPS, *, limit: int = 0, workers: int = 8, dry_run: bool = False, only_missing: bool = False,
        reconcile: bool = False) -> dict:
    """Post the steps (in STEPS order), optionally reconcile; writes output/portbell_to_v3/<time>.json.
    Called by main() and dags/portbell_moh204a_dag.py."""
    r = Run(dry_run, limit, workers, only_missing)
    report = {"target": TARGET, "org": r.c.organization_id, "facility": r.c.facility_id, "dry_run": dry_run,
              "limit": limit, "started": time.strftime("%Y-%m-%d %H:%M:%S"), "steps": {}}
    for s in STEPS:
        if s in steps:
            t0 = time.time()
            try:
                report["steps"][s] = dict(getattr(r, s)())
            except Exception as e:
                log.exception("%s failed", s)
                report["steps"][s] = {"error": f"{type(e).__name__}: {str(e)[:300]}"}
            report["steps"][s]["seconds"] = round(time.time() - t0)
            log.info("%s: %s", s, report["steps"][s])
    if reconcile:
        report["reconcile"] = r.reconcile()
        log.info("reconcile: %s", json.dumps(report["reconcile"], default=str))
    out = Path(__file__).resolve().parent / "output" / "portbell_to_v3"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"portbell-{time.strftime('%Y%m%d_%H%M%S')}.json"
    path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    report["report"] = str(path)
    log.info("report: %s", path)
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--steps", nargs="*", default=list(STEPS), choices=STEPS)
    ap.add_argument("--limit", type=int, default=0, help="post at most N rows per step (canary)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-missing", action="store_true", help="post only rows whose uuid V3 doesn't have yet")
    ap.add_argument("--reconcile", action="store_true", help="afterwards, compare V3 with PORTBELL_CLEAN")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rep = run(a.steps, limit=a.limit, workers=a.workers, dry_run=a.dry_run, only_missing=a.only_missing,
              reconcile=a.reconcile)
    failed = any("error" in s or s.get("failed") for s in rep["steps"].values())
    sys.exit(1 if failed or (rep.get("reconcile") or {}).get("problems") else 0)


if __name__ == "__main__":
    main()
