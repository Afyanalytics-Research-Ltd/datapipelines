"""
gyno_v3_reconcile.py — every table of the Gyno Solutions HMS (its Snowflake mirror
HOSPITALS.GYNOSOLUTIONS_HISTORICAL_DATA) against V3 migrate org 6, row by row: each V3 row gyno_to_v3.py writes
carries the stable uuid5 of its source table + id, so a source row is "in V3" exactly when its uuid is there.

Writes output/gyno_to_v3/gyno_v3_reconciliation_<date>.csv:
  source_table, source_rows, v3_target, in_v3, missing, pct_in_v3, status, note

    python gyno_v3_reconcile.py
"""
from __future__ import annotations

import csv
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, "/opt/airflow/dags")
log = logging.getLogger("gyno_v3_reconcile")

# source table → [(service, model, uuid key)] — the V3 rows made from it (gyno_to_v3.gid(key, source id))
LOADED = {
    "patients": [("reception", "patient", "patients")],
    "visits": [("evaluation", "visits", "visits")],
    "triage": [("evaluation", "vitals", "triage")],
    "doctorsnotes": [("evaluation", "doctor_notes", "doctorsnotes")],
    "discharge": [("evaluation", "doctor_notes", "discharge")],
    "doctorsnotesfornurses": [("evaluation", "doctor_notes", "doctorsnotesfornurses")],
    "inpatientexternalprocedures": [("evaluation", "doctor_notes", "inpatientexternalprocedures")],
    "nursecardex": [("inpatient", "note", "nursecardex")],
    "edd": [("evaluation", "anc_profiles", "edd")],
    "laboratory": [("evaluation", "procedures", "laboratory"), ("evaluation", "procedure_prices", "laboratory-price")],
    "otherservices": [("evaluation", "procedures", "otherservices"), ("evaluation", "procedure_prices", "otherservices-price")],
    "otherprocedures": [("evaluation", "procedures", "otherprocedures"), ("evaluation", "procedure_prices", "otherprocedures-price")],
    "lab_requests": [("evaluation", "investigations", "lab_requests")],
    "services_records": [("evaluation", "investigations", "services_records")],
    "otherequests": [("evaluation", "investigations", "otherequests")],
    "pharmacy": [("inventory", "product", "pharmacy"), ("inventory", "stock", "pharmacy-stock")],
    "pharmacy_records": [("evaluation", "prescriptions", "pharmacy_records")],
    "stocks": [("inventory", "product", "stocks"), ("inventory", "stock", "stocks-stock")],
    "stock_requests": [("inventory", "stock_movement", "stock_requests")],
    "payments": [("finance", "evaluation_payment", "evaluation-payments"), ("finance", "receipt", "receipt-payments"), ("finance", "payment", "payments")],
    "inpatientdeposit": [("finance", "receipt", "receipt-inpatientdeposit"), ("finance", "payment", "inpatientdeposit")],    "generalpaymentdetails": [("finance", "invoice", "generalpaymentdetails")],
    "bookings": [("reception", "appointment", "bookings")],
    "users": [("core", "users", "users"), ("core", "user_profile", "user_profiles")],
    "preferedpayment": [("reception", "patient_insurance", "preferedpayment")],
    "externaltests": [("reception", "patient_document", "externaltests")],
    "scanneddocument": [("reception", "patient_document", "scanneddocument")],
    "suppliers": [("inventory", "supplier", "suppliers")],
    "lpos": [("inventory", "purchase_order", "lpos")],
    "order_products": [("inventory", "purchase_order_item", "order_products")],
    "deliveries": [("inventory", "goods_received_note", "deliveries"), ("inventory", "goods_received_item", "deliveries-item")],
}
# loaded as part of another row, or a lookup — no V3 row of their own
FOLDED = {
    "doctors": "lookup: attending doctor on visits (visit.user) and doctor names on notes",
    "paymentype": "lookup: payment method on payments/receipts; insurers → finance/insurance_company",
    "consultants": "no V3 model of its own (4 rows) — activity_log is read-only through the gateway",
}
# expected gaps, per source table
NOTES = {
    "visits": "9,854 point at no patient in Gyno; 13 future bookings",
    "triage": "vitals of visits not in V3",
    "payments": "every payment has a receipt (what V3's money screens show); 34k finance/payment rows refused (payment_type)",
    "preferedpayment": "only insurance preferences become patient insurance; cash / M-Pesa preferences have nothing to load",
    "generalpaymentdetails": "46 refused by V3; invoices of visits not in V3 kept unlinked",
    "externaltests": "records only — PDFs before June 2024 aren't in the S3 manifest",
    "otherequests": "free-text requests: no catalogue link in Gyno",
}
NO_HOME = "no writable V3 model (activity_log is read-only through the gateway) — kept in Snowflake"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from pipelines_common import use_pipelines_dir
    target = os.environ.get("GYNO_TARGET", "gyno")
    use_pipelines_dir([target])
    from v3_to_v3_client import V3Client
    import gyno_to_v3 as g
    import snowflake_to_v3_migration as s2v3
    c = V3Client(target)
    c.login()
    cur = s2v3._snowflake_connect().cursor()
    schema = g.SF.split(".")[1]
    cur.execute(f"SELECT LOWER(TABLE_NAME) FROM HOSPITALS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' ORDER BY 1")
    tables = [t for (t,) in cur.fetchall()]
    uuids: dict[tuple, set] = {}

    def v3_uuids(svc, alias):
        if (svc, alias) not in uuids:
            log.info("reading V3 %s/%s", svc, alias)
            uuids[(svc, alias)] = {str(r.get("uuid")).lower() for r in c.read_all(svc, alias, workers=8) if r.get("uuid")}
        return uuids[(svc, alias)]

    out = []
    for t in tables:
        cur.execute(f'SELECT "ID" FROM {g.SF}."{t.upper()}"')
        ids = [i for (i,) in cur.fetchall()]
        n = len(ids)
        if t in LOADED:
            for svc, alias, key in LOADED[t]:
                try:
                    have = v3_uuids(svc, alias)
                except Exception as e:          # e.g. production doesn't expose core/user_profile
                    out.append([t, n, f"{svc}/{alias}", "", "", "", "can't check",
                                f"V3 gateway: {str(e).split(': ', 1)[-1][:160]}"])
                    continue
                found = sum(1 for i in ids if g.gid(key, i) in have)
                miss = n - found
                pct = round(100 * found / n, 1) if n else 100.0
                status = "complete" if miss == 0 else ("complete (expected gaps)" if t in NOTES else "partial")
                out.append([t, n, f"{svc}/{alias}", found, miss, pct, status, NOTES.get(t, "")])
        elif n == 0:
            out.append([t, 0, "", 0, 0, 100.0, "empty in Gyno", "nothing to load"])
        elif t in FOLDED:
            out.append([t, n, "", "", "", "", "folded into other V3 rows", FOLDED[t]])
        else:
            out.append([t, n, "", 0, n, 0.0, "not in V3", NO_HOME])
    path = Path(__file__).resolve().parent / "output" / "gyno_to_v3" / f"gyno_v3_reconciliation_{target}_{time.strftime('%Y%m%d')}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source_table", "source_rows", "v3_target", "in_v3", "missing", "pct_in_v3", "status", "note"])
        w.writerows(out)
    log.info("wrote %s (%d rows)", path, len(out))
    print(path)


if __name__ == "__main__":
    main()
