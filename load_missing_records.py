"""
load_missing_records.py — post the CSVs written by export_missing_records.py into a V3 organisation
through the model gateway (the SQL load path needs DB access; this needs only the gateway account).

Every row is an insert with match_on uuid, so a re-run updates rows already posted — never a second
copy. Each row ends up as one of:
  inserted        V3 accepted it
  duplicate       V3 already holds this record under another uuid (its unique key — invoice number,
                  admission number, name … — is taken): skipped, nothing to do
  parent missing  a link (patient, admission …) doesn't exist in V3: held; load the parent, re-run
  rejected        V3 refused the data (value too long / out of range …): written to rejects/<table>.csv
                  with V3's message, for fixing by hand

Used by dags/load_missing_records_dag.py (one task per table, parents first).
"""
from __future__ import annotations

import io
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger("load_missing_records")

# exported DB table → (service, gateway alias). The exporter writes <table>.model.json next to each
# CSV from now on; this covers exports made before that.
TABLE_MODEL: dict[str, tuple[str, str]] = {
    "evaluation_age_groups": ("evaluation", "age_groups"),
    "evaluation_diagnosis": ("evaluation", "diagnoses"),
    "evaluation_procedure_prices": ("evaluation", "procedure_prices"),
    "evaluation_ophthalmology_notes": ("evaluation", "ophthalmology_notes"),
    "evaluation_units": ("evaluation", "lab_test_units"),
    "procedure_categories": ("evaluation", "procedure_categories"),
    "vitals": ("evaluation", "vitals"),
    "visits": ("evaluation", "visits"),
    "doctor_notes": ("evaluation", "doctor_notes"),
    "investigation_result_details": ("evaluation", "investigation_result_details"),
    "evaluation_result_details_ranges": ("evaluation", "result_detail_ranges"),
    "inp_admissions": ("inpatient", "admission"),
    "inp_admission_requests": ("inpatient", "admission_request"),
    "inp_daily_charges_verifications": ("inpatient", "daily_charges_verification"),
    "inp_discharges": ("inpatient", "discharge"),
    "inp_discharge_requests": ("inpatient", "discharge_request"),
    "inp_notes": ("inpatient", "note"),
    "inp_vitals": ("inpatient", "vital"),
    "inp_blood_transfusions": ("inpatient", "blood_transfusion"),
    "insurance_companies": ("finance", "insurance_company"),
    "invoices": ("finance", "invoice"),
    "patient_accounts": ("finance", "patient_account"),
    "receipts": ("finance", "receipt"),
    "inv_product_categories": ("inventory", "product_category"),
    "inv_stock_movements": ("inventory", "stock_movement"),
    "inv_stocks": ("inventory", "stock"),
    "patient_random_notes": ("reception", "patient_random_note"),
    "visit_consultants": ("reception", "visit_consultant"),
    "visit_destinations": ("reception", "visit_destination"),
    "theatre_anaesthetist_checks": ("theatre", "theatre_anaesthetist_check"),
    "theatre_bookings": ("theatre", "theatre_booking"),
    "theatre_deposits": ("theatre", "theatre_deposit"),
    "theatre_doctor_checks": ("theatre", "theatre_doctor_check"),
    "theatre_nurse_checks": ("theatre", "theatre_nurse_check"),
    "theatre_operations": ("theatre", "theatre_operation"),
    "theatre_operation_medics": ("theatre", "theatre_operation_medic"),
    "theatre_operation_procedures": ("theatre", "theatre_operation_procedure"),
    "theatre_schedules": ("theatre", "theatre_schedule"),
}

# load order: 1 reference data, 2 records others hang off, 3 everything else
TIER: dict[str, int] = {
    "evaluation_age_groups": 1, "evaluation_units": 1, "procedure_categories": 1, "insurance_companies": 1,
    "inv_product_categories": 1, "evaluation_procedure_prices": 1, "theatre_deposits": 1,
    "visits": 2, "inp_admission_requests": 2, "inp_admissions": 2, "invoices": 2, "patient_accounts": 2,
    "theatre_bookings": 2, "theatre_schedules": 2, "theatre_operations": 2, "inp_discharge_requests": 2,
}

_DUPLICATE = re.compile(r"Duplicate entry", re.I)
_PARENT = re.compile(r"foreign key constraint fails|Column '\w+_id' cannot be null|No query results", re.I)


def tier_of(table: str) -> int:
    return TIER.get(table, 3)


def parse_csv(text: str) -> list[list]:
    """The exporter's format: every value "quoted" ("" escapes a quote); an unquoted NULL is null."""
    rows, row, field, in_q, quoted = [], [], [], False, False
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if in_q:
            if ch == '"':
                if i + 1 < n and text[i + 1] == '"':
                    field.append('"')
                    i += 1
                else:
                    in_q = False
            else:
                field.append(ch)
        elif ch == '"':
            in_q = quoted = True
        elif ch in ",\n":
            v = "".join(field)
            row.append(v if quoted else (None if v == "NULL" else v))
            field, quoted = [], False
            if ch == "\n":
                rows.append(row)
                row = []
        elif ch != "\r":
            field.append(ch)
        i += 1
    if field or row:
        v = "".join(field)
        row.append(v if quoted else (None if v == "NULL" else v))
        rows.append(row)
    return [r for r in rows if r and r != [None]]


def to_value(v):
    """CSV text → the value to post: V2's literal "NULL" text is null; JSON columns go as JSON."""
    if v is None or v == "NULL":
        return None
    if v[:1] in "[{":
        try:
            return json.loads(v)
        except ValueError:
            pass
    return v


def classify(status: int, text: str) -> str:
    if status < 300:
        return "inserted"
    if _DUPLICATE.search(text):
        return "duplicate"
    if _PARENT.search(text):
        return "parent missing"
    return "rejected"


def load_rows(client, service: str, alias: str, header: list[str], rows: list[list], *,
              workers: int = 8, drop: tuple[str, ...] = ("organization_id",)) -> tuple[dict, list[dict]]:
    """Post rows; returns (counts by outcome, [{row…, _outcome, _error}] for everything not inserted)."""
    counts = {"inserted": 0, "duplicate": 0, "parent missing": 0, "rejected": 0, "error": 0}
    misses: list[dict] = []
    lock = threading.Lock()

    def one(r):
        rec = {k: to_value(v) for k, v in zip(header, r) if k not in drop}
        outcome, msg = "error", ""
        for attempt in range(4):
            try:
                resp = client.insert(service, alias, rec, match_on="uuid")
            except Exception as e:                       # network / timeout: retry
                msg = str(e)[:300]
                time.sleep(3 * (attempt + 1))
                continue
            if resp.status_code >= 500 and "insert rejected" not in resp.text:
                msg = f"{resp.status_code} {resp.text[:300]}"
                time.sleep(3 * (attempt + 1))
                continue
            outcome, msg = classify(resp.status_code, resp.text), ("" if resp.ok else resp.text[:500])
            break
        with lock:
            counts[outcome] += 1
            if outcome != "inserted":
                misses.append({**dict(zip(header, r)), "_outcome": outcome, "_error": msg})
            done = sum(counts.values())
            if done % 1000 == 0:
                log.info("  %s/%s: %d / %d posted — %s", service, alias, done, len(rows), counts)

    with ThreadPoolExecutor(workers) as ex:
        list(ex.map(one, rows))
    return counts, misses


def misses_csv(misses: list[dict]) -> str:
    """rejects/<table>.csv: the rows not inserted, with outcome + V3's message."""
    import csv
    if not misses:
        return ""
    cols = list(dict.fromkeys(k for m in misses for k in m))
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols)
    w.writeheader()
    w.writerows(misses)
    return buf.getvalue()
