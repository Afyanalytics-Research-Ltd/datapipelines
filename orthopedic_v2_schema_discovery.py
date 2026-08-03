#!/usr/bin/env python3
"""
orthopedic_v2_schema_discovery.py — data-governance gate for orthopedic_v2_*.

Purpose: before orthopedic_v2_clean_pipeline.py is allowed to mask/keep a
single field, we need to know what fields each of the 19 models actually
has. Guessing field names risks two opposite failures: leaking PII because a
guessed field name didn't match the real (differently-spelled) column, or
needlessly destroying clinical data by masking something that only looked
like PII. So: ask the API.

This script:
  1. Logs in (same credentials as the other v2 scripts).
  2. For each of the 19 namespaces, POSTs /api/gateway with
     {"namespace": ..., "action": "describe"} — internal admin mode, per the
     Postman collection ("DESCRIBE — table schema"), no facility_id/
     connection_id needed for describe.
  3. If describe returns no usable field list (some Laravel models only
     expose fillable/casts, not a full schema), falls back to fetching ONE
     sample row (action="get", per_page=1) and derives field names from its
     keys — still useful for classification purposes.
  4. Runs every discovered field name through a regex-based PII classifier
     (see PII_PATTERNS below) and buckets it into:
       - DIRECT_IDENTIFIER   → clean pipeline will HASH (SHA2) or NULL it
       - QUASI_IDENTIFIER    → kept (needed clinically), flagged for review
       - STAFF_IDENTIFIER    → kept (clinical accountability, not patient PII)
       - CLINICAL_CONTENT    → kept as-is (needed for analytics)
       - SYSTEM_META         → kept as-is (ids, timestamps, status, etc.)
       - UNKNOWN             → fail-safe default: HASH + flagged NEEDS_REVIEW
  5. Writes:
       schema_cache/<table>.json          — raw describe (or sample) response
       pii_classification_v2.json         — the machine-suggested classification,
                                             with "approved": false
       governance_report_v2.md            — human-readable summary to review

GOVERNANCE GATE
  orthopedic_v2_clean_pipeline.py refuses to run against a model until its
  entry in pii_classification_v2.json has "approved": true. This script only
  produces a SUGGESTION — a human (Martin / data governance owner) must open
  pii_classification_v2.json, correct any misclassified fields, and flip
  "approved" to true per model before the clean layer will touch it.

USAGE
  python orthopedic_v2_schema_discovery.py                 # discover all 19 models
  python orthopedic_v2_schema_discovery.py --models patients,triage
  python orthopedic_v2_schema_discovery.py --sample-fallback-per-page 3

ENV VARS — same .env as orthopedic_v2_raw_pipeline.py:
  AFYA_EXTRACTION_USERNAME, AFYA_EXTRACTION_PASSWORD, AFYA_EXTRACTION_BASE_URL
  AFYA_EXTRACTION_V2_CONNECTION_ID (default 20), AFYA_EXTRACTION_V2_FACILITY_ID (default 47)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

log = logging.getLogger("orthopedic_v2_schema_discovery")
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter(
        "%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S",
    ))
    log.addHandler(_h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ─── CONFIG ──────────────────────────────────────────────────────────────────

_base = os.getenv("AFYA_EXTRACTION_BASE_URL", "https://afyapi.afyaanalytics.ai/api").rstrip("/")
LOGIN_URL   = f"{_base}/auth/login"
GATEWAY_URL = f"{_base}/gateway"
CONNECTION_ID = int(os.getenv("AFYA_EXTRACTION_V2_CONNECTION_ID", "20"))
FACILITY_ID   = int(os.getenv("AFYA_EXTRACTION_V2_FACILITY_ID", "47"))

HERE               = Path(__file__).resolve().parent
SCHEMA_CACHE_DIR    = HERE / "schema_cache"
CLASSIFICATION_FILE = HERE / "pii_classification_v2.json"
REPORT_FILE         = HERE / "governance_report_v2.md"

# Same 19 models as orthopedic_v2_raw_pipeline.py. Kept as an independent
# literal list (not imported) so this script has zero dependency on
# boto3/snowflake-connector — it should run anywhere `requests` is installed.
MODELS: list[dict] = [
    {"namespace": r"App\Models\Admonotes",       "table": "admnotes"},
    {"namespace": r"App\Models\Cadex",            "table": "cadex"},
    {"namespace": r"App\Models\History",          "table": "history"},
    {"namespace": r"App\Models\ICD10diagnosis",   "table": "icd10diagnosis"},
    {"namespace": r"App\Models\ICD10diseases",    "table": "icd10diseases"},
    {"namespace": r"App\Models\Impression",       "table": "impression"},
    {"namespace": r"App\Models\Inpatients",       "table": "inpatients"},
    {"namespace": r"App\Models\Labrequests",      "table": "labrequests"},
    {"namespace": r"App\Models\Labtestresults",   "table": "labtestresults"},
    {"namespace": r"App\Models\Newprescription",  "table": "newprescription"},
    {"namespace": r"App\Models\Patientsmodel",    "table": "patients"},
    {"namespace": r"App\Models\Pharmrequests",    "table": "pharmrequests"},
    {"namespace": r"App\Models\phyexam",          "table": "phyexam"},
    {"namespace": r"App\Models\Physical",         "table": "physical"},
    {"namespace": r"App\Models\Procrequests",     "table": "procrequests"},
    {"namespace": r"App\Models\Progress",         "table": "progress"},
    {"namespace": r"App\Models\Radrequests",      "table": "radrequests"},
    {"namespace": r"App\Models\Theatrequests",    "table": "theatrequests"},
    {"namespace": r"App\Models\Triage",           "table": "triage"},
]

# ─── PII CLASSIFIER ───────────────────────────────────────────────────────────
# Order matters — first matching category wins. This mirrors the discretion
# the original orthopedic_api_to_snowflake.py team exercised by hand
# (mask fields tied to a PATIENT/SUBJECT/CUSTOMER identity — name, phone,
# nokName, nokPhone, email, subjectName; leave STAFF/system records like
# Users2, Report, Shift, SystemLog untouched).

PII_PATTERNS: list[tuple[str, str]] = [
    # category, regex (case-insensitive, matched against the bare field name)
    # STAFF/CLINICIAN checks come FIRST and deliberately win over the generic
    # "*name$" pattern below — otherwise "doctorName"/"staffName"/"nurseName"
    # would get hashed as if they were patient identifiers. Patient-side name
    # variants (guardianName, nokName, patientName, ...) don't match these
    # staff-role keywords, so they still fall through to DIRECT_IDENTIFIER.
    ("STAFF_IDENTIFIER", r"doctor|clinician|physician|surgeon|nurse"),
    ("STAFF_IDENTIFIER", r"requested.?by|created.?by|updated.?by|attended.?by|performed.?by|prescrib(ed|er)"),
    ("STAFF_IDENTIFIER", r"staff"),
    ("DIRECT_IDENTIFIER", r"(full|first|last|middle|other|maiden)?name$"),
    ("DIRECT_IDENTIFIER", r"surname"),
    ("DIRECT_IDENTIFIER", r"guardian"),
    ("DIRECT_IDENTIFIER", r"nok[_]?(name|phone|contact|email|address)?"),
    ("DIRECT_IDENTIFIER", r"next.?of.?kin"),
    ("DIRECT_IDENTIFIER", r"emergency.?contact"),
    ("DIRECT_IDENTIFIER", r"(mobile|cell)(no|number)?$"),
    ("DIRECT_IDENTIFIER", r"tel(ephone)?(no|number)?$"),
    ("DIRECT_IDENTIFIER", r"^phone"),
    ("DIRECT_IDENTIFIER", r"e?mail"),
    ("DIRECT_IDENTIFIER", r"(physical|postal|home)?address"),
    ("DIRECT_IDENTIFIER", r"national.?id"),
    ("DIRECT_IDENTIFIER", r"id.?(no|number)$"),
    ("DIRECT_IDENTIFIER", r"passport"),
    ("DIRECT_IDENTIFIER", r"huduma"),
    ("DIRECT_IDENTIFIER", r"nhif"),
    ("DIRECT_IDENTIFIER", r"insurance.?(no|number)"),
    ("DIRECT_IDENTIFIER", r"bank|account.?(no|number)"),
    ("DIRECT_IDENTIFIER", r"signature"),
    ("DIRECT_IDENTIFIER", r"photo|image|avatar"),
    ("DIRECT_IDENTIFIER", r"biometric|fingerprint"),
    ("DIRECT_IDENTIFIER", r"ip.?address"),
    ("DIRECT_IDENTIFIER", r"username|login"),
    # quasi-identifiers: usually clinically necessary, keep but flag
    ("QUASI_IDENTIFIER", r"dob$|date.?of.?birth|birth.?date"),
    ("QUASI_IDENTIFIER", r"^age$"),
    ("QUASI_IDENTIFIER", r"gender|^sex$"),
    ("QUASI_IDENTIFIER", r"marital"),
    ("QUASI_IDENTIFIER", r"occupation"),
    ("QUASI_IDENTIFIER", r"nationality"),
    ("QUASI_IDENTIFIER", r"county|sub.?county|ward$|village"),
    # clinical content — keep, this is the analytical payload
    ("CLINICAL_CONTENT", r"diagnos|icd"),
    ("CLINICAL_CONTENT", r"note|impression|history|complaint|symptom|finding"),
    ("CLINICAL_CONTENT", r"exam|vital|\bbp\b|temp|pulse|weight|height|spo2|resp"),
    ("CLINICAL_CONTENT", r"prescription|drug|dosage|medication|frequency|route"),
    ("CLINICAL_CONTENT", r"procedure|result|specimen|triage|progress|theatre|radiology"),
    ("CLINICAL_CONTENT", r"reason|remarks?|comment|instruction"),
    # system / structural metadata — keep
    ("SYSTEM_META", r"^id$|_id$"),
    ("SYSTEM_META", r"created_at|updated_at|deleted_at"),
    ("SYSTEM_META", r"^status$|type$|^code$|facility|ward$|bed"),
]

_COMPILED = [(cat, re.compile(pat, re.IGNORECASE)) for cat, pat in PII_PATTERNS]

# Default handling per category — this is what orthopedic_v2_clean_pipeline.py
# consults (via the approved pii_classification_v2.json) when building masking SQL.
CATEGORY_ACTION = {
    "DIRECT_IDENTIFIER": "HASH",      # SHA2 pseudonymize (or NULL for free-form contact fields)
    "QUASI_IDENTIFIER":  "KEEP",      # clinically necessary; consider generalizing DOB->year if policy requires
    "STAFF_IDENTIFIER":  "KEEP",      # clinical accountability, not patient PII
    "CLINICAL_CONTENT":  "KEEP",      # the analytical payload
    "SYSTEM_META":       "KEEP",
    "UNKNOWN":           "HASH",      # fail-safe: mask anything we can't classify
}

def classify_field(field_name: str) -> tuple[str, str]:
    """Return (category, action) for a single field name."""
    for cat, rx in _COMPILED:
        if rx.search(field_name):
            return cat, CATEGORY_ACTION[cat]
    return "UNKNOWN", CATEGORY_ACTION["UNKNOWN"]

# ─── HTTP ─────────────────────────────────────────────────────────────────────

_session = requests.Session()
_token: str | None = None

def _login() -> str:
    global _token
    username = os.getenv("AFYA_EXTRACTION_USERNAME")
    password = os.getenv("AFYA_EXTRACTION_PASSWORD")
    if not username or not password:
        raise RuntimeError(
            "Missing AFYA_EXTRACTION_USERNAME / AFYA_EXTRACTION_PASSWORD in .env"
        )
    r = _session.post(
        LOGIN_URL,
        json={"username": username, "password": password},
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"Login failed ({r.status_code}): {r.text[:300]}")
    token = r.json().get("token")
    if not token:
        raise RuntimeError(f"Login returned no token: {r.text[:300]}")
    _token = token
    return token

def _headers() -> dict:
    global _token
    if not _token:
        _login()
    return {
        "Authorization": f"Bearer {_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def _post_gateway(body: dict, retry_on_401: bool = True) -> requests.Response:
    r = _session.post(GATEWAY_URL, headers=_headers(), json=body, timeout=60)
    if r.status_code == 401 and retry_on_401:
        _login()
        r = _session.post(GATEWAY_URL, headers=_headers(), json=body, timeout=60)
    return r

def describe_model(namespace: str) -> dict | None:
    """POST {"namespace":..., "action":"describe"} — internal admin mode."""
    r = _post_gateway({"namespace": namespace, "action": "describe"})
    if not r.ok:
        log.warning("  describe(%s) → HTTP %s: %s", namespace, r.status_code, r.text[:200])
        return None
    try:
        return r.json()
    except Exception:
        log.warning("  describe(%s) → non-JSON response: %s", namespace, r.text[:200])
        return None

def sample_row(namespace: str, connection_id: int, facility_id: int, per_page: int = 3) -> dict | None:
    """Fallback: pull a tiny sample of live rows and derive field names from their keys."""
    body = {
        "namespace": namespace,
        "action": "get",
        "connection_id": connection_id,
        "facility_id": facility_id,
        "page": 1,
        "per_page": per_page,
    }
    r = _post_gateway(body)
    if not r.ok:
        log.warning("  sample(%s) → HTTP %s: %s", namespace, r.status_code, r.text[:200])
        return None
    try:
        return r.json()
    except Exception:
        return None

def _extract_field_names(describe_payload: dict | None, sample_payload: dict | None) -> list[str]:
    """Best-effort extraction of field names from either a describe response
    or a sample-row fallback. The gateway's exact describe shape isn't
    documented in the Postman collection (empty response array), so this
    tries several common shapes defensively."""
    names: list[str] = []

    def _from_columns(cols) -> list[str]:
        out = []
        for c in cols:
            if isinstance(c, str):
                out.append(c)
            elif isinstance(c, dict):
                n = c.get("name") or c.get("field") or c.get("column")
                if n:
                    out.append(n)
        return out

    if isinstance(describe_payload, dict):
        for key in ("columns", "fields", "fillable", "schema"):
            val = describe_payload.get(key)
            if isinstance(val, list):
                names.extend(_from_columns(val))
            elif isinstance(val, dict):
                names.extend(val.keys())
        data = describe_payload.get("data")
        if isinstance(data, dict):
            for key in ("columns", "fields", "fillable"):
                val = data.get(key)
                if isinstance(val, list):
                    names.extend(_from_columns(val))

    if not names and isinstance(sample_payload, dict):
        rows = sample_payload.get("data")
        if isinstance(rows, dict):
            rows = rows.get("data")
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            names.extend(rows[0].keys())

    # de-dupe, preserve order
    seen = set()
    out = []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out

# ─── MAIN DISCOVERY LOOP ──────────────────────────────────────────────────────

def discover(models: list[dict], sample_per_page: int = 3) -> dict:
    SCHEMA_CACHE_DIR.mkdir(exist_ok=True)
    classification: dict = _load_existing_classification()
    classification.setdefault("generated_at", None)
    classification.setdefault("connection_id", CONNECTION_ID)
    classification.setdefault("facility_id", FACILITY_ID)
    classification.setdefault("models", {})

    for m in models:
        ns, table = m["namespace"], m["table"]
        log.info("── Describing %-25s (table=%s)", ns.split("\\")[-1], table)
        describe_payload = None
        try:
            describe_payload = describe_model(ns)
        except Exception as e:
            log.warning("  describe(%s) raised: %s", ns, e)

        field_names = _extract_field_names(describe_payload, None)

        sample_payload = None
        if not field_names:
            log.info("  describe gave no field list — falling back to a %d-row sample", sample_per_page)
            try:
                sample_payload = sample_row(ns, CONNECTION_ID, FACILITY_ID, sample_per_page)
            except Exception as e:
                log.warning("  sample(%s) raised: %s", ns, e)
            field_names = _extract_field_names(None, sample_payload)

        (SCHEMA_CACHE_DIR / f"{table}.json").write_text(json.dumps(
            {"namespace": ns, "table": table,
             "describe_response": describe_payload,
             "sample_response": sample_payload,
             "field_names_found": field_names},
            indent=2, default=str,
        ))

        if not field_names:
            log.warning(
                "  ✗ %-25s no fields discovered (describe and sample both empty/failed) — "
                "clean pipeline will REFUSE this model until schema_cache/%s.json has data.",
                table, table,
            )

        existing_model_entry = classification["models"].get(table, {})
        existing_fields = existing_model_entry.get("fields", {})

        fields_out = {}
        needs_review = []
        for fname in field_names:
            cat, action = classify_field(fname)
            prior = existing_fields.get(fname)
            entry = {
                "category": cat,
                "action": action,
                "suggested_by": "regex_classifier",
            }
            # Preserve a human's prior manual override if present
            if prior and prior.get("reviewed"):
                entry = prior
            else:
                entry["reviewed"] = False
            if cat == "UNKNOWN":
                needs_review.append(fname)
            fields_out[fname] = entry

        classification["models"][table] = {
            "namespace": ns,
            "fields": fields_out,
            "needs_review": needs_review,
            # Governance gate consulted by orthopedic_v2_clean_pipeline.py —
            # stays false until a human reviews `fields` above and flips this.
            "approved": existing_model_entry.get("approved", False),
            "discovered_at": datetime.now(timezone.utc).isoformat(),
            "field_source": "describe" if describe_payload and field_names else (
                "sample" if field_names else "none"
            ),
        }

    classification["generated_at"] = datetime.now(timezone.utc).isoformat()
    CLASSIFICATION_FILE.write_text(json.dumps(classification, indent=2, sort_keys=False))
    _write_report(classification)
    return classification

def _load_existing_classification() -> dict:
    if CLASSIFICATION_FILE.exists():
        try:
            return json.loads(CLASSIFICATION_FILE.read_text())
        except Exception:
            log.warning("Could not parse existing %s — starting fresh.", CLASSIFICATION_FILE)
    return {}

def _write_report(classification: dict) -> None:
    lines = [
        "# Orthopedic V2 — PII Classification Report (machine-suggested)",
        "",
        f"Generated: {classification.get('generated_at')}",
        f"connection_id={classification.get('connection_id')}  facility_id={classification.get('facility_id')}",
        "",
        "**This is a SUGGESTION, not an approval.** Every model stays gated "
        "(`approved: false`) in `pii_classification_v2.json` until a human "
        "reviews its field list below — especially anything under NEEDS REVIEW "
        "— and flips `approved` to `true`. `orthopedic_v2_clean_pipeline.py` "
        "will refuse to process a model that isn't approved.",
        "",
    ]
    for table, m in classification.get("models", {}).items():
        lines.append(f"## {table}  ({m['namespace']})")
        lines.append(f"- approved: **{m['approved']}**")
        lines.append(f"- field source: {m['field_source']}")
        if not m["fields"]:
            lines.append("- ⚠️ NO FIELDS DISCOVERED — describe and sample both failed/empty.")
        else:
            lines.append("")
            lines.append("| field | category | action |")
            lines.append("|---|---|---|")
            for fname, info in m["fields"].items():
                flag = " ⚠️ NEEDS REVIEW" if info["category"] == "UNKNOWN" else ""
                lines.append(f"| {fname} | {info['category']} | {info['action']}{flag} |")
        lines.append("")
    REPORT_FILE.write_text("\n".join(lines))
    log.info("Wrote %s and %s", CLASSIFICATION_FILE.name, REPORT_FILE.name)

# ─── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--models", help="Comma-separated table names to (re)discover. Omit for all 19.")
    ap.add_argument("--sample-fallback-per-page", type=int, default=3,
                     help="Rows to pull when describe returns no field list (default 3).")
    args = ap.parse_args()

    models = MODELS
    if args.models:
        wanted = {t.strip().lower() for t in args.models.split(",") if t.strip()}
        models = [m for m in MODELS if m["table"] in wanted]
        if not models:
            log.error("No matching models for --models %s", args.models)
            sys.exit(1)

    discover(models, sample_per_page=args.sample_fallback_per_page)
    log.info(
        "Done. Review %s (and %s for a human-readable summary), "
        "then set \"approved\": true per model before running orthopedic_v2_clean_pipeline.py.",
        CLASSIFICATION_FILE.name, REPORT_FILE.name,
    )


if __name__ == "__main__":
    main()