"""
v3_passthrough.py — post V3-shaped records (from_json_mappings_to_snowflake)
through snowflake_to_v3_migration, without the V2 transforms.

A facility loaded by from_json_mappings_to_snowflake already holds V3 records
in {NAME}_RAW.EVENTS_RAW: source_table = the V3 table (visits, prescriptions …),
module_source = the V3 service ("patient-evaluation-service"), namespace = the
V3 model. What's left to do before posting:

  model   V3 table → gateway alias, matched on (service, table) against the
          gateway's own model list (describe), cached per process.
  links   every link field is translated to the new V3 id via the facility's
          id map (keyed by the source row id), with the rules in LINKS:
            parent is one of this facility's tables → its V3 id; the record is
              held back until that parent is in V3 (the usual "waiting");
            parent not loaded for this facility (patients, invoices, wards …)
              → posted empty, never as a raw source id — except patient links,
              which hold the record back (a clinical row without its patient
              is useless);
            facility_id → the destination facility; organization_id → dropped.
          Fields ending in _id / _by that no rule covers are posted empty and
          reported, so a missing rule can't leak raw source ids into V3.
  order   tiers from the link graph — parents before children.
  skip    setup tables (users, roles, permissions, organisation, facility …):
          the destination org's own setup is used.

snowflake_to_v3_migration calls prepare() from discover_tables(), then for
each table uses transform key "v3:<table>" with transform().
"""
from __future__ import annotations

import logging
import re
import threading
from collections import defaultdict

import v2_to_v3_api_migration as v2v3

log = logging.getLogger("v3_passthrough")

PREFIX = "v3:"

# "v3 service" in the mappings → gateway service name
SERVICES = {"reception-service": "reception", "patient-evaluation-service": "evaluation",
            "core-service": "core", "finance-service": "finance", "inventory-service": "inventory",
            "inpatient-service": "inpatient", "theatre-service": "theatre", "dialysis-service": "dialysis"}

# The destination organisation's own setup is used for these — never posted.
SKIP_TABLES = {"users": "setup — the destination's own users are used",
               "roles": "setup", "permissions": "setup", "user_profiles": "setup (users)",
               "core_organizations": "setup — the destination organisation", "core_facilities": "setup",
               "notifications": "user notifications — not migrated"}

# link field → parent V3 table. Same name in every table unless (table, field) overrides it.
USERS = "users"
LINKS: dict[str, str] = {
    "patient_id": "patients", "patient": "patients",
    "visit_id": "visits", "visit": "visits",
    "created_by": USERS, "updated_by": USERS, "recorded_by": USERS, "requested_by": USERS,
    "confirmed_by": USERS, "cancelled_by": USERS, "doctor_id": USERS, "consultant_id": USERS,
    "user_id": USERS, "user": USERS, "reversed_by_id": USERS,
    "department_id": "core_departments",
    "gl_account_id": "gl_accounts", "parent_account_id": "gl_accounts", "gl_transaction_id": "gl_transactions",
    "investigation_id": "investigations", "prescription_id": "prescriptions",
    "product_id": "inv_products", "tax_category_id": "inv_tax_categories", "tax_id": "inv_tax_categories",
    "store_id": "inv_stores", "from_store_id": "inv_stores", "to_store_id": "inv_stores",
    "supplier_id": "inv_suppliers", "petty_cash_id": "petty_cash", "config_id": "core_pos_config",
    "appointment_category_id": "appointment_categories", "diagnosis_code_id": "icd11_codes",
    "evaluation_procedure_id": "procedures", "procedure_id": "procedures",
    "evaluation_payment_id": "payments",
    # parents this export never loads → always posted empty
    "invoice_id": "invoices", "unit_id": "inv_units", "payment_term_id": "inv_payment_terms",
    "sale_id": "inv_sales", "batch_id": "inv_batches", "receipt_id": "receipts",
    "preferred_ward_id": "wards", "destination_id": "service_destinations", "clinic_id": "clinics",
    "item_id": "items", "source_id": "sources",
}
LINK_OVERRIDES: dict[tuple[str, str], str] = {("core_departments", "parent_id"): "core_departments"}
HOLD_IF_MISSING = {"patients"}            # parent absent → hold the record, never post it unlinked
DROP_FIELDS = {"organization_id", "deleted_at", "source_schema"}

_lock = threading.Lock()
_catalog: dict[tuple[str, str], tuple[str, str]] | None = None   # (service, table) → (alias, class)
_tables: dict[str, dict] = {}       # transform key → {"table", "alias", "service", "links", "facility_tables"}
_unknown_links: dict[str, set] = defaultdict(set)


def is_passthrough(transform_key: str | None) -> bool:
    return bool(transform_key) and transform_key.startswith(PREFIX)


def _default_table(cls: str) -> str:
    """Laravel's default table name for a model class (snake_case plural)."""
    snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", cls.split("\\")[-1]).lower()
    if snake.endswith("y") and not snake.endswith(("ay", "ey", "oy", "uy")):
        return snake[:-1] + "ies"
    return snake if snake.endswith("s") else snake + "s"


def _gateway_catalog() -> dict[tuple[str, str], tuple[str, str]]:
    """(service, table) → (alias, class), from describe on every gateway model."""
    global _catalog
    with _lock:
        if _catalog is not None:
            return _catalog
        out = {}
        for alias, service in sorted(v2v3._alias_to_service.items()):
            try:
                r = v2v3._gateway_post(service, {"action": "describe", "model": alias}, timeout=60)
                d = r.json().get("data") or {} if r.ok else {}
            except Exception as e:
                log.warning("describe %s:%s failed: %s", service, alias, e)
                continue
            cls = d.get("class") or ""
            table = d.get("table") or _default_table(cls or alias)
            out.setdefault((service, table), (alias, cls))
            if cls:   # also by class name: the export names tables the Laravel way, the gateway may not
                out.setdefault((service, "class:" + cls.split("\\")[-1].lower()), (alias, cls))
        _catalog = out
        log.info("Gateway catalog: %d models across %d services", len(out), len({s for s, _ in out}))
        return out


def _links_for(table: str, fields: set[str]) -> dict[str, str]:
    return {f: LINK_OVERRIDES.get((table, f)) or LINKS[f] for f in fields
            if (table, f) in LINK_OVERRIDES or f in LINKS}


def prepare(facility: str, rows: list[tuple[str, str, list]], sample_fields: dict[str, set]) -> list[dict]:
    """discover_tables() entries for this facility's V3-shaped tables, and
    registers per-table aliases / services / FK rules / tiers with the
    migration. rows: (source_table, module_source, [namespace]);
    sample_fields: table → the payload field names seen in RAW."""
    import snowflake_to_v3_migration as m      # late: m imports this module

    catalog = _gateway_catalog()
    present = {t for t, _, _ in rows}
    entries, deps = [], {}
    for table, module_source, stored in rows:
        ns = next((x for x in stored if x), None)
        entry = {"table": table, "module": module_source, "namespace": ns, "v3": None, "transform": None}
        service = SERVICES.get(str(module_source).lower())
        hit = (catalog.get((service, table)) or catalog.get((service, "class:" + (ns or "").split("\\")[-1].lower()))
               if service else None)
        if table in SKIP_TABLES:
            entry["reason"] = SKIP_TABLES[table]
        elif not service:
            entry["reason"] = f"service {module_source!r} has no gateway"
        elif not hit:
            entry["reason"] = f"no gateway model for {service} table {table}"
        if entry.get("reason"):
            entries.append(entry)
            continue
        alias, cls = hit
        key = PREFIX + table
        fields = set(sample_fields.get(table, set())) - {"id"}
        links = _links_for(table, fields)
        _unknown_links[table] = {f for f in fields - set(links) - DROP_FIELDS - {"facility_id"}
                                 if f.endswith(("_id", "_by"))}
        in_facility = {f: p for f, p in links.items() if p in present and p not in SKIP_TABLES}
        alias_of = {}
        for f, parent in in_facility.items():
            p_svc = SERVICES.get(_module_of(rows, parent), "")
            p_ns = next((x for t, _, st in rows if t == parent for x in st if x), "")
            p_hit = catalog.get((p_svc, parent)) or catalog.get((p_svc, "class:" + p_ns.split("\\")[-1].lower()))
            if p_hit:
                alias_of[f] = p_hit[0]
        _tables[key] = {"table": table, "alias": alias, "service": service, "links": links,
                        "resolvable": alias_of}
        m._ALIAS_OVERRIDE[key] = alias
        m._SERVICE_OVERRIDE[key] = service
        m._NO_V2_ID_ON_POST.add(alias)                  # the source id is the progress key, not a V3 id
        v2v3._FK_REMAP[key] = alias_of
        m._CRITICAL_FK_FIELDS[key] = list(alias_of)    # parent not in V3 yet → held back
        deps[table] = {in_facility[f] for f in alias_of} - {table}
        entry.update(v3=ns or cls, transform=key)
        entries.append(entry)
        if _unknown_links[table]:
            log.warning("[%s] %s: no link rule for %s — posted empty", facility, table,
                        ", ".join(sorted(_unknown_links[table])))
    tiers = _tiers(deps)
    for e in entries:
        if e["transform"]:
            e["tier"] = tiers.get(e["table"], 1)
    skipped = [e for e in entries if not e["transform"]]
    if skipped:
        log.warning("[%s] V3 pass-through skips %d table(s): %s", facility, len(skipped),
                    "; ".join(f"{e['table']} ({e['reason']})" for e in skipped))
    return entries


def _module_of(rows, table: str) -> str:
    return next((str(ms).lower() for t, ms, _ in rows if t == table), "")


def _tiers(deps: dict[str, set]) -> dict[str, int]:
    """1 + longest chain of parents (cycles broken: a table already on the path counts as tier 1)."""
    out: dict[str, int] = {}

    def depth(t, path):
        if t in out:
            return out[t]
        if t in path:
            return 1
        d = 1 + max((depth(p, path | {t}) for p in deps.get(t, ()) if p in deps), default=0)
        out[t] = d
        return d

    for t in deps:
        depth(t, frozenset())
    return {t: min(d, 6) for t, d in out.items()}


def transform(record: dict, transform_key: str, org_cfg: dict) -> dict | None:
    """V3 record from the CLEAN view → gateway payload. Links to parents in
    this facility stay as source ids here (the migration's FK remap turns them
    into V3 ids, or holds the record back); links to parents never loaded are
    emptied — or the record is dropped (held) when that parent is the patient."""
    spec = _tables[transform_key]
    out = {k: v for k, v in record.items() if k not in DROP_FIELDS}
    for field, parent in spec["links"].items():
        if field in spec["resolvable"] or out.get(field) in (None, ""):
            continue
        if parent in HOLD_IF_MISSING:
            return None          # counted as held back by the migration
        out[field] = None
    for field in _unknown_links.get(spec["table"], ()):
        out[field] = None
    if "facility_id" in out:
        out["facility_id"] = org_cfg.get("facility_id")
    return out
