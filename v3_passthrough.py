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
          User links (created_by, doctor_id, user …) resolve through the users
          this facility brought along — by source id, or by username where the
          source stores one ("admin"); otherwise created_by is the migrating
          account and the rest are posted empty. They never hold a record back,
          except a user profile's own user_id.
  users   source users → V3 users of the destination org: email made
          undeliverable and unique ("<email>.<facility>-<id>.invalid"), random
          password with a forced reset (source hashes can't be migrated),
          username suffixed ".<facility>" only if the org already has it.
  order   tiers from the link graph — parents (users first) before children.
  skip    setup tables (roles, permissions, organisation, facility …):
          the destination org's own setup is used.

snowflake_to_v3_migration calls prepare() from discover_tables(), then for
each table uses transform key "v3:<table>" with transform().
"""
from __future__ import annotations

import logging
import secrets
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
SKIP_TABLES = {"roles": "setup — the destination's own roles are used", "permissions": "setup",
               "core_organizations": "setup — the destination organisation", "core_facilities": "setup",
               "notifications": "user notifications — not migrated",
               "core_settings": "the source system's own settings — not meaningful in V3, could clash with V3 keys",
               "icd11_codes": "reference list V3 already has — would duplicate it per organisation",
               "queue": "live check-in queue — the queue screens read visits, not this table"}

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
# NOT NULL user columns: when the original user can't be found in V3, record
# the migrating V3 account instead of posting NULL.
USER_FALLBACK_FIELDS = {"created_by"}
# user links that DO hold their record back until that user is in V3
CRITICAL_USER_LINKS = {("user_profiles", "user_id")}
# NOT NULL columns V3 can't derive: a record without them is held back (not
# posted, so no 500) until the mappings supply them.
REQUIRED: dict[str, set[str]] = {
    "credit_notes": {"total_amount"},
    "prescriptions": {"visit"},                       # the tool currently sends the row id as visit
    "evaluation_investigation_approvals": {"investigation_id", "status_id"},
    "evaluation_investigation_result_details": {"investigation_result_id"},
    "orthopedic_surgical_consents": {"visit_id"},
    "patient_documents": {"file_path", "document_type"},   # the file itself isn't migrated
    "gl_accounts": {"account_type_id", "account_group_id"},
    "gl_transactions": {"transaction_type"},
    "gl_transaction_lines": {"amount", "entry_type"},
    "petty_cash": {"fund_name", "fund_code"},
    "petty_cash_transactions": {"transaction_type", "balance_before", "balance_after"},
    "inv_products": {"category_id"},
    "inv_sale_items": {"product_name"},
    "user_profiles": {"first_name", "last_name"},
}
_held_missing: dict[str, int] = defaultdict(int)
# patient_id from the parent record when the source row doesn't carry it:
# table → (link field on this record, parent table's CLEAN view, its patient column)
PATIENT_FROM_PARENT: dict[str, tuple[str, str, str]] = {
    "evaluation_requested_samples": ("visit_id", "visits", "patient"),
    "investigation_results": ("investigation_id", "investigations", "patient_id"),
}
# patients without a number (the tool doesn't send person_number yet) get an
# obviously-migrated one, never a number that could be mistaken for a real one
PATIENT_NO_PREFIX = "MIG"

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
        if table in PATIENT_FROM_PARENT:
            fields.add("patient_id")          # filled from the parent record in transform()
        links = _links_for(table, fields)
        _unknown_links[table] = {f for f in fields - set(links) - DROP_FIELDS - {"facility_id"}
                                 if f.endswith(("_id", "_by"))}
        in_facility = {f: p for f, p in links.items() if p in present and p not in SKIP_TABLES}
        # user links are resolved in transform() (by id or username, with a
        # fallback) — only the critical ones go through the migration's FK remap
        user_links = {f for f, p in in_facility.items() if p == USERS and (table, f) not in CRITICAL_USER_LINKS}
        alias_of = {}
        for f, parent in in_facility.items():
            if f in user_links:
                continue
            p_svc = SERVICES.get(_module_of(rows, parent), "")
            p_ns = next((x for t, _, st in rows if t == parent for x in st if x), "")
            p_hit = catalog.get((p_svc, parent)) or catalog.get((p_svc, "class:" + p_ns.split("\\")[-1].lower()))
            if p_hit:
                alias_of[f] = p_hit[0]
        users_hit = catalog.get(("core", USERS)) or catalog.get(("core", "class:user"))
        _tables[key] = {"table": table, "alias": alias, "service": service, "links": links,
                        "resolvable": alias_of, "user_links": user_links, "facility": facility,
                        "users_alias": users_hit[0] if users_hit else USERS,
                        # the gateway's facility column for this model (None: the model has none)
                        "facility_col": (v2v3._gateway_model_meta.get(alias) or {}).get("facility")}
        m._ALIAS_OVERRIDE[key] = alias
        m._SERVICE_OVERRIDE[key] = service
        m._NO_V2_ID_ON_POST.add(alias)                  # the source id is the progress key, not a V3 id
        v2v3._FK_REMAP[key] = alias_of
        m._CRITICAL_FK_FIELDS[key] = list(alias_of)    # parent not in V3 yet → held back
        deps[table] = ({in_facility[f] for f in alias_of} | ({USERS} if user_links else set())) - {table}
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
    if spec["table"] in PATIENT_FROM_PARENT and out.get("patient_id") in (None, ""):
        out["patient_id"] = _patient_from_parent(spec, out)
    if spec["table"] == "patients" and out.get("patient_no") in (None, ""):
        out["patient_no"] = f"{PATIENT_NO_PREFIX}-{spec['facility'].upper()}-{out.get('id')}"
    missing = [f for f in REQUIRED.get(spec["table"], ()) if out.get(f) in (None, "")]
    if missing:
        with _lock:
            _held_missing[spec["table"]] += 1
            first = _held_missing[spec["table"]] == 1
        if first:
            log.warning("%s: no %s in the mappings/source — records held back until it is mapped",
                        spec["table"], ", ".join(missing))
        return None
    if spec["table"] == USERS:
        return _user_payload(out, spec, org_cfg)
    for field, parent in spec["links"].items():
        if field in spec["resolvable"] or field in spec["user_links"] or out.get(field) in (None, ""):
            continue
        if parent in HOLD_IF_MISSING:
            return None          # counted as held back by the migration
        out[field] = org_cfg.get("user_id") if field in USER_FALLBACK_FIELDS and parent == USERS else None
    for field in spec["user_links"]:
        v3_user = _v3_user(out.get(field), spec)
        out[field] = v3_user if v3_user is not None else (
            org_cfg.get("user_id") if field in USER_FALLBACK_FIELDS else None)
    for field in USER_FALLBACK_FIELDS:      # mapped but empty in the source
        if field in out and out[field] in (None, "") and spec["links"].get(field) == USERS:
            out[field] = org_cfg.get("user_id")
    for field in _unknown_links.get(spec["table"], ()):
        out[field] = None
    # facility: only on models the gateway scopes by facility (it sets the
    # destination facility itself); other tables may have no facility column
    # at all, and posting one there is a 500
    out.pop("facility_id", None)
    if spec.get("facility_col"):
        out[spec["facility_col"]] = org_cfg.get("facility_id")
    return out


_parent_patients: dict[tuple[str, str], dict] = {}     # (facility, parent view) → {source id: source patient id}


def _patient_from_parent(spec: dict, out: dict):
    link, parent, col = PATIENT_FROM_PARENT[spec["table"]]
    raw = out.get(link)
    if raw in (None, ""):
        return None
    key = (spec["facility"], parent)
    with _lock:
        cached = _parent_patients.get(key)
    if cached is None:
        import snowflake_to_v3_migration as m
        cached = {}
        try:
            with m._snowflake_connect() as conn:
                for pid, patient in conn.cursor().execute(
                        f"SELECT ID, {col.upper()} FROM {m.sf_schema(spec['facility'], 'CLEAN')}.{parent.upper()}").fetchall():
                    cached[str(pid)] = patient
        except Exception as e:
            log.warning("[%s] could not read %s for patient links: %s", spec["facility"], parent, str(e)[:120])
        with _lock:
            _parent_patients[key] = cached
    return cached.get(str(raw))


# ─── users ───────────────────────────────────────────────────────────────

_source_usernames: dict[str, dict[str, int]] = {}     # facility → {username: source id}
_org_users: dict[int, dict[str, str]] = {}            # org → {username: email} already in V3


def _source_username_map(facility: str) -> dict[str, int]:
    """username → source user id, from the facility's CLEAN users view (some
    tables store the creator's username, e.g. created_by = "admin")."""
    with _lock:
        if facility in _source_usernames:
            return _source_usernames[facility]
    import snowflake_to_v3_migration as m
    out: dict[str, int] = {}
    try:
        with m._snowflake_connect() as conn:
            for sid, uname in conn.cursor().execute(
                    f"SELECT ID, USERNAME FROM {m.sf_schema(facility, 'CLEAN')}.USERS").fetchall():
                if uname and sid is not None:
                    out[str(uname).strip().lower()] = int(sid)
    except Exception as e:
        log.warning("[%s] couldn't read the users view for username links: %s", facility, str(e)[:120])
    with _lock:
        _source_usernames[facility] = out
    return out


def _v3_user(raw, spec: dict):
    """V3 user id for a source user reference (source id or username), or None."""
    if raw in (None, ""):
        return None
    import snowflake_to_v3_migration as m
    id_map = m._id_map_for(spec["users_alias"])
    text = str(raw).strip()
    sid = int(text) if text.isdigit() else _source_username_map(spec["facility"]).get(text.lower())
    if sid is None:
        return None
    return id_map.get(sid) or id_map.get(str(sid))


def _existing_org_users(org_cfg: dict, alias: str) -> dict[str, str]:
    org = org_cfg.get("organization_id")
    with _lock:
        if org in _org_users:
            return _org_users[org]
    try:
        rows = v2v3._fetch_v3_records(alias, org_cfg, service_name="core")
    except Exception as e:
        log.warning("couldn't read the org's V3 users (%s) — usernames posted as they are", str(e)[:120])
        rows = []
    found = {str(r.get("username") or "").lower(): str(r.get("email") or "").lower() for r in rows}
    with _lock:
        _org_users[org] = found
    return found


def _user_payload(out: dict, spec: dict, org_cfg: dict) -> dict | None:
    sid = out.get("id")
    if sid in (None, ""):
        return None
    facility = spec["facility"]
    username = str(out.get("username") or f"user{sid}").strip()
    email = str(out.get("email") or "").strip()
    base = email if "@" in email else f"{username}@migrated.{facility}"
    email = f"{base}.{facility}-{sid}.invalid"            # undeliverable, unique per facility + source id
    taken = _existing_org_users(org_cfg, spec["alias"])
    owner = taken.get(username.lower())
    if owner is not None and owner != email.lower():    # someone else in the org has this username
        username = f"{username}.{facility}"
    active = out.get("active")
    payload = {"id": sid, "username": username, "email": email,
               "password": secrets.token_urlsafe(18), "enforce_password_reset": 1,
               "active": 1 if active in (None, "") else int(str(active).lower() in ("1", "true", "yes")),
               "last_login": out.get("last_login")}
    if out.get("employee_number") not in (None, ""):
        payload["employee_number"] = str(out["employee_number"])
    return payload
