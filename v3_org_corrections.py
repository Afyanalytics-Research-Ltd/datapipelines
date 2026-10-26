"""
v3_org_corrections.py — the corrections a V3 org needs after a V2 → V3 load, a V3 → V3 copy or a V2 delta,
each found on Kisumu (org 4 / org 284) 2026-10-16/17 and re-runnable at any time (every step compares
first and only writes what differs; --dry-run writes nothing):

  restore_destinations      service destinations the source org has and the target lost (missing, or
                            soft-deleted: the gateway's reads hide those, an insert by uuid only updates
                            them, so deleted_at is sent empty). A destination the target holds under
                            another uuid but the same name is left alone. Needs a source org (V3 → V3).
  destination_categories    service_destinations.associated_procedure_category is a list of procedure
                            category ids; the V2 load and the V3 → V3 copy carried V2's ("1,6,46"), which
                            point at nothing. Re-pointed to the org's own ids through V2's names.
                            (treatment_actions / procedures: V2 has no such link and the gateway can't
                            write them — set up in V3.)
  eval_visit_destinations   the frontend reads visit destinations from the EVALUATION service
                            (evaluation_visit_destinations), not reception's visit_destinations: V2's rows
                            loaded there (stable uuid5 per V2 id — the same in every org) and each one's
                            destination_id pointed at the org's current core_service_destinations row
                            (what V3 resolves it against: /setup/destinations/{id}). With relink, rows
                            already there whose destination was deleted or recreated are re-pointed.
  eval_consumables          V2 evaluation_consumables → the evaluation service's consumables (registered on the
                            gateway 2026-10-19), V2's dates kept; visit / product (item_no, name) / invoice
                            (invoice_no) / users linked to the org's own; stable uuid5 per V2 id.
  eval_investigation_links  investigations' empty procedure / user filled from V2 (procedure by code, user by
                            v2_user_lookup); rows paired by uuid5, else visit + type + price + quantity +
                            created_at (V3 is 3 h behind V2), else without the time; never overwrites.
  admission_discharges      V3's inpatient list shows an admission while it's 'admitted', with bed / ward
                            from its open ward stay (no discharge_date, no transfer_out_date). From the
                            live V2 API: discharged → status 'discharged' + discharge_date = V2 discharged_at
                            and its open stays closed; still in → 'admitted' and its latest stay re-opened
                            (or created from V2's ward / bed).
  facility_check            rows of the org on another facility than its own (org 284: 247 for 283 — the
                            gateway can't move them, facility_id isn't fillable): counted per table and an
                            SQL file written for the DBA (before / update / after per table).

    python v3_org_corrections.py --target org284 --source kisumu_v3 --dry-run
    python v3_org_corrections.py --target org284 --steps eval_visit_destinations admission_discharges
    python v3_org_corrections.py --target org284 --steps facility_check

Writes output/v3_org_corrections/<target>-<time>.json (each step's counts).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import pickle
import re
import sys
import tempfile
import threading
import time
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

sys.path.insert(0, "/opt/airflow/dags")
log = logging.getLogger("v3_org_corrections")

STEPS = ("restore_destinations", "destination_categories", "eval_visit_destinations", "eval_consumables",
         "eval_investigation_links", "admission_discharges", "facility_check")
VD_UUID_NS = uuid.UUID("6f1c2a52-3c1e-4c8e-9a51-0e5f0d6c7a10")   # never change: rows are matched by it
VD_NS = r"Ignite\Evaluation\Entities\VisitDestinations"
VISIT_NS = r"Ignite\Evaluation\Entities\Visit"
ADMISSION_NS = r"Ignite\Inpatient\Entities\Admission"
WARD_NS = r"Ignite\Inpatient\Entities\Ward"
BED_NS = r"Ignite\Inpatient\Entities\Bed"
PLACEHOLDER = re.compile(r"\.v2-(\d+)\.invalid$", re.IGNORECASE)
USER_UUID_NS = uuid.UUID("8d3f0f5e-2a51-4c3e-9b0e-6a1f3c2d7e41")  # snowflake_to_v3_migration._STABLE_UUID_NS
DEST_FIELDS = ("uuid", "name", "slug", "code", "destination_type", "status", "icon", "auto_checkin",
               "hidden_from_queues", "is_pharmacy", "is_lab", "is_radiology", "associated_procedure_category",
               "created_at", "updated_at")
SERVICES = ("core", "reception", "evaluation", "inventory", "finance", "inpatient", "theatre", "dialysis")


# ── small pure helpers (tests/test_v3_org_corrections.py) ───────────────────
def ts(v):
    """A V2 / V3 time as 'YYYY-MM-DD HH:MM:SS', or None."""
    s = "" if v is None else str(v).strip()
    return None if s in ("", "None", "0000-00-00 00:00:00") else s.replace("T", " ")[:19]


def epoch(v) -> float:
    try:
        return datetime.fromisoformat(str(ts(v))).timestamp()
    except (TypeError, ValueError):
        return 0.0


def name_key(v) -> str:
    """Destination slug / name for matching: letters and digits only."""
    return re.sub(r"[^a-z0-9]+", "", str(v or "").lower())


def words(v) -> str:
    return " ".join(str(v or "").lower().split())


def latest_per_id(rows: list[dict]) -> list[dict]:
    """The Snowflake snapshot holds each V2 row once per source schema: one per V2 id, the latest edit."""
    latest: dict[str, dict] = {}
    for r in rows:
        k = str(r["id"])
        if k not in latest or str(ts(r.get("updated_at")) or "") >= str(ts(latest[k].get("updated_at")) or ""):
            latest[k] = r
    return list(latest.values())


def v2_user_lookup(v3_users: list[dict], v2_users: dict[str, tuple], facility: str):
    """V2 user id → the org's V3 user, or None: the stable uuid5 the loaders gave it ("<facility>/users/<V2 id>"),
    else the V2 id in a placeholder e-mail, else V2's e-mail, else V2's username — those two only when exactly
    one V3 user has it. v2_users: {V2 id: (email, username)}. Org 284 (2026-10-19): 97 of the 194 V2 users on
    consumables found this way (96.6k rows) against 3 by placeholder alone."""
    by_uuid = {str(u.get("uuid") or "").lower(): u["id"] for u in v3_users}
    by_ph, emails, names = {}, defaultdict(set), defaultdict(set)
    for u in v3_users:
        e = words(u.get("email"))
        m = PLACEHOLDER.search(e)
        if m:
            by_ph[m.group(1)] = u["id"]
        elif e:
            emails[e].add(u["id"])
        if words(u.get("username")):
            names[words(u["username"])].add(u["id"])
    one = lambda ids: next(iter(ids)) if len(ids) == 1 else None

    def user_of(v):
        v = str(v or "").strip()
        if not v.isdigit() or v == "0":
            return None
        email, username = v2_users.get(v, (None, None))
        return (by_uuid.get(str(uuid.uuid5(USER_UUID_NS, f"{facility}/users/{v}"))) or by_ph.get(v)
                or one(emails.get(words(email), ())) or one(names.get(words(username), ())))
    return user_of


def inv_link_plan(v2_rows: list[dict], v3_rows: list[dict], *, facility: str, visit_of, proc_of, user_of,
                  v2_id_of: dict | None = None):
    """V3 investigations missing procedure / user → [(V3 id, {field: value})], Counter. Each V3 row is paired
    with V2 by, in turn: the stable uuid5; visit + type + price + quantity + created_at (V3 holds V2's time
    3 h earlier — all 96,017 one-to-one pairs in org 284, 2026-10-19 — or unshifted); the same without the
    time. A field is filled only when every V2 candidate agrees on it, and only where V3 has none.
    v2_id_of ({V3 id: V2 id}, from the loads' id maps) pairs first: rows copied V3 → V3 keep the source
    org's uuid, and visit + type + price + quantity is ambiguous for repeated orders (2026-10-22)."""
    money = lambda v: f"{float(v or 0):.2f}"
    base = lambda visit, r: (str(visit), words(r.get("type")), money(r.get("price")), money(r.get("quantity")))

    def at(v, hours=0):
        t = epoch(v)
        return int(t + hours * 3600) if t else None
    by_uuid, shifted, same, coarse = {}, defaultdict(list), defaultdict(list), defaultdict(list)
    for r in v2_rows:
        by_uuid[str(uuid.uuid5(USER_UUID_NS, f"{facility}/investigations/{r['id']}"))] = r
        v = visit_of(r.get("visit"))
        if v is None:
            continue
        b = base(v, r)
        coarse[b].append(r)
        if at(r.get("created_at")):
            shifted[b + (at(r.get("created_at"), -3),)].append(r)
            same[b + (at(r.get("created_at")),)].append(r)
    plan, why = [], Counter()
    by_id = {str(r["id"]): r for r in v2_rows}
    v2_id_of = v2_id_of or {}
    for r in v3_rows:
        b = base(r.get("visit"), r)
        mapped = by_id.get(str(v2_id_of.get(r.get("id"), v2_id_of.get(str(r.get("id"))))))
        cand = ([mapped] if mapped else
                [by_uuid[str(r.get("uuid")).lower()]] if str(r.get("uuid")).lower() in by_uuid else
                shifted.get(b + (at(r.get("created_at")),)) or same.get(b + (at(r.get("created_at")),))
                or coarse.get(b))
        if not cand:
            why["no V2 row (V3-only)"] += 1
            continue
        data = {}
        for field, of, v2_field in (("procedure", proc_of, "procedure"), ("user", user_of, "user")):
            vals = {of(c.get(v2_field)) for c in cand}
            new = next(iter(vals)) if len(vals) == 1 else None
            have = r.get(field)
            if have not in (None, "", 0):
                why[f"{field}: already set"] += 1
                why[f"{field}: set but V2 says otherwise (left)"] += bool(new) and str(new) != str(have)
            elif new is not None:
                data[field] = new
            else:
                why[f"{field}: {'ambiguous' if len(vals) > 1 else 'V2 value unmapped'} (left blank)"] += 1
        if data:
            plan.append((r["id"], data))
            for f in data:
                why[f"{f}: to fill"] += 1
    return plan, why


def vd_uuid(facility: str, v2_id) -> str:
    return str(uuid.uuid5(VD_UUID_NS, f"{facility}:evaluation_visit_destinations:{v2_id}"))


def vd_payload(r: dict, visit_id: int, dest: int | None, user_id: int | None, uid: str, dest_name=None) -> dict:
    """A V2 evaluation_visit_destinations row → the V3 evaluation visit_destinations payload."""
    out = str(r.get("checkout")).lower() in ("1", "true")
    fin = ts(r.get("finish_at"))
    return {"uuid": uid, "visit_id": visit_id, "user_id": user_id,
            "department": r.get("department") or dest_name, "checkout": 1 if out else 0,
            "begin_at": ts(r.get("begin_at")), "finish_at": fin,
            "status": "completed" if (fin or out) else "pending", "destination_id": dest,
            "created_at": ts(r.get("created_at")), "updated_at": ts(r.get("updated_at")),
            "deleted_at": ts(r.get("deleted_at"))}


def v2_admission_for(ad: dict, v2_by_id: dict, v2_by_visit: dict) -> dict | None:
    """The V2 admission of a V3 one: by admission number (ADM-<V2 id>, ADM-<id>.<org> when renamed), else
    through its visit (several on one visit: the nearest admission time)."""
    num = str(ad.get("admission_number") or "")
    n = num[4:].split(".")[0] if num.upper().startswith("ADM-") else ""
    cands = [v2_by_id[n]] if n in v2_by_id else (v2_by_visit.get(ad.get("visit_id")) or [])
    if not cands:
        return None
    t = epoch(ad.get("admission_date"))
    return min(cands, key=lambda r: abs(epoch(r.get("admitted_at")) - t) if t else 0)


def discharge_plan(ad: dict, v2: dict, stays: list[dict]) -> tuple[list, list, bool]:
    """(admission updates, ward-stay updates, needs a new ward stay) to make a V3 admission match V2."""
    out = ts(v2.get("discharged_at"))
    mine = sorted(stays, key=lambda w: (str(w.get("allocation_date") or ""), w["id"]))
    adm, stay = [], []
    if out:
        if ad.get("status") != "discharged" or ts(ad.get("discharge_date")) is None:
            adm.append((ad["id"], {"status": "discharged", "discharge_date": out}))
        for w in mine:
            if not w.get("discharge_date") and not w.get("transfer_out_date"):
                stay.append((w["id"], {"discharge_date": out, "transfer_out_date": out,
                                       "release_date": w.get("release_date") or out}))
        return adm, stay, False
    if ad.get("status") != "admitted" or ad.get("discharge_date"):
        adm.append((ad["id"], {"status": "admitted", "discharge_date": None}))
    if mine and (mine[-1].get("discharge_date") or mine[-1].get("transfer_out_date")):
        stay.append((mine[-1]["id"], {"discharge_date": None, "transfer_out_date": None,
                                      "transfer_out_time": None, "release_date": None}))
    return adm, stay, not mine


def category_ids(v2_list: str, v2cat: dict[str, str], by_name: dict[str, str]) -> tuple[str | None, list[str]]:
    """V2 category ids "1,6,46" → the org's ids by name; (new value, V2 names the org lacks)."""
    want, missing = [], []
    for x in str(v2_list or "").split(","):
        n = v2cat.get(x.strip())
        if n:
            (want if by_name.get(words(n)) else missing).append(by_name.get(words(n)) or n)
    return ",".join(dict.fromkeys(want)) or None, missing


def facility_sql(org: int, facility: int, found: list[dict]) -> str:
    """found: [{service, table, wrong: {facility: rows}}] → the DBA's SQL (one transaction per service)."""
    out = [f"-- Move organization {org}'s rows to its facility {facility}. {datetime.now():%Y-%m-%d %H:%M}.",
           "-- Written by v3_org_corrections.py facility_check (table names from the gateway's describe).",
           f"-- Only rows WHERE organization_id = {org} are touched. Each block runs on its SERVICE's database.", ""]
    by_svc = defaultdict(list)
    for f in found:
        by_svc[f["service"]].append(f)
    for svc, items in by_svc.items():
        out += [f"-- ================= {svc.upper()} service database =================", "START TRANSACTION;"]
        for f in items:
            t, wrong = f["table"], f["wrong"]
            n = sum(wrong.values())
            cnt = (f"SELECT '{t}' AS tbl, '{{step}}' AS step, SUM(facility_id = {facility}) AS on_{facility}, "
                   f"SUM(facility_id <> {facility}) AS elsewhere FROM {t} WHERE organization_id = {org};")
            ids = ", ".join(str(k) for k in sorted(wrong))
            out += [f"-- {t}: expect {n:,} (on facility {ids})", cnt.replace("{step}", "before"),
                    f"UPDATE {t} SET facility_id = {facility} WHERE organization_id = {org} AND facility_id IN ({ids});",
                    f"SELECT ROW_COUNT() AS updated, {n} AS expected;", cnt.replace("{step}", "after"), ""]
        out += ["-- updated = expected and elsewhere = 0 → COMMIT;   otherwise → ROLLBACK;", ""]
    return "\n".join(out)


# ── context ────────────────────────────────────────────────────────────────
class Ctx:
    """The target org's client, plus the V2 side (live API + Snowflake snapshot) on first use."""

    def __init__(self, target: str, facility: str, dry_run: bool, workers: int):
        from pipelines_common import use_pipelines_dir
        use_pipelines_dir([facility, target])
        from v3_to_v3_client import V3Client
        self.target, self.facility, self.dry_run, self.workers = target, facility, dry_run, workers
        self.c = V3Client(target)
        self.c.login()
        self.org = self.c.organization_id
        self._loader = None
        self._v2_visit_uid = None

    @property
    def L(self):
        if self._loader is None:
            import fast_v2_live_to_v3 as f
            self._loader = f.Loader(self.facility, 4, 16, True, 0)
        return self._loader

    @property
    def clean(self) -> str:
        import snowflake_to_v3_migration as s2v3
        return s2v3.sf_schema(self.facility, "CLEAN")

    def v2_visit_uid(self) -> dict[str, str]:
        """V2 visit id → unique_id (snapshot + the live API's newer visits)."""
        if self._v2_visit_uid is None:
            m = {str(i): u for i, u in self.L.sf(f'SELECT "ID", "UNIQUE_ID" FROM {self.clean}.EVALUATION_VISITS')}
            for v in self.L.api_tail(VISIT_NS, self.L.snapshot_max_id("evaluation_visits")):
                m[str(v["id"])] = v.get("unique_id")
            self._v2_visit_uid = m
        return self._v2_visit_uid

    def visit_by_uid(self) -> dict[str, int]:
        return {str(v.get("unique_id")): v["id"] for v in self.c.read_all("evaluation", "visits", workers=8)
                if v.get("unique_id")}

    def has_model(self, service: str, alias: str) -> bool:
        return alias in [(m.get("alias") if isinstance(m, dict) else m) for m in self.c.models(service)]

    def write(self, service: str, model: str, items: list, *, insert: bool = False) -> Counter:
        """items: [(id, data)] updated, or [data] inserted (match_on uuid when it has one)."""
        res, lock = Counter(), threading.Lock()

        def one(item):
            if insert:
                r = self.c.insert(service, model, item, match_on="uuid" if item.get("uuid") else None)
                ok = r.ok and (r.json() or {}).get("success", True)
            else:
                rid, data = item
                r = self.c.post(service, {"action": "update", "model": model, "source_tenant_id": self.org,
                                          "destination_tenant_id": self.org, "ids": [rid], "data": data},
                                timeout=120)
                body = r.json() if r.ok else {}
                ok = r.ok and not any((x or {}).get("ignored") for x in (body.get("data") or []))
            with lock:
                res["ok" if ok else "failed"] += 1
                if not ok and res["failed"] <= 3:
                    log.error("  %s/%s: %s %s", service, model, r.status_code, r.text[:250])
                if (res["ok"] + res["failed"]) % 5000 == 0:
                    log.info("  %s/%s: %d / %d written (%d failed)", service, model, res["ok"] + res["failed"],
                             len(items), res["failed"])
        with ThreadPoolExecutor(self.workers) as ex:
            list(ex.map(one, items))
        return res


# ── steps ──────────────────────────────────────────────────────────────────
def restore_destinations(x: Ctx, source: str | None) -> dict:
    if not source or source == x.target:
        return {"skipped": "no source org (only a V3 → V3 target has one)"}
    from v3_to_v3_client import V3Client
    src = V3Client(source)
    src.login()
    have = list(x.c.read_all("core", "service_destinations", workers=4))
    uuids = {str(r.get("uuid")).lower() for r in have}
    names = {words(r.get("name")) for r in have}
    missing = [r for r in src.read_all("core", "service_destinations", workers=4)
               if str(r.get("uuid")).lower() not in uuids and words(r.get("name")) not in names]
    log.info("restore_destinations: %s has %d; %d of %s's missing: %s", x.target, len(have), len(missing), source,
             ", ".join(str(r.get("name")) for r in missing))
    res = Counter()
    if not x.dry_run:
        for r in missing:
            data = {f: r.get(f) for f in DEST_FIELDS} | {"deleted_at": None}
            resp = x.c.insert("core", "service_destinations", data)
            if not resp.ok and "code" in resp.text.lower() and data.get("code"):
                data["code"] = f"{data['code']}.{x.org}"          # code is unique instance-wide
                resp = x.c.insert("core", "service_destinations", data)
            res["ok" if resp.ok else "failed"] += 1
            if not resp.ok:
                log.error("  %s: %s %s", r.get("name"), resp.status_code, resp.text[:250])
    return {"target_destinations": len(have), "missing": len(missing), "names": [r.get("name") for r in missing],
            **dict(res)}


def destination_categories(x: Ctx) -> dict:
    with __import__("snowflake_to_v3_migration")._snowflake_connect() as conn:
        cur = conn.cursor()
        v2cat = {str(i): n for i, n in cur.execute(
            f'SELECT DISTINCT "ID", "NAME" FROM {x.clean}.EVALUATION_PROCEDURE_CATEGORIES').fetchall()}
        v2dest = {words(n): str(c or "") for n, c in cur.execute(
            f'SELECT DISTINCT "NAME", "ASSOCIATED_PROCEDURE_CATEGORY" FROM {x.clean}.SETTINGS_DESTINATIONS '
            f'WHERE "DELETED_AT" IS NULL').fetchall()}
    cats = {str(r["id"]): r.get("name") for r in x.c.read_all("evaluation", "procedure_categories", workers=4)}
    by_name: dict[str, str] = {}
    for i, n in sorted(cats.items(), key=lambda kv: int(kv[0])):
        by_name.setdefault(words(n), i)
    updates, right, unknown = [], 0, Counter()
    for d in x.c.read_all("core", "service_destinations", workers=4):
        cur_ids = [s.strip() for s in str(d.get("associated_procedure_category") or "").split(",") if s.strip()]
        if cur_ids and all(s in cats for s in cur_ids):
            right += 1
            continue
        new, missing = category_ids(v2dest.get(words(d.get("name")), ""), v2cat, by_name)
        unknown.update(missing)
        if new == (",".join(cur_ids) or None):
            right += 1
            continue
        log.info("  %-40s %s → %s", d.get("name"), ",".join(cur_ids) or "-", new or "-")
        updates.append((d["id"], {"associated_procedure_category": new}))
    res = Counter() if x.dry_run else x.write("core", "service_destinations", updates)
    log.info("destination_categories: %d to fix, %d already right %s", len(updates), right, dict(res))
    return {"to_fix": len(updates), "already_right": right, "v2_categories_missing": dict(unknown), **dict(res)}


def _eval_vd_snapshot(x: Ctx) -> list[dict]:
    cols = ("ID", "VISIT", "USER", "DESTINATION_ID", "DEPARTMENT", "CHECKOUT", "BEGIN_AT", "FINISH_AT",
            "CREATED_AT", "UPDATED_AT", "DELETED_AT")
    cache = Path(tempfile.gettempdir()) / f"eval_vd_snapshot_{x.facility}.pkl"   # the read takes ~20 min
    snap_max = x.L.snapshot_max_id("evaluation_visit_destinations")
    raw = None
    try:
        with open(cache, "rb") as fh:
            got = pickle.load(fh)
        if isinstance(got, list):               # a cache from before the snapshot id was kept with it
            raw = got
        elif got[0] == snap_max:                # else a newer snapshot: read it again
            raw = got[1]
    except (OSError, pickle.PickleError, EOFError, ValueError, TypeError):
        raw = None
    if raw is None:
        raw = x.L.sf(f'SELECT {", ".join(chr(34) + c + chr(34) for c in cols)} FROM '
                     f'{x.clean}.EVALUATION_VISIT_DESTINATIONS WHERE "VISIT" IN (SELECT "ID" FROM {x.clean}.EVALUATION_VISITS)')
        try:
            with open(cache, "wb") as fh:
                pickle.dump((snap_max, raw), fh)
        except OSError:
            pass
    rows = [dict(zip([c.lower() for c in cols], r)) for r in raw]
    uid = x.v2_visit_uid()
    tail = [r for r in x.L.api_tail(VD_NS, snap_max) if str(r.get("visit")) in uid]
    log.info("eval_visit_destinations: V2 snapshot %d rows + %d live past id %d", len(rows), len(tail), snap_max)
    return latest_per_id(rows + tail)


def eval_visit_destinations(x: Ctx, relink: bool = True, limit: int = 0) -> dict:
    if not x.has_model("evaluation", "visit_destinations"):
        return {"skipped": f"evaluation/visit_destinations isn't on {x.c.urls['evaluation']} (deploy it first)"}
    visits = x.visit_by_uid()
    users = {}
    for u in x.c.read_all("core", "users", workers=8):
        m = PLACEHOLDER.search(str(u.get("email") or ""))
        if m:
            users[int(m.group(1))] = u["id"]
    dests: dict[str, int] = {}
    for d in x.c.read_all("core", "service_destinations", workers=4):
        for k in (d.get("slug"), d.get("name")):
            if name_key(k):
                dests.setdefault(name_key(k), d["id"])
    done = {str(r.get("uuid")).lower(): r.get("destination_id")
            for r in x.c.read_all("evaluation", "visit_destinations", workers=8)}
    sd = {str(i): (s, n) for i, s, n in x.L.sf(f'SELECT "ID", "SLUG", "NAME" FROM {x.clean}.SETTINGS_DESTINATIONS')}
    uid = x.v2_visit_uid()
    payloads, why = [], Counter()
    for r in _eval_vd_snapshot(x):
        vid = visits.get(str(uid.get(str(r.get("visit")))))
        if vid is None:
            why["visit not in the org"] += 1
            continue
        u = vd_uuid(x.facility, r["id"])
        slug, name = sd.get(str(r.get("destination_id")), (None, None))
        dest = dests.get(name_key(slug)) or dests.get(name_key(name)) or dests.get(name_key(r.get("department")))
        if u in done:
            if relink and dest and done[u] != dest:
                why["re-linked to the current destination"] += 1
            else:
                why["already right"] += 1
                continue
        else:
            why["new"] += 1
        user = users.get(int(r["user"])) if str(r.get("user") or "").isdigit() else None
        payloads.append(vd_payload(r, vid, dest, user, u, name))
    if limit:
        payloads = payloads[:limit]
    log.info("eval_visit_destinations: %s (org %s) has %d; to write %d — %s", x.target, x.org, len(done),
             len(payloads), dict(why))
    res = Counter() if x.dry_run else x.write("evaluation", "visit_destinations", payloads, insert=True)
    return {"in_org": len(done), "to_write": len(payloads), **dict(why), **dict(res)}


def cons_payload(r: dict, visit_id: int, product_id, user_of, invoice_id, location_id, uid: str,
                 store_product_id=None) -> dict:
    """A V2 evaluation_consumables row → the V3 evaluation consumables payload (V2's own dates kept)."""
    yes = lambda v: 1 if str(v).strip().lower() in ("1", "true") else 0
    num = lambda v: None if v in (None, "") else v
    return {"uuid": uid, "visit_id": visit_id, "product_id": product_id, "user_id": user_of(r.get("user_id")),
            "quantity": num(r.get("quantity")), "discount": num(r.get("discount")), "price": num(r.get("price")),
            "amount": num(r.get("amount")), "notes": r.get("notes"), "bill": yes(r.get("bill")),
            "moved_from_chargesheet": yes(r.get("moved_from_chargesheet")), "is_interim": yes(r.get("is_interim")),
            "consumable_type_name": r.get("consumable_type_name"), "invoice_id": invoice_id,
            "status": "cancelled" if yes(r.get("cancelled")) else ("billed" if invoice_id else "pending"),
            "cancelled": yes(r.get("cancelled")), "cancelled_by_id": user_of(r.get("cancelled_by_id")),
            "cancel_reason": r.get("cancel_reason"), "date_changed_by": user_of(r.get("date_changed_by")),
            "date_adjusted_from": (ts(r.get("date_adjusted_from")) or "")[:10] or None,
            "changes_track": r.get("changes_track"), "treatment_location_id": location_id,
            "store_product_id": store_product_id,
            "created_at": ts(r.get("created_at")), "updated_at": ts(r.get("updated_at")),
            "deleted_at": ts(r.get("deleted_at"))}


def eval_consumables(x: Ctx, limit: int = 0) -> dict:
    """V2 evaluation_consumables → the V3 evaluation service's consumables (registered 2026-10-19). Links:
    visit (V2 visit → unique_id → the org's), product (V2 product's item_no, else its name, against the org's
    products), invoice (V2 invoice_no; "<no>.<org>" when renamed), users (v2_user_lookup; user_id is what the
    API returns as requested_by), treatment
    location (name), store_product_id (V2 store product → its product → the org's inv_products row, the
    way Muniu asked 2026-10-19; blank when V2 has no such store product). consumable_type_id / split bill /
    payment: no V3 counterpart → empty (the type's name is kept). Stable uuid5 per V2 id, so re-runs update instead of duplicating."""
    if not x.has_model("evaluation", "consumables"):
        return {"skipped": f"evaluation/consumables isn't on {x.c.urls['evaluation']} (register it first)"}
    visits = x.visit_by_uid()
    v2_users = {str(i): (e, n) for i, e, n in x.L.sf(f'SELECT DISTINCT "ID", "EMAIL", "USERNAME" FROM {x.clean}.USERS')}
    user_of = v2_user_lookup(list(x.c.read_all("core", "users", workers=8)), v2_users, x.facility)
    by_item, by_name = {}, {}
    for p in x.c.read_all("inventory", "product", workers=8):
        if p.get("item_no"):
            by_item.setdefault(words(p["item_no"]), p["id"])
        for k in (p.get("generic_name"), p.get("name"), p.get("description")):
            if words(k):
                by_name.setdefault(words(k), p["id"])
    v2prod = {str(i): (it, n) for i, it, n in x.L.sf(
        f'SELECT DISTINCT "ID", "ITEM_NO", "NAME" FROM {x.clean}.INVENTORY_PRODUCTS')}
    sp_product = {str(i): str(pid) for i, pid in x.L.sf(
        f'SELECT DISTINCT "ID", "PRODUCT_ID" FROM {x.clean}.INVENTORY_STORE_PRODUCTS')}

    def product_of(v2_product_id, fallback_name=None):
        item, pname = v2prod.get(str(v2_product_id), (None, None))
        return by_item.get(words(item)) or by_name.get(words(pname)) or by_name.get(words(fallback_name))
    inv_by_no = {}
    for i in x.c.read_all("finance", "invoice", workers=8):
        inv_by_no.setdefault(str(i.get("invoice_no") or "").rsplit(f".{x.org}", 1)[0], i["id"])
    v2inv = {str(i): str(n) for i, n in x.L.sf(f'SELECT DISTINCT "ID", "INVOICE_NO" FROM {x.clean}.FINANCE_INVOICES')}
    locs = {words(r.get("name")): r["id"] for r in x.c.read_all("evaluation", "treatment_locations")}
    v2loc = {str(i): n for i, n in x.L.sf(f'SELECT DISTINCT "ID", "NAME" FROM {x.clean}.EVALUATION_TREATMENT_LOCATIONS')}
    done = {str(r.get("uuid")).lower(): r for r in x.c.read_all("evaluation", "consumables", workers=8)}
    cols = ("ID", "VISIT_ID", "PRODUCT_ID", "USER_ID", "QUANTITY", "DISCOUNT", "PRICE", "AMOUNT", "NOTES", "BILL",
            "MOVED_FROM_CHARGESHEET", "IS_INTERIM", "CONSUMABLE_TYPE_NAME", "INVOICE_ID", "CANCELLED",
            "CANCELLED_BY_ID", "CANCEL_REASON", "DATE_CHANGED_BY", "DATE_ADJUSTED_FROM", "CHANGES_TRACK",
            "TREATMENT_LOCATION_ID", "STORE_PRODUCT_ID", "CREATED_AT", "UPDATED_AT", "DELETED_AT")
    rows = [dict(zip([c.lower() for c in cols], r)) for r in x.L.sf(
        f'SELECT {", ".join(chr(34) + c + chr(34) for c in cols)} FROM {x.clean}.EVALUATION_CONSUMABLES')]
    rows = latest_per_id(rows + x.L.api_tail(r"Ignite\Evaluation\Entities\Consumable",
                                             x.L.snapshot_max_id("evaluation_consumables")))
    uid = x.v2_visit_uid()
    payloads, why = [], Counter()
    for r in rows:
        vid = visits.get(str(uid.get(str(r.get("visit_id")))))
        if vid is None:
            why["visit not in the org"] += 1
            continue
        u = str(uuid.uuid5(VD_UUID_NS, f"{x.facility}:evaluation_consumables:{r['id']}"))
        prod = product_of(r.get("product_id"), r.get("consumable_type_name"))
        sp = sp_product.get(str(r.get("store_product_id")))
        store_prod = product_of(sp) if sp else None
        inv = inv_by_no.get(v2inv.get(str(r.get("invoice_id")), "\0"))
        loc = locs.get(words(v2loc.get(str(r.get("treatment_location_id")))))
        p = cons_payload(r, vid, prod, user_of, inv, loc, u, store_prod)
        have = done.get(u)
        if have is not None and all(str(have.get(k) or "") == str(p.get(k) or "")
                                    for k in ("visit_id", "product_id", "store_product_id", "invoice_id",
                                              "cancelled", "user_id", "cancelled_by_id", "date_changed_by")):
            why["already right"] += 1
            continue
        why["update" if have is not None else "new"] += 1
        why["V2 user not in the org (requested_by blank)"] += bool(str(r.get("user_id") or "").strip("0")) \
            and p["user_id"] is None
        why["no product match (kept: type name)"] += prod is None
        why["V2 invoice not in the org"] += bool(r.get("invoice_id")) and inv is None
        why["store product blank (V2 has none / unmapped)"] += store_prod is None
        payloads.append(p)
    if limit:
        payloads = payloads[:limit]
    log.info("eval_consumables: %s (org %s) has %d; V2 %d; to write %d — %s", x.target, x.org, len(done), len(rows),
             len(payloads), dict(why))
    res = Counter() if x.dry_run else x.write("evaluation", "consumables", payloads, insert=True)
    return {"in_org": len(done), "v2_rows": len(rows), "to_write": len(payloads), **dict(why), **dict(res)}


def eval_investigation_links(x: Ctx, limit: int = 0) -> dict:
    """V3 investigations whose procedure / user is empty (org 284, 2026-10-19: 976 of 181,894 had a
    procedure, 19 a user) filled from V2 (snapshot + live API): procedure through V2's procedure code (unique
    in the org for every procedure V2's investigations use), user through v2_user_lookup. See inv_link_plan."""
    v3p = defaultdict(set)
    for p in x.c.read_all("evaluation", "procedures", workers=8):
        if p.get("code") and not p.get("deleted_at"):        # soft-deleted duplicates don't count (2026-10-21)
            v3p[words(p["code"])].add(p["id"])
    v2code = {str(i): c for i, c in x.L.sf(f'SELECT DISTINCT "ID", "CODE" FROM {x.clean}.EVALUATION_PROCEDURES')}

    def proc_of(v):
        ids = v3p.get(words(v2code.get(str(v))), ())
        return next(iter(ids)) if len(ids) == 1 else None
    v2_users = {str(i): (e, n) for i, e, n in x.L.sf(f'SELECT DISTINCT "ID", "EMAIL", "USERNAME" FROM {x.clean}.USERS')}
    user_of = v2_user_lookup(list(x.c.read_all("core", "users", workers=8)), v2_users, x.facility)
    uid, visits = x.v2_visit_uid(), x.visit_by_uid()
    visit_of = lambda v: visits.get(str(uid.get(str(v))))
    cols = ("ID", "VISIT", "TYPE", "PROCEDURE", "PRICE", "QUANTITY", "USER", "CREATED_AT", "UPDATED_AT")
    rows = [dict(zip([c.lower() for c in cols], r)) for r in x.L.sf(
        f'SELECT {", ".join(chr(34) + c + chr(34) for c in cols)} FROM {x.clean}.EVALUATION_INVESTIGATIONS')]
    rows = latest_per_id(rows + x.L.api_tail(r"Ignite\Evaluation\Entities\Investigations",
                                             x.L.snapshot_max_id("evaluation_investigations")))
    v3 = list(x.c.read_all("evaluation", "investigations", workers=8))
    v2_id_of = {}
    for state in sorted((Path(__file__).resolve().parent / ".migration_state").glob(f"v2*_{x.facility}_to_{x.target}/{x.facility}/.migration_id_map.json")):
        for k, v in (json.loads(state.read_text()).get("investigations") or {}).items():
            v2_id_of[int(v)] = k                                  # V3 id → V2 id (the loads' id maps)
    plan, why = inv_link_plan(rows, v3, facility=x.facility, visit_of=visit_of, proc_of=proc_of, user_of=user_of,
                              v2_id_of=v2_id_of)
    why["paired by id map"] = sum(1 for r in v3 if r.get("id") in v2_id_of)
    if limit:
        plan = plan[:limit]
    log.info("eval_investigation_links: %s (org %s) has %d; V2 %d; to update %d — %s", x.target, x.org, len(v3),
             len(rows), len(plan), dict(why))
    res = Counter() if x.dry_run else x.write("evaluation", "investigations", plan)
    return {"in_org": len(v3), "v2_rows": len(rows), "to_update": len(plan), **dict(why), **dict(res)}


def admission_discharges(x: Ctx) -> dict:
    v2adm = x.L.api_tail(ADMISSION_NS, 0)
    uid = x.v2_visit_uid()
    vid_by_uid = x.visit_by_uid()
    adms = list(x.c.read_all("inpatient", "admission", workers=8))
    stays = defaultdict(list)
    for w in x.c.read_all("inpatient", "visit_ward", workers=8):
        stays[w.get("admission_id")].append(w)
    v2_by_id = {str(r["id"]): r for r in v2adm}
    v2_by_visit = defaultdict(list)
    for r in v2adm:
        vid = vid_by_uid.get(str(uid.get(str(r.get("visit_id")))))
        if vid is not None:
            v2_by_visit[vid].append(r)
    # V2 ward / bed → the org's: ward by name, bed by number within it
    v2wards = {str(w["id"]): w for w in x.L.api_tail(WARD_NS, 0)}
    v2beds = {str(b["id"]): b for b in x.L.api_tail(BED_NS, 0)}
    wards = {words(w.get("name")): w["id"] for w in x.c.read_all("inpatient", "ward")}
    beds = {(b.get("ward_id"), words(b.get("number"))): b["id"] for b in x.c.read_all("inpatient", "bed")}
    adm_up, stay_up, new_stays, tally = [], [], [], Counter()
    for ad in adms:
        v2 = v2_admission_for(ad, v2_by_id, v2_by_visit)
        if v2 is None:
            tally["no V2 admission"] += 1
            continue
        tally["discharged in V2" if ts(v2.get("discharged_at")) else "still in, in V2"] += 1
        a, s, need_stay = discharge_plan(ad, v2, stays.get(ad["id"], []))
        adm_up += a
        stay_up += s
        if need_stay:
            ward = wards.get(words((v2wards.get(str(v2.get("ward_id"))) or {}).get("name")))
            bed = beds.get((ward, words((v2beds.get(str(v2.get("bed_id"))) or {}).get("number"))))
            if ward and bed:
                who = ad.get("admitting_doctor_id") or 1
                new_stays.append({"admission_id": ad["id"], "ward_id": ward, "bed_id": bed,
                                  "allocation_date": ts(v2.get("admitted_at")), "allocation_type": "admission",
                                  "allocated_by": who, "created_by": who})
            else:
                tally["still in, no ward stay, V2's ward / bed not in the org"] += 1
    log.info("admission_discharges: %s (org %s) — V2 %d admissions (%d still in) | %s | admission updates %d, "
             "ward stay updates %d, new ward stays %d", x.target, x.org, len(v2adm),
             sum(1 for r in v2adm if not ts(r.get("discharged_at"))), dict(tally), len(adm_up), len(stay_up),
             len(new_stays))
    out = {"v2_admissions": len(v2adm), **dict(tally), "admission_updates": len(adm_up),
           "ward_stay_updates": len(stay_up), "new_ward_stays": len(new_stays)}
    if not x.dry_run:
        out["admissions_written"] = dict(x.write("inpatient", "admission", adm_up))
        out["ward_stays_written"] = dict(x.write("inpatient", "visit_ward", stay_up))
        out["new_ward_stays_written"] = dict(x.write("inpatient", "visit_ward", new_stays, insert=True))
    return out


def facility_check(x: Ctx, out_dir: Path) -> dict:
    """Read-only: every model with facility_id, rows of the org on another facility → SQL for the DBA."""
    fac = x.c.facility_id
    found, lock = [], threading.Lock()
    jobs = [(s, m.get("alias") if isinstance(m, dict) else m) for s in SERVICES for m in x.c.models(s)]

    def one(job):
        svc, alias = job
        if not alias or alias == "activity_log":
            return
        try:
            rows, _ = x.c.read_page(svc, alias, 1, per_page=1)
            if not rows or "facility_id" not in rows[0]:
                return
            cnt = Counter(r.get("facility_id") for r in x.c.read_all(svc, alias, workers=4)
                          if r.get("organization_id") in (None, x.org))
            wrong = {k: n for k, n in cnt.items() if k not in (None, fac)}
            if wrong:
                table = (x.c.describe(svc, alias) or {}).get("table") or alias
                with lock:
                    found.append({"service": svc, "model": alias, "table": table, "wrong": wrong,
                                  "on_own": cnt.get(fac, 0)})
        except Exception as e:                       # one model's read error mustn't stop the check
            log.warning("facility_check %s/%s: %s", svc, alias, str(e)[:120])
    with ThreadPoolExecutor(6) as ex:
        list(ex.map(one, jobs))
    found.sort(key=lambda f: (SERVICES.index(f["service"]), -sum(f["wrong"].values())))
    out = {"facility": fac, "tables": len(found), "rows_elsewhere": sum(sum(f["wrong"].values()) for f in found),
           "by_table": {f"{f['service']}/{f['model']}": f["wrong"] for f in found}}
    if found:
        sql = out_dir / f"{x.target}_facility_to_{fac}_{time.strftime('%Y%m%d_%H%M')}.sql"
        sql.write_text(facility_sql(x.org, fac, found), encoding="utf-8")
        out["sql"] = str(sql)
    log.info("facility_check: %s (org %s, facility %s) — %d table(s), %d row(s) on another facility%s", x.target,
             x.org, fac, out["tables"], out["rows_elsewhere"], f" → {out.get('sql')}" if found else "")
    return out


def run(target: str, *, source: str | None = None, facility: str = "kisumu_v3", steps=None, dry_run: bool = False,
        relink: bool = True, workers: int = 12, limit: int = 0, out_dir: Path | None = None) -> dict:
    """Run the steps in order; one step's error is reported, the rest still run. Returns {step: counts}."""
    steps = [s for s in (steps or STEPS) if s]
    bad = [s for s in steps if s not in STEPS]
    if bad:
        raise ValueError(f"unknown step(s) {bad}; steps are {STEPS}")
    x = Ctx(target, facility, dry_run, workers)
    out_dir = out_dir or Path(__file__).resolve().parent / "output" / "v3_org_corrections"
    out_dir.mkdir(parents=True, exist_ok=True)
    report = {"target": target, "org": x.org, "source": source, "facility": facility, "dry_run": dry_run,
              "started": time.strftime("%Y-%m-%d %H:%M:%S"), "steps": {}}
    for s in STEPS:
        if s not in steps:
            continue
        t0 = time.time()
        try:
            r = {"restore_destinations": lambda: restore_destinations(x, source),
                 "destination_categories": lambda: destination_categories(x),
                 "eval_visit_destinations": lambda: eval_visit_destinations(x, relink, limit),
                 "eval_consumables": lambda: eval_consumables(x, limit),
                 "eval_investigation_links": lambda: eval_investigation_links(x, limit),
                 "admission_discharges": lambda: admission_discharges(x),
                 "facility_check": lambda: facility_check(x, out_dir)}[s]()
        except Exception as e:
            log.exception("%s failed", s)
            r = {"error": f"{type(e).__name__}: {str(e)[:300]}"}
        r["seconds"] = round(time.time() - t0)
        report["steps"][s] = r
    path = out_dir / f"{target}-{time.strftime('%Y%m%d_%H%M%S')}.json"
    path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    report["report"] = str(path)
    log.info("report: %s", path)
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--target", required=True, help="the V3 org to correct (facility key, e.g. org284, kisumu_v3)")
    ap.add_argument("--source", default=None, help="the org it was copied from (V3 → V3), for restore_destinations")
    ap.add_argument("--facility", default="kisumu_v3", help="the V2 facility key (its V2 API and snapshot)")
    ap.add_argument("--steps", nargs="*", default=None, help=f"default: all of {STEPS}")
    ap.add_argument("--no-relink", action="store_true", help="eval_visit_destinations: only post new rows")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--limit", type=int, default=0, help="eval_visit_destinations: at most N rows (canary)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rep = run(a.target, source=a.source, facility=a.facility, steps=a.steps, dry_run=a.dry_run,
              relink=not a.no_relink, workers=a.workers, limit=a.limit)
    errors = [s for s, r in rep["steps"].items() if "error" in r]
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
