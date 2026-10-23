#!/usr/bin/env python3
"""
patient_journey_v3.py — make every migrated patient's journey hang together
in V3, using the V2 Patient Journey API as the source of truth.

Why: V3 rows were linked to their patient / visit through the migration's id
map and content matching (repair_v3_links.py), which misses or mislinks rows
— patients still don't show their whole journey in V3. V2 now publishes an
identity map per patient (POST /api/system/access/patient/journey): the
patient, every visit, every record filed under each visit and the records
that hang off the patient. That map says, record by record, which patient
and visit each row belongs to — this pipeline makes V3 agree with it:

  check      `describe` the journey API: route deployed, token good, identity
             map populated on this V2 instance.
  walk       page through every patient (`list`, keyset cursor `after_id`)
             → <state>/journey/journeys.ndjson. Resumable: the cursor is saved
             after every page, an interrupted walk carries on from it.
  plan       read the V3 rows of every model the journeys touch, find each
             journey record's V3 row (by uuid — V2's uuid is carried to V3
             unchanged — else through the migration id map), and work out the
             patient / visit links each row should have. Writes nothing to V3;
             per-model updates go to <state>/journey/targets_<model>.json.
  apply      set the links that are missing or wrong: gateway insert with
             match_on=uuid = an update of that row, only the link columns
             (plus its facility column). Throttled, resumable, one model at a
             time (shares repair_v3_links' adaptive throttle).
  verify     re-read V3 and count rows whose links still differ.

Which columns are links comes from the migration's own config — the FK remap
tables (_FK_REMAP / _NS_FK_REMAP), the injected patient_id fields and
_V3_COLUMN_RENAMES — so this never sets a column the migration doesn't, and
only one V3 actually returned when read.

A link that is NULL in V3 is filled whenever the record and its parent are
found. A link that holds a DIFFERENT value is only overwritten when both the
record and the parent were found by uuid (`--overwrite strong`, the default);
`all` also trusts the id map, `never` only fills empty links. Rows the plan
can't place (not in V3, parent not in V3, two journeys wanting different
values …) are listed in problems.csv, never guessed.

  python patient_journey_v3.py --facility kisumu_v3                     # check + walk + plan (writes nothing to V3)
  python patient_journey_v3.py --facility kisumu_v3 --execute           # + apply + verify
  python patient_journey_v3.py --facility kisumu_v3 --execute --no-walk # re-plan from the last walk
  python patient_journey_v3.py --facility kisumu_v3 --resume            # carry on applying the last plan
  python patient_journey_v3.py --facility kisumu_v3 --journey-url http://10.0.0.5 --rewalk

The journey API host defaults to the facility's V2 host; override with
--journey-url or JOURNEY_<FACILITY>_BASE_URL. Auth is the facility's V2
login (FACILITY_<F>_USERNAME/_PASSWORD), or a fixed JOURNEY_<FACILITY>_TOKEN.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path

import requests

import repair_v3_links as rl
import snowflake_to_v3_migration as m
import v2_to_v3_api_migration as v2v3

log = logging.getLogger("patient_journey_v3")

JOURNEY_PATH = "/api/system/access/patient/journey"
MAX_PER_PAGE = 100            # the API's own ceiling
LIST_MIN_INTERVAL = 0.5       # `list` is rate limited to 120/min
READ_PER_PAGE = 500
READ_WORKERS = 6
ALLOW_GROWTH = True           # V3 rows added while reading: warn, link them on a later run
OVERWRITE_MODES = ("strong", "all", "never")

PATIENT, VISIT = "patient", "visit"
_KIND_OF_ALIAS = {"patient": PATIENT, "patients": PATIENT, "visit": VISIT, "visits": VISIT}
# Journey `table` names that don't resolve through NAMESPACE_MAP on their own
# (module, table) — table → (module, table) to resolve as instead.
TABLE_ALIASES: dict[str, tuple[str, str]] = {}


class JourneyUnavailable(RuntimeError):
    """The journey API can't serve this facility (route missing, identity map
    not installed, bad credentials) — nothing to do until V2 is fixed."""


# ─── journey API client ──────────────────────────────────────────────────

class JourneyClient:
    """POST {base}/api/system/access/patient/journey with a V2 bearer token.
    Retries 429 (honouring Retry-After), 5xx and network errors with backoff;
    refreshes the token once per 401.

    The token must come from the host being called — a token from another V2
    instance isn't 401'd there but 500s (it can't be decoded). So on the
    facility's own V2 host the migration's cached login is reused; on any
    other host (a journey URL) this logs in there itself, with
    JOURNEY_<F>_USERNAME/_PASSWORD or else FACILITY_<F>_USERNAME/_PASSWORD."""

    def __init__(self, facility: str, base_url: str | None = None, *, timeout: int = 180,
                 max_retries: int = 8, min_interval: float = LIST_MIN_INTERVAL,
                 session: requests.Session | None = None, sleep=time.sleep):
        self.facility = facility
        up = facility.upper()
        env_url = (os.getenv(f"JOURNEY_{up}_BASE_URL") or "").strip()
        try:
            facility_host = v2v3.v2_facility_config(facility)["base_url"].rstrip("/")
        except KeyError:
            facility_host = None
        self.base_url = (base_url or env_url or facility_host or "").rstrip("/")
        if not self.base_url:
            raise KeyError(f"No journey API host for {facility!r} — pass one or set JOURNEY_{up}_BASE_URL")
        self.own_login = self.base_url != facility_host
        self.url = self.base_url + JOURNEY_PATH
        self.fixed_token = (os.getenv(f"JOURNEY_{up}_TOKEN") or "").strip() or None
        self.timeout, self.max_retries, self.min_interval = timeout, max_retries, min_interval
        self.session = session or requests.Session()
        self.sleep = sleep
        self._last = 0.0
        self._login_token: str | None = None

    def _token(self, refresh: bool = False) -> str:
        if self.fixed_token:
            return self.fixed_token
        if not self.own_login:
            if refresh:
                v2v3._v2_invalidate_token(self.facility)
            return v2v3._v2_token(self.facility)
        if refresh or not self._login_token:
            self._login_token = self._login()
        return self._login_token

    def _login(self) -> str:
        up = self.facility.upper()
        user = (os.getenv(f"JOURNEY_{up}_USERNAME") or os.getenv(f"FACILITY_{up}_USERNAME") or "").strip()
        pwd = (os.getenv(f"JOURNEY_{up}_PASSWORD") or os.getenv(f"FACILITY_{up}_PASSWORD") or "").strip()
        if not user or not pwd:
            raise JourneyUnavailable(f"no credentials for {self.base_url}: set JOURNEY_{up}_USERNAME/_PASSWORD "
                                     f"(or FACILITY_{up}_…), or JOURNEY_{up}_TOKEN")
        url = f"{self.base_url}/api/users/authenticate/user"
        try:
            r = self.session.post(url, json={"username": user, "password": pwd}, timeout=60,
                                  headers={"Accept": "application/json"})
            token = ((r.json() or {}).get("success") or {}).get("token") if r.status_code == 200 else None
        except (requests.RequestException, ValueError) as e:
            raise JourneyUnavailable(f"login at {url} failed: {e}") from None
        if not token:
            raise JourneyUnavailable(f"login at {url} failed: HTTP {r.status_code} {_message(r)}")
        return token

    def call(self, body: dict) -> dict:
        wait, refreshed = 5, False
        for attempt in range(1, self.max_retries + 1):
            gap = self.min_interval - (time.time() - self._last)
            if gap > 0:
                self.sleep(gap)
            self._last = time.time()
            try:
                r = self.session.post(self.url, json=body, timeout=self.timeout, headers={
                    "Authorization": f"Bearer {self._token(refresh=False)}",
                    "Accept": "application/json", "Content-Type": "application/json"})
            except (requests.Timeout, requests.ConnectionError) as e:
                log.warning("  journey %s: %s (%d/%d) — retrying in %ss", body.get("action"), e,
                            attempt, self.max_retries, wait)
                self.sleep(wait); wait = min(wait * 2, 120)
                continue
            if r.status_code == 200:
                out = r.json()
                if out.get("success") is False:
                    raise RuntimeError(f"journey {body.get('action')}: success=false · {out.get('message')}")
                return out
            msg = _message(r)
            if r.status_code == 401 and not refreshed and not self.fixed_token:
                refreshed = True
                self._token(refresh=True)
                continue
            if r.status_code == 429:
                pause = _retry_after(r, default=60)
                log.warning("  journey rate limited — waiting %ss (%d/%d)", pause, attempt, self.max_retries)
                self.sleep(pause)
                continue
            if r.status_code in (500, 502, 504):
                log.warning("  journey %s: HTTP %s %s (%d/%d) — retrying in %ss", body.get("action"),
                            r.status_code, msg, attempt, self.max_retries, wait)
                self.sleep(wait); wait = min(wait * 2, 120)
                continue
            if r.status_code == 404 and body.get("action") != "show":
                raise JourneyUnavailable(f"{self.url} → 404: the journey route isn't deployed on this V2 "
                                         f"host (or the URL is wrong). {msg}".strip())
            if r.status_code in (401, 403):
                raise JourneyUnavailable(f"{self.url} → {r.status_code}: V2 refused the token. {msg}".strip())
            if r.status_code == 503:
                raise JourneyUnavailable(f"{self.url} → 503: the identity map isn't installed on this V2 "
                                         f"instance. {msg}".strip())
            raise RuntimeError(f"journey {body.get('action')}: HTTP {r.status_code} {msg}")
        raise RuntimeError(f"journey {body.get('action')}: gave up after {self.max_retries} attempts")

    def describe(self) -> dict:
        return self.call({"action": "describe"})

    def page(self, after_id: int, per_page: int = MAX_PER_PAGE, modules=None, tables=None) -> dict:
        body = {"action": "list", "after_id": int(after_id), "per_page": max(1, min(int(per_page), MAX_PER_PAGE))}
        if modules:
            body["modules"] = list(modules)
        if tables:
            body["tables"] = list(tables)
        return self.call(body)


def _message(r) -> str:
    try:
        return str(r.json().get("message") or "")[:300]
    except ValueError:
        return (r.text or "")[:300]


def _retry_after(r, default: int) -> int:
    for val in (r.headers.get("Retry-After"), _json_field(r, "retry_after_seconds"), _json_field(r, "retry_after")):
        try:
            if val not in (None, ""):
                return max(1, int(float(val)))
        except (TypeError, ValueError):
            pass
    return default


def _json_field(r, key):
    try:
        body = r.json()
        return body.get(key) if isinstance(body, dict) else None
    except ValueError:
        return None


# ─── state ───────────────────────────────────────────────────────────────

class State:
    """<state dir>/journey/ — the walk, the V3 snapshot, the plan, progress."""

    def __init__(self, facility: str):
        self.facility = facility
        self.dir = v2v3.use_state_dir(facility)
        self.work = self.dir / "journey"
        self.work.mkdir(exist_ok=True)
        self.journeys = self.work / "journeys.ndjson"
        self.cursor_file = self.work / "cursor.json"

    def path(self, name: str) -> Path:
        return self.work / name

    def read_state(self, name: str) -> dict:
        p = self.dir / name
        return json.loads(p.read_text()) if p.exists() else {}

    def dump(self, name: str, data) -> None:
        v2v3._atomic_write(self.work / name, json.dumps(data, default=str))

    def load(self, name: str, default=None):
        p = self.work / name
        if not p.exists():
            if default is not None:
                return default
            raise FileNotFoundError(f"No {p} — run the earlier step first")
        return json.loads(p.read_text())

    def cursor(self) -> dict:
        return self.load("cursor.json", {"after_id": 0, "walked": 0, "complete": False})


# ─── walk ────────────────────────────────────────────────────────────────

def check(client: JourneyClient) -> dict:
    """`describe`: fail fast when the API or its identity map isn't there."""
    body = client.describe()
    cov = (body.get("data") or {}).get("coverage") or {}
    if body.get("warnings"):
        log.warning("journey describe warnings: %s", json.dumps(body["warnings"])[:1000])
    log.info("journey API %s · identity map %s row(s), %s reachable, %s without a patient",
             client.base_url, cov.get("identity_map_rows"), cov.get("reachable"), cov.get("without_patient"))
    if not cov.get("identity_map_rows"):
        raise JourneyUnavailable(f"{client.url}: the identity map is empty on this V2 instance — "
                                 f"populate it before walking")
    return {"base_url": client.base_url, "coverage": cov, "warnings": body.get("warnings")}


def walk(st: State, client: JourneyClient, *, per_page: int = MAX_PER_PAGE, modules=None, tables=None,
         fresh: bool = False, max_pages: int | None = None) -> dict:
    """Page through every patient into journeys.ndjson. The cursor is a real
    patient id, saved after each page is on disk, so a re-run resumes; a
    finished walk is left alone unless `fresh`."""
    cur = st.cursor()
    filters = {"modules": sorted(modules or []), "tables": sorted(tables or []), "base_url": client.base_url}
    if not fresh and cur.get("after_id") and cur.get("filters") not in (None, filters):
        raise RuntimeError(f"the saved walk used {cur.get('filters')}, this one {filters} — "
                           f"re-walk from the start (fresh) instead of mixing them")
    if fresh or not cur.get("after_id"):
        st.journeys.write_text("")
        (st.work / "walk_warnings.jsonl").unlink(missing_ok=True)
        cur = {"after_id": 0, "walked": 0, "complete": False}
    elif cur.get("complete"):
        log.info("walk already complete (%d patients) — skipping; re-walk with fresh", cur["walked"])
        return cur
    cur.update(filters=filters, started_at=cur.get("started_at") or time.strftime("%Y-%m-%dT%H:%M:%S"))
    after, pages, t0 = int(cur["after_id"]), 0, time.time()
    log.info("walk from after_id %d (%d patients already on disk)", after, cur["walked"])
    while True:
        body = client.page(after, per_page, modules, tables)
        data = body.get("data") or []
        p = body.get("pagination") or {}
        if body.get("warnings"):
            with (st.work / "walk_warnings.jsonl").open("a") as f:
                f.write(json.dumps({"after_id": after, "warnings": body["warnings"]}, default=str) + "\n")
        if data:
            with st.journeys.open("a") as f:
                for j in data:
                    f.write(json.dumps(j, default=str) + "\n")
                f.flush(); os.fsync(f.fileno())
        nxt = p.get("next_after_id")
        more = bool(p.get("has_more_pages"))
        if more and (nxt is None or int(nxt) <= after):
            raise RuntimeError(f"journey list: cursor didn't advance (after_id {after} → {nxt}) — "
                               f"stopping instead of looping")
        cur["walked"] += len(data)
        cur["after_id"] = int(nxt) if nxt is not None else after
        cur["complete"] = not more
        st.dump("cursor.json", cur)
        after, pages = cur["after_id"], pages + 1
        if pages % 20 == 0 or not more:
            log.info("  walk: %d patients · after_id %d · %.1f pages/s", cur["walked"], after,
                     pages / max(time.time() - t0, 1e-6))
        if not more or (max_pages and pages >= max_pages):
            break
    log.info("walk %s: %d patients on disk", "complete" if cur["complete"] else "paused", cur["walked"])
    return cur


# ─── journeys → flat records ─────────────────────────────────────────────

@dataclass(frozen=True)
class Entry:
    table: str
    module: str
    v2_id: int | None
    uuid: str | None
    patient_id: int | None      # V2 patient id
    patient_uuid: str | None
    visit_id: int | None        # V2 visit id (None for patient-level records)
    visit_uuid: str | None
    # the reception_patients row's own uuid (journey `registration_uuid`) —
    # patient_uuid is the journey's identity, not the row's
    patient_reg_uuid: str | None = None


def _u(x) -> str | None:
    return str(x).strip().lower() if x not in (None, "") else None


def _i(x) -> int | None:
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


VISIT_TABLE = ("Evaluation", "evaluation_visits")


def iter_entries(st: State):
    """Every record of every journey as an Entry, each visit included as a
    record of its own (the visit's link to the patient). A patient seen twice
    (a page re-read after an interrupted walk) counts once."""
    seen: set = set()
    with st.journeys.open() as f:
        for line in f:
            if not line.strip():
                continue
            j = json.loads(line)
            pat = j.get("patient") or {}
            pid, puuid = _i(pat.get("id")), _u(pat.get("patient_uuid") or pat.get("uuid"))
            preg = _u(pat.get("registration_uuid"))
            key = pid if pid is not None else puuid
            if key in seen:
                continue
            seen.add(key)
            done: set = set()

            def entry(e: dict, vid, vuuid):
                tbl = e.get("table") or ""
                k = (tbl, _i(e.get("id", e.get("record_id"))), _u(e.get("uuid") or e.get("record_uuid")))
                if k in done:
                    return None
                done.add(k)
                return Entry(tbl, e.get("module") or "", k[1], k[2], pid,
                             puuid, _i(e.get("visit_id", vid)), _u(e.get("visit_uuid") or vuuid), preg)

            for v in j.get("visits") or []:
                vid, vuuid = _i(v.get("visit_id", v.get("id"))), _u(v.get("visit_uuid") or v.get("uuid"))
                if vid is not None or vuuid:
                    e = entry({"table": VISIT_TABLE[1], "module": VISIT_TABLE[0], "id": vid, "uuid": vuuid}, None, None)
                    if e:
                        yield e
                for rec in v.get("records") or []:
                    e = entry(rec, vid, vuuid)
                    if e:
                        yield e
            for rec in j.get("patient_level") or []:
                e = entry({**rec, "visit_id": None, "visit_uuid": None}, None, None)
                if e:
                    yield e


# ─── V2 table → V3 model + link columns ──────────────────────────────────

@dataclass
class Target:
    transform: str
    namespace: str
    alias: str
    service: str
    links: dict[str, str] = field(default_factory=dict)   # V3 column → PATIENT / VISIT


def _links_for(tk: str, ns: str) -> dict[str, str]:
    fk = {**v2v3._NS_FK_REMAP.get(ns, {}), **v2v3._FK_REMAP.get(tk, {})}
    fields = {f: _KIND_OF_ALIAS[a] for f, a in fk.items() if a in _KIND_OF_ALIAS}
    for f in getattr(v2v3, "_PER_KEY_INJECT", {}).get(tk, {}):
        if f in ("patient_id", "visit_id"):
            fields.setdefault(f, PATIENT if f == "patient_id" else VISIT)
    rules = m._V3_COLUMN_RENAMES.get(tk) or {}
    for src, dst in rules.get("move", {}).items():
        if src in fields:
            fields[dst] = fields.pop(src)
    for src, dst in rules.get("copy", {}).items():
        if src in fields:
            fields[dst] = fields[src]
    return fields


_VITAL_SPLIT = ("outpatient_vital", "inpatient_vital")


class Resolver:
    """(module, table) → candidate V3 targets, as the migration would post
    them. Vitals have two (outpatient `vitals`, inpatient `vital`) — the row
    is looked up in both."""

    def __init__(self):
        self._cache: dict[tuple[str, str], list[Target]] = {}

    def __call__(self, module: str, table: str) -> list[Target]:
        key = (module or "", table or "")
        if key not in self._cache:
            self._cache[key] = self._resolve(*TABLE_ALIASES.get(table, key))
        return self._cache[key]

    @staticmethod
    def _resolve(module: str, table: str) -> list[Target]:
        if not table:
            return []
        mods = [module] if module else []
        if "_" in table and table.split("_", 1)[0].capitalize() not in mods:
            mods.append(table.split("_", 1)[0].capitalize())   # module missing / differs: the table's prefix
        for mod in mods:
            for ns in m._candidate_namespaces(mod, table):
                mp = v2v3.NAMESPACE_MAP.get(ns)
                if not mp or not mp.get("transform") or not mp.get("v3"):
                    continue
                tks = _VITAL_SPLIT if mp["transform"] in _VITAL_SPLIT else (mp["transform"],)
                out = []
                for tk in tks:
                    alias = m._ALIAS_OVERRIDE.get(tk) or v2v3._v3_alias(mp["v3"])
                    service = m._SERVICE_OVERRIDE.get(tk) or v2v3._alias_to_service.get(alias)
                    if service:
                        out.append(Target(tk, mp["v3"], alias, service, _links_for(tk, mp["v3"])))
                return out
        return []


# ─── V3 snapshot ─────────────────────────────────────────────────────────

_LINK_COLS = ("patient", "patient_id", "visit", "visit_id")


def _gateway_read(service: str, body: dict) -> dict:
    wait = 3
    for attempt in range(1, 7):
        try:
            r = v2v3._gateway_post(service, body, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 401:
                v2v3._v3_invalidate_token()
            log.warning("  gateway read %s: HTTP %s %s", body.get("model"), r.status_code, r.text[:150])
        except Exception as e:   # noqa: BLE001 — retried, then raised below
            log.warning("  gateway read %s: %s", body.get("model"), e)
        if attempt < 6:
            time.sleep(wait); wait = min(wait * 2, 60)
    raise RuntimeError(f"gateway read {body.get('model')} failed 6 times")


def read_model(service: str, alias: str, org: int) -> dict:
    """{"columns": [...], "facility_col": str|None, "rows": [{id, uuid, facility, link cols}]}"""
    facility_col = (v2v3._gateway_model_meta.get(alias) or {}).get("facility")

    def page(p):
        return _gateway_read(service, {"action": "read", "model": alias, "source_tenant_id": org,
                                       "per_page": READ_PER_PAGE, "page": p})
    first = page(1)
    raw = list(first.get("data") or [])
    last = int((first.get("meta") or {}).get("last_page") or 1)
    with ThreadPoolExecutor(max_workers=READ_WORKERS) as pool:
        for body in pool.map(page, range(2, last + 1)):
            raw += body.get("data") or []
    columns = sorted({c for r in raw[:200] for c in r})
    keep = ["id", "uuid", *[c for c in _LINK_COLS if c in columns]] + ([facility_col] if facility_col else [])
    rows = list({r["id"]: {c: r.get(c) for c in keep} for r in raw}.values())
    total = (first.get("meta") or {}).get("total")
    if total is not None and len(rows) != int(total):
        msg = f"{alias}: read {len(rows)} distinct rows, total was {total} — rows are being added while reading"
        if not ALLOW_GROWTH:
            raise RuntimeError(msg)
        log.warning("%s — going on; rows added after this read get linked on a later run", msg)
    return {"columns": columns, "facility_col": facility_col, "rows": rows}


class V3Index:
    """One model's V3 rows: by id, and by uuid (lower case)."""

    def __init__(self, snap: dict):
        self.columns = set(snap["columns"])
        self.facility_col = snap.get("facility_col")
        self.by_id = {r["id"]: r for r in snap["rows"]}
        self.by_uuid: dict[str, list[int]] = defaultdict(list)
        for r in snap["rows"]:
            if r.get("uuid"):
                self.by_uuid[str(r["uuid"]).lower()].append(r["id"])


# ─── plan ────────────────────────────────────────────────────────────────

def _id_map_for(id_map: dict, alias: str) -> dict:
    for a in m._alias_variants(alias):
        if id_map.get(a):
            return id_map[a]
    return {}


class Finder:
    """V2 record → V3 row id: the V3 row carrying its uuid, else the id map
    (keyed by uuid, then by V2 id) when that row still exists."""

    def __init__(self, indexes: dict[str, V3Index], id_map: dict):
        self.indexes, self.id_map = indexes, id_map

    def find(self, alias: str, v2_id, uuid) -> tuple[int | None, str]:
        """(v3 id, how): how ∈ uuid | id_map | uuid_ambiguous | id_map_stale | not_in_v3.
        `uuid` may be several candidates (a patient: registration row uuid, then
        journey patient_uuid), tried in order."""
        idx = self.indexes.get(alias)
        if idx is None:
            return None, "not_in_v3"
        uuids = [u for u in (uuid if isinstance(uuid, (tuple, list)) else (uuid,)) if u]
        for u in uuids:
            hits = idx.by_uuid.get(u, [])
            if len(hits) == 1:
                return hits[0], "uuid"
            if len(hits) > 1:
                return None, "uuid_ambiguous"
        mp = _id_map_for(self.id_map, alias)
        for k in uuids + ([str(v2_id)] if v2_id is not None else []):
            v3 = mp.get(k)
            if v3 is not None:
                v3 = _i(v3)
                return (v3, "id_map") if v3 in idx.by_id else (None, "id_map_stale")
        return None, "not_in_v3"


def models_needed(st: State, resolver: Resolver) -> tuple[dict[str, str], Counter]:
    """{alias: service} for every model a journey record maps to (plus the
    patient and visit models, which every link points at), and the record
    count per journey table."""
    per_table: Counter = Counter()
    seen: dict[tuple[str, str], None] = {}
    for e in iter_entries(st):
        per_table[e.table] += 1
        seen.setdefault((e.module, e.table))
    need = {}
    for mod, tbl in list(seen) + [("Reception", "reception_patients"), VISIT_TABLE]:
        for t in resolver(mod, tbl):
            need[t.alias] = t.service
    return need, per_table


def snapshot(st: State, models: dict[str, str], org: int) -> None:
    for alias, service in sorted(models.items()):
        if alias not in v2v3._gateway_model_meta:
            log.warning("snapshot %-22s not exposed by the V3 gateway — its records can't be linked", alias)
            continue
        t0 = time.time()
        snap = read_model(service, alias, org)
        st.dump(f"v3_{alias}.json", snap)
        log.info("snapshot %-22s %7d rows · link columns %s (%.0fs)", alias, len(snap["rows"]),
                 [c for c in _LINK_COLS if c in snap["columns"]] or "none", time.time() - t0)


def _load_indexes(st: State, aliases) -> dict[str, V3Index]:
    out = {}
    for a in aliases:
        p = st.path(f"v3_{a}.json")
        if p.exists():
            out[a] = V3Index(json.loads(p.read_text()))
    return out


def plan(st: State, resolver: Resolver, *, overwrite: str = "strong", aliases: list[str] | None = None) -> dict:
    """Match every journey record to its V3 row and work out the links each
    row should have. Writes targets_<alias>.json, problems.csv and
    plan_report.json; returns the report."""
    if overwrite not in OVERWRITE_MODES:
        raise ValueError(f"overwrite must be one of {OVERWRITE_MODES}")
    patient_alias = resolver("Reception", "reception_patients")[0].alias
    visit_alias = resolver(*VISIT_TABLE)[0].alias
    snaps = sorted(p.name[3:-5] for p in st.work.glob("v3_*.json"))
    idx = _load_indexes(st, snaps)
    finder = Finder(idx, st.read_state(".migration_id_map.json"))
    parents: dict[tuple[str, int | None, str | None], tuple[int | None, str]] = {}

    def parent(kind: str, v2_id, uuid):
        key = (kind, v2_id, uuid)
        if key not in parents:
            parents[key] = finder.find(patient_alias if kind == PATIENT else visit_alias, v2_id, uuid)
        return parents[key]

    rep: dict[str, Counter] = defaultdict(Counter)
    want: dict[str, dict[int, dict[str, set]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    strength: dict[tuple[str, int, str], bool] = {}
    claims: dict[tuple[str, int], set] = defaultdict(set)   # V3 row → the (patient, visit) of each record on it
    problems: list[list] = []
    patients_ok: dict = {}

    for e in iter_entries(st):
        r = rep[e.table]
        r["records"] += 1
        pkey = e.patient_id if e.patient_id is not None else e.patient_uuid
        patients_ok.setdefault(pkey, True)
        targets = resolver(e.module, e.table)
        if not targets:
            r["no_v3_model"] += 1
            continue
        if aliases and not any(t.alias in aliases for t in targets):
            r["not_selected"] += 1
            continue
        hit, how, tgt = None, "not_in_v3", None
        for t in targets:                     # vitals: outpatient, then inpatient
            found, why = finder.find(t.alias, e.v2_id, e.uuid)
            if found is not None:
                hit, how, tgt = found, why, t
                break
            if how == "not_in_v3":
                how = why                     # keep the most telling reason
        r[f"found_by_{how}" if hit is not None else how] += 1
        if hit is None:
            problems.append([e.table, e.v2_id, e.uuid, e.patient_id, e.visit_id, how])
            patients_ok[pkey] = False
            continue
        row = idx[tgt.alias].by_id[hit]
        claims[(tgt.alias, hit)].add((e.patient_id, e.patient_uuid, e.visit_id, e.visit_uuid))
        links = {c: k for c, k in tgt.links.items() if c in idx[tgt.alias].columns}
        if not links:
            r["no_link_columns"] += 1
            continue
        if not row.get("uuid"):
            r["v3_row_without_uuid"] += 1
            problems.append([e.table, e.v2_id, e.uuid, e.patient_id, e.visit_id, "v3_row_without_uuid"])
            patients_ok[pkey] = False
            continue
        for col, kind in links.items():
            if kind == VISIT and e.visit_id is None and not e.visit_uuid:
                continue                                    # patient-level record: no visit
            pid, phow = (parent(PATIENT, e.patient_id, (e.patient_reg_uuid, e.patient_uuid)) if kind == PATIENT
                         else parent(VISIT, e.visit_id, e.visit_uuid))
            if pid is None:
                r[f"{kind}_not_in_v3"] += 1
                problems.append([e.table, e.v2_id, e.uuid, e.patient_id, e.visit_id, f"{kind}_{phow}"])
                patients_ok[pkey] = False
                continue
            want[tgt.alias][hit][col].add(pid)
            s = (tgt.alias, hit, col)
            strength[s] = strength.get(s, True) and how == "uuid" and phow == "uuid"

    # decide per V3 row/column
    totals = Counter()
    for alias, rows in want.items():
        index = idx[alias]
        out = {}
        a_rep = rep[f"[{alias}]"]
        for v3id, cols in rows.items():
            row = index.by_id[v3id]
            # one V3 row, records from different patients / visits: the
            # identity map disagrees with itself — leave the row alone
            owners = claims[(alias, v3id)]
            if len(owners) > 1 or any(len(v) > 1 for v in cols.values()):
                a_rep["conflicting_journeys"] += 1
                problems.append([alias, None, row.get("uuid"), None, None,
                                 f"v3 row {v3id}: claimed by {len(owners)} different journeys "
                                 f"(patient, patient_uuid, visit, visit_uuid): {sorted(owners, key=str)}"])
                continue
            sets = {}
            for col, vals in cols.items():
                val = next(iter(vals))
                have = _i(row.get(col))
                if have == val:
                    a_rep["already_right"] += 1
                elif have in (None, 0):
                    sets[col] = val
                    a_rep["to_fill"] += 1
                elif overwrite == "all" or (overwrite == "strong" and strength[(alias, v3id, col)]):
                    sets[col] = val
                    a_rep["to_correct"] += 1
                else:
                    a_rep["wrong_kept"] += 1
                    problems.append([alias, None, row.get("uuid"), None, None,
                                     f"v3 row {v3id}: {col} is {have}, journey says {val} — kept ({overwrite})"])
            if sets:
                out[str(v3id)] = {"uuid": row["uuid"], "set": sets,
                                  **({"facility": {index.facility_col: row.get(index.facility_col)}}
                                     if index.facility_col and row.get(index.facility_col) is not None else {})}
        st.dump(f"targets_{alias}.json", out)
        st.path(f"applied_{alias}.json").unlink(missing_ok=True)   # fresh targets = all still to do
        a_rep["rows_to_update"] = len(out)
        totals["rows_to_update"] += len(out)
    for alias in idx:
        if alias not in want and st.path(f"targets_{alias}.json").exists():
            st.dump(f"targets_{alias}.json", {})

    with st.path("problems.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["table_or_model", "v2_id", "uuid", "v2_patient_id", "v2_visit_id", "problem"])
        w.writerows(problems)
    report = {"tables": {k: dict(v) for k, v in sorted(rep.items())},
              "patients": len(patients_ok), "patients_fully_placed": sum(patients_ok.values()),
              "problems": len(problems), "rows_to_update": totals["rows_to_update"],
              "models_to_update": sorted(a for a in want if st.load(f"targets_{a}.json", {})),
              "overwrite": overwrite}
    st.dump("plan_report.json", report)
    log.info("plan: %d patients (%d fully placed in V3) · %d V3 rows to update · %d problems (problems.csv)",
             report["patients"], report["patients_fully_placed"], report["rows_to_update"], report["problems"])
    for k, v in report["tables"].items():
        log.info("  %-34s %s", k, json.dumps(v))
    return report


# ─── apply / verify ──────────────────────────────────────────────────────

def apply_model(st: State, alias: str, service: str, org: int, pace: rl.Pace = rl.Pace()) -> dict:
    """Post one model's link updates (targets_<alias>.json). Resumable."""
    targets = st.load(f"targets_{alias}.json", {})
    done_file = st.path(f"applied_{alias}.json")
    done = set(json.loads(done_file.read_text())) if done_file.exists() else set()
    queue = [(k, v, 0) for k, v in targets.items() if k not in done]
    log.info("apply %-22s %d rows to update (%d done earlier) · start %d parallel, max %d, ≤%s req/s",
             alias, len(queue), len(targets) - len(queue), pace.start_workers, pace.max_workers, pace.max_rps or "∞")
    throttle = rl.Throttle(pace, alias)
    lock = threading.Lock()
    failed: list[str] = []
    stats = {"ok": 0, "since_flush": 0}
    stop: list[BaseException] = []
    t0 = time.time()

    def flush():
        v2v3._atomic_write(done_file, json.dumps(sorted(done)))

    def post_once(t: dict) -> None:
        data = {"uuid": t["uuid"], **(t.get("facility") or {}), **t["set"]}
        r = v2v3._gateway_post(service, {"action": "insert", "model": alias, "destination_tenant_id": org,
                                         "match_on": "uuid", "data": data}, timeout=60)
        if r.status_code == 401:
            v2v3._v3_invalidate_token()
        if r.status_code not in (200, 201):
            raise RuntimeError(f"HTTP {r.status_code} {r.text[:150]}")
        if (r.json() or {}).get("created"):
            # the uuid matched nothing, so V3 inserted a new row — never carry on
            raise SystemExit(f"{alias}: uuid {t['uuid']} INSERTED a new V3 row instead of updating — stopped")

    def worker():
        while not stop:
            with lock:
                if not queue:
                    return
                key, t, attempts = queue.pop()
            throttle.acquire()
            start, ok = time.time(), False
            try:
                post_once(t)
                ok = True
            except SystemExit as e:
                stop.append(e)
                throttle.release(False, 0)
                return
            except Exception as e:   # noqa: BLE001 — retried, then recorded
                with lock:
                    if attempts + 1 < 8:
                        queue.insert(0, (key, t, attempts + 1))
                    else:
                        failed.append(f"{key}: {str(e)[:150]}")
            throttle.release(ok, time.time() - start)
            if ok:
                with lock:
                    done.add(key)
                    stats["ok"] += 1
                    stats["since_flush"] += 1
                    if stats["since_flush"] >= 500:
                        stats["since_flush"] = 0
                        flush()
                        rate = stats["ok"] / max(time.time() - t0, 1)
                        log.info("  %s: %d / %d · %.1f/s · %d parallel", alias, len(done), len(targets),
                                 rate, throttle.limit)

    with ThreadPoolExecutor(max_workers=max(1, pace.max_workers)) as pool:
        for f in [pool.submit(worker) for _ in range(max(1, pace.max_workers))]:
            f.result()
    flush()
    if stop:
        raise stop[0]
    if failed:
        with st.path(f"failed_{alias}.txt").open("w") as f:
            f.write("\n".join(failed) + "\n")
    log.info("apply %-22s updated %d · failed %d · %.1f min", alias, stats["ok"], len(failed), (time.time() - t0) / 60)
    return {"updated": stats["ok"], "failed": len(failed), "failures_sample": failed[:3]}


def verify_model(st: State, alias: str, service: str, org: int) -> dict:
    targets = st.load(f"targets_{alias}.json", {})
    now = {r["id"]: r for r in read_model(service, alias, org)["rows"]}
    wrong = [k for k, t in targets.items()
             if any(_i(now.get(int(k), {}).get(c)) != v for c, v in t["set"].items())]
    log.info("verify %-22s %d of %d updated rows still differ%s", alias, len(wrong), len(targets),
             f" (e.g. {wrong[:5]})" if wrong else "")
    return {"still_wrong": len(wrong), "sample": wrong[:10]}


# ─── phases (the DAG runs each in its own task) ──────────────────────────

def _connect_v3(facility: str) -> int:
    v2v3.set_v3_target_facility(facility)
    org = v2v3.v3_login_org_cfg()["organization_id"]
    v2v3._fetch_available_models()
    return org


def _locks(stack: ExitStack, aliases) -> None:
    """The migration's per-table locks for the models being written (its lock
    names are Snowflake table names; both common spellings are taken), plus
    one per model so two journey runs never write the same model at once."""
    names = {f"journey_{a}" for a in aliases}
    for a in aliases:
        for t in m._alias_variants(a):
            names |= {t, f"evaluation_{t}", f"reception_{t}"}
    for n in sorted(names):
        stack.enter_context(m.table_lock(n))


def run_check(facility: str, journey_url: str | None = None) -> dict:
    return check(JourneyClient(facility, journey_url))


def run_walk(facility: str, journey_url: str | None = None, *, per_page: int = MAX_PER_PAGE,
             modules=None, tables=None, fresh: bool = False) -> dict:
    st = State(facility)
    return walk(st, JourneyClient(facility, journey_url), per_page=per_page, modules=modules,
                tables=tables, fresh=fresh)


def run_plan(facility: str, *, overwrite: str = "strong", snapshot_first: bool = True,
             models: list[str] | None = None) -> dict:
    """snapshot V3 + plan. Returns the report, with `apply` = [{alias, service}]."""
    st = State(facility)
    if not st.cursor().get("walked"):
        raise RuntimeError("no journeys on disk — walk first")
    org = _connect_v3(facility)
    resolver = Resolver()
    need, per_table = models_needed(st, resolver)
    log.info("journeys: %d records across %d tables · V3 models %s", sum(per_table.values()), len(per_table),
             ", ".join(sorted(need)))
    if snapshot_first:
        snapshot(st, need, org)
    report = plan(st, resolver, overwrite=overwrite, aliases=models)
    report["apply"] = [{"alias": a, "service": need[a]} for a in report["models_to_update"] if a in need]
    st.dump("plan_report.json", report)
    return report


def run_apply(facility: str, alias: str, service: str, pace: rl.Pace = rl.Pace(), *, check_after: bool = True) -> dict:
    st = State(facility)
    org = _connect_v3(facility)
    with ExitStack() as stack:
        _locks(stack, [alias])
        out = {"apply": apply_model(st, alias, service, org, pace)}
        if check_after:
            out["verify"] = verify_model(st, alias, service, org)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--facility", required=True)
    ap.add_argument("--journey-url", help="Journey API host (default: the facility's V2 host)")
    ap.add_argument("--execute", action="store_true", help="Update V3 (default: check + walk + plan only)")
    ap.add_argument("--rewalk", action="store_true", help="Walk every patient again from the start")
    ap.add_argument("--no-walk", action="store_true", help="Use the journeys already on disk")
    ap.add_argument("--no-snapshot", action="store_true", help="Re-plan from the last V3 snapshot")
    ap.add_argument("--resume", action="store_true", help="Skip walk/plan: carry on applying the last plan")
    ap.add_argument("--per-page", type=int, default=MAX_PER_PAGE)
    ap.add_argument("--modules", nargs="+", help="Only these journey modules (e.g. Evaluation Reception)")
    ap.add_argument("--tables", nargs="+", help="Only these journey tables")
    ap.add_argument("--models", nargs="+", help="Only update these V3 models (gateway aliases)")
    ap.add_argument("--overwrite", choices=OVERWRITE_MODES, default="strong")
    ap.add_argument("--start-workers", type=int, default=rl.Pace.start_workers)
    ap.add_argument("--max-workers", type=int, default=rl.Pace.max_workers)
    ap.add_argument("--max-rps", type=float, default=rl.Pace.max_rps)
    ap.add_argument("--slow-seconds", type=float, default=rl.Pace.slow_seconds)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    for noisy in ("snowflake.connector", "botocore", "urllib3", "v2_to_v3_migration", "snowflake_to_v3_migration"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    pace = rl.Pace(args.start_workers, args.max_workers, args.max_rps, args.slow_seconds)
    try:
        if args.resume:
            report = State(args.facility).load("plan_report.json")
        else:
            if not args.no_walk:
                run_check(args.facility, args.journey_url)
                run_walk(args.facility, args.journey_url, per_page=args.per_page, modules=args.modules,
                         tables=args.tables, fresh=args.rewalk)
            report = run_plan(args.facility, overwrite=args.overwrite, snapshot_first=not args.no_snapshot,
                              models=args.models)
        results = {}
        if args.execute or args.resume:
            for item in report.get("apply") or []:
                results[item["alias"]] = run_apply(args.facility, item["alias"], item["service"], pace)
    except (JourneyUnavailable, m.TableBusy) as e:
        sys.exit(f"✗ {e}")
    print(json.dumps({"plan": {k: v for k, v in report.items() if k != "tables"}, "apply": results},
                     indent=2, default=str))
    bad = any(r["apply"]["failed"] or (r.get("verify") or {}).get("still_wrong") for r in results.values())
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
