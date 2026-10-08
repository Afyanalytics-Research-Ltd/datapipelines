#!/usr/bin/env python3
"""
repair_v3_links.py — fill the record links V3 never got, and repair the
migration state so re-runs stop re-posting rows that are already in V3.

Why: the migration posted evaluation-service links under the transform's
names (patient_id / visit_id), but visits store `patient` and prescriptions,
investigations, doctor notes and vitals store `visit` — V3 dropped them, so
no patient showed their visits, prescriptions, results or notes. The code is
fixed (snowflake_to_v3_migration._V3_COLUMN_RENAMES); this repairs the rows
already in V3, through the gateway (no SQL needed):

  snapshot   read the facility's V3 rows (id, uuid, links, a few content
             columns) and its Snowflake CLEAN rows → <state>/link_repair/
  match      pair every V3 row with its V2 row: the id map, each pair checked
             on content (created_at + table columns); V3 rows the map misses
             are matched on content when that is unambiguous. Extra copies
             (duplicate inserts) are listed and linked like the original.
  state      repair the id map (verified pairs in, other tables' stray ids
             out), add every verified V2 id to the record progress and save
             the V3 uuid per V2 id (.migration_v3_uuid.json) — the migration
             re-posts with that uuid, which updates in place.
  apply      set the missing / wrong links on each V3 row: gateway insert
             with match_on=uuid = update of that row, only the link columns
             (plus updated_at). Resumable; progress in <state>/link_repair/.
  verify     re-read V3 and count rows whose links still differ.

  python repair_v3_links.py --facility kisumu_v3                 # plan only (snapshot + match)
  python repair_v3_links.py --facility kisumu_v3 --execute       # + state + apply + verify
  python repair_v3_links.py --facility kisumu_v3 --execute --tables visits vitals
  python repair_v3_links.py --facility kisumu_v3 --execute --no-snapshot   # reuse the last snapshot
  python repair_v3_links.py --facility kisumu_v3 --execute --allow-growth  # while rows are still being added

Holds each table's migration lock while it runs: it refuses to start while a
migration is writing one of these tables, and blocks the migration meanwhile.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import shutil
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

import snowflake_to_v3_migration as m
import v2_to_v3_api_migration as v2v3

log = logging.getLogger("repair_v3_links")

PER_PAGE = 500
READ_WORKERS = 6
ALLOW_GROWTH = False   # --allow-growth: snapshot even while rows are being inserted


# ─── comparison helpers (V3 returns UTC "…Z"; Snowflake holds V2 local time, EAT) ──

def _ts(x):
    if x in (None, ""):
        return None
    s = str(x).replace("T", " ")[:19]
    if str(x).endswith("Z"):
        return s
    try:
        return (datetime.strptime(s, "%Y-%m-%d %H:%M:%S") - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return s


def _num(x):
    try:
        return round(float(x), 2)
    except (TypeError, ValueError):
        return None if x in (None, "") else str(x)


def _int(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


@dataclass
class LinkTable:
    name: str                     # short name, CLI / file names
    sf_table: str                 # Snowflake CLEAN view / migration table
    alias: str                    # gateway model + id-map alias
    job_suffix: str               # record-progress job key suffix
    sf_cols: list[str]            # CLEAN columns to read (lower case)
    v3_cols: list[str]            # V3 columns to read
    key: Callable[[dict], tuple]  # content key, same on both sides
    parent: str                   # V2 column holding the parent id ("visit" / "patient")
    targets: list[str]            # V3 columns to fill


TABLES = {t.name: t for t in [
    LinkTable("visits", "visits", "visits", "",
              ["id", "patient", "created_at", "unique_id"],
              ["id", "uuid", "facility_id", "created_at", "patient", "unique_id"],
              lambda r: (_ts(r["created_at"]), r.get("unique_id")), "patient", ["patient"]),
    LinkTable("prescriptions", "evaluation_prescriptions", "prescriptions", "",
              ["id", "visit", "created_at", "drug", "quantity"],
              ["id", "uuid", "facility_id", "created_at", "visit", "drug", "quantity"],
              lambda r: (_ts(r["created_at"]), _int(r.get("drug")), _num(r.get("quantity"))), "visit", ["visit"]),
    LinkTable("investigations", "evaluation_investigations", "investigations", "",
              ["id", "visit", "created_at", "price", "invoiced"],
              ["id", "uuid", "created_at", "visit", "patient_id", "price", "invoiced"],
              lambda r: (_ts(r["created_at"]), _num(r.get("price")), _int(r.get("invoiced"))), "visit",
              ["visit", "patient_id"]),
    LinkTable("doctor_notes", "evaluation_doctor_notes", "doctor_notes", "",
              ["id", "visit", "created_at"],
              ["id", "uuid", "facility_id", "created_at", "visit", "visit_id", "patient_id"],
              lambda r: (_ts(r["created_at"]),), "visit", ["visit", "visit_id"]),
    # pulse is NULL in V2 on a few rows that V3 has as 0 → temperature only
    LinkTable("vitals", "evaluation_vitals", "vitals", "::outpatient",
              ["id", "visit", "created_at", "temperature"],
              ["id", "uuid", "facility_id", "created_at", "visit", "visit_id", "patient_id", "temperature"],
              lambda r: (_ts(r["created_at"]), _num(r.get("temperature"))), "visit", ["visit", "visit_id"]),
]}


# ─── state files ─────────────────────────────────────────────────────────

class State:
    def __init__(self, facility: str):
        self.facility = facility
        self.dir = v2v3.use_state_dir(facility)
        self.work = self.dir / "link_repair"
        self.work.mkdir(exist_ok=True)

    def read(self, name: str) -> dict:
        p = self.dir / name
        return json.loads(p.read_text()) if p.exists() else {}

    def write(self, name: str, data: dict, stamp: str) -> None:
        p = self.dir / name
        with v2v3._file_lock(p):
            if p.exists():
                shutil.copy2(p, p.with_name(f"{p.name}.bak_repair_{stamp}"))
            v2v3._atomic_write(p, json.dumps(data, indent=2))

    def dump(self, name: str, data) -> None:
        (self.work / name).write_text(json.dumps(data, default=str))

    def load(self, name: str):
        p = self.work / name
        if not p.exists():
            raise SystemExit(f"No {p} — run without --no-snapshot first")
        return json.loads(p.read_text())


# ─── snapshot ────────────────────────────────────────────────────────────

def _gateway(body: dict) -> dict:
    wait = 3
    for attempt in range(1, 7):
        try:
            r = v2v3._gateway_post("evaluation", body, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 401:
                v2v3._v3_invalidate_token()
            log.warning("  gateway %s %s: HTTP %s %s", body.get("action"), body.get("model"), r.status_code, r.text[:150])
        except Exception as e:
            log.warning("  gateway %s %s: %s", body.get("action"), body.get("model"), e)
        if attempt < 6:
            time.sleep(wait); wait = min(wait * 2, 60)
    raise RuntimeError(f"gateway {body.get('action')} {body.get('model')} failed 6 times")


def read_v3(t: LinkTable, org: int) -> list[dict]:
    def page(p):
        return _gateway({"action": "read", "model": t.alias, "source_tenant_id": org, "per_page": PER_PAGE, "page": p})
    first = page(1)
    rows = list(first["data"])
    with ThreadPoolExecutor(max_workers=READ_WORKERS) as pool:
        for body in pool.map(page, range(2, first["meta"]["last_page"] + 1)):
            rows += body["data"]
    rows = list({r["id"]: {c: r.get(c) for c in t.v3_cols} for r in rows}.values())
    if len(rows) != first["meta"]["total"]:
        msg = (f"{t.alias}: read {len(rows)} distinct rows, total was {first['meta']['total']} — "
               f"rows are being added while reading; is a migration running?")
        if not ALLOW_GROWTH:
            raise RuntimeError(msg + " (--allow-growth to go on anyway)")
        log.warning("%s — going on (--allow-growth); rows added after this read get linked on a later run", msg)
    return rows


def read_sf(t: LinkTable, facility: str) -> list[dict]:
    with m._snowflake_connect() as conn:
        cur = conn.cursor()
        src = f"{m.sf_schema(facility, 'CLEAN')}.{t.sf_table.upper()}"
        cur.execute(f"SELECT * FROM {src} LIMIT 0")
        have = {d[0].lower() for d in cur.description}
        cols = [c for c in t.sf_cols if c in have]
        cur.execute(f"SELECT {', '.join(cols)} FROM {src}")
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def snapshot(st: State, tables: list[LinkTable], org: int) -> None:
    for t in [TABLES["visits"]] + [t for t in tables if t.name != "visits"]:   # visits always: parent of the rest
        t0 = time.time()
        v3 = read_v3(t, org)
        sf = read_sf(t, st.facility)
        st.dump(f"v3_{t.name}.json", v3)
        st.dump(f"sf_{t.name}.json", sf)
        log.info("snapshot %-14s V3 %7d rows · Snowflake %7d rows (%.0fs)", t.name, len(v3), len(sf), time.time() - t0)


# ─── match ───────────────────────────────────────────────────────────────

def _map_for(id_map: dict, alias: str) -> dict:
    for a in m._alias_variants(alias):
        if id_map.get(a):
            return {int(k) if str(k).isdigit() else k: v for k, v in id_map[a].items()}
    return {}


def match(st: State, tables: list[LinkTable]) -> dict:
    """{table: {"pairs": {v2: v3}, "verified": {v2: v3} (one-to-one, for the
    state), "duplicates": [...], "targets": {v3_id: {col: value}}, "report": {...}}}"""
    id_map = st.read(".migration_id_map.json")
    visit_map = _map_for(id_map, "visits")
    patient_map = _map_for(id_map, "patient")
    sf_visits = {int(r["id"]): r for r in st.load("sf_visits.json")}
    visit_patient = {v: _int(r["patient"]) for v, r in sf_visits.items()}
    visit_patient_file = {int(k): v for k, v in st.read(".migration_visit_patient.json").items()}

    def target_values(t: LinkTable, src: dict) -> dict | None:
        if t.name == "visits":
            pat = patient_map.get(visit_patient.get(int(src["id"])))
            return None if pat is None else {"patient": pat}
        vis = visit_map.get(_int(src["visit"]))
        if vis is None:
            return None
        out = {c: vis for c in t.targets if c in ("visit", "visit_id")}
        if "patient_id" in t.targets:
            v = _int(src["visit"])
            pat = patient_map.get(visit_patient.get(v) or visit_patient_file.get(v))
            if pat is not None:
                out["patient_id"] = pat
        return out

    result = {}
    for t in tables:
        sf = {int(r["id"]): r for r in st.load(f"sf_{t.name}.json")}
        v3 = {r["id"]: r for r in st.load(f"v3_{t.name}.json")}
        key, mp = t.key, _map_for(id_map, t.alias)
        rep = {"v3_rows": len(v3), "snowflake_rows": len(sf), "id_map_entries": len(mp)}

        # 1. id-map pairs whose content matches
        verified, mismatched, gone = {}, 0, 0
        for v2, v3id in mp.items():
            if v2 not in sf:
                continue
            row = v3.get(v3id)
            if row is None:
                gone += 1
            elif key(sf[v2]) == key(row):
                verified[v2] = v3id
            else:
                mismatched += 1
        if len(set(verified.values())) != len(verified):
            raise RuntimeError(f"{t.name}: one V3 row content-matched by several V2 ids")
        rep.update(map_pairs_verified=len(verified), map_pairs_content_mismatch=mismatched,
                   map_pairs_v3_row_missing=gone)

        # 2. V3 rows the map misses: unambiguous content match, or a same-size
        #    group whose V2 rows share one parent (the link is the same for all)
        covered = set(verified.values())
        loose = {i: r for i, r in v3.items() if i not in covered}
        by_key_v2, by_key_v3, all_by_key = {}, {}, {}
        for v2, r in sf.items():
            all_by_key.setdefault(key(r), []).append(v2)
            if v2 not in verified:
                by_key_v2.setdefault(key(r), []).append(v2)
        for i, r in loose.items():
            by_key_v3.setdefault(key(r), []).append(i)
        recovered = {}
        parent = lambda v2: _int(sf[v2][t.parent])
        for k, ids in by_key_v3.items():
            cands = by_key_v2.get(k, [])
            if k[0] is None or len(cands) != len(ids):
                continue
            if len(ids) == 1 or len({parent(c) for c in cands}) == 1:
                recovered.update(zip(sorted(cands), sorted(ids)))
        for v2, i in recovered.items():
            verified[v2] = i
        rep["recovered_by_content"] = len(recovered)

        # 3. what's left: extra copies of an already-matched row (duplicate
        #    inserts) — linkable when all V2 rows with that content share a parent
        taken = set(verified.values())
        key_to_v3 = {key(sf[v2]): i for v2, i in verified.items()}
        extra, duplicates, unidentified = {}, [], []
        n_v3_by_key = Counter(key(r) for r in v3.values())
        for i, r in v3.items():
            if i in taken:
                continue
            k = key(r)
            cands = all_by_key.get(k, [])
            if k[0] is not None and cands and len({parent(c) for c in cands}) == 1:
                extra[i] = cands[0]
                if n_v3_by_key[k] > len(cands):
                    duplicates.append({"v3_id": i, "duplicate_of_v3_id": key_to_v3.get(k), "v2_id": cands[0]})
            else:
                unidentified.append(i)
        rep.update(duplicates=len(duplicates), linked_as_same_parent=len(extra) - len(duplicates),
                   v3_rows_unidentified=len(unidentified), unidentified_sample=sorted(unidentified)[:8])

        # 4. link values per V3 row — only where V3 differs
        targets, unresolved, already = {}, 0, 0
        for v3id, v2 in [(i, v2) for v2, i in verified.items()] + list(extra.items()):
            want = target_values(t, sf[v2])
            if not want:
                unresolved += 1
                continue
            have = v3[v3id]
            diff = {c: v for c, v in want.items() if _int(have.get(c)) != v}
            if diff:
                targets[v3id] = diff
            else:
                already += 1
        rep.update(links_to_set=len(targets), links_already_right=already, parent_not_in_v3=unresolved)

        # stray id-map keys: another link table's V2 ids stored under this alias
        others = set()
        for o in TABLES.values():
            if o.name != t.name and (st.work / f"sf_{o.name}.json").exists():
                others |= {int(r["id"]) for r in st.load(f"sf_{o.name}.json")}
        stray = [k for k in mp if k not in sf and k in others]
        rep["stray_id_map_keys"] = len(stray)
        result[t.name] = {"verified": verified, "duplicates": duplicates, "targets": targets,
                          "stray": stray, "uuids": {v2: v3[i]["uuid"] for v2, i in verified.items()},
                          "report": rep}
        log.info("match %-14s %s", t.name, json.dumps({k: v for k, v in rep.items() if k != "unidentified_sample"}))
    return result


# ─── state repair ────────────────────────────────────────────────────────

def repair_state(st: State, matched: dict, tables: list[LinkTable]) -> None:
    stamp = time.strftime("%Y%m%dT%H%M%S")
    id_map = st.read(".migration_id_map.json")
    progress = st.read(".migration_record_progress.json")
    uuids = st.read(".migration_v3_uuid.json")
    for t in tables:
        res = matched[t.name]
        alias = next((a for a in m._alias_variants(t.alias) if id_map.get(a)), t.alias)
        mp = id_map.setdefault(alias, {})
        before = len(mp)
        for k in res["stray"]:
            mp.pop(str(k), None)
        fixed = 0
        for v2, v3id in res["verified"].items():
            if mp.get(str(v2)) != v3id:
                fixed += 1
            mp[str(v2)] = v3id
        job = f"{st.facility}|sf:{t.sf_table}{t.job_suffix}"
        done = set(progress.get(job, []))
        added = len(set(res["verified"]) - done)
        progress[job] = sorted(done | set(res["verified"]))
        uuids.setdefault(t.alias, {}).update({str(k): v for k, v in res["uuids"].items()})
        log.info("state %-14s id map %d → %d (%d stray removed, %d added/corrected) · progress +%d · uuids %d",
                 t.name, before, len(mp), len(res["stray"]), fixed, added, len(res["uuids"]))
    st.write(".migration_id_map.json", id_map, stamp)
    st.write(".migration_record_progress.json", progress, stamp)
    st.write(".migration_v3_uuid.json", uuids, stamp)
    log.info("state files written (backups *.bak_repair_%s)", stamp)


# ─── apply (adaptive, server-friendly) ───────────────────────────────────

@dataclass
class Pace:
    """How hard apply may push V3."""
    start_workers: int = 2       # parallel requests to begin with
    max_workers: int = 8         # never more than this
    max_rps: float = 15.0        # requests per second ceiling (0 = none)
    slow_seconds: float = 5.0    # a reply slower than this counts as "V3 is struggling"


class Throttle:
    """Adaptive concurrency (additive increase, halve on trouble) + a rate cap.
    Grows by one request slot after a run of fast replies; on an error or a
    slow reply it halves (at most every 10s) and, on errors, pauses all
    requests for 15s so a struggling gateway can recover."""

    def __init__(self, pace: Pace, label: str):
        self.pace, self.label = pace, label
        self.limit = max(1, min(pace.start_workers, pace.max_workers))
        self.active, self.streak = 0, 0
        self.pause_until = self.next_slot = self.last_cut = 0.0
        self.interval = 1.0 / pace.max_rps if pace.max_rps else 0.0
        self.cond = threading.Condition()

    def acquire(self) -> None:
        with self.cond:
            while self.active >= self.limit or time.time() < self.pause_until:
                self.cond.wait(timeout=0.5)
            self.active += 1
            wait = 0.0
            if self.interval:
                now = time.time()
                slot = max(now, self.next_slot)
                self.next_slot = slot + self.interval
                wait = slot - now
        if wait > 0:
            time.sleep(wait)

    def release(self, ok: bool, seconds: float) -> None:
        with self.cond:
            self.active -= 1
            now = time.time()
            if not ok or seconds > self.pace.slow_seconds:
                self.streak = 0
                if not ok:
                    self.pause_until = max(self.pause_until, now + 15)
                if now - self.last_cut > 10 and self.limit > 1:
                    self.limit = max(1, self.limit // 2)
                    self.last_cut = now
                    log.info("  %s: V3 %s — down to %d parallel", self.label,
                             "error" if not ok else f"slow ({seconds:.1f}s)", self.limit)
            else:
                self.streak += 1
                if self.streak >= 20 * self.limit and self.limit < self.pace.max_workers:
                    self.limit += 1
                    self.streak = 0
                    log.info("  %s: V3 answering fast — up to %d parallel", self.label, self.limit)
            self.cond.notify_all()


def apply_links(st: State, t: LinkTable, org: int, pace: Pace = Pace()) -> dict:
    """Post this table's link updates (targets_<table>.json from prepare)."""
    targets = {int(k): v for k, v in st.load(f"targets_{t.name}.json").items()}
    v3 = {r["id"]: r for r in st.load(f"v3_{t.name}.json")}
    done_file = st.work / f"applied_{t.name}.json"
    done = set(json.loads(done_file.read_text())) if done_file.exists() else set()
    queue = [(i, vals, 0) for i, vals in targets.items() if i not in done]
    facility_col = (v2v3._gateway_model_meta.get(t.alias) or {}).get("facility")
    log.info("apply %-14s %d rows to update (%d done earlier) · start %d parallel, max %d, ≤%s req/s",
             t.name, len(queue), len(targets) - len(queue), pace.start_workers, pace.max_workers, pace.max_rps or "∞")
    throttle = Throttle(pace, t.name)
    lock = threading.Lock()
    failed: list[str] = []
    stats = {"ok": 0, "since_flush": 0}
    t0 = time.time()

    def flush():
        v2v3._atomic_write(done_file, json.dumps(sorted(done)))

    def post_once(v3id: int, vals: dict) -> None:
        row = v3[v3id]
        data = {"uuid": row["uuid"], **vals}
        if facility_col:
            data[facility_col] = row.get(facility_col)
        r = v2v3._gateway_post("evaluation", {"action": "insert", "model": t.alias, "destination_tenant_id": org,
                                              "match_on": "uuid", "data": data}, timeout=60)
        if r.status_code == 401:
            v2v3._v3_invalidate_token()
        if r.status_code not in (200, 201):
            raise RuntimeError(f"HTTP {r.status_code} {r.text[:150]}")
        if r.json().get("created"):
            # the uuid matched nothing, so V3 inserted a new row — never carry on
            raise SystemExit(f"{t.name}: V3 row {v3id} was INSERTED instead of updated — stopped")

    def worker():
        while True:
            with lock:
                if not queue:
                    return
                v3id, vals, attempts = queue.pop()
            throttle.acquire()
            start, ok = time.time(), False
            try:
                post_once(v3id, vals)
                ok = True
            except SystemExit:
                throttle.release(False, 0)
                raise
            except Exception as e:
                with lock:
                    if attempts + 1 < 8:
                        queue.insert(0, (v3id, vals, attempts + 1))   # retry later
                    else:
                        failed.append(f"{v3id}: {str(e)[:150]}")
            throttle.release(ok, time.time() - start)
            if ok:
                with lock:
                    done.add(v3id)
                    stats["ok"] += 1
                    stats["since_flush"] += 1
                    if stats["since_flush"] >= 1000:
                        stats["since_flush"] = 0
                        flush()
                        rate = stats["ok"] / max(time.time() - t0, 1)
                        left = len(queue) / rate / 60 if rate else 0
                        log.info("  %s: %d / %d · %.1f/s · %d parallel · ~%.0f min left",
                                 t.name, len(targets) - len(queue) - len(failed) - throttle.active,
                                 len(targets), rate, throttle.limit, left)

    with ThreadPoolExecutor(max_workers=max(1, pace.max_workers)) as pool:
        for f in [pool.submit(worker) for _ in range(max(1, pace.max_workers))]:
            f.result()
    flush()
    log.info("apply %-14s updated %d · failed %d · %.0f min", t.name, stats["ok"], len(failed), (time.time() - t0) / 60)
    return {"updated": stats["ok"], "failed": len(failed), "failures_sample": failed[:3]}


def verify(st: State, t: LinkTable, org: int) -> dict:
    """Re-read V3 and count target rows whose links still differ."""
    targets = st.load(f"targets_{t.name}.json")
    now = {r["id"]: r for r in read_v3(t, org)}
    wrong = [i for i, vals in targets.items() if any(_int(now.get(int(i), {}).get(c)) != v for c, v in vals.items())]
    log.info("verify %-14s %d of %d updated rows still differ%s", t.name, len(wrong), len(targets),
             f" (e.g. {wrong[:5]})" if wrong else "")
    return {"still_wrong": len(wrong), "sample": wrong[:10]}


# ─── phases (the DAG runs each in its own task) ──────────────────────────

def _connect(facility: str) -> tuple[State, int]:
    st = State(facility)
    v2v3.set_v3_target_facility(facility)
    org = v2v3.v3_login_org_cfg()["organization_id"]
    v2v3._fetch_available_models()
    return st, org


def prepare(facility: str, tables: list[str] | None = None, *, execute: bool = False,
            snapshot_first: bool = True) -> dict:
    """snapshot + match (+ with execute: state repair and the per-table
    targets apply works from). Re-running it is safe: rows already fixed in
    V3 simply drop out of the targets."""
    chosen = [TABLES[n] for n in (tables or TABLES)]
    st, org = _connect(facility)
    log.info("facility %s · V3 org %s · tables %s · %s", facility, org, ", ".join(t.name for t in chosen),
             "prepare" if execute else "plan only")
    with ExitStack() as locks:
        for name in sorted({"visits", *(t.name for t in chosen)}) if execute else ():
            locks.enter_context(m.table_lock(TABLES[name].sf_table))
        if snapshot_first:
            snapshot(st, chosen, org)
        matched = match(st, chosen)
        with (st.work / "duplicates.csv").open("w", newline="") as f:
            w = csv.writer(f); w.writerow(["table", "v3_id", "duplicate_of_v3_id", "v2_id"])
            for n, r in matched.items():
                w.writerows([n, d["v3_id"], d["duplicate_of_v3_id"], d["v2_id"]] for d in r["duplicates"])
        if execute:
            repair_state(st, matched, chosen)
            for t in chosen:
                st.dump(f"targets_{t.name}.json", matched[t.name]["targets"])
                (st.work / f"applied_{t.name}.json").unlink(missing_ok=True)   # fresh targets = all still to do
    return {n: r["report"] for n, r in matched.items()}


def apply_table(facility: str, table: str, pace: Pace = Pace(), *, check: bool = True) -> dict:
    """apply (+ verify) one table, holding its migration lock."""
    t = TABLES[table]
    st, org = _connect(facility)
    with m.table_lock(t.sf_table):
        out = {"apply": apply_links(st, t, org, pace)}
        if check:
            out["verify"] = verify(st, t, org)
    return out


def run(facility: str, tables: list[str] | None = None, *, execute: bool = False, snapshot_first: bool = True,
        pace: Pace = Pace()) -> dict:
    report = prepare(facility, tables, execute=execute, snapshot_first=snapshot_first)
    if execute:
        for name in tables or TABLES:
            report[name].update(apply_table(facility, name, pace))
    out = {"report": report}
    State(facility).dump("last_report.json", out)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--facility", required=True)
    ap.add_argument("--tables", nargs="+", choices=list(TABLES))
    ap.add_argument("--execute", action="store_true", help="Repair state and update V3 (default: plan only)")
    ap.add_argument("--allow-growth", action="store_true",
                    help="Go on when V3 rows are added while reading (they get linked on a later run)")
    ap.add_argument("--no-snapshot", action="store_true", help="Reuse the last snapshot")
    ap.add_argument("--resume", action="store_true",
                    help="Skip prepare: continue applying the last prepared targets")
    ap.add_argument("--start-workers", type=int, default=Pace.start_workers)
    ap.add_argument("--max-workers", type=int, default=Pace.max_workers)
    ap.add_argument("--max-rps", type=float, default=Pace.max_rps, help="Requests/second ceiling (0 = none)")
    ap.add_argument("--slow-seconds", type=float, default=Pace.slow_seconds)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    for noisy in ("snowflake.connector", "botocore", "urllib3", "v2_to_v3_migration", "snowflake_to_v3_migration", "facility_pipeline"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    global ALLOW_GROWTH
    ALLOW_GROWTH = args.allow_growth
    pace = Pace(args.start_workers, args.max_workers, args.max_rps, args.slow_seconds)
    try:
        if args.resume:
            out = {"report": {n: apply_table(args.facility, n, pace) for n in args.tables or TABLES}}
        else:
            out = run(args.facility, args.tables, execute=args.execute, snapshot_first=not args.no_snapshot, pace=pace)
    except m.TableBusy as e:
        sys.exit(f"✗ {e}")
    print(json.dumps(out, indent=2, default=str))
    bad = any((r.get("apply") or {}).get("failed") or (r.get("verify") or {}).get("still_wrong")
              for r in out["report"].values())
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
