"""
v3_to_v3_migration.py — copy one V3 organisation into another, gateway to
gateway (the two may be on different V3 instances), so the target mirrors
the source: every row, every field, every link.

  plan     catalogue the source models, order them (parents first), and
           AUDIT the link rules against the source's own data — prints the
           tiers, row counts, and every id column that is unmapped or points
           at the wrong model. Reads only.
  migrate  copy model by model. Each row keeps its uuid (insert match_on
           uuid, so a re-run updates, never duplicates); its id columns are
           remapped to the target's ids through per-model id maps.
  relink   second pass for optional links whose parent wasn't copied yet
           when the row went in (cycles: departments ↔ users …).
  verify   read both sides and compare: every source row present on the
           target by uuid, every field equal (links translated). Exits 1 on
           any difference.

    python v3_to_v3_migration.py --source kisumu_v3 --target org250 plan
    python v3_to_v3_migration.py --source kisumu_v3 --target org250 migrate --record-limit 3
    python v3_to_v3_migration.py --source kisumu_v3 --target org250 migrate --tier 1
    python v3_to_v3_migration.py --source kisumu_v3 --target org250 verify

Each side's account / URLs / org come from AFYA_<KEY>_* (v3_to_v3_client).
State (id maps, held rows, dead letters, reports) lives in
.migration_state/v3v3_<source>_to_<target>/, one file per model, so models
can run in parallel processes.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import secrets
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import v3_to_v3_links as L
from v3_to_v3_client import SERVICES, GatewayError, V3Client

log = logging.getLogger("v3_to_v3")

STATE_ROOT = Path(os.getenv("V3V3_STATE_DIR") or Path(__file__).resolve().parent / ".migration_state")
SAVE_EVERY = 500
_UNKNOWN_COL = re.compile(r"Unknown column '(?:[\w]+\.)?(\w+)'")


# ─── state ──────────────────────────────────────────────────────────────────

def _fname(model: str) -> str:
    return model.replace("/", "__")


def _atomic_write(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, default=str))
    os.replace(tmp, path)


class State:
    """Per-pair state, one file per model and kind — a model's files are only
    written by the process copying that model; others just read them."""

    def __init__(self, source: str, target: str, root: Path = STATE_ROOT):
        self.dir = root / f"v3v3_{source}_to_{target}"
        self._maps: dict[str, dict[str, int]] = {}
        self._lock = threading.Lock()

    def _path(self, kind: str, model: str, ext: str = "json") -> Path:
        return self.dir / kind / f"{_fname(model)}.{ext}"

    def id_map(self, model: str, reload: bool = False) -> dict[str, int]:
        with self._lock:
            if reload or model not in self._maps:
                p = self._path("id_map", model)
                self._maps[model] = json.loads(p.read_text()) if p.exists() else {}
            return self._maps[model]

    def save_map(self, model: str) -> None:
        with self._lock:
            _atomic_write(self._path("id_map", model), self._maps.get(model, {}))

    def load(self, kind: str, model: str, default):
        p = self._path(kind, model)
        return json.loads(p.read_text()) if p.exists() else default

    def save(self, kind: str, model: str, data) -> None:
        _atomic_write(self._path(kind, model), data)

    def dead_letter(self, model: str, row: dict, error: str) -> None:
        p = self._path("dead_letters", model, "jsonl")
        p.parent.mkdir(parents=True, exist_ok=True)
        safe = {k: v for k, v in row.items() if k not in ("password", "remember_token", "api_token")}
        with self._lock, p.open("a") as f:
            f.write(json.dumps({"at": time.strftime("%Y-%m-%d %H:%M:%S"), "error": error[:2000], "row": safe},
                               default=str) + "\n")


# ─── catalogue + plan ───────────────────────────────────────────────────────

def catalogue(src: V3Client) -> dict[str, dict]:
    """model → {service, alias, tenant, facility, ops} for every source model."""
    out = {}
    for svc in SERVICES:
        try:
            for m in src.models(svc):
                out[f"{svc}/{m['alias']}"] = {"service": svc, "alias": m["alias"], "tenant": m.get("tenant"),
                                              "facility": m.get("facility"), "ops": m.get("operations") or []}
        except GatewayError as e:
            log.warning("source %s: %s", svc, e)
    return out


def model_kind(model: str, meta: dict, cols: set[str]) -> str:
    """copy | org_rows | parent_rows | reference | skip"""
    if model in L.SKIP_MODELS:
        return "skip"
    if meta.get("tenant"):
        return "copy"
    if model in L.REFERENCE_MATCH:
        return "reference"
    if "organization_id" in cols:
        return "org_rows"          # no tenant column on the gateway, but rows carry the org
    if L.critical_cols(model):
        return "parent_rows"       # rows belong to the org through their parent
    return "reference"


def model_links(model: str, cols) -> dict[str, str]:
    """id column → target for this model's columns (DROP / FACILITY included)."""
    out = {}
    for c in cols:
        t = L.link_target(model, c)
        if t:
            out[c] = t
    return out


def tiers(models: dict[str, dict]) -> dict[str, int]:
    """model → tier (1 = no parents). Critical links always order parent
    first; an optional link is ignored where it would close a cycle (the
    relink pass fills it in afterwards)."""
    level: dict[str, int] = {}
    visiting: set[str] = set()

    def visit(m: str) -> int:
        if m in level:
            return level[m]
        if m in visiting:
            return 0
        visiting.add(m)
        crit = set(L.critical_cols(m))
        best = 0
        for col, dep in models[m]["links"].items():
            if dep in (L.FACILITY, L.DROP) or dep == m or dep not in models or models[dep]["kind"] == "skip":
                continue
            if dep in visiting:
                if col in crit:
                    raise ValueError(f"critical link cycle: {m}.{col} → {dep}")
                continue                                   # optional back edge: relink later
            best = max(best, visit(dep))
        visiting.discard(m)
        level[m] = best + 1
        return level[m]

    for m in models:
        visit(m)
    return level


def build_plan(src: V3Client, only: list[str] | None = None, exclude: list[str] | None = None) -> dict:
    cat = catalogue(src)
    models: dict[str, dict] = {}
    for model, meta in sorted(cat.items()):
        if "read" not in meta["ops"]:
            continue
        try:
            rows, page_meta = src.read_page(meta["service"], meta["alias"], 1, per_page=50)
        except GatewayError as e:
            models[model] = {**meta, "kind": "error", "error": str(e)[:300], "links": {}, "cols": [], "count": None}
            continue
        cols = sorted({c for r in rows for c in r})
        kind = model_kind(model, meta, set(cols))
        if not rows and kind != "skip":
            kind = "empty"
        models[model] = {**meta, "kind": kind, "count": page_meta.get("total", len(rows)), "cols": cols,
                         "links": model_links(model, cols),
                         "unmapped": [c for c in cols if L.looks_like_id(c) and L.link_target(model, c) is None
                                      and c not in L.MODEL_LINKS.get(model, {})]}
    selected = {m for m, x in models.items() if x["kind"] in ("copy", "org_rows", "parent_rows", "reference")}
    if only:
        selected &= set(only)
    if exclude:
        selected -= set(exclude)
    live = {m: x for m, x in models.items() if x["kind"] not in ("error", "empty")}
    lv = tiers(live)
    for m, x in models.items():
        x["tier"] = lv.get(m)
        x["selected"] = m in selected
    return {"source": src.key, "organization_id": src.organization_id, "models": models}


def audit_links(src: V3Client, plan: dict, sample: int = 2000, id_cache: dict | None = None) -> dict:
    """For each link of each selected model: how many of a sample of source
    values are ids of the model the rule points at (in the source org). A
    wrong rule shows up as a low rate. Returns {model: {col: {...}}}."""
    models = plan["models"]
    ids: dict[str, set[str]] = id_cache if id_cache is not None else {}

    def ids_of(target: str) -> set[str]:
        if target not in ids:
            t = models.get(target)
            if not t or t["kind"] in ("error", "empty"):
                ids[target] = set()
            else:
                ids[target] = {str(r["id"]) for r in src.read_all(t["service"], t["alias"]) if r.get("id") is not None}
        return ids[target]

    out = {}
    for model, x in sorted(models.items()):
        if not x.get("selected"):
            continue
        rows = []
        for r in src.read_all(x["service"], x["alias"]):
            rows.append(r)
            if len(rows) >= sample:
                break
        res = {}
        for col, target in x["links"].items():
            vals = [str(r[col]) for r in rows if r.get(col) not in (None, "", 0, "0")]
            if target in (L.FACILITY, L.DROP):
                res[col] = {"target": target, "non_null": len(vals)}
                continue
            known = ids_of(target)
            hit = sum(v in known for v in vals)
            res[col] = {"target": target, "non_null": len(vals), "resolvable": hit,
                        "rate": round(hit / len(vals), 3) if vals else None,
                        "missing_examples": [v for v in vals if v not in known][:5]}
        for col in x.get("unmapped", []):
            vals = [r[col] for r in rows if r.get(col) not in (None, "", 0, "0")]
            res[col] = {"target": None, "non_null": len(vals), "examples": vals[:5]}
        out[model] = res
    return out


# ─── copy ───────────────────────────────────────────────────────────────────

class Copier:
    def __init__(self, src: V3Client, dst: V3Client, state: State, plan: dict, *, workers: int = 4,
                 record_limit: int = 0):
        self.src, self.dst, self.state, self.plan = src, dst, state, plan
        self.models = plan["models"]
        self.workers = workers
        self.record_limit = record_limit
        self._dropped_cols: dict[str, set[str]] = {}

    # rows of the source org, with the model's own filter
    def source_rows(self, model: str):
        x = self.models[model]
        for r in self.src.read_all(x["service"], x["alias"]):
            if x["kind"] == "org_rows" and str(r.get("organization_id")) != str(self.src.organization_id):
                continue
            yield r

    def target_uuids(self, model: str) -> dict[str, int]:
        x = self.models[model]
        out = {}
        try:
            for r in self.dst.read_all(x["service"], x["alias"]):
                if r.get("uuid") and r.get("id") is not None:
                    out[str(r["uuid"]).lower()] = r["id"]
        except GatewayError as e:
            log.warning("%s: can't read target rows (%s) — relying on match_on uuid", model, e)
        return out

    def translate(self, model: str, row: dict) -> tuple[dict, list[str], dict[str, object]]:
        """(payload, unresolved critical links, unresolved optional links {col: source value})."""
        x = self.models[model]
        crit = set(L.critical_cols(model))
        out, missing_crit, deferred = {}, [], {}
        for col, val in row.items():
            if col in L.NOT_COPIED:
                continue
            target = x["links"].get(col)
            if target is None or val in (None, "") or isinstance(val, bool):
                out[col] = val
                continue
            if target == L.FACILITY:
                out[col] = self.dst.facility_id
            elif target == L.DROP:
                out[col] = None
            else:
                mapped = self.state.id_map(target).get(str(val))
                if mapped is None and target == model:
                    mapped = self.state.id_map(model).get(str(val))
                if mapped is not None:
                    out[col] = mapped
                elif col in crit:
                    missing_crit.append(col)
                    out[col] = None
                else:
                    deferred[col] = val
                    out[col] = None
        if model == L.USERS:
            # the gateway never returns passwords: each copied account gets a
            # random one and must reset it at first login
            out["password"] = secrets.token_urlsafe(18)
            out["enforce_password_reset"] = 1
        if x.get("facility") and self.dst.facility_id is not None:
            out[x["facility"]] = self.dst.facility_id
        for c in self._dropped_cols.get(model, ()):
            out.pop(c, None)
        return out, missing_crit, deferred

    def _insert(self, model: str, payload: dict) -> tuple[int | None, str | None]:
        x = self.models[model]
        for _ in range(8):
            r = self.dst.insert(x["service"], x["alias"], payload)
            if r.ok:
                try:
                    j = r.json()
                except ValueError:
                    return None, None
                d = j.get("data") if isinstance(j.get("data"), dict) else {}
                return (j.get("id") or d.get("id")), None
            m = _UNKNOWN_COL.search(r.text)
            if m and m.group(1) in payload:            # the target's schema lacks this column
                self._dropped_cols.setdefault(model, set()).add(m.group(1))
                log.warning("%s: target has no column %s — not copied", model, m.group(1))
                payload = {k: v for k, v in payload.items() if k != m.group(1)}
                continue
            return None, f"{r.status_code} {r.text[:1500]}"
        return None, "too many unknown columns"

    def copy_model(self, model: str) -> dict:
        x = self.models[model]
        if x["kind"] == "reference":
            return self.link_reference(model)
        t0 = time.time()
        idm = self.state.id_map(model)
        on_target = self.target_uuids(model)
        held = {}
        deferred_all = self.state.load("deferred", model, {})
        src_ids: list[str] = []
        counts = {"read": 0, "already": 0, "posted": 0, "held": 0, "foreign": 0, "failed": 0, "deferred": 0}
        lock = threading.Lock()
        new = 0

        def post(row, payload, deferred):
            new_id, err = self._insert(model, payload)
            with lock:
                if err:
                    counts["failed"] += 1
                    self.state.dead_letter(model, payload, err)
                    return
                if new_id is not None:
                    idm[str(row["id"])] = new_id
                counts["posted"] += 1
                if deferred:
                    deferred_all[str(row["id"])] = deferred
                    counts["deferred"] += 1
                if counts["posted"] % SAVE_EVERY == 0:
                    # checkpoint: a crash loses at most the rows since here, and
                    # those are found on the target by uuid on the next run
                    self.state.save_map(model)
                    self.state.save("deferred", model, deferred_all)

        with ThreadPoolExecutor(self.workers) as pool:
            futures = []
            for row in self.source_rows(model):
                counts["read"] += 1
                sid = str(row.get("id"))
                payload, missing_crit, deferred = self.translate(model, row)
                if missing_crit:
                    # model without a tenant column: a parent outside the source
                    # org means the row is another org's
                    if x["kind"] == "parent_rows" and not self._parent_in_source(model, row, missing_crit):
                        counts["foreign"] += 1
                        continue
                    held[sid] = {c: row.get(c) for c in missing_crit}
                    counts["held"] += 1
                    src_ids.append(sid)
                    continue
                src_ids.append(sid)
                uuid = str(row.get("uuid") or "").lower()
                if uuid and uuid in on_target:
                    # already on the target (earlier run, or posted just before a
                    # crash): never re-posted — a full re-post would overwrite the
                    # row (a user's password …). Links still missing go to relink,
                    # which patches only those columns.
                    idm[sid] = on_target[uuid]
                    counts["already"] += 1
                    if deferred:
                        with lock:
                            deferred_all[sid] = deferred
                    continue
                if self.record_limit and new >= self.record_limit:
                    continue
                new += 1
                futures.append(pool.submit(post, row, payload, deferred))
            for f in futures:
                f.result()
        self.state.save_map(model)
        self.state.save("held", model, held)
        self.state.save("deferred", model, deferred_all)
        self.state.save("src_ids", model, src_ids)
        counts["seconds"] = round(time.time() - t0)
        counts["status"] = ("failed" if counts["failed"] else "waiting" if counts["held"] else
                            "canary" if self.record_limit else "done")
        self.state.save("report", model, counts)
        log.info("%s: %s", model, counts)
        return counts

    def _parent_in_source(self, model: str, row: dict, cols: list[str]) -> bool:
        x = self.models[model]
        for c in cols:
            parent = x["links"].get(c)
            if parent and str(row.get(c)) in set(self.state.load("src_ids", parent, [])):
                return True
        return False

    def link_reference(self, model: str) -> dict:
        """Shared reference rows: the target's own row (uuid, then the match
        fields); inserted only when the target has none."""
        x = self.models[model]
        idm = self.state.id_map(model)
        fields = L.REFERENCE_MATCH.get(model, ("code", "name"))
        by_uuid, by_key = {}, {}
        for r in self.dst.read_all(x["service"], x["alias"]):
            if r.get("uuid"):
                by_uuid[str(r["uuid"]).lower()] = r["id"]
            for f in fields:
                if r.get(f) not in (None, ""):
                    by_key.setdefault((f, str(r[f]).strip().lower()), r["id"])
        counts = {"read": 0, "linked": 0, "posted": 0, "failed": 0}
        for row in self.source_rows(model):
            counts["read"] += 1
            sid = str(row["id"])
            hit = by_uuid.get(str(row.get("uuid") or "").lower())
            for f in fields:
                if hit is None and row.get(f) not in (None, ""):
                    hit = by_key.get((f, str(row[f]).strip().lower()))
            if hit is not None:
                idm[sid] = hit
                counts["linked"] += 1
                continue
            if self.record_limit and counts["posted"] >= self.record_limit:
                continue
            payload, _, _ = self.translate(model, row)
            new_id, err = self._insert(model, payload)
            if err:
                counts["failed"] += 1
                self.state.dead_letter(model, payload, err)
            else:
                idm[sid] = new_id
                counts["posted"] += 1
        self.state.save_map(model)
        counts["status"] = "failed" if counts["failed"] else "done"
        self.state.save("report", model, counts)
        log.info("%s (reference): %s", model, counts)
        return counts

    def relink(self, model: str) -> dict:
        """Optional links left null because the parent came later: post just
        those columns (match_on uuid updates the row in place)."""
        x = self.models[model]
        deferred = self.state.load("deferred", model, {})
        if not deferred:
            return {"relinked": 0, "still_missing": 0}
        uuids = {}
        for r in self.source_rows(model):
            if str(r["id"]) in deferred:
                uuids[str(r["id"])] = r.get("uuid")
        left, done = {}, 0
        for sid, cols in deferred.items():
            patch, missing = {"uuid": uuids.get(sid)}, {}
            for col, val in cols.items():
                mapped = self.state.id_map(x["links"][col], reload=True).get(str(val))
                if mapped is None:
                    missing[col] = val
                else:
                    patch[col] = mapped
            if len(patch) > 1 and patch["uuid"]:
                _, err = self._insert(model, patch)
                if err:
                    self.state.dead_letter(model, patch, "relink: " + err)
                    missing.update({c: cols[c] for c in patch if c != "uuid"})
                else:
                    done += 1
            if missing:
                left[sid] = missing
        self.state.save("deferred", model, left)
        res = {"relinked": done, "still_missing": len(left)}
        log.info("%s relink: %s", model, res)
        return res


# ─── verify ─────────────────────────────────────────────────────────────────

def _norm(v):
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, default=str)
    s = str(v).strip()
    if re.fullmatch(r"-?\d+(\.\d+)?", s):
        return float(s)
    if s.lower() in ("true", "false"):
        return int(s.lower() == "true")
    m = re.fullmatch(r"(\d{4}-\d\d-\d\d)[T ](\d\d:\d\d:\d\d)(\.\d+)?Z?", s)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    if s.startswith(("{", "[")):
        try:
            return json.dumps(json.loads(s), sort_keys=True, default=str)
        except ValueError:
            pass
    return s


def verify_model(copier: Copier, model: str, examples: int = 5, canary: bool = False) -> dict:
    """canary: only the rows a canary run put on the target are compared
    (missing rows aren't counted; at least one must be there)."""
    x = copier.models[model]
    if x["kind"] == "reference":
        src_ids = [str(r["id"]) for r in copier.source_rows(model)]
        idm = copier.state.id_map(model, reload=True)
        miss = [i for i in src_ids if i not in idm]
        if canary:
            miss = []
        return {"source": len(src_ids), "linked": len(src_ids) - len(miss), "missing": len(miss),
                "missing_examples": miss[:examples], "ok": not miss}
    dst = {}
    for r in copier.dst.read_all(x["service"], x["alias"]):
        if r.get("uuid"):
            dst[str(r["uuid"]).lower()] = r
    held = copier.state.load("held", model, {})
    excluded = set()
    try:
        excluded = set(copier.src.describe(x["service"], x["alias"]).get("excluded") or [])
    except GatewayError:
        pass
    n_src = missing = 0
    mismatch: dict[str, int] = {}
    miss_ex, mm_ex = [], {}
    seen = set()
    for row in copier.source_rows(model):
        payload, missing_crit, _ = copier.translate(model, row)
        if missing_crit and x["kind"] == "parent_rows" and not copier._parent_in_source(model, row, missing_crit):
            continue                                            # another org's row
        n_src += 1
        uuid = str(row.get("uuid") or "").lower()
        seen.add(uuid)
        got = dst.get(uuid)
        if got is None and canary:
            n_src -= 1
            continue
        if got is None:
            missing += 1
            if len(miss_ex) < examples:
                miss_ex.append({"id": row.get("id"), "uuid": uuid, "held": str(row.get("id")) in held})
            continue
        for col, want in payload.items():
            if col in L.NOT_COMPARED or col in excluded or col not in got:
                continue
            if _norm(want) != _norm(got.get(col)):
                mismatch[col] = mismatch.get(col, 0) + 1
                mm_ex.setdefault(col, [])
                if len(mm_ex[col]) < examples:
                    mm_ex[col].append({"uuid": uuid, "expected": want, "target": got.get(col)})
    extra = len(set(dst) - seen)
    return {"source": n_src, "target": len(dst), "missing": missing, "extra_on_target": extra,
            "mismatched_fields": mismatch, "missing_examples": miss_ex, "mismatch_examples": mm_ex,
            "ok": missing == 0 and not mismatch and (n_src > 0 or not canary)}


# ─── entry points ───────────────────────────────────────────────────────────

def clients(source: str, target: str) -> tuple[V3Client, V3Client]:
    return V3Client(source), V3Client(target)


def load_or_build_plan(src: V3Client, state: State, models=None, exclude=None, refresh: bool = False) -> dict:
    p = state.dir / "plan.json"
    if p.exists() and not refresh:
        plan = json.loads(p.read_text())
        for m, x in plan["models"].items():
            x["selected"] = (x["kind"] in ("copy", "org_rows", "parent_rows", "reference")
                             and (not models or m in models) and (not exclude or m not in exclude))
        return plan
    plan = build_plan(src, models, exclude)
    _atomic_write(p, plan)
    return plan


def print_plan(plan: dict, audit: dict | None = None) -> list[str]:
    """Logs the plan; returns the problems (empty = every link accounted for)."""
    problems = []
    models = plan["models"]
    for tier in sorted({x["tier"] for x in models.values() if x.get("tier")}):
        names = [f"{m}({models[m]['count']})" for m in sorted(models) if models[m].get("tier") == tier
                 and models[m]["selected"]]
        if names:
            log.info("tier %d — %s", tier, ", ".join(names))
    for m, x in sorted(models.items()):
        if x["kind"] in ("error", "skip", "empty") or not x["selected"]:
            log.info("not copied: %s — %s", m, x.get("error") or L.SKIP_MODELS.get(m) or x["kind"])
    if audit:
        for m, cols in sorted(audit.items()):
            for col, a in cols.items():
                if a["target"] is None and a["non_null"]:
                    problems.append(f"{m}.{col}: id-like column with values but no link rule ({a['examples']})")
                elif a.get("rate") is not None and a["rate"] < 0.95:
                    problems.append(f"{m}.{col} → {a['target']}: only {a['resolvable']}/{a['non_null']} values are "
                                    f"ids there (e.g. {a['missing_examples']})")
    for p in problems:
        log.warning("LINK: %s", p)
    return problems


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("command", choices=["plan", "migrate", "relink", "verify"])
    ap.add_argument("--models", nargs="*", default=None, help="service/alias … (default: all)")
    ap.add_argument("--exclude", nargs="*", default=None)
    ap.add_argument("--tier", type=int, default=None)
    ap.add_argument("--record-limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--refresh-plan", action="store_true")
    ap.add_argument("--audit-sample", type=int, default=2000)
    ap.add_argument("--canary", action="store_true", help="verify: compare only the rows on the target")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")

    src, dst = clients(a.source, a.target)
    state = State(a.source, a.target)
    if a.command == "plan":
        plan = load_or_build_plan(src, state, a.models, a.exclude, refresh=True)
        audit = audit_links(src, plan, sample=a.audit_sample)
        state.save("audit", "all", audit)
        problems = print_plan(plan, audit)
        return 1 if problems else 0
    plan = load_or_build_plan(src, state, a.models, a.exclude, refresh=a.refresh_plan)
    copier = Copier(src, dst, state, plan, workers=a.workers, record_limit=a.record_limit)
    todo = sorted((m for m, x in plan["models"].items() if x["selected"] and (a.tier is None or x["tier"] == a.tier)),
                  key=lambda m: (plan["models"][m]["tier"], m))
    bad = 0
    for m in todo:
        if a.command == "migrate":
            res = copier.copy_model(m)
            bad += res["status"] == "failed"
        elif a.command == "relink":
            copier.relink(m)
        else:
            res = verify_model(copier, m, canary=a.canary)
            state.save("verify", m, res)
            log.info("verify %s: %s", m, {k: v for k, v in res.items() if not k.endswith("examples")})
            bad += not res["ok"]
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
