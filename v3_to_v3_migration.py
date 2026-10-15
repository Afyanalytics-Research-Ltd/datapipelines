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
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import v3_to_v3_links as L
from v3_to_v3_client import SERVICES, GatewayError, V3Client

log = logging.getLogger("v3_to_v3")

STATE_ROOT = Path(os.getenv("V3V3_STATE_DIR") or Path(__file__).resolve().parent / ".migration_state")
CHECKPOINT_SECONDS = 30     # id map / deferred links flushed to disk at least this often
READ_WORKERS = int(os.getenv("V3V3_READ_WORKERS") or 8)   # parallel page reads per table
# a model stops (state saved, rest left for the re-run) after this many POSTs
# in a row fail at the transport / 5xx level, or when its first this-many
# rows are all rejected — the target is down or the schema is wrong, and
# posting the rest would only fill the dead letters
MAX_ERROR_STREAK = 50
_UNKNOWN_COL = re.compile(r"Unknown column '(?:[\w]+\.)?(\w+)'")
_UUID_KEY = "__uuid__"      # deferred links: the row's uuid, so relink needn't re-read the source
# The gateway serialises dates in UTC ('2026-10-05T21:00:00.000000Z' for a
# DATE 2026-10-06 in Nairobi) but stores what it is sent as wall-clock time:
# posting the UTC text back shifts every date a day and every time 3 h
# earlier. So dates go out as the source's wall-clock time.
SOURCE_TZ = ZoneInfo(os.getenv("V3V3_SOURCE_TZ") or "Africa/Nairobi")
_ISO_UTC = re.compile(r"^(\d{4}-\d\d-\d\d)T(\d\d:\d\d:\d\d)(?:\.\d+)?Z$")


def to_wall_time(v):
    """'2026-10-05T21:00:00.000000Z' → '2026-10-06' (midnight) / 'YYYY-MM-DD HH:MM:SS'
    in SOURCE_TZ; anything else unchanged."""
    m = _ISO_UTC.match(v) if isinstance(v, str) else None
    if not m:
        return v
    t = datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}+00:00").astimezone(SOURCE_TZ)
    return t.strftime("%Y-%m-%d") if (t.hour, t.minute, t.second) == (0, 0, 0) else t.strftime("%Y-%m-%d %H:%M:%S")


def from_wall_time(v):
    """The other way, for comparing what we sent with what the gateway serialises."""
    if not isinstance(v, str):
        return v
    m = re.fullmatch(r"(\d{4}-\d\d-\d\d)(?: (\d\d:\d\d:\d\d))?", v)
    if not m:
        return v
    t = datetime.fromisoformat(f"{m.group(1)}T{m.group(2) or '00:00:00'}").replace(tzinfo=SOURCE_TZ)
    return t.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S.000000Z")


TIMESTAMPS: set[str] = set()       # (all fields are compared and patched now)


# ─── state ──────────────────────────────────────────────────────────────────

def _fname(model: str) -> str:
    return model.replace("/", "__")


def _atomic_write(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{threading.get_ident()}.tmp")
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
            snapshot = dict(self._maps.get(model, {}))   # POST threads keep adding while it's written
        _atomic_write(self._path("id_map", model), snapshot)

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


def load_v2_map(source: str, root: Path = STATE_ROOT) -> dict[str, dict[str, int]]:
    """The V2 → V3 load's id map for the source org (only the aliases
    L.V2_KEYED uses): V2 id → source V3 id. Empty if there is none."""
    p = Path(os.getenv("V3V3_V2_ID_MAP") or root / source / ".migration_id_map.json")
    if not p.exists():
        return {}
    want = set(L.V2_KEYED.values())
    full = json.loads(p.read_text())
    out = {a: {str(k): v for k, v in (full.get(a) or {}).items() if v is not None} for a in want}
    log.info("V2→V3 id map %s: %s", p, {a: len(m) for a, m in out.items()})
    return out


def repair_v2(v2_map: dict, model: str, col: str, val):
    """(value to link with, was it repaired): a V2-keyed column's V2 id →
    the source's V3 id, or None when the map lacks it (never the raw value:
    a V2 id can collide with an unrelated V3 id)."""
    alias = L.V2_KEYED.get((model, col))
    if alias is None or val in (None, "") or isinstance(val, bool):
        return val, False
    return (v2_map.get(alias) or {}).get(str(val)), True


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


def build_plan(src: V3Client, only: list[str] | None = None, exclude: list[str] | None = None,
               workers: int = 16) -> dict:
    cat = catalogue(src)
    readable = sorted(m for m, meta in cat.items() if "read" in meta["ops"])

    def first_page(model):
        meta = cat[model]
        try:
            return src.read_page(meta["service"], meta["alias"], 1, per_page=50)
        except GatewayError as e:
            return e

    with ThreadPoolExecutor(workers) as pool:                 # ~230 models, one read each
        pages = dict(zip(readable, pool.map(first_page, readable)))
    models: dict[str, dict] = {}
    for model in readable:
        meta, got = cat[model], pages[model]
        if isinstance(got, GatewayError):
            models[model] = {**meta, "kind": "error", "error": str(got)[:300], "links": {}, "cols": [], "count": None}
            continue
        rows, page_meta = got
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


def audit_links(src: V3Client, plan: dict, sample: int = 2000, id_cache: dict | None = None,
                v2_map: dict | None = None) -> dict:
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
                ids[target] = {str(r["id"]) for r in src.read_all(t["service"], t["alias"], workers=READ_WORKERS)
                               if r.get("id") is not None}
        return ids[target]

    out = {}
    for model, x in sorted(models.items()):
        if not x.get("selected"):
            continue
        # a model with no tenant column serves every org's rows: only the ones
        # hanging off this org's parents are this org's (and audited) — the
        # rest would read as broken links
        mine = None
        if x["kind"] == "parent_rows":
            crit = {c: x["links"][c] for c in L.critical_cols(model)
                    if x["links"].get(c) not in (None, L.FACILITY, L.DROP)}
            parents = {c: ids_of(t) for c, t in crit.items()}
            mine = lambda r: any(str(r.get(c)) in ids for c, ids in parents.items())
        rows, scanned = [], 0
        for r in src.read_all(x["service"], x["alias"], workers=min(READ_WORKERS, -(-sample // 500))):
            scanned += 1
            if mine is None or mine(r):
                rows.append(r)
            if len(rows) >= sample or scanned >= sample * 20:
                break
        res = {}
        for col, target in x["links"].items():
            vals = [str(r[col]) for r in rows if r.get(col) not in (None, "", 0, "0")]
            if target in (L.FACILITY, L.DROP):
                res[col] = {"target": target, "non_null": len(vals)}
                continue
            known = ids_of(target)
            v2 = (model, col) in L.V2_KEYED
            if v2:                                     # V2 ids: judged after the V2→V3 map
                vals = [str(repair_v2(v2_map or {}, model, col, v)[0]) for v in vals]
            hit = sum(v in known for v in vals)
            res[col] = {"target": target, "non_null": len(vals), "resolvable": hit,
                        "rate": round(hit / len(vals), 3) if vals else None,
                        "missing_examples": [v for v in vals if v not in known][:5],
                        **({"v2_keyed": True} if v2 else {}),
                        **({"accepted": L.ACCEPT_UNRESOLVED[(model, col)]}
                           if (model, col) in L.ACCEPT_UNRESOLVED else {})}
        for col in x.get("unmapped", []):
            vals = [r[col] for r in rows if r.get(col) not in (None, "", 0, "0")]
            res[col] = {"target": None, "non_null": len(vals), "examples": vals[:5]}
        out[model] = res
    return out


_ERR_HINTS: list[tuple[re.Pattern, str]] = [(re.compile(p, re.I), h) for p, h in (
    (r"restricted to superadmin|Forbidden", "the target account lacks the superadmin role on that instance — "
                                            "ask for it (or use an account that has it), then re-run"),
    (r"Duplicate entry '(?P<v>[^']*)' for key '(?P<k>[^']*)'",
     "the target already has a row with {v!r} in unique key {k} (under another uuid) — it was copied before "
     "by something else, or the source has two rows with the same key"),
    (r"Column '(?P<c>\w+)' cannot be null", "the target requires {c}, but it is empty here — in the source, or "
                                            "because the row it links to isn't on the target (see held/deferred)"),
    (r"foreign key constraint fails .*REFERENCES `(?P<t>\w+)`", "the row links to a {t} row that isn't on the "
                                                                "target — copy that model first"),
    (r"Unknown column '(?:\w+\.)?(?P<c>\w+)'", "the target's schema has no column {c} (older/newer V3 build)"),
    (r"Data too long for column '(?P<c>\w+)'", "{c} is longer than the target's column allows"),
    (r"Out of range value for column '(?P<c>\w+)'", "{c} is out of the target column's range"),
    (r"Incorrect (?:datetime|date|integer|decimal) value: '(?P<v>[^']*)' for column '(?P<c>\w+)'",
     "{c} = {v!r} isn't a valid value for the target's column type"),
    (r"Field '(?P<c>\w+)' doesn't have a default value", "the target requires {c}, which the source row lacks"),
    (r"Table '(?P<t>[\w.]+)' doesn't exist", "the table {t} doesn't exist on that instance's database"),
    (r"is not registered", "the target gateway has no such model (service not deployed / older build)"),
    (r"\b401\b|Unauthenticated", "the login token was refused — check the account's password"),
    (r"Timeout|timed out|ConnectionError|Connection refused|RemoteDisconnected",
     "the target didn't answer (network / overloaded) — re-run; rows already in are matched by uuid"),
    (r"^5\d\d\b", "the target's server failed on this row (HTTP 5xx) — see its message"),
)]


def describe_error(err: str) -> tuple[str, str]:
    """(V3's own message, what it means) for a gateway error text like
    '422 {"success":false,"message":"…"}' or an exception string."""
    status, _, body = str(err).partition(" ")
    msg = body
    try:
        j = json.loads(body)
        msg = j.get("message") or j.get("error") or body
        if isinstance(j.get("errors"), dict):        # Laravel validation: field → [messages]
            msg += " — " + "; ".join(f"{k}: {' '.join(v) if isinstance(v, list) else v}"
                                     for k, v in j["errors"].items())
    except ValueError:
        msg = str(err)
    msg = re.sub(r"\s*\(SQL: .*", "", " ".join(str(msg).split()))[:400]
    if status.isdigit():
        msg = f"HTTP {status}: {msg}"
    for pat, hint in _ERR_HINTS:
        m = pat.search(str(err))
        if m:
            return msg, hint.format(**{k: v for k, v in m.groupdict().items()})
    return msg, "see V3's message"


def _reason_key(msg: str) -> str:
    """The message with values / ids stripped, so rows failing the same way group together."""
    s = re.sub(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "<uuid>", msg)
    s = re.sub(r"'[^']{0,60}' for key", "'…' for key", s)
    return re.sub(r"(?<![\w])\d+(?![\w])", "N", s)[:300]


def held_summary(x: dict, held: dict) -> dict:
    """{column: {rows, parent model, example parent ids}} for rows held because
    a parent they can't exist without isn't on the target."""
    out: dict[str, dict] = {}
    for cols in held.values():
        for c, v in cols.items():
            e = out.setdefault(c, {"rows": 0, "parent": x["links"].get(c), "missing_parent_ids": []})
            e["rows"] += 1
            if len(e["missing_parent_ids"]) < 5 and v not in e["missing_parent_ids"]:
                e["missing_parent_ids"].append(v)
    return out


def describe_held(model: str, held_because: dict) -> str:
    return "\n".join(
        f"  • {h['rows']} row(s): {model}.{c} → {h['parent']} ids {h['missing_parent_ids']} are not on the target "
        f"— that model hasn't been copied yet, failed, or those parents aren't the source org's (copy "
        f"{h['parent']} first, then re-run this model)" for c, h in held_because.items())


def describe_result(model: str, res: dict, state: "State") -> str:
    """A model's copy outcome in words: what went in, what didn't, and why."""
    head = (f"{model}: read {res.get('read', 0)}, posted {res.get('posted', 0)}, already on target "
            f"{res.get('already', res.get('linked', 0))}, failed {res.get('failed', 0)}, held {res.get('held', 0)}, "
            f"other org's rows skipped {res.get('foreign', 0)}")
    parts = [head]
    if res.get("aborted"):
        parts.append(f"STOPPED EARLY: {res['aborted']}")
    if res.get("errors"):
        parts.append(f"{res['failed']} row(s) refused by the target:")
        for e in res["errors"][:6]:
            parts.append(f"  • {e['count']} row(s) [{e['stage']}] — {e['meaning']}\n"
                         f"      V3 said: {e['v3_message']}\n"
                         f"      e.g. source id {e['example']['source_id']}, uuid {e['example']['uuid']}")
    if res.get("held_because"):
        parts.append(f"{res['held']} row(s) held — not posted because a parent they need isn't on the target:")
        parts.append(describe_held(model, res["held_because"]))
    if res.get("v2_ids_not_in_map"):
        parts.append("copied with a null link — the source row held a V2 id the V2→V3 id map doesn't have: " +
                     ", ".join(f"{c} ×{n}" for c, n in res["v2_ids_not_in_map"].items()))
    if res.get("failed") or res.get("held"):
        parts.append(f"every refused row, with V3's full message: {state._path('dead_letters', model, 'jsonl')}; "
                     f"held rows: {state._path('held', model)}")
    return "\n".join(parts)


def describe_verify(model: str, res: dict) -> str:
    parts = [f"{model}: target differs from source — {res.get('missing', 0)} of {res.get('source', 0)} source "
             f"row(s) missing on the target, {sum((res.get('mismatched_fields') or {}).values())} field "
             f"difference(s), {res.get('extra_on_target', 0)} target row(s) not in the source"]
    for ex in res.get("missing_examples") or []:
        why = "held: its parent isn't on the target" if ex.get("held") else "never posted, or refused (see dead letters)"
        parts.append(f"  • missing: source id {ex.get('id')}, uuid {ex.get('uuid')} — {why}")
    for col, n in sorted((res.get("mismatched_fields") or {}).items(), key=lambda kv: -kv[1]):
        exs = (res.get("mismatch_examples") or {}).get(col) or []
        e = exs[0] if exs else {}
        parts.append(f"  • {col}: differs on {n} row(s) — e.g. uuid {e.get('uuid')}: source {e.get('expected')!r}, "
                     f"target {e.get('target')!r}")
    return "\n".join(parts)


class _Hint:
    def format(self, c) -> str:
        return (f"the gateway refuses {c.key}'s account (user {getattr(c, 'user_id', None)}, org "
                f"{c.organization_id}, facility {c.facility_id}): it needs the superadmin role on that instance")


FORBIDDEN_HINT = _Hint()


def _forbidden(e: Exception) -> bool:
    return " 403 " in str(e) or "restricted to superadmin" in str(e)


def preflight(client: V3Client, services=SERVICES) -> None:
    """Before anything moves: the account can read and describe on every
    service it will touch. Raises GatewayError naming what's refused."""
    client.token()
    refused = []
    for svc in services:
        try:
            models = client.models(svc)
        except GatewayError as e:
            if _forbidden(e):
                refused.append(f"{svc} (list)")
            continue                                   # service not deployed there: the plan says so
        if not models:
            continue
        alias = models[0]["alias"]
        try:
            client.read_page(svc, alias, 1, per_page=1)
            client.describe(svc, alias)
        except GatewayError as e:
            if _forbidden(e):
                refused.append(f"{svc}/{alias}")
            else:
                log.warning("preflight %s %s/%s: %s", client.key, svc, alias, e)
    if refused:
        raise GatewayError(f"{FORBIDDEN_HINT.format(c=client)} — refused: {', '.join(refused)}")
    log.info("preflight %s: gateway access ok on %s", client.key, ", ".join(services))


# ─── copy ───────────────────────────────────────────────────────────────────

class Copier:
    def __init__(self, src: V3Client, dst: V3Client, state: State, plan: dict, *, workers: int = 4,
                 record_limit: int = 0, read_workers: int = READ_WORKERS, v2_map: dict | None = None):
        self.src, self.dst, self.state, self.plan = src, dst, state, plan
        self.v2_map = v2_map or {}
        self.unrepaired: dict[str, int] = {}          # "model.col" → V2 ids the map lacked (copied as null)
        self.errors: dict[str, dict[str, dict]] = {}  # model → cause → {count, v3_message, meaning, example}
        self.models = plan["models"]
        self.workers = max(1, workers)
        self.read_workers = max(1, read_workers)
        self.record_limit = record_limit
        self.map_only = False      # True: rebuild id maps from the target by uuid, post nothing (read-only audit)
        self._dropped_cols: dict[str, set[str]] = {}
        self._src_id_sets: dict[str, set[str]] = {}
        self._lock = threading.Lock()

    # rows of the source org, with the model's own filter
    def source_rows(self, model: str):
        x = self.models[model]
        for r in self.src.read_all(x["service"], x["alias"], workers=self.read_workers):
            if x["kind"] == "org_rows" and str(r.get("organization_id")) != str(self.src.organization_id):
                continue
            yield r

    def target_rows(self, model: str):
        x = self.models[model]
        return self.dst.read_all(x["service"], x["alias"], workers=self.read_workers)

    def target_uuids(self, model: str) -> dict[str, int]:
        out = {}
        try:
            for r in self.target_rows(model):
                if r.get("uuid") and r.get("id") is not None:
                    out[str(r["uuid"]).lower()] = r["id"]
        except GatewayError as e:
            if _forbidden(e):        # it can't insert either — every row would only be dead-lettered
                raise GatewayError(f"{model}: {e} — {FORBIDDEN_HINT.format(c=self.dst)}") from e
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
                out[col] = to_wall_time(val)
                continue
            if target == L.FACILITY:
                out[col] = self.dst.facility_id
            elif target == L.DROP:
                out[col] = None
            else:
                val, v2 = repair_v2(self.v2_map, model, col, val)
                if v2 and val is None:                # a V2 id nothing maps: never linked to a guess
                    k = f"{model}.{col}"
                    self.unrepaired[k] = self.unrepaired.get(k, 0) + 1
                    out[col] = None
                    if col in crit:
                        missing_crit.append(col)
                    continue
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
                with self._lock:
                    self._dropped_cols.setdefault(model, set()).add(m.group(1))
                log.warning("%s: target has no column %s — not copied", model, m.group(1))
                payload = {k: v for k, v in payload.items() if k != m.group(1)}
                continue
            return None, f"{r.status_code} {r.text[:1500]}"
        return None, "too many unknown columns"

    def _post_pool(self, model: str):
        return _PostPool(self, model)

    def record_error(self, model: str, row: dict, payload: dict, err: str, stage: str = "insert") -> None:
        """Dead-letter the row and count its cause (V3's message + what it
        means + one example row) for the task's failure message."""
        msg, meaning = describe_error(err)
        self.state.dead_letter(model, {"_source_id": row.get("id"), **payload}, f"{stage}: {msg} — {meaning}")
        with self._lock:
            e = self.errors.setdefault(model, {}).setdefault(_reason_key(f"{stage}: {msg}"), {
                "count": 0, "stage": stage, "v3_message": msg, "meaning": meaning,
                "example": {"source_id": row.get("id"), "uuid": payload.get("uuid") or row.get("uuid")}})
            e["count"] += 1

    def error_summary(self, model: str, top: int = 5) -> str:
        """Multi-line: each cause, how many rows, V3's message, what it means, an example."""
        errs = sorted((self.errors.get(model) or {}).values(), key=lambda e: -e["count"])
        lines = []
        for e in errs[:top]:
            lines.append(f"  • {e['count']} row(s) [{e['stage']}] — {e['meaning']}\n"
                         f"      V3 said: {e['v3_message']}\n"
                         f"      e.g. source id {e['example']['source_id']}, uuid {e['example']['uuid']}")
        if len(errs) > top:
            lines.append(f"  • … and {len(errs) - top} more cause(s)")
        return "\n".join(lines)

    def copy_model(self, model: str) -> dict:
        """Read the source org's rows (pages in parallel), translate, and POST
        the new ones with `workers` in flight; a row already on the target
        (by uuid) is only mapped. The id map and deferred links are flushed at
        least every CHECKPOINT_SECONDS and always on the way out, so a crash
        or abort loses nothing the re-run can't find on the target by uuid."""
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
        new = 0
        last_save = time.time()

        def save():
            self.state.save_map(model)
            with pool.lock:
                snapshot = dict(deferred_all)
            self.state.save("deferred", model, snapshot)

        def done(row, payload, deferred, new_id, err):          # runs on a POST thread, under pool.lock
            sid = str(row["id"])
            if err:
                counts["failed"] += 1
                self.record_error(model, row, payload, err)
                return
            if new_id is not None:
                idm[sid] = new_id
            counts["posted"] += 1
            if deferred:
                deferred_all[sid] = {**deferred, _UUID_KEY: row.get("uuid")}
                counts["deferred"] += 1

        pool = self._post_pool(model)
        complete = False
        try:
            for row in self.source_rows(model):
                if pool.aborted:
                    break
                counts["read"] += 1
                sid = str(row.get("id"))
                uuid = str(row.get("uuid") or "").lower()
                payload, missing_crit, deferred = self.translate(model, row)
                there = (uuid and uuid in on_target) or (not uuid and sid in idm)
                if missing_crit and not there:
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
                if there:
                    # on the target already, whatever its parents' state: always
                    # paired (its children link through this map)
                    # already on the target (earlier run, or posted just before a
                    # crash): never re-posted — a full re-post would overwrite the
                    # row (a user's password …). Links still missing go to relink,
                    # which patches only those columns.
                    with pool.lock:
                        if uuid:
                            idm[sid] = on_target[uuid]
                        counts["already"] += 1
                        if deferred:
                            deferred_all[sid] = {**deferred, _UUID_KEY: row.get("uuid")}
                    continue
                if self.map_only or (self.record_limit and new >= self.record_limit):
                    continue
                new += 1
                pool.submit(row, payload, deferred, done)
                if time.time() - last_save > CHECKPOINT_SECONDS:
                    save()
                    last_save = time.time()
                    log.info("%s: %s (%.0f rows/s)", model, counts, counts["read"] / (last_save - t0))
            else:
                complete = True
        finally:
            pool.close()
            save()
            if complete:
                # only a full read: children (parent_rows) and verify take a
                # parent id missing from this list as another org's row
                self.state.save("held", model, held)
                self.state.save("src_ids", model, src_ids)
        counts["seconds"] = round(time.time() - t0)
        v2_null = {k.split(".", 1)[1]: n for k, n in self.unrepaired.items() if k.startswith(model + ".")}
        if v2_null:
            counts["v2_ids_not_in_map"] = v2_null          # linked as null: the source held a V2 id
        if held:
            counts["held_because"] = held_summary(x, held)
        if self.errors.get(model):
            counts["errors"] = sorted(self.errors[model].values(), key=lambda e: -e["count"])
        if pool.aborted:
            counts["aborted"] = pool.aborted
        counts["status"] = ("failed" if counts["failed"] or pool.aborted else "waiting" if counts["held"] else
                            "canary" if self.record_limit else "done")
        self.state.save("report", model, counts)
        log.info("%s: %s", model, counts)
        if pool.aborted:
            raise GatewayError(f"{model}: stopped — {pool.aborted}")
        return counts

    def _parent_in_source(self, model: str, row: dict, cols: list[str]) -> bool:
        x = self.models[model]
        for c in cols:
            parent = x["links"].get(c)
            if not parent:
                continue
            ids = self._src_id_sets.get(parent)
            if ids is None:
                with self._lock:
                    ids = self._src_id_sets.get(parent)
                    if ids is None:
                        ids = self._src_id_sets[parent] = set(self.state.load("src_ids", parent, []))
            if str(row.get(c)) in ids:
                return True
        return False

    def link_reference(self, model: str) -> dict:
        """Shared reference rows: the target's own row (uuid, then the match
        fields); inserted only when the target has none."""
        idm = self.state.id_map(model)
        fields = L.REFERENCE_MATCH.get(model, ("code", "name"))
        by_uuid, by_key = {}, {}
        for r in self.target_rows(model):
            if r.get("uuid"):
                by_uuid[str(r["uuid"]).lower()] = r["id"]
            for f in fields:
                if r.get(f) not in (None, ""):
                    by_key.setdefault((f, str(r[f]).strip().lower()), r["id"])
        counts = {"read": 0, "linked": 0, "posted": 0, "failed": 0}

        def done(row, payload, deferred, new_id, err):
            if err:
                counts["failed"] += 1
                self.record_error(model, row, payload, err)
            else:
                idm[str(row["id"])] = new_id
                counts["posted"] += 1

        pool = self._post_pool(model)
        posting = 0
        try:
            for row in self.source_rows(model):
                if pool.aborted:
                    break
                counts["read"] += 1
                sid = str(row["id"])
                hit = by_uuid.get(str(row.get("uuid") or "").lower())
                for f in fields:
                    if hit is None and row.get(f) not in (None, ""):
                        hit = by_key.get((f, str(row[f]).strip().lower()))
                if hit is not None:
                    with pool.lock:
                        idm[sid] = hit
                        counts["linked"] += 1
                    continue
                if self.map_only or (self.record_limit and posting >= self.record_limit):
                    continue
                posting += 1
                payload, _, _ = self.translate(model, row)
                pool.submit(row, payload, None, done)
        finally:
            pool.close()
            self.state.save_map(model)
        counts["status"] = "failed" if counts["failed"] or pool.aborted else "done"
        self.state.save("report", model, counts)
        log.info("%s (reference): %s", model, counts)
        if pool.aborted:
            raise GatewayError(f"{model}: stopped — {pool.aborted}")
        return counts

    def relink(self, model: str) -> dict:
        """Optional links left null because the parent came later: post just
        those columns (match_on uuid updates the row in place)."""
        x = self.models[model]
        deferred = self.state.load("deferred", model, {})
        if not deferred:
            return {"relinked": 0, "still_missing": 0}
        uuids = {sid: cols.get(_UUID_KEY) for sid, cols in deferred.items()}
        if any(not u for u in uuids.values()):            # state from before uuids were kept
            for r in self.source_rows(model):
                sid = str(r["id"])
                if sid in uuids and not uuids[sid]:
                    uuids[sid] = r.get("uuid")
        # each parent's map read from disk once (other processes wrote them)
        targets = {x["links"][c] for cols in deferred.values() for c in cols if c != _UUID_KEY}
        maps = {t: dict(self.state.id_map(t, reload=True)) for t in targets}
        left: dict[str, dict] = {}
        relinked = [0]

        def done(row, patch, cols, _new_id, err):
            if err:
                self.record_error(model, row, patch, err, stage="relink")
                left.setdefault(row["id"], {_UUID_KEY: patch["uuid"]}).update(
                    {c: cols[c] for c in patch if c != "uuid"})
            else:
                relinked[0] += 1

        pool = self._post_pool(model)
        try:
            for sid, cols in deferred.items():
                if pool.aborted:
                    left[sid] = cols
                    continue
                patch, missing = {"uuid": uuids.get(sid)}, {}
                for col, val in cols.items():
                    if col == _UUID_KEY:
                        continue
                    # a link deferred by an older run may hold a V2 id: repaired the same way as on copy
                    fixed, v2 = repair_v2(self.v2_map, model, col, val)
                    if v2 and fixed is None:
                        missing[col] = val
                        continue
                    mapped = maps[x["links"][col]].get(str(fixed))
                    if mapped is None:
                        missing[col] = val
                    else:
                        patch[col] = mapped
                if missing:
                    left[sid] = {**missing, _UUID_KEY: uuids.get(sid)}
                if len(patch) > 1 and patch["uuid"]:
                    pool.submit({"id": sid}, patch, cols, done)
        finally:
            pool.close()
            self.state.save("deferred", model, left)
        res = {"relinked": relinked[0], "still_missing": len(left)}
        log.info("%s relink: %s", model, res)
        if pool.aborted:
            raise GatewayError(f"{model} relink: stopped — {pool.aborted}")
        return res


class _PostPool:
    """`workers` POSTs in flight for one model, at most 4×workers queued (so a
    big table streams instead of piling up in memory). Each result goes to the
    caller's `done(row, payload, extra, new_id, err)` under `lock`. Stops
    taking work (`aborted` says why) after MAX_ERROR_STREAK transport / 5xx
    failures in a row, or when the first MAX_ERROR_STREAK rows all fail."""

    def __init__(self, copier: Copier, model: str):
        self.copier, self.model = copier, model
        self.lock = threading.Lock()
        self.aborted: str | None = None
        self._slots = threading.BoundedSemaphore(copier.workers * 4)
        self._pool = ThreadPoolExecutor(copier.workers, thread_name_prefix=f"post-{model.split('/')[-1]}")
        self._streak = self._ok = self._bad = 0

    def submit(self, row: dict, payload: dict, extra, done) -> None:
        self._slots.acquire()
        if self.aborted:
            self._slots.release()
            return
        self._pool.submit(self._run, row, payload, extra, done)

    def _run(self, row, payload, extra, done):
        try:
            if self.aborted:
                return
            systemic = False
            try:
                new_id, err = self.copier._insert(self.model, payload)
                systemic = bool(err) and err[:1] == "5"
            except Exception as e:                         # transport failure after the client's retries
                new_id, err, systemic = None, f"{type(e).__name__}: {e}", True
            with self.lock:
                done(row, payload, extra, new_id, err)
                if err:
                    self._bad += 1
                    self._streak = self._streak + 1 if systemic else 0
                else:
                    self._ok += 1
                    self._streak = 0
                if not self.aborted:
                    if self._streak or (self._bad >= MAX_ERROR_STREAK and not self._ok):
                        msg, meaning = describe_error(err)
                    if self._streak >= MAX_ERROR_STREAK:
                        self.aborted = (f"{self._streak} POSTs in a row failed — {meaning}. Last answer: {msg}. "
                                        f"Stopped so the rest isn't dead-lettered; nothing is lost — re-run once "
                                        f"the target is back")
                    elif self._bad >= MAX_ERROR_STREAK and not self._ok:
                        self.aborted = (f"the first {self._bad} rows were all rejected — {meaning}. V3 said: {msg}. "
                                        f"Stopped: posting the rest would fail the same way")
                    if self.aborted:
                        log.error("%s: %s — stopping this model", self.model, self.aborted)
        finally:
            self._slots.release()

    def close(self) -> None:
        self._pool.shutdown(wait=True)


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


def verify_model(copier: Copier, model: str, examples: int = 5, canary: bool = False, collect: bool = False) -> dict:
    """canary: only the rows a canary run put on the target are compared
    (missing rows aren't counted; at least one must be there). collect: also
    return `_patches` {uuid: {uuid, differing fields…}} for sync_fields."""
    x = copier.models[model]
    if x["kind"] == "reference":
        src_ids = [str(r["id"]) for r in copier.source_rows(model)]
        idm = copier.state.id_map(model, reload=True)
        miss = [i for i in src_ids if i not in idm]
        if canary:
            miss = []
        return {"source": len(src_ids), "linked": len(src_ids) - len(miss), "missing": len(miss),
                "missing_examples": miss[:examples], "ok": not miss}
    # target rows kept compact (a tuple of normalised values per uuid): the
    # biggest tables are ~0.5M rows and several verify at once
    absent = object()
    keys: list[str] = []
    idx: dict[str, int] = {}
    tkeys: list[str] = []
    tidx: dict[str, int] = {}
    fac_diff: dict[str, int] = {}
    fac_ex: dict[str, dict] = {}
    dst: dict[str, tuple] = {}
    n_target = 0
    for r in copier.target_rows(model):
        n_target += 1
        if not keys:
            keys = sorted(set(r) - L.NOT_COMPARED)
            idx = {k: i for i, k in enumerate(keys)}
            tkeys = sorted(set(r) & TIMESTAMPS)
            tidx = {k: i for i, k in enumerate(tkeys)}
        if r.get("uuid"):
            dst[str(r["uuid"]).lower()] = (tuple(_norm(r[k]) if k in r else absent for k in keys),
                                           tuple(_norm(r[k]) if k in r else absent for k in tkeys))
    held = copier.state.load("held", model, {})
    dead = _dead_letter_uuids(copier.state, model)
    excluded = set()
    try:
        excluded = set(copier.src.describe(x["service"], x["alias"]).get("excluded") or [])
    except GatewayError:
        pass
    n_src = missing = no_uuid = 0
    why_missing = {"held: a parent it needs isn't on the target": 0, "refused by the target (dead-lettered)": 0,
                   "not posted yet (new in the source since the copy, or a canary run)": 0}
    mismatch: dict[str, int] = {}
    miss_ex, mm_ex = [], {}
    patches: dict[str, dict] = {}
    seen = set()
    for row in copier.source_rows(model):
        payload, missing_crit, _ = copier.translate(model, row)
        if missing_crit and x["kind"] == "parent_rows" and not copier._parent_in_source(model, row, missing_crit):
            continue                                            # another org's row
        n_src += 1
        uuid = str(row.get("uuid") or "").lower()
        if not uuid:
            no_uuid += 1
            continue
        seen.add(uuid)
        got_pair = dst.get(uuid)
        got, got_ts = got_pair if got_pair is not None else (None, None)
        if got is None and canary:
            n_src -= 1
            continue
        if got is None:
            missing += 1
            sid = str(row.get("id"))
            reason = (list(why_missing)[0] if sid in held else list(why_missing)[1] if uuid in dead
                      else list(why_missing)[2])
            why_missing[reason] += 1
            if len(miss_ex) < examples:
                miss_ex.append({"id": row.get("id"), "uuid": uuid, "held": sid in held, "why": reason,
                                **({"held_for": held[sid]} if sid in held else {})})
            continue
        row_patch = None
        for col, sent in payload.items():
            if (col in L.NOT_COMPARED or col in excluded or col not in idx or got[idx[col]] is absent
                    or L.is_instance_derived(col)
                    or (isinstance(sent, dict) and "uuid" in sent)):     # an embedded related record, not a column
                continue
            # a link: what was sent (target ids); a plain value: the source's own
            # (sent dates are wall-clock text, the gateway serialises them in UTC)
            # (a value the copier sets on purpose — a user's enforce_password_reset — is compared as sent)
            want = (row.get(col) if not x["links"].get(col) and col in row and to_wall_time(row.get(col)) == sent
                    else sent)
            if _norm(want) == got[idx[col]]:
                continue
            if col == x.get("facility") or x["links"].get(col) == L.FACILITY:
                # the target gateway stamps its own facility — not patchable through it
                fac_diff[col] = fac_diff.get(col, 0) + 1
                fac_ex.setdefault(col, {"sent": sent, "target": got[idx[col]]})
                continue
            mismatch[col] = mismatch.get(col, 0) + 1
            if collect:
                if row_patch is None:
                    row_patch = patches.setdefault(uuid, {"uuid": row.get("uuid"), "_source_id": row.get("id")})
                    if "updated_at" in payload:     # else the update itself would stamp a new updated_at
                        row_patch["updated_at"] = payload["updated_at"]
                row_patch[col] = sent
            mm_ex.setdefault(col, [])
            if len(mm_ex[col]) < examples:
                mm_ex[col].append({"uuid": uuid, "expected": want, "target": got[idx[col]]})
    extra = len(set(dst) - seen)
    out = {"source": n_src, "target": n_target, "missing": missing, "extra_on_target": extra,
           "facility_differs": fac_diff, "facility_example": fac_ex,
           "source_without_uuid": no_uuid, "missing_why": {k: v for k, v in why_missing.items() if v},
           "mismatched_fields": mismatch, "mismatched_rows": len(patches) if collect else None,
           "missing_examples": miss_ex, "mismatch_examples": mm_ex,
           "ok": missing == 0 and not mismatch and (n_src > 0 or not canary)}
    if collect:
        out["_patches"] = patches
    return out


def _dead_letter_uuids(state: "State", model: str) -> set[str]:
    p = state._path("dead_letters", model, "jsonl")
    out = set()
    if p.exists():
        for line in p.read_text().splitlines():
            try:
                u = (json.loads(line).get("row") or {}).get("uuid")
            except ValueError:
                continue
            if u:
                out.add(str(u).lower())
    return out


def sync_fields(copier: Copier, model: str, patches: dict[str, dict]) -> dict:
    """Rows on both sides whose fields differ: post just those fields
    (match_on uuid updates the row in place) so the target matches."""
    if not patches:
        return {"patched": 0, "failed": 0}
    counts = {"patched": 0, "failed": 0}

    def done(row, patch, _extra, _id, err):
        if err:
            counts["failed"] += 1
            copier.record_error(model, row, patch, err, stage="sync")
        else:
            counts["patched"] += 1

    pool = copier._post_pool(model)
    try:
        for p in patches.values():
            row = {"id": p.pop("_source_id", None), "uuid": p["uuid"]}
            pool.submit(row, p, None, done)
    finally:
        pool.close()
    log.info("%s sync: %s", model, counts)
    return counts


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
    for m, why in sorted(getattr(L, "NOT_IN_GATEWAY", {}).items()):
        log.warning("not copied (no gateway model — copy with SQL): %s — %s", m, why)
    if audit:
        for m, cols in sorted(audit.items()):
            for col, a in cols.items():
                if a["target"] is None and a["non_null"]:
                    problems.append(f"{m}.{col}: id-like column with values but no link rule ({a['examples']})")
                elif a.get("rate") is not None and a["rate"] < 0.95:
                    msg = (f"{m}.{col} → {a['target']}: only {a['resolvable']}/{a['non_null']} values are ids there "
                           f"(e.g. {a['missing_examples']})")
                    if a.get("v2_keyed") or a.get("accepted"):
                        # the source's own data: copied as null / held, and counted
                        log.warning("SOURCE DATA: %s — %s", msg, a.get("accepted") or
                                    "V2 ids the V2→V3 id map doesn't have")
                    else:
                        problems.append(msg)
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
    ap.add_argument("--workers", type=int, default=16, help="POSTs in flight per model")
    ap.add_argument("--read-workers", type=int, default=READ_WORKERS, help="page reads in flight per table")
    ap.add_argument("--parallel-models", type=int, default=1,
                    help="models of the same tier run at once (tiers still run in order)")
    ap.add_argument("--refresh-plan", action="store_true")
    ap.add_argument("--audit-sample", type=int, default=2000)
    ap.add_argument("--canary", action="store_true", help="verify: compare only the rows on the target")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")

    src, dst = clients(a.source, a.target)
    state = State(a.source, a.target)
    if a.command == "plan":
        plan = load_or_build_plan(src, state, a.models, a.exclude, refresh=True)
        audit = audit_links(src, plan, sample=a.audit_sample, v2_map=load_v2_map(a.source))
        state.save("audit", "all", audit)
        problems = print_plan(plan, audit)
        return 1 if problems else 0
    if a.command in ("migrate", "relink"):
        preflight(src)
        preflight(dst)                 # a refused account stops here, before a row is posted
    plan = load_or_build_plan(src, state, a.models, a.exclude, refresh=a.refresh_plan)
    copier = Copier(src, dst, state, plan, workers=a.workers, record_limit=a.record_limit,
                    v2_map=load_v2_map(a.source),
                    read_workers=a.read_workers)
    todo = sorted((m for m, x in plan["models"].items() if x["selected"] and (a.tier is None or x["tier"] == a.tier)),
                  key=lambda m: (plan["models"][m]["tier"], m))

    def run(m) -> bool:
        """True = this model is fine."""
        try:
            if a.command == "migrate":
                res = copier.copy_model(m)
                if res["status"] in ("failed", "waiting"):
                    (log.error if res["status"] == "failed" else log.warning)("%s", describe_result(m, res, state))
                return res["status"] != "failed"
            if a.command == "relink":
                copier.relink(m)
                return True
            res = verify_model(copier, m, canary=a.canary)
            state.save("verify", m, res)
            log.info("verify %s: %s", m, {k: v for k, v in res.items() if not k.endswith("examples")})
            if not res["ok"]:
                log.error("%s", describe_verify(m, res))
            return res["ok"]
        except GatewayError as e:
            log.error("%s: %s\n%s", m, e, copier.error_summary(m))
            return False

    bad = 0
    if a.command == "relink" or a.parallel_models <= 1:
        bad = sum(not run(m) for m in todo)
    else:
        # a tier's models share no links, so they can run side by side; the
        # next tier starts only when this one is through (it needs the maps)
        for tier in sorted({plan["models"][m]["tier"] for m in todo}):
            batch = [m for m in todo if plan["models"][m]["tier"] == tier]
            with ThreadPoolExecutor(a.parallel_models) as pool:
                bad += sum(not ok for ok in pool.map(run, batch))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
