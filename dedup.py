#!/usr/bin/env python3
"""
dedup.py — find duplicates in one hospital's <NAME>_RAW records (as flattened into
CLEAN tables): merge the certain ones, list the uncertain ones for review. Rules
are per V3 table (dedup_rules.py); RAW is never changed.

Writes to HOSPITALS.DEDUP, shared by all hospitals (FACILITY column):
  CROSSWALK  records merged away → their survivor (replaced each run)
  REVIEW     uncertain pairs (replaced each run); undecided pairs migrate as two records
  LOG        every decision with its evidence (replaced each run)
  RUNS       rows in / out per table per run
  DECISIONS  the team's answers to review pairs ('same' / 'different'), used next run
snowflake_flatten_clean runs it between working out RAW's fields and building CLEAN, and
CLEAN leaves out the records merged away (crosswalk_select).

  python dedup.py --facility wema_medical [--tables patients visits] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path

import dedup_rules as dr

log = logging.getLogger("dedup")

DB = "HOSPITALS"
OUTPUT_SCHEMA = os.getenv("DEDUP_OUTPUT_SCHEMA", f"{DB}.DEDUP")

BLOCK_MAX = 300          # a value shared by more records than this is too common to compare on
PHONE_SHARED_MAX = 5     # a phone used under more names than this (a family's) is not identity
ID_SHARED_MAX = 2        # an ID number used under more names than this (a parent's) is not identity
CONFLICT_CHECK_MAX = 60  # at most this many members of each cluster are checked before a join

# ─── NORMALISATION (matching only) ──────────────────────────────────────────

_TITLES = {"mr", "mrs", "ms", "miss", "dr", "prof", "rev", "sr", "jr", "mstr", "master"}
_PLACEHOLDER_NAMES = {"unknown", "unk", "walkin", "walk", "in", "anonymous", "anon", "test", "patient",
                      "client", "na", "nil", "none", "null", "noname", "xxx", "x", "otc", "pharmacy",
                      "counter", "customer", "cash", "sale", "sales", "over", "the"}


def _blank(v) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def norm_text(v) -> str:
    """Lower case, accents dropped, whitespace collapsed."""
    if _blank(v):
        return ""
    s = unicodedata.normalize("NFKD", str(v)).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s).strip().lower()


def norm_key(v) -> str:
    """Letters and digits only: "H. Pylori Ag" == "h pylori ag" == "HPyloriAg"."""
    return re.sub(r"[^a-z0-9]", "", norm_text(v))


def name_tokens(*parts) -> tuple[str, ...]:
    """Sorted name words, titles dropped: "Wanjiru, Mary  W." == "mary w wanjiru"."""
    words = re.sub(r"[^a-z]+", " ", norm_text(" ".join(str(p) for p in parts if not _blank(p)))).split()
    return tuple(sorted(w for w in words if w not in _TITLES))


def norm_phone(v) -> str | None:
    """Last 9 digits (+254 712…, 0712…, 712… are one number); junk → None."""
    digits = re.sub(r"\D", "", "" if v is None else str(v))
    if len(digits) < 9 or len(set(digits[-9:])) <= 2:      # "07", "0700000000"
        return None
    return digits[-9:]


def norm_gender(v) -> str | None:
    s = norm_text(v)
    if s in ("m", "male", "1", "man", "boy"):
        return "m"
    if s in ("f", "female", "2", "woman", "girl"):
        return "f"
    return None


def norm_id(v) -> str | None:
    s = re.sub(r"[^A-Z0-9]", "", norm_text(v).upper())
    return s if len(s) >= 5 and len(set(s)) > 2 else None


@lru_cache(maxsize=200_000)
def _parse_ts(s: str) -> datetime | None:
    if not re.match(r"^\d{4}-\d{1,2}-\d{1,2}", s) or s.startswith("0000"):
        try:
            from dateutil import parser
            return parser.parse(s, dayfirst=True) if len(s) >= 8 and re.search(r"\d{4}", s) else None
        except (ValueError, OverflowError, ImportError):
            return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00").replace(" ", "T", 1))
    except ValueError:
        return None


def norm_ts(v) -> datetime | None:
    """Any timestamp-ish value → naive datetime (aware ones in UTC)."""
    if _blank(v):
        return None
    if isinstance(v, datetime):
        ts = v
    elif isinstance(v, date):
        ts = datetime(v.year, v.month, v.day)
    else:
        ts = _parse_ts(str(v).strip())
    if ts is not None and ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    return ts


def norm_dob(v) -> date | None:
    ts = norm_ts(v)
    if ts is None or ts.year < 1900 or ts.date() > date.today():
        return None
    return ts.date()


def jaro_winkler(a: str, b: str) -> float:
    if a == b:
        return 1.0
    la, lb = len(a), len(b)
    if not la or not lb:
        return 0.0
    window = max(max(la, lb) // 2 - 1, 0)
    ma, mb = [False] * la, [False] * lb
    matches = 0
    for i, ch in enumerate(a):
        for j in range(max(0, i - window), min(lb, i + window + 1)):
            if not mb[j] and b[j] == ch:
                ma[i] = mb[j] = True
                matches += 1
                break
    if not matches:
        return 0.0
    bs = [b[j] for j in range(lb) if mb[j]]
    transpositions = sum(1 for i, j in zip((a[i] for i in range(la) if ma[i]), bs) if i != j) / 2
    jaro = (matches / la + matches / lb + (matches - transpositions) / matches) / 3
    prefix = 0
    for x, y in zip(a[:4], b[:4]):
        if x != y:
            break
        prefix += 1
    return jaro + prefix * 0.1 * (1 - jaro)


def _word_match(x: str, y: str) -> bool:
    return x == y or (len(x) >= 4 and len(y) >= 4 and jaro_winkler(x, y) >= 0.92)


def names_relation(a: tuple[str, ...], b: tuple[str, ...]) -> str | None:
    """"equal": the same 2+ words in any order. "compatible": every word of
    the shorter name (2+ words) matches a different word of the longer one,
    allowing small typos — a dropped or added name, "Wanjku" for "Wanjiru".
    Twins ("Faith Wanjiru" / "Hope Wanjiru") are neither."""
    if len(a) < 2 or len(b) < 2:
        return None
    if a == b:
        return "equal"
    short, long_ = (a, b) if len(a) <= len(b) else (b, a)
    left = list(long_)
    for w in short:
        hit = next((i for i, x in enumerate(left) if _word_match(w, x)), None)
        if hit is None:
            return None
        left.pop(hit)
    return "compatible"


def _id_order(v) -> tuple:
    s = str(v)
    return (0, int(s), "") if s.isdigit() else (1, 0, s)


def _pick(row: dict, fields, columns) -> object:
    """The first of `fields` this table has (and this row fills)."""
    for f in fields:
        if f in columns and not _blank(row.get(f)):
            return row.get(f)
    return None


def _present(fields, columns) -> list[str]:
    return [f for f in fields if f in columns]


# ─── CLUSTERS ───────────────────────────────────────────────────────────────

@dataclass
class Edge:
    table: str
    a: str
    b: str
    kind: str
    rule: str
    action: str                     # auto | review | human_same | blocked_by_decision | blocked_conflict
    evidence: dict = field(default_factory=dict)


class Clusters:
    """Union-find over record ids. Two clusters only join when no pair across
    them conflicts (or a reviewer said 'different'), so A~B and B~C can't
    pull A and C together when A and C disagree on date of birth."""

    def __init__(self, ids, conflict=None):
        self.parent = {i: i for i in ids}
        self.members = {i: [i] for i in ids}
        self.conflict = conflict or (lambda x, y: False)

    def find(self, i: str) -> str:
        while self.parent[i] != i:
            self.parent[i] = self.parent[self.parent[i]]
            i = self.parent[i]
        return i

    def union(self, a: str, b: str, *, force: bool = False) -> bool:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return True
        if not force:
            for x in self.members[ra][:CONFLICT_CHECK_MAX]:
                for y in self.members[rb][:CONFLICT_CHECK_MAX]:
                    if self.conflict(x, y):
                        return False
        if len(self.members[ra]) < len(self.members[rb]):
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.members[ra] += self.members.pop(rb)
        return True


# ─── MATCHERS ───────────────────────────────────────────────────────────────

@dataclass
class _Person:
    id: str
    tokens: tuple
    dob: date | None
    dob_approx: bool
    gender: str | None
    phone: str | None
    ids: dict
    when: datetime | None
    placeholder: bool


def _people(rows, rule, columns) -> list[_Person]:
    name_parts = _present([f for f in rule["name"] if f not in ("full_name", "name")], columns)
    whole = _present([f for f in rule["name"] if f in ("full_name", "name")], columns)
    out = []
    for r in rows:
        tokens = name_tokens(*(r.get(f) for f in name_parts))
        if not tokens:
            tokens = name_tokens(*(r.get(f) for f in whole[:1]))
        out.append(_Person(
            id=r["id"], tokens=tokens, dob=norm_dob(_pick(r, rule["dob"], columns)), dob_approx=False,
            gender=norm_gender(_pick(r, rule["gender"], columns)),
            phone=norm_phone(_pick(r, rule["phone"], columns)),
            ids={f: norm_id(r.get(f)) for f in _present(rule.get("strong_ids", ()), columns) if norm_id(r.get(f))},
            when=norm_ts(_pick(r, rule.get("registered", ()) + dr.TIME_FIELDS, columns)),
            placeholder=not tokens or set(tokens) <= _PLACEHOLDER_NAMES,
        ))
    # Estimated birth dates (1 January, or a hospital's default day) count for the year only.
    dobs = [p.dob for p in out if p.dob]
    days = Counter((d.month, d.day) for d in dobs)
    approx_days = {(1, 1)} | {md for md, n in days.items() if n > max(20, 0.03 * len(dobs))}
    # Values shared by many different names are placeholders, not identity.
    phone_names = defaultdict(set)
    id_names = defaultdict(set)
    for p in out:
        if p.phone:
            phone_names[p.phone].add(p.tokens)
        for f, v in p.ids.items():
            id_names[(f, v)].add(p.tokens)
    for p in out:
        p.dob_approx = bool(p.dob) and (p.dob.month, p.dob.day) in approx_days
        if p.phone and len(phone_names[p.phone]) > PHONE_SHARED_MAX:
            p.phone = None
        p.ids = {f: v for f, v in p.ids.items() if len(id_names[(f, v)]) <= ID_SHARED_MAX}
    return out


def _person_conflict(a: _Person, b: _Person) -> str | None:
    if a.gender and b.gender and a.gender != b.gender:
        return "gender differs"
    if a.dob and b.dob and a.dob != b.dob:
        if not (a.dob_approx or b.dob_approx) or abs(a.dob.year - b.dob.year) > 1:
            return "date of birth differs"
    for f in a.ids.keys() & b.ids.keys():
        if a.ids[f] != b.ids[f]:
            return f"{f} differs"
    return None


def _match_people(table, rows, rule, columns) -> tuple[list[Edge], callable]:
    people = _people(rows, rule, columns)
    by_id = {p.id: p for p in people}
    name_counts = Counter(p.tokens for p in people)
    blocks = defaultdict(list)
    for i, p in enumerate(people):
        if p.placeholder:
            continue
        if len(p.tokens) >= 2:
            blocks[("name", p.tokens)].append(i)
        if p.dob and not p.dob_approx:
            blocks[("dob", p.dob)].append(i)
        if p.phone:
            blocks[("phone", p.phone)].append(i)
        for f, v in p.ids.items():
            blocks[("id", f, v)].append(i)
    pairs = set()
    skipped = 0
    for key, idx in blocks.items():
        if len(idx) > BLOCK_MAX:
            skipped += 1
            continue
        pairs.update((i, j) for n, i in enumerate(idx) for j in idx[n + 1:])
    if skipped:
        log.warning("  %s: %d candidate group(s) over %d records skipped (common value)", table, skipped, BLOCK_MAX)

    burst = rule.get("burst_seconds", 600)
    edges = []
    for i, j in sorted(pairs, key=lambda ij: (_id_order(people[ij[0]].id), _id_order(people[ij[1]].id))):
        a, b = people[i], people[j]
        conflict = _person_conflict(a, b)
        if conflict:
            continue
        names = names_relation(a.tokens, b.tokens)
        same_id = sorted(f for f in a.ids.keys() & b.ids.keys() if a.ids[f] == b.ids[f])
        exact_dob = a.dob and b.dob and a.dob == b.dob and not (a.dob_approx and b.dob_approx)
        gap = abs((a.when - b.when).total_seconds()) if a.when and b.when else None
        same_phone = bool(a.phone) and a.phone == b.phone
        ev = {"names": names, "same_id": same_id or None, "dob": "same" if exact_dob else None,
              "phone": "same" if same_phone else None, "gap_seconds": gap}
        ev = {k: v for k, v in ev.items() if v is not None}
        if same_id and names:
            edges.append(Edge(table, a.id, b.id, "identity", "same_strong_id", "auto", ev))
        elif names == "equal" and exact_dob:
            edges.append(Edge(table, a.id, b.id, "identity", "same_name_dob", "auto", ev))
        elif names == "equal" and gap is not None and gap <= burst:
            edges.append(Edge(table, a.id, b.id, "entry", "double_registration", "auto", ev))
        elif names == "compatible" and exact_dob and same_phone:
            # a typo or a dropped / added name, same exact birth date and phone
            edges.append(Edge(table, a.id, b.id, "identity", "similar_name_dob_phone", "auto", ev))
        elif names == "equal" and same_phone and name_counts[a.tokens] <= 2 and not (a.dob and b.dob):
            # a full name no one else has, same phone, no birth date against it
            edges.append(Edge(table, a.id, b.id, "identity", "same_rare_name_phone", "auto", ev))
        elif same_id:
            edges.append(Edge(table, a.id, b.id, "identity", "same_id_other_name", "review", ev))
        elif names == "equal" and same_phone:
            edges.append(Edge(table, a.id, b.id, "identity", "same_name_phone", "review", ev))
        elif names == "compatible" and exact_dob:
            edges.append(Edge(table, a.id, b.id, "identity", "similar_name_same_dob", "review", ev))
        elif names == "equal" and a.dob and b.dob:
            edges.append(Edge(table, a.id, b.id, "identity", "same_name_estimated_dob", "review", ev))
        elif names == "equal" and len(a.tokens) >= 3 and name_counts[a.tokens] <= 2:
            edges.append(Edge(table, a.id, b.id, "identity", "same_rare_full_name", "review", ev))
    return edges, (lambda x, y: _person_conflict(by_id[x], by_id[y]) is not None)


HUB_MIN = 100            # a parent with more children than this, and …
HUB_FACTOR = 10          # … this many times the 99th percentile, is a shared placeholder


def _hub_parents(values: list) -> set[str]:
    counts = Counter(str(v) for v in values if not _blank(v))
    if not counts:
        return set()
    sizes = sorted(counts.values())
    p99 = sizes[min(len(sizes) - 1, int(0.99 * len(sizes)))]
    limit = max(HUB_MIN, HUB_FACTOR * p99)
    return {v for v, n in counts.items() if n > limit}


def _match_events(table, rows, rule, columns) -> tuple[list[Edge], callable]:
    dims = [_present(alts, columns) for alts in rule["group"]]
    # Without what was entered (a test name …) entries can't be told apart: review only.
    # `optional` fields (a department …) only narrow the match.
    optional = set(rule.get("optional", ()))
    missing = [alts[0] for alts in rule["group"] if not _present(alts, columns) and not optional & set(alts)]
    certain = not missing
    dims = [d for d in dims if d]
    if not dims:
        return [], None
    if not certain:
        log.info("  %s: no %s — matches go to review, none merged", table, " / ".join(missing))
    time_fields = _present(rule.get("time", ()), columns)
    agree = _present(rule.get("agree", ()), columns)
    auto_s, review_s = rule.get("auto_seconds", 120), rule.get("review_seconds", 900)
    # Tables where repeats are normal turn these off (see dedup_rules.py).
    review_differences = rule.get("review_differences", True)
    review_untimed = rule.get("review_untimed", True)
    review_minutes = rule.get("review_minutes", True)
    text_min = rule.get("auto_identical_text")
    times = {r["id"]: norm_ts(_pick(r, time_fields, columns)) for r in rows} if time_fields else {}
    known = [t for t in times.values() if t]
    if known and sum(1 for t in known if t.time() == datetime.min.time()) > 0.95 * len(known):
        log.info("  %s: %s holds dates only — compared as entries without a time", table, time_fields[0])
        times = {}
    # A patient with far more children than anyone else is a shared placeholder
    # ("walk-in"): its entries belong to different people and are never matched.
    to_person = any((dr.DEFAULT_RULES.get(p) or {}).get("type") == "person"
                    for f in dims[0] for p in dr.LINKS.get(f, ()))
    hubs = _hub_parents([_pick(r, dims[0], columns) for r in rows]) if to_person else set()
    if hubs:
        log.info("  %s: %d shared placeholder parent(s) (%s) — their entries are not matched",
                 table, len(hubs), ", ".join(sorted(map(str, hubs))[:5]))
    groups = defaultdict(list)
    info = {}
    for r in rows:
        if hubs and str(_pick(r, dims[0], columns)) in hubs:
            continue
        key = []
        for d in dims:
            v = _pick(r, d, columns)
            if v is None and any(f in dr.LINKS for f in d) and not optional & set(d):
                key = None                       # no parent → nothing to compare it with
                break
            key.append(norm_text(v))             # an optional link left blank only narrows less
        if key is None:
            continue
        ts = times.get(r["id"])
        values = {f: norm_text(r.get(f)) for f in agree}
        info[r["id"]] = values
        groups[tuple(key)].append((ts, r["id"], values, r.get("__links")))

    def disagree(x: dict, y: dict) -> list[str]:
        return [f for f in agree if x.get(f) and y.get(f) and x[f] != y[f]]

    def action(want: str) -> str:
        return want if certain else "review"

    def worth_a_look(x: dict, y: dict) -> bool:
        # without what was entered, only identical filled-in content is worth a look
        return certain or any(x.get(f) and x.get(f) == y.get(f) for f in agree)

    def same_text(x: dict, y: dict) -> bool:
        filled = [f for f in agree if x.get(f) or y.get(f)]
        return bool(text_min) and bool(filled) and all(x.get(f) == y.get(f) for f in filled) \
            and max(len(x.get(f) or "") for f in filled) >= text_min

    edges = []
    for key in sorted(groups):
        items = sorted(groups[key], key=lambda t: (t[0] is None, t[0] or datetime.min, _id_order(t[1])))
        timed = [t for t in items if t[0] is not None]
        untimed = [t for t in items if t[0] is None]
        first = None
        last = None                     # the latest entry of the current burst
        for ts, rid, values, links in timed:
            if first is None:
                first, last = (ts, rid, values, links), ts
                continue
            gap = (ts - first[0]).total_seconds()
            step = (ts - last).total_seconds()
            diff = disagree(first[2], values)
            ev = {"gap_seconds": gap, **({"differs": diff} if diff else {})}
            if gap <= review_s and not worth_a_look(first[2], values):
                pass
            elif not diff and (gap <= auto_s or (step <= auto_s and gap <= review_s)):
                # a burst: each entry seconds after the one before
                edges.append(Edge(table, first[1], rid, "entry", "same_entry_seconds_apart", action("auto"), ev))
                last = ts
                continue
            elif gap <= review_s and same_text(first[2], values):
                edges.append(Edge(table, first[1], rid, "entry", "same_text_minutes_apart", action("auto"), ev))
                last = ts
                continue
            elif not diff and gap <= review_s and links != first[3]:
                # the same line on each copy of a merged parent
                edges.append(Edge(table, first[1], rid, "entry", "copy_on_merged_parent", action("auto"), ev))
                continue
            elif gap <= review_s and review_minutes and (review_differences or not diff):
                edges.append(Edge(table, first[1], rid, "entry", "same_entry_minutes_apart", "review", ev))
            first, last = (ts, rid, values, links), ts
        # Without a time, identical lines on one parent may both be real (two units): review.
        if untimed:
            pool = untimed if timed else items
            head = pool[0]
            for _, rid, values, links in pool[1:]:
                if (disagree(head[2], values) or any(head[2].get(f) != values.get(f) for f in agree)
                        or not worth_a_look(head[2], values)):
                    continue
                if links != head[3]:
                    edges.append(Edge(table, head[1], rid, "entry", "copy_on_merged_parent", action("auto"), {}))
                elif same_text(head[2], values):
                    edges.append(Edge(table, head[1], rid, "entry", "same_text_without_time", action("auto"), {}))
                elif review_untimed:
                    edges.append(Edge(table, head[1], rid, "entry", "identical_without_time", "review", {}))
    return edges, (lambda x, y: bool(disagree(info[x], info[y])))


def _match_catalogue(table, rows, rule, columns) -> tuple[list[Edge], callable]:
    attrs = _present(rule.get("attrs", ()), columns)
    prices = _present(rule.get("review_if_differs", ()), columns)
    # Without its price column a list can't rule out price tiers: review only.
    blind = bool(rule.get("review_if_differs")) and not prices
    if blind:
        log.info("  %s: no %s — same-name matches go to review", table, " / ".join(rule["review_if_differs"]))
    vals = {r["id"]: ({f: norm_key(r.get(f)) for f in attrs}, {f: norm_key(r.get(f)) for f in prices}) for r in rows}

    def differs(x: dict, y: dict) -> list[str]:
        return [f for f in x if x[f] and y.get(f) and x[f] != y[f]]

    edges = []
    for spec in rule.get("keys", ()):
        fields = _present(spec["fields"], columns)
        if not fields:
            continue
        groups = defaultdict(list)
        for r in rows:
            k = norm_key(_pick(r, fields, columns))
            if k:
                groups[k].append(r["id"])
        for k in sorted(groups):
            heads: list[str] = []
            for rid in sorted(groups[k], key=_id_order):
                head = next((h for h in heads if not differs(vals[h][0], vals[rid][0])), None)
                if head is None:
                    heads.append(rid)                  # same name, different strength / department …
                    continue
                price = differs(vals[head][1], vals[rid][1])
                action = "review" if price or blind else spec["action"]
                edges.append(Edge(table, head, rid, "identity", f"same_{fields[0]}", action,
                                  {"differs": price} if price else {}))
    return edges, (lambda x, y: bool(differs(vals[x][0], vals[y][0])))


_MATCHERS = {"person": _match_people, "event": _match_events, "catalogue": _match_catalogue}


# ─── ENGINE ─────────────────────────────────────────────────────────────────

@dataclass
class TableOutcome:
    table: str
    rows_in: int = 0
    copies_dropped: int = 0
    crosswalk: list[tuple] = field(default_factory=list)   # (loser, survivor, kind, copies_dropped)
    review: list[tuple] = field(default_factory=list)      # (id_a, id_b, kind, rule)
    log: list[Edge] = field(default_factory=list)
    survivor_of: dict = field(default_factory=dict)        # loser id → survivor id (merges only)
    kept: list[dict] | None = None                         # dedupe_records only

    @property
    def merged_away(self) -> int:
        return len(self.survivor_of)

    @property
    def rows_out(self) -> int:
        return self.rows_in - self.copies_dropped - self.merged_away


def collapse_copies(rows: list[dict], time_fields=dr.TIME_FIELDS) -> tuple[list[dict], dict, dict]:
    """One row per ID — the newest by updated_at / created_at. Returns
    (rows, {id: copies dropped}, {id: "load" | "version"})."""
    by_id = defaultdict(list)
    for r in rows:
        by_id[str(r.get("id"))].append(r)
    out, dropped, kinds = [], {}, {}
    for rid, copies in by_id.items():
        if len(copies) > 1 and rid != "None":
            dropped[rid] = len(copies) - 1
            same = all(json.dumps(c, sort_keys=True, default=str) == json.dumps(copies[0], sort_keys=True, default=str)
                       for c in copies)
            kinds[rid] = "load" if same else "version"
            copies = sorted(copies, key=lambda c: (norm_ts(_pick(c, time_fields, c.keys())) or datetime.min,
                                                   json.dumps(c, sort_keys=True, default=str)))[-1:]
        out.extend(copies)
    return out, dropped, kinds


def table_order(tables: dict[str, set]) -> list[str]:
    """Parents before children (by the link fields each table has), so a
    child's links can be moved to its parent's survivors before matching."""
    deps = {}
    for t, cols in tables.items():
        deps[t] = {next((p for p in dr.LINKS[c] if p in tables), None) for c in cols if c in dr.LINKS} - {None, t}
    order, done = [], set()
    while len(done) < len(tables):
        ready = sorted(t for t in tables if t not in done and deps[t] <= done)
        if not ready:                                   # a cycle: take the rest in name order
            ready = sorted(t for t in tables if t not in done)[:1]
        order += ready
        done.update(ready)
    return order


def dedupe_table(table: str, rows: list[dict], columns: set, rule: dict | None, *,
                 parents: dict[str, dict] | None = None, decisions: dict | None = None,
                 copies: dict | None = None, copy_kinds: dict | None = None) -> TableOutcome:
    """Match one table's rows (one per ID, ids as text, lower-case keys)."""
    out = TableOutcome(table, rows_in=len(rows) + sum((copies or {}).values()),
                       copies_dropped=sum((copies or {}).values()))
    for rid, n in sorted((copies or {}).items(), key=lambda kv: _id_order(kv[0])):
        out.crosswalk.append((rid, rid, (copy_kinds or {}).get(rid, "load"), n))
    if not rule or not rows:
        return out
    # links moved to the parents' survivors for matching; "__links" keeps the originals
    link_fields = sorted(columns & dr.LINKS.keys())
    grouped_by = {f for alts in rule.get("group", ()) for f in alts}
    rows = [{**r, "__links": tuple(str(r.get(f)) for f in link_fields if f in grouped_by)} for r in rows]
    for f in link_fields:
        moved = next((parents[p] for p in dr.LINKS[f] if parents and p in parents), None)
        if moved:
            rows = [{**r, f: moved.get(str(r[f]), r[f])} if not _blank(r.get(f)) else r for r in rows]
    edges, conflict = _MATCHERS[rule["type"]](table, rows, rule, columns)
    decided = {k[1]: v for k, v in (decisions or {}).items() if k[0] == table}
    said_different = {pair for pair, d in decided.items() if d == "different"}

    def blocked(x, y):
        return frozenset((x, y)) in said_different or bool(conflict and conflict(x, y))

    ids = [r["id"] for r in rows]
    clusters = Clusters(ids, blocked)
    review = []
    for e in edges:
        pair = frozenset((e.a, e.b))
        if pair in decided:
            e.action = "human_same" if decided[pair] == "same" else "blocked_by_decision"
        if e.action == "human_same":
            clusters.union(e.a, e.b, force=True)
        elif e.action == "auto" and not clusters.union(e.a, e.b):
            e.action = "blocked_conflict"
        elif e.action == "review":
            review.append(e)
        out.log.append(e)
    for pair, d in sorted(decided.items(), key=lambda kv: sorted(kv[0])):
        a, b = sorted(pair, key=_id_order)
        if d == "same" and a in clusters.parent and b in clusters.parent and not any(
                frozenset((e.a, e.b)) == pair for e in edges):
            clusters.union(a, b, force=True)
            out.log.append(Edge(table, a, b, "identity", "reviewer", "human_same"))

    when = {}
    tf = _present(tuple(rule.get("time", ())) + tuple(rule.get("registered", ())) + dr.TIME_FIELDS, columns)
    for r in rows:
        when[r["id"]] = norm_ts(_pick(r, tf, columns))
    first_kind = {}
    for e in out.log:
        if e.action in ("auto", "human_same"):
            first_kind.setdefault(e.a, e.kind)
            first_kind.setdefault(e.b, e.kind)
    for root in sorted(clusters.members, key=_id_order):
        members = clusters.members[root]
        if len(members) < 2:
            continue
        survivor = min(members, key=lambda i: (when[i] is None, when[i] or datetime.min, _id_order(i)))
        for m in sorted(members, key=_id_order):
            if m != survivor:
                out.survivor_of[m] = survivor
                out.crosswalk.append((m, survivor, first_kind.get(m, "identity"), 0))
    seen = set()
    for e in review:
        ra, rb = clusters.find(e.a), clusters.find(e.b)
        # only a reviewer's 'different' drops a pair from review
        if ra == rb or frozenset((ra, rb)) in seen or frozenset((e.a, e.b)) in said_different:
            continue
        seen.add(frozenset((ra, rb)))
        out.review.append((e.a, e.b, e.kind, e.rule))
    return out


def run_engine(tables: dict[str, list[dict]], columns: dict[str, set], *, overrides: dict | None = None,
               decisions: dict | None = None, copies: dict | None = None,
               copy_kinds: dict | None = None) -> dict[str, TableOutcome]:
    """Every table, parents first. `tables` holds one row per ID."""
    results, parents = {}, {}
    for t in table_order(columns):
        res = dedupe_table(t, tables.get(t, []), columns[t], dr.rules_for(t, overrides), parents=parents,
                           decisions=decisions, copies=(copies or {}).get(t), copy_kinds=(copy_kinds or {}).get(t))
        if res.survivor_of:
            parents[t] = res.survivor_of
        results[t] = res
    return results


def dedupe_records(table: str, records: list[dict], *, overrides: dict | None = None,
                   parents: dict[str, dict] | None = None, decisions: dict | None = None) -> TableOutcome:
    """For loaders that build V3 payloads in Python: the same rules on a list
    of records (V3 field names). `outcome.kept` is what to post; move links
    to merged records with `outcome.survivor_of` (loser id → survivor id), and
    pass it as `parents={table: …}` when deduping that table's children."""
    rows = [{**{str(k).lower(): v for k, v in r.items()}, "id": str(r.get("id"))} for r in records]
    rows, dropped, kinds = collapse_copies(rows)
    columns = set().union(*(r.keys() for r in rows)) if rows else set()
    out = dedupe_table(table.lower(), rows, columns, dr.rules_for(table, overrides), parents=parents,
                       decisions=decisions, copies=dropped, copy_kinds=kinds)
    out.kept = [r for r in rows if r["id"] not in out.survivor_of]
    return out


# ─── SNOWFLAKE ──────────────────────────────────────────────────────────────

def _connect():
    import snowflake.connector
    from dotenv import load_dotenv
    here = Path(__file__).resolve().parent
    load_dotenv(here / ".env", override=False)
    key = Path(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH").strip())
    if not key.is_absolute() and not key.exists():
        key = here / key                       # the .env's relative path, run from anywhere
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER").strip(), account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(), database=os.getenv("SNOWFLAKE_DATABASE", DB).strip(),
        role=(os.getenv("SNOWFLAKE_ROLE") or "").strip() or None, private_key_file=str(key),
    )


def _split(schema: str) -> tuple[str, str]:
    db, _, name = schema.upper().rpartition(".")
    return db or DB, name


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def raw_tables(cur, raw_schema: str, tables=None, overrides=None,
               fields=None) -> dict[str, tuple[str, dict[str, tuple[str, str]]]]:
    """{table: (query, {lower column: (column, SQL type)})}: `tables`, else those with a rule, as RAW flattens
    them before dedup. fields: {table: expanded fields} the flatten already worked out."""
    import flatten_jsons_schemas as fl
    known = {t.lower(): f for t, f in (fields or {}).items()}
    out = {}
    for t in fl.get_source_tables(cur, raw_schema):
        if not (t.lower() in tables if tables else dr.rules_for(t, overrides)):
            continue
        expanded = known.get(t.lower()) or fl.expand_objects(cur, raw_schema, t, fl.discover_fields(cur, raw_schema, t))
        out[t.lower()] = (f"({fl.flatten_select(raw_schema, t, expanded)})",
                          {c.lower(): (c, typ or "VARIANT") for c, typ in fl.flatten_columns(expanded)})
    return out


def _time_expr(cols: dict, rule: dict | None, alias: str = "") -> str:
    fields = [f for f in tuple((rule or {}).get("time", ())) + tuple((rule or {}).get("registered", ()))
              + dr.TIME_FIELDS if f in cols]
    if not fields:
        return "NULL::TIMESTAMP_NTZ"
    parts = [f"TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({alias}{_q(cols[f][0])}))" for f in dict.fromkeys(fields)]
    return parts[0] if len(parts) == 1 else "COALESCE(" + ", ".join(parts) + ")"


def fetch_rows(cur, source: str, cols: dict, rule: dict | None) -> list[dict]:
    """One row per ID (the newest copy), only the columns matching needs."""
    want = {"id"} | set(dr.TIME_FIELDS) | (cols.keys() & dr.LINKS.keys())
    for k, v in (rule or {}).items():
        if k in ("keys",):
            for spec in v:
                want.update(spec["fields"])
        elif k == "group":
            for alts in v:
                want.update(alts)
        elif isinstance(v, (tuple, list)):
            want.update(x for x in v if isinstance(x, str))
    names = sorted(want & cols.keys())
    ts = _time_expr(cols, rule)
    # HASH(*) breaks ties so the same copy wins every run
    cur.execute(f"SELECT {', '.join(_q(cols[c][0]) for c in names)} "
                f"FROM (SELECT *, HASH(*) AS \"__H\" FROM {source}) "
                f"QUALIFY ROW_NUMBER() OVER (PARTITION BY TO_VARCHAR({_q(cols['id'][0])}) "
                f"ORDER BY {ts} DESC NULLS LAST, \"__H\") = 1")
    rows = []
    for rec in cur.fetchall():
        r = dict(zip(names, rec))
        if r.get("id") is None:
            continue
        r["id"] = str(r["id"])
        rows.append(r)
    return rows


def fetch_copies(cur, source: str, cols: dict) -> tuple[dict, dict]:
    idc = _q(cols["id"][0])
    cur.execute(f"SELECT TO_VARCHAR({idc}), COUNT(*), COUNT(DISTINCT HASH(*)) FROM {source} "
                f"WHERE {idc} IS NOT NULL GROUP BY 1 HAVING COUNT(*) > 1")
    copies, kinds = {}, {}
    for rid, n, distinct in cur.fetchall():
        copies[rid] = n - 1
        kinds[rid] = "load" if distinct == 1 else "version"
    return copies, kinds


def facility_of(input_schema: str) -> str:
    """HOSPITALS.WEMA_MEDICAL_RAW → WEMA_MEDICAL."""
    return _split(input_schema)[1].removesuffix("_RAW")


def load_decisions(cur, out_schema: str, facility: str) -> dict:
    db, name = _split(out_schema)
    cur.execute(f"SELECT COUNT(*) FROM {db}.INFORMATION_SCHEMA.TABLES WHERE table_schema = %s AND table_name = 'DECISIONS'",
                (name,))
    if not cur.fetchone()[0]:
        return {}
    cur.execute(f"SELECT LOWER(V3_TABLE), ID_A, ID_B, LOWER(DECISION) FROM {out_schema}.DECISIONS "
                f"WHERE FACILITY = %s AND LOWER(DECISION) IN ('same', 'different') ORDER BY DECIDED_AT", (facility,))
    return {(t, frozenset((str(a), str(b)))): d for t, a, b, d in cur.fetchall()}


def _lit(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def crosswalk_select(cur, facility: str, table: str, query: str, columns, output_schema: str = OUTPUT_SCHEMA) -> str:
    """`query` without the records merged away and with links moved to the survivors; `query` itself when the
    facility has no crosswalk. columns: [(column, SQL type or None if not a scalar)]."""
    facility = facility.upper()
    db, name = _split(output_schema)
    cur.execute(f"SELECT COUNT(*) FROM {db}.INFORMATION_SCHEMA.TABLES WHERE table_schema = %s AND table_name = 'CROSSWALK'",
                (name,))
    if not cur.fetchone()[0]:
        return query
    cw = f"{db}.{name}.CROSSWALK"
    cur.execute(f"SELECT COUNT(*) FROM {cw} WHERE FACILITY = %s", (facility,))
    if not cur.fetchone()[0]:
        return query

    def merged(tables):  # loser → survivor; when a link can point at several tables, the first listed wins
        order = " ".join(f"WHEN {_lit(t)} THEN {n}" for n, t in enumerate(tables))
        return (f"(SELECT LOSER_ID, SURVIVOR_ID FROM {cw} WHERE FACILITY = {_lit(facility)} "
                f"AND LOSER_ID <> SURVIVOR_ID AND V3_TABLE IN ({', '.join(map(_lit, tables))}) "
                f"QUALIFY ROW_NUMBER() OVER (PARTITION BY LOSER_ID ORDER BY CASE V3_TABLE {order} END) = 1)")

    select, joins, gone = ["f.SOURCE_SCHEMA"], [], []
    for col, sql_type in columns:
        ref = f"f.{_q(col)}"
        parents = dr.LINKS.get(col.lower()) if sql_type else None
        if parents:
            a = f"l{len(joins)}"
            joins.append(f"LEFT JOIN {merged(parents)} {a} ON {a}.LOSER_ID = TO_VARCHAR({ref})")
            select.append(f"COALESCE(TRY_CAST({a}.SURVIVOR_ID AS {sql_type}), {ref}) AS {_q(col)}")
        else:
            select.append(ref)
        if col.lower() == "id" and sql_type:
            gone = [f"LEFT JOIN {merged((table.lower(),))} gone ON gone.LOSER_ID = TO_VARCHAR({ref})",
                    "WHERE gone.LOSER_ID IS NULL"]
    if not joins and not gone:
        return query
    return f"SELECT {', '.join(select)}\n    FROM ({query}) f\n    " + "\n    ".join(joins + gone)


def _create_outputs(cur, out_schema: str) -> None:
    p = f"{out_schema}."
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {out_schema}")
    cur.execute(f"CREATE TABLE IF NOT EXISTS {p}CROSSWALK (FACILITY STRING, V3_TABLE STRING, LOSER_ID STRING, "
                f"SURVIVOR_ID STRING, KIND STRING, COPIES_DROPPED NUMBER, RUN_ID STRING)")
    cur.execute(f"CREATE TABLE IF NOT EXISTS {p}REVIEW (FACILITY STRING, V3_TABLE STRING, ID_A STRING, ID_B STRING, "
                f"KIND STRING, RULE STRING, RUN_ID STRING)")
    cur.execute(f"CREATE TABLE IF NOT EXISTS {p}LOG (FACILITY STRING, RUN_ID STRING, V3_TABLE STRING, ID_A STRING, "
                f"ID_B STRING, KIND STRING, RULE STRING, ACTION STRING, EVIDENCE STRING)")
    cur.execute(f"CREATE TABLE IF NOT EXISTS {p}DECISIONS (FACILITY STRING, V3_TABLE STRING, ID_A STRING, ID_B STRING, "
                f"DECISION STRING, DECIDED_BY STRING, DECIDED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), NOTE STRING)")
    cur.execute(f"CREATE TABLE IF NOT EXISTS {p}RUNS (FACILITY STRING, RUN_ID STRING, INPUT_SCHEMA STRING, "
                f"V3_TABLE STRING, ROWS_IN NUMBER, ROWS_OUT NUMBER, MERGED_AWAY NUMBER, COPIES_DROPPED NUMBER, "
                f"REVIEW_PAIRS NUMBER, RULE STRING, FINISHED_AT TIMESTAMP_TZ)")


def _insert(cur, table: str, rows: list[tuple], n: int, chunk: int = 5000) -> None:
    marks = ", ".join(["%s"] * n)
    for i in range(0, len(rows), chunk):
        cur.executemany(f"INSERT INTO {table} VALUES ({marks})", rows[i:i + chunk])


def run(input_schema: str, output_schema: str = OUTPUT_SCHEMA, *, facility: str | None = None,
        tables: list[str] | None = None, overrides: dict | None = None,
        dry_run: bool = False, fields: dict | None = None) -> dict[str, TableOutcome]:
    input_schema, output_schema = input_schema.upper(), output_schema.upper()
    facility = (facility or facility_of(input_schema)).upper()
    if _split(input_schema) == _split(output_schema):
        raise SystemExit("The output schema must differ from the input schema")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    conn = _connect()
    try:
        cur = conn.cursor()
        described = raw_tables(cur, input_schema, tables, overrides, fields)
        if not described and tables:
            raise SystemExit(f"None of {tables} in {input_schema}.EVENTS_RAW")
        if not described:
            log.info("No tables with a dedup rule in %s", input_schema)
            return {}
        selected = {t: c for t, (_, c) in described.items() if "id" in c}
        skipped = sorted(set(described) - set(selected))
        if skipped:
            log.info("No ID column or not selected, left out: %s", ", ".join(skipped))
        decisions = {} if dry_run else load_decisions(cur, output_schema, facility)
        rows, copies, kinds = {}, {}, {}
        for t, cols in sorted(selected.items()):
            rule = dr.rules_for(t, overrides)
            copies[t], kinds[t] = fetch_copies(cur, described[t][0], cols)
            rows[t] = fetch_rows(cur, described[t][0], cols, rule) if rule else []
            log.info("  %-34s %s", t, f"{len(rows[t])} records" if rule else "no rule — copies only")
        results = run_engine(rows, {t: set(c) for t, c in selected.items()}, overrides=overrides,
                             decisions=decisions, copies=copies, copy_kinds=kinds)
        for t, res in results.items():           # every row, rows without an ID included
            cur.execute(f"SELECT COUNT(*) FROM {described[t][0]}")
            res.rows_in = cur.fetchone()[0]
        print_report(results, input_schema, output_schema, facility, dry_run)
        if dry_run:
            return results
        _create_outputs(cur, output_schema)
        p = f"{output_schema}."
        # this hospital's rows are swapped in one transaction, so readers never see a half-written crosswalk
        cur.execute("BEGIN")
        for t in ("CROSSWALK", "REVIEW", "LOG"):
            cur.execute(f"DELETE FROM {p}{t} WHERE FACILITY = %s", (facility,))
        _insert(cur, f"{p}CROSSWALK", [(facility, t, l, s, k, c, run_id) for t, r in results.items()
                                       for l, s, k, c in r.crosswalk], 7)
        _insert(cur, f"{p}REVIEW", [(facility, t, a, b, k, ru, run_id) for t, r in results.items()
                                    for a, b, k, ru in r.review], 7)
        _insert(cur, f"{p}LOG", [(facility, run_id, e.table, e.a, e.b, e.kind, e.rule, e.action,
                                  json.dumps(e.evidence, default=str)) for r in results.values() for e in r.log], 9)
        now = datetime.now(timezone.utc).isoformat()
        _insert(cur, f"{p}RUNS", [(facility, run_id, input_schema, t, r.rows_in, r.rows_out, r.merged_away,
                                   r.copies_dropped, len(r.review), json.dumps(dr.rules_for(t, overrides), default=str),
                                   now) for t, r in results.items()], 11)
        cur.execute("COMMIT")
        return results
    finally:
        conn.close()


def print_report(results: dict[str, TableOutcome], input_schema, output_schema, facility, dry_run) -> None:
    print(f"\n  {input_schema} → {output_schema} ({facility}){'  (dry run, nothing written)' if dry_run else ''}")
    print(f"  {'table':34s} {'rows in':>8s} {'copies':>7s} {'merged':>7s} {'review':>7s} {'rows out':>9s}  rules")
    empty = sorted(t for t, r in results.items() if not r.rows_in)
    for t, r in results.items():
        if not r.rows_in:
            continue
        rules = Counter(e.rule for e in r.log if e.action in ("auto", "human_same"))
        note = ", ".join(f"{k} {v}" for k, v in rules.most_common(3))
        print(f"  {t:34s} {r.rows_in:>8} {r.copies_dropped:>7} {r.merged_away:>7} {len(r.review):>7} "
              f"{r.rows_out:>9}  {note}")
    if empty:
        print(f"  ({len(empty)} empty table(s) not shown)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--facility", help="Facility key: reads <KEY>_RAW")
    ap.add_argument("--input-schema", help="Instead of --facility, e.g. HOSPITALS.WEMA_MEDICAL_RAW")
    ap.add_argument("--output-schema", default=OUTPUT_SCHEMA)
    ap.add_argument("--tables", nargs="+", help="Only these V3 tables")
    ap.add_argument("--mappings", type=Path, help="Mappings JSON whose \"dedup\" block overrides the default rules")
    ap.add_argument("--dry-run", action="store_true", help="Match and report; write nothing")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s · %(levelname)-7s · %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
    if not (args.facility or args.input_schema):
        ap.error("--facility or --input-schema is required")
    key = re.sub(r"[^A-Z0-9]+", "_", (args.facility or "").upper()).strip("_")
    input_schema = args.input_schema or f"{DB}.{key}_RAW"
    overrides = json.loads(args.mappings.read_text()).get("dedup", {}) if args.mappings else None
    run(input_schema, args.output_schema, facility=key or None, tables=[t.lower() for t in args.tables or []] or None,
        overrides=overrides, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
