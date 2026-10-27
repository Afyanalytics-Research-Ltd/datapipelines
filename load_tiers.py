#!/usr/bin/env python3
"""
load_tiers.py — load order (tiers) for any migration pipeline, worked out from
the links between tables instead of a hand-kept list.

A table's tier is 1 + the highest tier of the tables it links to, so every
parent is loaded before its children:  users (1) → patients (2) → visits (3)
→ vitals (4). Tables in the same tier don't depend on each other and can load
in parallel; tier N+1 starts once tier N is through.

This replaces, for every pipeline, what each one does on its own today:
  v2_to_v3_api_migration._namespace_tier   position in NAMESPACE_MAP + _TIER_BOUNDARIES
  load_missing_records.TIER                hand-written dict
  v3_passthrough._tiers / v3_to_v3_migration.tiers   their own graph walks

THREE STEPS
  1. LINKS    which tables each table points at — discover_links() combines,
              strongest first:
                declared   links the source database or V3 states
                overrides  a person's answer for a column (the review list)
                names      patient_id → patients, admission_id → inp_admissions,
                           insurance_schemes.company_id → insurance_companies,
                           nurse_in_charge_id → users (links_from_columns)
                data       values of visits.pat_ref all found in patients.PatNo
                           (links_from_data) — for schemas that follow no
                           convention; needs the name or text codes to agree,
                           number-only matches go to review
              Also: links_from_mappings(path) for a facility's mappings JSON,
              links_from_deps({table: {parents}}) for a pipeline's own rules.
  2. TIERS    compute_tiers(tables, links) → TierPlan
                Links are HARD (the child can't exist without the parent) or
                SOFT (audit / user columns: created_by, doctor_id …). A soft link
                that closes a loop is dropped and reported — the loader fills it
                in afterwards. A loop of hard links is an error, never a guess.
  3. RUN      run_in_tiers(plan, fn, workers=8)
                Tier by tier, parallel within a tier. A table whose hard parent
                failed is skipped, not loaded with broken links.

BOTH DIRECTIONS
  Down the tiers (parents first) to load; up the tiers (children first,
  reverse=True) to delete or roll back a facility — vitals before visits
  before patients. A table is skipped when what it waits on didn't go through:
  its parents when loading, its children when deleting.
    plan.parents(t) / plan.children(t)          one step either way
    plan.with_parents([t])                      t + all it needs first   (load just t)
    plan.with_children([t])                     t + all that depends on it (re-load / delete t)

USAGE
  python load_tiers.py --mappings dags/jsons/wema_medical_data_mappings.json
  python load_tiers.py --rows samples.json --overrides overrides.json
  python load_tiers.py --raw wema_medical --tables patients visits vitals
  python load_tiers.py --mappings … --reverse                 # delete / roll back order
  python load_tiers.py --mappings … --with-parents vitals     # what loading vitals needs
  python load_tiers.py --mappings … --with-children patients  # what re-loading patients touches
  python load_tiers.py --mappings … --json
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Mapping

log = logging.getLogger("load_tiers")


# ─── LINKS ───────────────────────────────────────────────────────────────

@dataclass(frozen=True, order=True)
class Link:
    table: str          # the child
    column: str         # its column pointing at the parent
    parent: str
    hard: bool = True   # False: may be filled in after loading (audit / user columns)
    evidence: str = field(default="name", compare=False)   # name | data | name+data | mapping | override | declared


# Set by the loader for the target instance, never by load order.
INSTANCE_COLUMNS = {"facility_id", "organization_id", "tenant_id"}

USERS = "users"
# a word in a <...>_id column that makes it a person: doctor_id, nurse_in_charge_id,
# procuring_officer_id → users, not a table of that name
USER_ROLES = {"user", "users", "doctor", "consultant", "approver", "custodian", "healthcare_provider", "provider",
              "requester", "cashier", "nurse", "clinician", "author", "owner", "assignee", "staff", "officer",
              "employee", "physician", "surgeon", "anaesthetist", "anesthetist", "pharmacist", "technician",
              "receptionist", "practitioner", "attendant", "reviewer", "verifier", "dispenser"}
# words that only qualify a link: preferred_ward_id is still a ward, parent_note_id still a note
QUALIFIERS = {"parent", "preferred", "default", "primary", "secondary", "main", "sub", "current", "previous",
              "next", "original", "source", "target", "from", "to", "requesting", "requested", "referring",
              "referred", "home", "linked", "related", "new", "old", "last", "first"}
# bare names that are attributes, not links, even where a table shares the name
NOT_LINKS = {"status", "type", "name", "code", "description", "notes", "date", "amount", "quantity", "value",
             "unit", "level", "priority", "method", "mode", "source", "category_name"}
_INSTANCE_WORDS = {"facility", "organization", "organisation", "tenant"}


def snake(name: str) -> str:
    """PatientNo → patient_no, OPNumber → op_number, fk_patient → patient_id."""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", str(name))
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s).lower().strip("_")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    if s.startswith("fk_"):
        s = s[3:] + "_id"
    return s


def _flat(name: str) -> str:
    return name.replace("_", "")


def _singular(name: str) -> str:
    if name.endswith("ies"):
        return name[:-3] + "y"
    if name.endswith(("ses", "xes", "ches", "shes")):
        return name[:-2]
    return name[:-1] if name.endswith("s") and not name.endswith("ss") else name


def _plurals(stem: str) -> set[str]:
    out = {stem, stem + "s", stem + "es"}
    if stem.endswith("y"):
        out.add(stem[:-1] + "ies")
    return {_flat(n) for n in out}


class _Names:
    """Table names for one lookup: flattened (sub_category = subcategory) and
    the module prefixes in use (inp_, inv_, core_, evaluation_ …)."""

    def __init__(self, tables: set[str]):
        self.by_flat = defaultdict(set)
        for t in tables:
            self.by_flat[_flat(t)].add(t)
        firsts = defaultdict(int)
        for t in tables:
            if "_" in t:
                firsts[t.split("_")[0]] += 1
        self.prefixes = {w for w, n in firsts.items() if n >= 2}

    def exact(self, stem: str) -> str | None:
        hits = {t for p in _plurals(stem) for t in self.by_flat.get(p, ())}
        return hits.pop() if len(hits) == 1 else None

    def with_prefix(self, stem: str, exclude: str = "") -> str | None:
        """admission → inp_admissions, result_detail → investigation_result_details,
        sub_category → icd10_subcategories: the one table that is the stem
        behind some prefix (never `exclude`, the column's own table)."""
        flats = _plurals(stem)
        hits = set()
        for ts in self.by_flat.values():
            for t in ts:
                parts = t.split("_")
                if t != exclude and any(_flat("_".join(parts[i:])) in flats for i in range(1, len(parts))):
                    hits.add(t)
        return hits.pop() if len(hits) == 1 else None


def _qualifier(word: str, names: _Names) -> bool:
    return word in QUALIFIERS or word in names.prefixes or word.endswith(("ing", "ed"))


def resolve_column(table: str, column: str, tables, users: str = USERS) -> str | None:
    """The table an <x>_id / <x>_by column points at, from its name, or None.
    In order — the first that names exactly one table wins:
      1. the whole name: visit_id → visits, doctor_note_id → doctor_notes
      2. a person: doctor_id, nurse_in_charge_id, created_by → users
      3. in this table's context: insurance_schemes.company_id →
         insurance_companies, samples.type_id → sample_types
      4. behind a prefix: admission_id → inp_admissions
      5. without leading qualifiers: preferred_ward_id, supplying_store_id,
         parent_note_id (→ this table when it holds notes)
    Bare names (visit, procedures, payment_mode) are left to the data check —
    by name alone they are as often a value or a list as a link."""
    col = snake(column)
    if col in INSTANCE_COLUMNS or col in ("id", "uuid") or col in NOT_LINKS:
        return None
    names = tables if isinstance(tables, _Names) else _Names(set(tables))
    table_names = {t for ts in names.by_flat.values() for t in ts}
    users_hit = users if users in table_names else None
    if col.endswith(("_by", "_by_id")):
        return users_hit
    if not col.endswith("_id"):
        return None
    words = col[:-3].split("_")
    if words[-1] in _INSTANCE_WORDS:
        return None
    if words == ["parent"]:
        return table                                  # parent_id: rows of this table
    own = table.split("/")[-1]
    own_words = own.split("_")
    contexts = [_singular("_".join(own_words[:k])) for k in range(len(own_words), 0, -1)]

    def in_context(stem: str) -> str | None:
        for ctx in contexts:
            if not stem.startswith(ctx):
                hit = names.exact(f"{ctx}_{stem}")
                if hit and hit != table:              # a link column never names its own table by context
                    return hit
        return None

    stem = "_".join(words)
    hit = names.exact(stem)
    if hit:
        return hit
    if any(w in USER_ROLES for w in words):
        return users_hit
    hit = in_context(stem) or names.with_prefix(stem, exclude=table)
    if hit:
        return hit
    own_singular = _singular(own)
    while len(words) > 1 and (_qualifier(words[0], names) or words[0] == own_singular):   # visits.visit_patient_id
        dropped, words = words[0], words[1:]
        stem = "_".join(words)
        if _flat(_singular(own)).endswith(_flat(stem)):
            return table                              # parent_note_id in doctor_notes: rows of this table
        hit = names.exact(stem) or in_context(stem) or names.with_prefix(stem, exclude=table)
        if hit:
            return hit
    return None


def is_person_column(column: str) -> bool:
    """created_by, recorded_by_id, doctor_id, SeenByStaff: who did something."""
    words = snake(column).split("_")
    return "by" in words or any(w in USER_ROLES for w in words)


def is_person_table(table: str) -> bool:
    """users, staff, employees, core_users: the facility's people, whatever it calls them."""
    return any(_singular(w) in USER_ROLES or w in USER_ROLES for w in table.split("/")[-1].split("_"))


def is_soft(table: str, column: str, parent: str, users: str = USERS) -> bool:
    """Links that don't stop a row existing: who created / approved / saw it."""
    return parent == users or is_person_column(column) and is_person_table(parent)


@dataclass
class Review:
    """A link a person should confirm before it is trusted (or overridden)."""
    table: str
    column: str
    parent: str
    coverage: float      # share of the column's distinct values found in the parent's key
    reason: str


@dataclass
class LinkReport:
    links: list[Link]
    unresolved: dict[str, list[str]] = field(default_factory=dict)   # table → id-like columns with no parent
    review: list[Review] = field(default_factory=list)


def links_from_columns(tables: Mapping[str, Iterable[str]], *,
                       overrides: Mapping[str, Mapping[str, str | None]] | None = None,
                       resolve: Callable[[str, str, set[str]], str | None] = resolve_column,
                       soft: Callable[[str, str, str], bool] = is_soft) -> LinkReport:
    """{table: its columns} → links between those tables.
    overrides: {table: {column: parent table, or None = not a link}} for what
    naming can't tell (parent_id, item_id …); {"*": {...}} applies to every table."""
    overrides = overrides or {}
    names = set(tables)
    lookup = _Names(names) if resolve is resolve_column else names   # built once, not per column
    links, unresolved = set(), defaultdict(list)
    for table, columns in tables.items():
        for col in sorted(set(columns)):
            per = {**overrides.get("*", {}), **overrides.get(table, {})}
            parent = per[col] if col in per else resolve(table, col, lookup)
            if parent is None:
                if col in per or snake(col) in INSTANCE_COLUMNS:
                    continue
                if snake(col).endswith(("_id", "_by")):
                    unresolved[table].append(col)
                continue
            if parent == table:
                continue                       # rows of one table point at each other: the loader's job
            if parent not in names:
                log.debug("%s.%s → %s: not among the tables being loaded", table, col, parent)
                continue
            links.add(Link(table, col, parent, hard=not soft(table, col, parent),
                           evidence="override" if col in per else "name"))
    return LinkReport(sorted(links), dict(unresolved))


# ─── LINKS FROM THE DATA ─────────────────────────────────────────────────
# Names fail on schemas that don't follow conventions (PatNo, pat_ref,
# OPNumber, a bare "patient" holding an id). The data doesn't: if the values
# of visits.pat_ref are all found in patients.patient_no, that is the link.
# Containment alone over-matches small numbers (quantity 1–50 sits inside any
# id range), so a data link needs a second piece of evidence — the name agrees
# or resembles the parent, or the values are text codes, which don't overlap
# by chance. Number-only matches go to review instead of the load order.

_KEY_WORDS = {"id", "no", "num", "number", "ref", "code", "key", "fk", "uuid", "by", "nbr"}


def _norm(value) -> str | None:
    """A comparable form of a key value: 12, 12.0 and "12" are the same key."""
    if value is None or isinstance(value, (bool, dict, list)):
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = str(value).strip()
    return text or None


def _is_numeric(values: set[str]) -> bool:
    return all(re.fullmatch(r"-?\d+", v) for v in values)


def _tokens(name: str, *, drop_keys: bool = True) -> list[str]:
    words = [w for w in snake(name).split("_") if w]
    return [w for w in words if not (drop_keys and w in _KEY_WORDS)]


def name_resembles(column: str, table: str) -> bool:
    """pat_ref ~ patients, PatientNo ~ patients, adm_id ~ inp_admissions:
    a column word of 3+ letters that starts a word of the table's name."""
    table_words = [_singular(w) for w in _tokens(table.split("/")[-1], drop_keys=False)]
    return any(len(c) >= 3 and any(t.startswith(c) or c.startswith(t) for t in table_words if len(t) >= 3)
               for c in _tokens(column))


def key_columns(rows: list[dict], *, min_rows: int = 2) -> dict[str, set[str]]:
    """Columns that identify a row: every non-empty value distinct. These are
    what other tables can point at (id, uuid, patient_no …)."""
    values = defaultdict(list)
    for r in rows:
        for c, v in r.items():
            n = _norm(v)
            if n is not None:
                values[c].append(n)
    return {c: set(vs) for c, vs in values.items()
            if len(vs) >= min_rows and len(set(vs)) == len(vs)}


def links_from_data(rows: Mapping[str, list[dict]], *, named: Iterable[Link] = (),
                    min_coverage: float = 0.95, min_values: int = 3,
                    soft: Callable[[str, str, str], bool] = is_soft) -> LinkReport:
    """{table: rows} → links found by value containment, plus a review list.

    Parent tables need their full key values (a sample of parents makes real
    links look broken); children can be a sample. `named`: links already found
    by name — confirmed here (evidence name+data) or flagged when the data
    disagrees."""
    keys = {t: key_columns(rs) for t, rs in rows.items()}
    named_by = {(l.table, l.column): l for l in named}
    found, review = {}, []
    for table, rs in rows.items():
        cols = defaultdict(set)
        for r in rs:
            for c, v in r.items():
                n = _norm(v)
                if n is not None:
                    cols[c].add(n)
        for col, vals in cols.items():
            low = snake(col)
            if low in ("id", "uuid") or low in INSTANCE_COLUMNS or len(vals) < min_values and (table, col) not in named_by:
                continue
            numeric = _is_numeric(vals)
            matches = []                                  # (coverage, parent, parent key)
            for parent, pkeys in keys.items():
                for pcol, pvals in pkeys.items():
                    if parent == table and pcol == col:
                        continue
                    if numeric != _is_numeric(pvals):
                        continue
                    cov = len(vals & pvals) / len(vals)
                    if cov >= min_coverage:
                        matches.append((cov, parent, pcol))
            name_link = named_by.get((table, col))
            if name_link is not None and name_link.parent not in keys:
                continue                                  # no parent rows to check against: the name stands
            if name_link is not None:
                if any(p == name_link.parent for _, p, _ in matches):
                    found[(table, col)] = Link(table, col, name_link.parent, name_link.hard, "name+data")
                else:
                    pvals = set().union(*keys.get(name_link.parent, {}).values()) if keys.get(name_link.parent) else set()
                    cov = len(vals & pvals) / len(vals) if vals else 0.0
                    review.append(Review(table, col, name_link.parent, round(cov, 3),
                                         "the name points here but the values mostly aren't found"))
                continue
            if not matches:
                continue
            if col in keys.get(table, {}) and name_resembles(col, table):
                continue                                  # the table's own identifier (StaffKey in staff)
            parents = {p for _, p, _ in matches if p != table}
            similar = {p for p in parents if name_resembles(col, p)
                       or is_person_column(col) and is_person_table(p)}
            if len(similar) == 1:
                parent, why = similar.pop(), "data"
            elif not numeric and len(parents) == 1:
                parent, why = next(iter(parents)), "data"   # text codes: containment is strong on its own
            else:
                others = [m for m in matches if m[1] != table]
                if others:
                    cov, parent, pcol = max(others)
                    review.append(Review(table, col, parent, round(cov, 3),
                                         f"values found in {parent}.{pcol}"
                                         + (f" (and {len(parents) - 1} other table(s))" if len(parents) > 1 else "")
                                         + " but the name doesn't say so"))
                continue
            found[(table, col)] = Link(table, col, parent, hard=not soft(table, col, parent), evidence=why)
    return LinkReport(sorted(found.values()), {}, review)


def discover_links(*, columns: Mapping[str, Iterable[str]] | None = None,
                   rows: Mapping[str, list[dict]] | None = None,
                   overrides: Mapping[str, Mapping[str, str | None]] | None = None,
                   declared: Iterable[Link] = (), **kw) -> LinkReport:
    """Every source of evidence for one facility, combined:
      declared   links the source database or V3 itself states (strongest)
      overrides  a person's answer for a column ({table: {column: parent | None}})
      names      column naming (links_from_columns)
      data       value containment (links_from_data), when rows are given
    Later sources never replace an earlier answer for the same column."""
    columns = {t: set(c) for t, c in (columns or {}).items()}
    for t, rs in (rows or {}).items():
        columns.setdefault(t, set()).update(c for r in rs for c in r)
    by_name = links_from_columns(columns, overrides=overrides, **kw)
    final: dict[tuple[str, str], Link] = {}
    for l in declared:
        final[(l.table, l.column)] = Link(l.table, l.column, l.parent, l.hard, "declared")
    for l in by_name.links:
        final.setdefault((l.table, l.column), l)
    unresolved, review = dict(by_name.unresolved), []
    if rows:
        by_data = links_from_data(rows, named=[l for l in by_name.links if l.evidence == "name"],
                                  soft=kw.get("soft", is_soft))
        not_links = {(t, c) for t, cols in (overrides or {}).items() for c, parent in cols.items() if parent is None}
        for l in by_data.links:
            if (l.table, l.column) in not_links or ("*", l.column) in not_links:
                continue                              # a person said this column is not a link
            if (l.table, l.column) in final and final[(l.table, l.column)].evidence in ("declared", "override"):
                continue
            final[(l.table, l.column)] = l
            if l.column in unresolved.get(l.table, []):
                unresolved[l.table] = [c for c in unresolved[l.table] if c != l.column]
        review = [r for r in by_data.review if (r.table, r.column) not in final
                  or final[(r.table, r.column)].evidence not in ("declared", "override")]
    unresolved = {t: cs for t, cs in unresolved.items() if cs}
    return LinkReport(sorted(final.values()), unresolved, review)


def links_from_deps(deps: Mapping[str, Iterable[str]], *, hard: bool = True) -> list[Link]:
    """{table: {parent tables}} — for pipelines that already know their links."""
    return sorted(Link(t, "", p, hard) for t, ps in deps.items() for p in ps if p != t)


def links_from_mappings(path: Path, **kw) -> tuple[list[str], LinkReport]:
    """A facility's mappings JSON → (its V3 tables, links between them),
    from the V3 field names each table is mapped to."""
    doc = json.loads(Path(path).read_text())
    by_model = {t["v3 model"]: t.get("v3 table") or t["v3 model"] for t in doc["tables"]}
    columns: dict[str, set[str]] = {t: set() for t in by_model.values()}
    for f in doc.get("fields", []):
        table = f.get("v3_table") or by_model.get(f.get("model")) or next(
            (t for m, t in by_model.items() if m.split("\\")[-1] == f.get("model")), None)
        if table in columns:
            # V3 names some links bare (visits.patient ← patient_id): use the
            # source column when only it says the value is an id
            target, source = f["field"], str(f.get("column") or "")
            id_like = lambda c: c.lower().endswith(("_id", "_by"))
            columns[table].add(source if not id_like(target) and id_like(source) else target)
    return sorted(columns), links_from_columns(columns, **kw)


# ─── TIERS ───────────────────────────────────────────────────────────────

class TierCycleError(ValueError):
    """Tables that need each other to exist first: no load order works."""


def _components(nodes: Iterable[str], edges: Mapping[str, set[str]]) -> list[set[str]]:
    """Strongly connected components (Tarjan), iterative: tables in a loop."""
    index, low, on_stack, stack, out, counter = {}, {}, set(), [], [], [0]
    for root in sorted(nodes):
        if root in index:
            continue
        work = [(root, iter(sorted(edges.get(root, ()))))]
        index[root] = low[root] = counter[0]; counter[0] += 1
        stack.append(root); on_stack.add(root)
        while work:
            node, it = work[-1]
            nxt = next(it, None)
            if nxt is not None:
                if nxt not in index:
                    index[nxt] = low[nxt] = counter[0]; counter[0] += 1
                    stack.append(nxt); on_stack.add(nxt)
                    work.append((nxt, iter(sorted(edges.get(nxt, ())))))
                elif nxt in on_stack:
                    low[node] = min(low[node], index[nxt])
                continue
            work.pop()
            if work:
                low[work[-1][0]] = min(low[work[-1][0]], low[node])
            if low[node] == index[node]:
                comp = set()
                while True:
                    n = stack.pop(); on_stack.discard(n); comp.add(n)
                    if n == node:
                        break
                out.append(comp)
    return out


@dataclass
class TierPlan:
    tiers: dict[str, int]                              # table → tier (1 = loads first)
    links: list[Link]                                  # links the order honours
    deferred: list[Link] = field(default_factory=list)  # soft links dropped to break loops: fill in after loading
    unresolved: dict[str, list[str]] = field(default_factory=dict)
    review: list[Review] = field(default_factory=list)  # links a person should confirm

    def tier_of(self, table: str, default: int = 1) -> int:
        return self.tiers.get(table, default)

    def groups(self, *, reverse: bool = False) -> list[tuple[int, list[str]]]:
        """[(tier, tables)] parents first — or, reverse, children first (the
        order to delete or roll back in: vitals before visits before patients)."""
        by = defaultdict(list)
        for t, n in self.tiers.items():
            by[n].append(t)
        return [(n, sorted(by[n])) for n in sorted(by, reverse=reverse)]

    def order(self, *, reverse: bool = False) -> list[str]:
        return [t for _, ts in self.groups(reverse=reverse) for t in ts]

    def parents(self, table: str, *, hard_only: bool = False) -> set[str]:
        """Tables this one points at (must exist before it)."""
        return {l.parent for l in self.links if l.table == table and (l.hard or not hard_only)}

    def children(self, table: str, *, hard_only: bool = False) -> set[str]:
        """Tables that point at this one (must go before it on delete)."""
        return {l.table for l in self.links if l.parent == table and (l.hard or not hard_only)}

    def _walk(self, tables: Iterable[str], step: Callable[..., set[str]], hard_only: bool) -> set[str]:
        seen, todo = set(tables), list(tables)
        while todo:
            for nxt in step(todo.pop(), hard_only=hard_only):
                if nxt not in seen:
                    seen.add(nxt)
                    todo.append(nxt)
        return seen

    def with_parents(self, tables: Iterable[str], *, hard_only: bool = False) -> set[str]:
        """The tables plus everything they need first, transitively — what to
        load for just these (vitals → visits, patients, users)."""
        return self._walk(tables, self.parents, hard_only)

    def with_children(self, tables: Iterable[str], *, hard_only: bool = False) -> set[str]:
        """The tables plus everything that depends on them, transitively — what
        a re-load or delete of these touches (patients → visits, vitals …)."""
        return self._walk(tables, self.children, hard_only)

    def check(self) -> list[str]:
        """Every honoured link has its parent in an earlier tier — [] when sound."""
        return [f"{l.table}.{l.column or '?'} → {l.parent}: tier {self.tiers[l.table]} ≤ {self.tiers[l.parent]}"
                for l in self.links if self.tiers[l.parent] >= self.tiers[l.table]]

    def describe(self, *, reverse: bool = False) -> str:
        lines = [f"tier {n}: {', '.join(ts)}" for n, ts in self.groups(reverse=reverse)]
        if self.deferred:
            lines.append("filled in after loading (loop broken): "
                         + ", ".join(f"{l.table}.{l.column or '?'} → {l.parent}" for l in self.deferred))
        if self.unresolved:
            lines.append("id columns with no parent found: "
                         + "; ".join(f"{t}: {', '.join(c)}" for t, c in sorted(self.unresolved.items())))
        for r in self.review:
            lines.append(f"review: {r.table}.{r.column} → {r.parent}? {r.reason} ({r.coverage:.0%})")
        return "\n".join(lines)


def compute_tiers(tables: Iterable[str], links: Iterable[Link] | LinkReport, *,
                  max_tier: int | None = None) -> TierPlan:
    """Tables + links → tiers. Soft links inside a loop are deferred; a loop
    that remains (hard links only) raises TierCycleError naming its tables.
    max_tier folds deeper tiers into the last one — only for callers with a
    fixed number of tier slots (an Airflow DAG with tier tasks 1..6)."""
    unresolved, review = {}, []
    if isinstance(links, LinkReport):
        links, unresolved, review = links.links, links.unresolved, links.review
    tables = set(tables)
    links = sorted({l for l in links if l.table in tables and l.parent in tables and l.table != l.parent})

    def edges_of(ls):
        e = defaultdict(set)
        for l in ls:
            e[l.table].add(l.parent)
        return e

    comp_of = {t: i for i, c in enumerate(_components(tables, edges_of(links))) for t in c}
    deferred = [l for l in links if not l.hard and comp_of[l.table] == comp_of[l.parent]]
    kept = [l for l in links if l not in deferred]
    loops = [c for c in _components(tables, edges_of(kept)) if len(c) > 1]
    if loops:
        detail = "; ".join(
            ", ".join(f"{l.table}.{l.column or '?'} → {l.parent}" for l in kept if l.table in c and l.parent in c)
            for c in loops)
        raise TierCycleError(f"tables that need each other first: {detail} — mark one link soft or override it")

    parents = edges_of(kept)
    tiers: dict[str, int] = {}

    def tier(t: str) -> int:              # graph is acyclic now; iterative to stay off the recursion limit
        stack = [t]
        while stack:
            n = stack[-1]
            todo = [p for p in parents.get(n, ()) if p not in tiers]
            if todo:
                stack.extend(todo)
                continue
            stack.pop()
            tiers[n] = 1 + max((tiers[p] for p in parents.get(n, ())), default=0)
        return tiers[t]

    for t in sorted(tables):
        tier(t)
    if max_tier:
        tiers = {t: min(n, max_tier) for t, n in tiers.items()}
    return TierPlan(tiers, kept, deferred, unresolved, review)


def plan_from_mappings(path: Path, **kw) -> TierPlan:
    tables, report = links_from_mappings(path, **kw)
    return compute_tiers(tables, report)


def rows_from_raw(name: str, tables: Iterable[str] | None = None) -> dict[str, list[dict]]:
    """A facility's source rows from Snowflake <NAME>_RAW.EVENTS_RAW — where
    every loader here lands a facility (source_table + JSON payload).
    Complete tables, so containment is exact. Holds every row in memory:
    fine for a facility's tables, not for a multi-million-row history at once."""
    from facility_to_snowflake_fast_resume import SF_DB, SnowflakeClient
    target = f"{SF_DB}.{re.sub(r'[^A-Za-z0-9]+', '_', name).strip('_').upper()}_RAW.EVENTS_RAW"
    sql = f"SELECT source_table, payload FROM {target} WHERE IS_OBJECT(payload)"
    params: list = []
    if tables:
        tables = list(tables)
        sql += f" AND source_table IN ({', '.join(['%s'] * len(tables))})"
        params = tables
    out: dict[str, list[dict]] = defaultdict(list)
    with SnowflakeClient() as sf:
        cur = sf._conn.cursor()
        try:
            cur.execute(sql, params)
            for table, payload in cur:
                out[table].append(json.loads(payload) if isinstance(payload, str) else payload)
        finally:
            cur.close()
    return dict(sorted(out.items()))


# ─── RUN ─────────────────────────────────────────────────────────────────

def run_in_tiers(plan: TierPlan, fn: Callable[[str], object], *, tables: Iterable[str] | None = None,
                 workers: int = 1, reverse: bool = False) -> dict[str, str]:
    """Run fn(table) tier by tier; within a tier up to `workers` at once.
    fn returning False or raising = failed. Returns {table: done | failed | skipped}.

      reverse=False  parents first (load): a table whose hard parent failed or
                     was skipped is skipped — no rows with broken links
      reverse=True   children first (delete, roll back): a table whose hard
                     child failed or was skipped is skipped — never remove
                     patients while their visits are still there"""
    wanted = set(plan.tiers if tables is None else tables)
    waits_on = plan.children if reverse else plan.parents
    status: dict[str, str] = {}
    for n, group in plan.groups(reverse=reverse):
        group = [t for t in group if t in wanted]
        if not group:
            continue
        ready = []
        for t in group:
            blocked = sorted(p for p in waits_on(t, hard_only=True) if status.get(p) in ("failed", "skipped"))
            if blocked:
                status[t] = "skipped"
                log.warning("  %-40s skipped — %s %s did not go through", t,
                            "child" if reverse else "parent", ", ".join(blocked))
            else:
                ready.append(t)
        log.info("── Tier %d ── %d table(s)%s", n, len(ready),
                 f", {len(group) - len(ready)} skipped" if len(ready) < len(group) else "")

        def one(t: str) -> str:
            try:
                return "failed" if fn(t) is False else "done"
            except Exception as e:
                log.error("  %-40s FAILED: %s", t, e)
                return "failed"

        if workers <= 1:
            for t in ready:
                status[t] = one(t)
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(one, t): t for t in ready}
                for f in as_completed(futures):
                    status[futures[f]] = f.result()
    return status


# ─── CLI ─────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--mappings", type=Path, help="A facility's mappings JSON: tiers of its V3 tables")
    src.add_argument("--rows", type=Path, help="JSON {table: [rows]}: tiers of those tables, from names + data")
    src.add_argument("--raw", metavar="NAME", help="The facility's source rows in Snowflake <NAME>_RAW.EVENTS_RAW")
    ap.add_argument("--tables", nargs="+", help="With --raw: only these source tables")
    ap.add_argument("--overrides", type=Path, help='JSON {table: {column: parent table | null}}, "*" for all tables')
    ap.add_argument("--reverse", action="store_true", help="Children first: the order to delete / roll back in")
    ap.add_argument("--with-parents", nargs="+", metavar="TABLE", help="Only these tables and all they need first")
    ap.add_argument("--with-children", nargs="+", metavar="TABLE", help="Only these tables and all that depend on them")
    ap.add_argument("--json", action="store_true", help="Print the plan as JSON")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    overrides = json.loads(args.overrides.read_text()) if args.overrides else None
    try:
        if args.mappings:
            plan = plan_from_mappings(args.mappings, overrides=overrides)
        else:
            rows = json.loads(args.rows.read_text()) if args.rows else rows_from_raw(args.raw, args.tables)
            plan = compute_tiers(rows, discover_links(rows=rows, overrides=overrides))
    except TierCycleError as e:
        sys.exit(f"No load order: {e}")
    for wanted, walk in ((args.with_parents, plan.with_parents), (args.with_children, plan.with_children)):
        if wanted:
            unknown = sorted(set(wanted) - set(plan.tiers))
            if unknown:
                sys.exit(f"Not in the plan: {unknown}")
            keep = walk(wanted)
            plan = TierPlan({t: n for t, n in plan.tiers.items() if t in keep},
                            [l for l in plan.links if l.table in keep and l.parent in keep],
                            [l for l in plan.deferred if l.table in keep and l.parent in keep],
                            {t: c for t, c in plan.unresolved.items() if t in keep},
                            [r for r in plan.review if r.table in keep])
    if args.json:
        print(json.dumps({"tiers": plan.tiers, "order": plan.order(reverse=args.reverse),
                          "links": [asdict(l) for l in plan.links],
                          "deferred": [asdict(l) for l in plan.deferred], "unresolved": plan.unresolved,
                          "review": [asdict(r) for r in plan.review]}, indent=2, sort_keys=True))
    else:
        print(plan.describe(reverse=args.reverse))


if __name__ == "__main__":
    main()
