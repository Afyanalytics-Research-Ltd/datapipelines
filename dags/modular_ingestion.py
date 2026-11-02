diff --git a/ingestion/__init__.py b/ingestion/__init__.py
new file mode 100644
index 0000000..1ac5c80
--- /dev/null
+++ b/ingestion/__init__.py
@@ -0,0 +1,8 @@
+"""Modular, per-domain ingestion for facility → V3 migrations.
+
+A *domain* is a unit of ingestion (patients, visits, clinical, billing, ...) that
+declares what it depends on. A *facility adapter* supplies the source rows and
+payload builders for the domains it supports. The runner orders domains by
+dependency, so a facility only implements the domains it has data for.
+"""
+from .core import Domain, Registry, Poster, RunReport, DomainError  # noqa: F401
diff --git a/ingestion/adapters_portbell.py b/ingestion/adapters_portbell.py
new file mode 100644
index 0000000..88aaf91
--- /dev/null
+++ b/ingestion/adapters_portbell.py
@@ -0,0 +1,18 @@
+"""Portbell wired into the registry by delegating to the existing portbell_to_v3.Run.
+
+Nothing in portbell_to_v3.py changes: each domain handler just calls the matching
+Run method, so behaviour is identical and the registry adds ordering, selection,
+and per-domain failure isolation on top.
+"""
+from __future__ import annotations
+
+from .core import Registry
+from .domains import STANDARD
+
+
+def build_registry(run) -> Registry:
+    """`run` is a portbell_to_v3.Run (already logged in to V3 / Snowflake)."""
+    reg = Registry("portbell")
+    for dom in STANDARD:
+        reg.register(dom, lambda _poster, _m=getattr(run, dom.name): _m())
+    return reg
diff --git a/ingestion/core.py b/ingestion/core.py
new file mode 100644
index 0000000..f1119c7
--- /dev/null
+++ b/ingestion/core.py
@@ -0,0 +1,161 @@
+"""Core of the per-domain ingestion strategy: Domain, Registry, Poster, runner."""
+from __future__ import annotations
+
+import json
+import logging
+import threading
+from collections import Counter
+from concurrent.futures import ThreadPoolExecutor
+from dataclasses import dataclass, field
+from typing import Callable, Iterable
+
+log = logging.getLogger("ingestion")
+
+
+class DomainError(Exception):
+    pass
+
+
+@dataclass(frozen=True)
+class Domain:
+    """One ingestible slice of a facility's data.
+
+    name        unique key, also the step name used in DAG params / CLI
+    depends_on  domains whose rows must already be in V3 (parents first)
+    group       optional label for parallel lanes in Airflow (clinical / money / structure)
+    """
+    name: str
+    depends_on: tuple[str, ...] = ()
+    group: str = "base"
+    description: str = ""
+
+
+@dataclass
+class RunReport:
+    steps: dict[str, dict] = field(default_factory=dict)
+    skipped: dict[str, str] = field(default_factory=dict)
+
+    @property
+    def failed(self) -> dict[str, dict]:
+        return {s: r for s, r in self.steps.items() if "error" in r or r.get("failed")}
+
+
+class Poster:
+    """Idempotent bulk writer to V3, shared by every domain and facility.
+
+    Rows carry a stable uuid5 and are posted with match_on=uuid, so re-runs update
+    instead of duplicating. Mirrors the semantics of the existing Run.post() in
+    gyno_to_v3.py / portbell_to_v3.py (limit = canary, only_missing = resume,
+    dry_run = build payloads only) so those classes can delegate here later.
+    """
+
+    def __init__(self, client, *, dry_run=True, limit=0, workers=8, only_missing=False):
+        self.c, self.dry_run, self.limit = client, dry_run, limit
+        self.workers, self.only_missing = workers, only_missing
+
+    def ids_by_uuid(self, svc: str, alias: str) -> dict[str, int]:
+        return {str(r["uuid"]).lower(): r["id"]
+                for r in self.c.read_all(svc, alias, workers=8) if r.get("uuid")}
+
+    def post(self, svc: str, alias: str, payloads: list[dict]) -> Counter:
+        skipped = 0
+        if self.only_missing:
+            have = set(self.ids_by_uuid(svc, alias))
+            before = len(payloads)
+            payloads = [p for p in payloads if str(p["uuid"]).lower() not in have]
+            skipped = before - len(payloads)
+        if self.limit:
+            payloads = payloads[: self.limit]
+        if self.dry_run:
+            for p in payloads[:2]:
+                log.info("  dry-run %s/%s: %s", svc, alias, json.dumps(p, default=str)[:600])
+            return Counter(would_post=len(payloads))
+        res, lock = Counter(), threading.Lock()
+
+        def one(p):
+            r = self.c.insert(svc, alias, p, match_on="uuid")
+            ok = r.ok and (r.json() or {}).get("success", True)
+            with lock:
+                res["ok" if ok else "failed"] += 1
+                if not ok and res["failed"] <= 5:
+                    log.error("  %s/%s %s: %s %s", svc, alias, p.get("uuid"), r.status_code, r.text[:300])
+
+        with ThreadPoolExecutor(self.workers) as ex:
+            list(ex.map(one, payloads))
+        if skipped:
+            res["already in V3 (skipped)"] = skipped
+        return res
+
+
+class Registry:
+    """Maps domain name → (Domain, handler). A facility registers only what it has."""
+
+    def __init__(self, target: str):
+        self.target = target
+        self._domains: dict[str, Domain] = {}
+        self._handlers: dict[str, Callable[[Poster], dict]] = {}
+
+    def register(self, domain: Domain, handler: Callable[[Poster], dict]) -> None:
+        if domain.name in self._domains:
+            raise DomainError(f"domain {domain.name!r} already registered for {self.target}")
+        self._domains[domain.name] = domain
+        self._handlers[domain.name] = handler
+
+    @property
+    def names(self) -> list[str]:
+        return list(self._domains)
+
+    def plan(self, wanted: Iterable[str] | None = None, *, include_deps: bool = False) -> list[str]:
+        """Dependency-ordered list of domains to run (stable, deterministic)."""
+        wanted = list(wanted) if wanted else self.names
+        unknown = [w for w in wanted if w not in self._domains]
+        if unknown:
+            raise DomainError(f"{self.target}: unknown domain(s) {unknown}; have {self.names}")
+        selected: set[str] = set(wanted)
+        if include_deps:
+            stack = list(wanted)
+            while stack:
+                for d in self._domains[stack.pop()].depends_on:
+                    if d not in selected:
+                        selected.add(d)
+                        stack.append(d)
+        order, state = [], {}
+
+        def visit(n: str):
+            if state.get(n) == 2:
+                return
+            if state.get(n) == 1:
+                raise DomainError(f"dependency cycle through {n!r}")
+            state[n] = 1
+            for d in self._domains[n].depends_on:
+                if d not in self._domains:
+                    raise DomainError(f"{n!r} depends on unregistered domain {d!r} for {self.target}")
+                visit(d)
+            state[n] = 2
+            if n in selected:
+                order.append(n)
+
+        for n in self.names:
+            if n in selected:
+                visit(n)
+        return order
+
+    def run(self, poster: Poster, wanted: Iterable[str] | None = None, *, include_deps=False) -> RunReport:
+        report, done_ok = RunReport(), set()
+        order = self.plan(wanted, include_deps=include_deps)
+        planned = set(order)
+        for name in order:
+            # a parent that was planned but did not finish cleanly (failed OR skipped) blocks its children
+            blocked = [d for d in self._domains[name].depends_on if d in planned and d not in done_ok]
+            if blocked:
+                report.skipped[name] = f"parent step(s) not completed: {blocked}"
+                continue
+            try:
+                out = self._handlers[name](poster)
+            except Exception as e:  # one domain's failure must not hide the rest of the report
+                log.exception("domain %s failed", name)
+                out = {"error": f"{type(e).__name__}: {e}"}
+            report.steps[name] = dict(out)
+            if "error" not in out and not out.get("failed"):
+                done_ok.add(name)
+        return report
diff --git a/ingestion/domains/__init__.py b/ingestion/domains/__init__.py
new file mode 100644
index 0000000..ac38b55
--- /dev/null
+++ b/ingestion/domains/__init__.py
@@ -0,0 +1,10 @@
+"""Standard domain definitions. Facility adapters import these and attach handlers."""
+from ..core import Domain
+
+PATIENTS = Domain("patients", group="base", description="reception/patient")
+VISITS = Domain("visits", ("patients",), "structure", "evaluation/visits")
+VITALS = Domain("vitals", ("visits",), "clinical", "evaluation/vitals")
+DOCTOR_NOTES = Domain("doctor_notes", ("visits",), "clinical", "evaluation/doctor_notes")
+PAYMENTS = Domain("payments", ("patients",), "money", "finance/payment")
+
+STANDARD = (PATIENTS, VISITS, VITALS, DOCTOR_NOTES, PAYMENTS)
diff --git a/tests/test_ingestion_registry.py b/tests/test_ingestion_registry.py
new file mode 100644
index 0000000..598770c
--- /dev/null
+++ b/tests/test_ingestion_registry.py
@@ -0,0 +1,68 @@
+import pytest
+from ingestion import Domain, DomainError, Poster, Registry
+from ingestion.domains import STANDARD
+
+
+def _reg(handlers=None):
+    r = Registry("t")
+    for d in STANDARD:
+        r.register(d, (handlers or {}).get(d.name, lambda p, n=d.name: {"ok": 1}))
+    return r
+
+
+def test_plan_orders_parents_first_even_when_selected_out_of_order():
+    assert _reg().plan(["payments", "vitals", "patients", "visits"]) == ["patients", "visits", "vitals", "payments"]
+
+
+def test_include_deps_pulls_in_parents():
+    assert _reg().plan(["vitals"], include_deps=True) == ["patients", "visits", "vitals"]
+
+
+def test_unknown_domain_rejected():
+    with pytest.raises(DomainError):
+        _reg().plan(["nurse_notes"])
+
+
+def test_cycle_detected():
+    r = Registry("t")
+    r.register(Domain("a", ("b",)), lambda p: {})
+    r.register(Domain("b", ("a",)), lambda p: {})
+    with pytest.raises(DomainError):
+        r.plan()
+
+
+def test_failure_isolated_and_children_skipped():
+    def boom(p):
+        raise RuntimeError("v3 down")
+    rep = _reg({"visits": boom}).run(Poster(None))
+    assert "error" in rep.steps["visits"]
+    assert "vitals" in rep.skipped and "doctor_notes" in rep.skipped
+    assert rep.steps["payments"] == {"ok": 1}      # independent branch still ran
+
+
+class _Resp:
+    ok = True
+    def json(self): return {"success": True}
+
+
+class _Client:
+    def __init__(self, have=()): self.have, self.posted = have, []
+    def read_all(self, svc, alias, workers=8): return [{"uuid": u, "id": i} for i, u in enumerate(self.have)]
+    def insert(self, svc, alias, p, match_on): self.posted.append(p["uuid"]); return _Resp()
+
+
+def test_poster_only_missing_limit_and_dry_run():
+    ps = [{"uuid": u} for u in "abcd"]
+    c = _Client(have=["a"])
+    out = Poster(c, dry_run=False, only_missing=True, limit=2).post("s", "x", ps)
+    assert out["ok"] == 2 and out["already in V3 (skipped)"] == 1 and sorted(c.posted) == ["b", "c"]
+    assert Poster(c, dry_run=True).post("s", "x", ps)["would_post"] == 4
+
+
+def test_skip_is_transitive():
+    r = Registry("t")
+    r.register(Domain("a"), lambda p: (_ for _ in ()).throw(RuntimeError("x")))
+    r.register(Domain("b", ("a",)), lambda p: {})
+    r.register(Domain("c", ("b",)), lambda p: {})
+    rep = r.run(Poster(None))
+    assert set(rep.skipped) == {"b", "c"}