"""
v3_to_v3_migration under load: tens of thousands of rows, many POST threads,
a target that is slow, flaky or down, and crashes mid-model. The fake target
here is thread-safe and indexed (the one in test_v3_to_v3_migration is a
plain list), so these tests measure the copier, not the fake.
"""
from __future__ import annotations

import itertools
import json
import random
import threading
import time

import pytest
import requests

import v3_to_v3_migration as vv
from v3_to_v3_client import V3Client
from tests.test_v3_to_v3_migration import FakeGateway, _Resp


class FastGateway(FakeGateway):
    """FakeGateway with a uuid index, a lock, optional latency and fault
    injection: fail(payload) → None (ok), an exception to raise, or a _Resp."""

    def __init__(self, *a, latency=0.0, fail=None, **kw):
        super().__init__(*a, **kw)
        self.latency, self.fail = latency, fail
        self._lock = threading.Lock()
        self._by_uuid: dict[str, dict[str, dict]] = {m: {} for m in self.meta}
        self.inflight = self.peak = 0
        self.posts = 0

    def add(self, model, **row):
        rid = super().add(model, **row)
        if row.get("uuid"):
            self._by_uuid[model][row["uuid"]] = self.rows[model][-1]
        return rid

    def insert(self, svc, alias, data, match_on="uuid"):
        model = f"{svc}/{alias}"
        with self._lock:
            self.inflight += 1
            self.peak = max(self.peak, self.inflight)
            self.posts += 1
        try:
            if self.latency:
                time.sleep(self.latency)
            if self.fail:
                bad = self.fail(data)
                if isinstance(bad, Exception):
                    raise bad
                if bad is not None:
                    return bad
            with self._lock:
                hit = self._by_uuid[model].get(data.get("uuid"))
                if hit is not None:
                    hit.update(data)
                    return _Resp(200, {"data": {"id": hit["id"], "created": False}})
                row = {**data, "id": next(self._ids)}
                if self.meta[model].get("tenant"):
                    row[self.meta[model]["tenant"]] = self.organization_id
                self.rows[model].append(row)
                if row.get("uuid"):
                    self._by_uuid[model][row["uuid"]] = row
                self.inserts += 1
                return _Resp(201, {"data": {"id": row["id"], "created": True}})
        finally:
            with self._lock:
                self.inflight -= 1


T = {"tenant": "organization_id"}
MODELS = {
    "core/users": T,
    "reception/patient": {"tenant": "organization_id", "facility": "facility_id"},
    "evaluation/visits": T,
    "evaluation/investigations": T,
    "evaluation/investigation_results": T,
    "evaluation/investigation_result_details": {"tenant": None},
}


def _big(n_patients=2000, visits_per=3, details_per=4, **dst_kw):
    """~n×(1 + v + v + v + v·d) rows: patients → visits → investigations →
    results → details (a parent_rows model), plus other orgs' detail rows."""
    src = FastGateway("src", 4, 4, MODELS, id_start=1)
    dst = FastGateway("dst", 250, 247, MODELS, id_start=5_000_000, **dst_kw)
    users = [src.add("core/users", uuid=f"u{i}", username=f"user{i}", created_by=None) for i in range(20)]
    for i in range(n_patients):
        p = src.add("reception/patient", uuid=f"p{i}", first_name=f"P{i}", facility_id=4,
                    created_by=users[i % 20])
        for j in range(visits_per):
            v = src.add("evaluation/visits", uuid=f"v{i}-{j}", patient=p, user=users[j], status="open")
            inv = src.add("evaluation/investigations", uuid=f"i{i}-{j}", visit=v, procedure_id=None,
                          ordered_by=users[(i + j) % 20])
            r = src.add("evaluation/investigation_results", uuid=f"r{i}-{j}", investigation_id=inv,
                        published_by=users[j], status="final")
            for k in range(details_per):
                src.add("evaluation/investigation_result_details", uuid=f"d{i}-{j}-{k}",
                        investigation_result_id=r, value=f"{k}.{i % 10}")
    for k in range(500):              # another org's details: their parent isn't org 4's
        src.add("evaluation/investigation_result_details", uuid=f"x{k}", investigation_result_id=10_000_000 + k,
                value="x")
    return src, dst


def _order(plan):
    return sorted((m for m, x in plan["models"].items() if x["selected"]), key=lambda m: plan["models"][m]["tier"])


def _copy_all(src, dst, tmp_path, workers=32, **kw):
    state = vv.State("src", "dst", root=tmp_path)
    plan = vv.build_plan(src)
    copier = vv.Copier(src, dst, state, plan, workers=workers, **kw)
    res = {m: copier.copy_model(m) for m in _order(plan)}
    for m in _order(plan):
        copier.relink(m)
    return state, plan, copier, res


def _no_duplicates(dst):
    for m, rows in dst.rows.items():
        uuids = [r["uuid"] for r in rows if r.get("uuid")]
        assert len(uuids) == len(set(uuids)), f"{m}: duplicate uuids on the target"


# ─── scale ──────────────────────────────────────────────────────────────────

def test_checkpoints_while_posting_never_corrupt_the_map(tmp_path, monkeypatch):
    """The map is flushed while 32 threads keep adding to it."""
    monkeypatch.setattr(vv, "CHECKPOINT_SECONDS", 0)      # a checkpoint after every row read
    src, dst = _big(n_patients=300, visits_per=2, details_per=2)
    state, plan, copier, res = _copy_all(src, dst, tmp_path, workers=32)
    assert not any(r.get("failed") for r in res.values())
    _no_duplicates(dst)
    for m in _order(plan):
        on_disk = json.loads((state.dir / "id_map" / f"{m.replace('/', '__')}.json").read_text())
        assert set(on_disk.values()) == {r["id"] for r in dst.rows[m]}, m
        assert vv.verify_model(copier, m)["ok"], m


def test_fifty_thousand_rows_copy_and_verify(tmp_path):
    src, dst = _big(n_patients=2000, visits_per=3, details_per=4)
    t0 = time.time()
    state, plan, copier, res = _copy_all(src, dst, tmp_path, workers=32)
    took = time.time() - t0
    total = sum(r.get("posted", 0) for r in res.values())
    assert total == 20 + 2000 + 3 * 6000 + 24000, res
    assert res["evaluation/investigation_result_details"]["foreign"] == 500
    _no_duplicates(dst)
    for m in _order(plan):
        v = vv.verify_model(copier, m)
        assert v["ok"], (m, {k: v[k] for k in v if k != "mismatch_examples"})
    # the id map on disk is complete and points at real target rows
    for m in ("reception/patient", "evaluation/investigation_result_details"):
        on_disk = json.loads((state.dir / "id_map" / f"{m.replace('/', '__')}.json").read_text())
        target_ids = {r["id"] for r in dst.rows[m]}
        assert len(on_disk) == len(target_ids) and set(on_disk.values()) == target_ids
    assert took < 60, f"copy took {took:.0f}s — something went quadratic"


def test_parent_rows_scale_linearly(tmp_path):
    """_parent_in_source used to reload the parent's id list from disk per
    row: 4× the rows must cost ~4× the time, not ~16×."""
    times = []
    for n in (500, 2000):
        src, dst = _big(n_patients=n, visits_per=1, details_per=4)
        state, plan, copier, _ = _copy_all(src, dst, tmp_path / str(n), workers=8)
        # only the details pass (rows held: their parents are skipped), timed alone
        copier2 = vv.Copier(src, dst, vv.State("src", "dst", root=tmp_path / f"{n}b"), plan, workers=8)
        (tmp_path / f"{n}b" / "v3v3_src_to_dst" / "src_ids").mkdir(parents=True)
        for m in ("evaluation/investigation_results",):
            src_ids = state.load("src_ids", m, [])
            copier2.state.save("src_ids", m, src_ids)
        t0 = time.time()
        res = copier2.copy_model("evaluation/investigation_result_details")
        times.append(time.time() - t0)
        assert res["held"] == 4 * n and res["foreign"] == 500
    assert times[1] < times[0] * 4 * 3 + 0.5, times


def test_posts_really_run_in_parallel(tmp_path):
    src, dst = _big(n_patients=300, visits_per=1, details_per=0, latency=0.01)
    plan = vv.build_plan(src)
    copier = vv.Copier(src, dst, vv.State("src", "dst", root=tmp_path), plan, workers=16)
    copier.copy_model("core/users")
    t0 = time.time()
    res = copier.copy_model("reception/patient")
    took = time.time() - t0
    assert res["posted"] == 300
    assert dst.peak >= 12, f"only {dst.peak} POSTs were ever in flight"
    assert took < 300 * 0.01 / 4, f"{took:.2f}s for 300 × 10 ms POSTs on 16 threads"


def test_queue_is_bounded(tmp_path):
    """The reader can't run ahead of the POST threads unboundedly (memory on a
    250k-row table): at most 4×workers rows wait."""
    src, dst = _big(n_patients=400, visits_per=0, details_per=0, latency=0.002)
    plan = vv.build_plan(src)
    copier = vv.Copier(src, dst, vv.State("src", "dst", root=tmp_path), plan, workers=2)
    copier.copy_model("core/users")
    seen = []
    orig = vv._PostPool.submit

    def spy(self, *a):
        seen.append(self._pool._work_queue.qsize())
        return orig(self, *a)

    vv._PostPool.submit = spy
    try:
        copier.copy_model("reception/patient")
    finally:
        vv._PostPool.submit = orig
    assert max(seen) <= 2 * 4


# ─── faults ─────────────────────────────────────────────────────────────────

def test_target_down_stops_the_model_and_keeps_state(tmp_path):
    src, dst = _big(n_patients=300, visits_per=0, details_per=0)
    plan = vv.build_plan(src)
    state = vv.State("src", "dst", root=tmp_path)
    copier = vv.Copier(src, dst, state, plan, workers=4)
    copier.copy_model("core/users")
    dst.fail = lambda d: requests.ConnectionError("connection refused")
    with pytest.raises(vv.GatewayError, match="in a row failed"):
        copier.copy_model("reception/patient")
    assert dst.posts - 20 < 300                                   # stopped early, not 300 failures
    assert state.load("report", "reception/patient", {})["status"] == "failed"
    assert not (state.dir / "src_ids" / "reception__patient.json").exists()   # partial read never saved
    # target back up: the re-run copies everything, once
    dst.fail = None
    res = vv.Copier(src, dst, vv.State("src", "dst", root=tmp_path), plan, workers=4).copy_model("reception/patient")
    assert res["posted"] + res["already"] == 300 and res["failed"] == 0
    _no_duplicates(dst)


def test_schema_rejecting_every_row_stops_early(tmp_path):
    src, dst = _big(n_patients=500, visits_per=0, details_per=0)
    plan = vv.build_plan(src)
    copier = vv.Copier(src, dst, vv.State("src", "dst", root=tmp_path), plan, workers=4)
    copier.copy_model("core/users")
    dst.fail = lambda d: _Resp(422, {"message": "insert rejected: Field 'x' doesn't have a default value"})
    with pytest.raises(vv.GatewayError, match="all rejected"):
        copier.copy_model("reception/patient")
    assert dst.posts - 20 < 150


def test_flaky_target_loses_nothing_after_a_rerun(tmp_path):
    rnd = random.Random(7)
    lock = threading.Lock()

    def flaky(_):
        with lock:
            r = rnd.random()
        if r < 0.05:
            return requests.Timeout("read timed out")
        if r < 0.10:
            return _Resp(500, {"message": "server error"})
        return None

    src, dst = _big(n_patients=600, visits_per=2, details_per=2, fail=flaky)
    _, plan, _, first = _copy_all(src, dst, tmp_path, workers=16)
    assert any(r.get("failed") for r in first.values())
    dst.fail = None
    state, plan, copier, again = _copy_all(src, dst, tmp_path, workers=16)
    assert not any(r.get("failed") for r in again.values()), again
    _no_duplicates(dst)
    for m in _order(plan):
        assert vv.verify_model(copier, m)["ok"], m


def test_crash_while_reading_source_keeps_progress(tmp_path, monkeypatch):
    src, dst = _big(n_patients=1000, visits_per=0, details_per=0)
    plan = vv.build_plan(src)
    state = vv.State("src", "dst", root=tmp_path)
    copier = vv.Copier(src, dst, state, plan, workers=8)
    copier.copy_model("core/users")
    real = src.read_all

    def dies(svc, alias, per_page=500, workers=1):
        for i, r in enumerate(real(svc, alias)):
            if i == 600:
                raise vv.GatewayError("source read p2: 502")
            yield r

    monkeypatch.setattr(src, "read_all", dies)
    with pytest.raises(vv.GatewayError):
        copier.copy_model("reception/patient")
    on_disk = json.loads((state.dir / "id_map" / "reception__patient.json").read_text())
    assert len(on_disk) == 600                                  # flushed on the way out
    monkeypatch.setattr(src, "read_all", real)
    posts = dst.posts
    res = vv.Copier(src, dst, vv.State("src", "dst", root=tmp_path), plan, workers=8).copy_model("reception/patient")
    assert res["already"] == 600 and res["posted"] == 400 and dst.posts - posts == 400


def test_relink_many_deferred_rows_in_parallel(tmp_path):
    """users ↔ users (created_by → a user later in the source) defers the
    link; relink must patch them all, with one map read per parent."""
    src = FastGateway("src", 4, 4, {"core/users": T}, id_start=1)
    dst = FastGateway("dst", 250, 247, {"core/users": T}, id_start=900_000)
    n = 3000
    for i in range(n):
        src.add("core/users", uuid=f"u{i}", username=f"u{i}", created_by=i + 2 if i + 1 < n else None)
    state = vv.State("src", "dst", root=tmp_path)
    plan = vv.build_plan(src)
    copier = vv.Copier(src, dst, state, plan, workers=16)
    res = copier.copy_model("core/users")
    assert res["deferred"] == n - 1
    reads = []
    real = state.id_map

    def counting(model, reload=False):
        if reload:
            reads.append(model)
        return real(model, reload)

    state.id_map = counting
    t0 = time.time()
    out = copier.relink("core/users")
    assert out == {"relinked": n - 1, "still_missing": 0}
    assert len(reads) == 1 and time.time() - t0 < 10
    ids = {r["uuid"]: r["id"] for r in dst.rows["core/users"]}
    assert all(r["created_by"] == ids[f"u{int(r['uuid'][1:]) + 1}"] for r in dst.rows["core/users"]
               if r["uuid"] != f"u{n - 1}")
    assert vv.verify_model(copier, "core/users")["ok"]


def test_relink_reads_old_state_without_uuids(tmp_path):
    src = FastGateway("src", 4, 4, {"core/users": T}, id_start=1)
    dst = FastGateway("dst", 250, 247, {"core/users": T}, id_start=900_000)
    src.add("core/users", uuid="a", username="a", created_by=2)
    src.add("core/users", uuid="b", username="b", created_by=None)
    state = vv.State("src", "dst", root=tmp_path)
    plan = vv.build_plan(src)
    copier = vv.Copier(src, dst, state, plan, workers=2)
    copier.copy_model("core/users")
    state.save("deferred", "core/users", {"1": {"created_by": 2}})        # written by the old code
    assert copier.relink("core/users") == {"relinked": 1, "still_missing": 0}


# ─── the client's parallel reader ───────────────────────────────────────────

class _PagedClient(V3Client):
    """V3Client whose read_page serves an in-memory table (optionally growing
    while it's read, or serving a row twice across a page boundary)."""

    def __init__(self, rows, grow=0, shift_at=None):
        super().__init__("t", urls={s: "http://x/" for s in ("core",)}, organization_id=1, facility_id=1)
        self.table, self.grow, self.shift_at = rows, grow, shift_at
        self.calls = 0
        self._l = threading.Lock()

    def token(self):
        return "t"

    def read_page(self, service, alias, page, per_page=500):
        with self._l:
            self.calls += 1
            if self.calls == 2 and self.grow:          # rows land after page 1 was read
                start = self.table[-1]["id"] + 1
                self.table += [{"id": i} for i in range(start, start + self.grow)]
            rows = list(self.table)
        if self.shift_at and page > self.shift_at:     # a row inserted mid-table shifts later pages
            off = (page - 1) * per_page - 1
        else:
            off = (page - 1) * per_page
        time.sleep(random.random() / 500)
        last = -(-len(rows) // per_page)
        return rows[off: off + per_page], {"total": len(rows), "last_page": last}


@pytest.mark.parametrize("workers", [1, 8])
@pytest.mark.parametrize("n", [0, 1, 500, 501, 12_345])
def test_read_all_returns_every_row_once_in_order(n, workers):
    c = _PagedClient([{"id": i} for i in range(1, n + 1)])
    got = [r["id"] for r in c.read_all("core", "x", workers=workers)]
    assert got == list(range(1, n + 1))


def test_read_all_picks_up_rows_added_while_reading():
    c = _PagedClient([{"id": i} for i in range(1, 2001)], grow=750)
    got = [r["id"] for r in c.read_all("core", "x", workers=8)]
    assert got == list(range(1, 2751))


def test_read_all_drops_a_row_served_twice():
    c = _PagedClient([{"id": i} for i in range(1, 3001)], shift_at=3)
    got = [r["id"] for r in c.read_all("core", "x", workers=8)]
    assert got == sorted(set(got)) and len(got) == 3000


def test_read_all_stops_reading_when_the_caller_stops():
    c = _PagedClient([{"id": i} for i in range(1, 100_001)])
    for i, _ in enumerate(c.read_all("core", "x", workers=4)):
        if i == 10:
            break
    time.sleep(0.05)
    assert c.calls <= 1 + 4 * 2 + 4        # page 1 + the window, not all 200 pages


def test_read_all_parallel_is_faster():
    rows = [{"id": i} for i in range(1, 20_001)]

    class Slow(_PagedClient):
        def read_page(self, *a, **k):
            time.sleep(0.02)
            return super().read_page(*a, **k)

    t0 = time.time()
    assert len(list(Slow(list(rows)).read_all("core", "x", workers=1))) == 20_000
    one = time.time() - t0
    t0 = time.time()
    assert len(list(Slow(list(rows)).read_all("core", "x", workers=8))) == 20_000
    eight = time.time() - t0
    assert eight < one / 3, (one, eight)


def test_read_page_never_asks_for_more_than_the_gateway_allows():
    c = V3Client("t", urls={"core": "http://x/"}, organization_id=1, facility_id=1)
    c.token = lambda: "t"
    sent = []

    def post(service, body, **kw):
        sent.append(body)
        return _Resp(200, {"data": [], "meta": {"total": 0, "last_page": 1}})

    c.post = post
    list(c.read_all("core", "x", per_page=5000))
    assert sent[0]["per_page"] == 500


def test_post_retries_transient_errors_with_backoff(monkeypatch):
    c = V3Client("t", urls={"core": "http://x/"}, organization_id=1, facility_id=1)
    c.token = lambda: "t"
    codes = iter([503, 502, 500, 200])
    monkeypatch.setattr(time, "sleep", lambda s: None)

    class R:
        def __init__(self, s):
            self.status_code, self.ok, self.text = s, s < 400, ""

    monkeypatch.setattr(c._session, "post", lambda *a, **k: R(next(codes)))
    assert c.post("core", {}, retry_500=True).status_code == 200
    codes = iter([500, 200])
    assert c.post("core", {}).status_code == 500          # an insert's 500 is not retried
