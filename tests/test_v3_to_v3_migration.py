"""
v3_to_v3_migration against two in-memory gateways: the target gives every
row an id far from its source id, so any link that isn't remapped shows up.
"""
from __future__ import annotations

import itertools
import json
from pathlib import Path

import pytest

import v3_to_v3_links as L
import v3_to_v3_migration as vv


class FakeGateway:
    """Enough of V3Client for the migration: list / read / describe / insert
    (match_on uuid = update in place), one tenant per client."""

    def __init__(self, key, org, facility, models, id_start, unknown_cols=()):
        self.key, self.organization_id, self.facility_id = key, org, facility
        self.meta = models                   # "svc/alias" → {"tenant": col|None, "facility": col|None}
        self.rows: dict[str, list[dict]] = {m: [] for m in models}
        self._ids = itertools.count(id_start)
        self.unknown_cols = set(unknown_cols)
        self.inserts = 0

    def token(self):
        return "t"

    def models(self, svc):
        return [{"alias": m.split("/", 1)[1], "tenant": x.get("tenant"), "facility": x.get("facility"),
                 "operations": ["read", "insert", "describe"]} for m, x in self.meta.items()
                if m.startswith(svc + "/")]

    def describe(self, svc, alias):
        return {"excluded": ["id", "organization_id", "created_at", "updated_at", "deleted_at"]}

    def _visible(self, model):
        tenant = self.meta[model].get("tenant")
        return [r for r in self.rows[model] if not tenant or r.get(tenant) == self.organization_id]

    def read_page(self, svc, alias, page, per_page=500):
        rows = self._visible(f"{svc}/{alias}")
        return [dict(r) for r in rows[(page - 1) * per_page: page * per_page]], {"total": len(rows)}

    def read_all(self, svc, alias, per_page=500):
        yield from (dict(r) for r in self._visible(f"{svc}/{alias}"))

    def insert(self, svc, alias, data, match_on="uuid"):
        model = f"{svc}/{alias}"
        bad = self.unknown_cols & set(data)
        if bad:
            return _Resp(422, {"message": f"insert rejected: Unknown column '{sorted(bad)[0]}' in 'field list'"})
        self.inserts += 1
        for r in self.rows[model]:
            if data.get("uuid") and r.get("uuid") == data["uuid"]:
                r.update(data)
                return _Resp(200, {"data": {"id": r["id"], "created": False}})
        row = {**data, "id": next(self._ids)}
        if self.meta[model].get("tenant"):
            row[self.meta[model]["tenant"]] = self.organization_id
        self.rows[model].append(row)
        return _Resp(201, {"data": {"id": row["id"], "created": True}})

    def add(self, model, **row):
        tenant = self.meta[model].get("tenant")
        row.setdefault("id", next(self._ids))
        if tenant:
            row.setdefault(tenant, self.organization_id)
        self.rows[model].append(row)
        return row["id"]


class _Resp:
    def __init__(self, status, body):
        self.status_code, self._body = status, body
        self.ok = status < 400
        self.text = json.dumps(body)

    def json(self):
        return self._body


T = {"tenant": "organization_id"}
MODELS = {
    "core/departments": T, "core/users": T,
    "reception/patient": {"tenant": "organization_id", "facility": "facility_id"},
    "evaluation/visits": T, "evaluation/investigations": T,
    "evaluation/investigation_results": T,
    "evaluation/investigation_result_details": {"tenant": None},
    "evaluation/symptoms": {"tenant": None},
    "core/organizations": {"tenant": None},
}


def _pair(unknown_cols=()):
    src = FakeGateway("src", 4, 4, MODELS, id_start=1)
    dst = FakeGateway("dst", 250, 247, MODELS, id_start=90_000, unknown_cols=unknown_cols)
    src.add("core/organizations", uuid="o4", name="Kisumu")
    d1 = src.add("core/departments", uuid="d1", name="Lab", head_user_id=None)
    u1 = src.add("core/users", uuid="u1", username="ann", department_id=d1, created_by=None)
    u2 = src.add("core/users", uuid="u2", username="ben", department_id=d1, created_by=u1)
    src.rows["core/departments"][0]["head_user_id"] = u2          # departments ↔ users cycle
    p1 = src.add("reception/patient", uuid="p1", first_name="Amy", facility_id=4, created_by=u1)
    p2 = src.add("reception/patient", uuid="p2", first_name="Bo", facility_id=4, created_by=u2)
    v1 = src.add("evaluation/visits", uuid="v1", patient=p1, user=u1, status="open")
    v2 = src.add("evaluation/visits", uuid="v2", patient=p2, user=u2, status="closed")
    i1 = src.add("evaluation/investigations", uuid="i1", visit=v1, procedure_id=None, ordered_by=u1)
    i2 = src.add("evaluation/investigations", uuid="i2", visit=v2, procedure_id=None, ordered_by=u2)
    r1 = src.add("evaluation/investigation_results", uuid="r1", investigation_id=i1, published_by=u2, status="final")
    src.add("evaluation/investigation_result_details", uuid="rd1", investigation_result_id=r1, value="5.1")
    # another org's detail row — the model has no tenant column, the parent isn't org 4's
    src.add("evaluation/investigation_result_details", uuid="rdX", investigation_result_id=777, value="9")
    src.add("evaluation/symptoms", uuid="s-src-1", name="Fever")
    src.add("evaluation/symptoms", uuid="s-src-2", name="Rash")
    dst.add("evaluation/symptoms", uuid="s-dst-1", name="fever")   # target already has it (other uuid)
    return src, dst


def _run(src, dst, tmp_path, **kw):
    state = vv.State("src", "dst", root=tmp_path)
    plan = vv.build_plan(src)
    copier = vv.Copier(src, dst, state, plan, workers=2, **kw)
    order = sorted((m for m, x in plan["models"].items() if x["selected"]), key=lambda m: plan["models"][m]["tier"])
    results = {m: copier.copy_model(m) for m in order}
    for m in order:
        copier.relink(m)
    return state, plan, copier, order, results


def test_plan_orders_parents_first_and_skips_the_org_itself():
    src, _ = _pair()
    plan = vv.build_plan(src)
    m = plan["models"]
    assert m["core/organizations"]["kind"] == "skip"
    assert m["evaluation/investigation_result_details"]["kind"] == "parent_rows"
    assert m["evaluation/symptoms"]["kind"] == "reference"
    assert m["reception/patient"]["tier"] < m["evaluation/visits"]["tier"] < m["evaluation/investigations"]["tier"]
    assert m["evaluation/investigation_results"]["tier"] < m["evaluation/investigation_result_details"]["tier"]


def test_full_copy_mirrors_every_row_and_link(tmp_path):
    src, dst = _pair()
    state, plan, copier, order, _ = _run(src, dst, tmp_path)
    for model in order:
        res = vv.verify_model(copier, model)
        assert res["ok"], (model, res)
    # links point at the TARGET's rows
    pat = {r["uuid"]: r["id"] for r in dst.rows["reception/patient"]}
    usr = {r["uuid"]: r["id"] for r in dst.rows["core/users"]}
    v1 = next(r for r in dst.rows["evaluation/visits"] if r["uuid"] == "v1")
    assert v1["patient"] == pat["p1"] and v1["user"] == usr["u1"] and v1["organization_id"] == 250
    assert all(r["facility_id"] == 247 for r in dst.rows["reception/patient"])
    # the cycle is closed by the relink pass
    dept = dst.rows["core/departments"][0]
    assert dept["head_user_id"] == usr["u2"]
    ben = next(r for r in dst.rows["core/users"] if r["uuid"] == "u2")
    assert ben["created_by"] == usr["u1"]


def test_other_orgs_rows_are_not_copied_and_reference_rows_are_linked(tmp_path):
    src, dst = _pair()
    _, _, _, _, results = _run(src, dst, tmp_path)
    details = dst.rows["evaluation/investigation_result_details"]
    assert [r["uuid"] for r in details] == ["rd1"]
    assert results["evaluation/investigation_result_details"]["foreign"] == 1
    symptoms = {r["name"]: r for r in dst.rows["evaluation/symptoms"]}
    assert set(symptoms) == {"fever", "Rash"}                 # Fever linked by name, Rash inserted
    assert results["evaluation/symptoms"] == {"read": 2, "linked": 1, "posted": 1, "failed": 0, "status": "done"}
    assert not dst.rows["core/organizations"]


def test_rerun_posts_nothing_new(tmp_path):
    src, dst = _pair()
    _run(src, dst, tmp_path)
    before = {m: len(r) for m, r in dst.rows.items()}
    _, _, _, _, again = _run(src, dst, tmp_path)
    assert {m: len(r) for m, r in dst.rows.items()} == before
    assert again["evaluation/visits"]["posted"] == 0 and again["evaluation/visits"]["already"] == 2


def test_child_waits_for_its_parent(tmp_path):
    src, dst = _pair()
    state = vv.State("src", "dst", root=tmp_path)
    plan = vv.build_plan(src)
    copier = vv.Copier(src, dst, state, plan, workers=1)
    res = copier.copy_model("evaluation/visits")                 # patients not copied yet
    assert res["held"] == 2 and res["status"] == "waiting" and not dst.rows["evaluation/visits"]
    copier.copy_model("core/departments")
    copier.copy_model("core/users")
    copier.copy_model("reception/patient")
    res = copier.copy_model("evaluation/visits")
    assert res["posted"] == 2 and res["held"] == 0


def test_canary_posts_at_most_n_per_model(tmp_path):
    src, dst = _pair()
    _, _, _, order, results = _run(src, dst, tmp_path, record_limit=1)
    assert len(dst.rows["reception/patient"]) == 1
    assert results["reception/patient"]["status"] == "canary"


def test_users_get_a_random_password_and_must_reset(tmp_path):
    src, dst = _pair()
    _run(src, dst, tmp_path)
    pw = [r["password"] for r in dst.rows["core/users"]]
    assert all(len(p) >= 20 for p in pw) and len(set(pw)) == len(pw)
    assert all(r["enforce_password_reset"] == 1 for r in dst.rows["core/users"])


def test_column_the_target_lacks_is_dropped_not_fatal(tmp_path):
    src, dst = _pair(unknown_cols={"status"})
    _, _, copier, _, results = _run(src, dst, tmp_path)
    assert results["evaluation/visits"]["failed"] == 0 and len(dst.rows["evaluation/visits"]) == 2
    assert "status" not in dst.rows["evaluation/visits"][0]


def test_verify_catches_a_changed_field_and_a_missing_row(tmp_path):
    src, dst = _pair()
    _, _, copier, _, _ = _run(src, dst, tmp_path)
    dst.rows["evaluation/visits"][0]["status"] = "tampered"
    dst.rows["reception/patient"].pop()
    v = vv.verify_model(copier, "evaluation/visits")
    p = vv.verify_model(copier, "reception/patient")
    assert not v["ok"] and v["mismatched_fields"] == {"status": 1}
    assert not p["ok"] and p["missing"] == 1


def test_norm_treats_formats_alike():
    assert vv._norm("2026-10-13T08:00:00.000000Z") == vv._norm("2026-10-13 08:00:00")
    assert vv._norm(True) == vv._norm(1) == vv._norm("1") == vv._norm("1.00")
    assert vv._norm("") is None and vv._norm(None) is None
    assert vv._norm({"a": 1}) == vv._norm('{"a": 1}')


# every id-like column org 4 really has is covered by a rule (snapshot of the
# source's columns: tests/fixtures/v3v3_org4_columns.json)
FIXTURE = Path(__file__).parent / "fixtures" / "v3v3_org4_columns.json"


@pytest.mark.skipif(not FIXTURE.exists(), reason="no column snapshot")
def test_every_id_column_of_org4_has_a_rule():
    cols = json.loads(FIXTURE.read_text())
    gaps = sorted(f"{m}.{c}" for m, cs in cols.items() for c in cs
                  if L.looks_like_id(c) and L.link_target(m, c) is None and c not in L.MODEL_LINKS.get(m, {}))
    assert gaps == []


@pytest.mark.skipif(not FIXTURE.exists(), reason="no column snapshot")
def test_link_targets_are_real_models():
    cols = json.loads(FIXTURE.read_text())
    for m, cs in cols.items():
        for c in cs:
            t = L.link_target(m, c)
            if t not in (None, L.FACILITY, L.DROP):
                assert t in cols, f"{m}.{c} → {t} is not a source model"


def test_canary_verify_compares_only_the_copied_rows(tmp_path):
    src, dst = _pair()
    _, _, copier, order, _ = _run(src, dst, tmp_path, record_limit=1)
    res = vv.verify_model(copier, "reception/patient", canary=True)
    assert res["ok"] and res["source"] == 1 and res["missing"] == 0
    full = vv.verify_model(copier, "reception/patient")
    assert not full["ok"] and full["missing"] == 1
    dst.rows["reception/patient"][0]["first_name"] = "changed"
    assert not vv.verify_model(copier, "reception/patient", canary=True)["ok"]


def test_crash_midway_resumes_without_duplicates_or_new_passwords(tmp_path):
    src, dst = _pair()
    state = vv.State("src", "dst", root=tmp_path)
    plan = vv.build_plan(src)
    copier = vv.Copier(src, dst, state, plan, workers=1)
    for m in ("core/departments", "core/users", "reception/patient"):
        copier.copy_model(m)
    # "crash": the id maps on disk are lost, the target keeps its rows
    import shutil
    shutil.rmtree(state.dir / "id_map")
    passwords = {r["uuid"]: r["password"] for r in dst.rows["core/users"]}
    before = {m: len(r) for m, r in dst.rows.items()}
    fresh = vv.Copier(src, dst, vv.State("src", "dst", root=tmp_path), plan, workers=1)
    for m in ("core/departments", "core/users", "reception/patient"):
        res = fresh.copy_model(m)
        assert res["posted"] == 0, (m, res)
    assert {m: len(r) for m, r in dst.rows.items()} == before
    assert {r["uuid"]: r["password"] for r in dst.rows["core/users"]} == passwords
    # the maps were rebuilt from the target, so children still link correctly
    res = fresh.copy_model("evaluation/visits")
    assert res["posted"] == 2 and res["held"] == 0
    for m in ("core/departments", "core/users"):
        fresh.relink(m)
    for m in ("core/departments", "core/users", "reception/patient", "evaluation/visits"):
        assert vv.verify_model(fresh, m)["ok"], m
