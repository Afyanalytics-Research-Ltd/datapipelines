"""
patient_journey_v3.py — the V2 Patient Journey API → V3 link pipeline.

Everything external is faked in memory: the journey API (a fake
requests.Session serving describe / list with the keyset cursor) and the V3
gateway (read with paging, insert match_on=uuid that updates in place or
reports `created`). State files go to a tmp dir. No network, no sleeping.

The properties that matter most:
  * the walk is resumable and never loops or double-counts a patient
  * a record is only placed in V3 by uuid or by the migration id map, never
    guessed; ambiguous / stale / missing rows are reported, not written
  * empty links are filled; a wrong link is only overwritten when the
    overwrite policy allows it
  * only link columns the migration posts AND V3 returned are ever written
  * apply updates in place, stops dead if V3 inserts instead, resumes, and
    a second plan after apply has nothing left to do
"""
from __future__ import annotations

import copy
import json
import logging
from unittest import mock

import pytest
import requests

import patient_journey_v3 as pj
import repair_v3_links as rl
import snowflake_to_v3_migration as m
import v2_to_v3_api_migration as v2v3

ORG = 4
FACILITY = "kisumu_v3"


# ─── fakes ───────────────────────────────────────────────────────────────

class Resp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body, headers or {}
        self.text = json.dumps(body) if body is not None else ""
        self.ok = 200 <= status < 300

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


class FakeJourneyAPI:
    """requests.Session stand-in for POST …/patient/journey."""

    def __init__(self, journeys, *, identity_rows=100):
        self.journeys = sorted(journeys, key=lambda j: j["patient"]["id"])
        self.identity_rows = identity_rows
        self.calls: list[dict] = []
        self.logins: list[dict] = []
        self.tokens = ["tok-1"]           # handed out by login, in order (the last one repeats)
        self.script: list = []           # queued Resp / exceptions returned before normal handling
        self.stuck_cursor = False

    def post(self, url, json=None, timeout=None, headers=None):
        if url.endswith("/api/users/authenticate/user"):
            self.logins.append(dict(json))
            if json.get("password") == "wrong":
                return Resp(400, {"error": "Email_or_password_wrong"})
            tok = self.tokens.pop(0) if len(self.tokens) > 1 else self.tokens[0]
            return Resp(200, {"success": {"token": tok}})
        assert url.endswith(pj.JOURNEY_PATH)
        self.calls.append({"body": dict(json), "auth": (headers or {}).get("Authorization")})
        if self.script:
            nxt = self.script.pop(0)
            if isinstance(nxt, Exception):
                raise nxt
            return nxt
        if json["action"] == "describe":
            return Resp(200, {"success": True, "data": {"coverage": {
                "identity_map_rows": self.identity_rows, "reachable": self.identity_rows, "without_patient": 0}}})
        if json["action"] == "list":
            after, per = int(json["after_id"]), int(json["per_page"])
            rest = [j for j in self.journeys if j["patient"]["id"] > after]
            page = rest[:per]
            more = len(rest) > per
            nxt = after if self.stuck_cursor else (page[-1]["patient"]["id"] if page else after)
            return Resp(200, {"success": True, "data": copy.deepcopy(page),
                              "pagination": {"has_more_pages": more, "next_after_id": nxt}})
        return Resp(400, {"success": False, "message": "unknown action"})


class FakeGateway:
    """The V3 gateways: models = {alias: {"service", "facility", "rows": {id: row}}}."""

    def __init__(self, models, per_page_cap=None):
        self.models = models
        self.inserts: list[dict] = []
        self.fail_next: list[Resp] = []
        self.ignore_updates = False
        self.per_page_cap = per_page_cap

    def post(self, service, body, timeout=30):
        mdl = self.models[body["model"]]
        assert mdl["service"] == service, f"{body['model']} posted to {service}, lives in {mdl['service']}"
        if body["action"] == "read":
            assert body["source_tenant_id"] == ORG
            per = self.per_page_cap or body["per_page"]
            rows = [copy.deepcopy(r) for _, r in sorted(mdl["rows"].items())]
            last = max(1, -(-len(rows) // per))
            p = body["page"]
            return Resp(200, {"data": rows[(p - 1) * per: p * per],
                              "meta": {"total": len(rows), "last_page": last}})
        if body["action"] == "insert":
            self.inserts.append(copy.deepcopy(body))
            if self.fail_next:
                return self.fail_next.pop(0)
            assert body["destination_tenant_id"] == ORG and body["match_on"] == "uuid"
            data = body["data"]
            hit = [r for r in mdl["rows"].values() if r.get("uuid") == data["uuid"]]
            if not hit:
                new_id = max(mdl["rows"] or [0]) + 1
                mdl["rows"][new_id] = {"id": new_id, **data}
                return Resp(200, {"created": True, "id": new_id})
            if not self.ignore_updates:
                for k, v in data.items():
                    assert k in hit[0], f"posted unknown column {k!r} to {body['model']}"
                    hit[0][k] = v
            return Resp(200, {"created": False, "id": hit[0]["id"]})
        raise AssertionError(body)


class NoThrottle:
    """rl.Throttle without its 15s error pauses."""

    def __init__(self, pace, label):
        self.limit, self.active = 1, 0

    def acquire(self):
        pass

    def release(self, ok, seconds):
        pass


# ─── scenario ────────────────────────────────────────────────────────────
# Patient 1 (uuid pu-1) is in V3 by uuid. Patient 2 is a kisumu-style row
# with a synthetic uuid — only the migration id map knows it.

def _rows(*rows):
    return {r["id"]: dict(r) for r in rows}


def make_models():
    return {
        "patient": {"service": "reception", "facility": "facility_id", "rows": _rows(
            {"id": 901, "uuid": "pu-1", "facility_id": 4, "first_name": "A"},
            {"id": 902, "uuid": "synthetic-p2", "facility_id": 4, "first_name": "B"},
        )},
        "visits": {"service": "evaluation", "facility": "facility_id", "rows": _rows(
            {"id": 11, "uuid": "vu-1", "facility_id": 4, "patient": None},          # empty → fill 901
            {"id": 12, "uuid": "vu-2", "facility_id": 4, "patient": 555},           # wrong, by uuid → correct
            {"id": 13, "uuid": "synthetic-v3", "facility_id": 4, "patient": None},  # id map → fill 902
            {"id": 14, "uuid": "synthetic-v4", "facility_id": 4, "patient": 777},   # wrong, id map → kept (strong)
        )},
        "prescriptions": {"service": "evaluation", "facility": "facility_id", "rows": _rows(
            {"id": 21, "uuid": "rx-1", "facility_id": 4, "visit": None},            # fill 11
            {"id": 22, "uuid": "rx-2", "facility_id": 4, "visit": 12},              # already right
            {"id": 23, "uuid": "rx-dup", "facility_id": 4, "visit": None},          # same uuid twice → ambiguous
            {"id": 24, "uuid": "rx-dup", "facility_id": 4, "visit": None},
            {"id": 25, "uuid": "rx-shared", "facility_id": 4, "visit": None},       # two journeys disagree
        )},
        "investigations": {"service": "evaluation", "facility": None, "rows": _rows(
            {"id": 31, "uuid": "inv-1", "visit": None, "patient_id": None, "price": 100},
        )},
        "doctor_notes": {"service": "evaluation", "facility": "facility_id", "rows": _rows(
            {"id": 41, "uuid": "dn-1", "facility_id": 4, "visit": None, "visit_id": None, "patient_id": None},
            {"id": 42, "uuid": None, "facility_id": 4, "visit": None, "visit_id": None},   # can't be updated
        )},
        "vitals": {"service": "evaluation", "facility": "facility_id", "rows": _rows(
            {"id": 51, "uuid": "vt-1", "facility_id": 4, "visit": None, "visit_id": None, "patient_id": None},
        )},
        "vital": {"service": "inpatient", "facility": "facility_id", "rows": _rows(
            {"id": 61, "uuid": "ivt-1", "facility_id": 4, "admission_id": 3},
        )},
    }


ID_MAP = {
    "patient": {"2": 902},
    "visits": {"3": 13, "4": 14, "9": 999},       # 9 → a V3 row that no longer exists
    "prescriptions": {},
}


def rec(table, id_, uuid, module="Evaluation"):
    return {"table": table, "id": id_, "uuid": uuid, "module": module}


def make_journeys():
    return [
        {"patient": {"id": 1, "patient_uuid": "PU-1", "first_name": "A", "last_name": "X"},
         "counts": {}, "visits": [
             {"visit_id": 1, "visit_uuid": "vu-1", "records": [
                 rec("evaluation_prescriptions", 101, "rx-1"),
                 rec("evaluation_investigations", 301, "inv-1"),
                 rec("evaluation_doctor_notes", 401, "dn-1"),
                 rec("evaluation_doctor_notes", 402, "no-such-uuid"),         # not in V3
                 rec("evaluation_vitals", 501, "vt-1"),                      # outpatient vital
                 rec("evaluation_vitals", 502, "ivt-1"),                     # inpatient vital: no link cols
                 rec("evaluation_prescriptions", 103, "rx-dup"),
                 rec("evaluation_prescriptions", 104, "rx-shared"),
             ]},
             {"visit_id": 2, "visit_uuid": "vu-2", "records": [
                 rec("evaluation_prescriptions", 102, "rx-2"),
             ]},
         ],
         "patient_level": [
             rec("reception_patients", 1, "pu-1", "Reception"),
             rec("reception_next_of_kins", 71, "nok-1", "Reception"),       # no V3 model mapped
         ]},
        {"patient": {"id": 2, "patient_uuid": "pu-2-not-in-v3"}, "counts": {}, "visits": [
            {"visit_id": 3, "visit_uuid": "vu-3-not-in-v3", "records": [
                rec("evaluation_prescriptions", 105, "rx-shared"),          # same V3 row, other visit
            ]},
            {"visit_id": 4, "visit_uuid": None, "records": []},
            {"visit_id": 9, "visit_uuid": None, "records": [
                rec("evaluation_doctor_notes", 403, None),                  # V3 row has no uuid
            ]},
        ], "patient_level": []},
    ]


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(v2v3, "STATE_ROOT", tmp_path)
    models = make_models()
    gw = FakeGateway(models, per_page_cap=2)            # several pages per model
    monkeypatch.setattr(v2v3, "_gateway_post", gw.post)
    monkeypatch.setattr(v2v3, "_gateway_model_meta",
                        {a: {"service": d["service"], "facility": d["facility"], "operations": ["insert", "read"]}
                         for a, d in models.items()})
    monkeypatch.setattr(v2v3, "_alias_to_service", {a: d["service"] for a, d in models.items()})
    monkeypatch.setenv("FACILITY_KISUMU_V3_USERNAME", "u")
    monkeypatch.setenv("FACILITY_KISUMU_V3_PASSWORD", "p")
    monkeypatch.delenv("JOURNEY_KISUMU_V3_TOKEN", raising=False)
    monkeypatch.delenv("JOURNEY_KISUMU_V3_BASE_URL", raising=False)
    monkeypatch.delenv("JOURNEY_KISUMU_V3_USERNAME", raising=False)
    monkeypatch.setattr(v2v3, "_v2_token", lambda f: "migration-token")
    monkeypatch.setattr(v2v3, "_v2_invalidate_token", lambda f: None)
    monkeypatch.setattr(v2v3, "_v3_invalidate_token", lambda: None)
    monkeypatch.setattr(pj, "_connect_v3", lambda facility: ORG)
    monkeypatch.setattr(pj.time, "sleep", lambda s: None)
    monkeypatch.setattr(rl, "Throttle", NoThrottle)
    st = pj.State(FACILITY)
    (st.dir / ".migration_id_map.json").write_text(json.dumps(ID_MAP))
    api = FakeJourneyAPI(make_journeys())
    client = pj.JourneyClient(FACILITY, "http://v2.test", session=api, sleep=lambda s: None, min_interval=0)
    return {"st": st, "gw": gw, "models": models, "api": api, "client": client}


def walk_and_plan(env, **kw):
    pj.walk(env["st"], env["client"], per_page=1)
    return pj.run_plan(FACILITY, **kw)


# ─── journey client ──────────────────────────────────────────────────────

class TestClient:
    def _client(self, api, **kw):
        return pj.JourneyClient(FACILITY, "http://v2.test/", session=api, sleep=kw.pop("sleep", lambda s: None),
                                min_interval=0, **kw)

    def test_url_and_auth(self, env):
        c = self._client(env["api"])
        assert c.url == "http://v2.test/api/system/access/patient/journey"
        c.describe()
        assert env["api"].calls[0]["auth"] == "Bearer tok-1"

    def test_base_url_defaults_to_facility_v2_host_and_env_overrides(self, monkeypatch):
        assert pj.JourneyClient(FACILITY, session=object()).base_url == "https://kshospital.collabmed.net"
        monkeypatch.setenv("JOURNEY_KISUMU_V3_BASE_URL", "http://10.0.0.5/")
        assert pj.JourneyClient(FACILITY, session=object()).base_url == "http://10.0.0.5"
        assert pj.JourneyClient(FACILITY, "http://explicit", session=object()).base_url == "http://explicit"

    def test_fixed_token_env(self, env, monkeypatch):
        monkeypatch.setenv("JOURNEY_KISUMU_V3_TOKEN", "static")
        self._client(env["api"]).describe()
        assert env["api"].calls[0]["auth"] == "Bearer static"

    def test_page_clamps_per_page_and_passes_filters(self, env):
        self._client(env["api"]).page(0, 500, modules=["Finance"], tables=["finance_invoices"])
        body = env["api"].calls[0]["body"]
        assert body == {"action": "list", "after_id": 0, "per_page": 100,
                        "modules": ["Finance"], "tables": ["finance_invoices"]}

    def test_429_waits_retry_after(self, env):
        slept = []
        env["api"].script = [Resp(429, {"success": False}, {"Retry-After": "7"})]
        self._client(env["api"], sleep=slept.append).describe()
        assert 7 in slept and len(env["api"].calls) == 2

    def test_429_retry_after_from_body(self, env):
        slept = []
        env["api"].script = [Resp(429, {"retry_after_seconds": 12})]
        self._client(env["api"], sleep=slept.append).describe()
        assert 12 in slept

    def test_5xx_and_network_errors_retry_with_backoff(self, env):
        slept = []
        env["api"].script = [Resp(502, {"message": "bad gw"}), requests.ConnectionError("reset"), Resp(500, None)]
        out = self._client(env["api"], sleep=slept.append).describe()
        assert out["success"] and slept == [5, 10, 20]

    def test_gives_up_after_max_retries(self, env):
        env["api"].script = [Resp(500, {})] * 3
        with pytest.raises(RuntimeError, match="gave up after 3"):
            self._client(env["api"], max_retries=3).describe()

    def test_401_logs_in_again_once(self, env):
        env["api"].tokens = ["old", "new"]
        env["api"].script = [Resp(401, {"message": "expired"})]
        self._client(env["api"]).describe()
        assert [c["auth"] for c in env["api"].calls] == ["Bearer old", "Bearer new"]
        assert len(env["api"].logins) == 2

    def test_logs_in_at_the_journey_host_with_facility_credentials(self, env):
        c = self._client(env["api"])
        c.describe(); c.describe()
        assert env["api"].logins == [{"username": "u", "password": "p"}]      # once, then cached
        assert env["api"].calls[0]["auth"] == "Bearer tok-1"

    def test_journey_credentials_win(self, env, monkeypatch):
        monkeypatch.setenv("JOURNEY_KISUMU_V3_USERNAME", "ju")
        monkeypatch.setenv("JOURNEY_KISUMU_V3_PASSWORD", "jp")
        self._client(env["api"]).describe()
        assert env["api"].logins == [{"username": "ju", "password": "jp"}]

    def test_facility_host_reuses_the_migration_login(self, env):
        api = env["api"]
        c = pj.JourneyClient(FACILITY, session=api, sleep=lambda s: None, min_interval=0)
        assert not c.own_login
        api.post = lambda url, json=None, timeout=None, headers=None: (
            api.calls.append({"body": json, "auth": headers["Authorization"]}) or Resp(200, {"success": True, "data": {}}))
        c.describe()
        assert api.calls[0]["auth"] == "Bearer migration-token" and api.logins == []

    def test_bad_login_is_unavailable(self, env, monkeypatch):
        monkeypatch.setenv("FACILITY_KISUMU_V3_PASSWORD", "wrong")
        with pytest.raises(pj.JourneyUnavailable, match="login at http://v2.test/api/users/authenticate/user failed"):
            self._client(env["api"]).describe()

    def test_no_credentials_is_unavailable(self, env, monkeypatch):
        monkeypatch.delenv("FACILITY_KISUMU_V3_USERNAME")
        with pytest.raises(pj.JourneyUnavailable, match="no credentials"):
            self._client(env["api"]).describe()

    def test_second_401_is_fatal(self, env):
        env["api"].script = [Resp(401, {}), Resp(401, {"message": "nope"})]
        with pytest.raises(pj.JourneyUnavailable, match="refused the token"):
            self._client(env["api"]).describe()

    @pytest.mark.parametrize("status,match", [(404, "isn't deployed"), (503, "identity map isn't installed"),
                                              (403, "refused")])
    def test_unavailable(self, env, status, match):
        env["api"].script = [Resp(status, {"message": "x"})]
        with pytest.raises(pj.JourneyUnavailable, match=match):
            self._client(env["api"]).describe()

    def test_other_4xx_raises_with_message(self, env):
        env["api"].script = [Resp(400, {"success": False, "message": "unknown action"})]
        with pytest.raises(RuntimeError, match="HTTP 400 unknown action"):
            self._client(env["api"]).describe()

    def test_success_false_raises(self, env):
        env["api"].script = [Resp(200, {"success": False, "message": "boom"})]
        with pytest.raises(RuntimeError, match="success=false"):
            self._client(env["api"]).describe()

    def test_min_interval_spaces_requests(self, env):
        slept = []
        c = pj.JourneyClient(FACILITY, "http://v2.test", session=env["api"], sleep=slept.append, min_interval=0.5)
        c.describe(); c.describe()
        assert slept and 0 < slept[-1] <= 0.5


# ─── check / walk ────────────────────────────────────────────────────────

class TestWalk:
    def test_check_reports_coverage(self, env):
        out = pj.check(env["client"])
        assert out["coverage"]["identity_map_rows"] == 100

    def test_check_fails_on_empty_identity_map(self, env):
        env["api"].identity_rows = 0
        with pytest.raises(pj.JourneyUnavailable, match="identity map is empty"):
            pj.check(env["client"])

    def test_walks_every_patient_page_by_page(self, env):
        cur = pj.walk(env["st"], env["client"], per_page=1)
        assert cur["complete"] and cur["walked"] == 2 and cur["after_id"] == 2
        lines = env["st"].journeys.read_text().splitlines()
        assert [json.loads(x)["patient"]["id"] for x in lines] == [1, 2]
        assert [c["body"]["after_id"] for c in env["api"].calls] == [0, 1]

    def test_finished_walk_is_reused_not_refetched(self, env):
        pj.walk(env["st"], env["client"], per_page=1)
        n = len(env["api"].calls)
        pj.walk(env["st"], env["client"], per_page=1)
        assert len(env["api"].calls) == n

    def test_fresh_rewalks_from_scratch(self, env):
        pj.walk(env["st"], env["client"], per_page=1)
        cur = pj.walk(env["st"], env["client"], per_page=5, fresh=True)
        assert cur["walked"] == 2 and len(env["st"].journeys.read_text().splitlines()) == 2

    def test_interrupted_walk_resumes_from_cursor(self, env):
        env["api"].script = [Resp(200, {"success": True, "data": [make_journeys()[0]],
                                        "pagination": {"has_more_pages": True, "next_after_id": 1}})]
        env["api"].script.append(requests.ConnectionError("down"))
        client = pj.JourneyClient(FACILITY, "http://v2.test", session=env["api"], sleep=lambda s: None,
                                  min_interval=0, max_retries=1)
        with pytest.raises(RuntimeError, match="gave up"):
            pj.walk(env["st"], client, per_page=1)
        assert env["st"].cursor()["after_id"] == 1 and not env["st"].cursor()["complete"]
        cur = pj.walk(env["st"], env["client"], per_page=1)
        assert cur["complete"] and cur["walked"] == 2
        assert env["api"].calls[-1]["body"]["after_id"] == 1

    def test_page_on_disk_but_cursor_unsaved_is_not_double_counted(self, env, monkeypatch):
        real_dump = pj.State.dump
        fails = {"n": 1}

        def flaky(self, name, data):
            if name == "cursor.json" and data.get("after_id") == 2 and fails["n"]:
                fails["n"] -= 1
                raise OSError("disk hiccup")
            return real_dump(self, name, data)
        monkeypatch.setattr(pj.State, "dump", flaky)
        with pytest.raises(OSError):
            pj.walk(env["st"], env["client"], per_page=1)
        pj.walk(env["st"], env["client"], per_page=1)        # re-reads patient 2
        ids = [json.loads(x)["patient"]["id"] for x in env["st"].journeys.read_text().splitlines()]
        assert ids == [1, 2, 2]
        patients = {e.patient_id for e in pj.iter_entries(env["st"])}
        assert patients == {1, 2}
        assert sum(1 for e in pj.iter_entries(env["st"]) if e.table == "evaluation_visits") == 5

    def test_stuck_cursor_stops_instead_of_looping(self, env):
        env["api"].stuck_cursor = True
        with pytest.raises(RuntimeError, match="cursor didn't advance"):
            pj.walk(env["st"], env["client"], per_page=1)

    def test_resume_with_different_filters_refused(self, env):
        pj.walk(env["st"], env["client"], per_page=1, tables=["evaluation_prescriptions"], max_pages=1)
        with pytest.raises(RuntimeError, match="re-walk"):
            pj.walk(env["st"], env["client"], per_page=1)

    def test_max_pages_pauses(self, env):
        cur = pj.walk(env["st"], env["client"], per_page=1, max_pages=1)
        assert not cur["complete"] and cur["walked"] == 1

    def test_warnings_are_kept(self, env):
        env["api"].script = [Resp(200, {"success": True, "data": make_journeys(), "warnings": {"decrypt": ["x"]},
                                        "pagination": {"has_more_pages": False, "next_after_id": 2}})]
        pj.walk(env["st"], env["client"])
        w = [json.loads(x) for x in env["st"].path("walk_warnings.jsonl").read_text().splitlines()]
        assert w == [{"after_id": 0, "warnings": {"decrypt": ["x"]}}]


# ─── flattening ──────────────────────────────────────────────────────────

class TestEntries:
    def test_flatten(self, env):
        pj.walk(env["st"], env["client"])
        es = list(pj.iter_entries(env["st"]))
        visits = [e for e in es if e.table == "evaluation_visits"]
        assert [(e.v2_id, e.uuid, e.patient_id) for e in visits] == [
            (1, "vu-1", 1), (2, "vu-2", 1), (3, "vu-3-not-in-v3", 2), (4, None, 2), (9, None, 2)]
        rx1 = next(e for e in es if e.uuid == "rx-1")
        assert (rx1.patient_id, rx1.patient_uuid, rx1.visit_id, rx1.visit_uuid) == (1, "pu-1", 1, "vu-1")
        nok = next(e for e in es if e.table == "reception_next_of_kins")
        assert nok.visit_id is None and nok.visit_uuid is None
        assert len(es) == 5 + 9 + 2 + 2

    def test_alternative_field_names(self, env):
        env["st"].journeys.write_text(json.dumps({
            "patient": {"id": "7", "uuid": "P7"},
            "visits": [{"id": 70, "uuid": "V70", "records": [{"table": "t", "record_id": "5", "record_uuid": "R5"}]}],
            "patient_level": []}) + "\n")
        es = list(pj.iter_entries(env["st"]))
        assert es[0] == pj.Entry("evaluation_visits", "Evaluation", 70, "v70", 7, "p7", None, None)
        assert es[1] == pj.Entry("t", "", 5, "r5", 7, "p7", 70, "v70")

    def test_visit_row_listed_as_record_is_not_doubled(self, env):
        env["st"].journeys.write_text(json.dumps({
            "patient": {"id": 1, "patient_uuid": "p"},
            "visits": [{"visit_id": 5, "visit_uuid": "v5", "records": [
                {"table": "evaluation_visits", "module": "Evaluation", "id": 5, "uuid": "v5"}]}],
            "patient_level": []}) + "\n")
        assert sum(e.table == "evaluation_visits" for e in pj.iter_entries(env["st"])) == 1


# ─── table → model resolution ────────────────────────────────────────────

class TestResolver:
    @pytest.fixture
    def resolve(self, env):
        return pj.Resolver()

    @pytest.mark.parametrize("module,table,alias,service,links", [
        ("Evaluation", "evaluation_visits", "visits", "evaluation", {"patient": "patient"}),
        ("Evaluation", "evaluation_prescriptions", "prescriptions", "evaluation", {"visit": "visit"}),
        ("Evaluation", "evaluation_investigations", "investigations", "evaluation",
         {"visit": "visit", "patient_id": "patient"}),
        ("Evaluation", "evaluation_doctor_notes", "doctor_notes", "evaluation",
         {"visit_id": "visit", "patient_id": "patient", "visit": "visit"}),
        ("Reception", "reception_patients", "patient", "reception", {}),
        ("", "evaluation_prescriptions", "prescriptions", "evaluation", {"visit": "visit"}),   # module missing
    ])
    def test_targets(self, resolve, module, table, alias, service, links):
        [t] = resolve(module, table)
        assert (t.alias, t.service, t.links) == (alias, service, links)

    def test_vitals_split_into_outpatient_and_inpatient(self, resolve):
        out, inp = resolve("Evaluation", "evaluation_vitals")
        assert (out.alias, out.service) == ("vitals", "evaluation")
        assert out.links == {"visit_id": "visit", "patient_id": "patient", "visit": "visit"}
        assert (inp.alias, inp.service, inp.links) == ("vital", "inpatient", {})

    def test_unmapped(self, resolve):
        assert resolve("Reception", "reception_next_of_kins") == []
        assert resolve("Nope", "") == []

    def test_table_aliases_override(self, resolve, monkeypatch):
        monkeypatch.setitem(pj.TABLE_ALIASES, "legacy_rx", ("Evaluation", "evaluation_prescriptions"))
        assert resolve("Whatever", "legacy_rx")[0].alias == "prescriptions"


# ─── plan ────────────────────────────────────────────────────────────────

class TestPlan:
    def test_snapshot_reads_every_page_and_only_needed_columns(self, env):
        walk_and_plan(env)
        snap = env["st"].load("v3_investigations.json")
        assert len(snap["rows"]) == 1 and set(snap["rows"][0]) == {"id", "uuid", "visit", "patient_id"}
        assert "price" in snap["columns"]
        assert len(env["st"].load("v3_prescriptions.json")["rows"]) == 5        # 3 pages of 2

    def test_targets(self, env):
        rep = walk_and_plan(env)
        t = {a: env["st"].load(f"targets_{a}.json") for a in ("visits", "prescriptions", "investigations",
                                                               "doctor_notes", "vitals")}
        assert t["visits"] == {
            "11": {"uuid": "vu-1", "set": {"patient": 901}, "facility": {"facility_id": 4}},
            "12": {"uuid": "vu-2", "set": {"patient": 901}, "facility": {"facility_id": 4}},
            "13": {"uuid": "synthetic-v3", "set": {"patient": 902}, "facility": {"facility_id": 4}},
        }
        assert t["prescriptions"] == {"21": {"uuid": "rx-1", "set": {"visit": 11}, "facility": {"facility_id": 4}}}
        assert t["investigations"] == {"31": {"uuid": "inv-1", "set": {"visit": 11, "patient_id": 901}}}
        assert t["doctor_notes"] == {"41": {"uuid": "dn-1", "set": {"visit_id": 11, "patient_id": 901, "visit": 11},
                                            "facility": {"facility_id": 4}}}
        assert t["vitals"] == {"51": {"uuid": "vt-1", "set": {"visit_id": 11, "patient_id": 901, "visit": 11},
                                      "facility": {"facility_id": 4}}}
        assert rep["rows_to_update"] == 7
        assert rep["apply"] == [{"alias": a, "service": "evaluation"} for a in
                                ("doctor_notes", "investigations", "prescriptions", "visits", "vitals")]

    def test_report_counts(self, env):
        tables = walk_and_plan(env)["tables"]
        assert tables["evaluation_visits"] == {"records": 5, "found_by_uuid": 2, "found_by_id_map": 2,
                                               "id_map_stale": 1}
        assert tables["evaluation_prescriptions"]["uuid_ambiguous"] == 1
        assert tables["evaluation_doctor_notes"] == {"records": 3, "found_by_uuid": 1, "not_in_v3": 2}
        assert tables["evaluation_vitals"] == {"records": 2, "found_by_uuid": 2, "no_link_columns": 1}
        assert tables["reception_next_of_kins"] == {"records": 1, "no_v3_model": 1}
        assert tables["reception_patients"] == {"records": 1, "found_by_uuid": 1, "no_link_columns": 1}
        assert tables["[visits]"] == {"to_fill": 2, "to_correct": 1, "wrong_kept": 1, "rows_to_update": 3}
        # rx-shared is claimed by patient 1 visit 1 AND patient 2 visit 3 — left alone even though
        # patient 2's visit isn't in V3 (so only one claim could have been resolved)
        assert tables["[prescriptions]"] == {"conflicting_journeys": 1, "already_right": 1, "to_fill": 1,
                                             "rows_to_update": 1}

    def test_wrong_link_found_via_id_map_is_kept_under_strong(self, env):
        walk_and_plan(env)
        assert "14" not in env["st"].load("targets_visits.json")
        problems = env["st"].path("problems.csv").read_text()
        assert "v3 row 14: patient is 777, journey says 902 — kept (strong)" in problems

    def test_overwrite_all_corrects_id_map_matches_too(self, env):
        walk_and_plan(env, overwrite="all")
        assert env["st"].load("targets_visits.json")["14"]["set"] == {"patient": 902}

    def test_overwrite_never_only_fills(self, env):
        walk_and_plan(env, overwrite="never")
        assert set(env["st"].load("targets_visits.json")) == {"11", "13"}

    def test_conflicting_journeys_skip_the_row(self, env):
        # rx-shared: patient 1's visit 1 (→ V3 11) vs patient 2's visit 3, here found by uuid
        env["models"]["visits"]["rows"][15] = {"id": 15, "uuid": "vu-3-not-in-v3", "facility_id": 4, "patient": 902}
        rep = walk_and_plan(env)
        assert "25" not in env["st"].load("targets_prescriptions.json")
        assert rep["tables"]["[prescriptions]"]["conflicting_journeys"] == 1
        assert "v3 row 25: claimed by 2 different journeys" in env["st"].path("problems.csv").read_text()

    def test_parent_missing_reported(self, env):
        # visit 3 is only known through the id map (3 -> 13); without that entry it is not in V3
        visits = {k: v for k, v in ID_MAP["visits"].items() if k != "3"}
        (env["st"].dir / ".migration_id_map.json").write_text(json.dumps({**ID_MAP, "visits": visits}))
        rep = walk_and_plan(env)
        assert rep["tables"]["evaluation_prescriptions"]["visit_not_in_v3"] == 1     # rx-shared via visit 3
        assert "visit_not_in_v3" in env["st"].path("problems.csv").read_text()

    def test_id_map_by_uuid_key(self, env):
        (env["st"].dir / ".migration_id_map.json").write_text(json.dumps({**ID_MAP, "patient": {"pu-2-not-in-v3": 902}}))
        walk_and_plan(env)
        assert env["st"].load("targets_visits.json")["13"]["set"] == {"patient": 902}

    def test_uuid_beats_id_map(self, env):
        (env["st"].dir / ".migration_id_map.json").write_text(json.dumps({**ID_MAP, "visits": {**ID_MAP["visits"], "1": 12}}))
        walk_and_plan(env)
        assert env["st"].load("targets_prescriptions.json")["21"]["set"] == {"visit": 11}

    def test_column_v3_does_not_have_is_never_set(self, env):
        for r in env["models"]["doctor_notes"]["rows"].values():
            r.pop("visit", None)
        walk_and_plan(env)
        assert env["st"].load("targets_doctor_notes.json")["41"]["set"] == {"visit_id": 11, "patient_id": 901}

    def test_models_filter(self, env):
        rep = walk_and_plan(env, models=["visits"])
        assert [i["alias"] for i in rep["apply"]] == ["visits"]
        assert rep["tables"]["evaluation_prescriptions"] == {"records": 5, "not_selected": 5}

    def test_bad_overwrite_mode(self, env):
        with pytest.raises(ValueError):
            pj.plan(env["st"], pj.Resolver(), overwrite="sometimes")

    def test_plan_needs_a_walk(self, env):
        with pytest.raises(RuntimeError, match="walk first"):
            pj.run_plan(FACILITY)

    def test_plan_writes_nothing_to_v3(self, env):
        walk_and_plan(env)
        assert env["gw"].inserts == []

    def test_model_not_in_gateway_is_skipped(self, env, monkeypatch):
        meta = dict(v2v3._gateway_model_meta)
        meta.pop("investigations")
        monkeypatch.setattr(v2v3, "_gateway_model_meta", meta)
        rep = walk_and_plan(env)
        assert rep["tables"]["evaluation_investigations"]["not_in_v3"] == 1
        assert not env["st"].path("v3_investigations.json").exists()


# ─── apply / verify / end to end ─────────────────────────────────────────

def link(models, alias, v3id):
    return {k: v for k, v in models[alias]["rows"][v3id].items() if k in pj._LINK_COLS}


class TestApply:
    def test_end_to_end_links_v3_and_second_plan_is_empty(self, env):
        rep = walk_and_plan(env)
        for item in rep["apply"]:
            out = pj.run_apply(FACILITY, item["alias"], item["service"])
            assert out["apply"]["failed"] == 0 and out["verify"]["still_wrong"] == 0
        M = env["models"]
        assert link(M, "visits", 11) == {"patient": 901}
        assert link(M, "visits", 12) == {"patient": 901}
        assert link(M, "visits", 13) == {"patient": 902}
        assert link(M, "visits", 14) == {"patient": 777}                         # kept
        assert link(M, "prescriptions", 21) == {"visit": 11}
        assert link(M, "investigations", 31) == {"visit": 11, "patient_id": 901}
        assert link(M, "doctor_notes", 41) == {"visit": 11, "visit_id": 11, "patient_id": 901}
        assert link(M, "vitals", 51) == {"visit": 11, "visit_id": 11, "patient_id": 901}
        assert M["investigations"]["rows"][31]["price"] == 100                  # other columns untouched
        assert all(len(m_["rows"]) == len(make_models()[a]["rows"]) for a, m_ in M.items())   # nothing inserted
        again = pj.run_plan(FACILITY)
        assert again["rows_to_update"] == 0 and again["apply"] == []

    def test_payload_is_uuid_facility_and_links_only(self, env):
        walk_and_plan(env)
        pj.run_apply(FACILITY, "prescriptions", "evaluation")
        assert env["gw"].inserts == [{"action": "insert", "model": "prescriptions", "destination_tenant_id": ORG,
                                      "match_on": "uuid", "data": {"uuid": "rx-1", "facility_id": 4, "visit": 11}}]

    def test_insert_instead_of_update_stops(self, env):
        walk_and_plan(env)
        env["models"]["visits"]["rows"][11]["uuid"] = "changed-under-us"
        with pytest.raises(SystemExit, match="INSERTED"):
            pj.run_apply(FACILITY, "visits", "evaluation", rl.Pace(1, 1, 0, 5))

    def test_transient_failure_retried(self, env):
        walk_and_plan(env)
        env["gw"].fail_next = [Resp(502, {"message": "bad gateway"}), Resp(500, {})]
        out = pj.run_apply(FACILITY, "prescriptions", "evaluation")
        assert out["apply"] == {"updated": 1, "failed": 0, "failures_sample": []}

    def test_persistent_failure_recorded_and_reported(self, env):
        walk_and_plan(env)
        env["gw"].fail_next = [Resp(500, {"message": "boom"})] * 8
        out = pj.run_apply(FACILITY, "prescriptions", "evaluation")
        assert out["apply"]["failed"] == 1 and out["verify"]["still_wrong"] == 1
        assert "HTTP 500" in env["st"].path("failed_prescriptions.txt").read_text()

    def test_resume_skips_rows_already_applied(self, env):
        walk_and_plan(env)
        env["st"].path("applied_visits.json").write_text(json.dumps(["11", "12"]))
        out = pj.run_apply(FACILITY, "visits", "evaluation", check_after=False)
        assert out["apply"]["updated"] == 1
        assert [i["data"]["uuid"] for i in env["gw"].inserts] == ["synthetic-v3"]
        assert sorted(env["st"].load("applied_visits.json")) == ["11", "12", "13"]

    def test_new_plan_resets_applied_progress(self, env):
        walk_and_plan(env)
        env["st"].path("applied_visits.json").write_text(json.dumps(["11"]))
        pj.run_plan(FACILITY, snapshot_first=False)
        assert not env["st"].path("applied_visits.json").exists()

    def test_verify_catches_updates_v3_ignored(self, env):
        walk_and_plan(env)
        env["gw"].ignore_updates = True
        out = pj.run_apply(FACILITY, "visits", "evaluation")
        assert out["apply"]["updated"] == 3 and out["verify"]["still_wrong"] == 3

    def test_apply_refuses_while_migration_holds_the_table(self, env):
        walk_and_plan(env)
        if v2v3.fcntl is None:
            pytest.skip("no fcntl")
        with m.table_lock("visits"):
            with pytest.raises(m.TableBusy):
                pj.run_apply(FACILITY, "visits", "evaluation")

    def test_parallel_apply_updates_each_row_once(self, env):
        walk_and_plan(env)
        out = pj.run_apply(FACILITY, "visits", "evaluation", rl.Pace(4, 4, 0, 5))
        assert out["apply"]["updated"] == 3
        assert sorted(i["data"]["uuid"] for i in env["gw"].inserts) == ["synthetic-v3", "vu-1", "vu-2"]


class TestCli:
    def test_plan_only_does_not_write_v3(self, env, monkeypatch, capsys):
        monkeypatch.setattr(pj, "JourneyClient", lambda f, url=None: env["client"])
        monkeypatch.setattr("sys.argv", ["patient_journey_v3.py", "--facility", FACILITY])
        with pytest.raises(SystemExit) as ex:
            pj.main()
        assert ex.value.code == 0 and env["gw"].inserts == []
        assert json.loads(capsys.readouterr().out)["plan"]["rows_to_update"] == 7

    def test_execute(self, env, monkeypatch, capsys):
        monkeypatch.setattr(pj, "JourneyClient", lambda f, url=None: env["client"])
        monkeypatch.setattr("sys.argv", ["patient_journey_v3.py", "--facility", FACILITY, "--execute"])
        with pytest.raises(SystemExit) as ex:
            pj.main()
        assert ex.value.code == 0
        out = json.loads(capsys.readouterr().out)
        assert sum(r["apply"]["updated"] for r in out["apply"].values()) == 7

    def test_unavailable_api_exits_cleanly(self, env, monkeypatch):
        env["api"].script = [Resp(404, {"message": ""})]
        monkeypatch.setattr(pj, "JourneyClient", lambda f, url=None: env["client"])
        monkeypatch.setattr("sys.argv", ["patient_journey_v3.py", "--facility", FACILITY])
        with pytest.raises(SystemExit, match="isn't deployed"):
            pj.main()


# ─── the live API's shape (level6.collabmed.net, 2026-10-10) ─────────────

LIVE_PAGE = {
    "success": True,
    "models": {"Evaluation": ["evaluation_visits"]},
    "pagination": {"mode": "keyset", "per_page": 2, "after_id": 0, "next_after_id": 232040,
                   "returned": 2, "has_more_pages": False},
    "data": [{
        "patient": {"id": 232040, "patient_uuid": "05f552ee-0000-0000-0000-000000000001",
                    "registration_uuid": "11795B94-0000-0000-0000-000000000002", "patient_no": 10,
                    "first_name": "x", "middle_name": None, "last_name": "y", "dob": None, "mobile": None, "email": None},
        "visits": [{"visit_uuid": "3feb8752-0000-0000-0000-000000000003", "visit_id": 314103, "records": [
            {"table": "evaluation_investigations", "module": "Evaluation", "type": "investigation",
             "id": 588909, "uuid": "2b5ec1a1-0000-0000-0000-000000000004"},
            {"table": "evaluation_visits", "module": "Evaluation", "type": "visit",
             "id": 314103, "uuid": "3feb8752-0000-0000-0000-000000000003"},
            {"table": "finance_invoice_items", "module": "Finance", "type": "invoice_item",
             "id": 629736, "uuid": "2107919c-0000-0000-0000-000000000005"},
        ]}],
        "patient_level": [
            {"table": "reception_patient_schemes", "module": "Reception", "type": "patient_scheme",
             "id": 60041, "uuid": "5fba691c-0000-0000-0000-000000000006"},
            {"table": "reception_patients", "module": "Reception", "type": "patient",
             "id": 232040, "uuid": "11795b94-0000-0000-0000-000000000002"},
        ],
        "counts": {"visits": 1, "visit_records": 3, "patient_level": 2},
    }],
}


class TestLiveShape:
    @pytest.fixture
    def live(self, env):
        env["api"].script = [Resp(200, copy.deepcopy(LIVE_PAGE))]
        pj.walk(env["st"], env["client"], per_page=2)
        return env

    def test_flatten_live_page(self, live):
        es = list(pj.iter_entries(live["st"]))
        # the visit's own row is listed as a record too: counted once
        assert sum(e.table == "evaluation_visits" for e in es) == 1
        assert len(es) == 1 + 2 + 2
        inv = next(e for e in es if e.table == "evaluation_investigations")
        assert (inv.patient_id, inv.patient_reg_uuid, inv.visit_id) == (232040, "11795b94-0000-0000-0000-000000000002", 314103)

    def test_patient_found_by_registration_uuid(self, live):
        # V3 patient row carries the V2 registration (row) uuid; patient_uuid matches nothing
        M = live["models"]
        M["patient"]["rows"][903] = {"id": 903, "uuid": "11795b94-0000-0000-0000-000000000002", "facility_id": 4}
        M["visits"]["rows"][16] = {"id": 16, "uuid": "3feb8752-0000-0000-0000-000000000003", "facility_id": 4,
                                   "patient": 555}
        M["investigations"]["rows"][32] = {"id": 32, "uuid": "2b5ec1a1-0000-0000-0000-000000000004",
                                           "visit": None, "patient_id": None, "price": 1}
        rep = pj.run_plan(FACILITY)
        assert live["st"].load("targets_visits.json")["16"]["set"] == {"patient": 903}       # corrected: all by uuid
        assert live["st"].load("targets_investigations.json")["32"]["set"] == {"visit": 16, "patient_id": 903}
        assert rep["tables"]["evaluation_visits"]["found_by_uuid"] == 1
        assert rep["tables"]["finance_invoice_items"] == {"records": 1, "no_v3_model": 1}

    def test_kisumu_style_v3_uuids_fall_back_to_id_map(self, live):
        # kisumu V3 rows have V3-generated uuids: only the id map places them,
        # so a wrong link is filled-if-empty but not overwritten under `strong`
        M = live["models"]
        M["patient"]["rows"][380626] = {"id": 380626, "uuid": "a2eb1d41-v3-own", "facility_id": 4}
        M["visits"]["rows"][129941] = {"id": 129941, "uuid": "a2eb4016-v3-own", "facility_id": 4, "patient": 1}
        M["investigations"]["rows"][77] = {"id": 77, "uuid": "a2eb-inv", "visit": None, "patient_id": None, "price": 1}
        (live["st"].dir / ".migration_id_map.json").write_text(json.dumps({
            "patient": {"232040": 380626}, "visits": {"314103": 129941}, "investigations": {"588909": 77}}))
        rep = pj.run_plan(FACILITY)
        assert "129941" not in live["st"].load("targets_visits.json")
        assert rep["tables"]["[visits]"]["wrong_kept"] == 1
        assert live["st"].load("targets_investigations.json")["77"]["set"] == {"visit": 129941, "patient_id": 380626}
        pj.run_plan(FACILITY, overwrite="all", snapshot_first=False)
        assert live["st"].load("targets_visits.json")["129941"]["set"] == {"patient": 380626}
