"""v2_delta_to_v3: the decisions that make a delta safe — which V2 rows count as changed, how links are
chained to the target, and which payloads update an existing record vs insert a new one."""
from __future__ import annotations

from fast_v2_live_to_v3 import changed_since
from v2_delta_to_v3 import compose_id_map, plan_delta, target_id


def test_target_id_records_updates_as_well_as_inserts():
    assert target_id(501, None) == 501            # insert: the id the post returned
    assert target_id(None, 77) == 77              # update: the post returns no id → the matched row
    assert target_id("501", None) == 501
    assert target_id(501, 77) == 501
    assert target_id(None, None) is None          # duplicate key / nothing posted
    assert target_id(0, None) is None
    assert target_id("", "") is None


def test_changed_since_new_and_edits():
    since = "2026-10-16"
    assert changed_since({"created_at": "2026-10-16 08:00:00"}, since)
    assert changed_since({"created_at": "2026-01-01 08:00:00", "updated_at": "2026-10-17T05:00:00.000000Z"}, since)
    assert not changed_since({"created_at": "2026-01-01 08:00:00", "updated_at": "2026-10-15 23:59:59"}, since)
    # new only: an edit to an old record doesn't count
    assert not changed_since({"created_at": "2026-01-01", "updated_at": "2026-10-17"}, since, new_only=True)
    # a time in `since`
    assert not changed_since({"created_at": "2026-10-16 07:59:59"}, "2026-10-16 08:00")
    assert not changed_since({}, since)


def test_compose_chains_v2_ids_to_the_target_and_drops_missing_parents():
    v2_map = {"visits": {"100": 5000, "101": 5001, "102": 5002}, "ward": {"7": 70}, "orphan": {"1": 1}}
    v3v3 = {"evaluation/visits": {"5000": 90000, "5001": 90001}, "inpatient/ward": {"70": 700},
            "dialysis/ward": {"70": 999}}
    out, dropped, services = compose_id_map(v2_map, v3v3, {"visits": "evaluation", "ward": "inpatient"})
    assert out["visits"] == {"100": 90000, "101": 90001}      # 102's source visit isn't on the target
    assert out["ward"] == {"7": 700}                           # the service the V2 load uses, not dialysis
    assert dropped == {"visits": 1, "orphan": 1}
    assert services == {"visits": "evaluation", "ward": "inpatient"}


def test_compose_pins_a_shared_alias_to_the_service_the_copy_used():
    """patient exists in reception AND dialysis on the target: the copy's (non-empty) map decides."""
    v2_map = {"patient": {"1": 10}}
    v3v3 = {"reception/patient": {"10": 500}, "dialysis/patient": {}}
    out, _, services = compose_id_map(v2_map, v3v3, {})
    assert services == {"patient": "reception"} and out["patient"] == {"1": 500}


def test_plan_updates_what_the_target_has_and_inserts_only_new_records():
    existing = [{"id": 900, "uuid": "T-900", "visit_no": "A"}, {"id": 901, "uuid": "T-901", "visit_no": "B"},
                {"id": 902, "uuid": "stable-3", "visit_no": "C"}]
    payloads = [
        {"id": 1, "uuid": "stable-1", "visit_no": "A", "created_at": "2026-01-01 08:00:00"},   # id map → 900
        {"id": 2, "uuid": "stable-2", "visit_no": "B", "created_at": "2026-01-01 08:00:00"},   # content → 901
        {"id": 3, "uuid": "stable-3", "visit_no": "C", "created_at": "2026-01-01 08:00:00"},   # uuid → 902
        {"id": 4, "uuid": "stable-4", "visit_no": "D", "created_at": "2026-10-16 09:00:00"},   # new → insert
        {"id": 5, "uuid": "stable-5", "visit_no": "E", "created_at": "2026-02-01 09:00:00"},   # old, unmatched
    ]
    todo, n = plan_delta(payloads, existing, {1: 900}, lambda r: (r.get("visit_no"),), "2026-10-16")
    got = {p["id"]: (p["uuid"], tid) for p, tid in todo}
    assert got == {1: ("T-900", 900), 2: ("T-901", 901), 3: ("stable-3", 902), 4: ("stable-4", None)}
    assert n == {"update (id map)": 1, "update (content)": 1, "update (uuid)": 1, "insert": 1,
                 "skip: edited, not found in target": 1}


def test_plan_without_a_content_key_never_guesses():
    todo, n = plan_delta([{"id": 9, "uuid": "u9", "created_at": "2025-01-01"}], [], {}, None, "2026-10-16")
    assert todo == [] and n == {"skip: edited, not found in target": 1}
