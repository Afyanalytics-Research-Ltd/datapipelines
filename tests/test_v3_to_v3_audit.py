"""v3_to_v3_audit end to end against the in-memory gateways: a copy with a row
missing, a field changed and a model the target doesn't have."""
from __future__ import annotations

import csv
import io

import v3_to_v3_audit as audit
import v3_to_v3_migration as vv
from tests.test_v3_to_v3_migration import _pair, _run


def _rows(path):
    body = path.read_text().split("\n\n", 1)[0]
    return {r["model"]: r for r in csv.DictReader(io.StringIO(body))}


def test_audit_reloads_patches_and_explains(tmp_path, monkeypatch):
    src, dst = _pair()
    _run(src, dst, tmp_path)                                  # the earlier copy
    dst.rows["reception/patient"].pop()                       # one row lost
    dst.rows["evaluation/visits"][0]["status"] = "tampered"   # one field drifted
    # the target's build has no investigation results model
    dst.meta = {m: x for m, x in dst.meta.items() if m != "evaluation/investigation_results"}
    real_state = vv.State
    monkeypatch.setattr(vv, "State", lambda s, t: real_state(s, t, root=tmp_path))
    monkeypatch.setattr(vv, "clients", lambda s, t: (src, dst))
    out = tmp_path / "audit.csv"
    assert audit.main(["--source", "src", "--target", "dst", "--out", str(out), "--workers", "2"]) == 0
    rows = _rows(out)

    p = rows["reception/patient"]
    assert p["posted_now"] == "1" and p["still_missing"] == "0" and "1 missing row(s) reloaded now" in p["why"]
    v = rows["evaluation/visits"]
    # the tampered status, and v2's patient link: p2 was reloaded under a new
    # target id, so the visit is re-pointed at it
    assert v["patched"] == "2" and v["still_mismatched"] == "0"
    assert "status ×1" in v["why"] and "patient ×1" in v["why"]
    pat = {r["uuid"]: r["id"] for r in dst.rows["reception/patient"]}
    assert next(r for r in dst.rows["evaluation/visits"] if r["uuid"] == "v2")["patient"] == pat["p2"]
    assert rows["core/users"]["why"].startswith("mirrors the source")
    r = rows["evaluation/investigation_results"]
    assert "REFUSED" in r["why"] and "not registered" in r["why"]
    assert "not copied by design" in rows["core/organizations"]["why"]
    assert dst.rows["evaluation/visits"][0]["status"] == "open"
    assert "summary" in out.read_text()


def test_audit_read_only_changes_nothing(tmp_path, monkeypatch):
    src, dst = _pair()
    _run(src, dst, tmp_path / "elsewhere")                    # copied by another machine: no maps here
    dst.rows["reception/patient"].pop()
    before = {m: [dict(r) for r in rs] for m, rs in dst.rows.items()}
    real_state = vv.State
    monkeypatch.setattr(vv, "State", lambda s, t: real_state(s, t, root=tmp_path))
    monkeypatch.setattr(vv, "clients", lambda s, t: (src, dst))
    out = tmp_path / "ro.csv"
    audit.main(["--source", "src", "--target", "dst", "--out", str(out), "--no-reload", "--no-sync"])
    assert {m: [dict(r) for r in rs] for m, rs in dst.rows.items()} == before
    rows = _rows(out)
    p = rows["reception/patient"]
    assert p["still_missing"] == "1" and "still not on the target" in p["why"]
    # links compared in target ids, rebuilt by uuid: no false differences
    assert rows["evaluation/investigations"]["why"].startswith("mirrors the source"), rows["evaluation/investigations"]
    # …except the one real difference: v2 points at a patient no longer on the target
    v = rows["evaluation/visits"]
    assert v["still_mismatched"] == "1" and "patient on 1 row(s)" in v["why"]
