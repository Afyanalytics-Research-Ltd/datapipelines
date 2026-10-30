"""v3_org_softdelete: id lists from the DBA's SQL, backup/verify, skip done rows, rollback (fake gateway)."""
import csv
import io
import json
import zipfile

import pytest

import v3_org_softdelete as sd

SQL = """-- EVALUATION service database
-- investigations: 3 rows
INSERT INTO zz_backup_20261019_org168_softdelete_investigations SELECT id, organization_id, deleted_at FROM investigations WHERE organization_id = 168 AND id IN (11,12);
INSERT INTO zz_backup_20261019_org168_softdelete_investigations SELECT id, organization_id, deleted_at FROM investigations WHERE organization_id = 168 AND id IN (13);
-- visits: 1 rows
INSERT INTO zz_backup_20261019_org168_softdelete_visits SELECT id, organization_id, deleted_at FROM visits WHERE organization_id = 168 AND id IN (100);
UPDATE investigations t JOIN zz_backup_20261019_org168_softdelete_investigations b ON b.id = t.id SET t.deleted_at = @ts;
"""
FIN = """INSERT INTO zz_backup_x_patient_accounts SELECT id, organization_id, status FROM patient_accounts WHERE organization_id = 168 AND id IN (7,8);
INSERT INTO zz_backup_x_invoices SELECT id, organization_id, deleted_at FROM invoices WHERE organization_id = 168 AND id IN (5);
"""
ROLLBACK = "UPDATE investigations t JOIN zz_backup_20261019_org168_softdelete_investigations b ON b.id = t.id SET t.deleted_at = b.deleted_at;"


class Resp:
    def __init__(self, status, body):
        self.status_code, self._body = status, body
        self.ok = status < 400
        self.text = json.dumps(body)

    def json(self):
        return self._body


class FakeGateway:
    """Rows per (service, alias); `update` refuses deleted_at like the real gateway; insert by uuid sets it."""

    def __init__(self, org=168):
        self.key, self.organization_id = "org168", org
        self.rows = {}
        self.calls = []

    def add(self, service, alias, **row):
        row.setdefault("organization_id", self.organization_id)
        row.setdefault("uuid", f"u-{alias}-{row['id']}")
        row.setdefault("deleted_at", None)
        self.rows.setdefault((service, alias), {})[row["id"]] = row

    def read_all(self, service, alias, per_page=500, workers=1):
        yield from (dict(r) for r in self.rows.get((service, alias), {}).values())

    def insert(self, service, alias, data, match_on="uuid"):
        self.calls.append(("insert", service, alias, dict(data)))
        for r in self.rows.get((service, alias), {}).values():
            if r["uuid"] == data["uuid"]:
                r.update(data)
                return Resp(200, {"success": True, "data": dict(r)})
        return Resp(422, {"success": False})

    def post(self, service, body, timeout=60):
        self.calls.append(("post", service, body["model"], body))
        assert body["action"] == "update"
        if "deleted_at" in body["data"]:
            return Resp(422, {"ignored": {"deleted_at": "protected"}})
        out = []
        for i in body["ids"]:
            r = self.rows.get((service, body["model"]), {}).get(i)
            if r:
                r.update(body["data"])
                out.append({"id": i, "changed": True})
        return Resp(200, {"success": True, "data": out})


def gateway():
    g = FakeGateway()
    for i in (11, 12, 13, 14):
        g.add("evaluation", "investigations", id=i)
    g.add("evaluation", "visits", id=100)
    g.add("evaluation", "visits", id=101)
    g.add("finance", "patient_account", id=7, status="active")
    g.add("finance", "patient_account", id=8, status="active")
    g.add("finance", "invoice", id=5)
    return g


IDS = {"evaluation.investigations": [11, 12, 13], "evaluation.visits": [100],
       "finance.patient_accounts": [7, 8], "finance.invoices": [5]}


# ── id lists ────────────────────────────────────────────────────────────────
def test_ids_from_sql_reads_step1_lists_only():
    ids, orgs = sd.ids_from_sql({"x/evaluation_softdelete.sql": SQL, "finance_softdelete.sql": FIN,
                                 "evaluation_softdelete_rollback.sql": ROLLBACK + "\n" + SQL.replace("11,12", "999")})
    assert ids == {"evaluation.investigations": [11, 12, 13], "evaluation.visits": [100],
                   "finance.patient_accounts": [7, 8], "finance.invoices": [5]}
    assert orgs == {168}


@pytest.mark.parametrize("form", ["dir", "zip", "json", "json_plain"])
def test_load_ids_every_form(tmp_path, form):
    if form == "dir":
        d = tmp_path / "org168_softdelete"
        d.mkdir()
        (d / "evaluation_softdelete.sql").write_text(SQL, encoding="utf-8")
        (d / "finance_softdelete.sql").write_text(FIN, encoding="utf-8")
        p = tmp_path
    elif form == "zip":
        p = tmp_path / "org168_softdelete (1).zip"
        with zipfile.ZipFile(p, "w") as z:
            z.writestr("org168_softdelete/evaluation_softdelete.sql", SQL)
            z.writestr("org168_softdelete/finance_softdelete.sql", FIN)
    else:
        p = tmp_path / "ids.json"
        doc = {"tables": {k: list(reversed(v)) + v[:1] for k, v in IDS.items()}} if form == "json" else IDS
        p.write_text(json.dumps(doc), encoding="utf-8")
    assert sd.load_ids(p) == IDS


def test_load_ids_rejects_sql_without_lists(tmp_path):
    (tmp_path / "evaluation_softdelete.sql").write_text("-- nothing here", encoding="utf-8")
    with pytest.raises(ValueError):
        sd.load_ids(tmp_path)


def test_order_children_first_visits_last():
    got = sd.order(["evaluation.visits", "evaluation.custom_thing", "finance.invoices", "evaluation.investigations",
                    "finance.patient_accounts"])
    assert got == ["finance.patient_accounts", "evaluation.investigations", "finance.invoices",
                   "evaluation.custom_thing", "evaluation.visits"]


def test_committed_org168_list_matches_the_sql_counts():
    doc = json.load(open("mapping/softdelete/org168_20261019.json", encoding="utf-8"))
    assert doc["organization_id"] == 168
    assert {k: len(v) for k, v in doc["tables"].items()} == {
        "evaluation.investigations": 138783, "evaluation.doctor_notes": 38117, "evaluation.visits": 5039,
        "finance.invoices": 15, "finance.patient_accounts": 958, "inpatient.inp_admissions": 711}


# ── run ─────────────────────────────────────────────────────────────────────
def test_dry_run_backs_up_and_writes_nothing(tmp_path):
    g = gateway()
    rep = sd.softdelete(g, IDS, dry_run=True, deleted_at="2026-10-30 15:40:19", backup_path=tmp_path / "b.csv")
    assert not [c for c in g.calls]
    assert rep["tables"]["evaluation.investigations"] == {"listed": 3, "found": 3, "missing": 0, "already": 0, "todo": 3}
    rows = list(csv.DictReader(open(tmp_path / "b.csv")))
    assert len(rows) == 7
    assert {(r["table"], r["field"], r["value"]) for r in rows if r["table"] == "finance.patient_accounts"} == {
        ("finance.patient_accounts", "status", "active")}


def test_run_soft_deletes_only_listed_rows_and_verifies(tmp_path):
    g = gateway()
    rep = sd.softdelete(g, IDS, dry_run=False, workers=4, deleted_at="2026-10-30 15:40:19", backup_path=tmp_path / "b.csv")
    inv = g.rows[("evaluation", "investigations")]
    assert [i for i, r in inv.items() if r["deleted_at"]] == [11, 12, 13]
    assert inv[14]["deleted_at"] is None                          # not listed: untouched
    assert g.rows[("evaluation", "visits")][101]["deleted_at"] is None
    assert {i: r["status"] for i, r in g.rows[("finance", "patient_account")].items()} == {7: "closed", 8: "closed"}
    for t in IDS:
        assert rep["tables"][t]["failed"] == 0 and rep["tables"][t]["still_not_deleted"] == 0
    # deleted_at goes by insert on uuid (update refuses it); status by update on ids
    assert all(c[0] == "insert" and set(c[3]) == {"uuid", "deleted_at"} for c in g.calls if c[2] != "patient_account")
    # visits after their children
    first_visit = next(n for n, c in enumerate(g.calls) if c[2] == "visits")
    assert all(c[2] == "visits" for c in g.calls[first_visit:])


def test_rerun_skips_done_rows(tmp_path):
    g = gateway()
    g.rows[("evaluation", "investigations")][11]["deleted_at"] = "2026-10-30T12:40:19Z"
    g.rows[("finance", "patient_account")][7]["status"] = "closed"
    rep = sd.softdelete(g, IDS, dry_run=False, deleted_at="2026-10-30 16:00:00", backup_path=tmp_path / "b.csv")
    assert rep["tables"]["evaluation.investigations"]["already"] == 1
    assert rep["tables"]["evaluation.investigations"]["written"] == 2
    assert g.rows[("evaluation", "investigations")][11]["deleted_at"] == "2026-10-30T12:40:19Z"   # kept, not re-stamped
    assert [c[3]["ids"] for c in g.calls if c[0] == "post"] == [[8]]


def test_listed_id_of_another_org_stops_that_table_only(tmp_path):
    g = gateway()
    del g.rows[("evaluation", "investigations")][13]               # gateway reads are tenant-scoped: not ours
    rep = sd.softdelete(g, IDS, dry_run=False, deleted_at="2026-10-30 15:40:19", backup_path=tmp_path / "b.csv")
    assert "error" in rep["tables"]["evaluation.investigations"]
    assert all(r["deleted_at"] is None for r in g.rows[("evaluation", "investigations")].values())
    assert g.rows[("finance", "invoice")][5]["deleted_at"]            # other tables still done


def test_write_failures_are_reported(tmp_path, monkeypatch):
    g = gateway()
    monkeypatch.setattr(sd.time, "sleep", lambda s: None)
    real = g.insert
    g.insert = lambda s, a, d, match_on="uuid": Resp(500, {"x": 1}) if d["uuid"] == "u-investigations-12" else real(s, a, d, match_on)
    rep = sd.softdelete(g, {"evaluation.investigations": [11, 12]}, dry_run=False, deleted_at="x", backup_path=tmp_path / "b.csv")
    t = rep["tables"]["evaluation.investigations"]
    assert (t["written"], t["failed"], t["still_not_deleted"]) == (1, 1, 1)
    assert t["failed_sample"][0][0] == 12


def test_rollback_restores_backup(tmp_path):
    g = gateway()
    g.rows[("evaluation", "investigations")][12]["deleted_at"] = "2026-01-01T00:00:00Z"   # already deleted before
    sd.softdelete(g, IDS, dry_run=False, deleted_at="2026-10-30 15:40:19", backup_path=tmp_path / "b.csv")
    sd.rollback(g, tmp_path / "b.csv", dry_run=False)
    inv = g.rows[("evaluation", "investigations")]
    assert inv[11]["deleted_at"] is None and inv[13]["deleted_at"] is None
    assert inv[12]["deleted_at"] == "2026-01-01T00:00:00Z"          # its own earlier deletion kept
    assert {r["status"] for r in g.rows[("finance", "patient_account")].values()} == {"active"}
    assert g.rows[("evaluation", "visits")][100]["deleted_at"] is None
