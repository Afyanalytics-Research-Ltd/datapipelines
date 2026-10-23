"""
flatten_jsons_schemas.flatten_table — a CLEAN object that already exists as a
TABLE (KISUMU_CLEAN.USERS & co, left from May 2026) used to fail the flatten
with "Object 'USERS' already exists as TABLE". Now:

  auto   a table is refreshed in place from RAW; anything else is a view
  view   always a view (a leftover table is replaced)
  table  always a table, refreshed every run

and a table is cloned to <CLEAN>_BACKUP before it is replaced.
"""
from __future__ import annotations

import re

import pytest
from freezegun import freeze_time

import flatten_jsons_schemas as fl

FIELDS = [("id", "id", "INTEGER"), ("name", "name", "VARCHAR")]


class Cur:
    """Records SQL; answers the INFORMATION_SCHEMA type lookup."""

    def __init__(self, existing: str | None):
        self.existing, self.sql, self._row = existing, [], None

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self.sql.append(s)
        if "INFORMATION_SCHEMA.TABLES" in s:
            self._row = None if self.existing is None else (
                ("VIEW",) if self.existing == "view" else ("BASE TABLE",))
        return self

    def fetchone(self):
        return self._row


def creates(cur):
    return [re.match(r"CREATE OR REPLACE (VIEW|TABLE) (\S+)", s).groups()
            for s in cur.sql if re.match(r"CREATE OR REPLACE (VIEW|TABLE) HOSPITALS\.K_CLEAN\.users AS", s)]


@freeze_time("2026-10-12")
class TestFlattenTable:
    def run(self, existing, mode):
        cur = Cur(existing)
        out = fl.flatten_table(cur, "HOSPITALS.K_RAW", "HOSPITALS.K_CLEAN", "users", FIELDS, materialize=mode)
        return cur, out

    def test_auto_refreshes_an_existing_table_in_place(self):
        cur, out = self.run("table", "auto")
        assert out == {"object": "table", "was": "table", "backup": "HOSPITALS.K_CLEAN_BACKUP.USERS__20261012"}
        assert "CREATE SCHEMA IF NOT EXISTS HOSPITALS.K_CLEAN_BACKUP" in cur.sql
        assert ("CREATE TABLE IF NOT EXISTS HOSPITALS.K_CLEAN_BACKUP.USERS__20261012 "
                "CLONE HOSPITALS.K_CLEAN.users") in cur.sql
        assert creates(cur) == [("TABLE", "HOSPITALS.K_CLEAN.users")]
        assert not any(s.startswith("DROP") for s in cur.sql)
        # the backup comes before the replace
        assert cur.sql.index(next(s for s in cur.sql if "CLONE" in s)) < \
            cur.sql.index(next(s for s in cur.sql if s.startswith("CREATE OR REPLACE TABLE HOSPITALS.K_CLEAN.users")))

    @pytest.mark.parametrize("existing", [None, "view"])
    def test_auto_otherwise_a_view_no_backup(self, existing):
        cur, out = self.run(existing, "auto")
        assert out == {"object": "view", "was": existing, "backup": None}
        assert creates(cur) == [("VIEW", "HOSPITALS.K_CLEAN.users")]
        assert not any("CLONE" in s or s.startswith("DROP") for s in cur.sql)

    def test_view_replaces_a_leftover_table_after_backup(self):
        cur, out = self.run("table", "view")
        assert out["object"] == "view" and out["backup"]
        assert "DROP TABLE HOSPITALS.K_CLEAN.users" in cur.sql
        assert creates(cur) == [("VIEW", "HOSPITALS.K_CLEAN.users")]

    def test_table_mode_turns_a_view_into_a_table(self):
        cur, out = self.run("view", "table")
        assert out == {"object": "table", "was": "view", "backup": None}
        assert "DROP VIEW HOSPITALS.K_CLEAN.users" in cur.sql
        assert creates(cur) == [("TABLE", "HOSPITALS.K_CLEAN.users")]

    def test_type_lookup_uses_the_schemas_database(self):
        cur, _ = self.run(None, "auto")
        assert cur.sql[0].startswith("SELECT table_type FROM HOSPITALS.INFORMATION_SCHEMA.TABLES")

    def test_bad_mode(self):
        with pytest.raises(ValueError):
            fl.flatten_table(Cur(None), "R", "C", "t", FIELDS, materialize="sometimes")


def test_build_flatten_sql_default_is_still_a_view():
    sql = " ".join(fl.build_flatten_sql("R", "C", "users", FIELDS).split())
    assert sql.startswith("CREATE OR REPLACE VIEW C.users AS")
    assert 'TRY_CAST(record:"id"::STRING AS' in sql


def test_same_query_for_table_and_view():
    view = fl.build_flatten_sql("R", "C", "users", FIELDS, materialize="view")
    table = fl.build_flatten_sql("R", "C", "users", FIELDS, materialize="table")
    assert view.replace("CREATE OR REPLACE VIEW", "X") == table.replace("CREATE OR REPLACE TABLE", "X")
