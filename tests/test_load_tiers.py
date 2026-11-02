"""
load_tiers.py: links from column names / mappings / deps, tiers from links
(loops, soft vs hard), and running tier by tier. No network, no credentials.
"""
from __future__ import annotations

import json
import threading

import pytest

import load_tiers as lt
from load_tiers import Link


def plan(tables: dict[str, list[str]], **kw) -> lt.TierPlan:
    return lt.compute_tiers(tables, lt.links_from_columns(tables, **kw))


class TestResolveColumn:
    TABLES = {"patients", "visits", "users", "inp_admissions", "inp_wards", "gl_accounts", "categories",
              "invoice_items", "inv_sale_items"}

    @pytest.mark.parametrize("col, parent", [
        ("patient_id", "patients"),             # plural
        ("visit_id", "visits"),
        ("admission_id", "inp_admissions"),     # module prefix
        ("preferred_ward_id", "inp_wards"),     # leading word dropped
        ("parent_account_id", "gl_accounts"),   # parent_account → account → gl_accounts
        ("gl_account_id", "gl_accounts"),
        ("category_id", "categories"),          # y → ies
        ("created_by", "users"),
        ("doctor_id", "users"),                 # a user role, not a doctors table
        ("item_id", None),                      # invoice_items or inv_sale_items: ambiguous
        ("facility_id", None),                  # set by the loader for the target instance
        ("status", None),
        ("id", None),
    ])
    def test_naming(self, col, parent):
        assert lt.resolve_column("t", col, self.TABLES) == parent

    def test_no_users_table(self):
        assert lt.resolve_column("t", "created_by", {"patients"}) is None


class TestLinksFromColumns:
    def test_links_soft_and_unresolved(self):
        report = lt.links_from_columns({
            "patients": ["id", "name", "created_by", "facility_id"],
            "visits": ["id", "patient_id", "batch_id"],
            "users": ["id"],
        })
        assert report.links == [Link("patients", "created_by", "users", hard=False),
                                Link("visits", "patient_id", "patients", hard=True)]
        assert report.unresolved == {"visits": ["batch_id"]}

    def test_self_link_ignored(self):
        report = lt.links_from_columns({"departments": ["parent_department_id"]})
        assert report.links == []

    def test_overrides(self):
        tables = {"core_departments": ["parent_id"], "invoice_items": ["item_id", "invoice_id"],
                  "invoices": [], "inv_products": [], "notes": ["system_id"]}
        report = lt.links_from_columns(tables, overrides={
            "invoice_items": {"item_id": "inv_products"},
            "*": {"system_id": None},
        })
        assert Link("invoice_items", "item_id", "inv_products") in report.links
        assert report.unresolved == {}          # parent_id: rows of core_departments itself, not unknown

    def test_custom_resolver(self):
        rules = {"patient": "patients"}
        report = lt.links_from_columns({"visits": ["patient"], "patients": []},
                                       resolve=lambda t, c, names: rules.get(c))
        assert report.links == [Link("visits", "patient", "patients")]

    def test_links_from_deps(self):
        assert lt.links_from_deps({"visits": {"patients", "visits"}}) == [Link("visits", "", "patients")]


class TestComputeTiers:
    def test_chain_and_parallel(self):
        p = plan({"users": [], "patients": ["created_by"], "visits": ["patient_id"],
                  "vitals": ["visit_id", "patient_id"], "wards": []})
        assert p.groups() == [(1, ["users", "wards"]), (2, ["patients"]), (3, ["visits"]), (4, ["vitals"])]
        assert p.check() == []

    def test_longest_chain_wins(self):
        p = plan({"a": [], "b": ["a_id"], "c": ["a_id", "b_id"]})
        assert p.tiers == {"a": 1, "b": 2, "c": 3}

    def test_soft_loop_is_deferred(self):
        # users.department_id → departments (hard); departments.created_by → users (soft)
        p = plan({"users": ["department_id"], "departments": ["created_by"]})
        assert p.tiers == {"departments": 1, "users": 2}
        assert p.deferred == [Link("departments", "created_by", "users", hard=False)]
        assert p.check() == []

    def test_soft_link_outside_a_loop_still_orders(self):
        p = plan({"users": [], "patients": ["created_by"]})
        assert p.tiers == {"users": 1, "patients": 2} and p.deferred == []

    def test_hard_loop_raises(self):
        links = [Link("a", "b_id", "b"), Link("b", "a_id", "a")]
        with pytest.raises(lt.TierCycleError, match="a.b_id → b"):
            lt.compute_tiers(["a", "b"], links)

    def test_links_outside_the_load_are_ignored(self):
        p = lt.compute_tiers(["visits"], [Link("visits", "patient_id", "patients")])
        assert p.tiers == {"visits": 1}

    def test_max_tier_folds(self):
        p = plan({"a": [], "b": ["a_id"], "c": ["b_id"], "d": ["c_id"]}, )
        assert lt.compute_tiers(p.tiers, p.links, max_tier=2).tiers == {"a": 1, "b": 2, "c": 2, "d": 2}

    def test_deep_chain_no_recursion_limit(self):
        n = 3000
        links = [Link(f"t{i}", "", f"t{i - 1}") for i in range(1, n)]
        assert lt.compute_tiers([f"t{i}" for i in range(n)], links).tiers[f"t{n - 1}"] == n

    def test_describe(self):
        p = plan({"users": ["department_id"], "departments": ["created_by"], "visits": ["thing_id"]})
        text = p.describe()
        assert "tier 1: departments, visits" in text
        assert "departments.created_by → users" in text
        assert "visits: thing_id" in text


class TestMappings:
    def test_wema_style_mappings(self, tmp_path):
        doc = {
            "connection": {"id": 1},
            "tables": [{"v3 model": "App\\Models\\Patient", "v3 table": "patients", "source table": "p"},
                       {"v3 model": "App\\Models\\Visit", "v3 table": "visits", "source table": "v"},
                       {"v3 model": "App\\Models\\Vital", "v3 table": "vitals", "source table": "vt"}],
            "fields": [
                {"v3_table": "visits", "model": "App\\Models\\Visit", "column": "patient_id", "field": "patient"},
                {"v3_table": "vitals", "model": "App\\Models\\Vital", "column": "visit", "field": "visit_id"},
                # fields for a table that isn't loaded don't count
                {"v3_table": "invoices", "model": "App\\Models\\Invoice", "column": "pid", "field": "patient_id"},
            ],
        }
        path = tmp_path / "m.json"
        path.write_text(json.dumps(doc))
        p = lt.plan_from_mappings(path)
        assert p.tiers == {"patients": 1, "visits": 2, "vitals": 3}

    @pytest.mark.parametrize("name", ["wema_medical_data_mappings.json", "silverwood_data_mappings.json"])
    def test_real_mappings_have_a_sound_order(self, name):
        p = lt.plan_from_mappings(lt.Path(__file__).resolve().parent.parent / "dags" / "jsons" / name)
        assert p.check() == []
        assert p.tier_of("patients") < p.tier_of("visits") < p.tier_of("vitals")


class TestNaming:
    @pytest.mark.parametrize("raw, snake", [("PatientNo", "patient_no"), ("OPNumber", "op_number"),
                                            ("fk_patient", "patient_id"), ("visitID", "visit_id")])
    def test_snake(self, raw, snake):
        assert lt.snake(raw) == snake

    @pytest.mark.parametrize("table, col, parent", [
        ("insurance_schemes", "company_id", "insurance_companies"),   # this table's context
        ("samples", "type_id", "sample_types"),
        ("visit_consultants", "consultant_id", "users"),              # a role, not itself
        ("wards", "nurse_in_charge_id", "users"),
        ("store_orders", "supplying_store_id", "stores"),             # -ing qualifier
        ("investigations", "result_detail_id", "investigation_result_details"),
        ("vitals", "temperature_location_id", None),                  # never guess by dropping real words
        ("visits", "patient", None),                                  # bare names: left to the data
        ("visits", "VisitPatientID", "patients"),
    ])
    def test_rules(self, table, col, parent):
        tables = {"insurance_schemes", "insurance_companies", "samples", "sample_types", "visit_consultants",
                  "users", "wards", "store_orders", "stores", "investigations", "investigation_result_details",
                  "vitals", "treatment_locations", "visits", "patients"}
        assert lt.resolve_column(table, col, tables) == parent

    def test_accuracy_on_real_v3_models(self):
        """Against the team's own link rules (v3_to_v3_links) on 108 real V3
        models: most links found by name, and none invented."""
        import v3_to_v3_links as L
        cols = json.loads((lt.Path(__file__).parent / "fixtures" / "v3v3_org4_columns.json").read_text())
        alias = {k.split("/")[1]: k for k in cols}
        names = lt._Names(set(alias))
        agree = invented = known = 0
        for model, cs in cols.items():
            for c in cs:
                truth = L.link_target(model, c)
                if truth == L.DROP:
                    continue
                truth = truth if truth in cols else None
                guess = lt.resolve_column(model.split("/")[1], c, names)
                guess = alias.get(guess) if guess else None
                known += truth is not None
                agree += truth is not None and guess == truth
                invented += truth is None and guess is not None
        assert known > 250
        assert agree / known >= 0.88
        assert invented == 0


def _ids(prefix: str, n: int) -> list[str]:
    return [f"{prefix}{i:04d}" for i in range(1, n + 1)]


class TestLinksFromData:
    """A facility whose schema follows no convention."""

    @pytest.fixture
    def rows(self):
        patients = [{"PatNo": p, "Names": f"P {p}"} for p in _ids("OP/", 40)]
        staff = [{"StaffKey": i, "Name": f"S{i}"} for i in range(1, 11)]
        visits = [{"VisitRef": i, "pat_ref": patients[i % 40]["PatNo"], "seen_by_staff": 1 + i % 10,
                   "qty": 1 + i % 5} for i in range(1, 61)]
        drugs = [{"DrugID": i, "Drug": f"D{i}"} for i in range(1, 200)]
        return {"patients": patients, "staff": staff, "visits": visits, "drugs": drugs}

    def test_text_codes_link_without_a_helpful_name(self, rows):
        report = lt.links_from_data(rows)
        assert Link("visits", "pat_ref", "patients") in report.links

    def test_numbers_need_the_name_to_agree(self, rows):
        report = lt.links_from_data(rows)
        by_col = {l.column: l for l in report.links}
        assert by_col["seen_by_staff"].parent == "staff"        # numbers, but "staff" names the table
        assert "qty" not in by_col                                # 1–5 sits inside every id range
        assert any(r.column == "qty" for r in report.review)     # … so a person sees it instead

    def test_name_confirmed_or_contradicted(self, rows):
        rows["visits"] = [{**v, "patient_id": v["pat_ref"], "drug_id": "X-unknown"} for v in rows["visits"]]
        named = [Link("visits", "patient_id", "patients"), Link("visits", "drug_id", "drugs")]
        report = lt.links_from_data(rows, named=named)
        by_col = {l.column: l for l in report.links}
        assert by_col["patient_id"].evidence == "name+data"
        assert any(r.column == "drug_id" and r.parent == "drugs" for r in report.review)

    def test_name_stands_when_parent_rows_absent(self, rows):
        report = lt.links_from_data({"visits": rows["visits"]}, named=[Link("visits", "patient_id", "patients")])
        assert report.review == [] or all(r.column != "patient_id" for r in report.review)

    def test_people_table_under_any_name(self):
        rows = {"staff": [{"StaffKey": i} for i in range(1, 11)],
                "vitals": [{"VitalID": i, "recorded_by": 1 + i % 10} for i in range(1, 60)]}
        report = lt.links_from_data(rows)
        assert report.links == [Link("vitals", "recorded_by", "staff", hard=False)]   # optional: who recorded it
        assert report.review == []                     # StaffKey is staff's own id, not a link to vitals

    def test_numbers_and_text_never_match(self):
        rows = {"a": [{"code": str(i)} for i in range(1, 9)], "b": [{"x": i} for i in range(1, 9)]}
        assert lt.links_from_data(rows).links == []

    def test_12_and_12_0_are_the_same_key(self):
        rows = {"patients": [{"id": i} for i in range(1, 9)],
                "visits": [{"patient": float(i)} for i in range(1, 9)]}
        assert Link("visits", "patient", "patients") in lt.links_from_data(rows).links


class TestDiscoverLinks:
    def test_unconventional_facility_end_to_end(self):
        """Names find nothing here; the data gives a sound load order."""
        patients = [{"PatNo": p} for p in _ids("OP/", 20)]
        visits = [{"VisitNo": v, "PatNo": patients[i % 20]["PatNo"]} for i, v in enumerate(_ids("V/", 30))]
        vitals = [{"VitalNo": i, "VisitNo": visits[i % 30]["VisitNo"]} for i in range(1, 50)]
        report = lt.discover_links(rows={"patients": patients, "visits": visits, "vitals": vitals})
        p = lt.compute_tiers(["patients", "visits", "vitals"], report)
        assert p.tiers == {"patients": 1, "visits": 2, "vitals": 3}
        assert {l.evidence for l in p.links} == {"data"}

    def test_overrides_and_declared_win(self):
        rows = {"patients": [{"id": i} for i in range(1, 9)],
                "visits": [{"id": i, "patient_id": i} for i in range(1, 9)]}
        report = lt.discover_links(rows=rows, overrides={"visits": {"patient_id": None}})
        assert report.links == []
        report = lt.discover_links(rows=rows, declared=[Link("visits", "patient_id", "patients")])
        assert report.links[0].evidence == "declared"

    def test_data_resolves_what_names_left_unresolved(self):
        rows = {"patients": [{"id": i, "op_no": f"OP{i}"} for i in range(1, 9)],
                "visits": [{"id": i, "op_no_id": f"OP{i}"} for i in range(1, 9)]}
        report = lt.discover_links(rows=rows)
        assert Link("visits", "op_no_id", "patients") in report.links
        assert report.unresolved == {}


class TestRunInTiers:
    def test_order_and_skip_children_of_failures(self):
        p = plan({"users": [], "patients": ["created_by"], "visits": ["patient_id"],
                  "vitals": ["visit_id"], "wards": []})
        ran = []

        def load(t):
            ran.append(t)
            if t == "patients":
                raise RuntimeError("API down")

        status = lt.run_in_tiers(p, load)
        assert ran == ["users", "wards", "patients"]
        assert status == {"users": "done", "wards": "done", "patients": "failed",
                          "visits": "skipped", "vitals": "skipped"}

    def test_soft_parent_failure_does_not_block(self):
        p = plan({"users": [], "patients": ["created_by"]})
        status = lt.run_in_tiers(p, lambda t: t != "users")
        assert status == {"users": "failed", "patients": "done"}

    def test_parallel_within_a_tier_only(self):
        p = plan({"a": [], "b": [], "c": ["a_id", "b_id"]})
        done, lock, seen = set(), threading.Lock(), {}

        def load(t):
            with lock:
                seen[t] = set(done)
                done.add(t)

        status = lt.run_in_tiers(p, load, workers=4)
        assert set(status.values()) == {"done"}
        assert seen["c"] == {"a", "b"}

    def test_subset(self):
        p = plan({"a": [], "b": ["a_id"]})
        assert lt.run_in_tiers(p, lambda t: None, tables=["b"]) == {"b": "done"}


class TestBothDirections:
    """Down the tiers to load, up the tiers to delete / roll back."""

    @pytest.fixture
    def p(self):
        return plan({"users": [], "wards": [], "patients": ["created_by"], "visits": ["patient_id"],
                     "vitals": ["visit_id", "patient_id"], "admissions": ["visit_id", "ward_id"]})

    def test_reverse_order_is_children_first(self, p):
        assert p.order() == ["users", "wards", "patients", "visits", "admissions", "vitals"]
        assert p.order(reverse=True) == ["admissions", "vitals", "visits", "patients", "users", "wards"]
        assert [n for n, _ in p.groups(reverse=True)] == [4, 3, 2, 1]

    def test_parents_and_children(self, p):
        assert p.parents("vitals") == {"visits", "patients"}
        assert p.children("visits") == {"vitals", "admissions"}
        assert p.children("users") == {"patients"}
        assert p.children("users", hard_only=True) == set()     # created_by is optional

    def test_with_parents_is_what_a_load_needs(self, p):
        assert p.with_parents(["vitals"]) == {"vitals", "visits", "patients", "users"}
        assert p.with_parents(["vitals"], hard_only=True) == {"vitals", "visits", "patients"}

    def test_with_children_is_what_a_change_touches(self, p):
        assert p.with_children(["patients"]) == {"patients", "visits", "vitals", "admissions"}
        assert p.with_children(["wards"]) == {"wards", "admissions"}

    def test_reverse_run_deletes_children_first(self, p):
        ran = []
        status = lt.run_in_tiers(p, ran.append, reverse=True)
        assert set(status.values()) == {"done"}
        assert ran.index("vitals") < ran.index("visits") < ran.index("patients")
        assert ran.index("admissions") < ran.index("wards")

    def test_reverse_run_keeps_parents_whose_children_remain(self, p):
        """visits can't be deleted → patients (still referenced) must stay;
        wards only waits on admissions, which went through."""
        def delete(t):
            if t == "visits":
                raise RuntimeError("FK: still referenced")
        status = lt.run_in_tiers(p, delete, reverse=True)
        assert status["visits"] == "failed"
        assert status["patients"] == "skipped"
        assert status["wards"] == "done"
        assert status["users"] == "done"          # only an optional link from patients

    def test_both_directions_on_real_mappings(self):
        p = lt.plan_from_mappings(lt.Path(__file__).resolve().parent.parent / "dags" / "jsons"
                                  / "wema_medical_data_mappings.json")
        fwd, back = p.order(), p.order(reverse=True)
        assert fwd.index("patients") < fwd.index("visits") < fwd.index("inp_notes")
        assert back.index("inp_notes") < back.index("visits") < back.index("patients")
        assert "inp_admissions" in p.with_children(["visits"])
        assert {"visits", "patients"} <= p.with_parents(["inp_notes"])
