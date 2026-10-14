"""export_missing_records: audit parsing, CSV fields, load / undo SQL, duplicate guard."""
import export_missing_records as emr

AUDIT = """tier,table,target,v2_rows,v3_rows,difference,paired,linked,stale,unpaired,why
before,evaluation_age_groups,evaluation/age_groups,123,93,-30,,,,,
before,settings_insurance,core/insurance_companies,64,8,-56,,,,,
before,settings_insurance,finance/insurance_company,64,64,0,,,,,
before,finance_banks,finance/bank,6,61,55,,,,,
journey,reception_patient_consents,-,,,,,,,,"not on this v2 site"
before,settings_scheme_exclusions,core/scheme_exclusions,2929,0,,,,,,"v3 takes only the rows matching excluded - counts shown, not compared"
journey,inpatient_notes,inpatient/note,172662,124568,-48094,,,,,

tier,tables,v2_rows,v3_rows,v3_short,paired,unpaired,paired_pct
all,165,3059467,2104676,1027239,0,0,
"""


def test_parse_audit_keeps_only_tables_short_in_v3():
    jobs = {j["v2_table"]: j["gap"] for j in emr.parse_audit(AUDIT)}
    assert jobs == {"evaluation_age_groups": 30, "settings_insurance": 56, "inpatient_notes": 48094}


def test_parse_audit_handles_windows_line_endings():
    assert len(emr.parse_audit(AUDIT.replace("\n", "\r\n"))) == 3


def test_csv_cell_for_mysql_load_data():
    assert emr.csv_cell(None) == "NULL"
    assert emr.csv_cell(True) == '"1"'
    assert emr.csv_cell('say "hi", ok') == '"say ""hi"", ok"'
    assert emr.csv_cell({"a": 1}) == '"{""a"": 1}"'


def test_load_sql_quotes_keyword_columns_and_commits_itself():
    sql = emr.load_sql("evaluation_age_groups", "evaluation", ["group", "name", "organization_id"], 33, 4, None, "now")
    assert "(`group`, `name`, `organization_id`)" in sql            # GROUP is a MySQL keyword
    assert "INTO TABLE `evaluation_age_groups`" in sql
    assert sql.index("SHOW WARNINGS") < sql.index("COMMIT;")         # batch mode: an open transaction is lost


def test_undo_deletes_exactly_the_loaded_uuids_in_chunks():
    sql = emr.undo_sql("inp_notes", [f"u{i}" for i in range(2500)], 4)
    assert sql.count("DELETE FROM `inp_notes` WHERE organization_id = 4 AND uuid IN (") == 3
    assert "'u2499'" in sql and sql.strip().endswith("organization_id = 4;")


def test_duplicate_guard():
    assert not emr.is_suspect(30, 30)
    assert not emr.is_suspect(36, 30)          # 5 % + 5 rows of slack
    assert emr.is_suspect(182, 7)              # lab test units: V3 has them under other uuids
    assert not emr.is_suspect(0, None) and not emr.is_suspect(10, None)
