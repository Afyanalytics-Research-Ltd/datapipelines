
import os
import pytest

import v2_to_v3_api_migration as pipeline



def test_new_facility_can_be_configured_dynamically(monkeypatch):
    facility = "TEST_FACILITY"

    # These are the environment-variable names the actual code expects.
    monkeypatch.setenv(
        "FACILITY_TEST_FACILITY_BASE_URL",
        "https://test-v2.example.com",
    )
    monkeypatch.setenv(
        "FACILITY_TEST_FACILITY_DB",
        "test_database",
    )

    config = pipeline.v2_facility_config(facility)

    assert config["base_url"] == "https://test-v2.example.com"
    assert config["db"] == "test_database"


def test_new_facility_has_v3_configuration(monkeypatch):
    facility = "TEST_FACILITY"

    # The actual function expects numeric IDs using the AFYA_ prefix.
    monkeypatch.setenv("AFYA_TEST_FACILITY_ORGANIZATION_ID", "123")
    monkeypatch.setenv("AFYA_TEST_FACILITY_FACILITY_ID", "456")

    config = pipeline.facility_v3_config(facility)

    assert config["organization_id"] == 123
    assert config["facility_id"] == 456




def test_state_paths_are_isolated_between_pipelines(tmp_path):
    """Each pipeline gets its own state directory."""

    from migration_state import StatePaths

    first = StatePaths.create(
        facility="facility_a",
        pipeline="pipeline_one",
        base=tmp_path,
        env="test",
    )

    second = StatePaths.create(
        facility="facility_a",
        pipeline="pipeline_two",
        base=tmp_path,
        env="test",
    )

    assert first.root != second.root
    assert first.file("progress") != second.file("progress")
    assert first.lock != second.lock
    assert first.root.exists()
    assert second.root.exists()



def test_state_paths_are_isolated_between_facilities(tmp_path):
    from migration_state import StatePaths

    first = StatePaths.create(
        facility="facility_a",
        pipeline="v2_to_v3",
        base=tmp_path,
        env="test",
    )

    second = StatePaths.create(
        facility="facility_b",
        pipeline="v2_to_v3",
        base=tmp_path,
        env="test",
    )

    assert first.root != second.root
    assert first.file("progress") != second.file("progress")
    assert first.lock != second.lock



def test_unknown_namespace_is_handled_safely():
    """An unknown source namespace should not silently map to an unrelated target."""

    unknown_namespace = "__TEST_UNKNOWN_NAMESPACE__"

    namespace_map = getattr(pipeline, "NAMESPACE_MAP", {})

    assert unknown_namespace not in namespace_map
