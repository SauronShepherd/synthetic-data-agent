from scripts.preflight_bundle import validate_output_prefixes


def test_controlled_outputs_must_use_target_catalog() -> None:
    values = {
        "sda_manifest_table": "sda_staging.profiles.run_manifests",
        "sda_pattern_registry_table": "sda_dev.profiles.pattern_registry",
    }
    assert validate_output_prefixes("staging", values) == ["sda_pattern_registry_table"]


def test_dev_output_catalog_is_explicitly_allowed() -> None:
    assert (
        validate_output_prefixes("dev", {"sda_manifest_table": "sda_dev.profiles.run_manifests"})
        == []
    )
