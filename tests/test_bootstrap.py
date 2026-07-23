from __future__ import annotations

import pytest

from sda.bootstrap import (
    BootstrapParameters,
    build_bootstrap_summary,
    load_bootstrap_parameters,
    parse_bool,
    require_identifier,
)


def test_require_identifier_accepts_simple_uc_names() -> None:
    assert require_identifier("sales_2026", "catalog_name") == "sales_2026"
    assert require_identifier("  sample_source  ", "schema_name") == "sample_source"


def test_require_identifier_rejects_unsafe_names() -> None:
    for value in ("", "main.sales", "bad-name", "1_catalog", "schema;drop"):
        with pytest.raises(ValueError):
            require_identifier(value, "catalog_name")


def test_parse_bool_accepts_common_values() -> None:
    assert parse_bool("true", "flag") is True
    assert parse_bool("YES", "flag") is True
    assert parse_bool("0", "flag") is False
    assert parse_bool("off", "flag") is False


def test_parse_bool_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="Invalid flag"):
        parse_bool("sometimes", "flag")


def test_load_bootstrap_parameters() -> None:
    values = {
        "catalog_name": "sda_dev",
        "schema_name": "sandbox",
        "source_schema_name": "sample_source",
        "target_environment": "dev",
        "auto_create_uc_objects": "true",
        "seed_sample_data": "true",
        "allow_catalog_fallback": "true",
    }
    params = load_bootstrap_parameters(values.__getitem__)
    assert params == BootstrapParameters(
        catalog_name="sda_dev",
        output_schema_name="sandbox",
        source_schema_name="sample_source",
        target_environment="dev",
        auto_create_uc_objects=True,
        seed_sample_data=True,
        allow_catalog_fallback=True,
    )


def test_load_bootstrap_parameters_uses_defaults_for_optional_widgets() -> None:
    values = {
        "catalog_name": "sda_dev",
        "schema_name": "sandbox",
        "source_schema_name": "sample_source",
    }
    params = load_bootstrap_parameters(values.__getitem__)
    assert params.target_environment == "dev"
    assert params.auto_create_uc_objects is True
    assert params.seed_sample_data is True
    assert params.allow_catalog_fallback is True


def test_bootstrap_parameters_with_scope() -> None:
    params = BootstrapParameters(
        catalog_name="sda_dev",
        output_schema_name="sandbox",
        source_schema_name="sample_source",
    )
    assert params.with_scope(
        catalog_name="main",
        source_schema_name="default",
        output_schema_name="default",
    ) == BootstrapParameters(
        catalog_name="main",
        output_schema_name="default",
        source_schema_name="default",
    )


def test_build_bootstrap_summary() -> None:
    params = BootstrapParameters(
        catalog_name="sda_dev",
        output_schema_name="sandbox",
        source_schema_name="sample_source",
        target_environment="dev",
        auto_create_uc_objects=True,
        seed_sample_data=True,
        allow_catalog_fallback=True,
    )
    summary = build_bootstrap_summary(
        parameters=params,
        visible_tables=2,
        visible_columns=8,
        warnings=("created objects",),
    )
    assert summary.as_dict() == {
        "catalog_name": "sda_dev",
        "source_schema_name": "sample_source",
        "output_schema_name": "sandbox",
        "visible_tables": 2,
        "visible_columns": 8,
        "target_environment": "dev",
        "auto_create_uc_objects": True,
        "seed_sample_data": True,
        "warnings": ["created objects"],
    }


def test_build_bootstrap_summary_rejects_negative_counts() -> None:
    params = BootstrapParameters(
        catalog_name="sda_dev",
        output_schema_name="sandbox",
        source_schema_name="sample_source",
    )
    with pytest.raises(ValueError, match="must not be negative"):
        build_bootstrap_summary(
            parameters=params,
            visible_tables=-1,
            visible_columns=0,
        )
