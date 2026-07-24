from __future__ import annotations

import pytest

from sda.config import Settings


def test_defaults() -> None:
    settings = Settings.from_env({})
    assert settings.app_name == "Synthetic Data Agent"
    assert settings.environment == "dev"
    assert settings.log_level == "INFO"


def test_environment_overrides() -> None:
    settings = Settings.from_env(
        {
            "SDA_APP_NAME": "SDA Test",
            "SDA_ENVIRONMENT": "staging",
            "SDA_LOG_LEVEL": "debug",
        }
    )
    assert settings.to_dict() == {
        "app_name": "SDA Test",
        "environment": "staging",
        "log_level": "DEBUG",
        "catalog_allowlist": ("main",),
        "schema_allowlist": (),
        "table_patterns": (),
        "max_metadata_objects": 100,
        "metadata_runtime": "auto",
        "databricks_server_hostname": None,
        "databricks_http_path_configured": False,
        "databricks_token_configured": False,
    }


def test_invalid_environment() -> None:
    for value in ("local", "production", ""):
        with pytest.raises(ValueError, match="SDA_ENVIRONMENT"):
            Settings.from_env({"SDA_ENVIRONMENT": value})


def test_invalid_log_level() -> None:
    with pytest.raises(ValueError, match="SDA_LOG_LEVEL"):
        Settings.from_env({"SDA_LOG_LEVEL": "TRACE"})


def test_metadata_reader_config_from_environment() -> None:
    settings = Settings.from_env(
        {
            "SDA_CATALOG_ALLOWLIST": "main,samples",
            "SDA_MAX_METADATA_OBJECTS": "25",
            "SDA_SCHEMA_ALLOWLIST": "sales,crm",
            "SDA_TABLE_PATTERNS": "customers*,orders",
        }
    )

    metadata_config = settings.metadata_read_config()

    assert metadata_config.catalog_allowlist == ("main", "samples")
    assert metadata_config.schema_allowlist == ("sales", "crm")
    assert metadata_config.table_patterns == ("customers*", "orders")
    assert metadata_config.max_objects == 25


def test_invalid_metadata_object_limit() -> None:
    with pytest.raises(ValueError, match="SDA_MAX_METADATA_OBJECTS"):
        Settings.from_env({"SDA_MAX_METADATA_OBJECTS": "0"})


def test_databricks_sql_settings_are_loaded_without_exposing_token() -> None:
    settings = Settings.from_env(
        {
            "SDA_METADATA_RUNTIME": "databricks_sql",
            "DATABRICKS_HOST": "https://example.cloud.databricks.com/",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/abc",
            "DATABRICKS_TOKEN": "secret-token",
        }
    )

    payload = settings.to_dict()

    assert settings.has_databricks_sql_credentials()
    assert settings.databricks_server_hostname == "example.cloud.databricks.com"
    assert payload["databricks_http_path_configured"] is True
    assert payload["databricks_token_configured"] is True
    assert "secret-token" not in str(payload)


def test_invalid_metadata_runtime() -> None:
    with pytest.raises(ValueError, match="SDA_METADATA_RUNTIME"):
        Settings.from_env({"SDA_METADATA_RUNTIME": "local"})
