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
    }


def test_invalid_environment() -> None:
    for value in ("local", "production", ""):
        with pytest.raises(ValueError, match="SDA_ENVIRONMENT"):
            Settings.from_env({"SDA_ENVIRONMENT": value})


def test_invalid_log_level() -> None:
    with pytest.raises(ValueError, match="SDA_LOG_LEVEL"):
        Settings.from_env({"SDA_LOG_LEVEL": "TRACE"})
