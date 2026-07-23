"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass

_DEFAULT_APP_NAME = "Synthetic Data Agent"
_DEFAULT_ENVIRONMENT = "dev"
_DEFAULT_LOG_LEVEL = "INFO"
_ALLOWED_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
_ALLOWED_ENVIRONMENTS = {"dev", "staging", "prod"}


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings for the Article 01 application shell."""

    app_name: str = _DEFAULT_APP_NAME
    environment: str = _DEFAULT_ENVIRONMENT
    log_level: str = _DEFAULT_LOG_LEVEL

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        """Create validated settings from an environment mapping."""
        values = os.environ if environ is None else environ
        app_name = values.get("SDA_APP_NAME", _DEFAULT_APP_NAME).strip()
        environment = values.get("SDA_ENVIRONMENT", _DEFAULT_ENVIRONMENT).strip().lower()
        log_level = values.get("SDA_LOG_LEVEL", _DEFAULT_LOG_LEVEL).strip().upper()

        if not app_name:
            raise ValueError("SDA_APP_NAME must not be empty")
        if environment not in _ALLOWED_ENVIRONMENTS:
            allowed = ", ".join(sorted(_ALLOWED_ENVIRONMENTS))
            raise ValueError(f"SDA_ENVIRONMENT must be one of: {allowed}")
        if log_level not in _ALLOWED_LOG_LEVELS:
            allowed = ", ".join(sorted(_ALLOWED_LOG_LEVELS))
            raise ValueError(f"SDA_LOG_LEVEL must be one of: {allowed}")

        return cls(app_name=app_name, environment=environment, log_level=log_level)

    def to_dict(self) -> dict[str, str]:
        """Return settings as a serializable dictionary."""
        return asdict(self)
