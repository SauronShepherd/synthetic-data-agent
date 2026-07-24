"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from sda.metadata_models import MetadataReadConfig

_DEFAULT_APP_NAME = "Synthetic Data Agent"
_DEFAULT_ENVIRONMENT = "dev"
_DEFAULT_LOG_LEVEL = "INFO"
_ALLOWED_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
_ALLOWED_ENVIRONMENTS = {"dev", "staging", "prod"}
_ALLOWED_METADATA_RUNTIMES = {"auto", "spark", "databricks_sql"}


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings for the Article 01 application shell."""

    app_name: str = _DEFAULT_APP_NAME
    environment: str = _DEFAULT_ENVIRONMENT
    log_level: str = _DEFAULT_LOG_LEVEL
    catalog_allowlist: tuple[str, ...] = ("main",)
    schema_allowlist: tuple[str, ...] = ()
    table_patterns: tuple[str, ...] = ()
    max_metadata_objects: int = 100
    metadata_runtime: str = "auto"
    databricks_server_hostname: str | None = None
    databricks_http_path: str | None = None
    databricks_token: str | None = None

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        """Create validated settings from an environment mapping."""
        values = os.environ if environ is None else environ
        app_name = values.get("SDA_APP_NAME", _DEFAULT_APP_NAME).strip()
        environment = values.get("SDA_ENVIRONMENT", _DEFAULT_ENVIRONMENT).strip().lower()
        log_level = values.get("SDA_LOG_LEVEL", _DEFAULT_LOG_LEVEL).strip().upper()
        catalog_allowlist = _parse_csv(values.get("SDA_CATALOG_ALLOWLIST", "main"))
        schema_allowlist = _parse_optional_csv(values.get("SDA_SCHEMA_ALLOWLIST", ""))
        table_patterns = _parse_optional_csv(values.get("SDA_TABLE_PATTERNS", ""))
        max_metadata_objects = _parse_positive_int(
            values.get("SDA_MAX_METADATA_OBJECTS", "100"),
            name="SDA_MAX_METADATA_OBJECTS",
        )
        metadata_runtime = values.get("SDA_METADATA_RUNTIME", "auto").strip().lower()
        databricks_server_hostname = _normalize_hostname(
            values.get("DATABRICKS_SERVER_HOSTNAME") or values.get("DATABRICKS_HOST")
        )
        databricks_http_path = _optional_non_empty(values.get("DATABRICKS_HTTP_PATH"))
        databricks_token = _optional_non_empty(values.get("DATABRICKS_TOKEN"))

        if not app_name:
            raise ValueError("SDA_APP_NAME must not be empty")
        if environment not in _ALLOWED_ENVIRONMENTS:
            allowed = ", ".join(sorted(_ALLOWED_ENVIRONMENTS))
            raise ValueError(f"SDA_ENVIRONMENT must be one of: {allowed}")
        if log_level not in _ALLOWED_LOG_LEVELS:
            allowed = ", ".join(sorted(_ALLOWED_LOG_LEVELS))
            raise ValueError(f"SDA_LOG_LEVEL must be one of: {allowed}")
        if metadata_runtime not in _ALLOWED_METADATA_RUNTIMES:
            allowed = ", ".join(sorted(_ALLOWED_METADATA_RUNTIMES))
            raise ValueError(f"SDA_METADATA_RUNTIME must be one of: {allowed}")

        return cls(
            app_name=app_name,
            environment=environment,
            log_level=log_level,
            catalog_allowlist=catalog_allowlist,
            schema_allowlist=schema_allowlist,
            table_patterns=table_patterns,
            max_metadata_objects=max_metadata_objects,
            metadata_runtime=metadata_runtime,
            databricks_server_hostname=databricks_server_hostname,
            databricks_http_path=databricks_http_path,
            databricks_token=databricks_token,
        )

    def metadata_read_config(self) -> MetadataReadConfig:
        """Build the explicit Article 04 metadata reader configuration."""
        return MetadataReadConfig(
            catalog_allowlist=self.catalog_allowlist,
            schema_allowlist=self.schema_allowlist,
            table_patterns=self.table_patterns,
            max_objects=self.max_metadata_objects,
        )

    def has_databricks_sql_credentials(self) -> bool:
        """Return whether enough SQL Warehouse settings exist for local UC reads."""
        return all(
            (
                self.databricks_server_hostname,
                self.databricks_http_path,
                self.databricks_token,
            )
        )

    def to_dict(self) -> dict[str, object]:
        """Return settings as a serializable dictionary without exposing secrets."""
        return {
            "app_name": self.app_name,
            "environment": self.environment,
            "log_level": self.log_level,
            "catalog_allowlist": self.catalog_allowlist,
            "schema_allowlist": self.schema_allowlist,
            "table_patterns": self.table_patterns,
            "max_metadata_objects": self.max_metadata_objects,
            "metadata_runtime": self.metadata_runtime,
            "databricks_server_hostname": self.databricks_server_hostname,
            "databricks_http_path_configured": self.databricks_http_path is not None,
            "databricks_token_configured": self.databricks_token is not None,
        }


def _parse_csv(value: str) -> tuple[str, ...]:
    parsed = tuple(item.strip() for item in value.split(",") if item.strip())
    if not parsed:
        raise ValueError("SDA_CATALOG_ALLOWLIST must contain at least one catalog")
    return parsed


def _parse_positive_int(value: str, *, name: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return parsed


def _parse_optional_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _optional_non_empty(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _normalize_hostname(value: str | None) -> str | None:
    stripped = _optional_non_empty(value)
    if stripped is None:
        return None
    return stripped.removeprefix("https://").removeprefix("http://").rstrip("/")


def load_settings(environ: Mapping[str, str] | None = None) -> Settings:
    """Backward-compatible settings loader used by earlier CLI snippets."""
    return Settings.from_env(environ)
