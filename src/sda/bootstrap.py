"""Bootstrap helpers for the Databricks bundle smoke test.

The functions in this module are intentionally small and dependency-free so they can
be tested locally. The Databricks notebook uses them as its parameter and summary
contract, while Spark-specific Unity Catalog setup and discovery stay in
``sda.uc_discovery``.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TRUE_VALUES = {"1", "true", "yes", "y", "on"}
_FALSE_VALUES = {"0", "false", "no", "n", "off"}


@dataclass(frozen=True, slots=True)
class BootstrapParameters:
    """Validated parameters passed from the Databricks job into the notebook."""

    catalog_name: str
    output_schema_name: str
    source_schema_name: str
    target_environment: str = "dev"
    auto_create_uc_objects: bool = True
    seed_sample_data: bool = True
    allow_catalog_fallback: bool = True

    def as_dict(self) -> dict[str, str | bool]:
        """Return a JSON-serializable representation."""
        return asdict(self)

    def with_scope(
        self,
        *,
        catalog_name: str,
        source_schema_name: str,
        output_schema_name: str,
    ) -> BootstrapParameters:
        """Return a copy with a resolved Unity Catalog scope."""
        return replace(
            self,
            catalog_name=require_identifier(catalog_name, "catalog_name"),
            source_schema_name=require_identifier(source_schema_name, "source_schema_name"),
            output_schema_name=require_identifier(output_schema_name, "schema_name"),
        )


@dataclass(frozen=True, slots=True)
class BootstrapSummary:
    """Small structured result emitted by the bootstrap workflow."""

    catalog_name: str
    source_schema_name: str
    output_schema_name: str
    visible_tables: int
    visible_columns: int
    target_environment: str
    auto_create_uc_objects: bool
    seed_sample_data: bool
    warnings: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, str | int | bool | list[str]]:
        """Return a JSON-serializable representation."""
        payload = asdict(self)
        payload["warnings"] = list(self.warnings)
        return payload


def require_identifier(value: str, name: str) -> str:
    """Validate a simple Unity Catalog identifier.

    The bootstrap workflow deliberately accepts simple identifiers only. Supporting
    quoted identifiers is possible later, but the first deployment check should avoid
    dynamic SQL ambiguity.
    """
    candidate = value.strip()
    if not candidate:
        raise ValueError(f"Missing required parameter: {name}")
    if not _IDENTIFIER.fullmatch(candidate):
        raise ValueError(
            f"Invalid {name}: {value!r}. Use simple Unity Catalog identifiers only."
        )
    return candidate


def parse_bool(value: str, name: str) -> bool:
    """Parse a Databricks widget boolean value."""
    normalized = value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(
        f"Invalid {name}: {value!r}. Use one of: true, false, yes, no, 1, 0."
    )


def _get_or_default(widget_get: Callable[[str], str], name: str, default: str) -> str:
    try:
        value = widget_get(name)
    except Exception:
        return default
    return value if value is not None else default


def load_bootstrap_parameters(widget_get: Callable[[str], str]) -> BootstrapParameters:
    """Load and validate Databricks widget parameters."""
    return BootstrapParameters(
        catalog_name=require_identifier(widget_get("catalog_name"), "catalog_name"),
        output_schema_name=require_identifier(widget_get("schema_name"), "schema_name"),
        source_schema_name=require_identifier(
            widget_get("source_schema_name"), "source_schema_name"
        ),
        target_environment=require_identifier(
            _get_or_default(widget_get, "target_environment", "dev"),
            "target_environment",
        ),
        auto_create_uc_objects=parse_bool(
            _get_or_default(widget_get, "auto_create_uc_objects", "true"),
            "auto_create_uc_objects",
        ),
        seed_sample_data=parse_bool(
            _get_or_default(widget_get, "seed_sample_data", "true"),
            "seed_sample_data",
        ),
        allow_catalog_fallback=parse_bool(
            _get_or_default(widget_get, "allow_catalog_fallback", "true"),
            "allow_catalog_fallback",
        ),
    )


def build_bootstrap_summary(
    *,
    parameters: BootstrapParameters,
    visible_tables: int,
    visible_columns: int,
    warnings: tuple[str, ...] = (),
) -> BootstrapSummary:
    """Create the final bootstrap discovery summary."""
    if visible_tables < 0 or visible_columns < 0:
        raise ValueError("visible table and column counts must not be negative")
    return BootstrapSummary(
        catalog_name=parameters.catalog_name,
        source_schema_name=parameters.source_schema_name,
        output_schema_name=parameters.output_schema_name,
        visible_tables=visible_tables,
        visible_columns=visible_columns,
        target_environment=parameters.target_environment.strip() or "unknown",
        auto_create_uc_objects=parameters.auto_create_uc_objects,
        seed_sample_data=parameters.seed_sample_data,
        warnings=warnings,
    )
