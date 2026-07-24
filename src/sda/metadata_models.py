"""Article 04 metadata contracts for Unity Catalog discovery.

These models intentionally store metadata and interpreted signals only. They never
store source table values.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class ObjectType(StrEnum):
    """Supported Unity Catalog relation types for the first metadata reader."""

    BASE_TABLE = "BASE TABLE"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED VIEW"
    STREAMING_TABLE = "STREAMING TABLE"
    UNKNOWN = "UNKNOWN"


class ConstraintKind(StrEnum):
    """Constraint categories collected as unvalidated metadata claims."""

    PRIMARY_KEY = "PRIMARY KEY"
    FOREIGN_KEY = "FOREIGN KEY"
    UNIQUE = "UNIQUE"
    CHECK = "CHECK"
    NOT_NULL = "NOT NULL"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class ColumnMetadata:
    """Column metadata collected from Unity Catalog information schema."""

    name: str
    data_type: str
    nullable: bool
    ordinal_position: int
    comment: str | None = None
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("column name must not be empty")
        if not self.data_type.strip():
            raise ValueError("column data_type must not be empty")
        if self.ordinal_position < 1:
            raise ValueError("ordinal_position must be greater than zero")


@dataclass(frozen=True, slots=True)
class ConstraintMetadata:
    """Declared constraint metadata captured before data validation."""

    name: str
    kind: ConstraintKind
    columns: tuple[str, ...]
    referenced_table: str | None = None
    referenced_columns: tuple[str, ...] = ()
    enforced: bool = False
    validated: bool = False

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("constraint name must not be empty")
        if not self.columns:
            raise ValueError("constraint columns must not be empty")
        if self.referenced_table is None and self.referenced_columns:
            raise ValueError("referenced_columns require referenced_table")
        if self.referenced_table is not None and not self.referenced_columns:
            raise ValueError("referenced_table requires referenced_columns")


@dataclass(frozen=True, slots=True)
class TableMetadata:
    """Normalized table or view metadata for the agent."""

    catalog: str
    schema: str
    object_name: str
    object_type: ObjectType
    owner: str | None = None
    comment: str | None = None
    table_tags: tuple[str, ...] = ()
    columns: tuple[ColumnMetadata, ...] = ()
    constraints: tuple[ConstraintMetadata, ...] = ()
    relationship_hints: tuple[str, ...] = ()
    sensitivity_signals: tuple[str, ...] = ()
    metadata_warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.catalog, "catalog"),
            (self.schema, "schema"),
            (self.object_name, "object_name"),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")

    @property
    def full_name(self) -> str:
        """Return the three-level Unity Catalog object name."""
        return f"{self.catalog}.{self.schema}.{self.object_name}"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable table metadata payload."""
        payload = asdict(self)
        payload["object_type"] = self.object_type.value
        payload["constraints"] = [
            {**asdict(constraint), "kind": constraint.kind.value}
            for constraint in self.constraints
        ]
        payload["full_name"] = self.full_name
        return payload


@dataclass(frozen=True, slots=True)
class MetadataInventory:
    """Agent-readable inventory produced by uc_metadata_reader."""

    tables: tuple[TableMetadata, ...]
    skipped_objects: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        names = [table.full_name for table in self.tables]
        if len(names) != len(set(names)):
            raise ValueError("inventory table names must be unique")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable inventory payload."""
        return {
            "tables": [table.to_dict() for table in self.tables],
            "skipped_objects": list(self.skipped_objects),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class MetadataReadConfig:
    """Explicit scope for metadata discovery."""

    catalog_allowlist: tuple[str, ...]
    schema_allowlist: tuple[str, ...] = ()
    table_patterns: tuple[str, ...] = ()
    max_objects: int = 100
    include_views: bool = True
    sensitivity_terms: tuple[str, ...] = field(
        default_factory=lambda: (
            "email",
            "ssn",
            "passport",
            "iban",
            "phone",
            "address",
            "birth",
            "national_id",
            "tax_id",
        )
    )

    def __post_init__(self) -> None:
        if not self.catalog_allowlist:
            raise ValueError("at least one catalog must be allowed")
        if any(not catalog.strip() for catalog in self.catalog_allowlist):
            raise ValueError("catalog allowlist contains an empty value")
        if self.max_objects <= 0:
            raise ValueError("max_objects must be greater than zero")
