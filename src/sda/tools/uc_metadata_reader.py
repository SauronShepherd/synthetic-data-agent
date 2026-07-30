"""Article 04 Unity Catalog metadata reader.

The module has two layers:

* ``UcMetadataReader`` normalizes raw metadata into the Article 04 contract.
* ``InformationSchemaMetadataAdapter`` executes real Unity Catalog
  ``INFORMATION_SCHEMA`` queries and returns that raw metadata.

The local demo still uses in-memory rows so tests can run without Databricks.
The production path should use the adapter with a Databricks Spark session.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from fnmatch import fnmatchcase
from importlib import import_module
from typing import Any, Protocol

from sda.metadata_models import (
    ColumnMetadata,
    ConstraintKind,
    ConstraintMetadata,
    MetadataInventory,
    MetadataReadConfig,
    ObjectType,
    TableMetadata,
)
from sda.models import AgentState, ArtifactRef, RunStage, ToolName, ToolResult
from sda.version import __version__

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SqlExecutor(Protocol):
    """Minimal SQL execution protocol used by the Databricks adapter."""

    def execute(self, sql: str) -> Sequence[Mapping[str, Any]]:
        """Execute SQL and return mapping-like rows."""


def information_schema_queries(catalog: str, schema: str | None = None) -> tuple[str, ...]:
    """Return deterministic SQL snippets used by the Databricks implementation.

    Use ``system.information_schema`` instead of ``<catalog>.information_schema``.
    Some Databricks Free/serverless workspaces expose the workspace-wide
    information schema through the ``system`` catalog but do not expose a local
    ``information_schema`` schema inside every catalog. Filtering by
    ``table_catalog`` keeps the result scoped to the requested catalog without
    assuming that ``<catalog>.information_schema`` exists.
    """
    catalog_predicate = f"table_catalog = {_quote_literal(catalog)}"
    schema_predicate = "" if schema is None else f" AND table_schema = {_quote_literal(schema)}"
    schema_only_predicate = "" if schema is None else f" AND schema_name = {_quote_literal(schema)}"

    return (
        "SELECT catalog_name AS catalog FROM system.information_schema.catalogs",
        (
            "SELECT catalog_name AS catalog, schema_name AS schema "
            "FROM system.information_schema.schemata "
            f"WHERE catalog_name = {_quote_literal(catalog)}{schema_only_predicate}"
        ),
        (
            "SELECT table_catalog AS catalog, table_schema AS schema, table_name AS name, "
            "table_type AS object_type, table_owner AS owner, comment "
            "FROM system.information_schema.tables "
            f"WHERE {catalog_predicate}{schema_predicate}"
        ),
        (
            "SELECT table_catalog AS catalog, table_schema AS schema, table_name AS name, "
            "column_name, ordinal_position, is_nullable, full_data_type, data_type, comment "
            "FROM system.information_schema.columns "
            f"WHERE {catalog_predicate}{schema_predicate}"
        ),
        (
            "SELECT catalog_name AS catalog, schema_name AS schema, table_name AS name, "
            "tag_name, tag_value "
            "FROM system.information_schema.table_tags "
            f"WHERE catalog_name = {_quote_literal(catalog)}{schema_only_predicate}"
        ),
        (
            "SELECT catalog_name AS catalog, schema_name AS schema, table_name AS name, "
            "column_name, tag_name, tag_value "
            "FROM system.information_schema.column_tags "
            f"WHERE catalog_name = {_quote_literal(catalog)}{schema_only_predicate}"
        ),
        (
            "SELECT constraint_catalog, constraint_schema, constraint_name, "
            "table_catalog AS catalog, table_schema AS schema, table_name AS name, "
            "constraint_type, enforced "
            "FROM system.information_schema.table_constraints "
            f"WHERE {catalog_predicate}{schema_predicate}"
        ),
        (
            "SELECT constraint_catalog, constraint_schema, constraint_name, "
            "table_catalog AS catalog, table_schema AS schema, table_name AS name, "
            "column_name, ordinal_position, position_in_unique_constraint "
            "FROM system.information_schema.key_column_usage "
            f"WHERE {catalog_predicate}{schema_predicate}"
        ),
        (
            "SELECT constraint_catalog, constraint_schema, constraint_name, "
            "unique_constraint_catalog, unique_constraint_schema, unique_constraint_name "
            "FROM system.information_schema.referential_constraints "
            f"WHERE constraint_catalog = {_quote_literal(catalog)}"
        ),
        (
            "SELECT constraint_catalog, constraint_schema, constraint_name, "
            "table_catalog AS referenced_catalog, table_schema AS referenced_schema, "
            "table_name AS referenced_table "
            "FROM system.information_schema.constraint_table_usage "
            f"WHERE constraint_catalog = {_quote_literal(catalog)}"
        ),
        (
            "SELECT constraint_catalog, constraint_schema, constraint_name, "
            "column_name AS referenced_column "
            "FROM system.information_schema.constraint_column_usage "
            f"WHERE constraint_catalog = {_quote_literal(catalog)}"
        ),
    )


@dataclass(frozen=True, slots=True)
class RawTableMetadata:
    """Small adapter-friendly representation of information schema rows."""

    catalog: str
    schema: str
    name: str
    object_type: str
    owner: str | None = None
    comment: str | None = None
    table_tags: tuple[str, ...] = ()
    columns: tuple[ColumnMetadata, ...] = ()
    constraints: tuple[ConstraintMetadata, ...] = ()


class SparkSqlExecutor:
    """SQL executor for Databricks/Spark sessions."""

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def execute(self, sql: str) -> Sequence[Mapping[str, Any]]:
        """Execute SQL with Spark and return plain dictionaries."""
        rows = self._spark.sql(sql).collect()
        return tuple(_row_to_mapping(row) for row in rows)


class DatabricksSqlConnectorExecutor:
    """SQL executor for local reads through a Databricks SQL Warehouse."""

    def __init__(
        self,
        *,
        server_hostname: str,
        http_path: str,
        access_token: str,
    ) -> None:
        self._server_hostname = server_hostname
        self._http_path = http_path
        self._access_token = access_token

    def execute(self, sql: str) -> Sequence[Mapping[str, Any]]:
        """Execute SQL with databricks-sql-connector and return dictionaries."""
        try:
            databricks_sql = import_module("databricks.sql")
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Databricks SQL mode requires databricks-sql-connector. "
                "Install it with: python -m pip install -e .[databricks]"
            ) from exc

        with (
            databricks_sql.connect(
                server_hostname=self._server_hostname,
                http_path=self._http_path,
                access_token=self._access_token,
            ) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(sql)
            description = cursor.description or ()
            column_names = tuple(str(column[0]) for column in description)
            rows = cursor.fetchall()

        return tuple(dict(zip(column_names, row, strict=True)) for row in rows)


class InformationSchemaMetadataAdapter:
    """Read Unity Catalog metadata through ``INFORMATION_SCHEMA`` SQL views."""

    def __init__(self, executor: SqlExecutor) -> None:
        self._executor = executor
        self._query_warnings: list[str] = []

    def read_raw_tables(self, config: MetadataReadConfig) -> tuple[RawTableMetadata, ...]:
        """Execute Unity Catalog metadata queries and return raw table metadata."""
        raw_tables: list[RawTableMetadata] = []
        for catalog in config.catalog_allowlist:
            for schema in _schema_scope(config):
                raw_tables.extend(
                    self._read_catalog_scope(catalog, schema=schema, max_objects=config.max_objects)
                )
        return tuple(raw_tables)

    def read_inventory(self, config: MetadataReadConfig) -> MetadataInventory:
        """Read and normalize a governed metadata inventory from Unity Catalog."""
        self._query_warnings = []
        visible_catalog_rows = self._safe_execute(
            "SELECT catalog_name AS catalog FROM system.information_schema.catalogs"
        )
        visible_catalogs = tuple(sorted({str(row["catalog"]) for row in visible_catalog_rows}))
        raw_tables = self.read_raw_tables(config)
        catalogs = tuple(
            catalog
            for catalog in config.catalog_allowlist
            if not visible_catalogs or catalog in visible_catalogs
        )
        schemas = tuple((table.catalog, table.schema) for table in raw_tables)
        inventory = UcMetadataReader(config=config, raw_tables=raw_tables).read_inventory(
            visible_catalogs=visible_catalogs,
            selected_catalogs=catalogs,
            visible_schemas=tuple(dict.fromkeys(schemas)),
            selected_schemas=tuple(dict.fromkeys(schemas)),
        )
        return MetadataInventory(
            tables=inventory.tables,
            skipped_objects=inventory.skipped_objects,
            warnings=inventory.warnings + tuple(self._query_warnings),
            visible_catalogs=inventory.visible_catalogs,
            selected_catalogs=inventory.selected_catalogs,
            visible_schemas=inventory.visible_schemas,
            selected_schemas=inventory.selected_schemas,
            provenance=inventory.provenance,
        )

    def _read_catalog_scope(
        self,
        catalog: str,
        *,
        schema: str | None,
        max_objects: int,
    ) -> tuple[RawTableMetadata, ...]:
        queries = information_schema_queries(catalog, schema=schema)
        tables = tuple(
            sorted(
                self._executor.execute(queries[2]),
                key=lambda r: (str(r["catalog"]), str(r["schema"]), str(r["name"])),
            )[:max_objects]
        )
        selected = {(str(r["catalog"]), str(r["schema"]), str(r["name"])) for r in tables}
        columns = self._executor.execute(_restrict_query(queries[3], selected))
        table_tags = self._safe_execute(_restrict_query(queries[4], selected))
        column_tags = self._safe_execute(_restrict_query(queries[5], selected))
        constraints = self._safe_execute(_restrict_query(queries[6], selected))
        key_columns = self._safe_execute(_restrict_query(queries[7], selected))
        referential_constraints = self._safe_execute(queries[8])
        constraint_tables = self._safe_execute(queries[9])
        constraint_columns = self._safe_execute(queries[10])

        columns_by_table = _build_columns_by_table(columns, column_tags)
        tags_by_table = _build_table_tags_by_table(table_tags)
        constraints_by_table = _build_constraints_by_table(
            constraints,
            key_columns,
            referential_constraints,
            constraint_tables,
            constraint_columns,
        )

        return tuple(
            RawTableMetadata(
                catalog=str(row["catalog"]),
                schema=str(row["schema"]),
                name=str(row["name"]),
                object_type=str(row.get("object_type", ObjectType.UNKNOWN.value)),
                owner=_optional_str(row.get("owner")),
                comment=_optional_str(row.get("comment")),
                table_tags=tags_by_table.get(_table_key(row), ()),
                columns=columns_by_table.get(_table_key(row), ()),
                constraints=constraints_by_table.get(_table_key(row), ()),
            )
            for row in tables
        )

    def _safe_execute(self, sql: str) -> Sequence[Mapping[str, Any]]:
        try:
            return self._executor.execute(sql)
        except Exception:  # pragma: no cover - Databricks version/permission dependent
            source = sql.lower().split("information_schema.")[-1].split()[0].upper()
            self._query_warnings.append(f"{source.lower()}_metadata_unavailable")
            return ()


class UcMetadataReader:
    """Normalize Unity Catalog metadata into an agent-readable inventory."""

    name = ToolName.UC_METADATA_READER

    def __init__(self, config: MetadataReadConfig, raw_tables: Iterable[RawTableMetadata]) -> None:
        self._config = config
        self._raw_tables = tuple(raw_tables)

    def run(self, state: AgentState) -> ToolResult:
        """Run the reader as an orchestration tool."""
        inventory = self.read_inventory()
        source_tables = {table.full_name for table in inventory.tables}
        requested_tables = {
            f"{state.request.source.catalog}.{state.request.source.schema}.{table}"
            for table in state.request.source.tables
        }
        missing_requested = tuple(sorted(requested_tables - source_tables))
        warnings = inventory.warnings + tuple(
            f"requested table not discovered: {table}" for table in missing_requested
        )
        artifact = ArtifactRef(
            artifact_id=f"{state.request.request_id}:metadata_inventory",
            artifact_type="metadata_inventory",
            produced_by=self.name,
            summary=summarize_inventory(inventory),
            metadata={
                "tool_name": self.name.value,
                "tool_version": __version__,
                "execution_timestamp": datetime.now(UTC).isoformat(),
                "configured_scope": {
                    "catalog_allowlist": list(self._config.catalog_allowlist),
                    "schema_allowlist": list(self._config.schema_allowlist),
                    "table_patterns": list(self._config.table_patterns),
                    "max_objects": self._config.max_objects,
                },
                "table_count": len(inventory.tables),
                "skipped_count": len(inventory.skipped_objects),
                "warning_count": len(warnings),
            },
        )
        return ToolResult(
            tool=self.name,
            stage=RunStage.METADATA_DISCOVERED,
            artifacts=(artifact,),
            warnings=warnings,
            metrics={"metadata_tables": len(inventory.tables)},
        )

    def read_inventory(self, **discovery: Any) -> MetadataInventory:
        """Filter, normalize, and summarize raw metadata rows."""
        accepted: list[TableMetadata] = []
        skipped: list[str] = []
        warnings: list[str] = []

        for raw_table in self._raw_tables:
            full_name = f"{raw_table.catalog}.{raw_table.schema}.{raw_table.name}"
            if not self._is_in_scope(raw_table):
                skipped.append(full_name)
                continue
            if len(accepted) >= self._config.max_objects:
                skipped.append(full_name)
                warnings.append(f"max_objects reached before reading {full_name}")
                continue

            table = self._normalize_table(raw_table)
            accepted.append(table)

        return MetadataInventory(
            tables=tuple(accepted),
            skipped_objects=tuple(skipped),
            warnings=tuple(warnings),
            **discovery,
            provenance={
                "tool_name": self.name.value,
                "tool_version": __version__,
                "configured_scope": {
                    "catalog_allowlist": list(self._config.catalog_allowlist),
                    "schema_allowlist": list(self._config.schema_allowlist),
                    "table_patterns": list(self._config.table_patterns),
                    "max_objects": self._config.max_objects,
                },
            },
        )

    def _is_in_scope(self, table: RawTableMetadata) -> bool:
        if table.catalog not in self._config.catalog_allowlist:
            return False
        if self._config.schema_allowlist and table.schema not in self._config.schema_allowlist:
            return False
        if (
            not self._config.include_views
            and ObjectType.from_platform(table.object_type) == ObjectType.VIEW
        ):
            return False
        if not self._config.table_patterns:
            return True
        return any(fnmatchcase(table.name, pattern) for pattern in self._config.table_patterns)

    def _normalize_table(self, raw_table: RawTableMetadata) -> TableMetadata:
        sensitivity_signals = self._sensitivity_signals(raw_table)
        warnings = self._metadata_warnings(raw_table, sensitivity_signals)
        relationships = tuple(
            f"{constraint.kind.value}:{','.join(constraint.columns)}"
            for constraint in raw_table.constraints
            if constraint.kind
            in {
                ConstraintKind.PRIMARY_KEY,
                ConstraintKind.FOREIGN_KEY,
                ConstraintKind.UNIQUE,
            }
        )
        return TableMetadata(
            catalog=raw_table.catalog,
            schema=raw_table.schema,
            object_name=raw_table.name,
            object_type=ObjectType.from_platform(raw_table.object_type),
            raw_table_type=raw_table.object_type,
            owner=raw_table.owner,
            comment=raw_table.comment,
            table_tags=raw_table.table_tags,
            columns=raw_table.columns,
            constraints=raw_table.constraints,
            relationship_hints=relationships,
            sensitivity_signals=sensitivity_signals,
            metadata_warnings=warnings,
        )

    def _sensitivity_signals(self, table: RawTableMetadata) -> tuple[str, ...]:
        signals: list[str] = []
        terms = tuple(term.lower() for term in self._config.sensitivity_terms)
        table_context = " ".join((table.schema, table.name, table.comment or "")).lower()

        if any(term in table_context for term in terms):
            signals.append("sensitive_table_context")

        for column in table.columns:
            column_context = " ".join((column.name, column.comment or "")).lower()
            tag_context = " ".join(column.tags).lower()
            if any(term in column_context for term in terms):
                signals.append(f"{column.name}:sensitive_name_or_comment")
            tag_terms = ("pii", "personal", "confidential", "sensitive")
            if any(term in tag_context for term in tag_terms):
                signals.append(f"{column.name}:sensitive_tag")
            for tag in column.tags:
                if tag.lower().startswith("class."):
                    signals.append(f"{column.name}:classification_tag:{tag}")

        for tag in table.table_tags:
            if tag.lower().startswith("class.") or any(
                term in tag.lower() for term in ("pii", "sensitive", "confidential")
            ):
                signals.append(f"table:sensitivity_tag:{tag}")

        return tuple(dict.fromkeys(signals))

    @staticmethod
    def _metadata_warnings(
        table: RawTableMetadata,
        sensitivity_signals: tuple[str, ...],
    ) -> tuple[str, ...]:
        warnings: list[str] = []
        if table.comment is None or not table.comment.strip():
            warnings.append("missing_table_comment")
        undocumented_columns = sum(
            1 for column in table.columns if column.comment is None or not column.comment.strip()
        )
        if undocumented_columns:
            warnings.append(f"undocumented_columns:{undocumented_columns}")
        if not table.constraints:
            warnings.append("no_declared_constraints")
        if sensitivity_signals and not any(
            "sensitive_tag" in signal for signal in sensitivity_signals
        ):
            warnings.append("sensitive_field_without_tag")
        if any(not constraint.validated for constraint in table.constraints):
            warnings.append("declared_constraints_unvalidated")
        return tuple(warnings)


def build_agent_summary(table: TableMetadata) -> str:
    """Build a compact, traceable summary for LLM reasoning."""
    sensitive = _friendly_join(table.sensitivity_signals, fallback="no sensitivity signals")
    warnings = _friendly_join(table.metadata_warnings, fallback="no metadata warnings")
    constraint_count = len(table.constraints)
    return (
        f"{table.full_name} is a {table.object_type.value.lower()} with "
        f"{len(table.columns)} columns and {constraint_count} declared constraints. "
        f"Sensitivity: {sensitive}. Warnings: {warnings}."
    )


def summarize_inventory(inventory: MetadataInventory) -> str:
    """Summarize the whole metadata inventory."""
    sensitive_tables = sum(1 for table in inventory.tables if table.sensitivity_signals)
    constrained_tables = sum(1 for table in inventory.tables if table.constraints)
    return (
        f"Discovered {len(inventory.tables)} governed objects. "
        f"Sensitive signals found in {sensitive_tables}. "
        f"Declared constraints found in {constrained_tables}. "
        f"Skipped {len(inventory.skipped_objects)} objects."
    )


def table_from_mapping(row: Mapping[str, Any]) -> RawTableMetadata:
    """Build raw table metadata from a mapping-like adapter result."""
    columns = tuple(row.get("columns", ()))
    constraints = tuple(row.get("constraints", ()))
    tags = tuple(row.get("table_tags", ()))
    return RawTableMetadata(
        catalog=str(row["catalog"]),
        schema=str(row["schema"]),
        name=str(row["name"]),
        object_type=str(row.get("object_type", ObjectType.UNKNOWN.value)),
        owner=_optional_str(row.get("owner")),
        comment=_optional_str(row.get("comment")),
        table_tags=tags,
        columns=columns,
        constraints=constraints,
    )


def read_uc_metadata_with_spark(config: MetadataReadConfig, spark: Any) -> MetadataInventory:
    """Read Unity Catalog metadata using a Databricks Spark session."""
    adapter = InformationSchemaMetadataAdapter(SparkSqlExecutor(spark))
    return adapter.read_inventory(config)


def read_uc_metadata_with_databricks_sql(
    config: MetadataReadConfig,
    *,
    server_hostname: str,
    http_path: str,
    access_token: str,
) -> MetadataInventory:
    """Read Unity Catalog metadata through a Databricks SQL Warehouse."""
    adapter = InformationSchemaMetadataAdapter(
        DatabricksSqlConnectorExecutor(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        )
    )
    return adapter.read_inventory(config)


def _build_columns_by_table(
    rows: Sequence[Mapping[str, Any]],
    tag_rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str, str], tuple[ColumnMetadata, ...]]:
    tags_by_column: dict[tuple[str, str, str, str], list[str]] = defaultdict(list)
    for row in tag_rows:
        tags_by_column[(*_table_key(row), str(row["column_name"]))].append(_format_tag(row))

    grouped: dict[tuple[str, str, str], list[ColumnMetadata]] = defaultdict(list)
    for row in rows:
        data_type = row.get("full_data_type") or row.get("data_type") or "UNKNOWN"
        column = ColumnMetadata(
            name=str(row["column_name"]),
            data_type=str(data_type),
            nullable=str(row.get("is_nullable", "YES")).upper() == "YES",
            ordinal_position=max(1, int(row["ordinal_position"])),
            comment=_optional_str(row.get("comment")),
            tags=tuple(tags_by_column.get((*_table_key(row), str(row["column_name"])), ())),
        )
        grouped[_table_key(row)].append(column)

    return {
        key: tuple(sorted(columns, key=lambda column: column.ordinal_position))
        for key, columns in grouped.items()
    }


def _build_table_tags_by_table(
    rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str, str], tuple[str, ...]]:
    grouped: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for row in rows:
        grouped[_table_key(row)].append(_format_tag(row))
    return {key: tuple(values) for key, values in grouped.items()}


def _build_constraints_by_table(
    constraints: Sequence[Mapping[str, Any]],
    key_columns: Sequence[Mapping[str, Any]],
    referential_constraints: Sequence[Mapping[str, Any]],
    constraint_tables: Sequence[Mapping[str, Any]],
    constraint_columns: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str, str], tuple[ConstraintMetadata, ...]]:
    columns_by_constraint: dict[tuple[str, str, str], list[tuple[int, str, int | None]]] = (
        defaultdict(list)
    )
    for row in key_columns:
        columns_by_constraint[_constraint_key(row)].append(
            (
                int(row.get("ordinal_position", 1)),
                str(row["column_name"]),
                int(row["position_in_unique_constraint"])
                if row.get("position_in_unique_constraint") is not None
                else None,
            )
        )

    unique_key_by_fk = {
        _constraint_key(row): (
            str(row["unique_constraint_catalog"]),
            str(row["unique_constraint_schema"]),
            str(row["unique_constraint_name"]),
        )
        for row in referential_constraints
    }
    referenced_table_by_constraint = {
        _constraint_key(row): (
            str(row["referenced_catalog"]),
            str(row["referenced_schema"]),
            str(row["referenced_table"]),
        )
        for row in constraint_tables
    }
    referenced_columns_by_constraint: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for row in constraint_columns:
        referenced_columns_by_constraint[_constraint_key(row)].append(str(row["referenced_column"]))

    grouped: dict[tuple[str, str, str], list[ConstraintMetadata]] = defaultdict(list)
    for row in constraints:
        constraint_key = _constraint_key(row)
        child_parts = sorted(
            columns_by_constraint.get(constraint_key, ()), key=lambda part: part[0]
        )
        ordered_columns = tuple(column for _, column, _ in child_parts)
        if not ordered_columns:
            continue

        referenced_table = None
        referenced_columns: tuple[str, ...] = ()
        unique_key = unique_key_by_fk.get(constraint_key)
        if unique_key is not None:
            referenced_parts = referenced_table_by_constraint.get(unique_key)
            if referenced_parts is not None:
                referenced_table = ".".join(referenced_parts)
                parent_parts = sorted(
                    columns_by_constraint.get(unique_key, ()), key=lambda part: part[0]
                )
                parent_by_position = {ordinal: column for ordinal, column, _ in parent_parts}
                if any(position is not None for _, _, position in child_parts):
                    referenced_columns = tuple(
                        parent_by_position[position]
                        for _, _, position in child_parts
                        if position is not None and position in parent_by_position
                    )
                else:
                    referenced_columns = tuple(column for _, column, _ in parent_parts)
                if len(referenced_columns) != len(ordered_columns):
                    referenced_columns = ()

        grouped[_table_key(row)].append(
            ConstraintMetadata(
                name=str(row["constraint_name"]),
                kind=_parse_constraint_kind(str(row.get("constraint_type", "UNKNOWN"))),
                columns=ordered_columns,
                referenced_table=referenced_table,
                referenced_columns=referenced_columns,
                enforced=str(row.get("enforced", "NO")).upper() == "YES",
                validated=False,
            )
        )
    return {key: tuple(values) for key, values in grouped.items()}


def _schema_scope(config: MetadataReadConfig) -> tuple[str | None, ...]:
    if not config.schema_allowlist:
        return (None,)
    return tuple(config.schema_allowlist)


def _table_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (str(row["catalog"]), str(row["schema"]), str(row["name"]))


def _constraint_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(row["constraint_catalog"]),
        str(row["constraint_schema"]),
        str(row["constraint_name"]),
    )


def _format_tag(row: Mapping[str, Any]) -> str:
    tag_name = str(row["tag_name"])
    tag_value = row.get("tag_value")
    if tag_value is None or str(tag_value) == "":
        return tag_name
    return f"{tag_name}={tag_value}"


def _parse_object_type(value: str) -> ObjectType:
    return ObjectType.from_platform(value)


def _restrict_query(sql: str, selected: set[tuple[str, str, str]]) -> str:
    if not selected:
        return sql + " AND 1 = 0"
    predicates = " OR ".join(
        f"(catalog = {_quote_literal(c)} AND schema = {_quote_literal(s)} "
        f"AND name = {_quote_literal(n)})"
        for c, s, n in sorted(selected)
    )
    return (
        f"SELECT * FROM ({sql}) AS selected_metadata "
        f"WHERE {predicates}"
    )


def _parse_constraint_kind(value: str) -> ConstraintKind:
    normalized = value.strip().upper().replace("_", " ")
    for kind in ConstraintKind:
        if normalized == kind.value:
            return kind
    return ConstraintKind.UNKNOWN


def _friendly_join(values: tuple[str, ...], *, fallback: str) -> str:
    if not values:
        return fallback
    return ", ".join(values)


def _quote_identifier(value: str) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid Unity Catalog identifier: {value!r}")
    return f"`{value}`"


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _row_to_mapping(row: Any) -> Mapping[str, Any]:
    if hasattr(row, "asDict"):
        return dict(row.asDict(recursive=True))
    if isinstance(row, Mapping):
        return row
    raise TypeError("SQL executor returned rows that are not mapping-like")
