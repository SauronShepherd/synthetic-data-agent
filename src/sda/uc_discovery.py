"""Unity Catalog setup and discovery helpers for the bootstrap milestone.

The bootstrap job has two responsibilities:

1. In development, make the demo self-contained by creating the configured catalog,
   schemas, and a tiny sample table when the run identity is allowed to do so.
2. In every environment, prove that the run identity can see the configured Unity
   Catalog scope through privilege-aware ``system.information_schema`` metadata.

This is discovery, not profiling. It never scans source data for statistics.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any

from sda.bootstrap import BootstrapParameters

_SAMPLE_TABLE_NAME = "sample_customers"


@dataclass(frozen=True, slots=True)
class PreparedUnityCatalogScope:
    """Effective Unity Catalog scope used by the bootstrap run."""

    parameters: BootstrapParameters
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class UnityCatalogDiscoveryResult:
    """Minimal metadata counts returned by the bootstrap discovery step."""

    visible_tables: int
    visible_columns: int


def _quote_identifier(value: str) -> str:
    """Backtick-quote a validated Unity Catalog identifier."""
    return f"`{value}`"


def _qualified_name(*parts: str) -> str:
    return ".".join(_quote_identifier(part) for part in parts)


def _spark_sql_error_message(exc: BaseException) -> str:
    return " ".join(str(exc).split())[:900]


def _seed_sample_table(spark: Any, parameters: BootstrapParameters) -> None:
    table_name = _qualified_name(
        parameters.catalog_name,
        parameters.source_schema_name,
        _SAMPLE_TABLE_NAME,
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          customer_id BIGINT NOT NULL,
          customer_tier STRING,
          country_code STRING,
          created_at TIMESTAMP,
          is_active BOOLEAN
        )
        USING DELTA
        """
    )
    row_count = spark.table(table_name).limit(1).count()
    if row_count == 0:
        spark.sql(
            f"""
            INSERT INTO {table_name} VALUES
              (1, 'BASIC', 'ES', timestamp('2026-01-10 09:00:00'), true),
              (2, 'PREMIUM', 'ES', timestamp('2026-01-12 11:30:00'), true),
              (3, 'ENTERPRISE', 'PT', timestamp('2026-01-15 15:45:00'), false)
            """
        )


def _create_configured_scope(spark: Any, parameters: BootstrapParameters) -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {_quote_identifier(parameters.catalog_name)}")
    source_schema = _qualified_name(
        parameters.catalog_name,
        parameters.source_schema_name,
    )
    output_schema = _qualified_name(
        parameters.catalog_name,
        parameters.output_schema_name,
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {source_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}")
    if parameters.seed_sample_data:
        _seed_sample_table(spark, parameters)


def _find_first_visible_scope(spark: Any) -> tuple[str, str] | None:
    """Return a visible non-system catalog/schema pair when available."""
    F = importlib.import_module("pyspark.sql.functions")
    catalogs = [
        row.catalog_name
        for row in (
            spark.table("system.information_schema.catalogs")
            .where(~F.col("catalog_name").isin("system"))
            .select("catalog_name")
            .orderBy("catalog_name")
            .collect()
        )
    ]
    schemata_df = spark.table("system.information_schema.schemata")
    for catalog_name in catalogs:
        schemas = [
            row.schema_name
            for row in (
                schemata_df.where(F.col("catalog_name") == catalog_name)
                .where(~F.col("schema_name").isin("information_schema"))
                .select("schema_name")
                .orderBy("schema_name")
                .limit(1)
                .collect()
            )
        ]
        if schemas:
            return catalog_name, schemas[0]
    return None


def prepare_unity_catalog_scope(
    spark: Any, parameters: BootstrapParameters
) -> PreparedUnityCatalogScope:
    """Create or resolve the Unity Catalog scope for the bootstrap run.

    The project is self-contained for development: it tries to create the configured
    ``sda_dev`` catalog, the source/output schemas, and a tiny sample table. If the
    developer does not have catalog-creation privileges and fallback is enabled, the
    function resolves the first visible catalog/schema instead of requiring manual SQL.

    Staging and production should normally set ``auto_create_uc_objects=false`` and
    ``allow_catalog_fallback=false`` so missing grants fail loudly.
    """
    warnings: list[str] = []

    if parameters.auto_create_uc_objects:
        try:
            _create_configured_scope(spark, parameters)
            warnings.append(
                "Verified or created configured Unity Catalog bootstrap objects."
            )
            return PreparedUnityCatalogScope(parameters=parameters, warnings=tuple(warnings))
        except Exception as exc:  # pragma: no cover - exercised in Databricks
            message = _spark_sql_error_message(exc)
            if not parameters.allow_catalog_fallback:
                raise RuntimeError(
                    "Automatic Unity Catalog bootstrap failed and fallback is disabled. "
                    f"Original error: {message}"
                ) from exc
            warnings.append(
                "Could not create configured Unity Catalog objects. Falling back to an "
                f"already visible catalog/schema. Original error: {message}"
            )

    fallback_scope = _find_first_visible_scope(spark)
    if fallback_scope is None:
        raise RuntimeError(
            "No visible Unity Catalog catalog/schema pair was found for the current run "
            "identity. The bundle is deployed correctly, but this workspace identity has "
            "neither permission to create the configured dev catalog nor visibility into "
            "an existing usable catalog. Ask for Unity Catalog privileges or deploy with "
            "--var catalog_name=<visible_catalog> --var source_schema_name=<visible_schema> "
            "--var output_schema_name=<visible_schema>."
        )

    catalog_name, schema_name = fallback_scope
    resolved = parameters.with_scope(
        catalog_name=catalog_name,
        source_schema_name=schema_name,
        output_schema_name=schema_name,
    )
    warnings.append(
        "Using fallback Unity Catalog scope "
        f"{catalog_name}.{schema_name} for this development bootstrap run."
    )
    return PreparedUnityCatalogScope(parameters=resolved, warnings=tuple(warnings))


def discover_unity_catalog_scope(
    spark: Any, parameters: BootstrapParameters
) -> UnityCatalogDiscoveryResult:
    """Validate and summarize the configured Unity Catalog scope.

    The ``spark`` argument is intentionally typed as ``Any`` because PySpark is not a
    local development dependency for this bootstrap repository. The function runs in
    Databricks, where ``spark`` and ``pyspark.sql.functions`` are available.
    """
    F = importlib.import_module("pyspark.sql.functions")

    catalogs_df = spark.table("system.information_schema.catalogs")
    catalog_exists = (
        catalogs_df.where(F.col("catalog_name") == parameters.catalog_name).limit(1).count()
    )
    if catalog_exists == 0:
        raise RuntimeError(
            f"Catalog {parameters.catalog_name!r} is not visible to the current run identity."
        )

    required_schemas = [parameters.output_schema_name, parameters.source_schema_name]
    schemata_df = spark.table("system.information_schema.schemata")
    visible_schemas = {
        row.schema_name
        for row in (
            schemata_df.where(F.col("catalog_name") == parameters.catalog_name)
            .where(F.col("schema_name").isin(required_schemas))
            .select("schema_name")
            .collect()
        )
    }
    missing_schemas = sorted(set(required_schemas) - visible_schemas)
    if missing_schemas:
        missing = ", ".join(missing_schemas)
        raise RuntimeError(
            f"Missing or inaccessible schemas in catalog {parameters.catalog_name!r}: {missing}"
        )

    tables_df = (
        spark.table("system.information_schema.tables")
        .where(F.col("table_catalog") == parameters.catalog_name)
        .where(F.col("table_schema") == parameters.source_schema_name)
        .select(
            "table_catalog",
            "table_schema",
            "table_name",
            "table_type",
            "data_source_format",
            "created",
            "last_altered",
        )
        .orderBy("table_name")
    )

    columns_df = (
        spark.table("system.information_schema.columns")
        .where(F.col("table_catalog") == parameters.catalog_name)
        .where(F.col("table_schema") == parameters.source_schema_name)
        .select(
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position",
            "data_type",
            "is_nullable",
            "comment",
        )
        .orderBy("table_name", "ordinal_position")
    )

    return UnityCatalogDiscoveryResult(
        visible_tables=tables_df.count(),
        visible_columns=columns_df.count(),
    )
