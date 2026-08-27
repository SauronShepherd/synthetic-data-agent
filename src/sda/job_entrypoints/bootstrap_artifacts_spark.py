"""Create the minimal governed Delta tables required by the metadata stage."""

from __future__ import annotations

import argparse
import re
from collections.abc import Sequence

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _qualified(name: str) -> str:
    parts = name.split(".")
    if len(parts) != 3 or any(not _IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError(f"expected a safe catalog.schema.table name, got {name!r}")
    return ".".join(f"`{part}`" for part in parts)


def _create_delta_table(spark: object, name: str, columns: Sequence[str]) -> None:
    qualified = _qualified(name)
    catalog, schema, _ = qualified.split(".")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")  # type: ignore[attr-defined]
    definitions = ", ".join(f"`{column}` STRING" for column in columns)
    spark.sql(  # type: ignore[attr-defined]
        f"CREATE TABLE IF NOT EXISTS {qualified} ({definitions}) USING DELTA"
    )
    existing = {column.lower() for column in spark.table(name).columns}  # type: ignore[attr-defined]
    for column in columns:
        if column.lower() not in existing:
            spark.sql(f"ALTER TABLE {qualified} ADD COLUMNS (`{column}` STRING)")  # type: ignore[attr-defined]


def main(argv: Sequence[str] | None = None) -> None:
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    _create_delta_table(
        spark,
        args.metadata_table,
        ("inventory_id", "artifact_schema_version", "created_at", "status", "payload"),
    )
    _create_delta_table(
        spark,
        args.registry_table,
        (
            "artifact_id",
            "artifact_type",
            "artifact_schema_version",
            "status",
            "tool_name",
            "tool_version",
            "strategy_version",
            "run_id",
            "environment",
            "created_at",
            "completed_at",
            "configuration_hash",
            "reuse_fingerprint",
            "content_checksum",
            "primary_location",
            "summary",
            "error_code",
            "error_message_safe",
            "related_locations_json",
            "source_references_json",
            "warnings_json",
            "input_artifact_ids_json",
            "content_json",
        ),
    )
    print(f"Initialized Delta artifact tables: {args.metadata_table}, {args.registry_table}")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata-table", required=True)
    parser.add_argument("--registry-table", required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    main()
