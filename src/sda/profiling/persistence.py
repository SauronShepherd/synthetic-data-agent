"""Governed Delta persistence adapter for versioned profile artifacts."""

from __future__ import annotations

import json
from contextlib import suppress
from typing import Any, Protocol

from sda.artifacts.delta import persist_artifact_registry
from sda.artifacts.models import ArtifactRef
from sda.profile_models import TableProfile


class SparkSessionLike(Protocol):
    def createDataFrame(
        self, data: Any, schema: Any = None, samplingRatio: Any = None, verifySchema: Any = True
    ) -> Any: ...


def find_reusable_profile(
    spark: SparkSessionLike,
    target: str,
    *,
    source_table: str,
    source_version: str | None,
    configuration_hash: str,
    metadata_inventory_id: str | None = None,
) -> dict[str, Any] | None:
    """Find a completed compatible profile header before scanning source data."""
    if not hasattr(spark, "table"):
        return None
    try:
        matches = (
            spark.table(f"{target}_table_profiles")
            .where("status = 'COMPLETE'")
            .where(f"source_table = '{source_table.replace(chr(39), chr(39) * 2)}'")
            .where(
                "source_version IS NULL"
                if source_version is None
                else f"source_version = '{str(source_version).replace(chr(39), chr(39) * 2)}'"
            )
            .where(
                f"configuration_hash = '{configuration_hash.replace(chr(39), chr(39) * 2)}'"
            )
            .where(
                "metadata_inventory_id IS NULL"
                if metadata_inventory_id is None
                else f"metadata_inventory_id = '{metadata_inventory_id.replace(chr(39), chr(39) * 2)}'"
            )
            .limit(1)
            .collect()
        )
    except Exception:
        return None
    if not matches:
        return None
    row = matches[0]
    return row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)


def persist_profile(
    spark: SparkSessionLike,
    profile: TableProfile,
    target: str,
    *,
    reuse_existing: bool = True,
) -> dict[str, str]:
    """Write queryable header and column evidence without raw source values."""
    payload = profile.to_dict()
    header = {
        key: (
            json.dumps(value, sort_keys=True, default=str)
            if isinstance(value, (dict, list, tuple))
            else (None if value is None else str(value))
        )
        for key, value in payload.items()
        if key != "column_profiles"
    }
    header["status"] = "COMPLETE"
    columns = []
    distributions = []
    for column in payload["column_profiles"]:
        columns.append(
            {
                "profile_id": profile.profile_id,
                "source_table": profile.source_table,
                "column_name": column["column_name"],
                "profile_kind": column["profile_kind"],
                "metrics_json": json.dumps(column["metrics"], sort_keys=True, default=str),
                "warnings_json": json.dumps(column["warnings"], sort_keys=True, default=str),
            }
        )
        for metric_name, metric in column["metrics"].items():
            distributions.append(
                {
                    "profile_id": profile.profile_id,
                    "source_table": profile.source_table,
                    "column_name": column["column_name"],
                    "metric_name": metric_name,
                    "metric_json": json.dumps(metric, sort_keys=True, default=str),
                }
            )
    table_name = f"{target}_table_profiles"
    column_name = f"{target}_column_profiles"
    # replaceWhere makes retries idempotent for an existing Delta table while
    # preserving unrelated profile fingerprints.
    from pyspark.sql.types import StringType, StructField, StructType

    locations = {
        "table_profiles": table_name,
        "column_profiles": column_name,
        "profile_distributions": f"{target}_profile_distributions",
    }
    if hasattr(spark, "sql"):
        with suppress(Exception):
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS (status STRING)")
        with suppress(Exception):
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS (metadata_inventory_id STRING)")
    can_reuse = reuse_existing and profile.snapshot_reproducible
    if can_reuse and hasattr(spark, "table"):
        try:
            existing = (
                spark.table(table_name)
                .where(f"profile_id = '{profile.profile_id}' AND status = 'COMPLETE'")
                .limit(1)
                .count()
            )
            if existing:
                return locations
        except Exception:
            pass

    column_schema = (
        StructType([StructField(key, StringType(), True) for key in columns[0]])
        if columns
        else StructType(
            [
                StructField("profile_id", StringType(), True),
                StructField("source_table", StringType(), True),
                StructField("column_name", StringType(), True),
                StructField("profile_kind", StringType(), True),
                StructField("metrics_json", StringType(), True),
                StructField("warnings_json", StringType(), True),
            ]
        )
    )
    column_rows = [tuple(row.values()) for row in columns]
    spark.createDataFrame(column_rows, schema=column_schema).write.format("delta").mode(
        "overwrite"
    ).option("replaceWhere", f"profile_id = '{profile.profile_id}'").saveAsTable(column_name)
    distribution_name = f"{target}_profile_distributions"
    distribution_schema = StructType(
        [
            StructField(key, StringType(), True)
            for key in (
                distributions[0]
                if distributions
                else {
                    "profile_id": "",
                    "source_table": "",
                    "column_name": "",
                    "metric_name": "",
                    "metric_json": "",
                }
            )
        ]
    )
    distribution_rows = [tuple(row.values()) for row in distributions]
    spark.createDataFrame(distribution_rows, schema=distribution_schema).write.format("delta").mode(
        "overwrite"
    ).option("replaceWhere", f"profile_id = '{profile.profile_id}'").saveAsTable(distribution_name)
    # Publish the COMPLETE header last. A retry can therefore never reuse a
    # profile whose child evidence was only partially written.
    header_schema = StructType([StructField(key, StringType(), True) for key in header])
    header_row = [tuple(header.values())]
    spark.createDataFrame(header_row, schema=header_schema).write.format("delta").mode(
        "overwrite"
    ).option("replaceWhere", f"profile_id = '{profile.profile_id}'").saveAsTable(table_name)
    return {**locations, "profile_distributions": distribution_name}


def persist_profile_registry(
    spark: SparkSessionLike, profile_artifact: ArtifactRef, registry_table: str
) -> None:
    """Publish the searchable registry header after profile details complete."""
    persist_artifact_registry(spark, profile_artifact, registry_table)
