"""Governed Delta persistence adapter for versioned profile artifacts."""

from __future__ import annotations

import json
from typing import Any, Protocol

from sda.profile_models import TableProfile


class SparkSessionLike(Protocol):
    def createDataFrame(
        self, data: Any, schema: Any = None, samplingRatio: Any = None, verifySchema: Any = True
    ) -> Any: ...


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
        key: (str(value) if key == "profile_id" else json.dumps(value, sort_keys=True, default=str))
        for key, value in payload.items()
        if key != "column_profiles"
    }
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
    if reuse_existing and hasattr(spark, "table"):
        try:
            existing = spark.table(table_name).where(
                f"profile_id = '{profile.profile_id}'"
            ).limit(1).count()
            if existing:
                return locations
        except Exception:
            pass

    header_schema = StructType([StructField(key, StringType(), True) for key in header])
    header_row = [tuple(header.values())]
    spark.createDataFrame(header_row, schema=header_schema).write.format("delta").mode(
        "overwrite"
    ).option("replaceWhere", f"profile_id = '{profile.profile_id}'").saveAsTable(table_name)
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
    return {**locations, "profile_distributions": distribution_name}
