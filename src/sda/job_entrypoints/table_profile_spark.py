"""Thin Databricks entrypoint for one-table SDA 05 profiling."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path

_filename = globals().get("__file__") or globals().get("filename")
_SRC_ROOT = (
    Path(_filename).resolve().parents[2]
    if isinstance(_filename, str) and _filename
    else Path.cwd() / "src"
)
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from sda.profile_models import ProfileMode, TableProfileRequest  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile one Unity Catalog relation.")
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--mode", choices=[mode.value for mode in ProfileMode], default="quick")
    parser.add_argument("--sample-fraction", type=float, default=0.1)
    parser.add_argument("--sample-seed", type=int, default=42)
    parser.add_argument("--column-allowlist", default="")
    parser.add_argument("--profile-catalog", default=os.getenv("SDA_PROFILE_CATALOG", "sda_dev"))
    parser.add_argument("--profile-schema", default=os.getenv("SDA_PROFILE_SCHEMA", "profiles"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    request = TableProfileRequest(
        source_table=args.source_table,
        mode=ProfileMode(args.mode),
        sample_fraction=1.0 if args.mode == "full" else args.sample_fraction,
        sample_seed=args.sample_seed,
        column_allowlist=tuple(
            name.strip()
            for name in re.split(r"[,;]", args.column_allowlist)
            if name.strip()
        ),
        profile_catalog=args.profile_catalog,
        profile_schema=args.profile_schema,
    )
    from pyspark.sql import SparkSession

    from sda.metadata_models import MetadataReadConfig
    from sda.profiling.persistence import persist_profile
    from sda.tools.table_profiler import TableProfiler
    from sda.tools.uc_metadata_reader import InformationSchemaMetadataAdapter, SparkSqlExecutor

    spark = SparkSession.builder.getOrCreate()
    catalog, schema, _ = args.source_table.split(".")
    metadata = InformationSchemaMetadataAdapter(SparkSqlExecutor(spark)).read_inventory(
        MetadataReadConfig(
            catalog_allowlist=(catalog,),
            schema_allowlist=(schema,),
            table_patterns=(args.source_table.rsplit(".", 1)[-1],),
            max_objects=1,
        )
    )
    dataframe = spark.table(args.source_table)
    if (
        len(metadata.tables) != 1
        or metadata.tables[0].full_name != args.source_table
        or not metadata.tables[0].columns
    ):
        from sda.metadata_models import ColumnMetadata, ObjectType, TableMetadata

        metadata_table = TableMetadata(
            catalog=catalog,
            schema=schema,
            object_name=args.source_table.rsplit(".", 1)[-1],
            object_type=ObjectType.TABLE,
            columns=tuple(
                ColumnMetadata(field.name, field.dataType.simpleString(), field.nullable, index + 1)
                for index, field in enumerate(dataframe.schema.fields)
            ),
            metadata_warnings=("metadata_inventory_unavailable_schema_fallback",),
        )
    else:
        metadata_table = metadata.tables[0]
    metadata_schema = {
        column.name: column.data_type.lower() for column in metadata_table.columns
    }
    live_schema = {
        field.name: field.dataType.simpleString().lower()
        for field in dataframe.schema.fields
    }
    schema_drift = (
        set(metadata_schema) != set(live_schema)
        or any(metadata_schema.get(name) != dtype for name, dtype in live_schema.items())
    )
    source_version = None
    storage_freshness: dict[str, object] = {}
    snapshot_warning = "source_version_unavailable"
    try:
        history = (
            spark.sql(f"DESCRIBE HISTORY {args.source_table}")
            .select("version", "timestamp", "operation")
            .limit(1)
            .collect()
        )
        if history:
            source_version = str(history[0]["version"])
            storage_freshness = {
                "available": True,
                "method": "metadata_derived",
                "latest_version": source_version,
                "latest_commit_timestamp": str(history[0]["timestamp"]),
                "latest_operation": str(history[0]["operation"]),
            }
            snapshot_warning = ""
    except Exception:
        pass
    profile = TableProfiler(
        request,
        metadata_table,
        session_timezone=str(spark.conf.get("spark.sql.session.timeZone", "UTC")),
    ).profile_spark(dataframe, source_version=source_version)
    if snapshot_warning:
        profile = replace(
            profile,
            warnings=tuple(dict.fromkeys((*profile.warnings, snapshot_warning))),
        )
    if schema_drift:
        profile = replace(
            profile,
            warnings=tuple(
                dict.fromkeys((*profile.warnings, "source_schema_changed_since_metadata_read"))
            ),
        )
    if storage_freshness:
        profile = replace(profile, storage_freshness=storage_freshness)
    else:
        profile = replace(
            profile,
            storage_freshness={
                "available": False,
                "method": "unavailable",
                "warning": "freshness_history_unavailable",
            },
        )
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{args.profile_catalog}`.`{args.profile_schema}`")
    locations = persist_profile(
        spark,
        profile,
        f"`{args.profile_catalog}`.`{args.profile_schema}`.profile",
        reuse_existing=request.reuse_existing,
    )
    print(
        json.dumps(
            {
                "profile_id": profile.profile_id,
                "agent_summary": profile.agent_summary,
                "artifact_locations": locations,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
