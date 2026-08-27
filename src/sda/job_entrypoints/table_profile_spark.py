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

from sda.profile_models import (  # noqa: E402
    ProfileMode,
    TableProfileRequest,
    ValueRetentionPolicy,
)
from sda.runtime.identifiers import QualifiedName  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile one Unity Catalog relation.")
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--mode", choices=[mode.value for mode in ProfileMode], default="quick")
    parser.add_argument("--column-allowlist", default="")
    parser.add_argument("--column-denylist", default="")
    parser.add_argument("--sample-fraction", type=float, default=0.1)
    parser.add_argument("--sample-seed", type=int, default=42)
    parser.add_argument("--stable-key-column", default=None)
    parser.add_argument("--max-category-values", type=int, default=100)
    parser.add_argument("--percentile-accuracy", type=int, default=10000)
    parser.add_argument("--business-event-column", default="")
    parser.add_argument("--conditional-null-segments", default="")
    parser.add_argument("--outlier-methods", default="iqr,percentile")
    parser.add_argument("--value-retention-policy", default="redact_values")
    parser.add_argument("--sensitive-value-retention-policy", default="no_values")
    parser.add_argument(
        "--reuse-existing", type=lambda value: value.lower() == "true", default=True
    )
    parser.add_argument(
        "--allow-best-effort-snapshot",
        type=lambda value: value.lower() == "true",
        default=True,
    )
    parser.add_argument(
        "--allow-profile-schema-create",
        type=lambda value: value.lower() == "true",
        default=False,
    )
    parser.add_argument(
        "--allow-metadata-fallback",
        type=lambda value: value.strip().lower() == "true",
        default=False,
    )
    parser.add_argument("--profile-catalog", default=os.getenv("SDA_PROFILE_CATALOG", "sda_dev"))
    parser.add_argument("--profile-schema", default=os.getenv("SDA_PROFILE_SCHEMA", "profiles"))
    parser.add_argument("--metadata-inventory-id", default="")
    parser.add_argument("--metadata-inventory-table", default="")
    parser.add_argument(
        "--artifact-registry-table", default=os.getenv("SDA_ARTIFACT_REGISTRY_TABLE", "")
    )
    parser.add_argument("--run-id", default=os.getenv("SDA_RUN_ID", "table-profile-local"))
    parser.add_argument("--environment", default=os.getenv("DATABRICKS_BUNDLE_TARGET", "dev"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if not args.run_id:
        from datetime import UTC, datetime

        args.run_id = f"table-profile-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
    source_name = QualifiedName.parse(args.source_table)
    request = TableProfileRequest(
        source_table=source_name.full_name,
        mode=ProfileMode(args.mode),
        sample_fraction=1.0 if args.mode == "full" else args.sample_fraction,
        sample_seed=args.sample_seed,
        stable_key_column=args.stable_key_column or None,
        column_allowlist=tuple(
            name.strip() for name in re.split(r"[,;]", args.column_allowlist) if name.strip()
        ),
        column_denylist=tuple(
            name.strip() for name in re.split(r"[,;]", args.column_denylist) if name.strip()
        ),
        max_category_values=args.max_category_values,
        percentile_accuracy=args.percentile_accuracy,
        business_event_column=args.business_event_column or None,
        conditional_null_segments=tuple(
            name.strip()
            for name in re.split(r"[,;]", args.conditional_null_segments)
            if name.strip()
        ),
        outlier_methods=tuple(
            name.strip() for name in re.split(r"[,;]", args.outlier_methods) if name.strip()
        ),
        value_retention_policy=ValueRetentionPolicy(args.value_retention_policy),
        sensitive_value_retention_policy=ValueRetentionPolicy(
            args.sensitive_value_retention_policy
        ),
        reuse_existing=args.reuse_existing,
        allow_best_effort_snapshot=args.allow_best_effort_snapshot,
        profile_catalog=args.profile_catalog,
        profile_schema=args.profile_schema,
    )
    from pyspark.sql import SparkSession

    from sda.artifacts.loaders import load_metadata_inventory
    from sda.metadata_models import MetadataInventory, MetadataReadConfig
    from sda.profiling.persistence import find_reusable_profile, persist_profile
    from sda.tools.table_profiler import TableProfiler
    from sda.tools.uc_metadata_reader import InformationSchemaMetadataAdapter, SparkSqlExecutor

    spark = SparkSession.builder.getOrCreate()
    catalog, schema, _ = source_name.full_name.split(".")
    persisted_inventory: MetadataInventory | None = None
    if args.metadata_inventory_id:
        if not args.metadata_inventory_table:
            raise ValueError("metadata inventory table is required with metadata inventory ID")
        persisted_payload = load_metadata_inventory(
            spark, args.metadata_inventory_table, args.metadata_inventory_id
        )
        from sda.artifacts.loaders import metadata_inventory_from_payload

        persisted_inventory = metadata_inventory_from_payload(persisted_payload)
        persisted_tables = {table.full_name for table in persisted_inventory.tables}
        if source_name.full_name not in persisted_tables:
            raise RuntimeError("metadata inventory does not contain the requested source table")
    metadata = (
        persisted_inventory
        if persisted_inventory is not None
        else InformationSchemaMetadataAdapter(SparkSqlExecutor(spark)).read_inventory(
            MetadataReadConfig(
                catalog_allowlist=(catalog,),
                schema_allowlist=(schema,),
                table_patterns=(source_name.object_name,),
                max_objects=1,
            )
        )
    )
    governed_table = next(
        (table for table in metadata.tables if table.full_name == source_name.full_name),
        None,
    )
    metadata_missing = governed_table is None or not governed_table.columns
    if metadata_missing and not args.allow_metadata_fallback:
        raise RuntimeError(
            f"Governed metadata unavailable for {args.source_table}; "
            "use --allow-metadata-fallback only for explicit development diagnostics"
        )
    if metadata_missing:
        dataframe = spark.table(source_name.quoted)
        from sda.metadata_models import ColumnMetadata, ObjectType, TableMetadata

        metadata_table = TableMetadata(
            catalog=catalog,
            schema=schema,
            object_name=source_name.object_name,
            object_type=ObjectType.TABLE,
            columns=tuple(
                ColumnMetadata(field.name, field.dataType.simpleString(), field.nullable, index + 1)
                for index, field in enumerate(dataframe.schema.fields)
            ),
            metadata_warnings=("metadata_inventory_unavailable_schema_fallback",),
        )
    else:
        # Constructing the relation is lazy; actions remain below the reuse
        # check, so compatible profiles can still return before a scan.
        dataframe = spark.table(source_name.quoted)
        assert governed_table is not None
        metadata_table = governed_table
    available_columns = {column.name: column for column in metadata_table.columns}
    requested_columns = set(request.column_allowlist) | set(request.column_denylist)
    requested_columns.update(request.conditional_null_segments)
    if request.business_event_column:
        requested_columns.add(request.business_event_column)
    missing_columns = sorted(requested_columns - set(available_columns))
    if missing_columns:
        raise ValueError(
            "Requested profiler columns are absent from governed metadata: "
            + ", ".join(missing_columns)
        )
    if request.business_event_column:
        event_type = available_columns[request.business_event_column].data_type.lower()
        if "date" not in event_type and "timestamp" not in event_type:
            raise ValueError("business_event_column must be temporal")
    metadata_schema = {column.name: column.data_type.lower() for column in metadata_table.columns}
    live_schema = {
        field.name: field.dataType.simpleString().lower() for field in dataframe.schema.fields
    }
    schema_drift = set(metadata_schema) != set(live_schema) or any(
        metadata_schema.get(name) != dtype for name, dtype in live_schema.items()
    )
    source_version = None
    storage_freshness: dict[str, object] = {}
    snapshot_warning = "source_version_unavailable"
    try:
        history = (
            spark.sql(f"DESCRIBE HISTORY {source_name.quoted}")
            .select("version", "timestamp", "operation")
            .orderBy("version", ascending=False)
            .limit(100)
            .collect()
        )
        if history:
            latest = history[0]
            data_operations = {
                "WRITE",
                "CREATE TABLE AS SELECT",
                "REPLACE TABLE AS SELECT",
                "MERGE",
                "UPDATE",
                "DELETE",
                "COPY INTO",
            }
            latest_data_change = next(
                (item for item in history if str(item["operation"]).upper() in data_operations),
                latest,
            )
            source_version = str(latest["version"])
            storage_freshness = {
                "available": True,
                "method": "metadata_derived",
                "latest_version": source_version,
                "latest_commit_timestamp": str(latest["timestamp"]),
                "latest_operation": str(latest["operation"]),
                "latest_data_change_version": str(latest_data_change["version"]),
                "latest_data_change_timestamp": str(latest_data_change["timestamp"]),
                "latest_data_change_operation": str(latest_data_change["operation"]),
            }
            snapshot_warning = ""
    except Exception as exc:
        # Snapshot history is optional only for an explicitly authorized
        # best-effort run. Preserve a safe diagnostic for the receipt; never
        # silently convert an unavailable snapshot into a reusable profile.
        snapshot_warning = "source_version_lookup_failed"
        storage_freshness = {
            "available": False,
            "method": "metadata_derived",
            "reason": type(exc).__name__,
        }
    if source_version is None and not request.allow_best_effort_snapshot:
        raise RuntimeError(
            f"A reproducible source snapshot is unavailable for {args.source_table}; "
            "set allow_best_effort_snapshot=true only when explicitly permitted"
        )
    if request.reuse_existing:
        reusable = find_reusable_profile(
            spark,
            f"`{args.profile_catalog}`.`{args.profile_schema}`.profile",
            source_table=source_name.full_name,
            source_version=source_version,
            configuration_hash=request.configuration_hash,
            metadata_inventory_id=args.metadata_inventory_id or None,
        )
        if reusable is not None:
            profile_id = str(reusable.get("profile_id") or "")
            registry_ready = False
            if args.artifact_registry_table and profile_id and hasattr(spark, "table"):
                try:
                    registry_ready = (
                        spark.table(args.artifact_registry_table)
                        .where(f"artifact_id = '{profile_id.replace(chr(39), chr(39) * 2)}'")
                        .where("status = 'complete'")
                        .limit(1)
                        .count()
                        > 0
                    )
                except Exception as exc:
                    message = str(exc).upper()
                    if not any(
                        marker in message
                        for marker in ("TABLE_OR_VIEW_NOT_FOUND", "TABLE_NOT_FOUND")
                    ):
                        raise
            if registry_ready or not args.artifact_registry_table:
                print(
                    json.dumps(
                        {
                            "status": "REUSED",
                            "profile_id": profile_id,
                            "source_table": source_name.full_name,
                            "source_version": source_version,
                        },
                        sort_keys=True,
                    )
                )
                return
    dataframe = spark.table(source_name.quoted)
    if source_version is not None:
        dataframe = spark.sql(
            f"SELECT * FROM {source_name.quoted} VERSION AS OF {int(source_version)}"
        )
    profile = TableProfiler(
        request,
        metadata_table,
        session_timezone=str(spark.conf.get("spark.sql.session.timeZone", "UTC")),
    ).profile_spark(dataframe, source_version=source_version)
    if snapshot_warning:
        profile = replace(
            profile,
            warnings=tuple(
                dict.fromkeys(
                    (*profile.warnings, *metadata_table.metadata_warnings, snapshot_warning)
                )
            ),
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
    if args.metadata_inventory_id:
        from sda.profile_models import sha256_json

        profile = replace(
            profile,
            profile_id=sha256_json(
                {
                    "profile_id": profile.profile_id,
                    "metadata_inventory_id": args.metadata_inventory_id,
                }
            ),
            metadata_inventory_id=args.metadata_inventory_id,
        )
    if args.allow_profile_schema_create:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{args.profile_catalog}`.`{args.profile_schema}`")
    locations = persist_profile(
        spark,
        profile,
        f"`{args.profile_catalog}`.`{args.profile_schema}`.profile",
        reuse_existing=request.reuse_existing,
    )
    if args.artifact_registry_table:
        from datetime import UTC, datetime

        from sda.artifacts.delta import persist_artifact_registry
        from sda.artifacts.fingerprint import fingerprint
        from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType, SourceReference
        from sda.version import __version__

        profile_artifact = ArtifactRef(
            artifact_id=profile.profile_id,
            artifact_type=ArtifactType.TABLE_PROFILE,
            artifact_schema_version=profile.profile_schema_version,
            status=ArtifactStatus.COMPLETE,
            tool_name="table_profiler",
            tool_version=__version__,
            strategy_version="table-profile-v1",
            run_id=args.run_id,
            environment=args.environment,
            created_at=datetime.now(UTC).isoformat(),
            configuration_hash=profile.configuration_hash,
            primary_location=locations["table_profiles"],
            related_locations={
                "column_profiles": locations["column_profiles"],
                "distributions": locations["profile_distributions"],
                "registry": args.artifact_registry_table,
            },
            source_references=(
                SourceReference(
                    profile.source_table,
                    "TABLE",
                    "delta_version" if profile.source_version else "best_effort",
                    profile.source_version,
                    None,
                    None,
                    metadata_inventory_id=profile.metadata_inventory_id,
                ),
            ),
            checksum=fingerprint(profile.to_dict()),
            summary=profile.agent_summary,
            warnings=profile.warnings,
            content=profile.to_dict(),
            input_artifact_ids=(
                (profile.metadata_inventory_id,) if profile.metadata_inventory_id else ()
            ),
        )
        persist_artifact_registry(spark, profile_artifact, args.artifact_registry_table)
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
