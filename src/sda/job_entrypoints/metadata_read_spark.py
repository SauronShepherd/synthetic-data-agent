"""Databricks job entrypoint for reading Unity Catalog metadata."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path


def _resolve_src_dir() -> Path | None:
    """Resolve the repository src directory in local and Databricks execution."""

    filename = globals().get("__file__") or globals().get("filename")
    if isinstance(filename, str) and filename:
        return Path(filename).resolve().parents[2]

    frame_filename = sys._getframe().f_code.co_filename  # noqa: SLF001
    if frame_filename:
        path = Path(frame_filename).resolve()
        if path.exists() and len(path.parents) >= 3:
            return path.parents[2]

    cwd_src = Path.cwd() / "src"
    if cwd_src.exists():
        return cwd_src

    return None


def _ensure_src_on_path() -> None:
    src_dir = _resolve_src_dir()
    if src_dir is not None and str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


def main(argv: Sequence[str] | None = None) -> None:
    """Read UC metadata through Spark SQL and print the normalized inventory."""

    _ensure_src_on_path()

    from pyspark.sql import SparkSession

    from sda.artifacts.delta import persist_artifact_registry
    from sda.artifacts.fingerprint import fingerprint
    from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType, SourceReference
    from sda.config import load_settings
    from sda.tools.uc_metadata_reader import read_uc_metadata_with_spark
    from sda.version import __version__

    args = _parse_args(argv)
    _apply_env_overrides(args)
    if not args.run_id:
        args.run_id = f"metadata-read-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"

    settings = load_settings()
    spark = SparkSession.builder.getOrCreate()
    inventory = read_uc_metadata_with_spark(settings.metadata_read_config(), spark)
    payload = inventory.to_dict()
    inventory_id = f"metadata_inventory_{fingerprint(payload)}"
    output_table = os.getenv("SDA_METADATA_OUTPUT_TABLE", "")
    registry_table = args.artifact_registry_table or os.getenv("SDA_ARTIFACT_REGISTRY_TABLE", "")
    if output_table:
        row = {
            "inventory_id": inventory_id,
            "artifact_schema_version": "1.0",
            "created_at": datetime.now(UTC).isoformat(),
            "status": "complete",
            "payload": json.dumps(payload, sort_keys=True),
        }
        frame = spark.createDataFrame([row])
        try:
            from delta.tables import DeltaTable

            target = DeltaTable.forName(spark, output_table)
            target_columns = {column.lower() for column in target.toDF().columns}
            condition = "target.inventory_id = source.inventory_id"
            if "status" in target_columns:
                condition += " AND target.status = source.status"
            values = {
                column: f"source.{column}"
                for column in frame.columns
                if column.lower() in target_columns
            }
            target.alias("target").merge(frame.alias("source"), condition).whenMatchedUpdate(
                set=values
            ).whenNotMatchedInsert(values=values).execute()
        except (ImportError, ModuleNotFoundError):
            frame.write.format("delta").mode("append").saveAsTable(output_table)
        except Exception as exc:
            if "TABLE_OR_VIEW_NOT_FOUND" not in str(exc).upper():
                raise
            frame.write.format("delta").mode("append").saveAsTable(output_table)
        if registry_table:
            artifact = ArtifactRef(
                artifact_id=inventory_id,
                artifact_type=ArtifactType.METADATA_INVENTORY,
                artifact_schema_version="1.0",
                status=ArtifactStatus.COMPLETE,
                tool_name="uc_metadata_reader",
                tool_version=__version__,
                strategy_version="metadata-read-v1",
                run_id=args.run_id,
                environment=args.environment,
                created_at=datetime.now(UTC).isoformat(),
                configuration_hash=fingerprint(vars(args)),
                primary_location=output_table,
                related_locations={"registry": registry_table},
                source_references=(
                    SourceReference("unity_catalog", "METADATA", "metadata_only", None, None, None),
                ),
                checksum=fingerprint(payload),
                summary="Normalized Unity Catalog metadata inventory",
            )
            persist_artifact_registry(spark, artifact, registry_table)
    print(
        json.dumps(
            {**payload, "inventory_id": inventory_id, "output_table": output_table},
            indent=2,
            sort_keys=True,
        )
    )


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read Unity Catalog metadata with Spark SQL.")
    parser.add_argument("--catalog-allowlist", default="main")
    parser.add_argument("--schema-allowlist", default="")
    parser.add_argument("--table-patterns", default="")
    parser.add_argument("--max-metadata-objects", default="100")
    parser.add_argument("--output-table", default="")
    parser.add_argument("--artifact-registry-table", default="")
    parser.add_argument("--run-id", default="metadata-read-local")
    parser.add_argument("--environment", default=os.getenv("DATABRICKS_BUNDLE_TARGET", "dev"))
    return parser.parse_args(argv)


def _apply_env_overrides(args: argparse.Namespace) -> None:
    os.environ["SDA_METADATA_RUNTIME"] = "spark"
    os.environ["SDA_CATALOG_ALLOWLIST"] = args.catalog_allowlist
    os.environ["SDA_SCHEMA_ALLOWLIST"] = args.schema_allowlist
    os.environ["SDA_TABLE_PATTERNS"] = args.table_patterns
    os.environ["SDA_MAX_METADATA_OBJECTS"] = args.max_metadata_objects
    if args.output_table:
        os.environ["SDA_METADATA_OUTPUT_TABLE"] = args.output_table


if __name__ == "__main__":
    main()
