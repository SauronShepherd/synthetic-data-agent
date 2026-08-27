"""Parameterized Spark entrypoint for one SDA-06 relationship analysis."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from typing import Any

from sda.artifacts.delta import persist_artifact_lifecycle
from sda.artifacts.fingerprint import fingerprint
from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType, SourceReference
from sda.relationships.spark_metrics import measure_spark_join
from sda.runtime.identifiers import QualifiedName
from sda.version import __version__


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent-table", required=True)
    parser.add_argument("--child-table", required=True)
    parser.add_argument("--parent-columns", required=True)
    parser.add_argument("--child-columns", required=True)
    parser.add_argument("--output-table", default="")
    parser.add_argument("--artifact-registry-table", default="")
    parser.add_argument("--dry-run", type=lambda value: value.lower() == "true", default=False)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--environment", default="dev")
    return parser.parse_args()


def run(spark: Any, args: argparse.Namespace) -> dict[str, Any]:
    parent_columns = tuple(item.strip() for item in args.parent_columns.split(",") if item.strip())
    child_columns = tuple(item.strip() for item in args.child_columns.split(",") if item.strip())
    parent_name = QualifiedName.parse(args.parent_table)
    child_name = QualifiedName.parse(args.child_table)
    if len(parent_columns) != len(child_columns) or not parent_columns:
        raise ValueError("parent and child column lists must have equal non-zero width")
    evidence = measure_spark_join(
        spark.table(parent_name.quoted),
        spark.table(child_name.quoted),
        parent_columns,
        child_columns,
    )
    run_id = args.run_id or f"relationship-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
    identity = {
        "parent_table": args.parent_table,
        "child_table": args.child_table,
        "parent_columns": parent_columns,
        "child_columns": child_columns,
    }
    record = {
        "analysis_id": f"relationship_analysis_{fingerprint(identity)}",
        "run_id": run_id,
        "parent_table": args.parent_table,
        "child_table": args.child_table,
        "parent_columns": list(parent_columns),
        "child_columns": list(child_columns),
        **evidence,
    }
    if args.output_table:
        registry_table = f"{args.output_table}_registry"
        artifact = ArtifactRef(
            artifact_id=record["analysis_id"],
            artifact_type=ArtifactType.RELATIONSHIP_ANALYSIS,
            artifact_schema_version="1.0",
            status=ArtifactStatus.WRITING,
            tool_name="relationship_detector",
            tool_version=__version__,
            strategy_version="spark-join-v1",
            run_id=run_id,
            environment=args.environment,
            created_at=datetime.now(UTC).isoformat(),
            configuration_hash=fingerprint(identity),
            primary_location=args.output_table,
            related_locations={"registry": registry_table},
            input_artifact_ids=(),
            source_references=(
                SourceReference(args.parent_table, "TABLE", "best_effort", None, None, None),
                SourceReference(args.child_table, "TABLE", "best_effort", None, None, None),
            ),
            checksum=fingerprint(record),
            summary="Spark-native relationship evidence",
            content=record,
        )
        persist_artifact_lifecycle(
            spark,
            artifact,
            [record],
            evidence_location=args.output_table,
            registry_location=args.artifact_registry_table or registry_table,
        )
        if args.artifact_registry_table:
            from sda.artifacts.delta import persist_artifact_registry

            persist_artifact_registry(
                spark,
                artifact.transition(ArtifactStatus.COMPLETE),
                args.artifact_registry_table,
            )
    elif not args.dry_run:
        raise ValueError("--output-table is required unless --dry-run is explicitly set")
    print(json.dumps(record, sort_keys=True))
    return record


def main() -> None:
    from pyspark.sql import SparkSession

    run(SparkSession.getActiveSession() or SparkSession.builder.getOrCreate(), parse_args())


if __name__ == "__main__":
    main()
