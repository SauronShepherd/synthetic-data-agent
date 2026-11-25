"""Databricks smoke job for SDA 11 fail-closed and validator error paths."""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from typing import Any

from sda.planning import ColumnGenerationSpec, GenerationPlan, PlanStatus
from sda.relational import (
    ForeignKeySpec,
    KeyDomainSpec,
    RelationalGenerationError,
    build_relational_plan,
)
from sda.relational_spark import (
    _staged_table_name,
    build_sda11_smoke_relational_plan,
    generate_relational_spark,
    validate_relational_spark,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", default="sda11-errors")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--staging-schema", required=True)
    parser.add_argument("--target-catalog", default="workspace")
    parser.add_argument("--target-schema", default="sda_integration")
    return parser.parse_args()


def _expect_error(label: str, operation: Callable[[], Any]) -> dict[str, str]:
    try:
        operation()
    except (RelationalGenerationError, ValueError) as exc:
        return {"case": label, "status": "PASS", "error_type": type(exc).__name__}
    return {"case": label, "status": "FAIL", "error_type": "no error raised"}


def _simple_plan(*, incompatible: bool = False):
    child_type = "int" if incompatible else "string"
    columns = (
        ColumnGenerationSpec("parents", "parent_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("children", "child_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("children", "parent_id", child_type, nullable=False, model="string"),
    )
    plan = GenerationPlan(
        plan_id="sda11-error-smoke",
        plan_version=1,
        request_id="sda11-error-smoke",
        source_snapshot_ids=("sda11-error-fixture",),
        input_artifact_ids=("sda11-error-graph",),
        target_catalog="workspace",
        target_schema="sda_integration",
        tables=("parents", "children"),
        columns=columns,
        budgets={"max_rows": 20},
    ).transition(PlanStatus.AWAITING_APPROVAL).transition(PlanStatus.APPROVED)
    foreign_key = ForeignKeySpec("children", "parent_id", "parents", "parent_id")
    artifact = {
        "status": "complete",
        "plan_fingerprint": plan.plan_fingerprint,
        "relationships": [
            {
                "child_table": "children",
                "child_columns": ["parent_id"],
                "parent_table": "parents",
                "parent_columns": ["parent_id"],
                "decision": "accepted",
                "accepted_for_graph": True,
                "validation_status": "complete",
                "review_status": "not_required",
            }
        ],
    }
    return build_relational_plan(
        plan,
        foreign_keys=(foreign_key,),
        key_domains=(
            KeyDomainSpec("parents", ("parent_id",)),
            KeyDomainSpec("children", ("child_id",)),
        ),
        relationship_artifact=artifact,
    )


def _write_delta(spark: Any, name: Any, rows: list[tuple[str, ...]], columns: list[str]) -> None:
    schema = "struct<" + ",".join(f"{column}:string" for column in columns) + ">"
    frame = spark.createDataFrame(rows, schema)
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        name.quoted
    )


def main() -> None:
    from pyspark.sql import SparkSession

    args = parse_args()
    if args.fixture != "sda11-errors":
        raise SystemExit("only the sda11-errors fixture is supported by this entrypoint")

    relational, row_counts, fanouts = build_sda11_smoke_relational_plan(
        target_catalog=args.target_catalog,
        target_schema=args.target_schema,
    )
    no_cycle_strategy = build_relational_plan(
        relational.plan,
        foreign_keys=relational.foreign_keys,
        composite_foreign_keys=relational.composite_foreign_keys,
        key_domains=relational.key_domains,
        bridge_tables=relational.bridge_tables,
        self_references=relational.self_references,
        relationship_artifact=relational.relationship_artifact,
    )
    rejected_artifact = dict(relational.relationship_artifact or {})
    rejected_edges = [dict(edge) for edge in rejected_artifact.get("relationships", [])]
    rejected_edges[0]["decision"] = "rejected"
    rejected_edges[0]["accepted_for_graph"] = False
    rejected_edges[0]["review_status"] = "rejected"
    rejected_artifact["relationships"] = rejected_edges
    stale_artifact = dict(relational.relationship_artifact or {})
    stale_artifact["status"] = "stale"

    spark = SparkSession.builder.getOrCreate()
    results = [
        _expect_error(
            "cycle_without_approved_strategy",
            lambda: generate_relational_spark(
                spark,
                no_cycle_strategy,
                row_counts=row_counts,
                staging_schema=args.staging_schema,
                run_id=f"{args.run_id}-cycle",
                manifest_table=f"{args.target_catalog}.profiles.relational_error_manifest",
                fanout_distributions=fanouts,
                fanout_mode="exact-total",
            ),
        ),
        _expect_error(
            "rejected_relationship_artifact",
            lambda: build_relational_plan(
                relational.plan,
                foreign_keys=relational.foreign_keys,
                composite_foreign_keys=relational.composite_foreign_keys,
                key_domains=relational.key_domains,
                bridge_tables=relational.bridge_tables,
                self_references=relational.self_references,
                cycle_strategies=relational.cycle_strategies,
                relationship_artifact=rejected_artifact,
            ),
        ),
        _expect_error(
            "stale_relationship_artifact",
            lambda: build_relational_plan(
                relational.plan,
                foreign_keys=relational.foreign_keys,
                composite_foreign_keys=relational.composite_foreign_keys,
                key_domains=relational.key_domains,
                bridge_tables=relational.bridge_tables,
                self_references=relational.self_references,
                cycle_strategies=relational.cycle_strategies,
                relationship_artifact=stale_artifact,
            ),
        ),
        _expect_error(
            "incompatible_key_types",
            lambda: generate_relational_spark(
                spark,
                _simple_plan(incompatible=True),
                row_counts={"parents": 1, "children": 1},
                staging_schema=args.staging_schema,
                run_id=f"{args.run_id}-types",
                manifest_table=f"{args.target_catalog}.profiles.relational_error_manifest",
            ),
        ),
    ]

    simple = _simple_plan()
    created = []
    try:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS `{args.staging_schema.split('.')[0]}`.`{args.staging_schema.split('.')[1]}`"
        )
        duplicate_parent = _staged_table_name(args.staging_schema, "sda11_error_duplicate_parents", args.run_id)
        duplicate_child = _staged_table_name(args.staging_schema, "sda11_error_duplicate_children", args.run_id)
        orphan_parent = _staged_table_name(args.staging_schema, "sda11_error_orphan_parents", args.run_id)
        orphan_child = _staged_table_name(args.staging_schema, "sda11_error_orphan_children", args.run_id)
        created = [duplicate_parent, duplicate_child, orphan_parent, orphan_child]
        _write_delta(spark, duplicate_parent, [("p1",), ("p1",)], ["parent_id"])
        _write_delta(spark, duplicate_child, [("c1", "p1")], ["child_id", "parent_id"])
        duplicate_report = validate_relational_spark(
            spark,
            simple,
            table_names={"parents": duplicate_parent.full_name, "children": duplicate_child.full_name},
            expected_counts={"parents": 2, "children": 1},
        )
        results.append(
            {
                "case": "duplicate_primary_key",
                "status": "PASS" if duplicate_report["status"] == "FAIL" else "FAIL",
            }
        )

        _write_delta(spark, orphan_parent, [("p1",)], ["parent_id"])
        _write_delta(
            spark,
            orphan_child,
            [("c1", "p1"), ("c2", "missing")],
            ["child_id", "parent_id"],
        )
        orphan_report = validate_relational_spark(
            spark,
            simple,
            table_names={"parents": orphan_parent.full_name, "children": orphan_child.full_name},
            expected_counts={"parents": 1, "children": 2},
        )
        results.append(
            {
                "case": "orphan_foreign_key",
                "status": "PASS" if orphan_report["status"] == "FAIL" else "FAIL",
            }
        )
    finally:
        for name in created:
            spark.sql(f"DROP TABLE IF EXISTS {name.quoted}")

    failed = [result for result in results if result["status"] != "PASS"]
    output = {"status": "FAIL" if failed else "PASS", "cases": results, "cleanup": "PASS"}
    print(json.dumps(output, sort_keys=True))
    if failed:
        raise SystemExit("one or more SDA 11 error smoke cases did not fail closed")


if __name__ == "__main__":
    main()
