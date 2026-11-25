"""Databricks entrypoint for SDA 11 relational structural validation."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

from sda.relational_spark import (
    _manifest_name,
    _staged_table_name,
    build_sda11_smoke_relational_plan,
    validate_relational_spark,
)
from sda.runtime.identifiers import QualifiedName


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", default="sda11-smoke")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--staging-schema", required=True)
    parser.add_argument("--manifest-table", required=True)
    parser.add_argument("--report-table", required=True)
    parser.add_argument("--target-catalog", default="workspace")
    parser.add_argument("--target-schema", default="sda_integration")
    parser.add_argument("--approve", default="false")
    return parser.parse_args()


def main() -> None:
    from pyspark.sql import SparkSession

    args = parse_args()
    if args.fixture != "sda11-smoke":
        raise SystemExit("only the sda11-smoke fixture is supported by this entrypoint")
    relational, expected_counts, _ = build_sda11_smoke_relational_plan(
        target_catalog=args.target_catalog,
        target_schema=args.target_schema,
    )
    spark = SparkSession.builder.getOrCreate()
    table_names = {
        table: _staged_table_name(args.staging_schema, table, args.run_id).full_name
        for table in relational.plan.tables
    }
    report = validate_relational_spark(
        spark,
        relational,
        table_names=table_names,
        expected_counts=expected_counts,
        fanout_bounds={("orders", "customer_id", "customers", "customer_id"): (0, 6)},
    )
    report_name = QualifiedName.parse(args.report_table)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{report_name.catalog}`.`{report_name.schema}`")
    report_row = {
        "run_id": args.run_id,
        "created_at": datetime.now(UTC).isoformat(),
        "technical_disposition": report["status"],
        "report_fingerprint": report["fingerprint"],
        "report_json": json.dumps(report, sort_keys=True, default=str),
    }
    if spark.catalog.tableExists(report_name.full_name):
        run_literal = args.run_id.replace("'", "''")
        spark.sql(f"DELETE FROM {report_name.quoted} WHERE run_id = '{run_literal}'")
    spark.createDataFrame([report_row]).write.format("delta").mode("append").saveAsTable(
        report_name.quoted
    )
    manifest_name = _manifest_name(args.manifest_table, args.run_id)
    manifest_status = None
    if spark.catalog.tableExists(manifest_name.full_name):
        manifest_columns = set(spark.table(manifest_name.quoted).columns)
        missing_columns = {
            column: data_type
            for column, data_type in {
                "schema_version": "STRING",
                "validation_report_fingerprint": "STRING",
                "approved_by": "STRING",
                "relationship_metrics_json": "STRING",
            }.items()
            if column not in manifest_columns
        }
        for column, data_type in missing_columns.items():
            spark.sql(
                f"ALTER TABLE {manifest_name.quoted} ADD COLUMNS ({column} {data_type})"
            )
        existing = spark.table(manifest_name.quoted).limit(1).first()
        if existing is not None:
            status = "VALIDATED" if report["status"] == "PASS" else "GENERATED_AWAITING_VALIDATION"
            status_literal = status.replace("'", "''")
            fingerprint_literal = str(report["fingerprint"]).replace("'", "''")
            relationship_metrics = {
                check["check_id"]: check
                for check in report["checks"]
                if check["check_id"].startswith(
                    ("foreign_key:", "cardinality:", "bridge_pairs:", "fanout:")
                )
            }
            metrics_literal = json.dumps(relationship_metrics, sort_keys=True).replace("'", "''")
            updates = [
                f"status = '{status_literal}'",
                f"validation_report_fingerprint = '{fingerprint_literal}'",
                f"relationship_metrics_json = '{metrics_literal}'",
            ]
            if args.approve.lower() == "true" and report["status"] == "PASS":
                updates[0] = "status = 'APPROVED'"
                updates.append("approved_by = 'explicit-job-parameter'")
                status = "APPROVED"
            run_literal = args.run_id.replace("'", "''")
            spark.sql(
                f"UPDATE {manifest_name.quoted} SET {', '.join(updates)} "
                f"WHERE run_id = '{run_literal}'"
            )
            manifest_status = status
    output = {**report, "manifest_status": manifest_status}
    print(json.dumps(output, sort_keys=True, default=str))
    if report["status"] != "PASS":
        raise SystemExit("relational output failed structural validation")


if __name__ == "__main__":
    main()
