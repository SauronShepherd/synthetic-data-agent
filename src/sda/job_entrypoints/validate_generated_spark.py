"""Bounded Spark validation entrypoint for generated Delta output."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from datetime import UTC, datetime

from sda.runtime.identifiers import QualifiedName
from sda.validation import CheckStatus, validate_tables


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-table", required=True)
    parser.add_argument("--report-table", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-count", type=int, required=True)
    parser.add_argument("--required-columns", default="")
    parser.add_argument("--intended-use", default="qa")
    parser.add_argument("--max-rows", type=int, default=100_000)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    if args.expected_count < 0 or args.max_rows < 1 or args.expected_count > args.max_rows:
        raise SystemExit("expected-count must be within max-rows")
    input_name = QualifiedName.parse(args.input_table)
    report_name = QualifiedName.parse(args.report_table)
    spark = SparkSession.builder.getOrCreate()
    frame = spark.table(input_name.quoted).limit(args.max_rows)
    rows = tuple(row.asDict(recursive=True) for row in frame.collect())
    table_key = input_name.full_name
    required = tuple(column.strip() for column in args.required_columns.split(",") if column.strip())
    report = validate_tables(
        {table_key: rows},
        expected_counts={table_key: args.expected_count},
        required_columns={table_key: required} if required else None,
        intended_use=args.intended_use,
        validation_vector={"schema": CheckStatus.PASS, "completeness": CheckStatus.PASS},
    )
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{report_name.catalog}`.`{report_name.schema}`")
    payload = report.to_dict()
    row = {
        "run_id": args.run_id,
        "input_table": table_key,
        "created_at": datetime.now(UTC).isoformat(),
        "technical_disposition": report.technical_disposition.value,
        "report_fingerprint": report.fingerprint,
        "report_json": json.dumps(payload, sort_keys=True, default=str),
    }
    spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable(report_name.quoted)
    print(json.dumps(payload, sort_keys=True))
    if report.technical_disposition is not CheckStatus.PASS:
        raise SystemExit("generated output failed technical validation")


if __name__ == "__main__":
    main()
