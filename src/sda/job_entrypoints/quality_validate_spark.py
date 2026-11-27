"""Databricks entrypoint for the SDA 14 quality-validation smoke flow."""

from __future__ import annotations

import argparse
import json

from sda.quality_spark import QualitySparkError, validate_quality_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", default="sda14-smoke")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--baseline-table", required=True)
    parser.add_argument("--candidate-table", required=True)
    parser.add_argument("--report-table", required=True)
    parser.add_argument("--expected-count", default=40, type=int)
    parser.add_argument("--expected-defects", default=4, type=int)
    parser.add_argument("--intended-use", default="qa")
    return parser.parse_args()


def main() -> None:
    from pyspark.sql import SparkSession

    args = parse_args()
    if args.fixture != "sda14-smoke":
        raise SystemExit("only the sda14-smoke fixture is supported by this entrypoint")
    try:
        result = validate_quality_spark(
            SparkSession.builder.getOrCreate(),
            baseline_table=args.baseline_table,
            candidate_table=args.candidate_table,
            report_table=args.report_table,
            run_id=args.run_id,
            expected_count=args.expected_count,
            expected_defects=args.expected_defects,
            intended_use=args.intended_use,
        )
    except QualitySparkError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(result, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
