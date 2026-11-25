"""Databricks entrypoint for the SDA 11 relational smoke flow."""

from __future__ import annotations

import argparse
import json

from sda.relational import RelationalGenerationError
from sda.relational_spark import (
    build_sda11_smoke_relational_plan,
    generate_relational_spark,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", default="sda11-smoke")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--staging-schema", required=True)
    parser.add_argument("--manifest-table", required=True)
    parser.add_argument("--target-catalog", default="workspace")
    parser.add_argument("--target-schema", default="sda_integration")
    parser.add_argument("--plan-fingerprint", default="")
    parser.add_argument("--approved", default="false")
    return parser.parse_args()


def main() -> None:
    from pyspark.sql import SparkSession

    args = parse_args()
    if args.fixture != "sda11-smoke":
        raise SystemExit("only the sda11-smoke fixture is supported by this entrypoint")
    if args.approved.lower() != "true":
        raise SystemExit("relational generation requires approved=true")
    relational, row_counts, fanouts = build_sda11_smoke_relational_plan(
        target_catalog=args.target_catalog,
        target_schema=args.target_schema,
    )
    if args.plan_fingerprint and args.plan_fingerprint != relational.plan.plan_fingerprint:
        raise RelationalGenerationError("approved plan fingerprint does not match the fixture")
    spark = SparkSession.builder.getOrCreate()
    result = generate_relational_spark(
        spark,
        relational,
        row_counts=row_counts,
        staging_schema=args.staging_schema,
        run_id=args.run_id,
        manifest_table=args.manifest_table,
        fanout_distributions=fanouts,
        fanout_mode="exact-total",
    )
    print(json.dumps(result, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
