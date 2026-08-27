"""Create or remove a run-scoped managed UC fixture for integration tests."""

from __future__ import annotations

import argparse
import re


def _identifier(value: str) -> str:
    value = value.strip().lower()
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,40}", value):
        raise SystemExit("fixture suffix must be a simple lowercase identifier")
    return value


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="sda_integration")
    parser.add_argument("--suffix", required=True)
    parser.add_argument("--mode", choices=("create", "cleanup"), required=True)
    args = parser.parse_args()
    suffix = _identifier(args.suffix)
    schema = f"{args.catalog}.{args.schema}"
    prefix = f"{schema}.sda07_{suffix}"
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    if args.mode == "cleanup":
        for name in ("orders", "customers"):
            spark.sql(f"DROP TABLE IF EXISTS {prefix}_{name}")
        spark.sql(f"DROP SCHEMA IF EXISTS {schema}")
        print(f"SDA07_FIXTURE_CLEANED {schema}")
        return
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(
        f"""CREATE OR REPLACE TABLE {prefix}_customers
        (customer_id BIGINT, segment STRING, premium BOOLEAN)
        USING DELTA"""
    )
    spark.sql(
        f"""CREATE OR REPLACE TABLE {prefix}_orders
        (order_id BIGINT, customer_id BIGINT, amount DOUBLE, status STRING,
         created_at TIMESTAMP, completed_at TIMESTAMP, cancel_at TIMESTAMP)
        USING DELTA"""
    )
    spark.sql(
        f"""INSERT INTO {prefix}_customers VALUES
        (1, 'premium', true), (2, 'standard', false), (3, 'premium', true)"""
    )
    spark.sql(
        f"""INSERT INTO {prefix}_orders VALUES
        (101, 1, 100.0, 'new', '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z', NULL),
        (102, 1, 200.0, 'active', '2026-01-02T00:00:00Z', '2026-01-03T00:00:00Z', NULL),
        (103, 2, 20.0, 'new', '2026-01-01T00:00:00Z', '2026-01-01T12:00:00Z', '2026-01-02T00:00:00Z')"""
    )
    print(f"SDA07_FIXTURE_CREATED {prefix}_customers {prefix}_orders")


if __name__ == "__main__":
    main()
