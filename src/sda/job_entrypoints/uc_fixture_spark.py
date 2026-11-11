"""Create or remove a run-scoped managed UC fixture for integration tests."""

from __future__ import annotations

import argparse
import re
from typing import Protocol


class _SparkSessionLike(Protocol):
    def sql(self, statement: str) -> object: ...


def _identifier(value: str) -> str:
    value = value.strip().lower()
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,40}", value):
        raise SystemExit("fixture suffix must be a simple lowercase identifier")
    return value


def _fixture_prefix(catalog: str, schema: str, suffix: str) -> str:
    """Return the fully qualified prefix owned by one integration run."""

    return f"{catalog}.{schema}.sda07_{suffix}"


def cleanup_fixture(
    spark: _SparkSessionLike, catalog: str, schema: str, suffix: str
) -> None:
    """Drop only the two tables owned by this run-scoped fixture.

    The integration schema is intentionally shared by concurrent or historical
    runs.  Dropping the schema during cleanup therefore makes an otherwise
    successful test destructive and fails as soon as another fixture is present.
    """

    prefix = _fixture_prefix(catalog, schema, suffix)
    for name in ("orders", "customers"):
        spark.sql(f"DROP TABLE IF EXISTS {prefix}_{name}")


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="sda_integration")
    parser.add_argument("--suffix", required=True)
    parser.add_argument("--mode", choices=("create", "cleanup"), required=True)
    args = parser.parse_args()
    catalog = _identifier(args.catalog)
    schema_name = _identifier(args.schema)
    suffix = _identifier(args.suffix)
    schema = f"{catalog}.{schema_name}"
    prefix = _fixture_prefix(catalog, schema_name, suffix)
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    if args.mode == "cleanup":
        cleanup_fixture(spark, catalog, schema_name, suffix)
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
