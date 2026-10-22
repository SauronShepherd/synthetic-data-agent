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
        for name in (
            "orders",
            "customers",
            "missingness_stability",
            "fanout_stability_parent",
            "fanout_stability_child",
        ):
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
    spark.sql(
        f"""CREATE OR REPLACE TABLE {prefix}_missingness_stability USING DELTA AS
        WITH rows AS (
          SELECT id, CAST(FLOOR(id / 200) AS INT) AS month_index,
                 CASE WHEN id % 200 < 100 THEN 'west' ELSE 'east' END AS region,
                 CAST(id % 100 AS INT) AS row_index
          FROM range(600)
        )
        SELECT region,
               ADD_MONTHS(TIMESTAMP '2026-01-01 00:00:00', month_index) AS event_time,
               CASE
                 WHEN region = 'west' AND row_index <
                   CASE month_index WHEN 0 THEN 80 WHEN 1 THEN 50 ELSE 20 END THEN NULL
                 WHEN region = 'east' AND row_index < 10 THEN NULL
                 ELSE CONCAT('value-', CAST(row_index AS STRING))
               END AS nullable_value
        FROM rows"""
    )
    spark.sql(
        f"""CREATE OR REPLACE TABLE {prefix}_fanout_stability_parent USING DELTA AS
        SELECT CONCAT(CAST(month_index AS STRING), '-', CAST(row_index AS STRING)) AS parent_id,
               'west' AS region,
               ADD_MONTHS(TIMESTAMP '2026-01-01 00:00:00', month_index) AS event_time
        FROM (
          SELECT CAST(FLOOR(id / 10) AS INT) AS month_index,
                 CAST(id % 10 AS INT) AS row_index
          FROM range(30)
        )"""
    )
    spark.sql(
        f"""CREATE OR REPLACE TABLE {prefix}_fanout_stability_child USING DELTA AS
        WITH parent_counts AS (
          SELECT CONCAT(CAST(month_index AS STRING), '-', CAST(row_index AS STRING)) AS parent_id,
                 CASE
                   WHEN row_index < (month_index * 5) THEN 0
                   ELSE month_index + 1
                 END AS child_count
          FROM (
            SELECT CAST(FLOOR(id / 10) AS INT) AS month_index,
                   CAST(id % 10 AS INT) AS row_index
            FROM range(30)
          )
        )
        SELECT parent_id
        FROM parent_counts
        LATERAL VIEW EXPLODE(SEQUENCE(1, child_count)) expanded AS child_number
        WHERE child_count > 0"""
    )
    print(
        f"SDA07_FIXTURE_CREATED {prefix}_customers {prefix}_orders "
        f"{prefix}_missingness_stability {prefix}_fanout_stability_parent "
        f"{prefix}_fanout_stability_child"
    )


if __name__ == "__main__":
    main()
