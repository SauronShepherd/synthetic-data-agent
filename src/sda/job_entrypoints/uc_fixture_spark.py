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
            "temporal_order_stability",
            "temporal_lag_stability",
            "transition_stability",
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
    spark.sql(
        f"""CREATE OR REPLACE TABLE {prefix}_temporal_order_stability USING DELTA AS
        WITH rows AS (
          SELECT CAST(FLOOR(id / 200) AS INT) AS month_index,
                 CASE WHEN id % 200 < 100 THEN 'west' ELSE 'east' END AS region,
                 CAST(id % 100 AS INT) AS row_index
          FROM range(600)
        ),
        dated AS (
          SELECT region, row_index,
                 ADD_MONTHS(TIMESTAMP '2026-01-01 00:00:00', month_index) AS event_time
          FROM rows
        )
        SELECT region, event_time, event_time AS created_at,
               CASE
                 WHEN row_index < CASE
                   WHEN region = 'west' THEN CASE month(event_time)
                     WHEN 1 THEN 10 WHEN 2 THEN 50 ELSE 90 END
                   ELSE 10 END
                 THEN event_time - INTERVAL 1 DAY
                 ELSE event_time + INTERVAL 1 DAY
               END AS closed_at
        FROM dated"""
    )
    spark.sql(
        f"""CREATE OR REPLACE TABLE {prefix}_temporal_lag_stability USING DELTA AS
        WITH rows AS (
          SELECT CAST(FLOOR(id / 100) AS INT) AS month_index
          FROM range(300)
        ), dated AS (
          SELECT ADD_MONTHS(TIMESTAMP '2026-01-01 00:00:00', month_index) AS event_time,
                 month_index
          FROM rows
        )
        SELECT event_time, event_time AS started_at,
               TIMESTAMPADD(DAY,
                 CASE month_index WHEN 0 THEN 1 WHEN 1 THEN 10 ELSE 30 END,
                 event_time) AS finished_at
        FROM dated"""
    )
    spark.sql(
        f"""CREATE OR REPLACE TABLE {prefix}_transition_stability USING DELTA AS
        WITH base AS (
          SELECT CAST(FLOOR(id / 30) AS INT) AS month_index,
                 CAST(id % 30 AS INT) AS entity_index
          FROM range(90)
        )
        SELECT CONCAT(CAST(month_index AS STRING), '-', CAST(entity_index AS STRING)) AS entity_id,
               CONCAT('source-', CAST(entity_index % 2 AS STRING)) AS source_system,
               CONCAT('partition-', CAST(month_index AS STRING)) AS partition_name,
               CASE month_index WHEN 0 THEN 'open' ELSE 'open' END AS status,
               ADD_MONTHS(TIMESTAMP '2026-01-01 00:00:00', month_index) AS event_time,
               (month_index * 1000) + (entity_index * 2) AS event_id
        FROM base
        UNION ALL
        SELECT CONCAT(CAST(month_index AS STRING), '-', CAST(entity_index AS STRING)) AS entity_id,
               CONCAT('source-', CAST(entity_index % 2 AS STRING)) AS source_system,
               CONCAT('partition-', CAST(month_index AS STRING)) AS partition_name,
               CASE month_index WHEN 1 THEN 'open' ELSE 'closed' END AS status,
               TIMESTAMPADD(
                 DAY,
                 CASE month_index WHEN 0 THEN 1 WHEN 1 THEN 1 ELSE 10 END,
                 ADD_MONTHS(TIMESTAMP '2026-01-01 00:00:00', month_index)
               ) AS event_time,
               (month_index * 1000) + (entity_index * 2) + 1 AS event_id
        FROM base"""
    )
    print(
        f"SDA07_FIXTURE_CREATED {prefix}_customers {prefix}_orders "
        f"{prefix}_missingness_stability {prefix}_fanout_stability_parent "
        f"{prefix}_fanout_stability_child {prefix}_temporal_order_stability "
        f"{prefix}_temporal_lag_stability {prefix}_transition_stability"
    )


if __name__ == "__main__":
    main()
