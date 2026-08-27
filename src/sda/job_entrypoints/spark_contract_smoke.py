"""Databricks-native SDA 07 Spark metric contract smoke test."""

from __future__ import annotations

import argparse


def main() -> None:
    from pyspark.sql import SparkSession

    from sda.patterns.spark_metrics import (
        spark_conditional_distribution,
        spark_conditional_missingness,
        spark_fanout_by_segment,
        spark_pearson,
        spark_spearman,
        spark_state_transitions,
        spark_temporal_lag,
        spark_temporal_order,
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--parent-table", required=True)
    parser.add_argument("--child-table", required=True)
    parser.add_argument("--parent-key", default="id")
    parser.add_argument("--child-key", default="customer_id")
    parser.add_argument("--segment", default="segment")
    args = parser.parse_args()
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    source = spark.table(args.source_table)
    fields = {field.name: field.dataType.simpleString() for field in source.schema.fields}
    numeric = [name for name, dtype in fields.items() if dtype in {"double", "float", "int", "bigint", "decimal"}]
    categorical = [name for name, dtype in fields.items() if dtype in {"string", "boolean"}]
    temporal = [name for name in fields if name.lower().endswith(("_at", "_date", "_time", "_ts"))]
    if len(numeric) < 2:
        raise RuntimeError("Spark contract requires at least two numeric columns")
    left, right = numeric[:2]
    for name, frame in (
        ("pearson", spark_pearson(source, left, right)),
        ("spearman", spark_spearman(source, left, right)),
    ):
        if frame.limit(1).count() != 1:
            raise RuntimeError(f"{name} Spark metric returned no aggregate result")
    if categorical:
        driver = categorical[0]
        if spark_conditional_distribution(source, (driver,), right).limit(1).count() < 1:
            raise RuntimeError("conditional distribution returned no aggregate result")
        if spark_conditional_missingness(source, (driver,), right).limit(1).count() < 1:
            raise RuntimeError("conditional missingness returned no aggregate result")
    if len(temporal) >= 2:
        earlier, later = temporal[:2]
        if spark_temporal_order(source, earlier, later).limit(1).count() != 1:
            raise RuntimeError("temporal order returned no aggregate result")
        if spark_temporal_lag(source, earlier, later).limit(1).count() != 1:
            raise RuntimeError("temporal lag returned no aggregate result")
    state = next((name for name in categorical if name.lower() in {"status", "state", "stage"}), None)
    entity = next((name for name in fields if name.lower().endswith(("_id", "id"))), None)
    if state and entity and temporal:
        repeated_entities = (
            source.groupBy(entity).count().where("count > 1").limit(1).count()
        )
        if repeated_entities and spark_state_transitions(
            source, entity_keys=(entity,), state_column=state, event_time=temporal[0]
        ).limit(1).count() < 1:
            raise RuntimeError("state transitions returned no aggregate result")
    parent = spark.table(args.parent_table)
    child = spark.table(args.child_table)
    parent_key = args.parent_key if args.parent_key in parent.columns else next(
        (name for name in parent.columns if name.lower().endswith(("_id", "id"))), None
    )
    child_key = args.child_key if args.child_key in child.columns else next(
        (name for name in child.columns if name.lower().endswith(("_id", "id"))), None
    )
    segment = args.segment if args.segment in parent.columns else next(
        (name for name in parent.columns if name != parent_key), None
    )
    if not parent_key or not child_key or not segment:
        raise RuntimeError("fan-out contract requires resolvable parent key, child key, and segment")
    fanout = spark_fanout_by_segment(
        parent,
        child,
        parent_keys=(parent_key,),
        child_keys=(child_key,),
        segments=(segment,),
    )
    if fanout.limit(1).count() < 1:
        raise RuntimeError("fan-out returned no aggregate result")
    print("SDA07_SPARK_CONTRACT_OK")


if __name__ == "__main__":
    main()
