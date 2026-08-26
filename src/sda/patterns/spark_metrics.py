"""Spark-native SDA 07 metric adapters.

These functions intentionally return DataFrames or bounded aggregate DataFrames;
they never collect source rows to the driver.
"""

from __future__ import annotations

from functools import reduce
from operator import and_
from typing import Any


def _functions() -> Any:
    from pyspark.sql import functions as F

    return F


def spark_pearson(frame: Any, left: str, right: str) -> Any:
    F = _functions()
    numeric_types = (
        "ByteType",
        "ShortType",
        "IntegerType",
        "LongType",
        "FloatType",
        "DoubleType",
        "DecimalType",
    )
    fields = {field.name: field.dataType.__class__.__name__ for field in frame.schema.fields}
    unsupported = [column for column in (left, right) if fields.get(column) not in numeric_types]
    if unsupported:
        raise ValueError(
            "spark_pearson requires numeric columns; unsupported columns: " + ", ".join(unsupported)
        )
    valid = frame.where(F.col(left).isNotNull() & F.col(right).isNotNull())
    return (
        valid.agg(
            F.count(F.lit(1)).alias("valid_pair_count"),
            F.count(F.lit(1)).alias("population_count"),
            F.corr(F.col(left), F.col(right)).alias("value"),
        )
        .withColumn("method", F.lit("pearson"))
        .withColumn("validation_mode", F.lit("exact"))
    )


def spark_conditional_distribution(frame: Any, drivers: tuple[str, ...], outcome: str) -> Any:
    F = _functions()
    if not drivers:
        raise ValueError("at least one driver column is required")
    grouped = frame.groupBy(*(F.col(column) for column in drivers), F.col(outcome)).count()
    totals = (
        frame.groupBy(*(F.col(column) for column in drivers))
        .count()
        .withColumnRenamed("count", "support_rows")
    )
    join_columns = [F.col(f"groups.{column}") == F.col(f"totals.{column}") for column in drivers]
    return (
        grouped.alias("groups")
        .join(totals.alias("totals"), reduce(and_, join_columns), "left")
        .select(
            *[F.col(f"groups.{column}").alias(column) for column in drivers],
            F.col(f"groups.{outcome}").alias(outcome),
            F.col("groups.count").alias("outcome_count"),
            F.col("totals.support_rows"),
        )
        .withColumn("conditional_rate", F.col("outcome_count") / F.col("support_rows"))
    )


def spark_conditional_missingness(frame: Any, drivers: tuple[str, ...], outcome: str) -> Any:
    F = _functions()
    return (
        frame.groupBy(*drivers)
        .agg(
            F.count(F.lit(1)).alias("support_rows"),
            F.sum(F.when(F.col(outcome).isNull(), 1).otherwise(0)).alias("null_rows"),
        )
        .withColumn("null_rate", F.col("null_rows") / F.col("support_rows"))
    )


def spark_fanout_by_segment(
    parent: Any,
    child: Any,
    *,
    parent_keys: tuple[str, ...],
    child_keys: tuple[str, ...],
    segments: tuple[str, ...],
) -> Any:
    F = _functions()
    if len(parent_keys) != len(child_keys) or not parent_keys:
        raise ValueError("parent and child key widths must match")
    if not segments:
        raise ValueError("fan-out metrics require at least one segment column")
    counts = child.groupBy(*child_keys).agg(F.count(F.lit(1)).alias("child_count"))
    condition = [
        F.col(f"parent.{left}") == F.col(f"counts.{right}")
        for left, right in zip(parent_keys, child_keys, strict=True)
    ]
    population = (
        parent.alias("parent")
        .join(counts.alias("counts"), reduce(and_, condition), "left")
        .select(
            *[F.col(f"parent.{segment}").alias(segment) for segment in segments],
            F.coalesce(F.col("counts.child_count"), F.lit(0)).alias("child_count"),
        )
    )
    return (
        population.groupBy(*segments)
        .agg(
            F.count(F.lit(1)).alias("parent_count"),
            F.sum(F.when(F.col("child_count") == 0, 1).otherwise(0)).alias("zero_child_count"),
            F.avg("child_count").alias("mean_child_count"),
            F.expr(
                "percentile_approx(child_count, array(0.25, 0.5, 0.75, 0.95, 0.99), 10000)"
            ).alias("child_count_percentiles"),
            F.max("child_count").alias("max_child_count"),
        )
        .withColumn("zero_child_rate", F.col("zero_child_count") / F.col("parent_count"))
    )


def spark_temporal_order(frame: Any, earlier: str, later: str) -> Any:
    F = _functions()
    eligible = frame.where(F.col(earlier).isNotNull() & F.col(later).isNotNull())
    return eligible.agg(
        F.count(F.lit(1)).alias("eligible_rows"),
        F.sum(F.when(F.col(earlier) <= F.col(later), 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.col(earlier) > F.col(later), 1).otherwise(0)).alias("violation_rows"),
        F.sum(F.when(F.col(earlier) == F.col(later), 1).otherwise(0)).alias("equal_rows"),
    ).withColumn("violation_rate", F.col("violation_rows") / F.col("eligible_rows"))


def spark_state_transitions(
    frame: Any,
    *,
    entity_keys: tuple[str, ...],
    state_column: str,
    event_time: str,
    tie_breakers: tuple[str, ...] = (),
) -> Any:
    F = _functions()
    from pyspark.sql.window import Window

    ordering = [
        F.col(event_time).asc_nulls_last(),
        *(F.col(column).asc_nulls_last() for column in tie_breakers),
    ]
    if not tie_breakers:
        # Equal event times are retained as ambiguous evidence rather than ordered arbitrarily.
        duplicate_times = frame.groupBy(*entity_keys, event_time).count().where(F.col("count") > 1)
        if duplicate_times.limit(1).count():
            raise ValueError("temporal_tie_breaker_missing")
    window = Window.partitionBy(*entity_keys).orderBy(*ordering)
    transitions = frame.withColumn("to_state", F.lead(F.col(state_column)).over(window)).where(
        F.col("to_state").isNotNull()
    )
    counts = transitions.groupBy(F.col(state_column).alias("from_state"), F.col("to_state")).count()
    outgoing = counts.groupBy("from_state").agg(F.sum("count").alias("outgoing_count"))
    return (
        counts.join(outgoing, "from_state")
        .withColumn("transition_probability", F.col("count") / F.col("outgoing_count"))
        .withColumn("self_transition", F.col("from_state") == F.col("to_state"))
    )
