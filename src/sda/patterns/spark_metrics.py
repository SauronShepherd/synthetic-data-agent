"""Spark-native SDA 07 metric adapters.

These functions intentionally return DataFrames or bounded aggregate DataFrames;
they never collect source rows to the driver.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from operator import and_
from typing import Any, cast


@dataclass(frozen=True, slots=True)
class UnsupportedMetricResult:
    """Raw-value-free, actionable result for an unavailable Spark metric."""

    metric: str
    reason: str
    supported: bool = False
    schema_version: str = "spark-metric-result-v1"

    def __post_init__(self) -> None:
        if not self.metric.strip() or not self.reason.strip():
            raise ValueError("unsupported metric identity and reason are required")
        if self.supported:
            raise ValueError("unsupported metric results must set supported=false")

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric,
            "supported": self.supported,
            "reason": self.reason,
            "schema_version": self.schema_version,
        }


def unsupported_metric_result(metric: str, reason: str) -> UnsupportedMetricResult:
    return UnsupportedMetricResult(metric, reason)


def spark_metric(frame: Any, metric: str, **kwargs: Any) -> Any:
    """Dispatch a configured metric or return an actionable unsupported result."""
    metric = metric.strip().lower()
    if not metric:
        return unsupported_metric_result("unknown", "metric name must not be empty")
    handlers = {
        "pearson": spark_pearson,
        "spearman": spark_spearman,
        "conditional_distribution": spark_conditional_distribution,
        "numeric_by_group": spark_numeric_by_group,
        "conditional_missingness": spark_conditional_missingness,
        "temporal_order": spark_temporal_order,
        "temporal_lag": spark_temporal_lag,
        "state_transitions": spark_state_transitions,
        "fanout_by_segment": spark_fanout_by_segment,
    }
    handler = cast(Any, handlers.get(metric))
    if handler is None:
        return unsupported_metric_result(metric, "metric is not implemented by the Spark adapter")
    required_by_metric = {
        "spearman": ("left", "right"),
        "pearson": ("left", "right"),
        "temporal_lag": ("earlier", "later"),
    }
    missing_kwargs = [name for name in required_by_metric.get(metric, ()) if name not in kwargs]
    if missing_kwargs:
        return unsupported_metric_result(
            metric, "missing required arguments: " + ", ".join(missing_kwargs)
        )
    try:
        return handler(frame, **kwargs)
    except (TypeError, ValueError) as exc:
        return unsupported_metric_result(metric, str(exc))


def _functions() -> Any:
    from pyspark.sql import functions as F

    return F


def _require_columns(frame: Any, columns: tuple[str, ...], *, metric: str) -> None:
    available = {field.name for field in frame.schema.fields}
    missing = [column for column in columns if column not in available]
    if missing:
        raise ValueError(f"{metric} requires columns: {', '.join(missing)}")


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


def spark_spearman(frame: Any, left: str, right: str) -> Any:
    """Compute Spearman using SQL window ranks (safe on Databricks Connect)."""
    _require_columns(frame, (left, right), metric="spark_spearman")
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    valid = frame.where(F.col(left).isNotNull() & F.col(right).isNotNull())
    ranked = valid.withColumn(
        "__sda_left_rank", F.percent_rank().over(Window.orderBy(F.col(left)))
    ).withColumn("__sda_right_rank", F.percent_rank().over(Window.orderBy(F.col(right))))
    return (
        ranked.agg(
            F.count(F.lit(1)).alias("valid_pair_count"),
            F.count(F.lit(1)).alias("population_count"),
            F.corr(F.col("__sda_left_rank"), F.col("__sda_right_rank")).alias("value"),
        )
        .withColumn("method", F.lit("spearman"))
        .withColumn("validation_mode", F.lit("exact"))
    )


def spark_correlation_outlier_diagnostic(frame: Any, left: str, right: str) -> Any:
    """Compare correlation on the full population with a bounded central slice."""
    _require_columns(frame, (left, right), metric="spark_correlation_outlier_diagnostic")
    from pyspark.sql import functions as F

    valid = frame.where(F.col(left).isNotNull() & F.col(right).isNotNull())
    quantiles = valid.approxQuantile([left, right], [0.01, 0.99], 0.01)
    if len(quantiles) != 2 or any(len(values) != 2 for values in quantiles):
        return valid.limit(1).select(
            F.lit(None).cast("double").alias("full_value"),
            F.lit(None).cast("double").alias("trimmed_value"),
            F.lit(False).alias("sign_changed"),
        )
    (left_low, left_high), (right_low, right_high) = quantiles
    full = valid.agg(F.corr(left, right).alias("full_value"))
    central = valid.where(
        (F.col(left) >= F.lit(left_low))
        & (F.col(left) <= F.lit(left_high))
        & (F.col(right) >= F.lit(right_low))
        & (F.col(right) <= F.lit(right_high))
    ).agg(F.corr(left, right).alias("trimmed_value"))
    return full.crossJoin(central).withColumn(
        "sign_changed",
        F.col("full_value").isNotNull()
        & F.col("trimmed_value").isNotNull()
        & (F.col("full_value") * F.col("trimmed_value") < 0),
    )


def spark_conditional_distribution(frame: Any, drivers: tuple[str, ...], outcome: str) -> Any:
    if not drivers:
        raise ValueError("at least one driver column is required")
    _require_columns(frame, (*drivers, outcome), metric="spark_conditional_distribution")
    F = _functions()
    grouped = frame.groupBy(*(F.col(column) for column in drivers), F.col(outcome)).count()
    totals = (
        frame.groupBy(*(F.col(column) for column in drivers))
        .count()
        .withColumnRenamed("count", "support_rows")
    )
    baseline = frame.groupBy(F.col(outcome)).count().withColumnRenamed("count", "global_count")
    join_columns = [
        F.col(f"groups.{column}").eqNullSafe(F.col(f"totals.{column}")) for column in drivers
    ]
    return (
        grouped.alias("groups")
        .join(totals.alias("totals"), reduce(and_, join_columns), "left")
        .join(
            baseline.alias("baseline"),
            F.col(f"groups.{outcome}").eqNullSafe(F.col(f"baseline.{outcome}")),
            "left",
        )
        .select(
            *[F.col(f"groups.{column}").alias(column) for column in drivers],
            F.col(f"groups.{outcome}").alias(outcome),
            F.col("groups.count").alias("outcome_count"),
            F.col("totals.support_rows"),
            F.col("baseline.global_count"),
        )
        .withColumn("conditional_rate", F.col("outcome_count") / F.col("support_rows"))
    )


def spark_numeric_by_group(frame: Any, drivers: tuple[str, ...], outcome: str) -> Any:
    """Numeric conditional summaries with a global non-null outcome baseline."""
    if not drivers:
        raise ValueError("at least one driver column is required")
    _require_columns(frame, (*drivers, outcome), metric="spark_numeric_by_group")
    F = _functions()
    numeric_types = {
        "ByteType",
        "ShortType",
        "IntegerType",
        "LongType",
        "FloatType",
        "DoubleType",
        "DecimalType",
    }
    field = next(field for field in frame.schema.fields if field.name == outcome)
    if field.dataType.__class__.__name__ not in numeric_types:
        raise ValueError("spark_numeric_by_group requires a numeric outcome")
    valid = frame.where(F.col(outcome).isNotNull())
    aggregates = (
        F.count(F.lit(1)).alias("count"),
        F.expr(f"percentile_approx(`{outcome.replace('`', '``')}`, 0.5, 10000)").alias("p50"),
        F.min(F.col(outcome)).alias("min"),
        F.max(F.col(outcome)).alias("max"),
        F.avg(F.col(outcome)).alias("mean"),
    )
    groups = valid.groupBy(*(F.col(column) for column in drivers)).agg(*aggregates)
    baseline = valid.agg(*aggregates).select(
        F.col("count").alias("global_count"),
        F.col("p50").alias("global_p50"),
        F.col("min").alias("global_min"),
        F.col("max").alias("global_max"),
        F.col("mean").alias("global_mean"),
    )
    return groups.crossJoin(baseline)


def spark_conditional_missingness(frame: Any, drivers: tuple[str, ...], outcome: str) -> Any:
    F = _functions()
    if not drivers:
        raise ValueError("at least one driver column is required")
    _require_columns(frame, (*drivers, outcome), metric="spark_conditional_missingness")
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
    _require_columns(parent, (*parent_keys, *segments), metric="spark_fanout_by_segment parent")
    _require_columns(child, child_keys, metric="spark_fanout_by_segment child")
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
    _require_columns(frame, (earlier, later), metric="spark_temporal_order")
    eligible = frame.where(F.col(earlier).isNotNull() & F.col(later).isNotNull())
    return eligible.agg(
        F.count(F.lit(1)).alias("eligible_rows"),
        F.sum(F.when(F.col(earlier) <= F.col(later), 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.col(earlier) > F.col(later), 1).otherwise(0)).alias("violation_rows"),
        F.sum(F.when(F.col(earlier) == F.col(later), 1).otherwise(0)).alias("equal_rows"),
    ).withColumn("violation_rate", F.col("violation_rows") / F.col("eligible_rows"))


def spark_temporal_lag(frame: Any, earlier: str, later: str) -> Any:
    """Return bounded lag distribution aggregates without collecting timestamps."""
    _require_columns(frame, (earlier, later), metric="spark_temporal_lag")
    F = _functions()
    earlier_ts = F.col(earlier).cast("timestamp")
    later_ts = F.col(later).cast("timestamp")
    eligible = frame.where(earlier_ts.isNotNull() & later_ts.isNotNull())
    lag_seconds = F.col("__sda_later_ts").cast("long") - F.col("__sda_earlier_ts").cast("long")
    return (
        eligible.select(earlier_ts.alias("__sda_earlier_ts"), later_ts.alias("__sda_later_ts"))
        .withColumn("__sda_lag_seconds", lag_seconds)
        .agg(
            F.count(F.lit(1)).alias("count"),
            F.sum(F.when(F.col("__sda_lag_seconds") == 0, 1).otherwise(0)).alias(
                "zero_duration_count"
            ),
            F.sum(F.when(F.col("__sda_lag_seconds") > 0, 1).otherwise(0)).alias(
                "positive_duration_count"
            ),
            F.sum(F.when(F.col("__sda_lag_seconds") < 0, 1).otherwise(0)).alias(
                "negative_duration_count"
            ),
            F.expr(
                "percentile_approx(__sda_lag_seconds, array(0.25, 0.5, 0.75, 0.95, 0.99), 10000)"
            ).alias("duration_percentiles_seconds"),
            F.max("__sda_lag_seconds").alias("max_duration_seconds"),
        )
    )


def spark_state_transitions(
    frame: Any,
    *,
    entity_keys: tuple[str, ...],
    state_column: str,
    event_time: str,
    tie_breakers: tuple[str, ...] = (),
) -> Any:
    if not entity_keys:
        raise ValueError("spark_state_transitions requires at least one entity key")
    _require_columns(
        frame,
        (*entity_keys, state_column, event_time, *tie_breakers),
        metric="spark_state_transitions",
    )
    F = _functions()
    from pyspark.sql.window import Window

    ordering = [
        F.col(event_time).asc_nulls_last(),
        *(F.col(column).asc_nulls_last() for column in tie_breakers),
    ]
    duplicate_order = (
        frame.groupBy(*entity_keys, event_time, *tie_breakers).count().where(F.col("count") > 1)
    )
    if duplicate_order.limit(1).count():
        raise ValueError(
            "temporal_tie_breaker_missing"
            if not tie_breakers
            else "temporal_tie_breaker_not_unique"
        )
    window = Window.partitionBy(*entity_keys).orderBy(*ordering)
    ordered = (
        frame.withColumn("__sda_time", F.col(event_time).cast("timestamp"))
        .withColumn("from_state", F.col(state_column))
        .withColumn("to_state", F.lead(F.col(state_column)).over(window))
        .withColumn("__sda_next_time", F.lead("__sda_time").over(window))
        .withColumn("__sda_has_next", F.lead(F.lit(1)).over(window).isNotNull())
    )
    censored = (
        ordered.where(~F.col("__sda_has_next"))
        .groupBy("from_state")
        .agg(F.count(F.lit(1)).alias("right_censored_count"))
    )
    transitions = ordered.where(F.col("__sda_has_next"))
    transitions = transitions.withColumn(
        "__sda_duration", F.col("__sda_next_time").cast("long") - F.col("__sda_time").cast("long")
    )
    counts = transitions.groupBy("from_state", "to_state").agg(
        F.count(F.lit(1)).alias("count"),
        F.avg("__sda_duration").alias("mean_time_in_state_seconds"),
        F.expr("percentile_approx(__sda_duration, 0.5, 10000)").alias("p50_time_in_state_seconds"),
        F.max("__sda_duration").alias("max_time_in_state_seconds"),
    )
    outgoing = counts.groupBy("from_state").agg(F.sum("count").alias("outgoing_count"))
    return (
        counts.join(outgoing, "from_state")
        .join(censored, "from_state", "left")
        .fillna({"right_censored_count": 0})
        .withColumn("transition_count", F.col("count"))
        .withColumn("transition_probability", F.col("count") / F.col("outgoing_count"))
        .withColumn("self_transition", F.col("from_state").eqNullSafe(F.col("to_state")))
        .withColumn("warnings", F.array(F.lit("unseen_transition_not_assumed_invalid")))
    )
