"""Distributed Spark evidence for a candidate parent-child relationship."""

from __future__ import annotations

from functools import reduce
from itertools import combinations
from operator import and_
from typing import Any


def discover_spark_key_candidates(
    parent_columns: tuple[Any, ...],
    child_columns: tuple[Any, ...],
    *,
    max_width: int = 2,
    max_candidates: int = 20,
) -> list[tuple[tuple[str, ...], tuple[str, ...]]]:
    """Return bounded, metadata-compatible key candidates for Spark validation.

    This produces only column-name combinations; values remain in Spark and are
    evaluated later by :func:`measure_spark_join`. Matching requires compatible
    declared types and either equal names or a shared normalized identifier
    stem (for example ``customer_id`` and ``customer_key``).
    """
    if max_width < 1 or max_candidates < 1:
        return []

    def stem(name: str) -> str:
        lowered = name.casefold()
        for suffix in ("_id", "_key", "_code"):
            if lowered.endswith(suffix):
                return lowered[: -len(suffix)]
        return lowered

    matches = [
        (str(parent.name), str(child.name))
        for parent in parent_columns
        for child in child_columns
        if str(parent.data_type).casefold() == str(child.data_type).casefold()
        and (
            str(parent.name).casefold() == str(child.name).casefold()
            or stem(str(parent.name)) == stem(str(child.name))
        )
    ]
    matches.sort(key=lambda item: (item[0].casefold(), item[1].casefold()))
    candidates: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
    for width in range(1, min(max_width, len(matches)) + 1):
        for selected in combinations(matches, width):
            parent_key = tuple(item[0] for item in selected)
            child_key = tuple(item[1] for item in selected)
            if parent_key not in {candidate[0] for candidate in candidates}:
                candidates.append((parent_key, child_key))
            if len(candidates) >= max_candidates:
                return candidates
    return candidates


def measure_spark_join(
    parent: Any, child: Any, parent_columns: tuple[str, ...], child_columns: tuple[str, ...]
) -> dict[str, Any]:
    """Measure directional inclusion using left-semi/left-anti joins.

    Only scalar aggregate rows are collected. Complete parent or child key
    sets are never materialized on the Python driver.
    """
    from pyspark.sql import functions as F

    if len(parent_columns) != len(child_columns) or not parent_columns:
        raise ValueError("parent and child key widths must match and be non-empty")
    parent_key = parent.select(
        *[F.col(column).alias(f"k{index}") for index, column in enumerate(parent_columns)]
    ).alias("parent_key")
    child_key = child.select(
        *[F.col(column).alias(f"k{index}") for index, column in enumerate(child_columns)]
    ).alias("child_key")
    child_total = child_key.count()
    child_all_null = child_key.where(
        reduce(and_, (F.col(f"child_key.k{index}").isNull() for index in range(len(child_columns))))
    ).count()
    child_any_null = child_key.where(
        reduce(
            and_,
            (F.col(f"child_key.k{index}").isNotNull() for index in range(len(child_columns))),
        )
    ).count()
    parent_populated = reduce(
        and_,
        (F.col(f"parent_key.k{index}").isNotNull() for index in range(len(parent_columns))),
    )
    child_populated = reduce(
        and_,
        (F.col(f"child_key.k{index}").isNotNull() for index in range(len(child_columns))),
    )
    parent_key = parent_key.where(parent_populated)
    child_key = child_key.where(child_populated)
    condition = [
        F.col(f"parent_key.k{index}") == F.col(f"child_key.k{index}")
        for index in range(len(parent_columns))
    ]
    condition_expr = condition[0]
    for expression in condition[1:]:
        condition_expr = condition_expr & expression
    distinct_parent = parent_key.dropDuplicates().count()
    parent_rows = parent_key.count()
    child_non_null = child_key
    child_rows = child_non_null.count()
    distinct_child = child_non_null.dropDuplicates().count()
    matched_rows = child_non_null.join(
        parent_key.dropDuplicates().alias("parent_key"), condition_expr, "left_semi"
    ).count()
    matched_values = (
        child_non_null.join(
            parent_key.dropDuplicates().alias("parent_key"), condition_expr, "left_semi"
        )
        .dropDuplicates()
        .count()
    )
    referenced_parents = (
        parent_key.dropDuplicates()
        .alias("parent_key")
        .join(child_non_null.dropDuplicates().alias("child_key"), condition_expr, "left_semi")
        .count()
    )
    fanout_stats = (
        child_non_null.join(
            parent_key.dropDuplicates().alias("parent_key"), condition_expr, "inner"
        )
        .groupBy(*[F.col(f"parent_key.k{index}") for index in range(len(parent_columns))])
        .count()
        .agg(
            F.avg("count").alias("mean"),
            F.max("count").alias("max"),
            F.expr("percentile_approx(count, 0.5)").alias("median"),
            F.expr("percentile_approx(count, 0.95)").alias("p95"),
        )
        .first()
    )
    fanout_mean = float(fanout_stats["mean"] or 0.0)
    fanout_p95 = int(fanout_stats["p95"] or 0)
    fanout_max = int(fanout_stats["max"] or 0)
    warnings = []
    if child_total and child_total != child_any_null:
        warnings.append("child_key_contains_nulls")
    if distinct_parent < parent_rows:
        warnings.append("parent_key_is_not_unique")
    if distinct_parent != parent_rows:
        cardinality = "parent_key_invalid"
    elif matched_values == child_rows:
        cardinality = "one_to_one"
    else:
        cardinality = "one_to_many"
    return {
        "parent_uniqueness_ratio": distinct_parent / parent_rows if parent_rows else 0.0,
        "child_row_coverage": matched_rows / child_rows if child_rows else 1.0,
        "child_value_coverage": matched_values / distinct_child if distinct_child else 1.0,
        "orphan_rate": (child_rows - matched_rows) / child_rows if child_rows else 0.0,
        "parent_reference_rate": referenced_parents / distinct_parent if distinct_parent else 0.0,
        "non_null_child_rows": child_rows,
        "matched_child_rows": matched_rows,
        "orphan_child_rows": child_rows - matched_rows,
        "distinct_child_keys": distinct_child,
        "matched_distinct_child_keys": matched_values,
        "validation_mode": "exact",
        "child_null_rate": (child_total - child_any_null) / child_total if child_total else 0.0,
        "child_all_null_rate": child_all_null / child_total if child_total else 0.0,
        "child_partial_null_rate": (
            (child_total - child_any_null - child_all_null) / child_total if child_total else 0.0
        ),
        "cardinality": cardinality,
        "fanout_mean": fanout_mean,
        "fanout_median": int(fanout_stats["median"] or 0),
        "fanout_p95": fanout_p95,
        "fanout_max": fanout_max,
        "parents_with_no_children": distinct_parent - referenced_parents,
        "zero_child_parent_rate": (
            (distinct_parent - referenced_parents) / distinct_parent if distinct_parent else 0.0
        ),
        "fanout_p95_to_mean": fanout_p95 / fanout_mean if fanout_mean else None,
        "fanout_max_to_mean": fanout_max / fanout_mean if fanout_mean else None,
        "warnings": warnings,
    }
