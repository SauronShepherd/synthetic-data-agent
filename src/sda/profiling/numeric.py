from __future__ import annotations

import math
from collections.abc import Iterable
from statistics import mean, pstdev
from typing import Any

from .common import finite, percentile


def numeric_metrics(values: Iterable[Any]) -> dict[str, Any]:
    raw = list(values)
    nums = finite(raw)
    if not nums:
        return {"available": False, "reason": "no_finite_numeric_values"}
    avg = mean(nums)
    deviation = pstdev(nums) if len(nums) > 1 else 0.0
    skewness = (
        sum((value - avg) ** 3 for value in nums) / len(nums) / deviation**3
        if deviation and len(nums) > 2
        else 0.0
    )
    return {
        "available": True,
        "count": len(nums),
        "min": min(nums),
        "max": max(nums),
        "mean": avg,
        "stddev_population": deviation,
        "skewness_population": skewness,
        "p01": percentile(nums, 0.01),
        "p05": percentile(nums, 0.05),
        "p25": percentile(nums, 0.25),
        "p50": percentile(nums, 0.5),
        "p75": percentile(nums, 0.75),
        "p95": percentile(nums, 0.95),
        "p99": percentile(nums, 0.99),
        "zero_rate": nums.count(0) / len(nums),
        "positive_rate": sum(value > 0 for value in nums) / len(nums),
        "negative_rate": sum(value < 0 for value in nums) / len(nums),
        "nan_count": sum(isinstance(value, float) and math.isnan(value) for value in raw),
        "positive_infinity_count": sum(value == math.inf for value in raw),
        "negative_infinity_count": sum(value == -math.inf for value in raw),
    }


def numeric_histogram(values: Iterable[Any], bins: int = 10) -> dict[str, Any]:
    """Return deterministic equal-width bins without retaining source values."""
    if bins < 1:
        raise ValueError("bins must be positive")
    nums = finite(values)
    if not nums:
        return {"available": False, "reason": "no_finite_numeric_values"}
    lo, hi = min(nums), max(nums)
    if lo == hi:
        return {
            "available": True,
            "min": lo,
            "max": hi,
            "bins": [{"lower": lo, "upper": hi, "count": len(nums)}],
        }
    width = (hi - lo) / bins
    counts = [0] * bins
    for value in nums:
        counts[min(bins - 1, int((value - lo) / width))] += 1
    return {"available": True, "min": lo, "max": hi, "bins": [
        {"lower": lo + i * width, "upper": lo + (i + 1) * width, "count": count}
        for i, count in enumerate(counts)
    ]}
