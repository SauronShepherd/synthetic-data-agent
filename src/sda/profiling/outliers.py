from __future__ import annotations

from typing import Any

from .common import finite, percentile


def numeric_outliers(values: list[Any]) -> tuple[dict[str, Any], ...]:
    nums = finite(values)
    if len(nums) < 4:
        return ()
    q1, q3 = percentile(nums, 0.25), percentile(nums, 0.75)
    assert q1 is not None and q3 is not None
    iqr = q3 - q1
    low, high = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    return (
        {
            "method": "iqr",
            "thresholds": {"lower": low, "upper": high},
            "outlier_count": sum(value < low or value > high for value in nums),
            "population_count": len(nums),
        },
    )
