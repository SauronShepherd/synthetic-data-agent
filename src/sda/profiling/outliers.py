from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .common import finite, percentile


def numeric_outliers(
    values: list[Any], methods: Iterable[str] = ("iqr",)
) -> tuple[dict[str, Any], ...]:
    nums = finite(values)
    if len(nums) < 4:
        return ()
    q1, q3 = percentile(nums, 0.25), percentile(nums, 0.75)
    assert q1 is not None and q3 is not None
    iqr = q3 - q1
    low, high = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    findings: list[dict[str, Any]] = []
    requested = tuple(dict.fromkeys(method.lower() for method in methods))
    if "iqr" in requested:
        findings.append(
            {
                "method": "iqr",
                "thresholds": {"lower": low, "upper": high},
                "outlier_count": sum(value < low or value > high for value in nums),
                "population_count": len(nums),
            }
        )
    if "percentile" in requested:
        p01, p99 = percentile(nums, 0.01), percentile(nums, 0.99)
        if p01 is not None and p99 is not None:
            findings.append(
                {
                    "method": "percentile",
                    "thresholds": {"lower": p01, "upper": p99},
                    "outlier_count": sum(value < p01 or value > p99 for value in nums),
                    "population_count": len(nums),
                }
            )
    if "mad" in requested:
        center = percentile(nums, 0.5)
        if center is not None:
            deviations = [abs(value - center) for value in nums]
            scale = percentile(deviations, 0.5)
            if scale:
                low_mad, high_mad = center - 3 * 1.4826 * scale, center + 3 * 1.4826 * scale
                findings.append(
                    {
                        "method": "mad",
                        "thresholds": {"lower": low_mad, "upper": high_mad},
                        "outlier_count": sum(value < low_mad or value > high_mad for value in nums),
                        "population_count": len(nums),
                    }
                )
            else:
                findings.append(
                    {
                        "method": "mad",
                        "thresholds": {},
                        "outlier_count": 0,
                        "population_count": len(nums),
                        "warning": "mad_zero_scale",
                    }
                )
    return tuple(findings)
