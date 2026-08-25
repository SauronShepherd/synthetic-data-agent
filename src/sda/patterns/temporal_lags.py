from __future__ import annotations

from statistics import quantiles
from typing import Any


def lag_distribution(rows: list[dict[str, Any]], *, earlier: str, later: str) -> dict[str, Any]:
    values = sorted(
        float(row[later] - row[earlier])
        for row in rows
        if row.get(earlier) is not None and row.get(later) is not None
    )
    if not values:
        return {"count": 0, "warning": "temporal_gap_analysis_unavailable"}
    q = quantiles(values, n=100, method="inclusive") if len(values) > 1 else [values[0]] * 99
    return {
        "count": len(values),
        "zero_duration_count": sum(value == 0 for value in values),
        "positive_duration_count": sum(value > 0 for value in values),
        "negative_duration_count": sum(value < 0 for value in values),
        "p25": q[24],
        "p50": q[49],
        "p75": q[74],
        "p95": q[94],
        "p99": q[98],
        "max": max(values),
        "long_tail_indicator": max(values) > 3 * q[49] if q[49] else False,
    }
