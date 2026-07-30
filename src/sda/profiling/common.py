from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Any

from sda.profile_models import MetricEvidence, MetricMethod


def evidence(
    value: Any,
    *,
    count: int | None = None,
    method: MetricMethod = MetricMethod.EXACT,
    sample_fraction: float | None = None,
    warning: str | None = None,
) -> MetricEvidence:
    return MetricEvidence(
        value=value,
        method=method,
        population_count=count,
        sample_fraction=sample_fraction,
        warning=warning,
    )


def finite(values: Iterable[Any]) -> list[float]:
    return [
        float(value)
        for value in values
        if isinstance(value, (int, float))
        and not isinstance(value, bool)
        and not (isinstance(value, float) and math.isnan(value))
        and math.isfinite(float(value))
    ]


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    lower, upper = math.floor(index), math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)
