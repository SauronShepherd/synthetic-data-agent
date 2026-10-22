from __future__ import annotations

from math import isfinite
from statistics import median


def stability(
    values: list[float],
    *,
    max_spread: float = 0.1,
    global_value: float | None = None,
    dimension: str = "provided_slices",
    support_counts: list[int] | None = None,
    min_support_rows: int = 1,
    min_slices: int = 2,
    truncated: bool = False,
) -> dict[str, object]:
    """Summarize bounded slice metrics against an explicit global baseline.

    Stability is unavailable, never implicitly true, when too few slices meet
    support or when the caller had to truncate an over-wide slice set.
    """
    if not dimension.strip() or max_spread < 0 or min_support_rows < 1 or min_slices < 2:
        raise ValueError("invalid stability bounds or dimension")
    supports = support_counts if support_counts is not None else [min_support_rows] * len(values)
    if len(supports) != len(values) or any(count < 0 for count in supports):
        raise ValueError("stability support counts must align with values and be non-negative")
    eligible = [
        (float(value), support)
        for value, support in zip(values, supports, strict=True)
        if support >= min_support_rows and isfinite(float(value))
    ]
    metrics = [value for value, _ in eligible]
    if not metrics:
        return {
            "dimension": dimension,
            "stable": None,
            "slice_count": len(values),
            "eligible_slice_count": 0,
            "excluded_slice_count": len(values),
            "warning": "stability_dimension_unavailable",
            "truncated": truncated,
        }
    spread = max(metrics) - min(metrics)
    baseline = float(global_value) if global_value is not None else median(metrics)
    max_delta = max(abs(value - baseline) for value in metrics)
    sufficient = len(metrics) >= min_slices and not truncated
    sign = [value > 0 for value in metrics]
    return {
        "dimension": dimension,
        "slice_count": len(values),
        "eligible_slice_count": len(metrics),
        "excluded_slice_count": len(values) - len(metrics),
        "min_slice_support": min(support for _, support in eligible),
        "max_slice_support": max(support for _, support in eligible),
        "stable": (spread <= max_spread and max_delta <= max_spread) if sufficient else None,
        "min_metric": min(metrics),
        "max_metric": max(metrics),
        "median_metric": median(metrics),
        "max_abs_delta_from_global": max_delta,
        "sign_flip_count": sum(sign[index] != sign[index - 1] for index in range(1, len(sign))),
        "divergence_max": spread,
        "global_metric": baseline,
        "truncated": truncated,
        **({"warning": "insufficient_or_truncated_slices"} if not sufficient else {}),
    }
