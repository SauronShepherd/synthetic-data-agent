from __future__ import annotations

from statistics import median


def stability(values: list[float], *, max_spread: float = 0.1) -> dict[str, object]:
    if not values:
        return {"stable": None, "warning": "stability_dimension_unavailable"}
    spread = max(values) - min(values)
    global_value = values[0]
    sign = [value > 0 for value in values]
    return {
        "dimension": "provided_slices",
        "slice_count": len(values),
        "eligible_slice_count": len(values),
        "stable": spread <= max_spread,
        "min_metric": min(values),
        "max_metric": max(values),
        "median_metric": median(values),
        "max_abs_delta_from_global": spread,
        "sign_flip_count": sum(sign[index] != sign[index - 1] for index in range(1, len(sign))),
        "divergence_max": spread,
        "global_metric": global_value,
    }
