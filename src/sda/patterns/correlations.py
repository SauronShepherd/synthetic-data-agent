from __future__ import annotations

import math
from typing import Any


def pearson(xs: list[float], ys: list[float]) -> dict[str, Any]:
    pairs = [(x, y) for x, y in zip(xs, ys, strict=False) if math.isfinite(x) and math.isfinite(y)]
    n = len(pairs)
    if n < 2:
        return {"value": None, "valid_pair_count": n, "method": "exact"}
    mx, my = sum(x for x, _ in pairs) / n, sum(y for _, y in pairs) / n
    dx, dy = [x - mx for x, _ in pairs], [y - my for _, y in pairs]
    den = math.sqrt(sum(v * v for v in dx) * sum(v * v for v in dy))
    return {
        "value": (sum(a * b for a, b in zip(dx, dy, strict=False)) / den if den else None),
        "valid_pair_count": n,
        "method": "exact",
    }


def spearman(xs: list[float], ys: list[float]) -> dict[str, Any]:
    pairs = [(x, y) for x, y in zip(xs, ys, strict=False) if math.isfinite(x) and math.isfinite(y)]
    if len(pairs) < 2:
        return {"value": None, "valid_pair_count": len(pairs), "method": "unavailable"}

    def ranks(values: list[float]) -> list[float]:
        ordered = sorted(enumerate(values), key=lambda item: (item[1], item[0]))
        result = [0.0] * len(values)
        for rank, (index, _) in enumerate(ordered, 1):
            result[index] = float(rank)
        return result

    ranked = pearson(ranks([x for x, _ in pairs]), ranks([y for _, y in pairs]))
    return {**ranked, "algorithm": "rank_ordinal"}


def correlation_outlier_diagnostic(
    xs: list[float], ys: list[float], *, trim_fraction: float = 0.01
) -> dict[str, Any]:
    full = pearson(xs, ys)
    trim = int(len(xs) * trim_fraction)
    trimmed_x = xs[trim : len(xs) - trim] if len(xs) > 2 * trim else xs
    trimmed_y = ys[trim : len(ys) - trim] if len(ys) > 2 * trim else ys
    trimmed = pearson(trimmed_x, trimmed_y)
    return {
        "full": full,
        "trimmed": trimmed,
        "sign_changed": full.get("value") is not None
        and trimmed.get("value") is not None
        and full["value"] * trimmed["value"] < 0,
    }
