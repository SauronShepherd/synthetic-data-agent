from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Any


def missing_metrics(values: Iterable[Any], sentinels: Iterable[Any] = ()) -> dict[str, int | float]:
    rows = list(values)
    nulls = sum(value is None for value in rows)
    blanks = sum(isinstance(value, str) and value == "" for value in rows)
    whitespace = sum(
        isinstance(value, str) and value.strip() == "" and value != "" for value in rows
    )
    nan = sum(isinstance(value, float) and math.isnan(value) for value in rows)
    sentinel_values = tuple(sentinels)
    sentinel = sum(value in sentinel_values for value in rows if value is not None)
    total = len(rows)
    return {
        "row_count": total,
        "null_count": nulls,
        "blank_count": blanks,
        "whitespace_count": whitespace,
        "nan_count": nan,
        "sentinel_count": sentinel,
        "null_rate": nulls / total if total else 0.0,
    }
