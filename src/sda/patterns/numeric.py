from __future__ import annotations

from collections import defaultdict
from statistics import median
from typing import Any


def numeric_by_group(
    rows: list[dict[str, Any]], *, group: str, outcome: str
) -> list[dict[str, Any]]:
    groups: dict[Any, list[float]] = defaultdict(list)
    for row in rows:
        value = row.get(outcome)
        if value is not None:
            groups[row.get(group)].append(float(value))
    return [
        {
            "group": key,
            "count": len(values),
            "p50": median(values),
            "min": min(values),
            "max": max(values),
            "mean": sum(values) / len(values),
        }
        for key, values in sorted(groups.items(), key=lambda item: str(item[0]))
    ]
