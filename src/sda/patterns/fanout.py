from __future__ import annotations

from collections import Counter
from statistics import median
from typing import Any


def fanout_by_segment(
    parents: list[dict[str, Any]],
    children: list[dict[str, Any]],
    *,
    parent_key: str,
    segment: str,
    child_key: str,
) -> list[dict[str, Any]]:
    counts = Counter(row.get(child_key) for row in children)
    grouped: dict[Any, list[int]] = {}
    for parent in parents:
        grouped.setdefault(parent.get(segment), []).append(counts.get(parent.get(parent_key), 0))
    return [
        {
            "segment": key,
            "parent_count": len(vals),
            "zero_child_count": sum(v == 0 for v in vals),
            "zero_child_rate": sum(v == 0 for v in vals) / len(vals),
            "median_child_count": median(vals),
            "max_child_count": max(vals),
        }
        for key, vals in sorted(grouped.items(), key=lambda x: str(x[0]))
    ]
