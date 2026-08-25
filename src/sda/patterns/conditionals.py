from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class FallbackLevel:
    condition_columns: tuple[str, ...]
    min_support_rows: int
    min_support_rate: float


@dataclass(frozen=True, slots=True)
class FallbackPlan:
    levels: tuple[FallbackLevel, ...]
    terminal: str = "global_distribution"


def deterministic_fallback_plan(
    condition_columns: tuple[str, ...], *, min_support_rows: int, min_support_rate: float
) -> FallbackPlan:
    ordered = tuple(sorted(condition_columns, key=str))
    levels = tuple(
        FallbackLevel(ordered[:index], min_support_rows, min_support_rate)
        for index in range(len(ordered) - 1, 0, -1)
    )
    return FallbackPlan(levels=levels)


def conditional_counts(
    rows: list[dict[str, Any]], drivers: tuple[str, ...], outcome: str, *, max_cells: int = 1000
) -> list[dict[str, Any]]:
    counts: Counter[tuple[tuple[str, Any], Any]] = Counter()
    for row in rows:
        key = tuple((col, row.get(col)) for col in drivers)
        counts[(key, row.get(outcome))] += 1
    if len(counts) > max_cells:
        return []
    totals = Counter(key for key, _ in counts)
    return [
        {"condition": dict(key), "outcome": value, "count": count, "rate": count / totals[key]}
        for (key, value), count in sorted(counts.items(), key=lambda item: str(item[0]))
    ]
