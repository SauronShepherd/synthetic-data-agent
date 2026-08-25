from __future__ import annotations

from collections.abc import Callable
from typing import Any


def conditional_missingness(
    rows: list[dict[str, Any]], condition: dict[str, Any], outcome: str
) -> dict[str, Any]:
    selected = [row for row in rows if all(row.get(k) == v for k, v in condition.items())]
    missing = sum(row.get(outcome) is None for row in selected)
    return {
        "support_rows": len(selected),
        "missing_rows": missing,
        "missing_rate": missing / len(selected) if selected else None,
        "satisfying_rows": len(selected) - missing,
    }


def missingness_signatures(
    rows: list[dict[str, Any]],
    column: str,
    *,
    sentinels: tuple[Any, ...] = (),
    parse_failure: Callable[[Any], Any] | None = None,
) -> dict[str, int]:
    """Keep null, blank, sentinel, and parse failures as separate evidence."""
    result = {"null_count": 0, "blank_count": 0, "sentinel_count": 0, "parse_failure_count": 0}
    for row in rows:
        value = row.get(column)
        if value is None:
            result["null_count"] += 1
        if isinstance(value, str) and not value.strip():
            result["blank_count"] += 1
        if value in sentinels:
            result["sentinel_count"] += 1
        if value is not None and parse_failure is not None:
            try:
                parse_failure(value)
            except (TypeError, ValueError):
                result["parse_failure_count"] += 1
    return result
