from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from typing import Any


def business_freshness(
    values: Iterable[Any], reference_time: datetime, windows: Iterable[int]
) -> dict[str, Any]:
    timestamps = [value for value in values if isinstance(value, date | datetime)]
    if not timestamps:
        return {"available": False, "warning": "business_event_column_missing_or_unreadable"}
    normalized = [
        datetime.combine(value, datetime.min.time(), tzinfo=UTC)
        if isinstance(value, date) and not isinstance(value, datetime)
        else value
        for value in timestamps
    ]
    return {
        "available": True,
        "max_event_time": max(normalized).isoformat(),
        "lag_seconds": max(0.0, (reference_time - max(normalized)).total_seconds()),
        "recent_window_counts": {
            str(days): sum(value >= reference_time - timedelta(days=days) for value in normalized)
            for days in windows
        },
    }
