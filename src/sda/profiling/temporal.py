from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from datetime import UTC, date, datetime
from typing import Any


def temporal_metrics(
    values: Iterable[Any], reference_time: datetime | None = None
) -> dict[str, Any]:
    dates = [value for value in values if isinstance(value, (date, datetime))]
    if not dates:
        return {"available": False, "reason": "no_temporal_values"}
    reference = reference_time or datetime.now(UTC)
    normalized = [
        datetime.combine(value, datetime.min.time(), tzinfo=UTC)
        if isinstance(value, date) and not isinstance(value, datetime)
        else value
        for value in dates
    ]
    return {
        "available": True,
        "min": min(dates).isoformat(),
        "max": max(dates).isoformat(),
        "future_count": sum(value > reference for value in normalized),
        "month_counts": dict(sorted(Counter(value.month for value in dates).items())),
        "year_counts": dict(sorted(Counter(value.year for value in dates).items())),
        "weekday_counts": dict(sorted(Counter(value.weekday() for value in dates).items())),
        "hour_counts": dict(
            sorted(
                Counter(value.hour if isinstance(value, datetime) else 0 for value in dates).items()
            )
        ),
        "midnight_count": sum(
            isinstance(value, datetime)
            and value.hour == 0
            and value.minute == 0
            and value.second == 0
            for value in dates
        ),
    }
