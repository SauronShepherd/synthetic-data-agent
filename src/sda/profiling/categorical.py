from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from typing import Any


def categorical_metrics(
    values: Iterable[Any], *, max_values: int = 100, redact: bool = True
) -> dict[str, Any]:
    counter = Counter(value for value in values if value is not None)
    total = sum(counter.values())
    top = counter.most_common(max_values)
    categories = [
        {
            "rank": index,
            "value": None if redact else str(value),
            "count": count,
            "share": count / total if total else 0.0,
        }
        for index, (value, count) in enumerate(top, start=1)
    ]
    return {
        "cardinality": len(counter),
        "cardinality_method": "exact",
        "top_values": categories,
        "dominant_share": top[0][1] / total if top and total else 0.0,
        "retained_count": len(top),
        "retained_weight": sum(item["share"] for item in categories),
    }
