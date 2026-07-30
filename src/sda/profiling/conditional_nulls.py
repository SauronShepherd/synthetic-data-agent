from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any


def conditional_null_hint(
    rows: Iterable[Mapping[str, Any]], target: str, segment: str, *, max_groups: int = 50
) -> dict[str, Any]:
    grouped: dict[Any, list[bool]] = defaultdict(list)
    for row in rows:
        grouped[row.get(segment)].append(row.get(target) is None)
    if len(grouped) > max_groups:
        return {"available": False, "warning": "conditional_null_segment_too_high_cardinality"}
    return {
        "available": True,
        "segment_column": segment,
        "target_column": target,
        "groups": [
            {
                "segment": str(key),
                "row_count": len(values),
                "null_rate": sum(values) / len(values) if values else 0.0,
            }
            for key, values in sorted(grouped.items(), key=lambda item: str(item[0]))
        ],
    }
