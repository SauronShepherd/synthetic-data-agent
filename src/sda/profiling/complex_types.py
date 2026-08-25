from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def complex_metrics(values: Iterable[Any], kind: str) -> tuple[dict[str, Any], tuple[str, ...]]:
    present = [value for value in values if value is not None]
    if kind == "array":
        sizes = [len(value) for value in present if isinstance(value, list | tuple)]
        return (
            {
                "present_count": len(present),
                "size_min": min(sizes) if sizes else None,
                "size_max": max(sizes) if sizes else None,
                "size_mean": sum(sizes) / len(sizes) if sizes else None,
                "empty_count": sum(size == 0 for size in sizes),
            },
            (),
        )
    if kind == "map":
        sizes = [len(value) for value in present if isinstance(value, dict)]
        return (
            {
                "present_count": len(present),
                "entry_count_min": min(sizes) if sizes else None,
                "entry_count_max": max(sizes) if sizes else None,
                "entry_count_mean": sum(sizes) / len(sizes) if sizes else None,
            },
            (),
        )
    if kind == "struct":
        keys = sorted({key for value in present if isinstance(value, dict) for key in value})
        return (
            {"present_count": len(present), "field_names": keys[:100], "field_count": len(keys)},
            (),
        )
    return ({"available": False}, ("unsupported_variant_profile",))
