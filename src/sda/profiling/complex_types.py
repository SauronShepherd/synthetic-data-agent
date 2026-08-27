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

        def depth(value: Any) -> int:
            if isinstance(value, dict):
                return 1 + (max((depth(item) for item in value.values()), default=0))
            if isinstance(value, (list, tuple)):
                return 1 + max((depth(item) for item in value), default=0)
            return 0

        element_types = sorted(
            {
                type(item).__name__
                for value in present
                if isinstance(value, dict)
                for item in value.values()
            }
        )
        return (
            {
                "present_count": len(present),
                "field_names": keys[:100],
                "field_count": len(keys),
                "schema_depth": max((depth(value) for value in present), default=0),
                "element_types": element_types[:100],
            },
            (),
        )
    if kind in {"binary", "large_text"}:
        return (
            {"present_count": len(present), "available": False},
            (f"{kind}_profile_unavailable",),
        )
    return ({"available": False}, ("unsupported_variant_profile",))
