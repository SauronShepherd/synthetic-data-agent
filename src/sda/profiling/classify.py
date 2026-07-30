from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sda.profile_models import ColumnProfileKind


def classify_column(
    data_type: str,
    values: Sequence[Any],
    *,
    cardinality_threshold: int = 100,
    uniqueness_threshold: float = 0.99,
) -> tuple[ColumnProfileKind, tuple[str, ...]]:
    dtype = data_type.lower()
    if any(token in dtype for token in ("array", "list")):
        return ColumnProfileKind.ARRAY, ("declared_array_type",)
    if "map" in dtype:
        return ColumnProfileKind.MAP, ("declared_map_type",)
    if "struct" in dtype:
        return ColumnProfileKind.STRUCT, ("declared_struct_type",)
    if "variant" in dtype:
        return ColumnProfileKind.VARIANT, ("declared_variant_type",)
    if "binary" in dtype:
        return ColumnProfileKind.BINARY, ("declared_binary_type",)
    if any(token in dtype for token in ("date", "timestamp")):
        return ColumnProfileKind.TEMPORAL, ("declared_temporal_type",)
    if any(
        token in dtype
        for token in ("int", "long", "short", "float", "double", "decimal", "numeric")
    ):
        return ColumnProfileKind.NUMERIC, ("declared_numeric_type",)
    non_null = [value for value in values if value is not None]
    distinct = len({str(value) for value in non_null})
    if non_null and distinct / len(non_null) >= uniqueness_threshold:
        return ColumnProfileKind.IDENTIFIER_LIKE, ("high_observed_uniqueness",)
    if distinct <= cardinality_threshold:
        return ColumnProfileKind.CATEGORICAL, ("low_observed_cardinality",)
    return ColumnProfileKind.STRING, ("declared_or_observed_string",)
