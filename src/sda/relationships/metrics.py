"""Exact relationship evidence over Python row mappings (Spark-compatible semantics)."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class JoinMetrics:
    parent_uniqueness_ratio: float
    child_null_rate: float
    child_row_coverage: float
    child_value_coverage: float
    orphan_rate: float
    parent_reference_rate: float
    cardinality: str
    children_per_parent: dict[str, float | int]
    non_null_child_rows: int = 0
    matched_child_rows: int = 0
    orphan_child_rows: int = 0
    distinct_child_keys: int = 0
    matched_distinct_child_keys: int = 0
    validation_mode: str = "exact"
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "warnings": list(self.warnings)}


def _key(row: dict[str, Any], cols: tuple[str, ...]) -> tuple[Any, ...] | None:
    values = tuple(row.get(c) for c in cols)
    # A composite key is testable only when every component is populated.
    return None if any(v is None for v in values) else values


def measure_join(
    parent: Iterable[dict[str, Any]],
    child: Iterable[dict[str, Any]],
    parent_columns: tuple[str, ...],
    child_columns: tuple[str, ...],
) -> JoinMetrics:
    parent_data = list(parent)
    child_rows = list(child)
    pkeys = [_key(r, parent_columns) for r in parent_data]
    ckeys = [_key(r, child_columns) for r in child_rows]
    nonnull_p = [k for k in pkeys if k is not None]
    nonnull_c = [k for k in ckeys if k is not None]
    counts = Counter(nonnull_p)
    pset = set(nonnull_p)
    cset = set(nonnull_c)
    matched = [k for k in nonnull_c if k in pset]
    child_reference_counts = Counter(k for k in nonnull_c if k in pset)
    unique_ratio = len(pset) / len(nonnull_p) if nonnull_p else 0.0
    row_cov = len(matched) / len(nonnull_c) if nonnull_c else 1.0
    value_cov = len(set(matched)) / len(cset) if cset else 1.0
    ref_rate = len(set(matched)) / len(pset) if pset else 0.0
    duplicated_parent = any(v > 1 for v in counts.values())
    duplicated_child = any(v > 1 for v in child_reference_counts.values())
    cardinality = (
        "parent_key_invalid"
        if duplicated_parent and duplicated_child
        else "many_to_one"
        if duplicated_child
        else "one_to_one"
    )
    raw_child_keys = [tuple(r.get(c) for c in child_columns) for r in child_rows]
    warnings = (
        ("partial_composite_keys_present",)
        if any(
            any(v is None for v in key) and not all(v is None for v in key)
            for key in raw_child_keys
        )
        else ()
    )
    fanout = [child_reference_counts.get(key, 0) for key in pset]
    sorted_fanout = sorted(fanout)
    p95_index = min(len(sorted_fanout) - 1, int(len(sorted_fanout) * 0.95)) if sorted_fanout else 0
    if duplicated_parent:
        warnings = tuple(dict.fromkeys((*warnings, "parent_key_is_not_unique", "candidate_rejected")))
    return JoinMetrics(
        unique_ratio,
        1 - len(nonnull_c) / len(ckeys) if ckeys else 0.0,
        row_cov,
        value_cov,
        (len(nonnull_c) - len(matched)) / len(nonnull_c) if nonnull_c else 0.0,
        ref_rate,
        cardinality,
        {
            "median": sorted(fanout)[len(fanout) // 2] if fanout else 0,
            "max": max(fanout, default=0),
            "mean": sum(fanout) / len(fanout) if fanout else 0.0,
            "p95": sorted_fanout[p95_index] if sorted_fanout else 0,
            "parent_count": len(pset),
            "parents_with_no_children": len(pset - set(child_reference_counts)),
        },
        len(nonnull_c),
        len(matched),
        len(nonnull_c) - len(matched),
        len(cset),
        len(set(matched)),
        warnings=warnings,
    )
