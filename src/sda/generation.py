"""Deterministic standalone-table generator for the first executable slice."""

from __future__ import annotations

import hashlib
import random
from collections.abc import Mapping, Sequence
from typing import Any

from sda.operations import ResourceBudget, enforce_budget
from sda.planning import ColumnGenerationSpec, GenerationPlan, RowCountMode


class GenerationError(ValueError):
    """Raised when a plan cannot be executed safely."""


def generate_rows(
    plan: GenerationPlan,
    *,
    row_count: int | None = None,
    vocabularies: Mapping[str, Sequence[str]] | None = None,
    weighted_vocabularies: Mapping[str, Sequence[tuple[str, float]]] | None = None,
    empirical_samples: Mapping[str, Sequence[Any]] | None = None,
) -> tuple[dict[str, Any], ...]:
    """Generate bounded rows from an approved plan without reading source data.

    This is deliberately a pure local implementation. Databricks execution can use
    the same row-coordinate and value-model rules in a distributed adapter later.
    """
    if plan.status.value != "approved":
        raise GenerationError("only approved plans may be executed")
    if row_count is None:
        row_count = resolve_row_count(plan)
    if row_count < 0:
        raise GenerationError("row_count must not be negative")
    max_rows = int(plan.budgets.get("max_rows", row_count))
    if row_count > max_rows:
        raise GenerationError("row_count exceeds plan max_rows budget")
    enforce_budget(ResourceBudget(max_rows=max_rows), rows=row_count)
    specs = _unique_specs(plan.columns)
    vocabularies = vocabularies or {}
    weighted_vocabularies = weighted_vocabularies or {}
    empirical_samples = empirical_samples or {}
    result: list[dict[str, Any]] = []
    for index in range(row_count):
        row: dict[str, Any] = {}
        for spec in specs:
            row[spec.column] = _value(
                plan,
                spec,
                index,
                vocabularies.get(spec.column, ()),
                weighted_vocabularies.get(spec.column, ()),
                empirical_samples.get(spec.column, ()),
            )
        result.append(row)
    return tuple(result)


def resolve_row_count(plan: GenerationPlan, *, source_row_count: int | None = None) -> int:
    """Resolve a deterministic output count from the immutable plan contract."""
    if plan.requested_row_count is not None:
        return plan.requested_row_count
    if plan.row_count_mode is RowCountMode.EXACT:
        raise GenerationError("exact plans require requested_row_count")
    if source_row_count is None or source_row_count < 0:
        raise GenerationError("probabilistic plans require a non-negative source_row_count")
    scaled = source_row_count * plan.scale_factor
    lower = int(scaled)
    fraction = scaled - lower
    if fraction > 0.5 or (fraction == 0.5 and plan.seed % 2 == 1):
        lower += 1
    return lower


def _unique_specs(specs: Sequence[ColumnGenerationSpec]) -> tuple[ColumnGenerationSpec, ...]:
    seen: set[tuple[str, str]] = set()
    unique: list[ColumnGenerationSpec] = []
    for spec in specs:
        key = (spec.table, spec.column)
        if key in seen:
            raise GenerationError(f"duplicate column specification: {spec.table}.{spec.column}")
        seen.add(key)
        unique.append(spec)
    return tuple(unique)


def _value(
    plan: GenerationPlan,
    spec: ColumnGenerationSpec,
    index: int,
    vocabulary: Sequence[str],
    weighted_vocabulary: Sequence[tuple[str, float]],
    empirical_sample: Sequence[Any],
) -> Any:
    model = spec.model.lower()
    rng = random.Random(_coordinate_seed(plan, spec, index))
    null_rate = float(spec.parameters.get("null_rate", 0.0))
    if not 0.0 <= null_rate <= 1.0:
        raise GenerationError(f"null_rate must be between 0 and 1 for {spec.column}")
    if spec.nullable and null_rate and rng.random() < null_rate:
        return None
    if model in {"identifier", "id"}:
        return _stable_id(plan, spec, index)
    if model in {"categorical", "vocabulary"}:
        if weighted_vocabulary:
            total = sum(weight for _, weight in weighted_vocabulary)
            if total <= 0 or any(weight < 0 for _, weight in weighted_vocabulary):
                raise GenerationError(
                    f"categorical weights must be non-negative and non-zero for {spec.column}"
                )
            point = rng.random() * total
            for category, weight in weighted_vocabulary:
                point -= weight
                if point < 0:
                    return category
            return weighted_vocabulary[-1][0]
        if not vocabulary:
            raise GenerationError(f"model {model} requires vocabulary for {spec.column}")
        return vocabulary[index % len(vocabulary)]
    if model in {"empirical", "empirical_numeric", "empirical_categorical"}:
        if not empirical_sample:
            raise GenerationError(f"model {model} requires empirical samples for {spec.column}")
        position = rng.randrange(len(empirical_sample))
        value = empirical_sample[position]
        if model == "empirical_numeric" and not isinstance(value, int | float):
            raise GenerationError(f"empirical sample for {spec.column} must be numeric")
        if model == "empirical_categorical" and not isinstance(value, str):
            raise GenerationError(f"empirical sample for {spec.column} must be strings")
        return value
    if model in {"integer", "numeric", "uniform"}:
        low = float(spec.parameters.get("min", 0))
        high = float(spec.parameters.get("max", 1))
        if high < low:
            raise GenerationError(f"invalid numeric range for {spec.column}")
        value = low if low == high else rng.uniform(low, high)
        return (
            int(round(value)) if spec.data_type.lower() in {"int", "integer", "bigint"} else value
        )
    if model in {"boolean", "bool"}:
        return bool(index % 2)
    if model in {"date", "timestamp"}:
        return f"2020-01-{(index % 28) + 1:02d}"
    if model in {"string", "format"}:
        prefix = str(spec.parameters.get("prefix", spec.column))
        return f"{prefix}-{index:08d}"
    raise GenerationError(f"unsupported generation model: {spec.model}")


def _coordinate_seed(plan: GenerationPlan, spec: ColumnGenerationSpec, index: int) -> int:
    raw = f"{plan.plan_fingerprint}|{spec.table}|{spec.column}|{index}".encode()
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], "big") ^ plan.seed


def _stable_id(plan: GenerationPlan, spec: ColumnGenerationSpec, index: int) -> str:
    raw = f"{plan.plan_fingerprint}|{spec.table}|{spec.column}|{index}".encode()
    return hashlib.sha256(raw).hexdigest()[:24]
