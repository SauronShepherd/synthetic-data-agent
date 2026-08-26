"""Controlled, deterministic mutation of a clean generated baseline."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Any

from sda.artifacts.fingerprint import fingerprint


class NoiseProfile(StrEnum):
    HISTORICAL = "historical"
    MILD = "mild"
    QA = "qa"
    STRESS = "stress"


SUPPORTED_DEFECTS = frozenset(
    {
        "null_injection",
        "casing",
        "malformed_value",
        "invalid_category",
        "out_of_range",
        "omission",
        "duplicate",
        "near_duplicate",
        "future_timestamp",
        "invalid_state",
        "out_of_order_timestamp",
        "broken_foreign_key",
    }
)


class NoiseError(ValueError):
    """Raised when a noise plan is invalid or exceeds its budget."""


class _FrozenDict(dict[str, Any]):
    def _immutable(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("noise result rows are immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _immutable  # type: ignore[assignment]


def _freeze_rows(rows: tuple[dict[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    return tuple(_FrozenDict(row) for row in rows)


@dataclass(frozen=True, slots=True)
class NoisePlan:
    noise_id: str
    baseline_fingerprint: str
    profile: NoiseProfile = NoiseProfile.QA
    defect_type: str = "null_injection"
    budget: int = 0
    seed: int = 1729
    scenario: str = ""
    budget_rate: float | None = None

    def __post_init__(self) -> None:
        if not self.noise_id.strip() or not self.baseline_fingerprint.strip():
            raise ValueError("noise identity and baseline fingerprint are required")
        if self.budget < 0 or self.seed < 0:
            raise ValueError("noise budget and seed must not be negative")
        if self.budget_rate is not None and not 0.0 <= self.budget_rate <= 1.0:
            raise ValueError("noise budget_rate must be between zero and one")
        if self.budget and self.budget_rate is not None:
            raise ValueError("noise budget and budget_rate are mutually exclusive")
        if not self.defect_type.strip():
            raise ValueError("defect_type must not be empty")
        if self.scenario and not self.scenario.strip():
            raise ValueError("scenario must not be whitespace")
        if self.defect_type not in SUPPORTED_DEFECTS:
            raise ValueError(f"unsupported defect_type: {self.defect_type}")


@dataclass(frozen=True, slots=True)
class Mutation:
    noise_id: str
    row_index: int
    column: str
    defect_type: str
    before: Any
    after: Any

    def to_dict(self) -> dict[str, Any]:
        """Return an audit record without exposing mutated values."""
        return {
            "noise_id": self.noise_id,
            "row_index": self.row_index,
            "column": self.column,
            "defect_type": self.defect_type,
            "before_fingerprint": fingerprint(self.before),
            "after_fingerprint": fingerprint(self.after),
        }


@dataclass(frozen=True, slots=True)
class NoiseResult:
    rows: tuple[dict[str, Any], ...]
    mutations: tuple[Mutation, ...]
    baseline_fingerprint: str
    output_fingerprint: str

    def truth_ledger(self) -> tuple[dict[str, Any], ...]:
        """Return deterministic, raw-value-free mutation evidence."""
        return tuple(mutation.to_dict() for mutation in self.mutations)

    def to_dict(self) -> dict[str, Any]:
        """Serialize audit metadata without including output rows or raw mutations."""
        return {
            "baseline_fingerprint": self.baseline_fingerprint,
            "output_fingerprint": self.output_fingerprint,
            "mutation_count": len(self.mutations),
            "truth_ledger": self.truth_ledger(),
        }


def _selection_key(plan: NoisePlan, index: int) -> bytes:
    return hashlib.sha256(f"{plan.noise_id}|{plan.seed}|{plan.scenario}|{index}".encode()).digest()


def _resolved_budget(plan: NoisePlan, row_count: int) -> int:
    if plan.budget_rate is None:
        return min(plan.budget, row_count)
    return min(int(plan.budget_rate * row_count + 0.5), row_count)


def inject_nulls(
    baseline: tuple[dict[str, Any], ...],
    plan: NoisePlan,
    *,
    column: str,
) -> NoiseResult:
    """Inject up to ``budget`` nulls without mutating the clean baseline."""
    if fingerprint(baseline) != plan.baseline_fingerprint:
        raise NoiseError("baseline fingerprint does not match the noise plan")
    if not baseline or _resolved_budget(plan, len(baseline)) == 0:
        copied = tuple(dict(row) for row in baseline)
        frozen = _freeze_rows(copied)
        return NoiseResult(frozen, (), plan.baseline_fingerprint, fingerprint(frozen))
    if any(column not in row for row in baseline):
        raise NoiseError(f"column not present in baseline: {column}")
    candidates = sorted(
        range(len(baseline)),
        key=lambda index: _selection_key(plan, index),
    )
    selected = set(candidates[: _resolved_budget(plan, len(baseline))])
    rows = [dict(row) for row in baseline]
    mutations: list[Mutation] = []
    for index in sorted(selected):
        before = rows[index][column]
        rows[index][column] = None
        mutations.append(Mutation(plan.noise_id, index, column, plan.defect_type, before, None))
    output = tuple(rows)
    frozen = _freeze_rows(output)
    return NoiseResult(frozen, tuple(mutations), plan.baseline_fingerprint, fingerprint(frozen))


def apply_noise(
    baseline: tuple[dict[str, Any], ...], plan: NoisePlan, *, column: str
) -> NoiseResult:
    """Apply one deterministic, bounded defect class to an immutable baseline."""
    if fingerprint(baseline) != plan.baseline_fingerprint:
        raise NoiseError("baseline fingerprint does not match the noise plan")
    if plan.defect_type == "null_injection":
        return inject_nulls(baseline, plan, column=column)
    if any(column not in row for row in baseline):
        raise NoiseError(f"column not present in baseline: {column}")
    candidates = sorted(
        range(len(baseline)),
        key=lambda index: _selection_key(plan, index),
    )[: _resolved_budget(plan, len(baseline))]
    rows = [dict(row) for row in baseline]
    mutations: list[Mutation] = []
    for index in sorted(candidates):
        before = rows[index][column]
        after: Any
        if plan.defect_type == "casing":
            if not isinstance(before, str):
                raise NoiseError("casing defects require a string column")
            after = before.swapcase()
        elif plan.defect_type == "malformed_value":
            after = f"{before}__MALFORMED"
        elif plan.defect_type == "invalid_category":
            after = f"__INVALID_CATEGORY_{plan.scenario or 'synthetic'}"
        elif plan.defect_type == "invalid_state":
            after = f"__INVALID_STATE_{plan.scenario or 'synthetic'}"
        elif plan.defect_type == "broken_foreign_key":
            after = f"__ORPHAN_FK_{plan.scenario or 'synthetic'}"
        elif plan.defect_type == "omission":
            after = None
            del rows[index][column]
        elif plan.defect_type == "duplicate":
            after = rows[(index - 1) % len(rows)][column]
        elif plan.defect_type == "near_duplicate":
            if not isinstance(before, str):
                raise NoiseError("near_duplicate defects require a string column")
            if not before:
                raise NoiseError("near_duplicate defects require non-empty strings")
            after = before[:-1] + ("_" if before[-1:] != "_" else "-")
        elif plan.defect_type == "future_timestamp":
            if not isinstance(before, date | datetime):
                raise NoiseError("future_timestamp defects require a date or datetime column")
            after = before + timedelta(days=365)
        elif plan.defect_type == "out_of_order_timestamp":
            if not isinstance(before, date | datetime):
                raise NoiseError("out_of_order_timestamp defects require a date or datetime column")
            after = before - timedelta(days=365)
        else:
            if not isinstance(before, int | float) or isinstance(before, bool):
                raise NoiseError("out_of_range defects require a numeric column")
            after = before * 10 + 1
        if plan.defect_type != "omission":
            rows[index][column] = after
        mutations.append(Mutation(plan.noise_id, index, column, plan.defect_type, before, after))
    output = tuple(rows)
    frozen = _freeze_rows(output)
    return NoiseResult(frozen, tuple(mutations), plan.baseline_fingerprint, fingerprint(frozen))
