"""Controlled, deterministic mutation of a clean generated baseline."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class NoiseProfile(StrEnum):
    MILD = "mild"
    QA = "qa"
    STRESS = "stress"


class NoiseError(ValueError):
    """Raised when a noise plan is invalid or exceeds its budget."""


@dataclass(frozen=True, slots=True)
class NoisePlan:
    noise_id: str
    baseline_fingerprint: str
    profile: NoiseProfile = NoiseProfile.QA
    defect_type: str = "null_injection"
    budget: int = 0
    seed: int = 1729

    def __post_init__(self) -> None:
        if not self.noise_id.strip() or not self.baseline_fingerprint.strip():
            raise ValueError("noise identity and baseline fingerprint are required")
        if self.budget < 0 or self.seed < 0:
            raise ValueError("noise budget and seed must not be negative")
        if not self.defect_type.strip():
            raise ValueError("defect_type must not be empty")


@dataclass(frozen=True, slots=True)
class Mutation:
    noise_id: str
    row_index: int
    column: str
    defect_type: str
    before: Any
    after: Any


@dataclass(frozen=True, slots=True)
class NoiseResult:
    rows: tuple[dict[str, Any], ...]
    mutations: tuple[Mutation, ...]
    baseline_fingerprint: str


def inject_nulls(
    baseline: tuple[dict[str, Any], ...],
    plan: NoisePlan,
    *,
    column: str,
) -> NoiseResult:
    """Inject up to ``budget`` nulls without mutating the clean baseline."""
    if not baseline or plan.budget == 0:
        return NoiseResult(tuple(dict(row) for row in baseline), (), plan.baseline_fingerprint)
    if any(column not in row for row in baseline):
        raise NoiseError(f"column not present in baseline: {column}")
    candidates = sorted(
        range(len(baseline)),
        key=lambda index: hashlib.sha256(f"{plan.noise_id}|{plan.seed}|{index}".encode()).digest(),
    )
    selected = set(candidates[: min(plan.budget, len(candidates))])
    rows = [dict(row) for row in baseline]
    mutations: list[Mutation] = []
    for index in sorted(selected):
        before = rows[index][column]
        rows[index][column] = None
        mutations.append(Mutation(plan.noise_id, index, column, plan.defect_type, before, None))
    return NoiseResult(tuple(rows), tuple(mutations), plan.baseline_fingerprint)
