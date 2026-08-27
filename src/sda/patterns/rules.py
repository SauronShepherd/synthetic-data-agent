from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Any

from sda.patterns.models import PatternOrigin


class RuleStrength(IntEnum):
    ANOMALY_SIGNAL = 1
    PROBABILISTIC = 2
    CONDITIONAL = 3
    HARD = 4


@dataclass(frozen=True, slots=True)
class BusinessRule:
    rule_id: str
    table: str
    predicates: tuple[dict[str, Any], ...]
    origin: PatternOrigin = PatternOrigin.USER_PROVIDED
    strength: RuleStrength = RuleStrength.PROBABILISTIC
    approved: bool = False
    effective_from: str | None = None
    effective_to: str | None = None
    review_status: str = "required"

    def __post_init__(self) -> None:
        if not self.rule_id or not self.table or not self.predicates:
            raise ValueError("rule identity, scope, and predicates are required")
        if self.strength is RuleStrength.HARD and self.origin is PatternOrigin.OBSERVED:
            raise ValueError("observed rules cannot be hard invariants")
        if (
            self.strength is RuleStrength.HARD
            and self.origin is PatternOrigin.USER_PROVIDED
            and not self.approved
        ):
            raise ValueError("unapproved hard user rules are not executable")


@dataclass(frozen=True, slots=True)
class RuleEvaluationResult:
    population_rows: int
    satisfying_rows: int
    violation_rows: int
    satisfaction_rate: float | None
    violation_rate: float | None
    condition_support_rows: int = 0
    condition_support_rate: float | None = None
    validation_mode: str = "exact"
    warnings: tuple[str, ...] = ()


def evaluate_rule(rows: list[dict[str, Any]], rule: BusinessRule) -> RuleEvaluationResult:
    selected = [
        r
        for r in rows
        if all(
            _predicate_matches(r, predicate)
            for predicate in rule.predicates
            if predicate.get("role", "condition") == "condition"
        )
    ]
    violations = sum(
        not all(
            _predicate_matches(r, predicate)
            for predicate in rule.predicates
            if predicate.get("role", "condition") != "condition"
        )
        for r in selected
    )
    return RuleEvaluationResult(
        len(selected),
        len(selected) - violations,
        violations,
        (len(selected) - violations) / len(selected) if selected else None,
        violations / len(selected) if selected else None,
        len(selected),
        len(selected) / len(rows) if rows else None,
    )


def _predicate_matches(row: dict[str, Any], predicate: dict[str, Any]) -> bool:
    value = row.get(predicate["column"])
    operator = predicate.get("operator", "eq")
    expected = predicate.get("value")
    if operator == "eq":
        return value == expected
    if operator == "neq":
        return value != expected
    if operator == "is_null":
        return value is None
    if operator == "is_not_null":
        return value is not None
    if operator == "in":
        return value in tuple(predicate.get("values", ()))
    if operator == "not_in":
        return value not in tuple(predicate.get("values", ()))
    if operator == "gte":
        return value is not None and value >= expected
    if operator == "lte":
        return value is not None and value <= expected
    if operator == "gt":
        return value is not None and value > expected
    if operator == "lt":
        return value is not None and value < expected
    raise ValueError(f"unsupported rule operator: {operator}")
