from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RuleConflict:
    left_rule_id: str
    right_rule_id: str
    conflict_type: str
    overlap_scope: dict[str, Any]
    winning_rule_id: str | None = None
    precedence_reason: str | None = None
    requires_review: bool = True


def detect_rule_conflicts(rules: list[Any]) -> tuple[RuleConflict, ...]:
    result = []
    for i, left in enumerate(rules):
        for right in rules[i + 1 :]:
            if getattr(left, "table", None) != getattr(right, "table", None):
                continue
            for lpred in getattr(left, "predicates", ()):
                for rpred in getattr(right, "predicates", ()):
                    if lpred.get("column") != rpred.get("column"):
                        continue
                    conflict_type = _conflict_type(lpred, rpred)
                    if conflict_type:
                        result.append(
                            RuleConflict(
                                left.rule_id,
                                right.rule_id,
                                conflict_type,
                                {"table": left.table, "column": lpred["column"]},
                            )
                        )
    return tuple(result)


def resolve_rule_conflicts(rules: list[Any], policy: Any) -> tuple[RuleConflict, ...]:
    """Attach deterministic precedence decisions without discarding conflicts."""
    conflicts = detect_rule_conflicts(rules)
    by_id = {rule.rule_id: rule for rule in rules}
    resolved = []
    for conflict in conflicts:
        left = by_id[conflict.left_rule_id]
        right = by_id[conflict.right_rule_id]
        left_rank = policy.rank(left.origin)
        right_rank = policy.rank(right.origin)
        if left_rank == right_rank:
            resolved.append(conflict)
            continue
        winner = left if left_rank > right_rank else right
        resolved.append(
            RuleConflict(
                conflict.left_rule_id,
                conflict.right_rule_id,
                conflict.conflict_type,
                conflict.overlap_scope,
                winning_rule_id=winner.rule_id,
                precedence_reason=f"origin_rank:{max(left_rank, right_rank)}",
                requires_review=False,
            )
        )
    return tuple(resolved)


def _conflict_type(left: dict[str, Any], right: dict[str, Any]) -> str | None:
    left_op, right_op = left.get("operator", "eq"), right.get("operator", "eq")
    if {left_op, right_op} == {"is_null", "is_not_null"}:
        return "nullability_opposites"
    if left_op == right_op == "eq" and left.get("value") != right.get("value"):
        return "mutually_exclusive_equality"
    if left_op == right_op == "in" and not set(left.get("values", ())) & set(
        right.get("values", ())
    ):
        return "disjoint_allowed_values"
    return None
