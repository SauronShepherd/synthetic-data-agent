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

    def to_dict(self) -> dict[str, Any]:
        return {
            "left_rule_id": self.left_rule_id,
            "right_rule_id": self.right_rule_id,
            "conflict_type": self.conflict_type,
            "overlap_scope": self.overlap_scope,
            "winning_rule_id": self.winning_rule_id,
            "precedence_reason": self.precedence_reason,
            "requires_review": self.requires_review,
        }


def detect_rule_conflicts(rules: list[Any]) -> tuple[RuleConflict, ...]:
    rule_ids = [getattr(rule, "rule_id", "") for rule in rules]
    if any(not rule_id for rule_id in rule_ids):
        raise ValueError("rules must have non-empty rule IDs")
    if len(rule_ids) != len(set(rule_ids)):
        raise ValueError("rule IDs must be unique for conflict resolution")
    result = []
    ordered_rules = sorted(rules, key=lambda rule: rule.rule_id)
    for i, left in enumerate(ordered_rules):
        for right in ordered_rules[i + 1 :]:
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
    return tuple(
        sorted(
            result,
            key=lambda conflict: (
                conflict.left_rule_id,
                conflict.right_rule_id,
                conflict.conflict_type,
            ),
        )
    )


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
            winner = min(left.rule_id, right.rule_id)
            resolved.append(
                RuleConflict(
                    conflict.left_rule_id,
                    conflict.right_rule_id,
                    conflict.conflict_type,
                    conflict.overlap_scope,
                    winning_rule_id=winner,
                    precedence_reason="rule_id_tiebreak",
                    requires_review=True,
                )
            )
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
