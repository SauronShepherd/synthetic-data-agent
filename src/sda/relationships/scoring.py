"""Hard gates and versioned, explainable confidence scoring."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

POLICY_VERSION = "sda06-v1"


@dataclass(frozen=True, slots=True)
class RelationshipScoringPolicy:
    """Versioned, explainable relationship decision policy."""

    version: str = POLICY_VERSION
    parent_uniqueness_weight: float = 0.20
    child_row_coverage_weight: float = 0.45
    child_value_coverage_weight: float = 0.25
    origin_evidence_weight: float = 0.10
    orphan_rate_gate: float = 0.05
    accepted_threshold: float = 0.85
    review_threshold: float = 0.65

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def score_relationship(
    metrics: Any,
    *,
    declared: bool = False,
    hints: tuple[str, ...] = (),
    policy: RelationshipScoringPolicy | None = None,
) -> dict[str, Any]:
    policy = policy or RelationshipScoringPolicy()
    reasons: list[str] = []
    warnings = list(metrics.warnings)
    if metrics.parent_uniqueness_ratio < 1:
        reasons.append("parent_key_not_unique")
    if metrics.orphan_rate > policy.orphan_rate_gate:
        reasons.append("orphan_rate_above_gate")
    contributions = {
        "child_row_coverage": policy.child_row_coverage_weight * metrics.child_row_coverage,
        "child_value_coverage": policy.child_value_coverage_weight * metrics.child_value_coverage,
        "parent_uniqueness": policy.parent_uniqueness_weight * metrics.parent_uniqueness_ratio,
        "origin_evidence": policy.origin_evidence_weight
        * (1 if declared else min(1, len(hints) / 2)),
    }
    score = sum(contributions.values())
    if reasons:
        band, decision = "low", "rejected"
    elif score >= policy.accepted_threshold:
        band, decision = "high", "accepted"
    elif score >= policy.review_threshold:
        band, decision = "medium", "awaiting_review"
    else:
        band, decision = "low", "rejected"
    return {
        "confidence_score": round(score, 6),
        "confidence_band": band,
        "decision": decision,
        "reason_codes": reasons,
        "warnings": warnings,
        "scoring_policy_version": policy.version,
        "scoring_policy": policy.to_dict(),
        "score_contributions": contributions,
        "hard_gates": {
            "parent_key_unique": metrics.parent_uniqueness_ratio >= 1,
            "orphan_rate_within_gate": metrics.orphan_rate <= policy.orphan_rate_gate,
        },
    }
