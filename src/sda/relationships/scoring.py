"""Hard gates and versioned, explainable confidence scoring."""

from __future__ import annotations

from typing import Any

POLICY_VERSION = "sda06-v1"


def score_relationship(
    metrics: Any, *, declared: bool = False, hints: tuple[str, ...] = ()
) -> dict[str, Any]:
    reasons: list[str] = []
    warnings = list(metrics.warnings)
    if metrics.parent_uniqueness_ratio < 1:
        reasons.append("parent_key_not_unique")
    if metrics.orphan_rate > 0.05:
        reasons.append("orphan_rate_above_gate")
    score = (
        0.45 * metrics.child_row_coverage
        + 0.25 * metrics.child_value_coverage
        + 0.20 * metrics.parent_uniqueness_ratio
        + 0.10 * (1 if declared else min(1, len(hints) / 2))
    )
    if reasons:
        band, decision = "low", "rejected"
    elif score >= 0.85:
        band, decision = "high", "accepted"
    elif score >= 0.65:
        band, decision = "medium", "awaiting_review"
    else:
        band, decision = "low", "rejected"
    return {
        "confidence_score": round(score, 6),
        "confidence_band": band,
        "decision": decision,
        "reason_codes": reasons,
        "warnings": warnings,
        "scoring_policy_version": POLICY_VERSION,
    }
