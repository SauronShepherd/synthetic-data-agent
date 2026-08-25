from __future__ import annotations

from dataclasses import dataclass

from sda.patterns.models import PatternDecision, PatternOrigin


@dataclass(frozen=True, slots=True)
class EvidenceQuality:
    support_quality: str
    validation_mode: str
    stability_quality: str
    source_quality: str
    warning_penalties: tuple[str, ...] = ()
    ranking_score: float | None = None


@dataclass(frozen=True, slots=True)
class PatternScoringPolicy:
    version: str = "sda07-policy-v1"
    min_support_rows: int = 100
    min_correlation_abs: float = 0.2

    def decide(
        self,
        *,
        support_rows: int,
        metric: float | None,
        quality: EvidenceQuality,
        origin: PatternOrigin = PatternOrigin.OBSERVED,
    ) -> PatternDecision:
        if support_rows < self.min_support_rows:
            return PatternDecision.INSUFFICIENT_EVIDENCE
        if metric is not None and abs(metric) < self.min_correlation_abs:
            return PatternDecision.REJECTED
        if quality.warning_penalties or quality.validation_mode != "exact":
            return PatternDecision.REVIEW_REQUIRED
        return PatternDecision.ACCEPTED_FOR_PLANNING
