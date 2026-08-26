"""Deterministic, evidence-preserving SDA 07 pattern detection."""

from sda.patterns.candidates import PatternCandidate, generate_candidates
from sda.patterns.detector import PatternDetector
from sda.patterns.models import (
    Pattern,
    PatternConfig,
    PatternDecision,
    PatternDetectionResult,
    PatternFamily,
    PatternInputRefs,
    PatternLifecycle,
    PatternOrigin,
)
from sda.patterns.precedence import RulePrecedencePolicy
from sda.patterns.scoring import EvidenceQuality, PatternScoringPolicy

__all__ = [
    "Pattern",
    "PatternConfig",
    "PatternDetector",
    "PatternCandidate",
    "generate_candidates",
    "PatternFamily",
    "PatternOrigin",
    "PatternLifecycle",
    "PatternInputRefs",
    "PatternDetectionResult",
    "PatternDecision",
    "PatternScoringPolicy",
    "EvidenceQuality",
    "RulePrecedencePolicy",
]
