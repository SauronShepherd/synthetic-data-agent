"""Deterministic, evidence-preserving SDA 07 pattern detection."""

from sda.patterns.detector import PatternDetector
from sda.patterns.models import (
    Pattern,
    PatternConfig,
    PatternDecision,
    PatternDetectionResult,
    PatternFamily,
    PatternInputRefs,
    PatternOrigin,
)
from sda.patterns.precedence import RulePrecedencePolicy
from sda.patterns.scoring import EvidenceQuality, PatternScoringPolicy

__all__ = [
    "Pattern",
    "PatternConfig",
    "PatternDetector",
    "PatternFamily",
    "PatternOrigin",
    "PatternInputRefs",
    "PatternDetectionResult",
    "PatternDecision",
    "PatternScoringPolicy",
    "EvidenceQuality",
    "RulePrecedencePolicy",
]
