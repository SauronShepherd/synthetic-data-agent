"""Local workflow service for the SDA 07 artifact-aware coordinator."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sda.patterns.detector import PatternDetector
from sda.patterns.models import PatternInputRefs


@dataclass(frozen=True, slots=True)
class PatternWorkflowSummary:
    stage: str
    pattern_artifact_id: str | None
    accepted_for_planning: int
    review_required: int
    insufficient_evidence: int
    warnings: tuple[str, ...]


def detect_patterns(
    detector: PatternDetector,
    *,
    rows: list[dict[str, Any]],
    run_id: str,
    environment: str,
    input_refs: PatternInputRefs,
    table: str,
    columns: dict[str, str],
) -> tuple[Any, PatternWorkflowSummary]:
    result = detector.detect(
        rows,
        table=table,
        columns=columns,
        run_id=run_id,
        environment=environment,
        input_refs=input_refs,
        selected_tables=(table,),
    )
    accepted = sum(pattern.decision == "accepted_for_planning" for pattern in result.patterns)
    review = sum(pattern.decision == "review_required" for pattern in result.patterns)
    insufficient = sum(pattern.decision == "insufficient_evidence" for pattern in result.patterns)
    summary = PatternWorkflowSummary(
        "patterns_detected",
        getattr(result.artifact_ref, "artifact_id", None),
        accepted,
        review,
        insufficient,
        result.warnings,
    )
    return result, summary
