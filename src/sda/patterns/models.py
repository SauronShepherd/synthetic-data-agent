from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

from sda.artifacts.fingerprint import fingerprint


class _FrozenDict(dict[str, Any]):
    def _immutable(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("pattern mappings are immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _immutable  # type: ignore[assignment]


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return _FrozenDict({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze(item) for item in value)
    return value


def _safe_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _safe_payload(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return tuple(_safe_payload(item) for item in value)
    if isinstance(value, int | float | bool) or value is None:
        return value
    return {"fingerprint": fingerprint(value)}


class PatternFamily(StrEnum):
    CORRELATION = "correlation"
    CONDITIONAL_DISTRIBUTION = "conditional_distribution"
    CONDITIONAL_MISSINGNESS = "conditional_missingness"
    FANOUT_BY_SEGMENT = "fanout_by_segment"
    TEMPORAL_ORDER = "temporal_order"
    STATE_TRANSITION = "state_transition"
    BUSINESS_RULE = "business_rule"


class PatternOrigin(StrEnum):
    OBSERVED = "observed"
    DECLARED = "declared"
    USER_PROVIDED = "user_provided"
    DOMAIN_APPROVED = "domain_approved"
    PLATFORM = "platform"
    DESTINATION_CONSTRAINT = "destination_constraint"


class PatternDecision(StrEnum):
    ACCEPTED_FOR_PLANNING = "accepted_for_planning"
    REVIEW_REQUIRED = "review_required"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    REJECTED = "rejected"
    CONFLICTED = "conflicted"


@dataclass(frozen=True, slots=True)
class PatternInputRefs:
    metadata_artifact_id: str
    profile_artifact_ids: tuple[str, ...]
    relationship_artifact_id: str
    dependency_graph_artifact_id: str

    def __post_init__(self) -> None:
        if (
            not self.metadata_artifact_id
            or not self.profile_artifact_ids
            or not self.relationship_artifact_id
            or not self.dependency_graph_artifact_id
        ):
            raise ValueError("all upstream pattern artifacts are required")


@dataclass(frozen=True, slots=True)
class ColumnRoleAssignment:
    table: str
    column: str
    role: str
    confidence: float | None = None


@dataclass(frozen=True, slots=True)
class PatternExecutionReceipt:
    candidate_count_total: int = 0
    candidate_count_by_family: Mapping[str, int] = field(default_factory=dict)
    candidate_skipped_by_reason: Mapping[str, int] = field(default_factory=dict)
    patterns_emitted: int = 0
    patterns_accepted_for_planning: int = 0
    patterns_review_required: int = 0
    patterns_rejected: int = 0
    patterns_insufficient: int = 0
    rules_evaluated: int = 0
    conflicts_found: int = 0
    source_tables_scanned: int = 0
    source_tables_reused: int = 0
    sample_fraction: float | None = None
    sample_seed: int | None = None

    def __post_init__(self) -> None:
        count_fields = (
            "candidate_count_total",
            "patterns_emitted",
            "patterns_accepted_for_planning",
            "patterns_review_required",
            "patterns_rejected",
            "patterns_insufficient",
            "rules_evaluated",
            "conflicts_found",
            "source_tables_scanned",
            "source_tables_reused",
        )
        if any(getattr(self, name) < 0 for name in count_fields):
            raise ValueError("pattern receipt counts must not be negative")
        if self.sample_fraction is not None and not 0 < self.sample_fraction <= 1:
            raise ValueError("pattern receipt sample_fraction must be between 0 and 1")
        if self.sample_seed is not None and self.sample_seed < 0:
            raise ValueError("pattern receipt sample_seed must not be negative")
        object.__setattr__(
            self, "candidate_count_by_family", _freeze(self.candidate_count_by_family)
        )
        object.__setattr__(
            self, "candidate_skipped_by_reason", _freeze(self.candidate_skipped_by_reason)
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize execution accounting without source rows or examples."""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PatternDetectionResult:
    patterns: tuple[Pattern, ...]
    artifact_ref: Any | None
    receipt: PatternExecutionReceipt
    warnings: tuple[str, ...] = ()
    review_questions: tuple[dict[str, Any], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "patterns", tuple(self.patterns))
        object.__setattr__(self, "warnings", tuple(self.warnings))
        object.__setattr__(
            self,
            "review_questions",
            tuple(_freeze(question) for question in self.review_questions),
        )

    def to_dict(self) -> dict[str, Any]:
        artifact = self.artifact_ref
        if artifact is not None and hasattr(artifact, "to_dict"):
            artifact = artifact.to_dict()
        return {
            "patterns": tuple(
                {
                    **pattern.to_dict(),
                    "condition": _safe_payload(pattern.condition),
                    "outcome": _safe_payload(pattern.outcome),
                    "metric": _safe_payload(pattern.metric),
                }
                for pattern in self.patterns
            ),
            "artifact_ref": artifact,
            "receipt": self.receipt.to_dict(),
            "warnings": self.warnings,
            "review_questions": self.review_questions,
        }


@dataclass(frozen=True, slots=True)
class PatternConfig:
    mode: str = "quick"
    min_support_rows: int = 30
    min_support_rate: float = 0.01
    max_candidates: int = 500
    max_category_values: int = 100
    include_spearman: bool = False
    sensitive_value_policy: str = "no_values"
    detector_version: str = "sda07-v1"
    scoring_version: str = "sda07-policy-v1"
    max_condition_depth: int = 2
    max_segment_cardinality: int = 50
    sample_fraction: float = 1.0
    sample_seed: int = 1729
    max_rows_scanned: int = 100_000

    def __post_init__(self) -> None:
        if (
            self.mode not in {"quick", "full"}
            or self.min_support_rows < 1
            or not 0 <= self.min_support_rate <= 1
            or self.max_candidates < 1
            or self.max_category_values < 1
            or self.max_condition_depth < 0
            or self.max_segment_cardinality < 1
            or not 0 < self.sample_fraction <= 1
            or self.max_rows_scanned < 1
        ):
            raise ValueError("invalid pattern configuration")

    @property
    def configuration_hash(self) -> str:
        return fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class Pattern:
    pattern_id: str
    analysis_id: str
    family: PatternFamily
    origin: PatternOrigin
    primary_table: str
    columns: tuple[str, ...]
    condition: dict[str, Any]
    outcome: dict[str, Any]
    support_rows: int
    support_rate: float | None
    metric: dict[str, Any]
    evidence_quality: dict[str, Any]
    decision: str = "review_required"
    rule_strength: str = "probabilistic_pattern"
    warnings: tuple[str, ...] = ()
    generation_action: dict[str, Any] = field(default_factory=dict)
    validation_action: dict[str, Any] = field(default_factory=dict)
    review_status: str = "not_required"

    def __post_init__(self) -> None:
        if self.support_rows < 0:
            raise ValueError("support_rows must not be negative")
        if self.support_rate is not None and not 0 <= self.support_rate <= 1:
            raise ValueError("support_rate must be between 0 and 1")
        if (
            not self.pattern_id.strip()
            or not self.analysis_id.strip()
            or not self.primary_table.strip()
        ):
            raise ValueError("pattern identity fields must not be empty")
        for name in (
            "condition",
            "outcome",
            "metric",
            "evidence_quality",
            "generation_action",
            "validation_action",
        ):
            object.__setattr__(self, name, _freeze(getattr(self, name)))

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["family"] = self.family.value
        value["origin"] = self.origin.value
        return value

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), default=str)
