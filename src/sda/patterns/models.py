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


class PatternLifecycle(StrEnum):
    """Explicit evidence lifecycle; observation never implies approval."""

    OBSERVED_PATTERN = "observed_pattern"
    DECLARED_RULE = "declared_rule"
    APPROVED_RULE = "approved_rule"
    REJECTED = "rejected"
    REVIEW_REQUIRED = "review_required"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


@dataclass(frozen=True, slots=True)
class PatternInputRefs:
    metadata_artifact_id: str
    profile_artifact_ids: tuple[str, ...]
    relationship_artifact_id: str
    dependency_graph_artifact_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "profile_artifact_ids", tuple(self.profile_artifact_ids))
        artifact_ids = (
            self.metadata_artifact_id,
            *self.profile_artifact_ids,
            self.relationship_artifact_id,
            self.dependency_graph_artifact_id,
        )
        if (
            not self.metadata_artifact_id.strip()
            or not self.profile_artifact_ids
            or any(not artifact_id.strip() for artifact_id in self.profile_artifact_ids)
            or len(set(artifact_ids)) != len(artifact_ids)
            or not self.relationship_artifact_id.strip()
            or not self.dependency_graph_artifact_id.strip()
        ):
            raise ValueError("all upstream pattern artifacts are required")


@dataclass(frozen=True, slots=True)
class ColumnRoleAssignment:
    table: str
    column: str
    role: str
    confidence: float | None = None

    def __post_init__(self) -> None:
        if not self.table.strip() or not self.column.strip() or not self.role.strip():
            raise ValueError("column role assignment identity fields must not be empty")
        if self.confidence is not None and not 0.0 <= self.confidence <= 1.0:
            raise ValueError("column role assignment confidence must be between zero and one")


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
    schema_version: str = "pattern-execution-receipt-v1"

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
        decision_total = (
            self.patterns_accepted_for_planning
            + self.patterns_review_required
            + self.patterns_rejected
            + self.patterns_insufficient
        )
        if decision_total != self.patterns_emitted:
            raise ValueError("pattern receipt decision buckets must sum to patterns_emitted")
        if (
            self.candidate_count_by_family
            and sum(self.candidate_count_by_family.values()) != self.candidate_count_total
        ):
            raise ValueError("pattern receipt family counts must sum to candidate_count_total")
        for name, counts in (
            ("candidate_count_by_family", self.candidate_count_by_family),
            ("candidate_skipped_by_reason", self.candidate_skipped_by_reason),
        ):
            if any(not str(key).strip() or value < 0 for key, value in counts.items()):
                raise ValueError(f"pattern receipt {name} contains invalid accounting")
        if self.source_tables_reused > self.source_tables_scanned:
            raise ValueError("pattern receipt reused tables cannot exceed scanned tables")
        if self.sample_fraction is not None and not 0 < self.sample_fraction <= 1:
            raise ValueError("pattern receipt sample_fraction must be between 0 and 1")
        if self.sample_seed is not None and self.sample_seed < 0:
            raise ValueError("pattern receipt sample_seed must not be negative")
        if not self.schema_version.strip():
            raise ValueError("pattern receipt schema_version must not be empty")
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
    schema_version: str = "pattern-detection-result-v1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "patterns", tuple(self.patterns))
        object.__setattr__(self, "warnings", tuple(self.warnings))
        if not self.schema_version.strip():
            raise ValueError("pattern result schema_version must not be empty")
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
            "schema_version": self.schema_version,
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
            or self.sensitive_value_policy not in {"no_values", "fingerprints_only"}
            or self.min_support_rows < 1
            or not 0 <= self.min_support_rate <= 1
            or self.max_candidates < 1
            or self.max_category_values < 1
            or self.max_condition_depth < 0
            or self.max_segment_cardinality < 1
            or not 0 < self.sample_fraction <= 1
            or self.sample_seed < 0
            or self.max_rows_scanned < 1
            or not self.detector_version.strip()
            or not self.scoring_version.strip()
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
    lifecycle: PatternLifecycle = PatternLifecycle.OBSERVED_PATTERN

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "warnings", tuple(self.warnings))
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
        allowed_decisions = {decision.value for decision in PatternDecision} | {"review_required"}
        if self.decision not in allowed_decisions:
            raise ValueError(f"unsupported pattern decision: {self.decision}")
        if not isinstance(self.lifecycle, PatternLifecycle):
            try:
                object.__setattr__(self, "lifecycle", PatternLifecycle(self.lifecycle))
            except ValueError as exc:
                raise ValueError(f"unsupported pattern lifecycle: {self.lifecycle}") from exc
        if (
            self.lifecycle is PatternLifecycle.OBSERVED_PATTERN
            and self.origin is not PatternOrigin.OBSERVED
        ):
            raise ValueError("only observed evidence may use observed_pattern lifecycle")
        if self.lifecycle is PatternLifecycle.DECLARED_RULE and self.origin not in {
            PatternOrigin.DECLARED,
            PatternOrigin.USER_PROVIDED,
        }:
            raise ValueError("declared_rule lifecycle requires a declared rule origin")
        if self.lifecycle is PatternLifecycle.APPROVED_RULE and self.origin not in {
            PatternOrigin.DOMAIN_APPROVED,
            PatternOrigin.DESTINATION_CONSTRAINT,
        }:
            raise ValueError("approved_rule lifecycle requires an approval origin")
        lifecycle_decisions = {
            PatternLifecycle.REJECTED: PatternDecision.REJECTED.value,
            PatternLifecycle.REVIEW_REQUIRED: PatternDecision.REVIEW_REQUIRED.value,
            PatternLifecycle.INSUFFICIENT_EVIDENCE: PatternDecision.INSUFFICIENT_EVIDENCE.value,
        }
        expected_decision = lifecycle_decisions.get(self.lifecycle)
        if expected_decision is not None and self.decision != expected_decision:
            raise ValueError(
                f"{self.lifecycle.value} lifecycle requires {expected_decision} decision"
            )
        if self.origin is PatternOrigin.OBSERVED and self.rule_strength == "hard_constraint":
            raise ValueError("observed patterns must not become hard constraints")
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
