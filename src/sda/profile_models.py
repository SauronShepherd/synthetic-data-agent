"""Versioned, JSON-safe contracts for SDA 05 value-level profiling."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class ProfileMode(StrEnum):
    QUICK = "quick"
    FULL = "full"


class MetricMethod(StrEnum):
    EXACT = "exact"
    APPROXIMATE = "approximate"
    SAMPLED = "sampled"
    METADATA_DERIVED = "metadata_derived"
    UNAVAILABLE = "unavailable"


class PopulationScope(StrEnum):
    FULL = "full"
    SAMPLE = "sample"
    METADATA = "metadata"


class CalculationMethod(StrEnum):
    EXACT = "exact"
    APPROXIMATE = "approximate"
    METADATA_DERIVED = "metadata_derived"
    UNAVAILABLE = "unavailable"


class ColumnProfileKind(StrEnum):
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    STRING = "string"
    TEMPORAL = "temporal"
    IDENTIFIER_LIKE = "identifier_like"
    ARRAY = "array"
    MAP = "map"
    STRUCT = "struct"
    VARIANT = "variant"
    BINARY = "binary"
    UNSUPPORTED = "unsupported"


class ValueRetentionPolicy(StrEnum):
    ALLOW_SAFE_VALUES = "allow_safe_values"
    REDACT_VALUES = "redact_values"
    NO_VALUES = "no_values"


@dataclass(frozen=True, slots=True)
class MetricEvidence:
    value: Any = None
    method: MetricMethod = MetricMethod.UNAVAILABLE
    population_count: int | None = None
    sample_fraction: float | None = None
    approximation: dict[str, Any] = field(default_factory=dict)
    warning: str | None = None
    population_scope: PopulationScope = PopulationScope.FULL
    calculation_method: CalculationMethod = CalculationMethod.EXACT
    sample_seed: int | None = None
    algorithm: str | None = None
    algorithm_parameters: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        value = {**asdict(self), "method": self.method.value}
        value["population_scope"] = self.population_scope.value
        value["calculation_method"] = self.calculation_method.value
        return value


@dataclass(frozen=True, slots=True)
class TableProfileRequest:
    source_table: str
    mode: ProfileMode = ProfileMode.QUICK
    column_allowlist: tuple[str, ...] = ()
    column_denylist: tuple[str, ...] = ()
    sample_fraction: float = 0.1
    sample_seed: int = 42
    max_category_values: int = 100
    category_cardinality_threshold: int = 100
    category_ratio_threshold: float = 0.05
    identifier_uniqueness_threshold: float = 0.99
    outlier_methods: tuple[str, ...] = ("iqr", "percentile")
    percentile_accuracy: int = 10_000
    business_event_column: str | None = None
    conditional_null_segments: tuple[str, ...] = ()
    recent_windows_days: tuple[int, ...] = (1, 7, 30)
    sentinel_candidates: dict[str, tuple[Any, ...]] = field(default_factory=dict)
    expected_string_patterns: dict[str, tuple[str, ...]] = field(default_factory=dict)
    value_retention_policy: ValueRetentionPolicy = ValueRetentionPolicy.REDACT_VALUES
    sensitive_value_retention_policy: ValueRetentionPolicy = ValueRetentionPolicy.NO_VALUES
    profile_catalog: str = "sda_dev"
    profile_schema: str = "profiles"
    reuse_existing: bool = True
    allow_best_effort_snapshot: bool = True

    def __post_init__(self) -> None:
        parts = self.source_table.split(".")
        if len(parts) != 3 or any(not part.strip() for part in parts):
            raise ValueError("source_table must be a three-level catalog.schema.object name")
        if not 0 < self.sample_fraction <= 1:
            raise ValueError("sample_fraction must be in (0, 1]")
        if self.mode is ProfileMode.FULL and self.sample_fraction != 1.0:
            raise ValueError("FULL mode requires sample_fraction=1.0")
        if set(self.column_allowlist) & set(self.column_denylist):
            raise ValueError("column allowlist and denylist overlap")
        if not 0 < self.max_category_values <= 10_000:
            raise ValueError("max_category_values must be between 1 and 10000")
        if not 0 < self.category_ratio_threshold <= 1:
            raise ValueError("category_ratio_threshold must be in (0, 1]")
        if len(self.conditional_null_segments) > 5:
            raise ValueError("at most five conditional-null segment columns are allowed")
        if any(window <= 0 for window in self.recent_windows_days):
            raise ValueError("recent windows must be positive")

    @property
    def configuration_hash(self) -> str:
        payload = asdict(self)
        payload["mode"] = self.mode.value
        payload["value_retention_policy"] = self.value_retention_policy.value
        payload["sensitive_value_retention_policy"] = self.sensitive_value_retention_policy.value
        return sha256_json(payload)


@dataclass(frozen=True, slots=True)
class ColumnProfile:
    column_name: str
    ordinal_position: int
    declared_data_type: str
    declared_nullable: bool | None
    profile_kind: ColumnProfileKind
    classification_evidence: tuple[str, ...] = ()
    sensitivity_signals: tuple[str, ...] = ()
    value_retention_policy: ValueRetentionPolicy = ValueRetentionPolicy.NO_VALUES
    row_count_basis: MetricEvidence = field(default_factory=MetricEvidence)
    non_null_count: MetricEvidence = field(default_factory=MetricEvidence)
    null_count: MetricEvidence = field(default_factory=MetricEvidence)
    null_rate: MetricEvidence = field(default_factory=MetricEvidence)
    metrics: dict[str, MetricEvidence] = field(default_factory=dict)
    outlier_findings: tuple[dict[str, Any], ...] = ()
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["profile_kind"] = self.profile_kind.value
        payload["value_retention_policy"] = self.value_retention_policy.value
        for key in ("row_count_basis", "non_null_count", "null_count", "null_rate"):
            payload[key] = getattr(self, key).to_dict()
        payload["metrics"] = {key: value.to_dict() for key, value in self.metrics.items()}
        return payload


@dataclass(frozen=True, slots=True)
class TableProfile:
    profile_id: str
    profile_schema_version: str
    source_table: str
    source_object_type: str
    source_version: str | None
    snapshot_kind: str
    snapshot_reproducible: bool
    metadata_fingerprint: str | None
    profile_started_at: str
    profile_completed_at: str
    profile_reference_time: str
    profile_mode: ProfileMode
    tool_name: str
    tool_version: str
    configuration: dict[str, Any]
    configuration_hash: str
    session_timezone: str
    source_row_count: MetricEvidence
    sample_row_count: MetricEvidence
    sample_fraction: float
    sample_seed: int
    sampling_method: str
    column_count: int
    profiled_column_count: int
    skipped_columns: tuple[dict[str, Any], ...]
    column_profiles: tuple[ColumnProfile, ...]
    storage_freshness: dict[str, Any] = field(default_factory=dict)
    business_freshness: dict[str, Any] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()
    agent_summary: str = ""
    artifact_locations: dict[str, str] = field(default_factory=dict)
    metadata_inventory_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["profile_mode"] = self.profile_mode.value
        payload["source_row_count"] = self.source_row_count.to_dict()
        payload["sample_row_count"] = self.sample_row_count.to_dict()
        payload["column_profiles"] = [column.to_dict() for column in self.column_profiles]
        return payload


def sha256_json(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()
