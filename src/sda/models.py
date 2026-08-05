"""Core contracts for the Article 02 architecture."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class RunStage(StrEnum):
    """Ordered stages in the designed synthetic-data workflow."""

    RECEIVED = "received"
    METADATA_DISCOVERED = "metadata_discovered"
    PROFILED = "profiled"
    RELATIONSHIPS_MAPPED = "relationships_mapped"
    PATTERNS_DETECTED = "patterns_detected"
    PLAN_DRAFTED = "plan_drafted"
    GENERATED = "generated"
    VALIDATED = "validated"
    PUBLISHED = "published"
    FAILED = "failed"


class ToolName(StrEnum):
    """Specialized deterministic tools named in Article 02."""

    UC_METADATA_READER = "uc_metadata_reader"
    TABLE_PROFILER = "table_profiler"
    RELATIONSHIP_DETECTOR = "relationship_detector"
    PATTERN_DETECTOR = "pattern_detector"
    GENERATION_PLANNER = "generation_planner"
    SYNTHETIC_DATA_GENERATOR = "synthetic_data_generator"
    QUALITY_VALIDATOR = "quality_validator"
    PUBLISHER = "publisher"


@dataclass(frozen=True, slots=True)
class SourceScope:
    """Governed source objects selected by the user."""

    catalog: str
    schema: str
    tables: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.catalog.strip() or not self.schema.strip():
            raise ValueError("catalog and schema must not be empty")
        if not self.tables or any(not table.strip() for table in self.tables):
            raise ValueError("at least one non-empty source table is required")
        if len(set(self.tables)) != len(self.tables):
            raise ValueError("source tables must be unique")


@dataclass(frozen=True, slots=True)
class GenerationRequest:
    """Normalized user intent; no source data is stored here."""

    request_id: str
    source: SourceScope
    scale_factor: float = 1.0
    preserve_relationships: bool = True
    privacy_mode: str = "strict"
    target_catalog: str | None = None
    target_schema: str | None = None

    def __post_init__(self) -> None:
        if not self.request_id.strip():
            raise ValueError("request_id must not be empty")
        if self.scale_factor <= 0:
            raise ValueError("scale_factor must be greater than zero")
        if self.privacy_mode not in {"strict", "standard"}:
            raise ValueError("privacy_mode must be 'strict' or 'standard'")
        if (self.target_catalog is None) != (self.target_schema is None):
            raise ValueError("target_catalog and target_schema must be provided together")


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    """Legacy orchestration reference; use ``sda.artifacts.ArtifactRef`` for durable evidence."""

    artifact_id: str
    artifact_type: str
    produced_by: ToolName
    summary: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.artifact_id.strip() or not self.artifact_type.strip():
            raise ValueError("artifact_id and artifact_type must not be empty")

    def to_durable(self, *, run_id: str, environment: str, location: str, checksum: str) -> Any:
        """Convert the legacy shell reference into the durable contract."""
        from datetime import UTC, datetime

        from sda.artifacts.models import ArtifactRef as DurableArtifactRef
        from sda.artifacts.models import ArtifactStatus, ArtifactType

        artifact_type = ArtifactType(self.artifact_type)
        return DurableArtifactRef(
            artifact_id=self.artifact_id,
            artifact_type=artifact_type,
            artifact_schema_version="1.0",
            status=ArtifactStatus.COMPLETE,
            tool_name=self.produced_by.value,
            tool_version=str(self.metadata.get("tool_version", "unknown")),
            run_id=run_id,
            environment=environment,
            created_at=datetime.now(UTC).isoformat(),
            configuration_hash=str(self.metadata.get("configuration_hash", "unknown")),
            primary_location=location,
            related_locations={},
            source_references=(),
            checksum=checksum,
            summary=self.summary,
            warnings=tuple(self.metadata.get("warnings", ())),
        )


@dataclass(frozen=True, slots=True)
class ToolResult:
    """Deterministic tool output consumed by the orchestrator."""

    tool: ToolName
    stage: RunStage
    artifacts: tuple[ArtifactRef, ...] = ()
    warnings: tuple[str, ...] = ()
    metrics: dict[str, float | int | str | bool] = field(default_factory=dict)
    durable_artifacts: tuple[Any, ...] = ()

    def to_durable_artifacts(
        self,
        *,
        run_id: str,
        environment: str,
        location_prefix: str,
        checksums: dict[str, str] | None = None,
    ) -> tuple[Any, ...]:
        """Convert legacy tool references at the orchestration boundary.

        Existing tools continue returning the small in-memory contract while
        callers that persist evidence can opt into the versioned durable
        contract in one place.  Checksums are keyed by legacy artifact id;
        missing values remain explicit rather than being fabricated.
        """
        checksums = checksums or {}
        return tuple(
            artifact.to_durable(
                run_id=run_id,
                environment=environment,
                location=f"{location_prefix.rstrip('/')}/{artifact.artifact_id}",
                checksum=checksums.get(artifact.artifact_id, "unverified"),
            )
            for artifact in self.artifacts
        )


@dataclass(slots=True)
class AgentState:
    """In-memory state model used to prove the orchestration contract."""

    request: GenerationRequest
    stage: RunStage = RunStage.RECEIVED
    artifacts: list[ArtifactRef] = field(default_factory=list)
    durable_artifacts: list[Any] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    completed_tools: list[ToolName] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable state snapshot."""
        return asdict(self)
