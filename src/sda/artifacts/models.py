"""JSON-safe artifact references for durable SDA evidence."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

from sda.artifacts.fingerprint import fingerprint


class ArtifactType(StrEnum):
    METADATA_INVENTORY = "metadata_inventory"
    TABLE_PROFILE = "table_profile"
    RELATIONSHIP_ANALYSIS = "relationship_analysis"
    DEPENDENCY_GRAPH = "dependency_graph"
    RUN_MANIFEST = "run_manifest"
    PATTERN_REGISTRY = "pattern_registry"
    GENERATION_PLAN = "generation_plan"
    VALIDATION_REPORT = "validation_report"
    PUBLICATION = "publication"


class ArtifactStatus(StrEnum):
    WRITING = "writing"
    COMPLETE = "complete"
    FAILED = "failed"
    SUPERSEDED = "superseded"


@dataclass(frozen=True, slots=True)
class SourceReference:
    full_name: str
    object_type: str
    snapshot_kind: str
    source_version: str | None
    snapshot_timestamp: str | None
    metadata_fingerprint: str | None
    selected_columns: tuple[str, ...] = ()
    metadata_inventory_id: str | None = None

    def __post_init__(self) -> None:
        if not self.full_name.strip():
            raise ValueError("source full_name must not be empty")
        if self.snapshot_kind not in {
            "delta_version",
            "timestamp",
            "best_effort",
            "metadata_only",
            "failed",
        }:
            raise ValueError(f"unsupported snapshot kind: {self.snapshot_kind}")
        if self.snapshot_kind == "delta_version" and not self.source_version:
            raise ValueError("delta_version references require source_version")


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    artifact_id: str
    artifact_type: ArtifactType
    artifact_schema_version: str
    status: ArtifactStatus
    tool_name: str
    tool_version: str
    run_id: str
    environment: str
    created_at: str
    configuration_hash: str
    primary_location: str
    related_locations: dict[str, str]
    source_references: tuple[SourceReference, ...]
    checksum: str
    summary: str
    warnings: tuple[str, ...] = ()
    strategy_version: str = "v1"
    completed_at: str | None = None
    reuse_fingerprint: str = ""
    content_checksum: str | None = None
    input_artifact_ids: tuple[str, ...] = ()
    error_code: str | None = None
    error_message_safe: str | None = None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["artifact_type"] = self.artifact_type.value
        value["status"] = self.status.value
        value["source_references"] = [asdict(item) for item in self.source_references]
        return value

    @property
    def effective_content_checksum(self) -> str | None:
        """Return the v6 checksum name while accepting the legacy field shape."""
        return self.content_checksum or self.checksum

    def transition(self, status: ArtifactStatus, *, completed_at: str | None = None) -> ArtifactRef:
        allowed = {
            ArtifactStatus.WRITING: {ArtifactStatus.COMPLETE, ArtifactStatus.FAILED},
            ArtifactStatus.COMPLETE: {ArtifactStatus.SUPERSEDED},
            ArtifactStatus.FAILED: {ArtifactStatus.SUPERSEDED},
            ArtifactStatus.SUPERSEDED: set(),
        }
        if status not in allowed[self.status]:
            raise ValueError(f"invalid artifact transition: {self.status.value} -> {status.value}")
        from dataclasses import replace

        if status is ArtifactStatus.COMPLETE and completed_at is None:
            from datetime import UTC, datetime

            completed_at = datetime.now(UTC).isoformat()

        return replace(
            self,
            status=status,
            completed_at=completed_at if status is ArtifactStatus.COMPLETE else self.completed_at,
        )

    def __post_init__(self) -> None:
        for name in (
            "artifact_id",
            "artifact_schema_version",
            "tool_name",
            "tool_version",
            "run_id",
            "environment",
            "primary_location",
            "checksum",
        ):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must not be empty")
        if not self.reuse_fingerprint:
            object.__setattr__(
                self,
                "reuse_fingerprint",
                fingerprint(
                    {
                        "artifact_type": self.artifact_type.value,
                        "artifact_schema_version": self.artifact_schema_version,
                        "tool_name": self.tool_name,
                        "tool_version": self.tool_version,
                        "strategy_version": self.strategy_version,
                        "configuration_hash": self.configuration_hash,
                        "input_artifact_ids": self.input_artifact_ids,
                        "source_references": self.source_references,
                    }
                ),
            )
