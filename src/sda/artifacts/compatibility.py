"""Fail-fast compatibility checks for artifact handoffs."""

from __future__ import annotations

from collections.abc import Iterable

from sda.artifacts.models import ArtifactRef, ArtifactStatus, SourceReference
from sda.runtime.errors import ArtifactCompatibilityError


def require_supported_schema(ref: ArtifactRef, supported: set[str]) -> None:
    if ref.artifact_schema_version not in supported:
        raise ArtifactCompatibilityError(
            f"unsupported artifact schema: {ref.artifact_schema_version}",
            details={"artifact_id": ref.artifact_id, "schema_version": ref.artifact_schema_version},
        )
    if ref.status is not ArtifactStatus.COMPLETE:
        raise ArtifactCompatibilityError(
            f"artifact is not reusable: {ref.artifact_id} ({ref.status.value})",
            details={"artifact_id": ref.artifact_id, "status": ref.status.value},
        )


def require_same_environment(*refs: ArtifactRef) -> None:
    environments = {ref.environment for ref in refs}
    if len(environments) > 1:
        raise ArtifactCompatibilityError(
            "artifact environments do not match",
            details={"environments": ",".join(sorted(environments))},
        )


def require_complete(ref: ArtifactRef) -> None:
    require_supported_schema(ref, {ref.artifact_schema_version})


def require_environment(ref: ArtifactRef, expected_environment: str) -> None:
    require_complete(ref)
    if ref.environment != expected_environment:
        raise ArtifactCompatibilityError(
            "artifact environment does not match execution environment",
            details={
                "artifact_environment": ref.environment,
                "expected_environment": expected_environment,
            },
        )


def require_input_lineage(ref: ArtifactRef, expected_input_artifact_ids: set[str]) -> None:
    actual = set(ref.input_artifact_ids)
    if actual != expected_input_artifact_ids:
        raise ArtifactCompatibilityError(
            "artifact input lineage does not match",
            details={"artifact_id": ref.artifact_id},
        )


def require_source_scope(ref: ArtifactRef, expected_source_refs: Iterable[SourceReference]) -> None:
    expected = {
        (source.full_name, source.source_version, source.snapshot_kind)
        for source in expected_source_refs
    }
    actual = {
        (source.full_name, source.source_version, source.snapshot_kind)
        for source in ref.source_references
    }
    if not expected.issubset(actual):
        raise ArtifactCompatibilityError(
            "artifact source scope does not match", details={"artifact_id": ref.artifact_id}
        )


def require_source_compatibility(
    metadata_ref: ArtifactRef, profile_refs: Iterable[ArtifactRef]
) -> None:
    allowed = {source.full_name for source in metadata_ref.source_references}
    for ref in profile_refs:
        for source in ref.source_references:
            if source.full_name not in allowed:
                raise ArtifactCompatibilityError(
                    f"profile source is outside metadata inventory: {source.full_name}",
                    details={"source": source.full_name},
                )


def require_current_source_snapshot(profile_ref: ArtifactRef, requested_source: str) -> None:
    """Reject a profile whose source does not match the requested relation."""
    matches = [
        source for source in profile_ref.source_references if source.full_name == requested_source
    ]
    if not matches:
        raise ArtifactCompatibilityError(
            f"profile does not describe requested source: {requested_source}",
            details={"requested_source": requested_source},
        )
    if any(source.snapshot_kind == "failed" for source in matches):
        raise ArtifactCompatibilityError(
            f"profile source snapshot is failed: {requested_source}",
            details={"requested_source": requested_source, "snapshot_kind": "failed"},
        )


def require_profile_columns(profile_ref: ArtifactRef, required_columns: set[str]) -> None:
    selected = {
        column for source in profile_ref.source_references for column in source.selected_columns
    }
    missing = required_columns - selected
    if missing:
        raise ArtifactCompatibilityError(
            f"profile omits required columns: {sorted(missing)}",
            details={"missing_columns": ",".join(sorted(missing))},
        )
