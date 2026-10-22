from __future__ import annotations

from collections.abc import Iterable

from sda.artifacts.compatibility import require_same_environment
from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType
from sda.patterns.models import PatternInputRefs
from sda.runtime.errors import ArtifactCompatibilityError, ArtifactNotFoundError

PATTERN_INPUT_SCHEMA_VERSIONS: dict[ArtifactType, frozenset[str]] = {
    ArtifactType.METADATA_INVENTORY: frozenset({"1.0"}),
    ArtifactType.TABLE_PROFILE: frozenset({"1.0"}),
    ArtifactType.RELATIONSHIP_ANALYSIS: frozenset({"1.0"}),
    ArtifactType.DEPENDENCY_GRAPH: frozenset({"1.0"}),
}


def require_pattern_inputs(
    refs: Iterable[ArtifactRef], requested: PatternInputRefs, *, environment: str
) -> tuple[ArtifactRef, ...]:
    values = tuple(refs)
    if len({ref.artifact_id for ref in values}) != len(values):
        raise ArtifactCompatibilityError(
            "pattern inputs contain duplicate artifact identifiers",
            details={"reason_code": "duplicate_artifact_id"},
        )
    by_id = {ref.artifact_id: ref for ref in values}
    required = (
        requested.metadata_artifact_id,
        *requested.profile_artifact_ids,
        requested.relationship_artifact_id,
        requested.dependency_graph_artifact_id,
    )
    missing = [artifact_id for artifact_id in required if artifact_id not in by_id]
    if missing:
        raise ArtifactNotFoundError(
            "required pattern input artifact is missing",
            details={"artifact_ids": ",".join(missing)},
        )
    selected = tuple(by_id[artifact_id] for artifact_id in required)
    if any(ref.status is not ArtifactStatus.COMPLETE for ref in selected):
        raise ArtifactCompatibilityError(
            "pattern inputs must be COMPLETE", details={"reason_code": "artifact_not_complete"}
        )
    if any(ref.environment != environment for ref in selected):
        raise ArtifactCompatibilityError(
            "pattern input environment mismatch",
            details={"reason_code": "artifact_environment_mismatch"},
        )
    expected_types = (
        ArtifactType.METADATA_INVENTORY,
        *(ArtifactType.TABLE_PROFILE for _ in requested.profile_artifact_ids),
        ArtifactType.RELATIONSHIP_ANALYSIS,
        ArtifactType.DEPENDENCY_GRAPH,
    )
    for ref, expected in zip(selected, expected_types, strict=True):
        if ref.artifact_type is not expected:
            raise ArtifactCompatibilityError(
                "pattern input has wrong artifact type",
                details={
                    "artifact_id": ref.artifact_id,
                    "expected_type": expected.value,
                    "actual_type": ref.artifact_type.value,
                },
            )
        supported = PATTERN_INPUT_SCHEMA_VERSIONS[expected]
        if ref.artifact_schema_version not in supported:
            raise ArtifactCompatibilityError(
                "pattern input schema version is unsupported",
                details={
                    "artifact_id": ref.artifact_id,
                    "artifact_schema_version": ref.artifact_schema_version,
                    "supported_versions": ",".join(sorted(supported)),
                },
            )
    for profile in selected[1:]:
        inventory_ids = {
            value
            for value in (
                profile.content.get("metadata_inventory_id"),
                *(source.metadata_inventory_id for source in profile.source_references),
            )
            if value
        }
        if inventory_ids and inventory_ids != {requested.metadata_artifact_id}:
            raise ArtifactCompatibilityError(
                "pattern input references a different metadata inventory",
                details={"artifact_id": profile.artifact_id},
            )
    relationship, graph = selected[-2:]
    graph_relationship_ids = {
        value
        for value in (
            graph.related_locations.get("relationship_analysis_id"),
            *graph.input_artifact_ids,
        )
        if value
    }
    if graph_relationship_ids and graph_relationship_ids != {relationship.artifact_id}:
        raise ArtifactCompatibilityError(
            "dependency graph references a different relationship analysis",
            details={"artifact_id": graph.artifact_id},
        )
    source_versions: dict[str, set[str]] = {}
    for ref in selected:
        for source in ref.source_references:
            if source.snapshot_kind == "delta_version" and source.source_version:
                source_versions.setdefault(source.full_name, set()).add(source.source_version)
    incompatible = sorted(table for table, versions in source_versions.items() if len(versions) > 1)
    if incompatible:
        raise ArtifactCompatibilityError(
            "pattern inputs reference incompatible source versions",
            details={"source_tables": ",".join(incompatible)},
        )
    require_same_environment(*selected)
    return selected
