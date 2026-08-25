from __future__ import annotations

from collections.abc import Iterable

from sda.artifacts.compatibility import require_same_environment
from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType
from sda.patterns.models import PatternInputRefs
from sda.runtime.errors import ArtifactCompatibilityError, ArtifactNotFoundError


def require_pattern_inputs(
    refs: Iterable[ArtifactRef], requested: PatternInputRefs, *, environment: str
) -> tuple[ArtifactRef, ...]:
    values = tuple(refs)
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
    if selected[0].artifact_type is not ArtifactType.METADATA_INVENTORY:
        raise ArtifactCompatibilityError("metadata input has wrong artifact type")
    require_same_environment(*selected)
    return selected
