from __future__ import annotations

from typing import Protocol

from sda.artifacts.models import ArtifactRef, ArtifactType
from sda.artifacts.registry import ArtifactRegistryStore
from sda.patterns.inputs import require_pattern_inputs
from sda.patterns.models import PatternInputRefs
from sda.runtime.errors import ArtifactNotFoundError


class LegacyPatternRegistry(Protocol):
    def require_latest_complete(self, artifact_id: str) -> ArtifactRef: ...


def load_pattern_inputs(
    store: ArtifactRegistryStore,
    refs: PatternInputRefs,
    *,
    environment: str,
    relationship_legacy: LegacyPatternRegistry | None = None,
    dependency_graph_legacy: LegacyPatternRegistry | None = None,
) -> tuple[ArtifactRef, ...]:
    requested = (
        (refs.metadata_artifact_id, ArtifactType.METADATA_INVENTORY),
        *((artifact_id, ArtifactType.TABLE_PROFILE) for artifact_id in refs.profile_artifact_ids),
        (refs.relationship_artifact_id, ArtifactType.RELATIONSHIP_ANALYSIS),
        (refs.dependency_graph_artifact_id, ArtifactType.DEPENDENCY_GRAPH),
    )
    artifacts: list[ArtifactRef] = []
    for artifact_id, artifact_type in requested:
        try:
            # A shared registry can contain historical rows for the same
            # artifact ID after retries. Resolve the newest complete row
            # instead of treating those expected lifecycle rows as a
            # duplicate-input failure.
            artifact = store.require_latest_complete(artifact_id)
        except ArtifactNotFoundError:
            legacy = (
                relationship_legacy
                if artifact_type is ArtifactType.RELATIONSHIP_ANALYSIS
                else dependency_graph_legacy
                if artifact_type is ArtifactType.DEPENDENCY_GRAPH
                else None
            )
            if legacy is None:
                raise
            artifact = legacy.require_latest_complete(artifact_id)
        artifacts.append(artifact)
    return require_pattern_inputs(artifacts, refs, environment=environment)
