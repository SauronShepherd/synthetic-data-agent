from __future__ import annotations

from sda.artifacts.registry import ArtifactRegistryStore
from sda.patterns.inputs import require_pattern_inputs
from sda.patterns.models import PatternInputRefs


def load_pattern_inputs(store: ArtifactRegistryStore, refs: PatternInputRefs, *, environment: str):
    artifacts = tuple(
        store.require_complete(artifact_id)
        for artifact_id in (
            refs.metadata_artifact_id,
            *refs.profile_artifact_ids,
            refs.relationship_artifact_id,
            refs.dependency_graph_artifact_id,
        )
    )
    return require_pattern_inputs(artifacts, refs, environment=environment)
