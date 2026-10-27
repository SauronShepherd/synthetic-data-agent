from dataclasses import replace

import pytest

from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType, SourceReference
from sda.artifacts.registry import InMemoryArtifactRegistry
from sda.patterns.inputs import require_pattern_inputs
from sda.patterns.loaders import load_pattern_inputs
from sda.patterns.models import PatternInputRefs
from sda.runtime.errors import ArtifactCompatibilityError, ArtifactNotFoundError


def ref(artifact_id: str, artifact_type: ArtifactType, schema: str = "1.0") -> ArtifactRef:
    return ArtifactRef(
        artifact_id,
        artifact_type,
        schema,
        ArtifactStatus.COMPLETE,
        "test",
        "1",
        "run",
        "dev",
        "now",
        "config",
        "memory",
        {},
        (),
        "checksum",
        "summary",
    )


def inputs() -> PatternInputRefs:
    return PatternInputRefs("metadata", ("profile",), "relationship", "graph")


def artifacts() -> tuple[ArtifactRef, ...]:
    return (
        ref("metadata", ArtifactType.METADATA_INVENTORY),
        ref("profile", ArtifactType.TABLE_PROFILE),
        ref("relationship", ArtifactType.RELATIONSHIP_ANALYSIS),
        ref("graph", ArtifactType.DEPENDENCY_GRAPH),
    )


def test_pattern_inputs_require_exact_types_and_versions() -> None:
    assert len(require_pattern_inputs(artifacts(), inputs(), environment="dev")) == 4
    wrong_type = (
        *artifacts()[:1],
        ref("profile", ArtifactType.TABLE_PROFILE, "2.0"),
        *artifacts()[2:],
    )
    with pytest.raises(ArtifactCompatibilityError, match="schema version"):
        require_pattern_inputs(wrong_type, inputs(), environment="dev")


def test_pattern_inputs_reject_duplicate_ids_instead_of_last_write_wins() -> None:
    with pytest.raises(ArtifactCompatibilityError, match="duplicate"):
        require_pattern_inputs((*artifacts(), artifacts()[0]), inputs(), environment="dev")


def test_pattern_inputs_reject_cross_inventory_and_graph_lineage() -> None:
    metadata, profile, relationship, graph = artifacts()
    wrong_profile = replace(profile, content={"metadata_inventory_id": "another-inventory"})
    with pytest.raises(ArtifactCompatibilityError, match="different metadata inventory"):
        require_pattern_inputs(
            (metadata, wrong_profile, relationship, graph), inputs(), environment="dev"
        )
    wrong_graph = replace(graph, input_artifact_ids=("another-relationship",))
    with pytest.raises(ArtifactCompatibilityError, match="different relationship"):
        require_pattern_inputs(
            (metadata, profile, relationship, wrong_graph), inputs(), environment="dev"
        )


def test_pattern_inputs_reject_incompatible_source_versions() -> None:
    metadata, profile, relationship, graph = artifacts()
    profile = replace(
        profile,
        source_references=(
            SourceReference("main.sales.orders", "TABLE", "delta_version", "1", None, None),
        ),
    )
    relationship = replace(
        relationship,
        source_references=(
            SourceReference("main.sales.orders", "TABLE", "delta_version", "2", None, None),
        ),
    )
    with pytest.raises(ArtifactCompatibilityError, match="incompatible source versions"):
        require_pattern_inputs(
            (metadata, profile, relationship, graph), inputs(), environment="dev"
        )


def test_legacy_registry_fallback_still_validates_complete_input_set() -> None:
    central = InMemoryArtifactRegistry()
    relationships = InMemoryArtifactRegistry()
    graph = InMemoryArtifactRegistry()
    for item in artifacts()[:2]:
        central.put(item)
    relationships.put(artifacts()[2])
    graph.put(artifacts()[3])
    resolved = load_pattern_inputs(
        central,
        inputs(),
        environment="dev",
        relationship_legacy=relationships,
        dependency_graph_legacy=graph,
    )
    assert tuple(item.artifact_id for item in resolved) == (
        "metadata",
        "profile",
        "relationship",
        "graph",
    )

    graph.put(replace(artifacts()[3], environment="prod"))
    with pytest.raises(ArtifactCompatibilityError, match="environment mismatch"):
        load_pattern_inputs(
            central,
            inputs(),
            environment="dev",
            relationship_legacy=relationships,
            dependency_graph_legacy=graph,
        )


def test_pattern_loader_uses_latest_complete_rows_from_shared_registry() -> None:
    class HistoricalRegistry(InMemoryArtifactRegistry):
        def require_complete(self, artifact_id: str) -> ArtifactRef:
            raise AssertionError("shared loader must resolve the latest complete row")

        def require_latest_complete(self, artifact_id: str) -> ArtifactRef:
            ref = self.get(artifact_id)
            if ref is None:
                raise ArtifactNotFoundError(
                    "artifact was not found", details={"artifact_id": artifact_id}
                )
            if ref.status is not ArtifactStatus.COMPLETE:
                raise ArtifactCompatibilityError(
                    "artifact is not complete", details={"artifact_id": artifact_id}
                )
            return ref

    central = HistoricalRegistry()
    for item in artifacts()[:2]:
        central.put(item)
    relationships = InMemoryArtifactRegistry()
    graph = InMemoryArtifactRegistry()
    relationships.put(artifacts()[2])
    graph.put(artifacts()[3])

    resolved = load_pattern_inputs(
        central,
        inputs(),
        environment="dev",
        relationship_legacy=relationships,
        dependency_graph_legacy=graph,
    )

    assert tuple(item.artifact_id for item in resolved) == (
        "metadata",
        "profile",
        "relationship",
        "graph",
    )


def test_incomplete_central_artifact_cannot_be_bypassed_by_legacy_fallback() -> None:
    central = InMemoryArtifactRegistry()
    relationships = InMemoryArtifactRegistry()
    for item in artifacts():
        central.put(item)
    central.put(replace(artifacts()[2], status=ArtifactStatus.WRITING))
    relationships.put(artifacts()[2])
    with pytest.raises(ArtifactCompatibilityError, match="not complete"):
        load_pattern_inputs(central, inputs(), environment="dev", relationship_legacy=relationships)
    missing = InMemoryArtifactRegistry()
    for item in (artifacts()[0], artifacts()[1], artifacts()[3]):
        missing.put(item)
    with pytest.raises(ArtifactNotFoundError):
        load_pattern_inputs(missing, inputs(), environment="dev")
