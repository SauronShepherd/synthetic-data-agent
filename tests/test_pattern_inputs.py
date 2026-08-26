import pytest

from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType
from sda.patterns.inputs import require_pattern_inputs
from sda.patterns.models import PatternInputRefs
from sda.runtime.errors import ArtifactCompatibilityError


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
