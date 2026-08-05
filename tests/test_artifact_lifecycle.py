import pytest

from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType


def make_ref() -> ArtifactRef:
    return ArtifactRef(
        "a",
        ArtifactType.TABLE_PROFILE,
        "1.0",
        ArtifactStatus.WRITING,
        "tool",
        "0.6.0",
        "run",
        "dev",
        "now",
        "cfg",
        "main.evidence.profile",
        {},
        (),
        "checksum",
        "summary",
    )


def test_artifact_lifecycle_allows_completion_but_not_reopening() -> None:
    complete = make_ref().transition(ArtifactStatus.COMPLETE, completed_at="later")
    assert complete.completed_at == "later"
    with pytest.raises(ValueError):
        complete.transition(ArtifactStatus.WRITING)
