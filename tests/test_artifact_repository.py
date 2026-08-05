from dataclasses import replace

from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType
from sda.artifacts.store import InMemoryArtifactStore


def make_ref() -> ArtifactRef:
    return ArtifactRef(
        "a",
        ArtifactType.TABLE_PROFILE,
        "1.0",
        ArtifactStatus.COMPLETE,
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


def test_repository_reuses_only_complete_matching_fingerprint() -> None:
    store = InMemoryArtifactStore()
    ref = make_ref()
    store.put(ref, [{"artifact_id": ref.artifact_id}])

    assert store.get_complete_by_fingerprint(ref.reuse_fingerprint) == ref
    store.put(replace(ref, status=ArtifactStatus.WRITING))
    assert store.get_complete_by_fingerprint(ref.reuse_fingerprint) is None


def test_repository_can_supersede_completed_artifact() -> None:
    store = InMemoryArtifactStore()
    ref = make_ref()
    store.put(ref)

    assert store.supersede(ref.artifact_id).status is ArtifactStatus.SUPERSEDED
