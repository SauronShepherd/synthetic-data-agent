from sda.artifacts.loaders import load_artifact_ref, load_rows
from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType
from sda.artifacts.store import InMemoryArtifactStore


def test_in_memory_store_uses_the_durable_loader_boundary() -> None:
    ref = ArtifactRef(
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
    store = InMemoryArtifactStore()
    store.put(ref, [{"metric": 1}])
    loaded = load_artifact_ref(store, "a", supported_schema={"1.0"})
    assert load_rows(store, loaded) == ({"metric": 1},)
