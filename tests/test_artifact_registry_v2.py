import json

from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType, SourceReference
from sda.artifacts.registry import (
    InMemoryArtifactRegistry,
    _artifact_ref_from_row,
    artifact_ref_from_registry_row,
    artifact_ref_to_registry_row,
)


def make_ref(
    *,
    run_id: str = "run",
    status: ArtifactStatus = ArtifactStatus.COMPLETE,
    artifact_id: str = "pat-1",
) -> ArtifactRef:
    return ArtifactRef(
        artifact_id=artifact_id,
        artifact_type=ArtifactType.PATTERN_REGISTRY,
        artifact_schema_version="2.0",
        status=status,
        tool_name="patterns",
        tool_version="0.7",
        run_id=run_id,
        environment="dev",
        created_at="now",
        configuration_hash="cfg",
        primary_location="sda_dev.profiles.pattern_registry",
        related_locations={"evidence": "sda_dev.profiles.pattern_evidence"},
        source_references=(
            SourceReference("sda_dev.s.t", "TABLE", "delta_version", "4", None, "m", ("a",)),
        ),
        checksum="sum",
        summary="safe",
        warnings=("warning",),
        input_artifact_ids=("z", "a"),
    )


def test_registry_v2_serializes_lineage_deterministically() -> None:
    row = artifact_ref_to_registry_row(make_ref())
    assert row["registry_schema_version"] == "2"
    assert json.loads(str(row["input_artifact_ids_json"])) == ["a", "z"]
    assert "sda_dev.s.t" in str(row["source_references_json"])


def test_registry_reuse_excludes_failed_and_is_content_based() -> None:
    store = InMemoryArtifactRegistry()
    complete = make_ref(run_id="one")
    failed = make_ref(run_id="two", status=ArtifactStatus.FAILED, artifact_id="pat-2")
    store.put(complete)
    store.put(failed)
    assert (
        store.find_reusable(
            artifact_type=ArtifactType.PATTERN_REGISTRY,
            reuse_fingerprint=complete.reuse_fingerprint,
            environment="dev",
        )
        is complete
    )
    assert (
        store.find_reusable(
            artifact_type=ArtifactType.PATTERN_REGISTRY,
            reuse_fingerprint=complete.reuse_fingerprint,
            environment="prod",
        )
        is None
    )


def test_registry_v2_row_rehydrates_lineage() -> None:
    ref = make_ref()
    row = type(
        "Row", (), {"asDict": lambda self, recursive=True: artifact_ref_to_registry_row(ref)}
    )()
    hydrated = _artifact_ref_from_row(row)
    assert hydrated.artifact_id == ref.artifact_id
    assert hydrated.related_locations == ref.related_locations
    assert hydrated.input_artifact_ids == ("a", "z")


def test_legacy_registry_row_is_read_with_warning() -> None:
    ref = make_ref()
    raw = artifact_ref_to_registry_row(ref)
    raw.pop("registry_schema_version")
    legacy = type("Row", (), {"asDict": lambda self, recursive=True: raw})()
    loaded = artifact_ref_from_registry_row(legacy)
    assert "legacy_artifact_registry_v1_missing_lineage_fields" in loaded.warnings
