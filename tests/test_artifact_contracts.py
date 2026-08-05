import pytest

from sda.artifacts.compatibility import (
    require_current_source_snapshot,
    require_same_environment,
    require_supported_schema,
)
from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType, SourceReference
from sda.runtime.errors import ArtifactCompatibilityError


def make_ref(
    environment: str = "dev", status: ArtifactStatus = ArtifactStatus.COMPLETE
) -> ArtifactRef:
    return ArtifactRef(
        "id",
        ArtifactType.TABLE_PROFILE,
        "1.0",
        status,
        "tool",
        "0.6.0",
        "run",
        environment,
        "now",
        "hash",
        "db.schema.table",
        {},
        (),
        "checksum",
        "summary",
    )


def test_artifact_compatibility_rejects_partial_and_mixed_environment() -> None:
    with pytest.raises(ValueError):
        require_supported_schema(make_ref(status=ArtifactStatus.WRITING), {"1.0"})
    with pytest.raises(ValueError):
        require_same_environment(make_ref(), make_ref("prod"))


def test_snapshot_compatibility_rejects_wrong_source() -> None:
    ref = ArtifactRef(
        "id",
        ArtifactType.TABLE_PROFILE,
        "1.0",
        ArtifactStatus.COMPLETE,
        "tool",
        "0.6.0",
        "run",
        "dev",
        "now",
        "hash",
        "db.schema.table",
        {},
        (SourceReference("main.sales.orders", "TABLE", "delta_version", "4", None, "fp"),),
        "checksum",
        "summary",
    )
    with pytest.raises(ValueError):
        require_current_source_snapshot(ref, "main.crm.customers")


def test_compatibility_failure_is_structured_and_value_error_compatible() -> None:
    with pytest.raises(ArtifactCompatibilityError) as error:
        require_supported_schema(make_ref(), {"2.0"})

    assert isinstance(error.value, ValueError)
    assert error.value.to_dict() == {
        "error_code": "ARTIFACT_COMPATIBILITY",
        "message": "unsupported artifact schema: 1.0",
        "details": {"artifact_id": "id", "schema_version": "1.0"},
    }
