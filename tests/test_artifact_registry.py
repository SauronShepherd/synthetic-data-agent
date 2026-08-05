from sda.artifacts.delta import persist_artifact_registry
from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType


class Writer:
    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []

    def format(self, value: str) -> "Writer":
        assert value == "delta"
        return self

    def mode(self, value: str) -> "Writer":
        assert value == "append"
        return self

    def saveAsTable(self, value: str) -> None:
        assert value == "main.evidence.sda_artifact_registry"


class Frame:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.write = Writer()
        self.rows = rows


class Spark:
    def createDataFrame(self, rows: list[dict[str, object]]) -> Frame:
        assert rows[0]["artifact_type"] == "table_profile"
        assert rows[0]["status"] == "complete"
        return Frame(rows)


def test_persist_artifact_registry_writes_typed_identity_fields() -> None:
    ref = ArtifactRef(
        artifact_id="profile-1",
        artifact_type=ArtifactType.TABLE_PROFILE,
        artifact_schema_version="1.0",
        status=ArtifactStatus.COMPLETE,
        tool_name="profiler",
        tool_version="0.6",
        run_id="run-1",
        environment="dev",
        created_at="now",
        configuration_hash="cfg",
        primary_location="main.evidence.profile",
        related_locations={},
        source_references=(),
        checksum="checksum",
        summary="summary",
        strategy_version="profile-v1",
    )

    persist_artifact_registry(Spark(), ref, "main.evidence.sda_artifact_registry")
