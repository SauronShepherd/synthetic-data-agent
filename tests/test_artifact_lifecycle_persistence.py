from sda.artifacts.delta import persist_artifact_lifecycle
from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType
from sda.runtime.errors import PersistenceError


class Writer:
    def format(self, value: str) -> "Writer":
        return self

    def mode(self, value: str) -> "Writer":
        return self

    def saveAsTable(self, value: str) -> None:
        return None


class Spark:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def createDataFrame(self, rows: list[dict[str, object]]) -> object:
        self.calls.append((str(rows[0]["artifact_id"]), str(rows[0]["status"])))
        return type("Frame", (), {"write": Writer()})()


class BrokenSpark(Spark):
    def createDataFrame(self, rows: list[dict[str, object]]) -> object:
        raise RuntimeError("persistence failure")


def test_lifecycle_publishes_writing_before_complete() -> None:
    ref = ArtifactRef(
        artifact_id="a",
        artifact_type=ArtifactType.TABLE_PROFILE,
        artifact_schema_version="1.0",
        status=ArtifactStatus.WRITING,
        tool_name="profiler",
        tool_version="0.6",
        run_id="run",
        environment="dev",
        created_at="now",
        configuration_hash="cfg",
        primary_location="main.evidence.profile",
        related_locations={},
        source_references=(),
        checksum="checksum",
        summary="summary",
    )
    spark = Spark()

    completed = persist_artifact_lifecycle(
        spark,
        ref,
        [{"value": 1}],
        evidence_location="main.evidence.profile",
        registry_location="main.evidence.registry",
    )

    assert completed.status is ArtifactStatus.COMPLETE
    assert completed.completed_at is not None
    assert [status for _, status in spark.calls] == ["writing", "writing", "complete", "complete"]


def test_lifecycle_failure_is_typed_and_not_marked_complete() -> None:
    ref = ArtifactRef(
        artifact_id="failed",
        artifact_type=ArtifactType.TABLE_PROFILE,
        artifact_schema_version="1.0",
        status=ArtifactStatus.WRITING,
        tool_name="profiler",
        tool_version="0.6",
        run_id="run",
        environment="dev",
        created_at="now",
        configuration_hash="cfg",
        primary_location="main.evidence.profile",
        related_locations={},
        source_references=(),
        checksum="checksum",
        summary="summary",
    )

    try:
        persist_artifact_lifecycle(
            BrokenSpark(),
            ref,
            [{"value": 1}],
            evidence_location="main.evidence.profile",
            registry_location="main.evidence.registry",
        )
    except PersistenceError:
        pass
    else:
        raise AssertionError("persistence failure must be typed")
