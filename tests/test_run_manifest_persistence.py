from sda.artifacts.delta import persist_run_manifest
from sda.artifacts.manifest import RunManifest


class Writer:
    def format(self, value: str) -> "Writer":
        return self

    def mode(self, value: str) -> "Writer":
        return self

    def saveAsTable(self, value: str) -> None:
        assert value == "main.evidence.sda_run_manifests"


class Spark:
    def createDataFrame(self, rows: list[dict[str, object]]) -> object:
        assert rows[0]["status"] == "complete"
        assert rows[0]["error_message"] is None
        return type("Frame", (), {"write": Writer()})()


def test_run_manifest_persistence_is_sanitized_and_typed() -> None:
    manifest = RunManifest(
        run_id="run-1",
        tool_name="analyze_scope",
        tool_version="0.6",
        artifact_schema_version="1.0",
        environment="dev",
        configuration_hash="cfg",
        status="complete",
        started_at="now",
        completed_at="later",
        output_artifact_ids=("artifact-1",),
    )

    persist_run_manifest(Spark(), manifest, "main.evidence.sda_run_manifests")
