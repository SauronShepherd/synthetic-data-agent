from sda.models import ArtifactRef, ToolName


def test_legacy_artifact_can_convert_to_durable_contract() -> None:
    legacy = ArtifactRef(
        "a", "table_profile", ToolName.TABLE_PROFILER, "summary", {"tool_version": "0.6.0"}
    )
    durable = legacy.to_durable(
        run_id="run", environment="dev", location="main.evidence.profile", checksum="x"
    )
    assert durable.artifact_id == "a"
    assert durable.artifact_schema_version == "1.0"
