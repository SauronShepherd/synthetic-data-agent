from sda.models import ArtifactRef, RunStage, ToolName, ToolResult


def test_tool_result_converts_legacy_artifacts_to_durable_contract() -> None:
    result = ToolResult(
        tool=ToolName.TABLE_PROFILER,
        stage=RunStage.PROFILED,
        artifacts=(
            ArtifactRef(
                artifact_id="request:profile",
                artifact_type="table_profile",
                produced_by=ToolName.TABLE_PROFILER,
                summary="profile",
                metadata={"tool_version": "0.6", "configuration_hash": "cfg"},
            ),
        ),
    )

    durable = result.to_durable_artifacts(
        run_id="run-1",
        environment="test",
        location_prefix="/evidence",
        checksums={"request:profile": "sha256:abc"},
    )

    assert len(durable) == 1
    assert durable[0].artifact_id == "request:profile"
    assert durable[0].primary_location == "/evidence/request:profile"
    assert durable[0].checksum == "sha256:abc"
    assert durable[0].status.value == "complete"


def test_tool_result_bridge_marks_missing_checksum_explicitly() -> None:
    result = ToolResult(
        tool=ToolName.UC_METADATA_READER,
        stage=RunStage.METADATA_DISCOVERED,
        artifacts=(
            ArtifactRef(
                artifact_id="request:metadata",
                artifact_type="metadata_inventory",
                produced_by=ToolName.UC_METADATA_READER,
                summary="inventory",
            ),
        ),
    )

    durable = result.to_durable_artifacts(
        run_id="run-1", environment="test", location_prefix="/evidence"
    )

    assert durable[0].checksum == "unverified"
