from dataclasses import dataclass

from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType
from sda.models import AgentState, GenerationRequest, RunStage, SourceScope, ToolName, ToolResult
from sda.workflows.analyze_scope import analyze_scope


@dataclass
class EvidenceTool:
    tool: ToolName
    stage: RunStage
    artifact_type: ArtifactType

    def run(self, state: AgentState) -> ToolResult:
        artifact = ArtifactRef(
            artifact_id=f"{state.request.request_id}:{self.artifact_type.value}",
            artifact_type=self.artifact_type,
            artifact_schema_version="1.0",
            status=ArtifactStatus.COMPLETE,
            tool_name=self.tool.value,
            tool_version="0.6",
            run_id=state.request.request_id,
            environment="local",
            created_at="now",
            configuration_hash="cfg",
            primary_location=f"main.evidence.{self.artifact_type.value}",
            related_locations={},
            source_references=(),
            checksum="checksum",
            summary=self.artifact_type.value,
        )
        return ToolResult(tool=self.tool, stage=self.stage, durable_artifacts=(artifact,))


def test_end_to_end_manifest_links_all_evidence_artifacts() -> None:
    state, manifest = analyze_scope(
        AgentState(GenerationRequest("run-1", SourceScope("main", "sales", ("orders",)))),
        [
            EvidenceTool(
                ToolName.UC_METADATA_READER,
                RunStage.METADATA_DISCOVERED,
                ArtifactType.METADATA_INVENTORY,
            ),
            EvidenceTool(
                ToolName.TABLE_PROFILER,
                RunStage.PROFILED,
                ArtifactType.TABLE_PROFILE,
            ),
            EvidenceTool(
                ToolName.RELATIONSHIP_DETECTOR,
                RunStage.RELATIONSHIPS_MAPPED,
                ArtifactType.RELATIONSHIP_ANALYSIS,
            ),
        ],
    )

    assert len(manifest.input_artifact_ids) == 3
    assert manifest.output_artifact_ids == manifest.input_artifact_ids
    assert state.stage is RunStage.RELATIONSHIPS_MAPPED
