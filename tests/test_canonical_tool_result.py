from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType
from sda.models import AgentState, GenerationRequest, RunStage, SourceScope, ToolName, ToolResult
from sda.orchestrator import SyntheticDataAgent


class CanonicalTool:
    def __init__(self, result: ToolResult) -> None:
        self.result = result

    def run(self, state: AgentState) -> ToolResult:
        del state
        return self.result


def test_orchestrator_propagates_canonical_artifact_references() -> None:
    request = GenerationRequest("r1", SourceScope("main", "sales", ("orders",)))
    durable = ArtifactRef(
        "durable-1",
        ArtifactType.METADATA_INVENTORY,
        "1.0",
        ArtifactStatus.COMPLETE,
        "uc_metadata_reader",
        "0.6.0",
        "run",
        "dev",
        "now",
        "cfg",
        "main.evidence.inventory",
        {},
        (),
        "checksum",
        "inventory",
    )
    result = ToolResult(
        ToolName.UC_METADATA_READER,
        RunStage.METADATA_DISCOVERED,
        durable_artifacts=(durable,),
    )

    state = SyntheticDataAgent().run(AgentState(request), [CanonicalTool(result)])

    assert state.durable_artifacts == [durable]
