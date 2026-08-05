from sda.models import AgentState, GenerationRequest, SourceScope
from sda.tools.design_stubs import article_02_toolchain
from sda.workflows.analyze_scope import analyze_scope


def test_analyze_scope_returns_manifest_linked_to_artifacts() -> None:
    request = GenerationRequest(
        request_id="request-1",
        source=SourceScope("main", "sales", ("orders",)),
    )

    state, manifest = analyze_scope(AgentState(request), article_02_toolchain()[:3])

    assert manifest.status == "complete"
    assert manifest.run_id == "request-1"
    assert manifest.output_artifact_ids == tuple(item.artifact_id for item in state.artifacts)
