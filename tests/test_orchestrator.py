from __future__ import annotations

import pytest

from sda.demo import run_design_demo
from sda.models import AgentState, GenerationRequest, RunStage, SourceScope, ToolName, ToolResult
from sda.orchestrator import OrchestrationError, SyntheticDataAgent


def make_state() -> AgentState:
    request = GenerationRequest(
        request_id="req-1",
        source=SourceScope(catalog="main", schema="sales", tables=("customers",)),
    )
    return AgentState(request=request)


def test_design_demo_stops_at_plan() -> None:
    state = run_design_demo()
    assert state.stage is RunStage.PLAN_DRAFTED
    assert state.completed_tools == [
        ToolName.UC_METADATA_READER,
        ToolName.TABLE_PROFILER,
        ToolName.RELATIONSHIP_DETECTOR,
        ToolName.GENERATION_PLANNER,
    ]
    assert len(state.artifacts) == 4


def test_illegal_transition_is_rejected() -> None:
    state = make_state()
    result = ToolResult(tool=ToolName.TABLE_PROFILER, stage=RunStage.PROFILED)
    with pytest.raises(OrchestrationError, match="illegal transition"):
        SyntheticDataAgent().apply_result(state, result)


def test_wrong_tool_for_stage_is_rejected() -> None:
    state = make_state()
    result = ToolResult(
        tool=ToolName.TABLE_PROFILER,
        stage=RunStage.METADATA_DISCOVERED,
    )
    with pytest.raises(OrchestrationError, match="must be produced"):
        SyntheticDataAgent().apply_result(state, result)
