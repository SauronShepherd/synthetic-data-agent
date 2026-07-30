# ruff: noqa: I001
"""Minimal orchestrator proving Article 02 responsibility boundaries."""

from collections.abc import Iterable

from .models import AgentState, RunStage, ToolName, ToolResult
from .tools.base import AgentTool


_ALLOWED_TRANSITIONS: dict[RunStage, frozenset[RunStage]] = {
    RunStage.RECEIVED: frozenset({RunStage.METADATA_DISCOVERED, RunStage.FAILED}),
    RunStage.METADATA_DISCOVERED: frozenset({RunStage.PROFILED, RunStage.FAILED}),
    RunStage.PROFILED: frozenset({RunStage.RELATIONSHIPS_MAPPED, RunStage.FAILED}),
    RunStage.RELATIONSHIPS_MAPPED: frozenset({RunStage.PATTERNS_DETECTED, RunStage.PLAN_DRAFTED, RunStage.FAILED}),
    RunStage.PATTERNS_DETECTED: frozenset({RunStage.PLAN_DRAFTED, RunStage.FAILED}),
    RunStage.PLAN_DRAFTED: frozenset({RunStage.GENERATED, RunStage.FAILED}),
    RunStage.GENERATED: frozenset({RunStage.VALIDATED, RunStage.FAILED}),
    RunStage.VALIDATED: frozenset({RunStage.PUBLISHED, RunStage.FAILED}),
    RunStage.PUBLISHED: frozenset(),
    RunStage.FAILED: frozenset(),
}

_REQUIRED_TOOL_FOR_STAGE: dict[RunStage, ToolName] = {
    RunStage.METADATA_DISCOVERED: ToolName.UC_METADATA_READER,
    RunStage.PROFILED: ToolName.TABLE_PROFILER,
    RunStage.RELATIONSHIPS_MAPPED: ToolName.RELATIONSHIP_DETECTOR,
    RunStage.PATTERNS_DETECTED: ToolName.PATTERN_DETECTOR,
    RunStage.PLAN_DRAFTED: ToolName.GENERATION_PLANNER,
    RunStage.GENERATED: ToolName.SYNTHETIC_DATA_GENERATOR,
    RunStage.VALIDATED: ToolName.QUALITY_VALIDATOR,
    RunStage.PUBLISHED: ToolName.PUBLISHER,
}


class OrchestrationError(RuntimeError):
    """Raised when a tool violates the explicit workflow contract."""


class SyntheticDataAgent:
    """Coordinate deterministic tools; do not perform their domain calculations."""

    def run(self, state: AgentState, tools: Iterable[AgentTool]) -> AgentState:
        """Execute tools in order and append their auditable results."""
        for tool in tools:
            result = tool.run(state)
            self.apply_result(state, result)
        return state

    def apply_result(self, state: AgentState, result: ToolResult) -> None:
        """Validate and apply one tool result atomically in memory."""
        allowed = _ALLOWED_TRANSITIONS[state.stage]
        if result.stage not in allowed:
            raise OrchestrationError(
                f"illegal transition: {state.stage.value} -> {result.stage.value}"
            )

        expected_tool = _REQUIRED_TOOL_FOR_STAGE.get(result.stage)
        if expected_tool is not None and result.tool is not expected_tool:
            raise OrchestrationError(
                f"{result.stage.value} must be produced by {expected_tool.value}, "
                f"not {result.tool.value}"
            )

        state.stage = result.stage
        state.artifacts.extend(result.artifacts)
        state.warnings.extend(result.warnings)
        state.completed_tools.append(result.tool)
