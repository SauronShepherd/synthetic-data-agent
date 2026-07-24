"""Deterministic design stubs used to exercise the Article 02 workflow.

These stubs do not access Databricks and do not generate data. They prove that the
agent coordinates specialized tools and records their outputs instead of performing
the calculations itself.
"""

from __future__ import annotations

from dataclasses import dataclass

from sda.models import AgentState, ArtifactRef, RunStage, ToolName, ToolResult


@dataclass(frozen=True, slots=True)
class DesignTool:
    """Small deterministic tool that emits one architecture artifact."""

    name: ToolName
    output_stage: RunStage
    artifact_type: str
    summary: str

    def run(self, state: AgentState) -> ToolResult:
        """Produce a stable artifact reference for the current request."""
        artifact = ArtifactRef(
            artifact_id=f"{state.request.request_id}:{self.artifact_type}",
            artifact_type=self.artifact_type,
            produced_by=self.name,
            summary=self.summary,
            metadata={"source_tables": len(state.request.source.tables)},
        )
        return ToolResult(tool=self.name, stage=self.output_stage, artifacts=(artifact,))


def article_02_toolchain() -> tuple[DesignTool, ...]:
    """Return the designed end-to-end tool sequence.

    Generation and publication remain design-only placeholders in this milestone.
    """
    return (
        DesignTool(
            ToolName.UC_METADATA_READER,
            RunStage.METADATA_DISCOVERED,
            "metadata_inventory",
            "Governed metadata inventory produced by uc_metadata_reader.",
        ),
        DesignTool(
            ToolName.TABLE_PROFILER,
            RunStage.PROFILED,
            "table_profiles",
            "Profiling contract recorded; statistical implementation arrives in Article 05.",
        ),
        DesignTool(
            ToolName.RELATIONSHIP_DETECTOR,
            RunStage.RELATIONSHIPS_MAPPED,
            "relationship_graph",
            "Relationship contract recorded; detection arrives in Article 06.",
        ),
        DesignTool(
            ToolName.GENERATION_PLANNER,
            RunStage.PLAN_DRAFTED,
            "generation_plan",
            "A reviewable plan is required before generation.",
        ),
    )
