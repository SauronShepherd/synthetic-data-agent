"""Deterministic design stubs used to exercise the Article 02 workflow.

These stubs do not access Databricks and do not generate data. They prove that the
agent coordinates specialized tools and records their outputs instead of performing
the calculations itself.
"""

from __future__ import annotations

from dataclasses import dataclass

from sda.models import AgentState, ArtifactRef, RunStage, ToolName, ToolResult
from sda.planning import ColumnGenerationSpec, GenerationMode, GenerationPlan, PlanStatus


@dataclass(frozen=True, slots=True)
class DesignTool:
    """Small deterministic tool that emits one architecture artifact."""

    name: ToolName
    output_stage: RunStage
    artifact_type: str
    summary: str

    def run(self, state: AgentState) -> ToolResult:
        """Produce a stable artifact reference for the current request."""
        metadata: dict[str, object] = {"source_tables": len(state.request.source.tables)}
        if self.name is ToolName.GENERATION_PLANNER:
            if state.request.target_catalog is None or state.request.target_schema is None:
                raise ValueError("generation planning requires a target location")
            plan = GenerationPlan(
                plan_id=f"{state.request.request_id}:plan",
                plan_version=1,
                request_id=state.request.request_id,
                source_snapshot_ids=(f"{state.request.request_id}:source-snapshot",),
                input_artifact_ids=tuple(artifact.artifact_id for artifact in state.artifacts),
                target_catalog=state.request.target_catalog,
                target_schema=state.request.target_schema,
                tables=state.request.source.tables,
                columns=tuple(
                    ColumnGenerationSpec(table, "synthetic_id", "string", nullable=False, model="identifier")
                    for table in state.request.source.tables
                ),
                mode=GenerationMode.CLEAN,
                scale_factor=state.request.scale_factor,
                intended_use=state.request.intended_use or "review",
                privacy_policy_ref=state.request.privacy_mode,
                validation_policy_ref=state.request.validation_policy_ref or "default",
                status=PlanStatus.DRAFT,
            )
            metadata.update({"plan_fingerprint": plan.plan_fingerprint, "plan_status": plan.status.value})
        artifact = ArtifactRef(
            artifact_id=f"{state.request.request_id}:{self.artifact_type}",
            artifact_type=self.artifact_type,
            produced_by=self.name,
            summary=self.summary,
            metadata=metadata,
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
            ToolName.PATTERN_DETECTOR,
            RunStage.PATTERNS_DETECTED,
            "detected_patterns",
            "Pattern detection contract recorded; implementation arrives in Article 07.",
        ),
        DesignTool(
            ToolName.GENERATION_PLANNER,
            RunStage.PLAN_DRAFTED,
            "generation_plan",
            "A reviewable plan is required before generation.",
        ),
    )
