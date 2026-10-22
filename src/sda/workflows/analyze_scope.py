"""Deterministic metadata-to-evidence orchestration boundary."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime

from sda.artifacts.manifest import RunManifest
from sda.models import AgentState
from sda.orchestrator import SyntheticDataAgent
from sda.state import (
    ArtifactDecision,
    ArtifactUse,
    PlanRecord,
    RequestRecord,
    RunRecord,
    RunStep,
    StateRepository,
    StepStatus,
    WorkflowStatus,
)
from sda.tools.base import AgentTool
from sda.version import __version__


def analyze_scope(
    state: AgentState,
    tools: Iterable[AgentTool],
    *,
    environment: str = "local",
    tool_version: str = __version__,
    configuration_hash: str = "workflow-default",
    state_repository: StateRepository | None = None,
    plan: PlanRecord | None = None,
) -> tuple[AgentState, RunManifest]:
    """Run supplied deterministic tools and return a linked run manifest.

    The coordinator deliberately receives tools instead of implementing domain
    calculations.  This keeps orchestration testable and allows the same
    contract to be used by a Spark entrypoint and local fixtures.
    """
    started = datetime.now(UTC).isoformat()
    if state_repository is not None:
        state_repository.create_request(
            RequestRecord(
                request_id=state.request.request_id,
                requester_id=state.request.requester or "unknown",
                environment=state.request.environment,
                idempotency_key=state.request.idempotency_key or state.request.request_id,
                source_scope=json.dumps(
                    {
                        "catalog": state.request.source.catalog,
                        "schema": state.request.source.schema,
                        "tables": state.request.source.tables,
                    },
                    sort_keys=True,
                ),
                generation_options=json.dumps(
                    {"mode": state.request.mode, "scale_factor": state.request.scale_factor},
                    sort_keys=True,
                ),
                target_catalog=state.request.target_catalog,
                target_schema=state.request.target_schema,
                intended_use=state.request.intended_use,
            )
        )
        if plan is not None:
            if plan.request_id != state.request.request_id:
                raise ValueError("plan request_id must match workflow request_id")
            if plan.status.lower() in {"rejected", "superseded", "expired"}:
                raise ValueError(f"plan status is not executable: {plan.status}")
            try:
                existing_run = state_repository.get_run(state.request.request_id)
            except Exception:
                existing_run = None
            if existing_run is not None and (
                existing_run.plan_id != plan.plan_id
                or existing_run.plan_fingerprint != plan.fingerprint
            ):
                raise ValueError("existing run is bound to a different plan")
            state_repository.create_plan(plan)
        run = state_repository.create_run(
            RunRecord(
                run_id=state.request.request_id,
                request_id=state.request.request_id,
                idempotency_key=state.request.idempotency_key or state.request.request_id,
                plan_id=plan.plan_id if plan is not None else None,
                plan_fingerprint=plan.fingerprint if plan is not None else None,
            )
        )
        if plan is not None and (
            run.plan_id != plan.plan_id or run.plan_fingerprint != plan.fingerprint
        ):
            raise ValueError("existing run is bound to a different plan")
        if plan is not None and run.status is WorkflowStatus.REQUESTED:
            run = state_repository.transition_run(
                run.run_id,
                WorkflowStatus.PLANNED,
                expected_version=run.version,
                actor="analyze_scope",
                reason="immutable evidence-analysis plan accepted",
            )
    agent = SyntheticDataAgent()
    tool_list = tuple(tools)
    active_tool_name: str | None = None
    try:
        final_state = state
        for tool in tool_list:
            tool_identifier = str(getattr(tool, "name", getattr(tool, "tool", "unknown")))
            if state_repository is not None:
                completed = any(
                    step.step_name == tool_identifier
                    and step.status is StepStatus.SUCCEEDED
                    and (
                        tool_identifier in {str(item) for item in final_state.completed_tools}
                        or bool(
                            set(step.output_artifacts)
                            & {item.artifact_id for item in final_state.durable_artifacts}
                        )
                    )
                    for step in state_repository.list_steps(state.request.request_id)
                )
                if completed:
                    continue
            previous_artifacts = len(final_state.durable_artifacts)
            active_tool_name = tool_identifier
            result = tool.run(final_state)
            agent.apply_result(final_state, result)
            if state_repository is not None:
                new_artifacts = final_state.durable_artifacts[previous_artifacts:]
                output_ids = tuple(item.artifact_id for item in new_artifacts)
                completed_step = RunStep(
                    step_id=f"{state.request.request_id}:{tool_identifier}",
                    run_id=state.request.request_id,
                    step_name=tool_identifier,
                    tool_name=tool_identifier,
                    status=StepStatus.SUCCEEDED,
                    output_artifacts=output_ids,
                    warning_count=len(result.warnings),
                    finished_at=datetime.now(UTC).isoformat(),
                )
                try:
                    state_repository.create_step(completed_step)
                except Exception as exc:
                    if "already exists" not in str(exc):
                        raise
                    state_repository.update_step(completed_step)
        manifest = RunManifest(
            run_id=state.request.request_id,
            tool_name="analyze_scope",
            tool_version=tool_version,
            artifact_schema_version="1.0",
            environment=environment,
            configuration_hash=configuration_hash,
            input_artifact_ids=tuple(
                artifact.artifact_id for artifact in final_state.durable_artifacts
            ),
            output_artifact_ids=tuple(
                artifact.artifact_id
                for artifact in (*final_state.artifacts, *final_state.durable_artifacts)
            ),
            status="complete",
            started_at=started,
            completed_at=datetime.now(UTC).isoformat(),
            warning_count=len(final_state.warnings),
        )
        if state_repository is not None:
            for artifact in final_state.durable_artifacts:
                state_repository.record_artifact_use(
                    ArtifactUse(
                        use_id=f"{state.request.request_id}:{artifact.artifact_id}:produced",
                        run_id=state.request.request_id,
                        artifact_id=artifact.artifact_id,
                        decision=ArtifactDecision.PRODUCED,
                        compatibility_fingerprint=artifact.reuse_fingerprint,
                        reason="artifact produced by deterministic tool",
                    )
                )
        return final_state, manifest
    except Exception as exc:
        manifest = RunManifest(
            run_id=state.request.request_id,
            tool_name="analyze_scope",
            tool_version=tool_version,
            artifact_schema_version="1.0",
            environment=environment,
            configuration_hash=configuration_hash,
            status="failed",
            started_at=started,
            completed_at=datetime.now(UTC).isoformat(),
            error_code=type(exc).__name__,
            error_message="scope analysis failed",
        )
        if state_repository is not None:
            if active_tool_name is not None:
                failed_step = RunStep(
                    step_id=f"{state.request.request_id}:{active_tool_name}",
                    run_id=state.request.request_id,
                    step_name=active_tool_name,
                    tool_name=active_tool_name,
                    status=StepStatus.FAILED,
                    output_artifacts=tuple(
                        artifact.artifact_id for artifact in state.durable_artifacts
                    ),
                    error_code=type(exc).__name__,
                    finished_at=datetime.now(UTC).isoformat(),
                )
                try:
                    state_repository.create_step(failed_step)
                except Exception:
                    state_repository.update_step(failed_step)
            completed_ids = tuple(artifact.artifact_id for artifact in state.durable_artifacts)
            state_repository.create_step(
                RunStep(
                    step_id=f"{state.request.request_id}:analyze_scope_failure",
                    run_id=state.request.request_id,
                    step_name="analyze_scope_failure",
                    tool_name="analyze_scope",
                    status=StepStatus.FAILED,
                    output_artifacts=completed_ids,
                    error_code=type(exc).__name__,
                    finished_at=datetime.now(UTC).isoformat(),
                )
            )
        raise RuntimeError(manifest.to_dict()) from exc
