"""Deterministic metadata-to-evidence orchestration boundary."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime

from sda.artifacts.manifest import RunManifest
from sda.models import AgentState
from sda.orchestrator import SyntheticDataAgent
from sda.tools.base import AgentTool
from sda.version import __version__


def analyze_scope(
    state: AgentState,
    tools: Iterable[AgentTool],
    *,
    environment: str = "local",
    tool_version: str = __version__,
    configuration_hash: str = "workflow-default",
) -> tuple[AgentState, RunManifest]:
    """Run supplied deterministic tools and return a linked run manifest.

    The coordinator deliberately receives tools instead of implementing domain
    calculations.  This keeps orchestration testable and allows the same
    contract to be used by a Spark entrypoint and local fixtures.
    """
    started = datetime.now(UTC).isoformat()
    agent = SyntheticDataAgent()
    try:
        final_state = agent.run(state, tools)
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
        raise RuntimeError(manifest.to_dict()) from exc
