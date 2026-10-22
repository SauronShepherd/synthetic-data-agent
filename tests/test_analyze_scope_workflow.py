import pytest

from sda.models import AgentState, GenerationRequest, SourceScope
from sda.state import InMemoryStateRepository, PlanRecord, WorkflowStatus
from sda.tools.design_stubs import article_02_toolchain
from sda.workflows.analyze_scope import analyze_scope


class FailingTool:
    @property
    def name(self):
        return "table_profiler"

    def run(self, state):
        del state
        raise ValueError("source value must not be persisted")


def test_analyze_scope_returns_manifest_linked_to_artifacts() -> None:
    request = GenerationRequest(
        request_id="request-1",
        source=SourceScope("main", "sales", ("orders",)),
    )

    state, manifest = analyze_scope(AgentState(request), article_02_toolchain()[:3])

    assert manifest.status == "complete"
    assert manifest.run_id == "request-1"
    assert manifest.output_artifact_ids == tuple(item.artifact_id for item in state.artifacts)


def test_analyze_scope_records_durable_step_handoffs() -> None:
    request = GenerationRequest(
        request_id="request-steps",
        source=SourceScope("main", "sales", ("orders",)),
    )
    repository = InMemoryStateRepository()
    state, _ = analyze_scope(
        AgentState(request), article_02_toolchain()[:3], state_repository=repository
    )
    steps = repository.list_steps(request.request_id)
    assert len(steps) == 3
    assert all(step.status.value == "succeeded" for step in steps)
    assert [step.step_name for step in steps] == [
        "uc_metadata_reader",
        "table_profiler",
        "relationship_detector",
    ]
    assert repository.get_request(request.request_id).source_scope
    assert len(repository.list_artifact_uses(request.request_id)) == len(state.durable_artifacts)


def test_analyze_scope_binds_run_to_immutable_plan_version() -> None:
    request = GenerationRequest(
        request_id="request-plan",
        source=SourceScope("main", "sales", ("orders",)),
    )
    repository = InMemoryStateRepository()
    plan = PlanRecord("plan-1", request.request_id, request.request_id, 1, "fp-1", "approved", "{}")
    analyze_scope(
        AgentState(request), article_02_toolchain()[:3], state_repository=repository, plan=plan
    )
    stored_run = repository.get_run(request.request_id)
    assert stored_run.plan_id == "plan-1"
    assert stored_run.plan_fingerprint == "fp-1"
    assert repository.get_plan("plan-1") == plan


def test_analyze_scope_advances_plan_bound_run_to_planned() -> None:
    request = GenerationRequest(
        request_id="request-lifecycle",
        source=SourceScope("main", "sales", ("orders",)),
    )
    repository = InMemoryStateRepository()
    plan = PlanRecord(
        "plan-lifecycle",
        request.request_id,
        request.request_id,
        1,
        "fp-lifecycle",
        "approved",
        "{}",
    )

    analyze_scope(
        AgentState(request), article_02_toolchain()[:1], state_repository=repository, plan=plan
    )

    run = repository.get_run(request.request_id)
    assert run.status is WorkflowStatus.PLANNED
    events = repository.list_transition_events(request.request_id)
    assert [(event.from_status, event.to_status) for event in events] == [
        (WorkflowStatus.REQUESTED, WorkflowStatus.PLANNED)
    ]


@pytest.mark.parametrize("status", ["rejected", "superseded", "expired"])
def test_analyze_scope_rejects_terminal_plan_status(status: str) -> None:
    request = GenerationRequest(
        request_id=f"request-{status}",
        source=SourceScope("main", "sales", ("orders",)),
    )
    plan = PlanRecord("plan-1", request.request_id, request.request_id, 1, "fp-1", status, "{}")
    with pytest.raises(ValueError, match="not executable"):
        analyze_scope(
            AgentState(request),
            article_02_toolchain()[:1],
            state_repository=InMemoryStateRepository(),
            plan=plan,
        )


def test_analyze_scope_rejects_plan_rebinding_on_retry() -> None:
    request = GenerationRequest(
        request_id="request-plan-rebind",
        source=SourceScope("main", "sales", ("orders",)),
    )
    repository = InMemoryStateRepository()
    plan_one = PlanRecord(
        "plan-1", request.request_id, request.request_id, 1, "fp-1", "approved", "{}"
    )
    plan_two = PlanRecord(
        "plan-2", request.request_id, request.request_id, 2, "fp-2", "approved", "{}"
    )
    analyze_scope(
        AgentState(request), article_02_toolchain()[:1], state_repository=repository, plan=plan_one
    )
    with pytest.raises(ValueError, match="different plan"):
        analyze_scope(
            AgentState(request),
            article_02_toolchain()[:1],
            state_repository=repository,
            plan=plan_two,
        )


def test_analyze_scope_resumes_by_skipping_completed_tools() -> None:
    request = GenerationRequest(
        request_id="request-resume",
        source=SourceScope("main", "sales", ("orders",)),
    )
    repository = InMemoryStateRepository()
    state, first_manifest = analyze_scope(
        AgentState(request), article_02_toolchain()[:3], state_repository=repository
    )
    second_state, second_manifest = analyze_scope(
        state, article_02_toolchain()[:3], state_repository=repository
    )
    assert second_state is state
    assert second_manifest.status == first_manifest.status == "complete"
    assert len(repository.list_steps(request.request_id)) == 3


def test_analyze_scope_persists_failed_step_without_raw_error_text() -> None:
    request = GenerationRequest(
        request_id="request-failure",
        source=SourceScope("main", "sales", ("orders",)),
    )
    repository = InMemoryStateRepository()
    with pytest.raises(RuntimeError):
        analyze_scope(AgentState(request), [FailingTool()], state_repository=repository)
    steps = repository.list_steps(request.request_id)
    assert steps[-1].status.value == "failed"
    assert steps[-1].error_code == "ValueError"
    assert "source value" not in str(steps[-1])


def test_failed_step_can_be_retried_and_completed() -> None:
    request = GenerationRequest(
        request_id="request-step-retry",
        source=SourceScope("main", "sales", ("orders",)),
    )
    repository = InMemoryStateRepository()

    class FlakyTool:
        calls = 0

        @property
        def name(self):
            return "table_profiler"

        def run(self, state):
            self.calls += 1
            if self.calls == 1:
                raise ValueError("transient failure")
            return article_02_toolchain()[1].run(state)

    tool = FlakyTool()
    state = AgentState(request)
    with pytest.raises(RuntimeError):
        analyze_scope(state, article_02_toolchain()[:1] + (tool,), state_repository=repository)
    assert any(step.status.value == "failed" for step in repository.list_steps(request.request_id))
    state, manifest = analyze_scope(
        state, article_02_toolchain()[:1] + (tool,), state_repository=repository
    )
    assert manifest.status == "complete"
    assert repository.list_steps(request.request_id)[0].status.value == "succeeded"
    assert state.completed_tools
