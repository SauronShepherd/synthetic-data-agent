from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from sda.state import (
    Approval,
    ArtifactDecision,
    ArtifactUse,
    AttemptStatus,
    ExecutionAttempt,
    Feedback,
    InMemoryStateRepository,
    PlanRecord,
    RequestRecord,
    RunRecord,
    RunStep,
    StateError,
    StepStatus,
    WorkflowStatus,
)


def test_idempotent_run_creation_and_legal_transitions() -> None:
    repo = InMemoryStateRepository()
    run = RunRecord("run-1", "req-1", "idem-1")
    assert repo.create_run(run) == repo.create_run(run)
    repo.transition_run("run-1", WorkflowStatus.PLANNED, actor="agent", reason="scope accepted")
    repo.transition_run("run-1", WorkflowStatus.AWAITING_APPROVAL)
    repo.transition_run("run-1", WorkflowStatus.APPROVED)
    repo.transition_run("run-1", WorkflowStatus.EXECUTING)
    assert repo.get_run("run-1").version == 4
    events = repo.list_transition_events("run-1")
    assert events[0].actor == "agent"
    assert events[0].reason == "scope accepted"
    assert [(event.from_status, event.to_status) for event in events] == [
        (WorkflowStatus.REQUESTED, WorkflowStatus.PLANNED),
        (WorkflowStatus.PLANNED, WorkflowStatus.AWAITING_APPROVAL),
        (WorkflowStatus.AWAITING_APPROVAL, WorkflowStatus.APPROVED),
        (WorkflowStatus.APPROVED, WorkflowStatus.EXECUTING),
    ]


def test_steps_are_idempotent_and_preserve_artifact_handoffs() -> None:
    repo = InMemoryStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    step = RunStep("step-1", "run-1", "profile", "table_profiler", input_artifacts=("meta-1",))
    assert repo.create_step(step) == repo.create_step(step)
    completed = replace(step, status=StepStatus.SUCCEEDED, output_artifacts=("profile-1",))
    repo.update_step(completed)
    assert repo.list_steps("run-1") == (completed,)
    with pytest.raises(StateError, match="terminal step"):
        repo.update_step(replace(completed, warning_count=1))


def test_artifact_use_decisions_are_idempotent_and_run_scoped() -> None:
    repo = InMemoryStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    use = ArtifactUse("use-1", "run-1", "profile-1", ArtifactDecision.REUSED, "fp-1", "cache hit")
    assert repo.record_artifact_use(use) == repo.record_artifact_use(use)
    assert repo.list_artifact_uses("run-1") == (use,)
    with pytest.raises(StateError, match="different content"):
        repo.record_artifact_use(ArtifactUse("use-1", "run-1", "profile-2", "rejected", "fp-2"))


def test_idempotency_key_rejects_conflicting_run_content() -> None:
    repo = InMemoryStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    with pytest.raises(StateError, match="different content"):
        repo.create_run(RunRecord("run-2", "req-2", "idem-1"))


def test_request_records_are_idempotent_and_separate_from_runs() -> None:
    repo = InMemoryStateRepository()
    request = RequestRecord("req-1", "alice", "dev", "idem-1", '{"tables":["orders"]}')
    assert repo.create_request(request) == repo.create_request(request)
    assert repo.get_request("req-1") == request
    repo.create_run(RunRecord("run-1", "req-1", "run-idem-1"))


def test_plan_versions_are_immutable_and_ordered() -> None:
    repo = InMemoryStateRepository()
    repo.create_request(RequestRecord("req-1", "alice", "dev", "idem-1", "{}"))
    plan = PlanRecord("plan-1", "req-1", "run-1", 1, "fp-1", "approved", "{}")
    assert repo.create_plan(plan) == repo.create_plan(plan)
    corrected = PlanRecord("plan-2", "req-1", "run-1", 2, "fp-2", "draft", "{}", supersedes_plan_id="plan-1")
    repo.create_plan(corrected)
    assert repo.list_plans("req-1") == (plan, corrected)
    with pytest.raises(StateError, match="different content"):
        repo.create_plan(PlanRecord("plan-1", "req-1", "run-1", 1, "other", "approved", "{}"))


def test_state_status_strings_are_normalized_and_invalid_values_rejected() -> None:
    assert RunRecord("run", "request", "key", status="planned").status is WorkflowStatus.PLANNED
    assert (
        ExecutionAttempt("run", "attempt", "stage", status="failed").status is AttemptStatus.FAILED
    )
    with pytest.raises(ValueError, match="unsupported workflow status"):
        RunRecord("run", "request", "key", status="unknown")
    with pytest.raises(ValueError, match="unsupported attempt status"):
        ExecutionAttempt("run", "attempt", "stage", status="unknown")
    with pytest.raises(ValueError, match="reason"):
        Approval("run", "human", "approved", "actor")


def test_state_rejects_illegal_transition_and_stale_version() -> None:
    repo = InMemoryStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    with pytest.raises(StateError, match="invalid run transition"):
        repo.transition_run("run-1", WorkflowStatus.PUBLISHED)
    repo.transition_run("run-1", WorkflowStatus.PLANNED)
    with pytest.raises(StateError, match="concurrency"):
        repo.transition_run("run-1", WorkflowStatus.APPROVED, expected_version=0)


def test_failed_run_can_be_explicitly_retried() -> None:
    repo = InMemoryStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    repo.transition_run("run-1", WorkflowStatus.PLANNED)
    repo.transition_run("run-1", WorkflowStatus.AWAITING_APPROVAL)
    repo.transition_run("run-1", WorkflowStatus.APPROVED)
    repo.transition_run("run-1", WorkflowStatus.EXECUTING)
    repo.transition_run("run-1", WorkflowStatus.FAILED)
    retried = repo.transition_run("run-1", WorkflowStatus.EXECUTING)
    assert retried.status is WorkflowStatus.EXECUTING


def test_attempt_lease_is_idempotent_and_completion_is_terminal() -> None:
    repo = InMemoryStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    attempt = repo.acquire_attempt(ExecutionAttempt("run-1", "attempt-1", "generate"))
    assert attempt.status is AttemptStatus.RUNNING
    assert repo.acquire_attempt(ExecutionAttempt("run-1", "attempt-1", "generate")) == attempt
    assert repo.list_attempts("run-1") == (attempt,)
    repo.complete_attempt("attempt-1", success=False, error_code="timeout")
    assert repo.list_attempts("run-1")[0].completed_at
    with pytest.raises(StateError, match="already complete"):
        repo.complete_attempt("attempt-1", success=True)


def test_only_one_running_attempt_per_run_stage_is_allowed() -> None:
    repo = InMemoryStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    repo.acquire_attempt(ExecutionAttempt("run-1", "attempt-1", "profile"))
    with pytest.raises(StateError, match="running attempt"):
        repo.acquire_attempt(ExecutionAttempt("run-1", "attempt-2", "profile"))
    repo.complete_attempt("attempt-1", success=False, error_code="test")
    assert repo.acquire_attempt(ExecutionAttempt("run-1", "attempt-2", "profile")).attempt_id == "attempt-2"


def test_attempt_acquisition_rejects_unknown_runs() -> None:
    repo = InMemoryStateRepository()
    with pytest.raises(StateError, match="unknown run"):
        repo.acquire_attempt(ExecutionAttempt("missing", "attempt-1", "generate"))


def test_approval_is_unique_per_type() -> None:
    repo = InMemoryStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    approval = Approval("run-1", "privacy", "approved", "reviewer", "approved for test")
    repo.record_approval(approval)
    assert repo.record_approval(approval) == approval
    assert approval.decided_at
    assert repo.list_approvals("run-1") == (approval,)
    with pytest.raises(StateError, match="different content"):
        repo.record_approval(
            Approval("run-1", "privacy", "rejected", "reviewer", "rejected for test")
        )


def test_in_memory_lease_renewal_and_stale_recovery_match_durable_contract() -> None:
    repo = InMemoryStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    attempt = repo.acquire_attempt(
        ExecutionAttempt("run-1", "attempt-1", "generate", worker_id="worker-1"),
        lease_seconds=30,
    )
    renewed = repo.renew_attempt_lease("attempt-1", worker_id="worker-1", lease_seconds=60)
    assert renewed.lease_expires_at != attempt.lease_expires_at
    with pytest.raises(StateError, match="owned"):
        repo.renew_attempt_lease("attempt-1", worker_id="other")
    recovered = repo.recover_stale_attempts(now=datetime.now(UTC) + timedelta(seconds=120))
    assert recovered[0].status is AttemptStatus.ABANDONED


def test_expired_worker_cannot_complete_an_attempt_before_recovery() -> None:
    repo = InMemoryStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    repo.acquire_attempt(
        ExecutionAttempt("run-1", "attempt-1", "generate", worker_id="worker-1"),
        lease_seconds=1,
    )
    current = repo._attempts["attempt-1"]
    repo._attempts["attempt-1"] = replace(
        current, lease_expires_at=(datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    )
    with pytest.raises(StateError, match="lease has expired"):
        repo.complete_attempt("attempt-1", success=True)


def test_feedback_is_idempotent_and_does_not_mutate_the_run() -> None:
    repo = InMemoryStateRepository()
    original = repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    feedback = Feedback("feedback-1", "run-1", "reviewer", "correction", "Use profile v2")
    assert repo.record_feedback(feedback) == feedback
    assert repo.record_feedback(feedback) == feedback
    assert repo.list_feedback("run-1") == (feedback,)
    assert repo.get_run("run-1") == original
    with pytest.raises(StateError, match="different content"):
        repo.record_feedback(Feedback("feedback-1", "run-1", "reviewer", "correction", "changed"))
