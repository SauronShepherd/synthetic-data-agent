from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from sda.state import (
    Approval,
    AttemptStatus,
    ExecutionAttempt,
    Feedback,
    InMemoryStateRepository,
    RunRecord,
    StateError,
    WorkflowStatus,
)


def test_idempotent_run_creation_and_legal_transitions() -> None:
    repo = InMemoryStateRepository()
    run = RunRecord("run-1", "req-1", "idem-1")
    assert repo.create_run(run) == repo.create_run(run)
    repo.transition_run("run-1", WorkflowStatus.PLANNED)
    repo.transition_run("run-1", WorkflowStatus.APPROVED)
    repo.transition_run("run-1", WorkflowStatus.EXECUTING)
    assert repo.get_run("run-1").version == 3


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


def test_attempt_acquisition_rejects_unknown_runs() -> None:
    repo = InMemoryStateRepository()
    with pytest.raises(StateError, match="unknown run"):
        repo.acquire_attempt(ExecutionAttempt("missing", "attempt-1", "generate"))


def test_approval_is_unique_per_type() -> None:
    repo = InMemoryStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    approval = Approval("run-1", "privacy", "approved", "reviewer")
    repo.record_approval(approval)
    assert approval.decided_at
    assert repo.list_approvals("run-1") == (approval,)
    with pytest.raises(StateError, match="already recorded"):
        repo.record_approval(approval)


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
