from __future__ import annotations

import pytest

from sda.state import (
    Approval,
    AttemptStatus,
    ExecutionAttempt,
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


def test_attempt_lease_is_idempotent_and_completion_is_terminal() -> None:
    repo = InMemoryStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    attempt = repo.acquire_attempt(ExecutionAttempt("run-1", "attempt-1", "generate"))
    assert attempt.status is AttemptStatus.RUNNING
    assert repo.acquire_attempt(ExecutionAttempt("run-1", "attempt-1", "generate")) == attempt
    repo.complete_attempt("attempt-1", success=False, error_code="timeout")
    with pytest.raises(StateError, match="already complete"):
        repo.complete_attempt("attempt-1", success=True)


def test_approval_is_unique_per_type() -> None:
    repo = InMemoryStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    approval = Approval("run-1", "privacy", "approved", "reviewer")
    repo.record_approval(approval)
    with pytest.raises(StateError, match="already recorded"):
        repo.record_approval(approval)
