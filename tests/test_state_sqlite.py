from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from sda.state import (
    Approval,
    ArtifactDecision,
    ArtifactUse,
    AttemptStatus,
    ExecutionAttempt,
    Feedback,
    PlanRecord,
    RequestRecord,
    RunRecord,
    RunStep,
    StateError,
    WorkflowStatus,
)
from sda.state_sqlite import SQLiteStateRepository


def test_sqlite_state_survives_repository_reopen(tmp_path: object) -> None:
    path = str(tmp_path / "state.db")
    repo = SQLiteStateRepository(path)
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    request = RequestRecord("req-1", "alice", "dev", "request-idem-1", '{"tables":["orders"]}')
    repo.create_request(request)
    repo.transition_run("run-1", WorkflowStatus.PLANNED, actor="agent", reason="scope accepted")
    repo.transition_run("run-1", WorkflowStatus.AWAITING_APPROVAL)
    repo.close()
    reopened = SQLiteStateRepository(path)
    assert reopened.get_run("run-1").status is WorkflowStatus.AWAITING_APPROVAL
    assert reopened.list_transition_events("run-1")[0].actor == "agent"
    assert reopened.list_transition_events("run-1")[0].reason == "scope accepted"
    assert reopened.get_request("req-1") == request
    plan = PlanRecord("plan-1", "req-1", "run-1", 1, "fp-1", "approved", "{}")
    reopened.create_plan(plan)
    reopened.close()
    reopened = SQLiteStateRepository(path)
    assert reopened.get_plan("plan-1") == plan
    assert len(reopened.list_transition_events("run-1")) == 2
    step = RunStep("step-1", "run-1", "metadata", "uc_metadata_reader", output_artifacts=("meta-1",))
    reopened.create_step(step)
    reopened.close()
    reopened = SQLiteStateRepository(path)
    assert reopened.list_steps("run-1")[0].output_artifacts == ("meta-1",)
    reopened.close()


def test_sqlite_state_enforces_idempotency_and_version() -> None:
    repo = SQLiteStateRepository()
    run = RunRecord("run-1", "req-1", "idem-1")
    assert repo.create_run(run) == repo.create_run(run)
    repo.transition_run("run-1", WorkflowStatus.PLANNED)
    repo.transition_run("run-1", WorkflowStatus.AWAITING_APPROVAL)
    with pytest.raises(StateError, match="concurrency"):
        repo.transition_run("run-1", WorkflowStatus.APPROVED, expected_version=0)
    repo.close()


def test_sqlite_state_rejects_conflicting_idempotency_content() -> None:
    repo = SQLiteStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    with pytest.raises(StateError, match="different content"):
        repo.create_run(RunRecord("run-2", "req-2", "idem-1"))
    repo.close()


def test_sqlite_state_persists_approvals_and_attempts() -> None:
    repo = SQLiteStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    approval = Approval("run-1", "human", "approved", "reviewer", "looks good")
    assert repo.record_approval(approval) == approval
    stored = repo._connection.execute(
        "SELECT decided_at FROM approvals WHERE run_id = ? AND approval_type = ?",
        ("run-1", "human"),
    ).fetchone()
    assert stored == (approval.decided_at,)
    assert repo.list_approvals("run-1") == (approval,)
    assert repo.record_approval(approval) == approval
    with pytest.raises(StateError, match="different content"):
        repo.record_approval(
            Approval("run-1", "human", "rejected", "reviewer", "rejected for test")
        )
    attempt = repo.acquire_attempt(ExecutionAttempt("run-1", "attempt-1", "generate"))
    assert attempt.status is AttemptStatus.RUNNING
    assert repo.list_attempts("run-1") == (attempt,)
    completed = repo.complete_attempt("attempt-1", success=True)
    assert completed.status is AttemptStatus.SUCCEEDED
    assert completed.started_at and completed.completed_at
    with pytest.raises(StateError, match="already complete"):
        repo.complete_attempt("attempt-1", success=True)
    repo.close()


def test_sqlite_state_allows_only_one_running_attempt_per_stage() -> None:
    repo = SQLiteStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    repo.acquire_attempt(ExecutionAttempt("run-1", "attempt-1", "profile"))
    with pytest.raises(StateError, match="running attempt"):
        repo.acquire_attempt(ExecutionAttempt("run-1", "attempt-2", "profile"))
    repo.complete_attempt("attempt-1", success=False, error_code="test")
    assert repo.acquire_attempt(ExecutionAttempt("run-1", "attempt-2", "profile")).attempt_id == "attempt-2"
    repo.close()


def test_sqlite_artifact_use_decisions_survive_reopen(tmp_path: object) -> None:
    path = str(tmp_path / "artifact-use.db")
    repo = SQLiteStateRepository(path)
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    use = ArtifactUse("use-1", "run-1", "profile-1", ArtifactDecision.PRODUCED, "fp-1")
    repo.record_artifact_use(use)
    repo.close()
    reopened = SQLiteStateRepository(path)
    assert reopened.list_artifact_uses("run-1") == (use,)
    reopened.close()


def test_sqlite_state_renews_and_recovers_stale_leases() -> None:
    repo = SQLiteStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    attempt = repo.acquire_attempt(
        ExecutionAttempt("run-1", "attempt-1", "generate", worker_id="worker-1"),
        lease_seconds=30,
    )
    renewed = repo.renew_attempt_lease("attempt-1", worker_id="worker-1", lease_seconds=60)
    assert renewed.lease_expires_at != attempt.lease_expires_at
    with pytest.raises(StateError, match="owned"):
        repo.renew_attempt_lease("attempt-1", worker_id="other")
    stale_time = datetime.now(UTC) + timedelta(seconds=120)
    recovered = repo.recover_stale_attempts(now=stale_time)
    assert recovered[0].status is AttemptStatus.ABANDONED
    with pytest.raises(StateError, match="owned"):
        repo.renew_attempt_lease("attempt-1", worker_id="worker-1")
    repo.close()


def test_sqlite_expired_worker_cannot_complete_before_recovery() -> None:
    repo = SQLiteStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    repo.acquire_attempt(
        ExecutionAttempt("run-1", "attempt-1", "generate", worker_id="worker-1"),
        lease_seconds=1,
    )
    repo._connection.execute(
        "UPDATE attempts SET lease_expires_at = ? WHERE attempt_id = ?",
        ((datetime.now(UTC) - timedelta(seconds=1)).isoformat(), "attempt-1"),
    )
    with pytest.raises(StateError, match="lease has expired"):
        repo.complete_attempt("attempt-1", success=True)
    repo.close()


def test_sqlite_failed_run_can_be_explicitly_retried() -> None:
    repo = SQLiteStateRepository()
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    for status in (
        WorkflowStatus.PLANNED,
        WorkflowStatus.AWAITING_APPROVAL,
        WorkflowStatus.APPROVED,
        WorkflowStatus.EXECUTING,
        WorkflowStatus.FAILED,
    ):
        repo.transition_run("run-1", status)
    assert repo.transition_run("run-1", WorkflowStatus.EXECUTING).status is WorkflowStatus.EXECUTING
    repo.close()


def test_sqlite_migrates_legacy_approvals_table(tmp_path: object) -> None:
    path = str(tmp_path / "legacy.db")
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE runs (run_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE, status TEXT NOT NULL, plan_id TEXT, plan_fingerprint TEXT, version INTEGER NOT NULL, updated_at TEXT NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE approvals (run_id TEXT NOT NULL, approval_type TEXT NOT NULL, decision TEXT NOT NULL, actor TEXT NOT NULL, reason TEXT NOT NULL, PRIMARY KEY (run_id, approval_type))"
    )
    connection.commit()
    connection.close()
    repo = SQLiteStateRepository(path)
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    approval = Approval("run-1", "human", "approved", "reviewer", "approved for test")
    repo.record_approval(approval)
    assert (
        repo._connection.execute("PRAGMA table_info(approvals)").fetchall()[-1][1] == "decided_at"
    )
    repo.close()


def test_sqlite_feedback_is_idempotent_and_survives_reopen(tmp_path: object) -> None:
    path = str(tmp_path / "state.db")
    repo = SQLiteStateRepository(path)
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    feedback = Feedback("feedback-1", "run-1", "reviewer", "correction", "Use profile v2")
    assert repo.record_feedback(feedback) == feedback
    assert repo.record_feedback(feedback) == feedback
    repo.close()
    reopened = SQLiteStateRepository(path)
    assert reopened.list_feedback("run-1") == (feedback,)
    with pytest.raises(StateError, match="different content"):
        reopened.record_feedback(
            Feedback("feedback-1", "run-1", "reviewer", "correction", "changed")
        )
    reopened.close()
