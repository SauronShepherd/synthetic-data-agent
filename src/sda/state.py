"""Durable workflow-state contracts and an in-memory reference repository.

The repository interface is deliberately storage-neutral: Lakebase/PostgreSQL can
implement it without changing orchestration semantics, while local tests remain fast.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from threading import RLock


class WorkflowStatus(StrEnum):
    REQUESTED = "requested"
    PLANNED = "planned"
    AWAITING_APPROVAL = "awaiting_approval"
    APPROVED = "approved"
    EXECUTING = "executing"
    GENERATED_AWAITING_VALIDATION = "generated_awaiting_validation"
    VALIDATED = "validated"
    PRIVACY_APPROVED = "privacy_approved"
    PUBLISHED = "published"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    FAILED = "failed"


class AttemptStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    ABANDONED = "abandoned"


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: str
    request_id: str
    idempotency_key: str
    status: WorkflowStatus = WorkflowStatus.REQUESTED
    plan_id: str | None = None
    plan_fingerprint: str | None = None
    version: int = 0
    updated_at: str = ""

    def __post_init__(self) -> None:
        if (
            not self.run_id.strip()
            or not self.request_id.strip()
            or not self.idempotency_key.strip()
        ):
            raise ValueError("run_id, request_id, and idempotency_key must not be empty")
        if self.version < 0:
            raise ValueError("version must not be negative")
        if not self.updated_at:
            object.__setattr__(self, "updated_at", datetime.now(UTC).isoformat())


@dataclass(frozen=True, slots=True)
class ExecutionAttempt:
    run_id: str
    attempt_id: str
    stage: str
    status: AttemptStatus = AttemptStatus.RUNNING
    worker_id: str | None = None
    lease_expires_at: str | None = None
    retry_number: int = 0
    error_code: str | None = None

    def __post_init__(self) -> None:
        if not self.run_id.strip() or not self.attempt_id.strip() or not self.stage.strip():
            raise ValueError("attempt identity and stage must not be empty")
        if self.retry_number < 0:
            raise ValueError("retry_number must not be negative")


@dataclass(frozen=True, slots=True)
class Approval:
    run_id: str
    approval_type: str
    decision: str
    actor: str
    reason: str = ""

    def __post_init__(self) -> None:
        if any(not value.strip() for value in (self.run_id, self.approval_type, self.actor)):
            raise ValueError("approval identity fields must not be empty")
        if self.decision not in {"approved", "rejected"}:
            raise ValueError("decision must be approved or rejected")


@dataclass(frozen=True, slots=True)
class Feedback:
    """Immutable correction or review note attached to a run/evidence snapshot."""

    feedback_id: str
    run_id: str
    actor: str
    category: str
    message: str
    evidence_ref: str | None = None
    created_at: str = ""

    def __post_init__(self) -> None:
        if any(
            not value.strip()
            for value in (self.feedback_id, self.run_id, self.actor, self.category, self.message)
        ):
            raise ValueError("feedback identity and message fields must not be empty")
        if not self.created_at:
            object.__setattr__(self, "created_at", datetime.now(UTC).isoformat())


_ALLOWED: dict[WorkflowStatus, frozenset[WorkflowStatus]] = {
    WorkflowStatus.REQUESTED: frozenset({WorkflowStatus.PLANNED, WorkflowStatus.CANCELLED}),
    WorkflowStatus.PLANNED: frozenset(
        {WorkflowStatus.AWAITING_APPROVAL, WorkflowStatus.APPROVED, WorkflowStatus.REJECTED}
    ),
    WorkflowStatus.AWAITING_APPROVAL: frozenset(
        {WorkflowStatus.APPROVED, WorkflowStatus.REJECTED, WorkflowStatus.CANCELLED}
    ),
    WorkflowStatus.APPROVED: frozenset({WorkflowStatus.EXECUTING, WorkflowStatus.CANCELLED}),
    WorkflowStatus.EXECUTING: frozenset(
        {WorkflowStatus.GENERATED_AWAITING_VALIDATION, WorkflowStatus.FAILED}
    ),
    WorkflowStatus.GENERATED_AWAITING_VALIDATION: frozenset(
        {WorkflowStatus.VALIDATED, WorkflowStatus.FAILED}
    ),
    WorkflowStatus.VALIDATED: frozenset({WorkflowStatus.PRIVACY_APPROVED, WorkflowStatus.FAILED}),
    WorkflowStatus.PRIVACY_APPROVED: frozenset({WorkflowStatus.PUBLISHED, WorkflowStatus.FAILED}),
    WorkflowStatus.PUBLISHED: frozenset(),
    WorkflowStatus.REJECTED: frozenset(),
    WorkflowStatus.CANCELLED: frozenset(),
    # Retry is explicit: orchestration must create a new attempt before re-entry.
    WorkflowStatus.FAILED: frozenset({WorkflowStatus.EXECUTING, WorkflowStatus.CANCELLED}),
}


class StateError(RuntimeError):
    """Raised for illegal or conflicting state operations."""


class InMemoryStateRepository:
    """Thread-safe reference implementation for local orchestration and tests."""

    def __init__(self) -> None:
        self._runs: dict[str, RunRecord] = {}
        self._by_idempotency: dict[str, str] = {}
        self._attempts: dict[str, ExecutionAttempt] = {}
        self._approvals: list[Approval] = []
        self._feedback: dict[str, Feedback] = {}
        self._lock = RLock()

    def create_run(self, run: RunRecord) -> RunRecord:
        with self._lock:
            existing_id = self._by_idempotency.get(run.idempotency_key)
            if existing_id is not None:
                return self._runs[existing_id]
            if run.run_id in self._runs:
                raise StateError(f"run already exists: {run.run_id}")
            self._runs[run.run_id] = run
            self._by_idempotency[run.idempotency_key] = run.run_id
            return run

    def get_run(self, run_id: str) -> RunRecord:
        try:
            return self._runs[run_id]
        except KeyError as exc:
            raise StateError(f"unknown run: {run_id}") from exc

    def transition_run(
        self, run_id: str, status: WorkflowStatus, *, expected_version: int | None = None
    ) -> RunRecord:
        with self._lock:
            current = self.get_run(run_id)
            if expected_version is not None and current.version != expected_version:
                raise StateError("optimistic concurrency conflict")
            if status not in _ALLOWED[current.status]:
                raise StateError(
                    f"invalid run transition: {current.status.value} -> {status.value}"
                )
            updated = replace(
                current,
                status=status,
                version=current.version + 1,
                updated_at=datetime.now(UTC).isoformat(),
            )
            self._runs[run_id] = updated
            return updated

    def record_approval(self, approval: Approval) -> Approval:
        with self._lock:
            if any(
                a.run_id == approval.run_id and a.approval_type == approval.approval_type
                for a in self._approvals
            ):
                raise StateError("approval already recorded")
            self.get_run(approval.run_id)
            self._approvals.append(approval)
            return approval

    def record_feedback(self, feedback: Feedback) -> Feedback:
        with self._lock:
            self.get_run(feedback.run_id)
            existing = self._feedback.get(feedback.feedback_id)
            if existing is not None:
                if existing != feedback:
                    raise StateError("feedback id already exists with different content")
                return existing
            self._feedback[feedback.feedback_id] = feedback
            return feedback

    def list_feedback(self, run_id: str) -> tuple[Feedback, ...]:
        with self._lock:
            self.get_run(run_id)
            return tuple(item for item in self._feedback.values() if item.run_id == run_id)

    def acquire_attempt(
        self, attempt: ExecutionAttempt, *, lease_seconds: int = 300
    ) -> ExecutionAttempt:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        with self._lock:
            existing = self._attempts.get(attempt.attempt_id)
            if existing is not None:
                return existing
            lease = datetime.now(UTC) + timedelta(seconds=lease_seconds)
            claimed = replace(attempt, lease_expires_at=lease.isoformat())
            self._attempts[attempt.attempt_id] = claimed
            return claimed

    def complete_attempt(
        self, attempt_id: str, *, success: bool, error_code: str | None = None
    ) -> ExecutionAttempt:
        with self._lock:
            try:
                current = self._attempts[attempt_id]
            except KeyError as exc:
                raise StateError(f"unknown attempt: {attempt_id}") from exc
            if current.status is not AttemptStatus.RUNNING:
                raise StateError("attempt is already complete")
            updated = replace(
                current,
                status=AttemptStatus.SUCCEEDED if success else AttemptStatus.FAILED,
                error_code=error_code,
            )
            self._attempts[attempt_id] = updated
            return updated

    def renew_attempt_lease(
        self, attempt_id: str, *, worker_id: str, lease_seconds: int = 300
    ) -> ExecutionAttempt:
        if not worker_id.strip() or lease_seconds < 1:
            raise ValueError("worker_id must not be empty and lease_seconds must be positive")
        with self._lock:
            current = self._attempts.get(attempt_id)
            if current is None:
                raise StateError(f"unknown attempt: {attempt_id}")
            if current.status is not AttemptStatus.RUNNING or current.worker_id != worker_id:
                raise StateError("attempt lease is not owned by worker")
            if current.lease_expires_at is None or _parse_time(
                current.lease_expires_at
            ) <= datetime.now(UTC):
                raise StateError("attempt lease has expired")
            updated = replace(
                current,
                lease_expires_at=(datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat(),
            )
            self._attempts[attempt_id] = updated
            return updated

    def recover_stale_attempts(
        self, *, now: datetime | None = None
    ) -> tuple[ExecutionAttempt, ...]:
        current_time = now or datetime.now(UTC)
        with self._lock:
            recovered: list[ExecutionAttempt] = []
            for attempt_id, attempt in self._attempts.items():
                if (
                    attempt.status is AttemptStatus.RUNNING
                    and attempt.lease_expires_at is not None
                    and _parse_time(attempt.lease_expires_at) <= current_time
                ):
                    updated = replace(
                        attempt,
                        status=AttemptStatus.ABANDONED,
                        error_code="stale_lease",
                        lease_expires_at=None,
                    )
                    self._attempts[attempt_id] = updated
                    recovered.append(updated)
            return tuple(recovered)


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
