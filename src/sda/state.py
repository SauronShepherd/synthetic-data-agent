"""Durable workflow-state contracts and an in-memory reference repository.

The repository interface is deliberately storage-neutral: Lakebase/PostgreSQL can
implement it without changing orchestration semantics, while local tests remain fast.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from threading import RLock
from typing import Protocol


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


class StepStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class ArtifactDecision(StrEnum):
    PRODUCED = "produced"
    REUSED = "reused"
    INVALIDATED = "invalidated"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class RequestRecord:
    """Normalized request intent retained independently from execution attempts."""

    request_id: str
    requester_id: str
    environment: str
    idempotency_key: str
    source_scope: str
    generation_options: str = "{}"
    target_catalog: str | None = None
    target_schema: str | None = None
    intended_use: str | None = None
    submitted_at: str = ""

    def __post_init__(self) -> None:
        for value in (
            self.request_id,
            self.requester_id,
            self.environment,
            self.idempotency_key,
            self.source_scope,
        ):
            if not value.strip():
                raise ValueError("request identity and source scope must not be empty")
        if not self.submitted_at:
            object.__setattr__(self, "submitted_at", datetime.now(UTC).isoformat())


@dataclass(frozen=True, slots=True)
class PlanRecord:
    """Immutable version of the decision handed from agent to tools."""

    plan_id: str
    request_id: str
    run_id: str
    version: int
    fingerprint: str
    status: str
    payload: str
    created_at: str = ""
    supersedes_plan_id: str | None = None

    def __post_init__(self) -> None:
        for value in (
            self.plan_id,
            self.request_id,
            self.run_id,
            self.fingerprint,
            self.status,
            self.payload,
        ):
            if not value.strip():
                raise ValueError("plan identity and payload fields must not be empty")
        if self.version < 1:
            raise ValueError("plan version must be positive")
        if not self.created_at:
            object.__setattr__(self, "created_at", datetime.now(UTC).isoformat())


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
        if not isinstance(self.status, WorkflowStatus):
            try:
                object.__setattr__(self, "status", WorkflowStatus(self.status))
            except ValueError as exc:
                raise ValueError(f"unsupported workflow status: {self.status}") from exc
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
    started_at: str = ""
    completed_at: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, AttemptStatus):
            try:
                object.__setattr__(self, "status", AttemptStatus(self.status))
            except ValueError as exc:
                raise ValueError(f"unsupported attempt status: {self.status}") from exc
        if not self.run_id.strip() or not self.attempt_id.strip() or not self.stage.strip():
            raise ValueError("attempt identity and stage must not be empty")
        if self.retry_number < 0:
            raise ValueError("retry_number must not be negative")
        if not self.started_at:
            object.__setattr__(self, "started_at", datetime.now(UTC).isoformat())


@dataclass(frozen=True, slots=True)
class Approval:
    run_id: str
    approval_type: str
    decision: str
    actor: str
    reason: str = ""
    decided_at: str = ""

    def __post_init__(self) -> None:
        if any(
            not value.strip()
            for value in (self.run_id, self.approval_type, self.actor, self.reason)
        ):
            raise ValueError("approval identity and reason fields must not be empty")
        if self.decision not in {"approved", "rejected"}:
            raise ValueError("decision must be approved or rejected")
        if not self.decided_at:
            object.__setattr__(self, "decided_at", datetime.now(UTC).isoformat())


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


@dataclass(frozen=True, slots=True)
class TransitionEvent:
    """Append-only record of a workflow state transition."""

    event_id: str
    run_id: str
    from_status: WorkflowStatus
    to_status: WorkflowStatus
    actor: str
    reason: str = ""
    occurred_at: str = ""

    def __post_init__(self) -> None:
        for value in (self.event_id, self.run_id, self.actor):
            if not value.strip():
                raise ValueError("transition event identity fields must not be empty")
        if not isinstance(self.from_status, WorkflowStatus):
            object.__setattr__(self, "from_status", WorkflowStatus(self.from_status))
        if not isinstance(self.to_status, WorkflowStatus):
            object.__setattr__(self, "to_status", WorkflowStatus(self.to_status))
        if not self.occurred_at:
            object.__setattr__(self, "occurred_at", datetime.now(UTC).isoformat())


@dataclass(frozen=True, slots=True)
class RunStep:
    """Durable stage checkpoint and artifact handoff record."""

    step_id: str
    run_id: str
    step_name: str
    tool_name: str
    status: StepStatus = StepStatus.PENDING
    input_artifacts: tuple[str, ...] = ()
    output_artifacts: tuple[str, ...] = ()
    warning_count: int = 0
    error_code: str | None = None
    started_at: str = ""
    finished_at: str | None = None

    def __post_init__(self) -> None:
        for value in (self.step_id, self.run_id, self.step_name, self.tool_name):
            if not value.strip():
                raise ValueError("step identity fields must not be empty")
        if not isinstance(self.status, StepStatus):
            object.__setattr__(self, "status", StepStatus(self.status))
        if self.warning_count < 0:
            raise ValueError("warning_count must not be negative")


@dataclass(frozen=True, slots=True)
class ArtifactUse:
    """Immutable record explaining an artifact decision in a run."""

    use_id: str
    run_id: str
    artifact_id: str
    decision: ArtifactDecision
    compatibility_fingerprint: str
    reason: str = ""
    recorded_at: str = ""

    def __post_init__(self) -> None:
        for value in (self.use_id, self.run_id, self.artifact_id, self.compatibility_fingerprint):
            if not value.strip():
                raise ValueError("artifact-use identity fields must not be empty")
        if not isinstance(self.decision, ArtifactDecision):
            object.__setattr__(self, "decision", ArtifactDecision(self.decision))
        if not self.recorded_at:
            object.__setattr__(self, "recorded_at", datetime.now(UTC).isoformat())


_ALLOWED: dict[WorkflowStatus, frozenset[WorkflowStatus]] = {
    WorkflowStatus.REQUESTED: frozenset({WorkflowStatus.PLANNED, WorkflowStatus.CANCELLED}),
    WorkflowStatus.PLANNED: frozenset({WorkflowStatus.AWAITING_APPROVAL, WorkflowStatus.REJECTED}),
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


class StateRepository(Protocol):
    """Storage-neutral contract implemented by local and durable adapters."""

    def create_run(self, run: RunRecord) -> RunRecord: ...
    def create_request(self, request: RequestRecord) -> RequestRecord: ...
    def get_request(self, request_id: str) -> RequestRecord: ...
    def create_plan(self, plan: PlanRecord) -> PlanRecord: ...
    def get_plan(self, plan_id: str) -> PlanRecord: ...
    def list_plans(self, request_id: str) -> tuple[PlanRecord, ...]: ...
    def get_run(self, run_id: str) -> RunRecord: ...
    def transition_run(
        self,
        run_id: str,
        status: WorkflowStatus,
        *,
        expected_version: int | None = None,
        actor: str = "system",
        reason: str = "",
    ) -> RunRecord: ...
    def record_approval(self, approval: Approval) -> Approval: ...
    def list_approvals(self, run_id: str) -> tuple[Approval, ...]: ...
    def record_feedback(self, feedback: Feedback) -> Feedback: ...
    def list_feedback(self, run_id: str) -> tuple[Feedback, ...]: ...
    def list_transition_events(self, run_id: str) -> tuple[TransitionEvent, ...]: ...
    def create_step(self, step: RunStep) -> RunStep: ...
    def get_step(self, step_id: str) -> RunStep: ...
    def list_steps(self, run_id: str) -> tuple[RunStep, ...]: ...
    def update_step(self, step: RunStep) -> RunStep: ...
    def record_artifact_use(self, use: ArtifactUse) -> ArtifactUse: ...
    def list_artifact_uses(self, run_id: str) -> tuple[ArtifactUse, ...]: ...
    def acquire_attempt(
        self, attempt: ExecutionAttempt, *, lease_seconds: int = 300
    ) -> ExecutionAttempt: ...
    def list_attempts(self, run_id: str) -> tuple[ExecutionAttempt, ...]: ...
    def complete_attempt(
        self, attempt_id: str, *, success: bool, error_code: str | None = None
    ) -> ExecutionAttempt: ...
    def renew_attempt_lease(
        self, attempt_id: str, *, worker_id: str, lease_seconds: int = 300
    ) -> ExecutionAttempt: ...
    def recover_stale_attempts(
        self, *, now: datetime | None = None
    ) -> tuple[ExecutionAttempt, ...]: ...


class InMemoryStateRepository:
    """Thread-safe reference implementation for local orchestration and tests."""

    def __init__(self) -> None:
        self._runs: dict[str, RunRecord] = {}
        self._requests: dict[str, RequestRecord] = {}
        self._requests_by_idempotency: dict[str, str] = {}
        self._plans: dict[str, PlanRecord] = {}
        self._by_idempotency: dict[str, str] = {}
        self._attempts: dict[str, ExecutionAttempt] = {}
        self._approvals: list[Approval] = []
        self._feedback: dict[str, Feedback] = {}
        self._transition_events: dict[str, list[TransitionEvent]] = {}
        self._steps: dict[str, RunStep] = {}
        self._artifact_uses: dict[str, ArtifactUse] = {}
        self._event_sequence = 0
        self._lock = RLock()

    def create_run(self, run: RunRecord) -> RunRecord:
        with self._lock:
            existing_id = self._by_idempotency.get(run.idempotency_key)
            if existing_id is not None:
                existing = self._runs[existing_id]
                if existing.run_id != run.run_id or existing.request_id != run.request_id:
                    raise StateError("idempotency key already exists with different content")
                return existing
            if run.run_id in self._runs:
                raise StateError(f"run already exists: {run.run_id}")
            self._runs[run.run_id] = run
            self._by_idempotency[run.idempotency_key] = run.run_id
            return run

    def create_request(self, request: RequestRecord) -> RequestRecord:
        with self._lock:
            existing_id = self._requests_by_idempotency.get(request.idempotency_key)
            if existing_id is not None:
                existing = self._requests[existing_id]
                if replace(request, submitted_at=existing.submitted_at) != existing:
                    raise StateError(
                        "request idempotency key already exists with different content"
                    )
                return existing
            if request.request_id in self._requests:
                raise StateError(f"request already exists: {request.request_id}")
            self._requests[request.request_id] = request
            self._requests_by_idempotency[request.idempotency_key] = request.request_id
            return request

    def get_request(self, request_id: str) -> RequestRecord:
        try:
            return self._requests[request_id]
        except KeyError as exc:
            raise StateError(f"unknown request: {request_id}") from exc

    def create_plan(self, plan: PlanRecord) -> PlanRecord:
        with self._lock:
            self.get_request(plan.request_id)
            existing = self._plans.get(plan.plan_id)
            if existing is not None:
                if existing != plan:
                    raise StateError("plan already exists with different content")
                return existing
            if any(
                current.request_id == plan.request_id and current.version == plan.version
                for current in self._plans.values()
            ):
                raise StateError("plan version already exists for this request")
            self._plans[plan.plan_id] = plan
            return plan

    def get_plan(self, plan_id: str) -> PlanRecord:
        try:
            return self._plans[plan_id]
        except KeyError as exc:
            raise StateError(f"unknown plan: {plan_id}") from exc

    def list_plans(self, request_id: str) -> tuple[PlanRecord, ...]:
        with self._lock:
            self.get_request(request_id)
            return tuple(
                sorted(
                    (plan for plan in self._plans.values() if plan.request_id == request_id),
                    key=lambda plan: plan.version,
                )
            )

    def get_run(self, run_id: str) -> RunRecord:
        try:
            return self._runs[run_id]
        except KeyError as exc:
            raise StateError(f"unknown run: {run_id}") from exc

    def transition_run(
        self,
        run_id: str,
        status: WorkflowStatus,
        *,
        expected_version: int | None = None,
        actor: str = "system",
        reason: str = "",
    ) -> RunRecord:
        if not actor.strip():
            raise ValueError("actor must not be empty")
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
            self._event_sequence += 1
            event = TransitionEvent(
                event_id=f"transition-{self._event_sequence}",
                run_id=run_id,
                from_status=current.status,
                to_status=status,
                actor=actor,
                reason=reason,
            )
            self._transition_events.setdefault(run_id, []).append(event)
            return updated

    def record_approval(self, approval: Approval) -> Approval:
        with self._lock:
            self.get_run(approval.run_id)
            for existing in self._approvals:
                if (
                    existing.run_id == approval.run_id
                    and existing.approval_type == approval.approval_type
                ):
                    if existing == approval:
                        return existing
                    raise StateError("approval already recorded with different content")
            self._approvals.append(approval)
            return approval

    def list_approvals(self, run_id: str) -> tuple[Approval, ...]:
        with self._lock:
            self.get_run(run_id)
            return tuple(approval for approval in self._approvals if approval.run_id == run_id)

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

    def list_transition_events(self, run_id: str) -> tuple[TransitionEvent, ...]:
        with self._lock:
            self.get_run(run_id)
            return tuple(self._transition_events.get(run_id, ()))

    def create_step(self, step: RunStep) -> RunStep:
        with self._lock:
            self.get_run(step.run_id)
            existing = self._steps.get(step.step_id)
            if existing is not None:
                if existing != step:
                    raise StateError("step already exists with different content")
                return existing
            if any(
                current.run_id == step.run_id and current.step_name == step.step_name
                for current in self._steps.values()
            ):
                raise StateError("step name already exists for this run")
            self._steps[step.step_id] = step
            return step

    def get_step(self, step_id: str) -> RunStep:
        try:
            return self._steps[step_id]
        except KeyError as exc:
            raise StateError(f"unknown step: {step_id}") from exc

    def list_steps(self, run_id: str) -> tuple[RunStep, ...]:
        with self._lock:
            self.get_run(run_id)
            return tuple(step for step in self._steps.values() if step.run_id == run_id)

    def update_step(self, step: RunStep) -> RunStep:
        with self._lock:
            current = self.get_step(step.step_id)
            if current.run_id != step.run_id or current.step_name != step.step_name:
                raise StateError("step identity cannot be changed")
            if current.status in {StepStatus.SUCCEEDED, StepStatus.CANCELLED} and current != step:
                raise StateError("terminal step cannot be changed")
            self._steps[step.step_id] = step
            return step

    def record_artifact_use(self, use: ArtifactUse) -> ArtifactUse:
        with self._lock:
            self.get_run(use.run_id)
            existing = self._artifact_uses.get(use.use_id)
            if existing is not None:
                if existing != use:
                    raise StateError("artifact-use ID already exists with different content")
                return existing
            self._artifact_uses[use.use_id] = use
            return use

    def list_artifact_uses(self, run_id: str) -> tuple[ArtifactUse, ...]:
        with self._lock:
            self.get_run(run_id)
            return tuple(use for use in self._artifact_uses.values() if use.run_id == run_id)

    def acquire_attempt(
        self, attempt: ExecutionAttempt, *, lease_seconds: int = 300
    ) -> ExecutionAttempt:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        with self._lock:
            self.get_run(attempt.run_id)
            existing = self._attempts.get(attempt.attempt_id)
            if existing is not None:
                return existing
            if any(
                current.run_id == attempt.run_id
                and current.stage == attempt.stage
                and current.status is AttemptStatus.RUNNING
                for current in self._attempts.values()
            ):
                raise StateError("a running attempt already exists for this run and stage")
            lease = datetime.now(UTC) + timedelta(seconds=lease_seconds)
            claimed = replace(attempt, lease_expires_at=lease.isoformat())
            self._attempts[attempt.attempt_id] = claimed
            return claimed

    def list_attempts(self, run_id: str) -> tuple[ExecutionAttempt, ...]:
        with self._lock:
            self.get_run(run_id)
            return tuple(attempt for attempt in self._attempts.values() if attempt.run_id == run_id)

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
            if current.lease_expires_at is not None and _parse_time(
                current.lease_expires_at
            ) <= datetime.now(UTC):
                raise StateError("attempt lease has expired")
            updated = replace(
                current,
                status=AttemptStatus.SUCCEEDED if success else AttemptStatus.FAILED,
                error_code=error_code,
                completed_at=datetime.now(UTC).isoformat(),
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
