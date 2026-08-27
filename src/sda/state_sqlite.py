"""SQLite reference persistence for the workflow-state contract.

The SQL shape intentionally mirrors the constraints expected from Lakebase. It is
used for local recovery tests; production deployments should provide a PostgreSQL
adapter with the same repository methods.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta

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
    StateRepository,
    StepStatus,
    TransitionEvent,
    WorkflowStatus,
)


class SQLiteStateRepository(StateRepository):
    def __init__(self, path: str = ":memory:") -> None:
        self._connection = sqlite3.connect(path)
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE, status TEXT NOT NULL, plan_id TEXT, plan_fingerprint TEXT, version INTEGER NOT NULL, updated_at TEXT NOT NULL)"
        )
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS requests (request_id TEXT PRIMARY KEY, requester_id TEXT NOT NULL, environment TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE, source_scope TEXT NOT NULL, generation_options TEXT NOT NULL, target_catalog TEXT, target_schema TEXT, intended_use TEXT, submitted_at TEXT NOT NULL)"
        )
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS plans (plan_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, run_id TEXT NOT NULL, version INTEGER NOT NULL, fingerprint TEXT NOT NULL, status TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL, supersedes_plan_id TEXT, UNIQUE(request_id, version), FOREIGN KEY(request_id) REFERENCES requests(request_id))"
        )
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS attempts (attempt_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(run_id), stage TEXT NOT NULL, status TEXT NOT NULL, worker_id TEXT, lease_expires_at TEXT, retry_number INTEGER NOT NULL, error_code TEXT)"
        )
        self._connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS one_running_attempt_per_stage "
            "ON attempts(run_id, stage) WHERE status = 'running'"
        )
        attempt_columns = {
            str(row[1]) for row in self._connection.execute("PRAGMA table_info(attempts)")
        }
        if "started_at" not in attempt_columns:
            self._connection.execute(
                "ALTER TABLE attempts ADD COLUMN started_at TEXT NOT NULL DEFAULT ''"
            )
            self._connection.execute(
                "UPDATE attempts SET started_at = CURRENT_TIMESTAMP WHERE started_at = ''"
            )
        if "completed_at" not in attempt_columns:
            self._connection.execute("ALTER TABLE attempts ADD COLUMN completed_at TEXT")
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS approvals (run_id TEXT NOT NULL REFERENCES runs(run_id), approval_type TEXT NOT NULL, decision TEXT NOT NULL, actor TEXT NOT NULL, reason TEXT NOT NULL, PRIMARY KEY (run_id, approval_type))"
        )
        approval_columns = {
            str(row[1]) for row in self._connection.execute("PRAGMA table_info(approvals)")
        }
        if "decided_at" not in approval_columns:
            self._connection.execute(
                "ALTER TABLE approvals ADD COLUMN decided_at TEXT NOT NULL DEFAULT ''"
            )
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS feedback (feedback_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(run_id), actor TEXT NOT NULL, category TEXT NOT NULL, message TEXT NOT NULL, evidence_ref TEXT, created_at TEXT NOT NULL)"
        )
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS transition_events (event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(run_id), from_status TEXT NOT NULL, to_status TEXT NOT NULL, actor TEXT NOT NULL, reason TEXT NOT NULL, occurred_at TEXT NOT NULL)"
        )
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS run_steps (step_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(run_id), step_name TEXT NOT NULL, tool_name TEXT NOT NULL, status TEXT NOT NULL, input_artifacts TEXT NOT NULL, output_artifacts TEXT NOT NULL, warning_count INTEGER NOT NULL, error_code TEXT, started_at TEXT NOT NULL, finished_at TEXT, UNIQUE(run_id, step_name))"
        )
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS artifact_uses (use_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(run_id), artifact_id TEXT NOT NULL, decision TEXT NOT NULL, compatibility_fingerprint TEXT NOT NULL, reason TEXT NOT NULL, recorded_at TEXT NOT NULL)"
        )
        self._connection.commit()

    def create_run(self, run: RunRecord) -> RunRecord:
        existing = self._connection.execute(
            "SELECT run_id FROM runs WHERE idempotency_key = ?", (run.idempotency_key,)
        ).fetchone()
        if existing:
            current = self.get_run(existing[0])
            if current.run_id != run.run_id or current.request_id != run.request_id:
                raise StateError("idempotency key already exists with different content")
            return current
        try:
            self._connection.execute(
                "INSERT INTO runs (run_id, request_id, idempotency_key, status, plan_id, plan_fingerprint, version, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run.run_id,
                    run.request_id,
                    run.idempotency_key,
                    run.status.value,
                    run.plan_id,
                    run.plan_fingerprint,
                    run.version,
                    run.updated_at,
                ),
            )
            self._connection.commit()
        except sqlite3.IntegrityError as exc:
            raise StateError("run already exists") from exc
        return run

    def create_request(self, request: RequestRecord) -> RequestRecord:
        existing = self._connection.execute(
            "SELECT request_id, requester_id, environment, idempotency_key, source_scope, generation_options, target_catalog, target_schema, intended_use, submitted_at FROM requests WHERE idempotency_key = ?",
            (request.idempotency_key,),
        ).fetchone()
        if existing is not None:
            current = self._request_from_row(existing)
            if replace(request, submitted_at=current.submitted_at) != current:
                raise StateError("request idempotency key already exists with different content")
            return current
        try:
            self._connection.execute(
                "INSERT INTO requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (request.request_id, request.requester_id, request.environment, request.idempotency_key,
                 request.source_scope, request.generation_options, request.target_catalog,
                 request.target_schema, request.intended_use, request.submitted_at),
            )
            self._connection.commit()
        except sqlite3.IntegrityError as exc:
            self._connection.rollback()
            raise StateError("request already exists") from exc
        return request

    def get_request(self, request_id: str) -> RequestRecord:
        row = self._connection.execute(
            "SELECT request_id, requester_id, environment, idempotency_key, source_scope, generation_options, target_catalog, target_schema, intended_use, submitted_at FROM requests WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        if row is None:
            raise StateError(f"unknown request: {request_id}")
        return self._request_from_row(row)

    def create_plan(self, plan: PlanRecord) -> PlanRecord:
        self.get_request(plan.request_id)
        existing = self._connection.execute(
            "SELECT plan_id, request_id, run_id, version, fingerprint, status, payload, created_at, supersedes_plan_id FROM plans WHERE plan_id = ?",
            (plan.plan_id,),
        ).fetchone()
        if existing is not None:
            current = self._plan_from_row(existing)
            if current != plan:
                raise StateError("plan already exists with different content")
            return current
        try:
            self._connection.execute(
                "INSERT INTO plans VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (plan.plan_id, plan.request_id, plan.run_id, plan.version, plan.fingerprint,
                 plan.status, plan.payload, plan.created_at, plan.supersedes_plan_id),
            )
            self._connection.commit()
        except sqlite3.IntegrityError as exc:
            self._connection.rollback()
            raise StateError("plan version already exists for this request") from exc
        return plan

    def get_plan(self, plan_id: str) -> PlanRecord:
        row = self._connection.execute(
            "SELECT plan_id, request_id, run_id, version, fingerprint, status, payload, created_at, supersedes_plan_id FROM plans WHERE plan_id = ?",
            (plan_id,),
        ).fetchone()
        if row is None:
            raise StateError(f"unknown plan: {plan_id}")
        return self._plan_from_row(row)

    def list_plans(self, request_id: str) -> tuple[PlanRecord, ...]:
        self.get_request(request_id)
        rows = self._connection.execute(
            "SELECT plan_id, request_id, run_id, version, fingerprint, status, payload, created_at, supersedes_plan_id FROM plans WHERE request_id = ? ORDER BY version",
            (request_id,),
        ).fetchall()
        return tuple(self._plan_from_row(row) for row in rows)

    @staticmethod
    def _plan_from_row(row: tuple[object, ...]) -> PlanRecord:
        return PlanRecord(str(row[0]), str(row[1]), str(row[2]), int(str(row[3])), str(row[4]),
                          str(row[5]), str(row[6]), str(row[7]),
                          str(row[8]) if row[8] is not None else None)

    @staticmethod
    def _request_from_row(row: tuple[object, ...]) -> RequestRecord:
        return RequestRecord(str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]),
                             str(row[5]), str(row[6]) if row[6] is not None else None,
                             str(row[7]) if row[7] is not None else None,
                             str(row[8]) if row[8] is not None else None, str(row[9]))

    def get_run(self, run_id: str) -> RunRecord:
        row = self._connection.execute(
            "SELECT run_id, request_id, idempotency_key, status, plan_id, plan_fingerprint, version, updated_at FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise StateError(f"unknown run: {run_id}")
        return RunRecord(
            row[0], row[1], row[2], WorkflowStatus(row[3]), row[4], row[5], row[6], row[7]
        )

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
        current = self.get_run(run_id)
        from sda.state import _ALLOWED

        if status not in _ALLOWED[current.status]:
            raise StateError(f"invalid run transition: {current.status.value} -> {status.value}")
        if expected_version is not None and current.version != expected_version:
            raise StateError("optimistic concurrency conflict")
        updated = replace(
            current,
            status=status,
            version=current.version + 1,
            updated_at=datetime.now(UTC).isoformat(),
        )
        params = (
            updated.status.value,
            updated.version,
            updated.updated_at,
            run_id,
            current.version,
        )
        changed = self._connection.execute(
            "UPDATE runs SET status = ?, version = ?, updated_at = ? WHERE run_id = ? AND version = ?",
            params,
        ).rowcount
        if changed != 1:
            self._connection.rollback()
            raise StateError("optimistic concurrency conflict")
        event = TransitionEvent(
            event_id=f"transition-{run_id}-{updated.version}",
            run_id=run_id,
            from_status=current.status,
            to_status=status,
            actor=actor,
            reason=reason,
        )
        self._connection.execute(
            "INSERT INTO transition_events (event_id, run_id, from_status, to_status, actor, reason, occurred_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (event.event_id, event.run_id, event.from_status.value, event.to_status.value,
            event.actor, event.reason, event.occurred_at),
        )
        self._connection.commit()
        return updated

    def close(self) -> None:
        self._connection.close()

    def record_approval(self, approval: Approval) -> Approval:
        self.get_run(approval.run_id)
        existing = self._connection.execute(
            "SELECT run_id, approval_type, decision, actor, reason, decided_at FROM approvals WHERE run_id = ? AND approval_type = ?",
            (approval.run_id, approval.approval_type),
        ).fetchone()
        if existing is not None:
            current = Approval(*existing)
            if current == approval:
                return current
            raise StateError("approval already recorded with different content")
        try:
            self._connection.execute(
                "INSERT INTO approvals (run_id, approval_type, decision, actor, reason, decided_at) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    approval.run_id,
                    approval.approval_type,
                    approval.decision,
                    approval.actor,
                    approval.reason,
                    approval.decided_at,
                ),
            )
            self._connection.commit()
        except sqlite3.IntegrityError as exc:
            self._connection.rollback()
            raise StateError("approval already recorded with different content") from exc
        return approval

    def list_approvals(self, run_id: str) -> tuple[Approval, ...]:
        self.get_run(run_id)
        rows = self._connection.execute(
            "SELECT run_id, approval_type, decision, actor, reason, decided_at FROM approvals WHERE run_id = ? ORDER BY decided_at, approval_type",
            (run_id,),
        ).fetchall()
        return tuple(Approval(*row) for row in rows)

    def record_feedback(self, feedback: Feedback) -> Feedback:
        self.get_run(feedback.run_id)
        existing = self._connection.execute(
            "SELECT feedback_id, run_id, actor, category, message, evidence_ref, created_at FROM feedback WHERE feedback_id = ?",
            (feedback.feedback_id,),
        ).fetchone()
        if existing is not None:
            current = Feedback(*existing)
            if current != feedback:
                raise StateError("feedback id already exists with different content")
            return current
        try:
            self._connection.execute(
                "INSERT INTO feedback VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    feedback.feedback_id,
                    feedback.run_id,
                    feedback.actor,
                    feedback.category,
                    feedback.message,
                    feedback.evidence_ref,
                    feedback.created_at,
                ),
            )
            self._connection.commit()
        except sqlite3.IntegrityError as exc:
            self._connection.rollback()
            raise StateError("feedback cannot be recorded") from exc
        return feedback

    def list_feedback(self, run_id: str) -> tuple[Feedback, ...]:
        self.get_run(run_id)
        rows = self._connection.execute(
            "SELECT feedback_id, run_id, actor, category, message, evidence_ref, created_at FROM feedback WHERE run_id = ? ORDER BY created_at, feedback_id",
            (run_id,),
        ).fetchall()
        return tuple(Feedback(*row) for row in rows)

    def list_transition_events(self, run_id: str) -> tuple[TransitionEvent, ...]:
        self.get_run(run_id)
        rows = self._connection.execute(
            "SELECT event_id, run_id, from_status, to_status, actor, reason, occurred_at FROM transition_events WHERE run_id = ? ORDER BY occurred_at, event_id",
            (run_id,),
        ).fetchall()
        return tuple(
            TransitionEvent(row[0], row[1], WorkflowStatus(row[2]), WorkflowStatus(row[3]), row[4], row[5], row[6])
            for row in rows
        )

    def create_step(self, step: RunStep) -> RunStep:
        self.get_run(step.run_id)
        existing = self._connection.execute(
            "SELECT step_id, run_id, step_name, tool_name, status, input_artifacts, output_artifacts, warning_count, error_code, started_at, finished_at FROM run_steps WHERE step_id = ?",
            (step.step_id,),
        ).fetchone()
        if existing is not None:
            current = self._step_from_row(existing)
            if current != step:
                raise StateError("step already exists with different content")
            return current
        try:
            self._connection.execute(
                "INSERT INTO run_steps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (step.step_id, step.run_id, step.step_name, step.tool_name, step.status.value,
                 _encode_artifacts(step.input_artifacts), _encode_artifacts(step.output_artifacts),
                 step.warning_count, step.error_code, step.started_at, step.finished_at),
            )
            self._connection.commit()
        except sqlite3.IntegrityError as exc:
            self._connection.rollback()
            raise StateError("step name already exists for this run") from exc
        return step

    def get_step(self, step_id: str) -> RunStep:
        row = self._connection.execute(
            "SELECT step_id, run_id, step_name, tool_name, status, input_artifacts, output_artifacts, warning_count, error_code, started_at, finished_at FROM run_steps WHERE step_id = ?",
            (step_id,),
        ).fetchone()
        if row is None:
            raise StateError(f"unknown step: {step_id}")
        return self._step_from_row(row)

    def list_steps(self, run_id: str) -> tuple[RunStep, ...]:
        self.get_run(run_id)
        rows = self._connection.execute(
            "SELECT step_id, run_id, step_name, tool_name, status, input_artifacts, output_artifacts, warning_count, error_code, started_at, finished_at FROM run_steps WHERE run_id = ? ORDER BY started_at, step_id",
            (run_id,),
        ).fetchall()
        return tuple(self._step_from_row(row) for row in rows)

    def update_step(self, step: RunStep) -> RunStep:
        current = self.get_step(step.step_id)
        if current.run_id != step.run_id or current.step_name != step.step_name:
            raise StateError("step identity cannot be changed")
        if current.status in {StepStatus.SUCCEEDED, StepStatus.CANCELLED} and current != step:
            raise StateError("terminal step cannot be changed")
        self._connection.execute(
            "UPDATE run_steps SET status = ?, input_artifacts = ?, output_artifacts = ?, warning_count = ?, error_code = ?, started_at = ?, finished_at = ? WHERE step_id = ?",
            (step.status.value, _encode_artifacts(step.input_artifacts), _encode_artifacts(step.output_artifacts),
             step.warning_count, step.error_code, step.started_at, step.finished_at, step.step_id),
        )
        self._connection.commit()
        return step

    def record_artifact_use(self, use: ArtifactUse) -> ArtifactUse:
        self.get_run(use.run_id)
        existing = self._connection.execute(
            "SELECT use_id, run_id, artifact_id, decision, compatibility_fingerprint, reason, recorded_at FROM artifact_uses WHERE use_id = ?",
            (use.use_id,),
        ).fetchone()
        if existing is not None:
            current = self._artifact_use_from_row(existing)
            if current != use:
                raise StateError("artifact-use ID already exists with different content")
            return current
        try:
            self._connection.execute(
                "INSERT INTO artifact_uses VALUES (?, ?, ?, ?, ?, ?, ?)",
                (use.use_id, use.run_id, use.artifact_id, use.decision.value,
                 use.compatibility_fingerprint, use.reason, use.recorded_at),
            )
            self._connection.commit()
        except sqlite3.IntegrityError as exc:
            self._connection.rollback()
            raise StateError("artifact-use ID already exists with different content") from exc
        return use

    def list_artifact_uses(self, run_id: str) -> tuple[ArtifactUse, ...]:
        self.get_run(run_id)
        rows = self._connection.execute(
            "SELECT use_id, run_id, artifact_id, decision, compatibility_fingerprint, reason, recorded_at FROM artifact_uses WHERE run_id = ? ORDER BY recorded_at, use_id",
            (run_id,),
        ).fetchall()
        return tuple(self._artifact_use_from_row(row) for row in rows)

    @staticmethod
    def _artifact_use_from_row(row: tuple[object, ...]) -> ArtifactUse:
        return ArtifactUse(str(row[0]), str(row[1]), str(row[2]), ArtifactDecision(str(row[3])),
                           str(row[4]), str(row[5]), str(row[6]))

    @staticmethod
    def _step_from_row(row: tuple[object, ...]) -> RunStep:
        return RunStep(str(row[0]), str(row[1]), str(row[2]), str(row[3]), StepStatus(str(row[4])),
                       tuple(json.loads(str(row[5]))) if row[5] else (),
                       tuple(json.loads(str(row[6]))) if row[6] else (), int(str(row[7])),
                       str(row[8]) if row[8] is not None else None, str(row[9]),
                       str(row[10]) if row[10] is not None else None)

    def acquire_attempt(
        self, attempt: ExecutionAttempt, *, lease_seconds: int = 300
    ) -> ExecutionAttempt:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        existing = self._connection.execute(
            "SELECT attempt_id, run_id, stage, status, worker_id, lease_expires_at, retry_number, error_code, started_at, completed_at FROM attempts WHERE attempt_id = ?",
            (attempt.attempt_id,),
        ).fetchone()
        if existing is not None:
            return self._attempt_from_row(existing)
        self.get_run(attempt.run_id)
        running = self._connection.execute(
            "SELECT 1 FROM attempts WHERE run_id = ? AND stage = ? AND status = ? LIMIT 1",
            (attempt.run_id, attempt.stage, AttemptStatus.RUNNING.value),
        ).fetchone()
        if running is not None:
            raise StateError("a running attempt already exists for this run and stage")
        lease = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat()
        claimed = replace(attempt, lease_expires_at=lease)
        try:
            self._connection.execute(
                "INSERT INTO attempts (attempt_id, run_id, stage, status, worker_id, lease_expires_at, retry_number, error_code, started_at, completed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    claimed.attempt_id,
                    claimed.run_id,
                    claimed.stage,
                    claimed.status.value,
                    claimed.worker_id,
                    claimed.lease_expires_at,
                    claimed.retry_number,
                    claimed.error_code,
                    claimed.started_at,
                    claimed.completed_at,
                ),
            )
            self._connection.commit()
        except sqlite3.IntegrityError as exc:
            self._connection.rollback()
            raise StateError("attempt cannot be acquired") from exc
        return claimed

    def list_attempts(self, run_id: str) -> tuple[ExecutionAttempt, ...]:
        self.get_run(run_id)
        rows = self._connection.execute(
            "SELECT attempt_id, run_id, stage, status, worker_id, lease_expires_at, retry_number, error_code, started_at, completed_at FROM attempts WHERE run_id = ? ORDER BY retry_number, attempt_id",
            (run_id,),
        ).fetchall()
        return tuple(self._attempt_from_row(row) for row in rows)

    def complete_attempt(
        self, attempt_id: str, *, success: bool, error_code: str | None = None
    ) -> ExecutionAttempt:
        row = self._connection.execute(
            "SELECT attempt_id, run_id, stage, status, worker_id, lease_expires_at, retry_number, error_code, started_at, completed_at FROM attempts WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()
        if row is None:
            raise StateError(f"unknown attempt: {attempt_id}")
        current = self._attempt_from_row(row)
        if current.status is not AttemptStatus.RUNNING:
            raise StateError("attempt is already complete")
        if current.lease_expires_at is not None and _parse_time(current.lease_expires_at) <= datetime.now(UTC):
            raise StateError("attempt lease has expired")
        status = AttemptStatus.SUCCEEDED if success else AttemptStatus.FAILED
        completed_at = datetime.now(UTC).isoformat()
        self._connection.execute(
            "UPDATE attempts SET status = ?, error_code = ?, completed_at = ?, lease_expires_at = NULL WHERE attempt_id = ? AND status = ?",
            (status.value, error_code, completed_at, attempt_id, AttemptStatus.RUNNING.value),
        )
        self._connection.commit()
        return replace(
            current,
            status=status,
            error_code=error_code,
            completed_at=completed_at,
            lease_expires_at=None,
        )

    def renew_attempt_lease(
        self, attempt_id: str, *, worker_id: str, lease_seconds: int = 300
    ) -> ExecutionAttempt:
        if not worker_id.strip() or lease_seconds < 1:
            raise ValueError("worker_id must not be empty and lease_seconds must be positive")
        row = self._connection.execute(
            "SELECT attempt_id, run_id, stage, status, worker_id, lease_expires_at, retry_number, error_code, started_at, completed_at FROM attempts WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()
        if row is None:
            raise StateError(f"unknown attempt: {attempt_id}")
        current = self._attempt_from_row(row)
        if current.status is not AttemptStatus.RUNNING or current.worker_id != worker_id:
            raise StateError("attempt lease is not owned by worker")
        if current.lease_expires_at is None or _parse_time(
            current.lease_expires_at
        ) <= datetime.now(UTC):
            raise StateError("attempt lease has expired")
        lease = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat()
        self._connection.execute(
            "UPDATE attempts SET lease_expires_at = ? WHERE attempt_id = ? AND status = ? AND worker_id = ?",
            (lease, attempt_id, AttemptStatus.RUNNING.value, worker_id),
        )
        self._connection.commit()
        return replace(current, lease_expires_at=lease)

    def recover_stale_attempts(
        self, *, now: datetime | None = None
    ) -> tuple[ExecutionAttempt, ...]:
        current_time = now or datetime.now(UTC)
        rows = self._connection.execute(
            "SELECT attempt_id, run_id, stage, status, worker_id, lease_expires_at, retry_number, error_code, started_at, completed_at FROM attempts WHERE status = ? AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?",
            (AttemptStatus.RUNNING.value, current_time.isoformat()),
        ).fetchall()
        recovered: list[ExecutionAttempt] = []
        for row in rows:
            attempt = self._attempt_from_row(row)
            self._connection.execute(
                "UPDATE attempts SET status = ?, error_code = ?, lease_expires_at = NULL WHERE attempt_id = ? AND status = ?",
                (
                    AttemptStatus.ABANDONED.value,
                    "stale_lease",
                    attempt.attempt_id,
                    AttemptStatus.RUNNING.value,
                ),
            )
            recovered.append(
                replace(
                    attempt,
                    status=AttemptStatus.ABANDONED,
                    error_code="stale_lease",
                    lease_expires_at=None,
                )
            )
        self._connection.commit()
        return tuple(recovered)

    @staticmethod
    def _attempt_from_row(row: tuple[object, ...]) -> ExecutionAttempt:
        return ExecutionAttempt(
            str(row[1]),
            str(row[0]),
            str(row[2]),
            AttemptStatus(str(row[3])),
            str(row[4]) if row[4] is not None else None,
            str(row[5]) if row[5] is not None else None,
            int(str(row[6])),
            str(row[7]) if row[7] is not None else None,
            str(row[8]) if row[8] is not None else "",
            str(row[9]) if row[9] is not None else None,
        )


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _encode_artifacts(artifacts: tuple[str, ...]) -> str:
    return json.dumps(list(artifacts), separators=(",", ":"))
