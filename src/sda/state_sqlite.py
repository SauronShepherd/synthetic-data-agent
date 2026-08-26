"""SQLite reference persistence for the workflow-state contract.

The SQL shape intentionally mirrors the constraints expected from Lakebase. It is
used for local recovery tests; production deployments should provide a PostgreSQL
adapter with the same repository methods.
"""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from sda.state import (
    Approval,
    AttemptStatus,
    ExecutionAttempt,
    Feedback,
    RunRecord,
    StateError,
    WorkflowStatus,
)


class SQLiteStateRepository:
    def __init__(self, path: str = ":memory:") -> None:
        self._connection = sqlite3.connect(path)
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE, status TEXT NOT NULL, plan_id TEXT, plan_fingerprint TEXT, version INTEGER NOT NULL, updated_at TEXT NOT NULL)"
        )
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS attempts (attempt_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(run_id), stage TEXT NOT NULL, status TEXT NOT NULL, worker_id TEXT, lease_expires_at TEXT, retry_number INTEGER NOT NULL, error_code TEXT)"
        )
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS approvals (run_id TEXT NOT NULL REFERENCES runs(run_id), approval_type TEXT NOT NULL, decision TEXT NOT NULL, actor TEXT NOT NULL, reason TEXT NOT NULL, PRIMARY KEY (run_id, approval_type))"
        )
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS feedback (feedback_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(run_id), actor TEXT NOT NULL, category TEXT NOT NULL, message TEXT NOT NULL, evidence_ref TEXT, created_at TEXT NOT NULL)"
        )
        self._connection.commit()

    def create_run(self, run: RunRecord) -> RunRecord:
        existing = self._connection.execute(
            "SELECT run_id FROM runs WHERE idempotency_key = ?", (run.idempotency_key,)
        ).fetchone()
        if existing:
            return self.get_run(existing[0])
        try:
            self._connection.execute(
                "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
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
        self, run_id: str, status: WorkflowStatus, *, expected_version: int | None = None
    ) -> RunRecord:
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
        self._connection.commit()
        return updated

    def close(self) -> None:
        self._connection.close()

    def record_approval(self, approval: Approval) -> Approval:
        self.get_run(approval.run_id)
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
            raise StateError("approval already recorded") from exc
        return approval

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

    def acquire_attempt(
        self, attempt: ExecutionAttempt, *, lease_seconds: int = 300
    ) -> ExecutionAttempt:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        existing = self._connection.execute(
            "SELECT attempt_id, run_id, stage, status, worker_id, lease_expires_at, retry_number, error_code FROM attempts WHERE attempt_id = ?",
            (attempt.attempt_id,),
        ).fetchone()
        if existing is not None:
            return self._attempt_from_row(existing)
        self.get_run(attempt.run_id)
        lease = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat()
        claimed = replace(attempt, lease_expires_at=lease)
        try:
            self._connection.execute(
                "INSERT INTO attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    claimed.attempt_id,
                    claimed.run_id,
                    claimed.stage,
                    claimed.status.value,
                    claimed.worker_id,
                    claimed.lease_expires_at,
                    claimed.retry_number,
                    claimed.error_code,
                ),
            )
            self._connection.commit()
        except sqlite3.IntegrityError as exc:
            self._connection.rollback()
            raise StateError("attempt cannot be acquired") from exc
        return claimed

    def complete_attempt(
        self, attempt_id: str, *, success: bool, error_code: str | None = None
    ) -> ExecutionAttempt:
        row = self._connection.execute(
            "SELECT attempt_id, run_id, stage, status, worker_id, lease_expires_at, retry_number, error_code FROM attempts WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()
        if row is None:
            raise StateError(f"unknown attempt: {attempt_id}")
        current = self._attempt_from_row(row)
        if current.status is not AttemptStatus.RUNNING:
            raise StateError("attempt is already complete")
        status = AttemptStatus.SUCCEEDED if success else AttemptStatus.FAILED
        self._connection.execute(
            "UPDATE attempts SET status = ?, error_code = ?, lease_expires_at = NULL WHERE attempt_id = ? AND status = ?",
            (status.value, error_code, attempt_id, AttemptStatus.RUNNING.value),
        )
        self._connection.commit()
        return replace(current, status=status, error_code=error_code, lease_expires_at=None)

    def renew_attempt_lease(
        self, attempt_id: str, *, worker_id: str, lease_seconds: int = 300
    ) -> ExecutionAttempt:
        if not worker_id.strip() or lease_seconds < 1:
            raise ValueError("worker_id must not be empty and lease_seconds must be positive")
        row = self._connection.execute(
            "SELECT attempt_id, run_id, stage, status, worker_id, lease_expires_at, retry_number, error_code FROM attempts WHERE attempt_id = ?",
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
            "SELECT attempt_id, run_id, stage, status, worker_id, lease_expires_at, retry_number, error_code FROM attempts WHERE status = ? AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?",
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
        )


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
