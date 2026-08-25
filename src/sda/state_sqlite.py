"""SQLite reference persistence for the workflow-state contract.

The SQL shape intentionally mirrors the constraints expected from Lakebase. It is
used for local recovery tests; production deployments should provide a PostgreSQL
adapter with the same repository methods.
"""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import UTC, datetime

from sda.state import RunRecord, StateError, WorkflowStatus


class SQLiteStateRepository:
    def __init__(self, path: str = ":memory:") -> None:
        self._connection = sqlite3.connect(path)
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE, status TEXT NOT NULL, plan_id TEXT, plan_fingerprint TEXT, version INTEGER NOT NULL, updated_at TEXT NOT NULL)"
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
