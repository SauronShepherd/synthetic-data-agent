from __future__ import annotations

import pytest

from sda.state import RunRecord, StateError, WorkflowStatus
from sda.state_sqlite import SQLiteStateRepository


def test_sqlite_state_survives_repository_reopen(tmp_path: object) -> None:
    path = str(tmp_path / "state.db")
    repo = SQLiteStateRepository(path)
    repo.create_run(RunRecord("run-1", "req-1", "idem-1"))
    repo.transition_run("run-1", WorkflowStatus.PLANNED)
    repo.close()
    reopened = SQLiteStateRepository(path)
    assert reopened.get_run("run-1").status is WorkflowStatus.PLANNED
    reopened.close()


def test_sqlite_state_enforces_idempotency_and_version() -> None:
    repo = SQLiteStateRepository()
    run = RunRecord("run-1", "req-1", "idem-1")
    assert repo.create_run(run) == repo.create_run(run)
    repo.transition_run("run-1", WorkflowStatus.PLANNED)
    with pytest.raises(StateError, match="concurrency"):
        repo.transition_run("run-1", WorkflowStatus.APPROVED, expected_version=0)
    repo.close()
