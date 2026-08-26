import os
from uuid import uuid4

import pytest

from sda.state import Approval, ExecutionAttempt, RunRecord, WorkflowStatus
from sda.state_postgres import PostgreSQLStateRepository


@pytest.mark.integration
def test_postgres_state_lifecycle() -> None:
    dsn = os.getenv("SDA_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set SDA_TEST_POSTGRES_DSN to run PostgreSQL integration tests")

    repository = PostgreSQLStateRepository.connect(dsn)
    run_id = "integration-" + uuid4().hex
    attempt_id = "attempt-" + uuid4().hex
    try:
        run = repository.create_run(RunRecord(run_id, "request", uuid4().hex))
        assert repository.get_run(run.run_id).status is WorkflowStatus.REQUESTED
        repository.record_approval(Approval(run_id, "human", "approved", "integration"))
        claimed = repository.acquire_attempt(
            ExecutionAttempt(run_id, attempt_id, "generate", worker_id="integration")
        )
        renewed = repository.renew_attempt_lease(
            attempt_id, worker_id="integration", lease_seconds=60
        )
        assert renewed.lease_expires_at != claimed.lease_expires_at
        completed = repository.complete_attempt(attempt_id, success=True)
        assert completed.status.value == "succeeded"
    finally:
        # Integration databases are disposable; remove only this test's run.
        repository._connection._connection.execute(
            "DELETE FROM sda_approvals WHERE run_id = %s", (run_id,)
        )
        repository._connection._connection.execute(
            "DELETE FROM sda_execution_attempts WHERE run_id = %s", (run_id,)
        )
        repository._connection._connection.execute(
            "DELETE FROM sda_runs WHERE run_id = %s", (run_id,)
        )
        repository._connection.commit()
        repository.close()
