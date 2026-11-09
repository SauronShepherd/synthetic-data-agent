import os
from uuid import uuid4

import pytest

from sda.operations import AuditEvent, AuditLevel
from sda.state import Approval, ExecutionAttempt, RequestRecord, RunRecord, WorkflowStatus
from sda.state_postgres import PostgreSQLStateRepository


@pytest.mark.integration
def test_postgres_state_lifecycle() -> None:
    dsn = os.getenv("SDA_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set SDA_TEST_POSTGRES_DSN to run PostgreSQL integration tests")

    repository = PostgreSQLStateRepository.connect(dsn)
    request_id = "request-" + uuid4().hex
    run_id = "integration-" + uuid4().hex
    attempt_id = "attempt-" + uuid4().hex
    try:
        request = repository.create_request(
            RequestRecord(request_id, "integration", "test", uuid4().hex, '{}')
        )
        run = repository.create_run(RunRecord(run_id, request.request_id, uuid4().hex))
        assert request.request_id == run.request_id
        assert repository.get_run(run.run_id).status is WorkflowStatus.REQUESTED
        audit_event = AuditEvent(
            run_id, "integration_started", AuditLevel.INFO, "Integration started", metadata={"live": True}
        )
        repository.record_audit_event(audit_event)
        assert repository.list_audit_events(run_id) == (audit_event,)
        repository.record_approval(
            Approval(run_id, "human", "approved", "integration", "integration approval")
        )
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
            "DELETE FROM sda_audit_events WHERE run_id = %s", (run_id,)
        )
        repository._connection._connection.execute(
            "DELETE FROM sda_execution_attempts WHERE run_id = %s", (run_id,)
        )
        repository._connection._connection.execute(
            "DELETE FROM sda_runs WHERE run_id = %s", (run_id,)
        )
        repository._connection._connection.execute(
            "DELETE FROM sda_generation_requests WHERE request_id = %s", (request_id,)
        )
        repository._connection.commit()
        repository.close()
