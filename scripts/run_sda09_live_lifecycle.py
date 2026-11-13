"""Run the disposable SDA-09 Lakebase lifecycle acceptance.

The flow exercises a request, run transition, immutable plan, audit event,
approval, failed and successful step attempts, artifact reuse, and a renewable
execution lease. Cleanup deletes child history before parent rows so foreign
keys prove the same relationship that production relies on.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import replace
from uuid import uuid4

from sda.operations import AuditEvent, AuditLevel
from sda.state import (
    Approval,
    ArtifactDecision,
    ArtifactUse,
    ExecutionAttempt,
    PlanRecord,
    RequestRecord,
    RunRecord,
    RunStep,
    StepStatus,
    WorkflowStatus,
)
from sda.state_postgres import PostgreSQLStateRepository


def _cleanup(repository: PostgreSQLStateRepository, run_id: str, request_id: str) -> None:
    raw = repository._connection._connection  # noqa: SLF001 - acceptance cleanup boundary
    for table in (
        "sda_approvals",
        "sda_audit_events",
        "sda_artifact_uses",
        "sda_run_steps",
        "sda_execution_attempts",
        "sda_generation_plans",
        "sda_transition_events",
    ):
        raw.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))
    raw.execute("DELETE FROM sda_runs WHERE run_id = %s", (run_id,))
    raw.execute("DELETE FROM sda_generation_requests WHERE request_id = %s", (request_id,))
    repository._connection.commit()  # noqa: SLF001 - acceptance cleanup boundary


def run_lifecycle(dsn: str) -> None:
    repository = PostgreSQLStateRepository.connect(dsn)
    request_id = "sda09-live-request-" + uuid4().hex
    run_id = "sda09-live-run-" + uuid4().hex
    try:
        request = repository.create_request(
            RequestRecord(request_id, "sda09-live", "dev", uuid4().hex, "{}")
        )
        run = repository.create_run(RunRecord(run_id, request.request_id, uuid4().hex))
        repository.transition_run(run.run_id, WorkflowStatus.PLANNED, actor="sda09-live")
        plan = PlanRecord(
            "sda09-live-plan-" + uuid4().hex,
            request.request_id,
            run.run_id,
            1,
            "sda09-live-fingerprint",
            "approved",
            "{}",
        )
        assert repository.create_plan(plan) == plan
        event = AuditEvent(
            run_id, "sda09_live", AuditLevel.INFO, "SDA-09 live lifecycle", metadata={"live": True}
        )
        assert repository.record_audit_event(event) == event
        assert repository.record_approval(
            Approval(run_id, "human", "approved", "sda09-live", "acceptance")
        )

        first_step = RunStep(
            "sda09-live-step-" + uuid4().hex,
            run_id,
            "profile",
            "table_profiler",
            input_artifacts=("metadata-1",),
            attempt_number=1,
        )
        repository.create_step(first_step)
        repository.update_step(replace(first_step, status=StepStatus.FAILED, error_code="TIMEOUT"))
        retry_step = RunStep(
            "sda09-live-step-" + uuid4().hex,
            run_id,
            "profile",
            "table_profiler",
            status=StepStatus.SUCCEEDED,
            output_artifacts=("profile-1",),
            attempt_number=2,
        )
        repository.create_step(retry_step)
        artifact_use = ArtifactUse(
            "sda09-live-use-" + uuid4().hex,
            run_id,
            "profile-1",
            ArtifactDecision.REUSED,
            "sda09-live-fingerprint",
            "compatible profile reused",
        )
        assert repository.record_artifact_use(artifact_use) == repository.record_artifact_use(
            artifact_use
        )
        attempt_id = "sda09-live-attempt-" + uuid4().hex
        claimed = repository.acquire_attempt(
            ExecutionAttempt(run_id, attempt_id, "generate", worker_id="sda09-live")
        )
        renewed = repository.renew_attempt_lease(
            attempt_id, worker_id="sda09-live", lease_seconds=60
        )
        assert renewed.lease_expires_at != claimed.lease_expires_at
        assert repository.complete_attempt(attempt_id, success=True).status.value == "succeeded"
        print("live_repository_lifecycle=OK")
    finally:
        _cleanup(repository, run_id, request_id)
        repository.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-env", default="SDA_POSTGRES_DSN")
    args = parser.parse_args()
    dsn = os.environ.get(args.dsn_env, "")
    if not dsn:
        raise SystemExit(f"environment variable {args.dsn_env} is empty")
    run_lifecycle(dsn)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
