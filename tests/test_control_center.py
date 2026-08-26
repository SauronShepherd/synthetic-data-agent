from __future__ import annotations

from sda.control_center import build_snapshot
from sda.operations import AuditEvent, AuditLevel, AuditLog
from sda.state import Approval, ExecutionAttempt, Feedback, RunRecord


def test_control_center_snapshot_is_safe_and_read_only() -> None:
    run = RunRecord("run-1", "req-1", "idem-1")
    log = AuditLog()
    log.append(AuditEvent("run-1", "started", AuditLevel.INFO, "started"))
    snapshot = build_snapshot(run, artifact_ids=("artifact-1",), audit_log=log)
    safe = snapshot.to_safe_dict()
    assert safe["run_id"] == "run-1"
    assert safe["artifact_ids"] == ("artifact-1",)
    assert safe["audit_event_count"] == 1


def test_control_center_includes_safe_workflow_history_counts() -> None:
    run = RunRecord("run-1", "req-1", "idem-1")
    safe = build_snapshot(
        run,
        approvals=(Approval("run-1", "human", "approved", "reviewer"),),
        attempts=(ExecutionAttempt("run-1", "attempt-1", "generate"),),
        feedback=(Feedback("feedback-1", "run-1", "reviewer", "note", "review"),),
    ).to_safe_dict()
    assert safe["approval_count"] == 1
    assert safe["attempt_count"] == 1
    assert safe["feedback_count"] == 1
