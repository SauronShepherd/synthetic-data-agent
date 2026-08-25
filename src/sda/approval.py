"""Human approval operations connecting plans to durable workflow state."""

from __future__ import annotations

from dataclasses import dataclass

from sda.planning import GenerationPlan, PlanStatus
from sda.state import Approval, InMemoryStateRepository, StateError, WorkflowStatus


@dataclass(frozen=True, slots=True)
class ApprovalDecision:
    plan: GenerationPlan
    workflow_status: WorkflowStatus
    approval: Approval


def approve_generation_plan(
    plan: GenerationPlan,
    *,
    run_id: str,
    actor: str,
    reason: str,
    repository: InMemoryStateRepository,
) -> ApprovalDecision:
    """Approve a draft plan only after it has entered an approval state."""
    if not actor.strip() or not reason.strip():
        raise ValueError("approval actor and reason are required")
    if plan.status is not PlanStatus.AWAITING_APPROVAL:
        raise StateError("only plans awaiting approval may be approved")
    approved_plan = plan.transition(PlanStatus.APPROVED)
    approval = Approval(run_id, "generation_plan", "approved", actor, reason)
    repository.record_approval(approval)
    current = repository.get_run(run_id)
    if current.status is WorkflowStatus.AWAITING_APPROVAL:
        repository.transition_run(run_id, WorkflowStatus.APPROVED, expected_version=current.version)
    return ApprovalDecision(approved_plan, WorkflowStatus.APPROVED, approval)
