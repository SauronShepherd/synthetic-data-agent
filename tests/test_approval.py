from __future__ import annotations

import pytest

from sda.approval import approve_generation_plan
from sda.planning import ColumnGenerationSpec, GenerationPlan, PlanStatus
from sda.state import InMemoryStateRepository, RunRecord, StateError, WorkflowStatus


def plan() -> GenerationPlan:
    return GenerationPlan(
        plan_id="p", plan_version=1, request_id="r", source_snapshot_ids=("s",), input_artifact_ids=("a",),
        target_catalog="c", target_schema="s", tables=("t",),
        columns=(ColumnGenerationSpec("t", "id", "string", nullable=False, model="identifier"),),
    ).transition(PlanStatus.AWAITING_APPROVAL)


def test_plan_approval_records_actor_and_advances_run() -> None:
    repository = InMemoryStateRepository()
    repository.create_run(RunRecord("run-1", "r", "idem"))
    repository.transition_run("run-1", WorkflowStatus.PLANNED)
    repository.transition_run("run-1", WorkflowStatus.AWAITING_APPROVAL)
    decision = approve_generation_plan(plan(), run_id="run-1", actor="reviewer", reason="approved for QA", repository=repository)
    assert decision.plan.status is PlanStatus.APPROVED
    assert repository.get_run("run-1").status is WorkflowStatus.APPROVED


def test_plan_approval_requires_pending_plan_and_reason() -> None:
    repository = InMemoryStateRepository()
    repository.create_run(RunRecord("run-1", "r", "idem"))
    with pytest.raises(StateError, match="awaiting"):
        approve_generation_plan(plan().transition(PlanStatus.REJECTED), run_id="run-1", actor="reviewer", reason="x", repository=repository)
