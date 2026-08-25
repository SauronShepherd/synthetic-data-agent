from __future__ import annotations

import pytest

from sda.planning import ColumnGenerationSpec, GenerationMode, GenerationPlan, PlanStatus


def make_plan() -> GenerationPlan:
    return GenerationPlan(
        plan_id="plan-1",
        plan_version=1,
        request_id="req-1",
        source_snapshot_ids=("snapshot-1",),
        input_artifact_ids=("profile-1", "pattern-1"),
        target_catalog="synthetic",
        target_schema="sales",
        tables=("customers",),
        columns=(ColumnGenerationSpec("customers", "id", "bigint", nullable=False),),
        mode=GenerationMode.CLEAN,
        intended_use="qa",
    )


def test_plan_fingerprint_is_stable_and_serializable() -> None:
    plan = make_plan()
    assert plan.plan_fingerprint == plan.compute_fingerprint()
    assert plan.to_dict()["status"] == "draft"


def test_plan_requires_evidence() -> None:
    with pytest.raises(ValueError, match="source snapshots"):
        GenerationPlan(
            plan_id="p", plan_version=1, request_id="r", source_snapshot_ids=(),
            input_artifact_ids=("a",), target_catalog="c", target_schema="s",
            tables=("t",), columns=(),
        )


def test_plan_transitions_are_fail_closed() -> None:
    plan = make_plan().transition(PlanStatus.AWAITING_APPROVAL).transition(PlanStatus.APPROVED)
    assert plan.status is PlanStatus.APPROVED
    with pytest.raises(ValueError, match="invalid plan transition"):
        plan.transition(PlanStatus.DRAFT)
