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


def test_plan_normalizes_enum_strings_and_rejects_unknown_values() -> None:
    plan = GenerationPlan(
        "p",
        1,
        "r",
        ("s",),
        ("a",),
        "c",
        "s",
        ("t",),
        (ColumnGenerationSpec("t", "id", "string"),),
        status="awaiting_approval",
        mode="clean",
        row_count_mode="exact",
    )
    assert plan.status is PlanStatus.AWAITING_APPROVAL
    with pytest.raises(ValueError, match="unsupported status"):
        GenerationPlan(
            "p",
            1,
            "r",
            ("s",),
            ("a",),
            "c",
            "s",
            ("t",),
            (ColumnGenerationSpec("t", "id", "string"),),
            status="unknown",
        )


def test_plan_requires_evidence() -> None:
    with pytest.raises(ValueError, match="source snapshots"):
        GenerationPlan(
            plan_id="p",
            plan_version=1,
            request_id="r",
            source_snapshot_ids=(),
            input_artifact_ids=("a",),
            target_catalog="c",
            target_schema="s",
            tables=("t",),
            columns=(),
        )


def test_plan_transitions_are_fail_closed() -> None:
    plan = make_plan().transition(PlanStatus.AWAITING_APPROVAL).transition(PlanStatus.APPROVED)
    assert plan.status is PlanStatus.APPROVED
    with pytest.raises(ValueError, match="invalid plan transition"):
        plan.transition(PlanStatus.DRAFT)


def test_nested_plan_mappings_are_immutable() -> None:
    plan = GenerationPlan(
        "p",
        1,
        "r",
        ("s",),
        ("a",),
        "c",
        "s",
        ("t",),
        (ColumnGenerationSpec("t", "id", "string", parameters={"prefix": "x"}),),
        budgets={"max_rows": 2},
    )
    with pytest.raises(TypeError, match="immutable"):
        plan.budgets["max_rows"] = 3  # type: ignore[index]
    with pytest.raises(TypeError, match="immutable"):
        plan.columns[0].parameters["prefix"] = "y"  # type: ignore[index]
    assert plan.plan_fingerprint == plan.compute_fingerprint()


def test_plan_rejects_ambiguous_or_undeclared_columns() -> None:
    base = dict(
        plan_id="p",
        plan_version=1,
        request_id="r",
        source_snapshot_ids=("s",),
        input_artifact_ids=("a",),
        target_catalog="c",
        target_schema="s",
        tables=("t",),
    )
    with pytest.raises(ValueError, match="unique table columns"):
        GenerationPlan(**base, columns=(ColumnGenerationSpec("t", "id", "string"),) * 2)
    with pytest.raises(ValueError, match="declared target"):
        GenerationPlan(**base, columns=(ColumnGenerationSpec("other", "id", "string"),))


@pytest.mark.parametrize("field", ["target_catalog", "target_schema"])
def test_plan_rejects_unsafe_target_identifiers(field: str) -> None:
    values = {
        "plan_id": "p",
        "plan_version": 1,
        "request_id": "r",
        "source_snapshot_ids": ("s",),
        "input_artifact_ids": ("a",),
        "target_catalog": "c",
        "target_schema": "s",
        "tables": ("t",),
        "columns": (ColumnGenerationSpec("t", "id", "string"),),
    }
    values[field] = "bad;drop"
    with pytest.raises(ValueError, match="unsafe"):
        GenerationPlan(**values)
