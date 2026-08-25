from __future__ import annotations

import pytest

from sda.planning import ColumnGenerationSpec, GenerationPlan, PlanStatus
from sda.relational import (
    CompositeForeignKeySpec,
    ForeignKeySpec,
    RelationalGenerationError,
    generate_relational,
)
from sda.validation import CheckStatus, validate_tables


def plan() -> GenerationPlan:
    return (
        GenerationPlan(
            plan_id="p",
            plan_version=1,
            request_id="r",
            source_snapshot_ids=("s",),
            input_artifact_ids=("a",),
            target_catalog="c",
            target_schema="s",
            tables=("parent", "child"),
            columns=(
                ColumnGenerationSpec("parent", "id", "string", nullable=False, model="identifier"),
                ColumnGenerationSpec("child", "id", "string", nullable=False, model="identifier"),
            ),
            budgets={"max_rows": 10},
        )
        .transition(PlanStatus.AWAITING_APPROVAL)
        .transition(PlanStatus.APPROVED)
    )


def test_relational_generation_is_orphan_free() -> None:
    fk = ForeignKeySpec("child", "parent_id", "parent", "id")
    tables = generate_relational(plan(), row_counts={"parent": 2, "child": 5}, foreign_keys=(fk,))
    report = validate_tables(
        tables,
        expected_counts={"parent": 2, "child": 5},
        unique_keys={"parent": "id"},
        foreign_keys=(("child", "parent_id", "parent", "id"),),
        intended_use="qa",
    )
    assert report.technical_disposition is CheckStatus.PASS
    assert len(report.checks) == 4


def test_cycles_fail_closed() -> None:
    with pytest.raises(RelationalGenerationError, match="cycle"):
        generate_relational(
            plan(),
            row_counts={"parent": 1, "child": 1},
            foreign_keys=(
                ForeignKeySpec("parent", "child_id", "child", "id"),
                ForeignKeySpec("child", "parent_id", "parent", "id"),
            ),
        )


def test_composite_foreign_keys_are_wired_as_tuples() -> None:
    composite_plan = (
        GenerationPlan(
            plan_id="cp",
            plan_version=1,
            request_id="r",
            source_snapshot_ids=("s",),
            input_artifact_ids=("a",),
            target_catalog="c",
            target_schema="s",
            tables=("parent", "child"),
            columns=(
                ColumnGenerationSpec(
                    "parent", "id_a", "string", nullable=False, model="identifier"
                ),
                ColumnGenerationSpec(
                    "parent", "id_b", "string", nullable=False, model="identifier"
                ),
                ColumnGenerationSpec("child", "id", "string", nullable=False, model="identifier"),
            ),
            budgets={"max_rows": 10},
        )
        .transition(PlanStatus.AWAITING_APPROVAL)
        .transition(PlanStatus.APPROVED)
    )
    fk = CompositeForeignKeySpec("child", ("a", "b"), "parent", ("id_a", "id_b"))
    tables = generate_relational(
        composite_plan, row_counts={"parent": 2, "child": 2}, composite_foreign_keys=(fk,)
    )
    assert all(
        (row["a"], row["b"]) in {(p["id_a"], p["id_b"]) for p in tables["parent"]}
        for row in tables["child"]
    )


def test_validator_reports_orphans_as_failure() -> None:
    report = validate_tables(
        {"p": (({"id": "p1"}),), "c": (({"p": "missing"}),)},
        foreign_keys=(("c", "p", "p", "id"),),
    )
    assert report.technical_disposition is CheckStatus.FAIL
