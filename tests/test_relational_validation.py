from __future__ import annotations

import pytest

from sda.planning import ColumnGenerationSpec, GenerationPlan, PlanStatus
from sda.relational import (
    CompositeForeignKeySpec,
    ForeignKeySpec,
    RelationalGenerationError,
    generate_relational,
)
from sda.validation import CheckStatus, ValidationCheck, ValidationReport, validate_tables


def test_validation_evidence_is_immutable_and_check_ids_are_unique() -> None:
    check = ValidationCheck("schema", CheckStatus.PASS, "ok", {"actual": 1})
    with pytest.raises(TypeError, match="immutable"):
        check.evidence["actual"] = 2
    with pytest.raises(ValueError, match="unique"):
        ValidationReport((check, check), "qa", CheckStatus.PASS)


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


def test_nullable_optional_foreign_keys_are_deterministic_and_orphan_free() -> None:
    fk = ForeignKeySpec("child", "parent_id", "parent", "id", nullable=True, optional_rate=0.5)
    first = generate_relational(plan(), row_counts={"parent": 3, "child": 7}, foreign_keys=(fk,))
    second = generate_relational(plan(), row_counts={"parent": 3, "child": 7}, foreign_keys=(fk,))
    assert first == second
    values = [row["parent_id"] for row in first["child"]]
    assert any(value is None for value in values)
    assert any(value is not None for value in values)
    assert set(value for value in values if value is not None) <= {
        row["id"] for row in first["parent"]
    }


def test_optional_rate_requires_nullable_foreign_key() -> None:
    with pytest.raises(ValueError, match="nullable"):
        ForeignKeySpec("child", "parent_id", "parent", "id", optional_rate=0.1)


def test_validation_does_not_turn_missing_inputs_into_success() -> None:
    report = validate_tables(
        {},
        expected_counts={"customers": 0},
        unique_keys={"customers": "id"},
        foreign_keys=(("orders", "customer_id", "customers", "id"),),
    )
    assert report.technical_disposition is CheckStatus.FAIL
    assert all(check.status is CheckStatus.FAIL for check in report.checks)
