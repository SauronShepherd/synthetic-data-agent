from __future__ import annotations

from dataclasses import replace

import pytest

from sda.generation import GenerationError, generate_rows, resolve_row_count
from sda.planning import ColumnGenerationSpec, GenerationPlan, PlanStatus, RowCountMode


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
            tables=("t",),
            columns=(
                ColumnGenerationSpec("t", "id", "string", nullable=False, model="identifier"),
                ColumnGenerationSpec("t", "segment", "string", model="categorical"),
                ColumnGenerationSpec(
                    "t", "amount", "double", model="uniform", parameters={"min": 10, "max": 10}
                ),
            ),
            budgets={"max_rows": 3},
            requested_row_count=3,
        )
        .transition(PlanStatus.AWAITING_APPROVAL)
        .transition(PlanStatus.APPROVED)
    )


def test_generation_is_reproducible_and_bounded() -> None:
    first = generate_rows(plan(), row_count=3, vocabularies={"segment": ("a", "b")})
    second = generate_rows(plan(), row_count=3, vocabularies={"segment": ("a", "b")})
    assert first == second
    assert len({row["id"] for row in first}) == 3
    assert [row["segment"] for row in first] == ["a", "b", "a"]


def test_generation_rejects_unapproved_and_over_budget_plans() -> None:
    with pytest.raises(GenerationError, match="approved"):
        generate_rows(replace(plan(), status=PlanStatus.DRAFT, plan_fingerprint=""), row_count=1)
    with pytest.raises(GenerationError, match="max_rows"):
        generate_rows(plan(), row_count=4)


def test_empirical_models_are_replayable_and_type_checked() -> None:
    empirical_plan = replace(
        plan(),
        columns=(
            ColumnGenerationSpec("t", "amount", "double", model="empirical_numeric"),
            ColumnGenerationSpec("t", "segment", "string", model="empirical_categorical"),
        ),
        plan_fingerprint="",
    )
    samples = {"amount": (10.0, 20.0), "segment": ("A", "B")}
    first = generate_rows(empirical_plan, row_count=3, empirical_samples=samples)
    assert first == generate_rows(empirical_plan, row_count=3, empirical_samples=samples)
    assert {row["amount"] for row in first} <= {10.0, 20.0}
    assert {row["segment"] for row in first} <= {"A", "B"}
    with pytest.raises(GenerationError, match="requires empirical"):
        generate_rows(empirical_plan, row_count=1)


def test_plan_resolves_exact_and_probabilistic_row_counts() -> None:
    exact = plan()
    assert resolve_row_count(exact) == 3
    assert len(generate_rows(exact, vocabularies={"segment": ("a", "b")})) == 3
    probabilistic = replace(
        exact,
        requested_row_count=None,
        row_count_mode=RowCountMode.PROBABILISTIC,
        scale_factor=1.5,
        plan_fingerprint="",
    )
    assert resolve_row_count(probabilistic, source_row_count=3) == 5


def test_generation_rejects_invalid_null_probabilities() -> None:
    invalid = replace(
        plan(),
        columns=(ColumnGenerationSpec("t", "value", "string", parameters={"null_rate": 1.1}),),
        plan_fingerprint="",
    )
    with pytest.raises(GenerationError, match="null_rate"):
        generate_rows(invalid, row_count=1)


def test_weighted_categorical_sampling_is_replayable_and_validated() -> None:
    first = generate_rows(
        plan(), row_count=3, weighted_vocabularies={"segment": (("rare", 1.0), ("common", 9.0))}
    )
    assert first == generate_rows(
        plan(), row_count=3, weighted_vocabularies={"segment": (("rare", 1.0), ("common", 9.0))}
    )
    assert {row["segment"] for row in first} <= {"rare", "common"}
    with pytest.raises(GenerationError, match="weights"):
        generate_rows(plan(), row_count=1, weighted_vocabularies={"segment": (("x", -1.0),)})
