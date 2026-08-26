from __future__ import annotations

from datetime import UTC, datetime

import pytest

from sda.artifacts.fingerprint import fingerprint
from sda.noise import (
    Mutation,
    NoiseError,
    NoisePlan,
    NoiseProfile,
    NoiseResult,
    apply_noise,
    inject_nulls,
)


def test_noise_is_deterministic_and_preserves_baseline() -> None:
    baseline = (({"id": 1, "value": "a"}), ({"id": 2, "value": "b"}), ({"id": 3, "value": "c"}))
    plan = NoisePlan("n", fingerprint(baseline), budget=2)
    first = inject_nulls(baseline, plan, column="value")
    second = inject_nulls(baseline, plan, column="value")
    assert first == second
    assert all(row["value"] is not None for row in baseline)
    assert len(first.mutations) == 2
    assert first.output_fingerprint == fingerprint(first.rows)
    assert first.output_fingerprint == second.output_fingerprint
    with pytest.raises(TypeError, match="immutable"):
        first.rows[0]["value"] = "changed"  # type: ignore[index]


def test_noise_rejects_unknown_columns() -> None:
    with pytest.raises(NoiseError, match="not present"):
        baseline = (({"id": 1},),)[0]
        inject_nulls(baseline, NoisePlan("n", fingerprint(baseline), budget=1), column="missing")


@pytest.mark.parametrize(
    ("defect_type", "column", "expected"),
    [
        ("casing", "value", "A"),
        ("misspelling", "value", "x"),
        ("malformed_value", "value", "a__MALFORMED"),
        ("invalid_category", "value", "__INVALID_CATEGORY_synthetic"),
        ("invalid_state", "value", "__INVALID_STATE_synthetic"),
        ("broken_foreign_key", "value", "__ORPHAN_FK_synthetic"),
        ("drift", "number", 2),
        ("out_of_range", "number", 11),
    ],
)
def test_supported_defects_are_deterministic_and_bounded(
    defect_type: str, column: str, expected: object
) -> None:
    baseline = (({"value": "a", "number": 1}), ({"value": "b", "number": 2}))
    plan = NoisePlan("n", fingerprint(baseline), defect_type=defect_type, budget=1)
    result = apply_noise(baseline, plan, column=column)
    assert len(result.mutations) == 1
    assert result.mutations[0].after != result.mutations[0].before
    assert result.mutations[0].defect_type == defect_type
    assert baseline[0][column] in {"a", 1}


def test_noise_rejects_unsupported_defect_and_wrong_type() -> None:
    with pytest.raises(ValueError, match="unsupported defect"):
        NoisePlan("n", "fp", defect_type="copy_production_id")
    with pytest.raises(NoiseError, match="string"):
        baseline = (({"value": 1}),)
        apply_noise(
            baseline,
            NoisePlan("n", fingerprint(baseline), defect_type="casing", budget=1),
            column="value",
        )


def test_future_timestamp_noise_preserves_datetime_type() -> None:
    baseline = (({"created_at": datetime(2020, 1, 1, tzinfo=UTC)}),)
    result = apply_noise(
        baseline,
        NoisePlan("n", fingerprint(baseline), defect_type="future_timestamp", budget=1),
        column="created_at",
    )
    assert result.rows[0]["created_at"] == datetime(2020, 12, 31, tzinfo=UTC)


def test_out_of_order_timestamp_noise_moves_datetime_backwards() -> None:
    baseline = (({"created_at": datetime(2020, 1, 1, tzinfo=UTC)}),)
    result = apply_noise(
        baseline,
        NoisePlan("n", fingerprint(baseline), defect_type="out_of_order_timestamp", budget=1),
        column="created_at",
    )
    assert result.rows[0]["created_at"] == datetime(2019, 1, 1, tzinfo=UTC)


def test_omission_removes_only_deterministically_selected_columns() -> None:
    baseline = (({"id": 1, "value": "a"}), ({"id": 2, "value": "b"}))
    result = apply_noise(
        baseline,
        NoisePlan("n", fingerprint(baseline), defect_type="omission", budget=1),
        column="value",
    )
    assert len(result.mutations) == 1
    assert sum("value" not in row for row in result.rows) == 1
    assert all("value" in row for row in baseline)


def test_duplicate_noise_copies_a_stable_neighbor_value() -> None:
    baseline = (({"id": 1, "value": "a"}), ({"id": 2, "value": "b"}))
    result = apply_noise(
        baseline,
        NoisePlan("n", fingerprint(baseline), defect_type="duplicate", budget=1),
        column="value",
    )
    mutation = result.mutations[0]
    assert mutation.after == result.rows[(mutation.row_index - 1) % len(result.rows)]["value"]
    assert baseline == (({"id": 1, "value": "a"}), ({"id": 2, "value": "b"}))


def test_duplicate_noise_rejects_single_row_baselines() -> None:
    baseline = (({"value": "a"}),)
    with pytest.raises(NoiseError, match="at least two"):
        apply_noise(
            baseline,
            NoisePlan("n", fingerprint(baseline), defect_type="duplicate", budget=1),
            column="value",
        )


def test_near_duplicate_noise_is_bounded_and_type_checked() -> None:
    baseline = (({"value": "alpha"}), ({"value": "beta"}))
    result = apply_noise(
        baseline,
        NoisePlan("n", fingerprint(baseline), defect_type="near_duplicate", budget=1),
        column="value",
    )
    mutation = result.mutations[0]
    assert mutation.after != mutation.before
    assert len(mutation.after) == len(mutation.before)
    with pytest.raises(NoiseError, match="string"):
        numeric = (({"value": 1}),)
        apply_noise(
            numeric,
            NoisePlan("n", fingerprint(numeric), defect_type="near_duplicate", budget=1),
            column="value",
        )


def test_near_duplicate_noise_rejects_empty_strings() -> None:
    baseline = (({"value": ""}),)
    with pytest.raises(NoiseError, match="non-empty"):
        apply_noise(
            baseline,
            NoisePlan("n", fingerprint(baseline), defect_type="near_duplicate", budget=1),
            column="value",
        )


def test_noise_truth_ledger_is_raw_value_free() -> None:
    baseline = (({"value": "secret"}),)
    result = apply_noise(
        baseline,
        NoisePlan("n", fingerprint(baseline), defect_type="casing", budget=1),
        column="value",
    )
    ledger = result.truth_ledger()
    assert ledger[0]["before_fingerprint"] == fingerprint("secret")
    assert ledger[0]["after_fingerprint"] == fingerprint("SECRET")
    assert "secret" not in str(ledger)
    serialized = result.to_dict()
    assert "secret" not in str(serialized)
    assert serialized["mutation_count"] == 1


def test_noise_result_rejects_forged_output_fingerprint() -> None:
    with pytest.raises(ValueError, match="does not match"):
        NoiseResult(({"value": "clean"},), (), "baseline", "forged")


def test_noise_result_rejects_out_of_range_mutations() -> None:
    with pytest.raises(ValueError, match="outside result rows"):
        NoiseResult(
            ({"value": "clean"},),
            (Mutation("n", 1, "value", "casing", "clean", "CLEAN"),),
            "baseline",
            fingerprint(({"value": "clean"},)),
        )


def test_historical_noise_profile_is_a_supported_plan_contract() -> None:
    plan = NoisePlan("n", "baseline", profile=NoiseProfile.HISTORICAL)
    assert plan.profile.value == "historical"


def test_noise_plan_normalizes_profile_strings_and_rejects_unknown_profiles() -> None:
    assert NoisePlan("n", "baseline", profile="qa").profile is NoiseProfile.QA
    with pytest.raises(ValueError, match="unsupported noise profile"):
        NoisePlan("n", "baseline", profile="unknown")


def test_noise_profiles_have_deterministic_default_budgets() -> None:
    baseline = tuple({"value": str(index)} for index in range(20))
    results = {
        profile: apply_noise(
            baseline,
            NoisePlan("n", fingerprint(baseline), profile=profile, defect_type="casing"),
            column="value",
        )
        for profile in NoiseProfile
    }
    assert len(results[NoiseProfile.HISTORICAL].mutations) == 0
    assert len(results[NoiseProfile.MILD].mutations) == 1
    assert len(results[NoiseProfile.QA].mutations) == 2
    assert len(results[NoiseProfile.STRESS].mutations) == 5


def test_noise_scenarios_are_deterministic_but_independently_selected() -> None:
    baseline = tuple({"value": str(index)} for index in range(4))
    first = apply_noise(
        baseline,
        NoisePlan("n", fingerprint(baseline), defect_type="casing", budget=1, scenario="holiday"),
        column="value",
    )
    second = apply_noise(
        baseline,
        NoisePlan("n", fingerprint(baseline), defect_type="casing", budget=1, scenario="holiday"),
        column="value",
    )
    other = apply_noise(
        baseline,
        NoisePlan("n", fingerprint(baseline), defect_type="casing", budget=1, scenario="peak"),
        column="value",
    )
    assert first == second
    assert first.mutations[0].row_index != other.mutations[0].row_index


def test_rate_based_noise_budget_is_deterministic_and_bounded() -> None:
    baseline = tuple({"value": str(index)} for index in range(5))
    result = apply_noise(
        baseline,
        NoisePlan("n", fingerprint(baseline), defect_type="casing", budget_rate=0.4),
        column="value",
    )
    assert len(result.mutations) == 2
    with pytest.raises(ValueError, match="mutually exclusive"):
        NoisePlan("n", "fp", budget=1, budget_rate=0.5)


def test_noise_plan_serialization_is_complete_and_raw_value_free() -> None:
    plan = NoisePlan(
        "noise-1",
        "baseline-fingerprint",
        profile=NoiseProfile.STRESS,
        defect_type="malformed_value",
        budget_rate=0.25,
        seed=7,
        scenario="incident",
    )
    assert plan.to_dict() == {
        "noise_id": "noise-1",
        "baseline_fingerprint": "baseline-fingerprint",
        "profile": "stress",
        "defect_type": "malformed_value",
        "budget": 0,
        "budget_rate": 0.25,
        "seed": 7,
        "scenario": "incident",
    }
    assert "secret" not in str(plan.to_dict())
