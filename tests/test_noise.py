from __future__ import annotations

import pytest

from sda.artifacts.fingerprint import fingerprint
from sda.noise import NoiseError, NoisePlan, NoiseProfile, apply_noise, inject_nulls


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
        ("malformed_value", "value", "a__MALFORMED"),
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


def test_historical_noise_profile_is_a_supported_plan_contract() -> None:
    plan = NoisePlan("n", "baseline", profile=NoiseProfile.HISTORICAL)
    assert plan.profile.value == "historical"
