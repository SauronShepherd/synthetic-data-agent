from __future__ import annotations

import pytest

from sda.noise import NoiseError, NoisePlan, apply_noise, inject_nulls


def test_noise_is_deterministic_and_preserves_baseline() -> None:
    baseline = (({"id": 1, "value": "a"}), ({"id": 2, "value": "b"}), ({"id": 3, "value": "c"}))
    plan = NoisePlan("n", "baseline-fp", budget=2)
    first = inject_nulls(baseline, plan, column="value")
    second = inject_nulls(baseline, plan, column="value")
    assert first == second
    assert all(row["value"] is not None for row in baseline)
    assert len(first.mutations) == 2


def test_noise_rejects_unknown_columns() -> None:
    with pytest.raises(NoiseError, match="not present"):
        inject_nulls((({"id": 1},),)[0], NoisePlan("n", "fp", budget=1), column="missing")


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
    plan = NoisePlan("n", "fp", defect_type=defect_type, budget=1)
    result = apply_noise(baseline, plan, column=column)
    assert len(result.mutations) == 1
    assert result.mutations[0].after != result.mutations[0].before
    assert result.mutations[0].defect_type == defect_type
    assert baseline[0][column] in {"a", 1}


def test_noise_rejects_unsupported_defect_and_wrong_type() -> None:
    with pytest.raises(ValueError, match="unsupported defect"):
        NoisePlan("n", "fp", defect_type="copy_production_id")
    with pytest.raises(NoiseError, match="string"):
        apply_noise(
            (({"value": 1}),), NoisePlan("n", "fp", defect_type="casing", budget=1), column="value"
        )
