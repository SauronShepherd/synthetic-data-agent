from __future__ import annotations

import pytest

from sda.noise import NoiseError, NoisePlan, inject_nulls


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
        inject_nulls((( {"id": 1}, ),)[0], NoisePlan("n", "fp", budget=1), column="missing")
