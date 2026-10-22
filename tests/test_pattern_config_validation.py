import pytest

from sda.patterns.models import PatternConfig


@pytest.mark.parametrize(
    "field,value",
    [
        ("min_support_rate", -0.1),
        ("min_support_rate", 1.1),
        ("max_category_values", 0),
        ("max_condition_depth", -1),
        ("max_segment_cardinality", 0),
        ("detector_version", " "),
        ("scoring_version", " "),
        ("stability_min_slices", 1),
        ("stability_max_slices", 1001),
        ("stability_max_spread", 1.1),
    ],
)
def test_pattern_config_rejects_invalid_bounds(field, value):
    with pytest.raises(ValueError):
        PatternConfig(**{field: value})


def test_pattern_config_rejects_unknown_sensitive_value_policy() -> None:
    with pytest.raises(ValueError):
        PatternConfig(sensitive_value_policy="raw_values")


def test_pattern_config_rejects_duplicate_or_unbounded_stability_dimensions() -> None:
    with pytest.raises(ValueError, match="stability slice columns"):
        PatternConfig(stability_slice_columns=("month", "month"))
    with pytest.raises(ValueError, match="stability slice columns"):
        PatternConfig(stability_slice_columns=tuple(f"slice_{i}" for i in range(11)))
