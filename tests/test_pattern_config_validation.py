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
    ],
)
def test_pattern_config_rejects_invalid_bounds(field, value):
    with pytest.raises(ValueError):
        PatternConfig(**{field: value})
