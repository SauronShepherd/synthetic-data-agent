from typing import Any

import pytest

from sda.relationships.detector import RelationshipDiscoveryConfig


@pytest.mark.parametrize(  # type: ignore[untyped-decorator]
    "kwargs",
    [
        {"max_composite_key_width": 0},
        {"max_candidates_per_table": 0},
        {"validation_mode": "unknown"},
    ],
)
def test_relationship_config_rejects_unsafe_values(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        RelationshipDiscoveryConfig(**kwargs)
