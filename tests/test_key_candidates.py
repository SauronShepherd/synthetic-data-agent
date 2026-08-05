from sda.relationships.candidates import discover_key_candidates


def test_composite_key_candidate_is_minimal_and_unique() -> None:
    result = discover_key_candidates(
        "orders",
        [
            {"order_id": "A", "line_number": 1},
            {"order_id": "A", "line_number": 2},
            {"order_id": "B", "line_number": 1},
        ],
        ["order_id", "line_number"],
        max_width=2,
    )
    composite = next(item for item in result if item.columns == ("order_id", "line_number"))
    assert composite.uniqueness_ratio == 1.0
    assert composite.minimal is True
