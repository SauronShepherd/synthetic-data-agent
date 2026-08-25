from sda.relationships.metrics import measure_join


def test_fanout_includes_zero_child_parents() -> None:
    metrics = measure_join(
        [{"id": 1}, {"id": 2}, {"id": 3}],
        [{"parent_id": 1}, {"parent_id": 1}, {"parent_id": 2}],
        ("id",),
        ("parent_id",),
    )

    assert metrics.children_per_parent["parents_with_no_children"] == 1
    assert metrics.children_per_parent["parent_count"] == 3
    assert metrics.children_per_parent["mean"] == 1
    assert metrics.children_per_parent["zero_child_parent_rate"] == 1 / 3
