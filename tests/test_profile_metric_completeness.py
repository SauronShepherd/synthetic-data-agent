from datetime import UTC, datetime, timedelta

from sda.profiling.numeric import numeric_histogram
from sda.profiling.strings import string_metrics
from sda.profiling.temporal import temporal_metrics
from sda.relationships.metrics import measure_join


def test_numeric_histogram_is_bounded_and_deterministic():
    result = numeric_histogram([0, 1, 2, 3, 4], bins=2)
    assert [item["count"] for item in result["bins"]] == [2, 3]
    assert result["min"] == 0 and result["max"] == 4


def test_string_metrics_exposes_null_and_punctuation_signals():
    result = string_metrics([None, "", "  ", "abc-123", "!!!"])
    assert result["null_count"] == 1
    assert result["punctuation_only_count"] == 1
    assert result["contains_punctuation_count"] == 2


def test_temporal_metrics_reports_distribution_of_ordered_gaps():
    start = datetime(2024, 1, 1, tzinfo=UTC)
    result = temporal_metrics(
        [start, start + timedelta(hours=1), start + timedelta(hours=3)], start
    )
    assert result["gap_seconds"]["count"] == 2
    assert result["gap_seconds"]["p50"] == 3600


def test_relationship_metrics_exposes_both_cardinality_directions():
    result = measure_join(
        [{"id": 1}, {"id": 2}],
        [{"parent_id": 1}, {"parent_id": 1}],
        ("id",),
        ("parent_id",),
    )
    assert result.parent_to_child_cardinality == "one_to_many"
    assert result.child_to_parent_cardinality == "many_to_one"
