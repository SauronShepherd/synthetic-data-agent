from __future__ import annotations

import pytest

from sda.quality import QualityError, evaluate_quality


def test_quality_report_separates_hard_gates_from_score() -> None:
    report = evaluate_quality(
        ({"id": "a", "value": 1}, {"id": "b", "value": 2}),
        expected_count=2,
        required_columns=("id", "value"),
        unique_key="id",
        intended_use="qa",
    )
    assert report.technical_disposition == "PASS"
    assert report.soft_score == 1.0


def test_quality_report_fails_duplicate_key() -> None:
    report = evaluate_quality(
        ({"id": "a"}, {"id": "a"}),
        expected_count=2,
        required_columns=("id",),
        unique_key="id",
        intended_use="qa",
    )
    assert report.technical_disposition == "FAIL"


def test_quality_rejects_invalid_expectation() -> None:
    with pytest.raises(QualityError, match="expected_count"):
        evaluate_quality(
            (), expected_count=-1, required_columns=(), unique_key="id", intended_use="qa"
        )
