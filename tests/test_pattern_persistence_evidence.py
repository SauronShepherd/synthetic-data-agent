import json

from sda.patterns.models import Pattern, PatternFamily, PatternOrigin
from sda.patterns.persistence import registry_rows


def test_pattern_registry_rows_preserve_population_baseline_and_stability():
    pattern = Pattern(
        pattern_id="p1",
        analysis_id="a1",
        family=PatternFamily.CORRELATION,
        origin=PatternOrigin.OBSERVED,
        primary_table="main.s.t",
        columns=("x", "y"),
        condition={},
        outcome={},
        support_rows=10,
        support_rate=0.25,
        metric={"value": 1.0},
        evidence_quality={
            "baseline": {"mean": 4},
            "stability": {"quality": "stable"},
        },
    )
    row = registry_rows((pattern,))[0]
    assert json.loads(row["population_json"]) == {"support_rate": 0.25, "support_rows": 10}
    assert json.loads(row["baseline_json"]) == {"mean": 4}
    assert json.loads(row["stability_json"]) == {"quality": "stable"}


def test_pattern_registry_rows_preserve_complete_evidence_contract():
    pattern = Pattern(
        pattern_id="p2",
        analysis_id="a1",
        family=PatternFamily.BUSINESS_RULE,
        origin=PatternOrigin.OBSERVED,
        primary_table="main.s.t",
        columns=("x",),
        condition={},
        outcome={},
        support_rows=10,
        support_rate=0.5,
        metric={"method": "exact"},
        evidence_quality={
            "confidence": 0.9,
            "stability": {"quality": "stable"},
            "violation_count": 2,
            "violation_rate": 0.2,
            "sampling": {"fraction": 0.5, "seed": 7},
            "limitations": ("bounded",),
        },
    )
    row = registry_rows((pattern,))[0]
    assert row["confidence"] == 0.9
    assert row["violation_count"] == 2
    assert row["violation_rate"] == 0.2
    assert json.loads(row["sampling_json"]) == {"fraction": 0.5, "seed": 7}
    assert json.loads(row["limitations_json"]) == ["bounded"]
