from sda.patterns.models import Pattern, PatternFamily, PatternOrigin
from sda.patterns.persistence import registry_rows


def test_pattern_registry_rows_preserve_configured_versions():
    pattern = Pattern(
        pattern_id="p1",
        analysis_id="a1",
        family=PatternFamily.CORRELATION,
        origin=PatternOrigin.OBSERVED,
        primary_table="main.s.t",
        columns=("x", "y"),
        condition={},
        outcome={},
        support_rows=1,
        support_rate=1.0,
        metric={},
        evidence_quality={},
    )
    row = registry_rows(
        (pattern,),
        detector_version="det-v2",
        scoring_policy_version="score-v3",
        precedence_policy_version="precedence-v4",
    )[0]
    assert row["detector_version"] == "det-v2"
    assert row["scoring_policy_version"] == "score-v3"
    assert row["rule_precedence_policy_version"] == "precedence-v4"
