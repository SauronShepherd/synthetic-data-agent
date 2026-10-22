from datetime import date

import pytest

from sda.patterns.detector import PatternDetector
from sda.patterns.models import PatternConfig, PatternLifecycle, PatternOrigin
from sda.patterns.rules import BusinessRule
from sda.patterns.spark_rules import (
    active_rules,
    detect_spark_rules,
    evaluate_spark_rule,
    parse_rules_json,
)


def test_rule_json_and_effective_scope():
    rules = parse_rules_json(
        '[{"rule_id":"a","table":"main.s.t","predicates":[{"column":"x","value":1}]}]'
    )
    assert len(active_rules(rules, "main.s.t", as_of=date(2026, 10, 22))) == 1
    assert active_rules(rules, "main.s.other", as_of=date(2026, 10, 22)) == ()
    future = BusinessRule(
        "future", "main.s.t", ({"column": "x", "value": 1},), effective_from="2999-01-01"
    )
    assert active_rules((future,), "main.s.t", as_of=date(2026, 10, 22)) == ()
    with pytest.raises(ValueError, match="inverted"):
        active_rules(
            (
                BusinessRule(
                    "bad",
                    "main.s.t",
                    ({"column": "x"},),
                    effective_from="2026-10-23",
                    effective_to="2026-10-22",
                ),
            ),
            "main.s.t",
            as_of=date(2026, 10, 22),
        )


def test_approved_rules_require_auditable_approval_metadata():
    with pytest.raises(ValueError, match="require owner_id, reviewer_id"):
        BusinessRule(
            "unverifiable",
            "main.s.t",
            ({"column": "status", "value": "A"},),
            origin=PatternOrigin.DOMAIN_APPROVED,
            approved=True,
        )
    rules = parse_rules_json(
        '[{"rule_id":"approved","table":"main.s.t","predicates":[{"column":"x"}],'
        '"approved":true,"owner_id":"owner","reviewer_id":"reviewer",'
        '"approval_reference":"ticket-1","approved_at":"2026-10-01T00:00:00Z",'
        '"rule_version":"2"}]'
    )
    assert rules[0].owner_id == "owner"
    assert rules[0].reviewer_id == "reviewer"
    assert rules[0].approved_at == "2026-10-01T00:00:00Z"
    assert rules[0].rule_version == "2"


def test_approved_rules_require_independent_reviewer_identity() -> None:
    with pytest.raises(ValueError, match="distinct owner and reviewer"):
        BusinessRule(
            "same-actor",
            "main.s.t",
            ({"column": "x"},),
            approved=True,
            owner_id="same-actor",
            reviewer_id="same-actor",
            approval_reference="ticket-1",
            approved_at="2026-10-01T00:00:00Z",
        )


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[1]").appName("sda-spark-rules-test").getOrCreate()
    yield session
    session.stop()


@pytest.mark.spark
def test_spark_rule_metrics_and_safe_compiler(spark):
    frame = spark.createDataFrame([("A", 1), ("A", -1), ("B", 1)], ["status", "amount"])
    rule = BusinessRule(
        "positive",
        "main.s.t",
        (
            {"column": "status", "operator": "eq", "value": "A"},
            {"column": "amount", "operator": "gt", "value": 0, "role": "requirement"},
        ),
    )
    result = evaluate_spark_rule(frame, rule)
    assert (result.population_rows, result.satisfying_rows, result.violation_rows) == (2, 1, 1)
    assert result.condition_support_rate == pytest.approx(2 / 3)
    with pytest.raises(ValueError, match="unavailable column"):
        evaluate_spark_rule(
            frame, BusinessRule("unsafe", "main.s.t", ({"column": "status) OR 1=1", "value": "A"},))
        )
    with pytest.raises(ValueError, match="unsupported rule operator"):
        evaluate_spark_rule(
            frame,
            BusinessRule(
                "unsafe", "main.s.t", ({"column": "status", "operator": "sql", "value": "A"},)
            ),
        )


@pytest.mark.spark
def test_conflict_precedence_remains_review_only(spark):
    frame = spark.createDataFrame([("A",), ("B",)], ["status"])
    rules = (
        BusinessRule(
            "user", "main.s.t", ({"column": "status", "value": "A", "role": "requirement"},)
        ),
        BusinessRule(
            "approved",
            "main.s.t",
            ({"column": "status", "value": "B", "role": "requirement"},),
            origin=PatternOrigin.DOMAIN_APPROVED,
            approved=True,
            owner_id="domain-owner",
            reviewer_id="domain-reviewer",
            approval_reference="approval-2026-01",
            approved_at="2026-10-01T12:00:00Z",
        ),
    )
    detector = PatternDetector(PatternConfig(min_support_rows=1))
    patterns = detect_spark_rules(
        frame, rules, table="main.s.t", as_of=date.today(), detector=detector, analysis_id="test"
    )
    assert len(patterns) == 2
    assert all(
        p.lifecycle is PatternLifecycle.REVIEW_REQUIRED and p.decision == "review_required"
        for p in patterns
    )
    assert all(p.metric["conflicts"][0]["winning_rule_id"] == "approved" for p in patterns)
