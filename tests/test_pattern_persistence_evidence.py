import json

from sda.patterns.models import Pattern, PatternFamily, PatternLifecycle, PatternOrigin
from sda.patterns.persistence import registry_rows
from sda.patterns.rules import BusinessRule


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
    assert row["lifecycle"] == "observed_pattern"
    assert row["association_name"] == "pearson"
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
    limitations = json.loads(row["limitations_json"])
    assert len(limitations) == 1
    assert set(limitations[0]) == {"fingerprint"}


def test_registry_redacts_strings_in_nested_evidence_and_actions():
    pattern = Pattern(
        pattern_id="p3",
        analysis_id="a1",
        family=PatternFamily.CORRELATION,
        origin=PatternOrigin.OBSERVED,
        primary_table="main.s.t",
        columns=("x", "y"),
        condition={},
        outcome={},
        support_rows=1,
        support_rate=1.0,
        metric={"value": 1.0},
        evidence_quality={
            "baseline": {"label": "secret-baseline"},
            "stability": {"label": "secret-stability"},
        },
        generation_action={"label": "secret-generation"},
        validation_action={"label": "secret-validation"},
    )
    row = registry_rows((pattern,))[0]
    for value in ("secret-baseline", "secret-stability", "secret-generation", "secret-validation"):
        assert value not in str(row)
    for field in (
        "baseline_json",
        "stability_json",
        "generation_action_json",
        "validation_action_json",
    ):
        assert set(json.loads(row[field])["label"]) == {"fingerprint"}


def test_registry_exposes_unavailable_stability_explicitly() -> None:
    pattern = Pattern(
        pattern_id="p4",
        analysis_id="a1",
        family=PatternFamily.CONDITIONAL_DISTRIBUTION,
        origin=PatternOrigin.OBSERVED,
        primary_table="main.s.t",
        columns=("x", "y"),
        condition={},
        outcome={},
        support_rows=10,
        support_rate=1.0,
        metric={"value": 0.5},
        evidence_quality={"stability": "unavailable", "baseline": {"global_rate": 0.3}},
    )
    row = registry_rows((pattern,))[0]
    assert json.loads(row["stability_json"]) == {"status": "unavailable"}
    assert json.loads(row["baseline_json"]) == {"global_rate": 0.3}


def test_registry_preserves_stability_dimensions_without_slice_values() -> None:
    pattern = Pattern(
        pattern_id="p5",
        analysis_id="a1",
        family=PatternFamily.CONDITIONAL_DISTRIBUTION,
        origin=PatternOrigin.OBSERVED,
        primary_table="main.s.t",
        columns=("segment", "status"),
        condition={"segment": "private-segment-value"},
        outcome={"status": "private-outcome-value"},
        support_rows=60,
        support_rate=0.5,
        metric={"value": 0.5},
        evidence_quality={
            "stability": {
                "event_time:month": {
                    "dimension": "event_time:month",
                    "stable": False,
                    "max_abs_delta_from_global": 0.3,
                }
            }
        },
    )
    row = registry_rows((pattern,))[0]
    stability_json = json.loads(row["stability_json"])
    assert stability_json["event_time:month"]["dimension"] == "event_time:month"
    assert stability_json["event_time:month"]["stable"] is False
    assert "private-segment-value" not in str(row)
    assert "private-outcome-value" not in str(row)


def test_registry_preserves_temporal_clock_and_ordering_provenance() -> None:
    pattern = Pattern(
        pattern_id="p-clock",
        analysis_id="a1",
        family=PatternFamily.STATE_TRANSITION,
        origin=PatternOrigin.OBSERVED,
        primary_table="main.s.events",
        columns=("state",),
        condition={"from_state": "OPEN"},
        outcome={"to_state": "CLOSED"},
        support_rows=10,
        support_rate=0.5,
        metric={},
        evidence_quality={
            "temporal_clock": {
                "event_time_column": "event_at",
                "ingestion_time_column": "ingested_at",
                "tie_breaker_columns": ["event_id"],
                "ingestion_order_disagreement_count": 2,
            }
        },
    )
    row = registry_rows((pattern,))[0]
    assert json.loads(row["temporal_clock_json"]) == {
        "event_time_column": "event_at",
        "ingestion_time_column": "ingested_at",
        "tie_breaker_columns": ["event_id"],
        "ingestion_order_disagreement_count": 2,
    }


def test_registry_preserves_rule_owner_approval_and_effective_period() -> None:
    rule = BusinessRule(
        "cancelled_timestamp_required",
        "main.sales.orders",
        ({"column": "cancel_at", "operator": "is_not_null", "role": "requirement"},),
        origin=PatternOrigin.DOMAIN_APPROVED,
        approved=True,
        effective_from="2026-01-01",
        effective_to="2026-12-31",
        rule_version="3",
        owner_id="domain-owner",
        reviewer_id="domain-reviewer",
        approval_reference="approval-2026-01",
        approved_at="2026-10-01T12:00:00Z",
        supersedes_rule_id="cancelled_timestamp_required_v2",
    )
    pattern = Pattern(
        pattern_id="p5",
        analysis_id="a1",
        family=PatternFamily.BUSINESS_RULE,
        origin=PatternOrigin.DOMAIN_APPROVED,
        primary_table="main.sales.orders",
        columns=("cancel_at",),
        condition={},
        outcome={},
        support_rows=10,
        support_rate=1.0,
        metric={"rule": rule},
        evidence_quality={},
        decision="review_required",
        review_status="required",
        lifecycle=PatternLifecycle.REVIEW_REQUIRED,
    )
    provenance = json.loads(registry_rows((pattern,))[0]["rule_provenance_json"])
    assert provenance == {
        "approval_reference": "approval-2026-01",
        "approval_status": "approved",
        "approved": True,
        "approved_at": "2026-10-01T12:00:00Z",
        "effective_from": "2026-01-01",
        "effective_to": "2026-12-31",
        "origin": "domain_approved",
        "owner_id": "domain-owner",
        "reviewer_id": "domain-reviewer",
        "rule_id": "cancelled_timestamp_required",
        "rule_version": "3",
        "review_status": "required",
        "strength": "PROBABILISTIC",
        "supersedes_rule_id": "cancelled_timestamp_required_v2",
        "table": "main.sales.orders",
    }
