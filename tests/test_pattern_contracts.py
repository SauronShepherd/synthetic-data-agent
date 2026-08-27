import pytest

from sda.patterns import PatternConfig, PatternDetector, PatternInputRefs, PatternLifecycle
from sda.patterns.candidates import PatternCandidate, generate_candidates
from sda.patterns.conflicts import detect_rule_conflicts, resolve_rule_conflicts
from sda.patterns.fanout import fanout_by_segment
from sda.patterns.models import (
    Pattern,
    PatternDetectionResult,
    PatternExecutionReceipt,
    PatternFamily,
    PatternOrigin,
)
from sda.patterns.persistence import (
    PATTERN_EVIDENCE_SCHEMA_VERSION,
    PATTERN_REGISTRY_SCHEMA_VERSION,
    evidence_rows,
    registry_rows,
    require_pattern_schema_version,
)
from sda.patterns.precedence import RulePrecedencePolicy
from sda.patterns.rules import BusinessRule, RuleStrength, evaluate_rule
from sda.patterns.safety import SafeValueKind, safe_pattern_value
from sda.patterns.scoring import PatternScoringPolicy
from sda.patterns.stability import stability
from sda.patterns.temporal import ordered_events, temporal_order
from sda.patterns.transitions import state_transitions
from sda.profiling.loaders import ProfileIndex
from sda.relationships.persistence import relationship_analysis_id
from sda.workflows.detect_patterns import detect_patterns


def test_correlation_is_evidence_and_observed_is_not_auto_approved() -> None:
    rows = [{"x": i, "y": i * 2} for i in range(40)]
    result = PatternDetector(PatternConfig(min_support_rows=10)).detect(
        rows, table="main.s.t", columns={"x": "double", "y": "double"}
    )
    assert len(result) == 1
    assert result[0].origin.value == "observed"
    assert result[0].decision == "accepted_for_planning"
    assert result[0].metric["valid_pair_count"] == 40
    assert result[0].evidence_quality["population_rows"] == 40
    assert result[0].evidence_quality["sampling"]["seed"] == 1729
    assert result[0].evidence_quality["violation_count"] == 0


def test_correlation_pairs_remain_aligned_when_values_are_null() -> None:
    rows = [
        {"x": 1, "y": None},
        {"x": None, "y": 2},
        {"x": 3, "y": 6},
        {"x": 4, "y": 8},
    ]
    result = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        rows, table="main.s.t", columns={"x": "double", "y": "double"}
    )
    assert len(result) == 1
    assert result[0].metric["valid_pair_count"] == 2
    assert result[0].metric["value"] == pytest.approx(1.0)


def test_correlation_infers_numeric_columns_when_types_are_omitted() -> None:
    rows = [{"x": index, "y": index * 2} for index in range(3)]
    result = PatternDetector(PatternConfig(min_support_rows=2)).detect(rows, table="main.s.t")
    assert len(result) == 1
    assert result[0].metric["valid_pair_count"] == 3


def test_standalone_detection_emits_conditional_categorical_patterns() -> None:
    rows = [
        {"segment": "A", "amount": 10},
        {"segment": "A", "amount": 20},
        {"segment": "A", "amount": 10},
        {"segment": "B", "amount": 1},
    ]
    result = PatternDetector(PatternConfig(min_support_rows=2)).detect(rows, table="main.s.t")
    assert any(
        pattern.family is PatternFamily.CONDITIONAL_DISTRIBUTION
        and pattern.condition == {"segment": "A"}
        for pattern in result
    )


def test_standalone_detection_emits_conditional_missingness_patterns() -> None:
    result = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [
            {"segment": "a", "value": None},
            {"segment": "a", "value": None},
            {"segment": "b", "value": 1},
        ],
        table="main.s.t",
    )
    missing = [p for p in result if p.family is PatternFamily.CONDITIONAL_MISSINGNESS]
    assert len(missing) == 1
    assert missing[0].metric["missing_rate"] == 1.0


def test_standalone_missingness_handles_nested_segment_values() -> None:
    patterns = PatternDetector(PatternConfig(min_support_rows=1)).detect(
        [
            {"segment": {"tier": "a"}, "value": None},
            {"segment": {"tier": "b"}, "value": 1},
        ],
        table="main.s.t",
    )
    assert any(p.family is PatternFamily.CONDITIONAL_MISSINGNESS for p in patterns)


def test_standalone_detection_emits_state_transition_patterns() -> None:
    patterns = PatternDetector(PatternConfig(min_support_rows=1)).detect(
        [
            {"id": "a", "status": "new", "event_at": 1},
            {"id": "a", "status": "active", "event_at": 2},
        ],
        table="main.events",
    )
    transitions = [p for p in patterns if p.family is PatternFamily.STATE_TRANSITION]
    assert len(transitions) == 1
    assert transitions[0].metric["transitions"][0]["transition_count"] == 1


def test_state_transition_evidence_preserves_event_vs_ingestion_order() -> None:
    patterns = PatternDetector(PatternConfig(min_support_rows=1)).detect(
        [
            {"id": "a", "status": "new", "event_at": 1, "ingested_at": 2},
            {"id": "a", "status": "active", "event_at": 2, "ingested_at": 1},
        ],
        table="main.events",
    )
    transition = next(
        pattern for pattern in patterns if pattern.family is PatternFamily.STATE_TRANSITION
    )
    assert transition.metric["event_order"]["late_arrival_entity_count"] == 1
    assert (
        "ingestion_order_differs_from_event_order" in transition.metric["event_order"]["warnings"]
    )


def test_coordinator_requires_all_upstream_artifacts_and_reports_receipt() -> None:
    refs = PatternInputRefs("meta", ("profile",), "rel", "graph")
    result = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [{"x": 1, "y": 2}, {"x": 2, "y": 4}],
        table="main.s.t",
        columns={"x": "double", "y": "double"},
        input_refs=refs,
        run_id="run",
        environment="dev",
        selected_tables=("main.s.t",),
    )
    assert result.receipt.patterns_emitted == 1
    assert result.receipt.source_tables_scanned == 1
    assert result.receipt.patterns_rejected == 0
    assert result.receipt.patterns_insufficient == 0
    assert result.receipt.candidate_count_by_family == {
        "correlation": 1,
        "conditional_distribution": 2,
        "conditional_missingness": 2,
    }
    assert (
        sum(result.receipt.candidate_count_by_family.values())
        == result.receipt.candidate_count_total
    )
    assert (
        result.receipt.patterns_accepted_for_planning
        + result.receipt.patterns_review_required
        + result.receipt.patterns_rejected
        + result.receipt.patterns_insufficient
        == result.receipt.patterns_emitted
    )


def test_coordinator_fails_closed_when_input_rows_are_unavailable() -> None:
    refs = PatternInputRefs("meta", ("profile",), "rel", "graph")
    with pytest.raises(ValueError, match="requires available input rows"):
        PatternDetector().detect(
            [],
            input_refs=refs,
            run_id="run-empty",
            environment="dev",
            selected_tables=("main.s.t",),
        )


def test_pattern_input_refs_reject_whitespace_artifact_ids() -> None:
    with pytest.raises(ValueError, match="upstream pattern artifacts"):
        PatternInputRefs(" ", ("profile",), "relationship", "graph")
    with pytest.raises(ValueError, match="upstream pattern artifacts"):
        PatternInputRefs("metadata", ("profile", " "), "relationship", "graph")


def test_pattern_input_refs_reject_duplicate_profile_artifacts() -> None:
    with pytest.raises(ValueError, match="upstream pattern artifacts"):
        PatternInputRefs("metadata", ("profile", "profile"), "relationship", "graph")


def test_pattern_input_refs_reject_duplicate_artifacts_across_roles() -> None:
    with pytest.raises(ValueError, match="upstream pattern artifacts"):
        PatternInputRefs("metadata", ("profile",), "profile", "graph")


def test_column_role_assignment_validates_identity_and_confidence() -> None:
    from sda.patterns.models import ColumnRoleAssignment

    assert ColumnRoleAssignment("main.t", "amount", "outcome", 0.5).confidence == 0.5
    with pytest.raises(ValueError, match="between zero and one"):
        ColumnRoleAssignment("main.t", "amount", "outcome", 1.1)


def test_pattern_config_rejects_negative_sampling_seeds() -> None:
    with pytest.raises(ValueError, match="invalid pattern configuration"):
        PatternConfig(sample_seed=-1)


def test_pattern_input_and_output_sequences_are_frozen() -> None:
    profiles = ["profile"]
    refs = PatternInputRefs("metadata", profiles, "relationship", "graph")
    profiles.append("changed")
    assert refs.profile_artifact_ids == ("profile",)
    pattern = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [{"x": 1, "y": 2}, {"x": 2, "y": 4}], table="main.s.t", columns={"x": "int", "y": "int"}
    )[0]
    assert isinstance(pattern.columns, tuple)


def test_coordinator_emits_conditional_missingness_for_each_segment() -> None:
    refs = PatternInputRefs("meta", ("profile",), "rel", "graph")
    result = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [
            {"segment": "A", "value": None},
            {"segment": "A", "value": None},
            {"segment": "B", "value": 1},
            {"segment": "B", "value": 2},
        ],
        table="main.s.t",
        input_refs=refs,
        run_id="run-segments",
        environment="dev",
        selected_tables=("main.s.t",),
    )
    findings = [
        pattern
        for pattern in result.patterns
        if pattern.family.value == "conditional_missingness"
        and pattern.outcome.get("outcome") == "value"
    ]
    assert {pattern.condition["segment"] for pattern in findings} == {"A", "B"}


def test_coordinator_emits_conditional_distributions_for_each_pair() -> None:
    refs = PatternInputRefs("meta", ("profile",), "rel", "graph")
    result = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [
            {"segment_a": "A", "segment_b": "X", "amount_a": 1, "amount_b": 10},
            {"segment_a": "A", "segment_b": "X", "amount_a": 1, "amount_b": 10},
            {"segment_a": "B", "segment_b": "Y", "amount_a": 3, "amount_b": 30},
            {"segment_a": "B", "segment_b": "Y", "amount_a": 3, "amount_b": 30},
        ],
        table="main.s.t",
        input_refs=refs,
        run_id="run-distributions",
        environment="dev",
        selected_tables=("main.s.t",),
    )
    findings = [
        pattern for pattern in result.patterns if pattern.family.value == "conditional_distribution"
    ]
    assert {(pattern.columns[0], pattern.columns[1]) for pattern in findings} == {
        ("segment_a", "amount_a"),
        ("segment_a", "amount_b"),
        ("segment_b", "amount_a"),
        ("segment_b", "amount_b"),
    }


def test_coordinator_multi_family_acceptance_fixture() -> None:
    rows = []
    for entity in range(60):
        segment = "premium" if entity % 2 == 0 else "standard"
        for event, status in ((1, "new"), (2, "active")):
            rows.append(
                {
                    "customer_id": f"c{entity}",
                    "segment": segment,
                    "status": status,
                    "amount": entity + event,
                    "amount_2": (entity + event) * 2,
                    "start_at": event,
                    "end_at": event + 1,
                    "cancel_at": None if segment == "premium" else event,
                }
            )
    rule_a = BusinessRule(
        "observed-rule",
        "main.orders",
        ({"column": "status", "value": "active", "role": "outcome"},),
        origin=PatternOrigin.OBSERVED,
    )
    rule_b = BusinessRule(
        "user-rule",
        "main.orders",
        ({"column": "status", "value": "cancelled", "role": "outcome"},),
        origin=PatternOrigin.USER_PROVIDED,
    )
    result = PatternDetector(PatternConfig(min_support_rows=10, max_candidates=500)).detect(
        rows,
        table="main.orders",
        input_refs=PatternInputRefs("meta", ("profile",), "rel", "graph"),
        run_id="multi-family",
        environment="dev",
        selected_tables=("main.orders",),
        rules=(rule_a, rule_b),
        fanout_inputs=(
            {
                "parents": [
                    {"customer_id": f"c{i}", "segment": "premium" if i % 2 == 0 else "standard"}
                    for i in range(60)
                ],
                "children": [{"customer_id": f"c{i}"} for i in range(0, 60, 2)],
                "parent_key": "customer_id",
                "child_key": "customer_id",
                "segment": "segment",
            },
        ),
    )
    families = {pattern.family for pattern in result.patterns}
    assert {
        PatternFamily.CORRELATION,
        PatternFamily.CONDITIONAL_DISTRIBUTION,
        PatternFamily.CONDITIONAL_MISSINGNESS,
        PatternFamily.FANOUT_BY_SEGMENT,
        PatternFamily.TEMPORAL_ORDER,
        PatternFamily.STATE_TRANSITION,
        PatternFamily.BUSINESS_RULE,
    } <= families
    assert result.receipt.conflicts_found == 1
    assert all(pattern.metric for pattern in result.patterns)


def test_coordinator_emits_segment_fanout_from_authorized_relationship_rows() -> None:
    refs = PatternInputRefs("meta", ("profile",), "rel", "graph")
    result = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [{"id": 1, "segment": "A"}, {"id": 2, "segment": "A"}],
        table="main.parents",
        input_refs=refs,
        run_id="run-fanout",
        environment="dev",
        selected_tables=("main.parents",),
        fanout_inputs=(
            {
                "parents": [{"id": 1, "segment": "A"}, {"id": 2, "segment": "A"}],
                "children": [{"parent_id": 1}],
                "parent_key": "id",
                "segment": "segment",
                "child_key": "parent_id",
            },
        ),
    )
    fanout = [p for p in result.patterns if p.family is PatternFamily.FANOUT_BY_SEGMENT]
    assert len(fanout) == 1
    assert fanout[0].metric["zero_child_count"] == 1


def test_coordinator_roles_exclude_entity_columns_from_driver_candidates() -> None:
    result = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [
            {"customer_id": "c1", "segment": "A", "amount": 10},
            {"customer_id": "c2", "segment": "A", "amount": 20},
            {"customer_id": "c3", "segment": "B", "amount": 30},
        ],
        table="main.orders",
        input_refs=PatternInputRefs("meta", ("profile",), "rel", "graph"),
        run_id="role-contract",
        environment="dev",
        selected_tables=("main.orders",),
    )
    assert all("customer_id" not in pattern.columns for pattern in result.patterns)


def test_candidates_are_bounded_and_deterministic() -> None:
    columns = {f"c{i}": "double" for i in range(20)}
    config = PatternConfig(max_candidates=7)
    left = generate_candidates("main.s.t", columns, config=config)
    right = generate_candidates("main.s.t", dict(reversed(list(columns.items()))), config=config)
    assert len(left) == 7
    assert left == right


def test_pattern_candidate_nested_fields_are_immutable() -> None:
    candidate = PatternCandidate(
        PatternFamily.BUSINESS_RULE,
        "main.s.t",
        ["driver"],
        ["outcome"],
        {"operator": "equals", "nested": {"values": ["a"]}},
    )
    assert candidate.driver_columns == ("driver",)
    assert candidate.outcome_columns == ("outcome",)
    with pytest.raises(TypeError, match="immutable"):
        candidate.condition["operator"] = "contains"  # type: ignore[index]
    with pytest.raises(TypeError):
        candidate.condition["nested"]["values"][0] = "b"  # type: ignore[index]


def test_candidates_include_structural_pattern_families() -> None:
    candidates = generate_candidates(
        "main.events",
        {"id": "int", "status": "string", "started_at": "timestamp", "ended_at": "timestamp"},
        config=PatternConfig(max_candidates=50),
    )
    families = {candidate.family for candidate in candidates}
    assert PatternFamily.TEMPORAL_ORDER in families
    assert PatternFamily.STATE_TRANSITION in families
    assert PatternFamily.CONDITIONAL_MISSINGNESS in families


def test_detector_is_independent_of_input_row_order() -> None:
    detector = PatternDetector(PatternConfig(min_support_rows=1, max_candidates=50))
    rows = [
        {"segment": "b", "x": 2, "started_at": 2, "finished_at": 4},
        {"segment": "a", "x": 1, "started_at": 1, "finished_at": 3},
        {"segment": "b", "x": 4, "started_at": 4, "finished_at": 8},
    ]
    forward = detector.detect(rows, table="main.s.events")
    reverse = detector.detect(list(reversed(rows)), table="main.s.events")
    assert forward == reverse


def test_pattern_detector_rejects_inputs_over_scan_budget() -> None:
    detector = PatternDetector(PatternConfig(min_support_rows=1, max_rows_scanned=1))
    with pytest.raises(ValueError, match="max_rows_scanned"):
        detector.detect([{"x": 1}, {"x": 2}], table="main.s.t", columns={"x": "int"})


def test_detector_emits_temporal_lag_metrics() -> None:
    detector = PatternDetector(PatternConfig(min_support_rows=2))
    patterns = detector.detect(
        [
            {"id": 1, "a_started_at": 1, "b_finished_at": 3},
            {"id": 2, "a_started_at": 2, "b_finished_at": 6},
        ],
        table="main.s.events",
        columns={"id": "int", "a_started_at": "int", "b_finished_at": "int"},
    )
    temporal = [pattern for pattern in patterns if pattern.family.value == "temporal_order"]
    assert len(temporal) == 1
    assert temporal[0].metric["count"] == 2
    assert temporal[0].metric["p50"] == 3.0
    assert temporal[0].support_rate == 1.0


def test_coordinated_detector_persists_support_rates() -> None:
    refs = PatternInputRefs("meta", ("profile",), "rel", "graph")
    result = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [{"x": 1, "y": 2}, {"x": 2, "y": 4}],
        table="main.s.t",
        columns={"x": "int", "y": "int"},
        input_refs=refs,
        run_id="run",
        environment="dev",
        selected_tables=("main.s.t",),
    )
    assert result.patterns
    assert all(pattern.support_rate is not None for pattern in result.patterns)


def test_pattern_nested_evidence_is_immutable() -> None:
    pattern = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [{"x": 1, "y": 2}, {"x": 2, "y": 4}],
        table="main.s.t",
        columns={"x": "int", "y": "int"},
    )[0]
    with pytest.raises(TypeError, match="immutable"):
        pattern.metric["value"] = 99


def test_pattern_receipt_counts_are_immutable() -> None:
    result = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [{"x": 1, "y": 2}, {"x": 2, "y": 4}],
        table="main.s.t",
        columns={"x": "int", "y": "int"},
    )
    # The local overload returns patterns; coordinated execution exposes receipts.
    assert result
    from sda.patterns.models import PatternExecutionReceipt

    receipt = PatternExecutionReceipt(
        candidate_count_total=1, candidate_count_by_family={"correlation": 1}
    )
    with pytest.raises(TypeError, match="immutable"):
        receipt.candidate_count_by_family["correlation"] = 2
    from sda.patterns.models import PatternExecutionReceipt

    with pytest.raises(ValueError, match="counts"):
        PatternExecutionReceipt(patterns_emitted=-1)
    with pytest.raises(ValueError, match="sample_fraction"):
        PatternExecutionReceipt(sample_fraction=0)
    with pytest.raises(ValueError, match="family counts"):
        PatternExecutionReceipt(candidate_count_total=2, candidate_count_by_family={"numeric": 1})
    with pytest.raises(ValueError, match="invalid accounting"):
        PatternExecutionReceipt(candidate_skipped_by_reason={"unsupported": -1})
    with pytest.raises(ValueError, match="reused tables"):
        PatternExecutionReceipt(source_tables_scanned=1, source_tables_reused=2)


def test_pattern_receipt_serializes_all_execution_accounting() -> None:
    receipt = PatternExecutionReceipt(
        candidate_count_total=4,
        patterns_emitted=2,
        patterns_accepted_for_planning=1,
        patterns_review_required=1,
        sample_fraction=0.5,
        sample_seed=9,
    )
    assert receipt.to_dict() == {
        "candidate_count_total": 4,
        "candidate_count_by_family": {},
        "candidate_skipped_by_reason": {},
        "patterns_emitted": 2,
        "patterns_accepted_for_planning": 1,
        "patterns_review_required": 1,
        "patterns_rejected": 0,
        "patterns_insufficient": 0,
        "rules_evaluated": 0,
        "conflicts_found": 0,
        "source_tables_scanned": 0,
        "source_tables_reused": 0,
        "sample_fraction": 0.5,
        "sample_seed": 9,
        "schema_version": "pattern-execution-receipt-v1",
    }


def test_pattern_detection_review_questions_are_immutable_and_serializable() -> None:
    result = PatternDetectionResult(
        (), None, PatternExecutionReceipt(), review_questions=({"reason": {"code": "review"}},)
    )
    with pytest.raises(TypeError, match="immutable"):
        result.review_questions[0]["reason"]["code"] = "changed"  # type: ignore[index]
    assert result.to_dict()["review_questions"] == result.review_questions
    assert result.to_dict()["schema_version"] == "pattern-detection-result-v1"


def test_pattern_detection_serialization_redacts_raw_pattern_values() -> None:
    pattern = Pattern(
        "pattern-secret",
        "analysis-1",
        PatternFamily.BUSINESS_RULE,
        PatternOrigin.OBSERVED,
        "main.s.t",
        ("status",),
        {"status": "secret-status"},
        {"result": "secret-result"},
        2,
        1.0,
        {"label": "secret-metric"},
        {"validation_mode": "exact"},
    )
    result = PatternDetectionResult((pattern,), None, PatternExecutionReceipt())
    payload = result.to_dict()
    assert "secret-status" not in str(payload)
    assert "secret-result" not in str(payload)
    assert "secret-metric" not in str(payload)


def test_observed_patterns_cannot_be_promoted_to_hard_constraints() -> None:
    with pytest.raises(ValueError, match="hard constraints"):
        Pattern(
            "p",
            "a",
            PatternFamily.CORRELATION,
            PatternOrigin.OBSERVED,
            "t",
            ("x", "y"),
            {},
            {},
            2,
            1.0,
            {},
            {},
            rule_strength="hard_constraint",
        )


def test_pattern_lifecycle_is_explicit_and_origin_compatible() -> None:
    pattern = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [{"x": 1, "y": 2}, {"x": 2, "y": 4}], table="main.s.t", columns={"x": "int", "y": "int"}
    )[0]
    assert pattern.lifecycle is PatternLifecycle.OBSERVED_PATTERN
    rejected = PatternDetector(
        PatternConfig(min_support_rows=2),
        scoring_policy=PatternScoringPolicy(min_support_rows=2, min_correlation_abs=2.0),
    ).detect(
        [{"x": 1, "y": 1}, {"x": 2, "y": 2}],
        table="main.s.t",
        columns={"x": "int", "y": "int"},
    )
    assert rejected[0].lifecycle is PatternLifecycle.REJECTED
    with pytest.raises(ValueError, match="rejected lifecycle"):
        Pattern(
            "p2",
            "a",
            PatternFamily.CORRELATION,
            PatternOrigin.OBSERVED,
            "t",
            ("x",),
            {},
            {},
            2,
            1.0,
            {},
            {},
            lifecycle=PatternLifecycle.REJECTED,
        )
    insufficient = PatternDetector(
        PatternConfig(min_support_rows=2),
        scoring_policy=PatternScoringPolicy(min_support_rows=3),
    ).detect(
        [{"x": 1, "y": 2}, {"x": 2, "y": 4}],
        table="main.s.t",
        columns={"x": "int", "y": "int"},
    )
    assert insufficient[0].lifecycle is PatternLifecycle.INSUFFICIENT_EVIDENCE
    with pytest.raises(ValueError, match="declared_rule lifecycle"):
        Pattern(
            "p",
            "a",
            PatternFamily.BUSINESS_RULE,
            PatternOrigin.OBSERVED,
            "t",
            ("x",),
            {},
            {},
            2,
            1.0,
            {},
            {},
            lifecycle=PatternLifecycle.DECLARED_RULE,
        )


def test_pattern_detection_serializes_artifact_references() -> None:
    class Reference:
        def to_dict(self) -> dict[str, str]:
            return {"artifact_id": "artifact-1"}

    result = PatternDetectionResult((), Reference(), PatternExecutionReceipt())
    assert result.to_dict()["artifact_ref"] == {"artifact_id": "artifact-1"}


def test_pattern_rejects_invalid_support_metrics() -> None:
    pattern = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [{"x": 1, "y": 2}, {"x": 2, "y": 4}],
        table="main.s.t",
        columns={"x": "int", "y": "int"},
    )[0]
    from dataclasses import replace

    with pytest.raises(ValueError, match="support_rate"):
        replace(pattern, support_rate=1.1, pattern_id="")


def test_fanout_includes_zero_child_parents() -> None:
    parents = [{"id": 1, "segment": "A"}, {"id": 2, "segment": "A"}]
    children = [{"parent_id": 1}]
    row = fanout_by_segment(
        parents, children, parent_key="id", segment="segment", child_key="parent_id"
    )[0]
    assert row["zero_child_count"] == 1
    assert row["zero_child_rate"] == 0.5


def test_transitions_preserve_rare_edges_and_censor_final_state() -> None:
    result = state_transitions(
        [
            {"id": 1, "state": "PENDING", "t": 1},
            {"id": 1, "state": "ACTIVE", "t": 2},
            {"id": 2, "state": "PENDING", "t": 1},
            {"id": 2, "state": "CANCELLED", "t": 2},
        ],
        entity_key="id",
        state_column="state",
        order_column="t",
    )
    assert {(r["from_state"], r["to_state"]) for r in result["transitions"]} == {
        ("PENDING", "ACTIVE"),
        ("PENDING", "CANCELLED"),
    }
    assert result["right_censored"] == {"ACTIVE": 1, "CANCELLED": 1}


def test_sensitive_pattern_values_are_not_written() -> None:
    assert (
        safe_pattern_value(value="secret", column_policy="no_values").kind is SafeValueKind.OMITTED
    )
    assert safe_pattern_value(value="secret", column_policy="redact_values").value == "<redacted>"


def test_registry_and_evidence_are_compact_and_json_safe() -> None:
    pattern = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [{"x": 1, "y": 2}, {"x": 2, "y": 4}],
        table="main.s.t",
        columns={"x": "double", "y": "double"},
    )[0]
    registry = registry_rows((pattern,))[0]
    evidence = evidence_rows((pattern,))[0]
    assert registry["pattern_id"] == pattern.pattern_id
    assert registry["schema_version"] == PATTERN_REGISTRY_SCHEMA_VERSION
    assert evidence["evidence_id"].startswith(pattern.pattern_id)
    assert evidence["schema_version"] == PATTERN_EVIDENCE_SCHEMA_VERSION
    require_pattern_schema_version(registry, expected=PATTERN_REGISTRY_SCHEMA_VERSION)
    with pytest.raises(ValueError, match="incompatible"):
        require_pattern_schema_version(registry, expected=PATTERN_EVIDENCE_SCHEMA_VERSION)


def test_pattern_persistence_redacts_non_numeric_values() -> None:
    pattern = Pattern(
        "pattern-secret",
        "analysis-1",
        PatternFamily.CONDITIONAL_DISTRIBUTION,
        PatternOrigin.OBSERVED,
        "main.s.t",
        ("segment", "value"),
        {"segment": "secret-segment"},
        {"outcome": "value"},
        2,
        1.0,
        {"conditional_rate": 1.0},
        {"validation_mode": "exact"},
    )
    registry = registry_rows((pattern,))[0]
    evidence = evidence_rows((pattern,))
    assert "secret-segment" not in str(registry)
    assert "secret-segment" not in str(evidence)


def test_rule_evaluator_separates_condition_support_from_violations() -> None:
    rule = BusinessRule(
        "r1",
        "main.s.t",
        (
            {"column": "status", "operator": "eq", "value": "CANCELLED", "role": "condition"},
            {"column": "cancelled_at", "operator": "is_not_null", "role": "requirement"},
        ),
        origin=PatternOrigin.OBSERVED,
        strength=RuleStrength.CONDITIONAL,
    )
    result = evaluate_rule(
        [
            {"status": "CANCELLED", "cancelled_at": 1},
            {"status": "CANCELLED", "cancelled_at": None},
            {"status": "OPEN", "cancelled_at": None},
        ],
        rule,
    )
    assert result.condition_support_rows == 2
    assert result.violation_rows == 1
    assert result.violation_rate == 0.5


def test_coordinator_preserves_declared_rule_origin() -> None:
    rule = BusinessRule(
        "declared-rule",
        "main.s.t",
        ({"column": "status", "operator": "eq", "value": "OPEN"},),
        origin=PatternOrigin.DECLARED,
    )
    refs = PatternInputRefs("meta", ("profile",), "rel", "graph")
    result = PatternDetector(PatternConfig(min_support_rows=2)).detect(
        [{"status": "OPEN"}, {"status": "OPEN"}],
        input_refs=refs,
        run_id="run-declared",
        environment="dev",
        selected_tables=("main.s.t",),
        rules=(rule,),
    )
    business_rules = [
        pattern for pattern in result.patterns if pattern.family is PatternFamily.BUSINESS_RULE
    ]
    assert business_rules[0].origin is PatternOrigin.DECLARED


def test_observed_hard_rule_is_rejected() -> None:
    try:
        BusinessRule(
            "r1",
            "main.s.t",
            ({"column": "x", "operator": "eq", "value": 1},),
            origin=PatternOrigin.OBSERVED,
            strength=RuleStrength.HARD,
        )
    except ValueError as error:
        assert "observed" in str(error)
    else:
        raise AssertionError("observed hard rule was accepted")


def test_temporal_tie_breaker_and_ingestion_clock_warnings_are_explicit() -> None:
    rows = [
        {"id": 1, "state": "A", "event": 1, "ingest": 2, "seq": 1},
        {"id": 1, "state": "B", "event": 2, "ingest": 1, "seq": 2},
    ]
    result = ordered_events(
        rows,
        entity_key="id",
        event_time="event",
        ingestion_time="ingest",
        state_column="state",
        tie_breakers=("seq",),
    )
    assert result["warnings"] == ("ingestion_order_differs_from_event_order",)
    assert temporal_order([{"a": 2, "b": 1}], "a", "b")["violation_rows"] == 1


def test_stability_reports_sign_flips_and_drift() -> None:
    result = stability([0.8, -0.7, 0.75])
    assert result["sign_flip_count"] == 2
    assert result["stable"] is False


def test_rule_conflict_detection_handles_unhashable_predicates() -> None:
    left = BusinessRule("left", "main.s.t", ({"column": "status", "operator": "eq", "value": "A"},))
    right = BusinessRule(
        "right", "main.s.t", ({"column": "status", "operator": "eq", "value": "B"},)
    )
    conflicts = detect_rule_conflicts([left, right])
    assert conflicts[0].conflict_type == "mutually_exclusive_equality"


def test_equal_precedence_conflicts_use_stable_rule_id_tiebreak() -> None:
    left = BusinessRule(
        "z-rule", "main.s.t", ({"column": "status", "operator": "eq", "value": "a"},)
    )
    right = BusinessRule(
        "a-rule", "main.s.t", ({"column": "status", "operator": "eq", "value": "b"},)
    )
    conflicts = resolve_rule_conflicts([left, right], RulePrecedencePolicy())
    assert conflicts[0].winning_rule_id == "a-rule"
    assert conflicts[0].precedence_reason == "rule_id_tiebreak"
    assert conflicts[0].requires_review
    assert conflicts == resolve_rule_conflicts([right, left], RulePrecedencePolicy())
    assert conflicts[0].to_dict()["winning_rule_id"] == "a-rule"


def test_conflict_detection_deduplicates_repeated_predicate_pairs() -> None:
    left = BusinessRule(
        rule_id="left",
        table="main.s.t",
        predicates=({"column": "x", "operator": "eq", "value": 1},) * 2,
        origin=PatternOrigin.DECLARED,
    )
    right = BusinessRule(
        rule_id="right",
        table="main.s.t",
        predicates=({"column": "x", "operator": "eq", "value": 2},),
        origin=PatternOrigin.DECLARED,
    )
    conflicts = detect_rule_conflicts([left, right])
    assert len(conflicts) == 1


def test_profile_index_and_relationship_identity_are_reusable() -> None:
    index = ProfileIndex.from_rows([{"source_table": "main.s.t", "columns": ["id"]}])
    assert index.has_column("main.s.t", "id")
    assert relationship_analysis_id(
        {"parent": "main.s.p", "child": "main.s.c", "run_id": "one"}
    ) == relationship_analysis_id({"parent": "main.s.p", "child": "main.s.c", "run_id": "two"})


def test_pattern_workflow_returns_compact_summary() -> None:
    refs = PatternInputRefs("m", ("p",), "r", "g")
    result, summary = detect_patterns(
        PatternDetector(PatternConfig(min_support_rows=2)),
        rows=[{"x": 1, "y": 2}, {"x": 2, "y": 4}],
        run_id="run",
        environment="dev",
        input_refs=refs,
        table="main.s.t",
        columns={"x": "double", "y": "double"},
    )
    assert summary.stage == "patterns_detected"
    assert summary.accepted_for_planning == len(result.patterns)
