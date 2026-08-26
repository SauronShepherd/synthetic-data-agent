import pytest

from sda.patterns import PatternConfig, PatternDetector, PatternInputRefs
from sda.patterns.candidates import generate_candidates
from sda.patterns.conflicts import detect_rule_conflicts
from sda.patterns.fanout import fanout_by_segment
from sda.patterns.models import PatternOrigin
from sda.patterns.persistence import (
    PATTERN_EVIDENCE_SCHEMA_VERSION,
    PATTERN_REGISTRY_SCHEMA_VERSION,
    evidence_rows,
    registry_rows,
    require_pattern_schema_version,
)
from sda.patterns.rules import BusinessRule, RuleStrength, evaluate_rule
from sda.patterns.safety import SafeValueKind, safe_pattern_value
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


def test_candidates_are_bounded_and_deterministic() -> None:
    columns = {f"c{i}": "double" for i in range(20)}
    config = PatternConfig(max_candidates=7)
    left = generate_candidates("main.s.t", columns, config=config)
    right = generate_candidates("main.s.t", dict(reversed(list(columns.items()))), config=config)
    assert len(left) == 7
    assert left == right


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
