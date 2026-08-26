from __future__ import annotations

import pytest

from sda.planning import ColumnGenerationSpec, GenerationPlan, PlanStatus
from sda.relational import (
    CompositeForeignKeySpec,
    ForeignKeySpec,
    RelationalGenerationError,
    generate_relational,
)
from sda.validation import CheckStatus, ValidationCheck, ValidationReport, validate_tables


def test_validation_evidence_is_immutable_and_check_ids_are_unique() -> None:
    check = ValidationCheck("schema", CheckStatus.PASS, "ok", {"actual": 1, "details": [{"x": 2}]})
    with pytest.raises(TypeError, match="immutable"):
        check.evidence["actual"] = 2
    with pytest.raises(TypeError, match="immutable"):
        check.evidence["details"][0]["x"] = 3
    with pytest.raises(ValueError, match="unique"):
        ValidationReport((check, check), "qa", CheckStatus.PASS)


def test_schema_validation_fails_closed_for_missing_required_columns() -> None:
    report = validate_tables(
        {"orders": ({"id": 1},)}, required_columns={"orders": ("id", "amount")}
    )
    assert report.technical_disposition is CheckStatus.FAIL
    assert report.checks[0].evidence["missing"] == ("amount",)


def test_validation_fails_closed_when_no_checks_are_requested() -> None:
    report = validate_tables({"users": (({"id": 1}),)})
    assert report.technical_disposition is CheckStatus.FAIL
    assert report.checks[0].check_id == "validation_scope"


def plan() -> GenerationPlan:
    return (
        GenerationPlan(
            plan_id="p",
            plan_version=1,
            request_id="r",
            source_snapshot_ids=("s",),
            input_artifact_ids=("a",),
            target_catalog="c",
            target_schema="s",
            tables=("parent", "child"),
            columns=(
                ColumnGenerationSpec("parent", "id", "string", nullable=False, model="identifier"),
                ColumnGenerationSpec("child", "id", "string", nullable=False, model="identifier"),
            ),
            budgets={"max_rows": 10},
        )
        .transition(PlanStatus.AWAITING_APPROVAL)
        .transition(PlanStatus.APPROVED)
    )


def test_relational_generation_is_orphan_free() -> None:
    fk = ForeignKeySpec("child", "parent_id", "parent", "id")
    tables = generate_relational(plan(), row_counts={"parent": 2, "child": 5}, foreign_keys=(fk,))
    report = validate_tables(
        tables,
        expected_counts={"parent": 2, "child": 5},
        unique_keys={"parent": "id"},
        foreign_keys=(("child", "parent_id", "parent", "id"),),
        intended_use="qa",
    )
    assert report.technical_disposition is CheckStatus.PASS
    assert len(report.checks) == 4


def test_relational_generation_reproduces_exact_fanout_distribution() -> None:
    fk = ForeignKeySpec("child", "parent_id", "parent", "id")
    first = generate_relational(
        plan(),
        row_counts={"parent": 3, "child": 6},
        foreign_keys=(fk,),
        fanout_distributions={("child", "parent"): (4, 0, 2)},
    )
    second = generate_relational(
        plan(),
        row_counts={"parent": 3, "child": 6},
        foreign_keys=(fk,),
        fanout_distributions={("child", "parent"): (4, 0, 2)},
    )
    assert first == second
    counts = {row["id"]: 0 for row in first["parent"]}
    for row in first["child"]:
        counts[row["parent_id"]] += 1
    assert tuple(counts[row["id"]] for row in first["parent"]) == (4, 0, 2)


@pytest.mark.parametrize(
    "distribution, message",
    [((1, 1, 1), "one count per parent"), ((1, 1), "sum to the child")],
)
def test_relational_generation_rejects_unreconciled_fanout(
    distribution: tuple[int, ...], message: str
) -> None:
    fk = ForeignKeySpec("child", "parent_id", "parent", "id")
    with pytest.raises(RelationalGenerationError, match=message):
        generate_relational(
            plan(),
            row_counts={"parent": 2, "child": 3},
            foreign_keys=(fk,),
            fanout_distributions={("child", "parent"): distribution},
        )


def test_cycles_fail_closed() -> None:
    with pytest.raises(RelationalGenerationError, match="cycle"):
        generate_relational(
            plan(),
            row_counts={"parent": 1, "child": 1},
            foreign_keys=(
                ForeignKeySpec("parent", "child_id", "child", "id"),
                ForeignKeySpec("child", "parent_id", "parent", "id"),
            ),
        )


def test_composite_foreign_keys_are_wired_as_tuples() -> None:
    composite_plan = (
        GenerationPlan(
            plan_id="cp",
            plan_version=1,
            request_id="r",
            source_snapshot_ids=("s",),
            input_artifact_ids=("a",),
            target_catalog="c",
            target_schema="s",
            tables=("parent", "child"),
            columns=(
                ColumnGenerationSpec(
                    "parent", "id_a", "string", nullable=False, model="identifier"
                ),
                ColumnGenerationSpec(
                    "parent", "id_b", "string", nullable=False, model="identifier"
                ),
                ColumnGenerationSpec("child", "id", "string", nullable=False, model="identifier"),
            ),
            budgets={"max_rows": 10},
        )
        .transition(PlanStatus.AWAITING_APPROVAL)
        .transition(PlanStatus.APPROVED)
    )
    fk = CompositeForeignKeySpec("child", ("a", "b"), "parent", ("id_a", "id_b"))
    tables = generate_relational(
        composite_plan, row_counts={"parent": 2, "child": 2}, composite_foreign_keys=(fk,)
    )
    assert all(
        (row["a"], row["b"]) in {(p["id_a"], p["id_b"]) for p in tables["parent"]}
        for row in tables["child"]
    )


def test_validator_reports_orphans_as_failure() -> None:
    report = validate_tables(
        {"p": (({"id": "p1"}),), "c": (({"p": "missing"}),)},
        foreign_keys=(("c", "p", "p", "id"),),
    )
    assert report.technical_disposition is CheckStatus.FAIL


def test_nullable_optional_foreign_keys_are_deterministic_and_orphan_free() -> None:
    fk = ForeignKeySpec("child", "parent_id", "parent", "id", nullable=True, optional_rate=0.5)
    first = generate_relational(plan(), row_counts={"parent": 3, "child": 7}, foreign_keys=(fk,))
    second = generate_relational(plan(), row_counts={"parent": 3, "child": 7}, foreign_keys=(fk,))
    assert first == second
    values = [row["parent_id"] for row in first["child"]]
    assert any(value is None for value in values)
    assert any(value is not None for value in values)
    assert set(value for value in values if value is not None) <= {
        row["id"] for row in first["parent"]
    }


def test_optional_rate_requires_nullable_foreign_key() -> None:
    with pytest.raises(ValueError, match="nullable"):
        ForeignKeySpec("child", "parent_id", "parent", "id", optional_rate=0.1)


def test_validation_does_not_turn_missing_inputs_into_success() -> None:
    report = validate_tables(
        {},
        expected_counts={"customers": 0},
        unique_keys={"customers": "id"},
        foreign_keys=(("orders", "customer_id", "customers", "id"),),
    )
    assert report.technical_disposition is CheckStatus.FAIL
    assert all(check.status is CheckStatus.FAIL for check in report.checks)


def test_not_applicable_validation_requires_explicit_reason() -> None:
    with pytest.raises(ValueError, match="unsupported_reason"):
        ValidationCheck("unsupported", CheckStatus.NOT_APPLICABLE, "n/a", {})


def test_validation_report_cannot_claim_pass_with_failed_check() -> None:
    with pytest.raises(ValueError, match="technical_disposition"):
        ValidationReport(
            (ValidationCheck("bad", CheckStatus.FAIL, "failed", {}),),
            "qa",
            CheckStatus.PASS,
        )


def test_validation_vector_is_immutable_and_dimensioned() -> None:
    report = ValidationReport(
        (), "qa", CheckStatus.PASS, {"schema": CheckStatus.PASS, "privacy": CheckStatus.WARN}
    )
    assert report.validation_vector["privacy"] is CheckStatus.WARN
    with pytest.raises(TypeError, match="immutable"):
        report.validation_vector["schema"] = CheckStatus.FAIL  # type: ignore[index]


def test_validation_checks_categorical_distributions_with_tolerance() -> None:
    tables = {"orders": (({"status": "new"}), ({"status": "new"}), ({"status": "closed"}))}
    report = validate_tables(
        tables,
        expected_distributions={"orders": {"status": {"new": 2 / 3, "closed": 1 / 3}}},
    )
    assert report.technical_disposition is CheckStatus.PASS
    assert report.checks[-1].population == "full_table"
    failed = validate_tables(
        tables,
        expected_distributions={"orders": {"status": {"new": 0.5, "closed": 0.5}}},
        distribution_tolerance=0.01,
    )
    assert failed.technical_disposition is CheckStatus.FAIL


def test_distribution_validation_fails_closed_for_missing_inputs() -> None:
    report = validate_tables({}, expected_distributions={"orders": {"status": {"new": 1.0}}})
    assert report.checks[0].status is CheckStatus.FAIL
    with pytest.raises(ValueError, match="sum to one"):
        validate_tables(
            {"orders": (({"status": "new"}),)},
            expected_distributions={"orders": {"status": {"new": 0.5}}},
        )


def test_validation_checks_conditional_null_rates() -> None:
    tables = {
        "orders": (
            {"segment": "retail", "note": None},
            {"segment": "retail", "note": "ok"},
            {"segment": "enterprise", "note": "ok"},
        )
    }
    report = validate_tables(
        tables,
        conditional_null_rates={
            "orders": {("segment", "note"): {"retail": 0.5, "enterprise": 0.0}}
        },
    )
    assert report.technical_disposition is CheckStatus.PASS
    assert report.checks[-1].population == "full_table_by_driver"


def test_validation_checks_string_formats_fail_closed() -> None:
    tables = {"users": (({"email": "a@example.com"}), ({"email": "invalid"}))}
    report = validate_tables(tables, format_patterns={"users": {"email": r"^[^@]+@[^@]+\.[^@]+$"}})
    assert report.technical_disposition is CheckStatus.FAIL
    assert report.checks[-1].evidence["invalid"] == 1
    with pytest.raises(ValueError, match="invalid format pattern"):
        validate_tables(tables, format_patterns={"users": {"email": "["}})


def test_validation_checks_parent_fanout_including_zero_child_parents() -> None:
    tables = {
        "parents": (({"id": 1}), ({"id": 2})),
        "children": (({"parent_id": 1}),),
    }
    report = validate_tables(
        tables, fanout_bounds={("children", "parent_id", "parents", "id"): (0, 1)}
    )
    assert report.technical_disposition is CheckStatus.PASS
    assert report.checks[-1].evidence["zero_child_parents"] == 1


def test_relational_generation_requires_explicit_count_for_each_table() -> None:
    with pytest.raises(RelationalGenerationError, match="every planned table"):
        generate_relational(plan(), row_counts={"parent": 2})


def test_validate_tables_preserves_intended_use_validation_vector() -> None:
    report = validate_tables(
        {"users": (({"id": 1}),)},
        intended_use="qa",
        validation_vector={"schema": CheckStatus.PASS, "privacy": CheckStatus.WARN},
    )
    assert report.intended_use == "qa"
    assert report.validation_vector["privacy"] is CheckStatus.WARN
    with pytest.raises(TypeError, match="immutable"):
        report.validation_vector["schema"] = CheckStatus.FAIL  # type: ignore[index]


def test_validation_report_serializes_check_contract_and_disposition() -> None:
    report = validate_tables(
        {"customers": (({"id": 1}),)},
        expected_counts={"customers": 1},
        intended_use="qa",
        validation_vector={"schema": CheckStatus.PASS},
    )
    payload = report.to_dict()
    assert payload["technical_disposition"] == "PASS"
    assert payload["schema_version"] == "validation-report-v1"
    assert payload["checks"][0]["schema_version"] == "validation-check-v1"
    assert payload["validation_vector"] == {"schema": "PASS"}
    assert payload["checks"][0]["check_id"] == "row_count:customers"
    assert payload["fingerprint"] == report.fingerprint


def test_validation_report_serialization_redacts_raw_string_evidence() -> None:
    check = ValidationCheck("distribution", CheckStatus.PASS, "ok", {"value": "secret"})
    report = ValidationReport((check,), "qa", CheckStatus.PASS)
    assert "secret" not in str(report.to_dict())
    assert report.checks[0].evidence["value"] == "secret"


def test_validation_checks_time_ordering() -> None:
    report = validate_tables(
        {"events": (({"started": 1, "ended": 2}), ({"started": 4, "ended": 3}))},
        time_orderings=(("events", "started", "ended"),),
    )
    assert report.technical_disposition is CheckStatus.FAIL
    assert report.checks[-1].evidence["invalid"] == 1


def test_validation_checks_numeric_bounds_and_allows_nulls() -> None:
    report = validate_tables(
        {"metrics": (({"score": 0.5}), ({"score": None}), ({"score": 2.0}))},
        numeric_bounds={"metrics": {"score": (0.0, 1.0)}},
    )
    assert report.technical_disposition is CheckStatus.FAIL
    assert report.checks[-1].evidence["invalid"] == 1


def test_validation_checks_composite_key_uniqueness() -> None:
    report = validate_tables(
        {
            "events": (
                {"tenant": "a", "event": 1},
                {"tenant": "a", "event": 2},
                {"tenant": "b", "event": 1},
            )
        },
        unique_key_sets={"events": ("tenant", "event")},
    )
    assert report.technical_disposition is CheckStatus.PASS
    assert report.checks[-1].evidence["distinct"] == 3
