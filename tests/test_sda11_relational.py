from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path

import pytest

from sda.relational import (
    CompositeForeignKeySpec,
    RelationalDatasetStatus,
    RelationalGenerationError,
    approve_relational_dataset,
    generate_relational,
    generate_relational_dataset,
    mark_relational_validated,
    recover_relational_staging,
    stage_relational_dataset,
    validate_relational_tables,
)
from sda.relational_spark import build_sda11_smoke_relational_plan


def smoke():
    return build_sda11_smoke_relational_plan()


def test_relational_fixture_is_reproducible_and_structurally_valid() -> None:
    relational, counts, fanouts = smoke()
    first = generate_relational(
        relational,
        row_counts=counts,
        fanout_distributions=fanouts,
        fanout_mode="exact-total",
    )
    second = generate_relational(
        relational,
        row_counts=counts,
        fanout_distributions=fanouts,
        fanout_mode="exact-total",
    )
    assert first == second
    report = validate_relational_tables(
        first,
        key_domains=relational.key_domains,
        foreign_keys=relational.foreign_keys,
        composite_foreign_keys=relational.composite_foreign_keys,
        bridge_tables=relational.bridge_tables,
        self_references=relational.self_references,
        expected_counts=counts,
        fanout_bounds={("orders", "customer_id", "customers", "customer_id"): (0, 6)},
        expected_fanouts=fanouts,
    )
    assert report.technical_disposition.value == "PASS"
    orders_by_customer = {row["customer_id"]: 0 for row in first["customers"]}
    for row in first["orders"]:
        orders_by_customer[row["customer_id"]] += 1
    assert 0 in orders_by_customer.values()
    assert max(orders_by_customer.values()) == 6
    bridge_pairs = {
        (row["customer_id"], row["product_id"]) for row in first["customer_product"]
    }
    assert len(bridge_pairs) == len(first["customer_product"])
    profiles = [row["customer_id"] for row in first["customer_profile"]]
    assert len(profiles) == len(set(profiles))
    assert any(row["manager_id"] is None for row in first["employees"])


def test_distribution_first_can_derive_child_total() -> None:
    relational, counts, fanouts = smoke()
    counts_without_orders = dict(counts)
    counts_without_orders.pop("orders")
    tables = generate_relational(
        relational,
        row_counts=counts_without_orders,
        fanout_distributions=fanouts,
        fanout_mode="distribution-first",
    )
    assert len(tables["orders"]) == sum(fanouts[("orders", "customers")])


def test_exact_total_reconciliation_is_deterministic_and_keeps_zero_children() -> None:
    relational, counts, fanouts = smoke()
    expanded = {**counts, "orders": 12}
    first = generate_relational(
        relational,
        row_counts=expanded,
        fanout_distributions=fanouts,
        fanout_mode="exact-total",
    )
    second = generate_relational(
        relational,
        row_counts=expanded,
        fanout_distributions=fanouts,
        fanout_mode="exact-total",
    )
    assert first == second
    counts_by_customer = {row["customer_id"]: 0 for row in first["customers"]}
    for row in first["orders"]:
        counts_by_customer[row["customer_id"]] += 1
    assert len(first["orders"]) == 12
    assert any(value == 0 for value in counts_by_customer.values())


def test_cycle_without_approved_strategy_fails_closed() -> None:
    relational, counts, fanouts = smoke()
    unresolved = replace(relational, cycle_strategies=(), relational_fingerprint="")
    with pytest.raises(RelationalGenerationError, match="unresolved cycle"):
        generate_relational(
            unresolved,
            row_counts=counts,
            fanout_distributions=fanouts,
            fanout_mode="exact-total",
        )


def test_rejected_and_stale_relationship_artifacts_are_not_promoted() -> None:
    relational, counts, fanouts = smoke()
    stale_artifact = dict(relational.relationship_artifact or {})
    stale_artifact["status"] = "stale"
    stale = replace(relational, relationship_artifact=stale_artifact, relational_fingerprint="")
    with pytest.raises(RelationalGenerationError, match="stale"):
        generate_relational(stale, row_counts=counts, fanout_distributions=fanouts)

    rejected_artifact = dict(relational.relationship_artifact or {})
    rejected_relationships = list(rejected_artifact["relationships"])
    rejected_relationships[0] = {**rejected_relationships[0], "decision": "rejected"}
    rejected_artifact["relationships"] = rejected_relationships
    rejected = replace(
        relational,
        relationship_artifact=rejected_artifact,
        relational_fingerprint="",
    )
    with pytest.raises(RelationalGenerationError, match="rejected"):
        generate_relational(rejected, row_counts=counts, fanout_distributions=fanouts)


def test_one_to_one_capacity_and_bridge_capacity_are_enforced() -> None:
    relational, counts, fanouts = smoke()
    too_many_profiles = {**counts, "customer_profile": 5}
    with pytest.raises(RelationalGenerationError, match="one-to-one"):
        generate_relational(
            relational,
            row_counts=too_many_profiles,
            fanout_distributions=fanouts,
            fanout_mode="exact-total",
        )
    too_many_pairs = {**counts, "customer_product": 13}
    with pytest.raises(RelationalGenerationError, match="bridge"):
        generate_relational(
            relational,
            row_counts=too_many_pairs,
            fanout_distributions=fanouts,
            fanout_mode="exact-total",
        )


def test_manifest_stages_retries_and_separates_validation_from_approval(tmp_path: Path) -> None:
    relational, counts, fanouts = smoke()
    tables, manifest = generate_relational_dataset(
        relational,
        row_counts=counts,
        run_id="retry-1",
        output_namespace="workspace.sda11",
        staging_base_path=str(tmp_path),
        fanout_distributions=fanouts,
        fanout_mode="exact-total",
    )
    assert manifest.status is RelationalDatasetStatus.GENERATED_AWAITING_VALIDATION
    staged = stage_relational_dataset(str(tmp_path), manifest, tables)
    assert staged == manifest.staging_namespace
    assert recover_relational_staging(str(tmp_path), "retry-1", manifest.dataset_fingerprint)["status"] == (
        "GENERATED_AWAITING_VALIDATION"
    )
    report = validate_relational_tables(
        tables,
        key_domains=relational.key_domains,
        foreign_keys=relational.foreign_keys,
        composite_foreign_keys=relational.composite_foreign_keys,
        bridge_tables=relational.bridge_tables,
        self_references=relational.self_references,
        expected_counts=counts,
    )
    validated = mark_relational_validated(manifest, report)
    assert validated.status is RelationalDatasetStatus.VALIDATED
    approved = approve_relational_dataset(validated, "test-operator")
    assert approved.status is RelationalDatasetStatus.APPROVED
    assert approved.approved_by == "test-operator"


def test_composite_partial_null_is_not_an_optional_relationship() -> None:
    report = validate_relational_tables(
        {
            "parent": ({"tenant": "t1", "number": "n1"},),
            "child": ({"tenant": "t1", "number": None},),
        },
        primary_keys={"parent": ("tenant", "number")},
        composite_foreign_keys=(
            CompositeForeignKeySpec("child", ("tenant", "number"), "parent", ("tenant", "number")),
        ),
    )
    assert report.technical_disposition.value == "FAIL"


def test_spark_adapter_does_not_collect_key_domains() -> None:
    source = Path("src/sda/relational_spark.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    assert not any(isinstance(node, ast.Attribute) and node.attr == "collect" for node in ast.walk(tree))
