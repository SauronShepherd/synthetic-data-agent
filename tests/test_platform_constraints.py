from __future__ import annotations

from decimal import Decimal

import pytest

from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType
from sda.metadata_models import ConstraintKind, ConstraintMetadata
from sda.patterns.models import PatternDecision, PatternLifecycle, PatternOrigin
from sda.patterns.platform_constraints import evaluate_platform_constraints, parse_check_clause
from sda.patterns.upstream import source_constraints


def test_check_clause_parser_normalizes_identifiers_and_rejects_sql_fragments() -> None:
    parsed = parse_check_clause(
        "CHECK ((amount > 0) AND (`status` IN ('OPEN', 'CLOSED'))) ",
        ("amount", "status"),
    )

    assert parsed.columns == ("amount", "status")
    assert "`amount`" in parsed.expression
    assert "`status`" in parsed.expression
    for unsafe in (
        "amount > 0; DROP TABLE orders",
        "abs(amount) > 0",
        "amount > 0 -- comment",
        "other.amount > 0",
    ):
        with pytest.raises(ValueError):
            parse_check_clause(unsafe, ("amount",))


def test_source_constraints_rehydrate_metadata_and_non_nullable_columns() -> None:
    metadata = ArtifactRef(
        artifact_id="metadata-1",
        artifact_type=ArtifactType.METADATA_INVENTORY,
        artifact_schema_version="1",
        status=ArtifactStatus.COMPLETE,
        tool_name="uc_metadata_reader",
        tool_version="1",
        run_id="run-1",
        environment="dev",
        created_at="2026-10-22T00:00:00Z",
        configuration_hash="config",
        primary_location="metadata_inventory",
        related_locations={},
        source_references=(),
        checksum="checksum",
        summary="metadata",
        content={
            "tables": [
                {
                    "full_name": "main.sales.orders",
                    "columns": [
                        {"name": "order_id", "nullable": False},
                        {"name": "comment", "nullable": True},
                    ],
                    "constraints": [
                        {
                            "name": "orders_pk",
                            "kind": "PRIMARY KEY",
                            "columns": ["order_id"],
                            "enforced": False,
                        }
                    ],
                }
            ]
        },
    )

    constraints = source_constraints("main.sales.orders", (metadata,))

    assert [(item.name, item.kind, item.enforced) for item in constraints] == [
        ("orders_pk", ConstraintKind.PRIMARY_KEY, False),
        ("metadata_not_null_order_id", ConstraintKind.NOT_NULL, True),
    ]


@pytest.mark.spark  # type: ignore[untyped-decorator]
def test_platform_constraints_measure_violations_and_keep_keys_informational(spark) -> None:
    from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType

    frame = spark.createDataFrame(
        [
            (1, "OPEN", Decimal("4.00")),
            (2, "CLOSED", Decimal("0.00")),
            (3, "OPEN", Decimal("-2.00")),
            (4, "OPEN", None),
        ],
        StructType(
            [
                StructField("order_id", LongType(), False),
                StructField("status", StringType(), True),
                StructField("amount", DecimalType(10, 2), True),
            ]
        ),
    )
    patterns = evaluate_platform_constraints(
        frame,
        (
            ConstraintMetadata(
                "amount_positive",
                ConstraintKind.CHECK,
                ("amount",),
                check_clause="amount > 0",
                enforced=True,
            ),
            ConstraintMetadata(
                "positive_order_id",
                ConstraintKind.CHECK,
                ("order_id",),
                check_clause="order_id > 0",
                enforced=True,
            ),
            ConstraintMetadata(
                "status_not_null",
                ConstraintKind.NOT_NULL,
                ("status",),
                enforced=True,
            ),
            ConstraintMetadata(
                "orders_pk",
                ConstraintKind.PRIMARY_KEY,
                ("order_id",),
            ),
        ),
        analysis_id="analysis-1",
        table="main.sales.orders",
    )

    by_name = {pattern.condition["constraint_name"]: pattern for pattern in patterns}
    check = by_name["amount_positive"]
    assert check.metric["population_rows"] == 4
    assert check.metric["violation_rows"] == 3
    assert check.metric["indeterminate_rows"] == 1
    assert check.origin is PatternOrigin.PLATFORM
    assert check.lifecycle is PatternLifecycle.REVIEW_REQUIRED
    assert check.decision == PatternDecision.REVIEW_REQUIRED.value
    assert check.generation_action["kind"] == "review_only"

    verified_check = by_name["positive_order_id"]
    assert verified_check.metric["violation_rows"] == 0
    assert verified_check.lifecycle is PatternLifecycle.APPROVED_RULE

    not_null = by_name["status_not_null"]
    assert not_null.metric["violation_rows"] == 0
    assert not_null.lifecycle is PatternLifecycle.APPROVED_RULE
    assert not_null.generation_action["kind"] == "enforce_platform_constraint"

    primary_key = by_name["orders_pk"]
    assert primary_key.metric["method"] == "metadata_only"
    assert primary_key.lifecycle is PatternLifecycle.DECLARED_RULE
    assert primary_key.origin is PatternOrigin.DECLARED
    assert "informational_key_requires_sda06_validation" in primary_key.warnings


@pytest.fixture(scope="module")
def spark():
    import os
    import sys

    if sys.version_info >= (3, 13):
        pytest.skip("local PySpark worker is incompatible with Python 3.13 on Windows")
    pyspark = pytest.importorskip("pyspark")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        pyspark.sql.SparkSession.builder.master("local[2]")
        .appName("sda07-platform-constraint-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
