import ast
import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest

from sda.job_entrypoints.pattern_detect_spark import _candidate_columns
from sda.metadata_models import ColumnMetadata, ObjectType, TableMetadata
from sda.patterns.spark_metrics import (
    spark_conditional_distribution,
    spark_conditional_missingness,
    spark_fanout_by_segment,
    spark_metric,
    spark_numeric_by_group,
    spark_pearson,
    spark_state_transitions,
    spark_temporal_lag,
    spark_temporal_order,
    unsupported_metric_result,
)
from sda.profile_models import ProfileMode, TableProfileRequest
from sda.tools.table_profiler import TableProfiler


def test_spark_entrypoint_candidates_respect_roles_and_exclusions() -> None:
    columns = {
        "account_id": "string",
        "amount": "decimal(12,2)",
        "small": "smallint",
        "segment": "string",
        "secret": "string",
        "created_at": "timestamp",
        "closed_at": "timestamp",
        "status": "string",
    }
    roles = {
        "driver": ("amount", "small", "segment", "secret", "status"),
        "outcome": ("amount", "small", "segment", "secret", "status"),
        "entity": ("account_id",),
        "lifecycle": ("created_at", "closed_at"),
    }
    numeric, categorical, temporal, entity = _candidate_columns(
        columns, roles, {"secret", "closed_at"}
    )
    assert numeric == ["amount", "small"]
    assert categorical == ["segment", "status"]
    assert temporal == ["created_at"]
    assert entity == ["account_id"]


@pytest.mark.spark  # type: ignore[untyped-decorator]
def test_table_profiler_handles_decimal_iqr_outliers(spark) -> None:
    metadata = TableMetadata(
        catalog="main",
        schema="quality",
        object_name="decimal_values",
        object_type=ObjectType.TABLE,
        columns=(ColumnMetadata("amount", "DECIMAL(10,2)", True, 1),),
    )
    frame = spark.createDataFrame(
        [(Decimal("1.00"),), (Decimal("2.00"),), (Decimal("3.00"),), (Decimal("100.00"),)],
        "amount decimal(10,2)",
    )
    profile = TableProfiler(
        TableProfileRequest(metadata.full_name, mode=ProfileMode.FULL, sample_fraction=1.0),
        metadata,
    ).profile_spark(frame)
    amount = next(column for column in profile.column_profiles if column.column_name == "amount")
    iqr = next(finding for finding in amount.outlier_findings if finding["method"] == "iqr")
    assert iqr["thresholds"] == {"lower": 0.5, "upper": 4.5}
    assert iqr["outlier_count"] == 1


def test_candidates_include_single_role_conditional_columns() -> None:
    numeric, categorical, _, _ = _candidate_columns(
        {"segment": "string", "amount": "double", "secret": "string"},
        {"driver": ("segment", "secret"), "outcome": ("amount",)},
        {"secret"},
    )
    assert numeric == ["amount"]
    assert categorical == ["segment"]


@pytest.fixture(scope="module")
def spark():
    if sys.version_info >= (3, 13):
        pytest.skip("local PySpark worker is incompatible with Python 3.13 on Windows")
    pyspark = pytest.importorskip("pyspark")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        pyspark.sql.SparkSession.builder.master("local[2]")
        .appName("sda07-metrics-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.spark  # type: ignore[untyped-decorator]
def test_spark_metrics_does_not_collect_key_domains() -> None:
    source = Path("src/sda/relationships/spark_metrics.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    assert not any(
        isinstance(node, ast.Attribute) and node.attr == "collect" for node in ast.walk(tree)
    )


def test_unsupported_spark_metric_result_is_actionable_and_raw_value_free() -> None:
    result = unsupported_metric_result("spearman", "requires numeric columns")
    assert result.to_dict() == {
        "metric": "spearman",
        "supported": False,
        "reason": "requires numeric columns",
        "schema_version": "spark-metric-result-v1",
    }
    result = spark_metric(object(), "spearman")
    assert result == unsupported_metric_result(
        "spearman", "missing required arguments: left, right"
    )
    assert spark_metric(object(), "  SPEARMAN ") == result


def test_temporal_lag_dispatch_is_registered() -> None:
    result = spark_metric(object(), "temporal_lag")
    assert result.metric == "temporal_lag"
    assert not result.supported
    assert "required" in result.reason.lower()


def test_temporal_metric_missing_columns_returns_actionable_result() -> None:
    class Frame:
        schema = type("Schema", (), {"fields": ()})()

    result = spark_metric(Frame(), "temporal_lag", earlier="started_at", later="finished_at")
    assert result.to_dict() == {
        "metric": "temporal_lag",
        "supported": False,
        "reason": "spark_temporal_lag requires columns: started_at, finished_at",
        "schema_version": "spark-metric-result-v1",
    }


def test_conditional_metric_missing_columns_returns_actionable_result() -> None:
    class Frame:
        schema = type("Schema", (), {"fields": ()})()

    result = spark_metric(
        Frame(), "conditional_distribution", drivers=("segment",), outcome="value"
    )
    assert not result.supported
    assert result.reason == "spark_conditional_distribution requires columns: segment, value"


def test_transition_metric_missing_columns_returns_actionable_result() -> None:
    class Frame:
        schema = type("Schema", (), {"fields": ()})()

    result = spark_metric(
        Frame(),
        "state_transitions",
        entity_keys=("entity_id",),
        state_column="status",
        event_time="event_time",
    )
    assert not result.supported
    assert result.reason == (
        "spark_state_transitions requires columns: entity_id, status, event_time"
    )


@pytest.mark.spark  # type: ignore[untyped-decorator]
def test_spark_metric_families_execute_on_deterministic_data(spark) -> None:
    frame = spark.createDataFrame(
        [
            ("premium", "open", 1.0, "2024-01-01", "c1"),
            ("premium", "closed", 2.0, "2024-01-02", "c1"),
            ("standard", "open", 3.0, "2024-01-03", "c2"),
            ("standard", None, None, "2024-01-04", "c2"),
        ],
        "segment string, status string, value double, event_time string, entity string",
    )
    assert spark_pearson(frame, "value", "value").first()["valid_pair_count"] == 3
    with pytest.raises(ValueError, match="requires numeric columns"):
        spark_pearson(frame, "status", "value")
    assert spark_conditional_distribution(frame, ("segment",), "status").count() == 4
    missing = spark_conditional_missingness(frame, ("segment",), "value").collect()
    assert {row["support_rows"] for row in missing} == {2}
    order = spark_temporal_order(frame, "event_time", "event_time").first()
    assert order["violation_rows"] == 0
    lag = spark_temporal_lag(frame, "event_time", "event_time").first()
    assert lag["count"] == 4
    assert lag["positive_duration_count"] == 0
    transitions = spark_state_transitions(
        frame.where("status is not null"),
        entity_keys=("entity",),
        state_column="status",
        event_time="event_time",
        tie_breakers=("status",),
    )
    assert transitions.count() == 1
    assert (
        spark_metric(frame, "pearson", left="value", right="value").first()["valid_pair_count"] == 3
    )
    assert (
        spark_metric(
            frame, "conditional_distribution", drivers=("segment",), outcome="status"
        ).count()
        == 4
    )
    assert (
        spark_metric(
            frame, "conditional_missingness", drivers=("segment",), outcome="value"
        ).count()
        == 2
    )
    assert (
        spark_metric(frame, "temporal_order", earlier="event_time", later="event_time").count() == 1
    )
    assert (
        spark_metric(frame, "temporal_lag", earlier="event_time", later="event_time").count() == 1
    )
    assert (
        spark_metric(
            frame,
            "state_transitions",
            entity_keys=("entity",),
            state_column="status",
            event_time="event_time",
            tie_breakers=("status",),
        ).count()
        == 2
    )


@pytest.mark.spark  # type: ignore[untyped-decorator]
def test_spark_dispatcher_handles_partitions_skew_and_nulls(spark) -> None:
    rows = [("hot", float(index), None if index % 7 == 0 else "open") for index in range(240)]
    rows.extend([("cold", 1.0, "closed"), ("cold", None, None)])
    frame = spark.createDataFrame(rows, "segment string, value double, status string").repartition(
        4
    )

    correlation = spark_metric(frame, "pearson", left="value", right="value")
    assert correlation.first()["valid_pair_count"] == 241
    distribution = spark_metric(
        frame, "conditional_distribution", drivers=("segment",), outcome="status"
    )
    assert distribution.where("segment = 'hot'").count() >= 1
    missing = spark_metric(frame, "conditional_missingness", drivers=("segment",), outcome="status")
    assert {row["support_rows"] for row in missing.collect()} == {2, 240}
    unsupported = spark_metric(frame, "pearson", left="status", right="value")
    assert not unsupported.supported


@pytest.mark.spark  # type: ignore[untyped-decorator]
def test_numeric_by_group_baseline_and_null_driver_join(spark) -> None:
    frame = spark.createDataFrame(
        [(None, 2.0, "open"), (None, 4.0, "closed"), ("a", 10.0, "open"), ("a", None, "open")],
        "segment string, value double, status string",
    )
    groups = {
        row["segment"]: row.asDict()
        for row in spark_numeric_by_group(frame, ("segment",), "value").collect()
    }
    assert groups[None]["count"] == 2
    assert groups[None]["mean"] == 3.0
    assert groups[None]["global_count"] == 3
    assert groups[None]["global_mean"] == pytest.approx(16 / 3)
    assert groups["a"]["min"] == 10.0
    assert (
        spark_metric(frame, "numeric_by_group", drivers=("segment",), outcome="value").count() == 2
    )
    distribution = spark_conditional_distribution(frame, ("segment",), "status").collect()
    assert all(row["support_rows"] == 2 for row in distribution)


@pytest.mark.spark  # type: ignore[untyped-decorator]
def test_state_transitions_ties_censoring_and_duration(spark) -> None:
    frame = spark.createDataFrame(
        [
            ("a", "open", "2024-01-01 00:00:00", 1),
            ("a", "open", "2024-01-01 00:00:00", 2),
            ("a", "closed", "2024-01-01 00:01:00", 3),
            ("b", "open", "2024-01-01 00:00:00", 1),
            ("c", "closed", "2024-01-01 00:00:00", 1),
        ],
        "entity string, status string, event_time string, sequence int",
    )
    with pytest.raises(ValueError, match="temporal_tie_breaker_missing"):
        spark_state_transitions(
            frame, entity_keys=("entity",), state_column="status", event_time="event_time"
        )
    with pytest.raises(ValueError, match="temporal_tie_breaker_not_unique"):
        spark_state_transitions(
            frame.withColumn("sequence", frame.sequence * 0),
            entity_keys=("entity",),
            state_column="status",
            event_time="event_time",
            tie_breakers=("sequence",),
        )
    rows = {
        (row["from_state"], row["to_state"]): row.asDict()
        for row in spark_state_transitions(
            frame.repartition(2),
            entity_keys=("entity",),
            state_column="status",
            event_time="event_time",
            tie_breakers=("sequence",),
        ).collect()
    }
    assert rows[("open", "open")]["self_transition"] is True
    assert rows[("open", "open")]["mean_time_in_state_seconds"] == 0
    assert rows[("open", "closed")]["mean_time_in_state_seconds"] == 60
    assert rows[("open", "closed")]["right_censored_count"] == 1
    assert rows[("open", "closed")]["transition_probability"] == 0.5


@pytest.mark.spark  # type: ignore[untyped-decorator]
def test_state_transitions_record_event_clock_ingestion_clock_and_late_order(spark) -> None:
    frame = spark.createDataFrame(
        [
            ("a", "open", "2024-01-01 00:01:00", "2024-01-01 00:03:00", 1),
            ("a", "closed", "2024-01-01 00:02:00", "2024-01-01 00:02:00", 2),
        ],
        "entity string, status string, event_time string, ingestion_time string, event_id int",
    )
    row = spark_state_transitions(
        frame,
        entity_keys=("entity",),
        state_column="status",
        event_time="event_time",
        ingestion_time="ingestion_time",
        tie_breakers=("event_id",),
    ).first()
    assert row["from_state"] == "open"
    assert row["to_state"] == "closed"
    assert row["ingestion_order_disagreement_count"] == 1
    assert row["event_time_column"] == "event_time"
    assert row["ingestion_time_column"] == "ingestion_time"
    assert row["tie_breaker_columns"] == ["event_id"]


@pytest.mark.spark  # type: ignore[untyped-decorator]
def test_spark_fanout_includes_zero_child_parents(spark) -> None:
    parents = spark.createDataFrame(
        [("p1", "premium"), ("p2", "standard")], "id string, segment string"
    )
    children = spark.createDataFrame([("p1",)], "parent_id string")
    rows = spark_fanout_by_segment(
        parents,
        children,
        parent_keys=("id",),
        child_keys=("parent_id",),
        segments=("segment",),
    ).collect()
    assert {row["zero_child_count"] for row in rows} == {0, 1}


@pytest.mark.spark  # type: ignore[untyped-decorator]
def test_spark_fanout_requires_segment_columns(spark) -> None:
    frame = spark.createDataFrame([("p1",)], "id string")
    with pytest.raises(ValueError, match="segment column"):
        spark_fanout_by_segment(frame, frame, parent_keys=("id",), child_keys=("id",), segments=())
