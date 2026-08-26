import ast
import os
import sys
from pathlib import Path

import pytest

from sda.patterns.spark_metrics import (
    spark_conditional_distribution,
    spark_conditional_missingness,
    spark_fanout_by_segment,
    spark_metric,
    spark_pearson,
    spark_state_transitions,
    spark_temporal_order,
    unsupported_metric_result,
)


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
        "spearman", "metric is not implemented by the Spark adapter"
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
    transitions = spark_state_transitions(
        frame.where("status is not null"),
        entity_keys=("entity",),
        state_column="status",
        event_time="event_time",
        tie_breakers=("status",),
    )
    assert transitions.count() == 1


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
