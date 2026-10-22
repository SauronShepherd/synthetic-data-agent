from __future__ import annotations

import sys
import types

import pytest

from sda.job_entrypoints.pattern_detect_spark import (
    _analysis_id,
    _bool_arg,
    _bounded_driver_sets,
    _orient_temporal_order,
    parse_args,
)


def test_analysis_identity_tracks_snapshot_inputs_and_configuration() -> None:
    kwargs = {
        "input_artifact_ids": ("profile-a", "metadata-a"),
        "source_table": "main.sales.orders",
        "snapshot_kind": "delta_version",
        "source_version": "7",
        "snapshot_timestamp": "2026-10-22T00:00:00Z",
        "configuration_hash": "config-a",
        "rule_set_hash": "rules-a",
        "run_id": "run-a",
        "started_at": "2026-10-22T00:00:00Z",
    }
    first = _analysis_id("", **kwargs)
    assert first == _analysis_id("", **kwargs)
    assert first != _analysis_id("", **{**kwargs, "source_version": "8"})
    assert first != _analysis_id("", **{**kwargs, "configuration_hash": "config-b"})
    assert first != _analysis_id("", **{**kwargs, "rule_set_hash": "rules-b"})
    assert _analysis_id("requested", **kwargs) == "requested"


def test_bounded_driver_sets_cover_interactions_in_deterministic_depth_order() -> None:
    assert _bounded_driver_sets(("country", "tier", "channel"), max_depth=2) == (
        ("country",),
        ("tier",),
        ("channel",),
        ("country", "tier"),
        ("country", "channel"),
        ("tier", "channel"),
    )


def test_temporal_direction_is_inferred_from_lower_violation_count_and_flagged() -> None:
    earlier, later, data = _orient_temporal_order(
        "closed_at",
        "created_at",
        {
            "eligible_rows": 100,
            "valid_rows": 10,
            "violation_rows": 90,
            "reverse_violation_rows": 10,
            "violation_rate": 0.9,
            "equal_rows": 0,
        },
    )
    assert (earlier, later) == ("created_at", "closed_at")
    assert data["valid_rows"] == 90
    assert data["violation_rows"] == 10
    assert data["violation_rate"] == 0.1
    assert data["direction_inferred"] is True
    assert data["direction_tied"] is False
    assert data["warnings"] == ["temporal_direction_inferred_from_observed_data"]

    earlier, later, tied = _orient_temporal_order(
        "a_at",
        "b_at",
        {
            "eligible_rows": 100,
            "valid_rows": 50,
            "violation_rows": 50,
            "reverse_violation_rows": 50,
            "violation_rate": 0.5,
            "equal_rows": 0,
        },
    )
    assert (earlier, later) == ("a_at", "b_at")
    assert tied["direction_tied"] is True
    assert tied["warnings"] == [
        "temporal_direction_inferred_from_observed_data",
        "temporal_direction_is_ambiguous",
    ]


def test_bounded_driver_sets_respect_depth_and_column_caps() -> None:
    assert _bounded_driver_sets(("a", "b", "c"), max_depth=1) == (("a",), ("b",), ("c",))
    assert _bounded_driver_sets(tuple("abcdefghijk"), max_depth=2)[-1] == ("i", "j")


def test_best_effort_analysis_identity_is_unique_per_run() -> None:
    kwargs = {
        "input_artifact_ids": (),
        "source_table": "main.sales.orders",
        "snapshot_kind": "best_effort",
        "source_version": None,
        "snapshot_timestamp": None,
        "configuration_hash": "config-a",
        "rule_set_hash": "rules-a",
        "run_id": "run-a",
        "started_at": "2026-10-22T00:00:00Z",
    }
    assert _analysis_id("", **kwargs) != _analysis_id("", **{**kwargs, "run_id": "run-b"})


@pytest.mark.parametrize("raw", ["true", "TRUE", "1", "yes", "on"])
def test_pattern_entrypoint_accepts_true_boolean_parameters(raw: str) -> None:
    assert _bool_arg(raw) is True


@pytest.mark.parametrize("raw", ["false", "FALSE", "0", "no", "off"])
def test_pattern_entrypoint_accepts_false_boolean_parameters(raw: str) -> None:
    assert _bool_arg(raw) is False


def test_pattern_entrypoint_parses_databricks_boolean_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pattern_detect_spark.py",
            "--source-table",
            "samples.demo.events",
            "--include-spearman",
            "true",
            "--allow-best-effort-snapshot",
            "false",
        ],
    )
    args = parse_args()
    assert args.include_spearman is True
    assert args.allow_best_effort_snapshot is False


def test_pattern_entrypoint_parses_temporal_clock_parameters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pattern_detect_spark.py",
            "--source-table",
            "samples.demo.events",
            "--event-time-column",
            "event_at",
            "--ingestion-time-column",
            "ingested_at",
            "--transition-tie-breakers",
            "event_id,sequence",
        ],
    )
    args = parse_args()
    assert args.event_time_column == "event_at"
    assert args.ingestion_time_column == "ingested_at"
    assert args.transition_tie_breakers == "event_id,sequence"


def test_pattern_entrypoint_parses_stability_slice_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pattern_detect_spark.py",
            "--source-table",
            "samples.demo.events",
            "--stability-slice-columns",
            "event_time,region,source_system",
            "--stability-time-grain",
            "quarter",
            "--stability-min-support-rows",
            "25",
            "--stability-min-slices",
            "4",
            "--stability-max-slices",
            "50",
            "--stability-max-spread",
            "0.15",
        ],
    )
    args = parse_args()
    assert args.stability_slice_columns == "event_time,region,source_system"
    assert args.stability_time_grain == "quarter"
    assert args.stability_min_support_rows == 25
    assert args.stability_min_slices == 4
    assert args.stability_max_slices == 50
    assert args.stability_max_spread == 0.15


def test_pattern_entrypoint_requires_source_table(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["pattern_detect_spark.py"])
    with pytest.raises(SystemExit):
        parse_args()


def test_main_rejects_upstream_ids_without_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    class _SparkSession:
        @staticmethod
        def getActiveSession() -> object:
            return object()

    fake_pyspark = types.ModuleType("pyspark")
    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.SparkSession = _SparkSession
    fake_pyspark.sql = fake_sql
    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pattern_detect_spark.py",
            "--source-table",
            "samples.demo.events",
            "--metadata-artifact-id",
            "metadata-id",
        ],
    )
    from sda.job_entrypoints import pattern_detect_spark

    with pytest.raises(SystemExit, match="artifact registry table"):
        pattern_detect_spark.main()


def test_main_rejects_partial_upstream_artifact_set(monkeypatch: pytest.MonkeyPatch) -> None:
    class _SparkSession:
        @staticmethod
        def getActiveSession() -> object:
            return object()

    fake_pyspark = types.ModuleType("pyspark")
    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.SparkSession = _SparkSession
    fake_pyspark.sql = fake_sql
    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pattern_detect_spark.py",
            "--source-table",
            "samples.demo.events",
            "--metadata-artifact-id",
            "metadata-id",
            "--artifact-registry-table",
            "workspace.profiles.artifact_registry",
        ],
    )
    from sda.job_entrypoints import pattern_detect_spark

    with pytest.raises(SystemExit, match="complete upstream artifact set"):
        pattern_detect_spark.main()
