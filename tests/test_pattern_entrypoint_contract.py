from __future__ import annotations

import sys
import types

import pytest

from sda.job_entrypoints.pattern_detect_spark import _analysis_id, _bool_arg, parse_args


def test_analysis_identity_tracks_snapshot_inputs_and_configuration() -> None:
    kwargs = {
        "input_artifact_ids": ("profile-a", "metadata-a"),
        "source_table": "main.sales.orders",
        "snapshot_kind": "delta_version",
        "source_version": "7",
        "snapshot_timestamp": "2026-10-22T00:00:00Z",
        "configuration_hash": "config-a",
        "run_id": "run-a",
        "started_at": "2026-10-22T00:00:00Z",
    }
    first = _analysis_id("", **kwargs)
    assert first == _analysis_id("", **kwargs)
    assert first != _analysis_id("", **{**kwargs, "source_version": "8"})
    assert first != _analysis_id("", **{**kwargs, "configuration_hash": "config-b"})
    assert _analysis_id("requested", **kwargs) == "requested"


def test_best_effort_analysis_identity_is_unique_per_run() -> None:
    kwargs = {
        "input_artifact_ids": (),
        "source_table": "main.sales.orders",
        "snapshot_kind": "best_effort",
        "source_version": None,
        "snapshot_timestamp": None,
        "configuration_hash": "config-a",
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
