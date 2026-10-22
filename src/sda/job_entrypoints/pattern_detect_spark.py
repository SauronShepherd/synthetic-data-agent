"""Thin Databricks entrypoint for SDA 07 pattern detection."""

from __future__ import annotations

import argparse
import json
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from itertools import combinations
from typing import Any

from sda.patterns.detector import PatternDetector
from sda.patterns.models import PatternConfig
from sda.patterns.roles import assign_roles
from sda.runtime.identifiers import QualifiedName


def _analysis_id(
    requested: str,
    *,
    input_artifact_ids: tuple[str, ...],
    source_table: str,
    snapshot_kind: str,
    source_version: str | None,
    snapshot_timestamp: str | None,
    configuration_hash: str,
    rule_set_hash: str,
    run_id: str,
    started_at: str,
) -> str:
    if requested:
        return requested
    from sda.artifacts.fingerprint import fingerprint

    identity: dict[str, object] = {
        "input_artifact_ids": sorted(input_artifact_ids),
        "source_table": source_table,
        "snapshot_kind": snapshot_kind,
        "source_version": source_version,
        "snapshot_timestamp": snapshot_timestamp,
        "configuration_hash": configuration_hash,
        "rule_set_hash": rule_set_hash,
    }
    if snapshot_kind != "delta_version":
        identity["run_id"] = run_id or started_at
    return f"pattern_analysis_{fingerprint(identity)}"


def _profile_ids(raw: str) -> tuple[str, ...]:
    """Parse JSON IDs plus the escaped single-item form from job parameters."""
    try:
        value = json.loads(raw)
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
            raise ValueError("profile artifact IDs must be a JSON string list")
        return tuple(value)
    except (json.JSONDecodeError, ValueError) as exc:
        cleaned = raw.replace("\\", "").strip()
        if cleaned.startswith("[") and cleaned.endswith("]"):
            item = cleaned[1:-1].strip().strip('"').strip("'")
            if item:
                return (item,)
        raise SystemExit("profile_artifact_ids_json must be a JSON string list") from exc


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--source-table", required=True)
    p.add_argument("--mode", choices=("quick", "full"), default="quick")
    p.add_argument("--analysis-id", default="")
    p.add_argument("--run-id", default="")
    p.add_argument("--environment", default="dev")
    p.add_argument("--output-table", default="")
    p.add_argument("--pattern-evidence-table", default="")
    p.add_argument("--artifact-registry-table", default="")
    p.add_argument("--metadata-artifact-id", default="")
    p.add_argument("--profile-artifact-ids-json", default="[]")
    p.add_argument("--relationship-artifact-id", default="")
    p.add_argument("--relationship-registry-table", default="")
    p.add_argument("--dependency-graph-registry-table", default="")
    p.add_argument("--dependency-graph-artifact-id", default="")
    p.add_argument("--fanout-parent-table", default="")
    p.add_argument("--fanout-child-table", default="")
    p.add_argument("--fanout-parent-key", default="")
    p.add_argument("--fanout-child-key", default="")
    p.add_argument("--fanout-segment", default="")
    p.add_argument("--event-time-column", default="")
    p.add_argument("--ingestion-time-column", default="")
    p.add_argument("--transition-tie-breakers", default="")
    p.add_argument("--stability-slice-columns", default="")
    p.add_argument(
        "--stability-time-grain",
        choices=("day", "week", "month", "quarter", "year"),
        default="month",
    )
    p.add_argument("--stability-min-support-rows", type=int, default=20)
    p.add_argument("--stability-min-slices", type=int, default=3)
    p.add_argument("--stability-max-slices", type=int, default=100)
    p.add_argument("--stability-max-spread", type=float, default=0.1)
    p.add_argument("--selected-tables", default="")
    p.add_argument("--business-rules-json", default="[]")
    p.add_argument("--min-support-rows", type=int, default=100)
    p.add_argument("--min-support-rate", type=float, default=0.001)
    p.add_argument("--sample-fraction", type=float, default=0.1)
    p.add_argument("--sample-seed", type=int, default=1729)
    p.add_argument("--max-rows-scanned", type=int, default=100_000)
    p.add_argument("--include-spearman", nargs="?", const=True, default=False, type=_bool_arg)
    p.add_argument(
        "--allow-best-effort-snapshot", nargs="?", const=True, default=False, type=_bool_arg
    )
    return p.parse_args()


def _bool_arg(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError("expected a boolean value")


def _column_list(raw: str) -> tuple[str, ...]:
    columns = tuple(column.strip() for column in raw.split(",") if column.strip())
    if len(columns) != len(set(columns)):
        raise SystemExit("transition tie-breaker columns must be unique")
    return columns


def _conditional_stability(
    frame: Any,
    *,
    condition: dict[str, Any],
    outcome: str,
    outcome_value: Any = None,
    null_outcome: bool = False,
    global_value: float | None = None,
    slice_columns: tuple[str, ...],
    min_support_rows: int,
    min_slices: int,
    max_slices: int,
    max_spread: float,
    time_grain: str,
) -> dict[str, dict[str, Any]]:
    if not slice_columns:
        return {}
    from sda.patterns.spark_metrics import spark_conditional_slice_rates
    from sda.patterns.stability import stability

    result: dict[str, dict[str, Any]] = {}
    fields = {field.name: field.dataType.__class__.__name__ for field in frame.schema.fields}
    for column in slice_columns:
        temporal = fields[column] in {"DateType", "TimestampType", "TimestampNTZType"}
        dimension = f"{column}:{time_grain}" if temporal else column
        rows = spark_conditional_slice_rates(
            frame,
            condition=condition,
            outcome=outcome,
            slice_column=column,
            outcome_value=outcome_value,
            null_outcome=null_outcome,
            time_grain=time_grain,
            min_support_rows=min_support_rows,
            max_slices=max_slices,
        ).collect()
        truncated = len(rows) > max_slices
        bounded_rows = rows[:max_slices]
        result[dimension] = stability(
            [float(row["rate"]) for row in bounded_rows],
            support_counts=[int(row["support_rows"]) for row in bounded_rows],
            min_support_rows=min_support_rows,
            min_slices=min_slices,
            max_spread=max_spread,
            dimension=dimension,
            truncated=truncated,
            global_value=global_value,
        )
    return result


def _correlation_stability(
    frame: Any,
    *,
    left: str,
    right: str,
    method: str,
    global_value: float | None,
    slice_columns: tuple[str, ...],
    min_support_rows: int,
    min_slices: int,
    max_slices: int,
    max_spread: float,
    time_grain: str,
) -> dict[str, dict[str, Any]]:
    if not slice_columns:
        return {}
    from math import nan

    from sda.patterns.spark_metrics import spark_correlation_by_slice
    from sda.patterns.stability import stability

    result: dict[str, dict[str, Any]] = {}
    fields = {field.name: field.dataType.__class__.__name__ for field in frame.schema.fields}
    for column in slice_columns:
        temporal = fields[column] in {"DateType", "TimestampType", "TimestampNTZType"}
        dimension = f"{column}:{time_grain}" if temporal else column
        rows = spark_correlation_by_slice(
            frame,
            left,
            right,
            slice_column=column,
            method=method,
            time_grain=time_grain,
            min_support_rows=min_support_rows,
            max_slices=max_slices,
        ).collect()
        bounded_rows = rows[:max_slices]
        result[dimension] = stability(
            [float(row["value"]) if row["value"] is not None else nan for row in bounded_rows],
            support_counts=[int(row["valid_pair_count"]) for row in bounded_rows],
            min_support_rows=min_support_rows,
            min_slices=min_slices,
            max_spread=max_spread,
            dimension=dimension,
            truncated=len(rows) > max_slices,
            global_value=global_value,
        )
    return result


def _bounded_driver_sets(
    drivers: Sequence[str], *, max_depth: int, max_columns: int = 10
) -> tuple[tuple[str, ...], ...]:
    bounded = tuple(drivers[:max_columns])
    return tuple(
        driver_set
        for depth in range(1, min(max_depth, len(bounded)) + 1)
        for driver_set in combinations(bounded, depth)
    )


def _candidate_columns(
    columns: Mapping[str, str], roles: Mapping[str, Sequence[str]], excluded: set[str]
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Keep role and sensitivity exclusions in every table-local family."""
    numeric_types = {
        "byte",
        "short",
        "smallint",
        "int",
        "integer",
        "long",
        "bigint",
        "float",
        "double",
    }
    allowed = (set(roles.get("driver", ())) | set(roles.get("outcome", ()))) - excluded
    numeric = [
        name
        for name, dtype in columns.items()
        if name in allowed and (dtype in numeric_types or dtype.startswith("decimal("))
    ]
    categorical = [
        name
        for name, dtype in columns.items()
        if name in allowed and dtype in {"string", "boolean"}
    ]
    temporal = [
        name for name in roles.get("lifecycle", ()) if name in columns and name not in excluded
    ]
    entity = [name for name in roles.get("entity", ()) if name in columns and name not in excluded]
    return numeric, categorical, temporal, entity


def main() -> None:
    from pyspark.sql import SparkSession

    args = parse_args()
    if (
        not 0 < args.sample_fraction <= 1
        or not 0 <= args.min_support_rate <= 1
        or args.min_support_rows < 1
        or args.max_rows_scanned < 1
    ):
        raise SystemExit("invalid pattern resource bounds")
    if args.environment in {"staging", "prod"} and not args.metadata_artifact_id:
        raise SystemExit("controlled pattern runs require metadata artifact id")
    if args.environment in {"staging", "prod"} and (
        not args.profile_artifact_ids_json
        or args.profile_artifact_ids_json == "[]"
        or not args.relationship_artifact_id
        or not args.dependency_graph_artifact_id
        or not args.output_table
        or not args.pattern_evidence_table
        or not args.artifact_registry_table
    ):
        raise SystemExit("controlled pattern runs require all upstream/output artifact locations")
    source_name = QualifiedName.parse(args.source_table)
    source_table = source_name.full_name
    if args.selected_tables:
        selected = {
            QualifiedName.parse(value.strip()).full_name
            for value in args.selected_tables.split(",")
            if value.strip()
        }
        if source_table not in selected:
            raise SystemExit("source table is not included in selected-tables")
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    profile_ids = _profile_ids(args.profile_artifact_ids_json)
    input_ids = tuple(
        value
        for value in (
            args.metadata_artifact_id,
            *profile_ids,
            args.relationship_artifact_id,
            args.dependency_graph_artifact_id,
        )
        if value
    )
    from sda.artifacts.models import ArtifactRef
    from sda.artifacts.registry import SparkArtifactRegistry
    from sda.patterns.loaders import load_pattern_inputs
    from sda.patterns.models import PatternInputRefs

    upstream_refs: tuple[ArtifactRef, ...] = ()

    if input_ids and not args.artifact_registry_table:
        raise SystemExit("pattern runs require an artifact registry table")
    if input_ids and not all(
        (
            args.metadata_artifact_id,
            profile_ids,
            args.relationship_artifact_id,
            args.dependency_graph_artifact_id,
        )
    ):
        raise SystemExit("pattern runs require a complete upstream artifact set")
    if input_ids:
        registry = SparkArtifactRegistry(spark, args.artifact_registry_table)
        try:
            upstream_refs = load_pattern_inputs(
                registry,
                PatternInputRefs(
                    args.metadata_artifact_id,
                    profile_ids,
                    args.relationship_artifact_id,
                    args.dependency_graph_artifact_id,
                ),
                environment=args.environment,
                relationship_legacy=(
                    SparkArtifactRegistry(spark, args.relationship_registry_table)
                    if args.relationship_registry_table
                    else None
                ),
                dependency_graph_legacy=(
                    SparkArtifactRegistry(spark, args.dependency_graph_registry_table)
                    if args.dependency_graph_registry_table
                    else None
                ),
            )
        except Exception as exc:
            raise SystemExit(f"unable to validate pattern inputs: {exc}") from exc
    from sda.artifacts.fingerprint import fingerprint
    from sda.patterns.spark_rules import parse_rules_json

    try:
        business_rules = parse_rules_json(args.business_rules_json)
    except (ValueError, KeyError, TypeError) as exc:
        raise SystemExit(f"invalid business rules: {exc}") from exc
    from pyspark.sql import functions as F

    source_version = None
    snapshot_kind = "best_effort"
    snapshot_timestamp = None
    try:
        history = (
            spark.sql(f"DESCRIBE HISTORY {source_name.quoted}")
            .select("version", "timestamp", "operation")
            .orderBy("version", ascending=False)
            .limit(100)
            .collect()
        )
        if history:
            source_version = str(history[0]["version"])
            snapshot_timestamp = str(history[0]["timestamp"])
            snapshot_kind = "delta_version"
    except Exception as exc:
        if not args.allow_best_effort_snapshot:
            raise SystemExit(
                "reproducible source snapshot unavailable; pass --allow-best-effort-snapshot"
            ) from exc
    if source_version is None and not args.allow_best_effort_snapshot:
        raise SystemExit(
            "reproducible source snapshot unavailable; pass --allow-best-effort-snapshot"
        )
    upstream_versions = {
        reference.source_version
        for ref in upstream_refs
        for reference in ref.source_references
        if reference.full_name == source_table
        and reference.snapshot_kind == "delta_version"
        and reference.source_version
    }
    if source_version is not None and upstream_versions and upstream_versions != {source_version}:
        raise SystemExit(
            "upstream artifacts are not compatible with the source snapshot: "
            f"source version {source_version}, upstream versions {sorted(upstream_versions)}"
        )
    source = (
        spark.sql(f"SELECT * FROM {source_name.quoted} VERSION AS OF {int(source_version)}")
        if source_version is not None
        else spark.table(source_name.quoted)
    )
    if args.mode == "quick" and args.sample_fraction < 1:
        source = source.sample(
            withReplacement=False,
            fraction=args.sample_fraction,
            seed=args.sample_seed,
        )
    source_count = source.count()
    if source_count > args.max_rows_scanned:
        raise SystemExit(f"source rows exceed max_rows_scanned budget ({args.max_rows_scanned})")
    columns = {field.name: field.dataType.simpleString() for field in source.schema.fields}
    transition_event_time = args.event_time_column or ""
    if transition_event_time and transition_event_time not in columns:
        raise SystemExit("event-time column is unavailable in the source schema")
    if args.ingestion_time_column and args.ingestion_time_column not in columns:
        raise SystemExit("ingestion-time column is unavailable in the source schema")
    for label, name in (
        ("event-time", transition_event_time),
        ("ingestion-time", args.ingestion_time_column),
    ):
        if name and not columns[name].lower().startswith(("timestamp", "date")):
            raise SystemExit(f"{label} column must have a date or timestamp type")
    transition_tie_breakers = _column_list(args.transition_tie_breakers)
    stability_slice_columns = _column_list(args.stability_slice_columns)
    unknown_tie_breakers = set(transition_tie_breakers) - set(columns)
    if unknown_tie_breakers:
        raise SystemExit(
            f"transition tie-breaker columns are unavailable: {sorted(unknown_tie_breakers)}"
        )
    if (
        args.stability_min_support_rows < 1
        or args.stability_min_slices < 2
        or args.stability_max_slices < args.stability_min_slices
        or args.stability_max_slices > 1000
        or not 0 <= args.stability_max_spread <= 1
    ):
        raise SystemExit("invalid stability resource bounds")
    if len(stability_slice_columns) > 10:
        raise SystemExit("at most ten stability slice columns may be configured")
    from sda.patterns.upstream import scoped_role_columns, source_constraints

    try:
        role_columns, unprofiled = scoped_role_columns(source_table, columns, upstream_refs)
    except ValueError as exc:
        raise SystemExit(f"unable to scope pattern columns: {exc}") from exc
    roles = assign_roles(role_columns)
    if unprofiled:
        roles["excluded"] = tuple(sorted(set(roles.get("excluded", ())) | unprofiled))
    excluded = set(roles.get("excluded", ()))
    sensitive_columns = {
        str(column["name"])
        for column in role_columns
        if column.get("sensitivity")
        or any(
            token in str(column["name"]).lower() for token in ("email", "ssn", "password", "token")
        )
    }
    unknown_stability_columns = set(stability_slice_columns) - set(columns)
    if unknown_stability_columns:
        raise SystemExit(
            f"stability slice columns are unavailable: {sorted(unknown_stability_columns)}"
        )
    if set(stability_slice_columns) & (sensitive_columns | unprofiled | excluded):
        raise SystemExit(
            "stability slice columns must be profiled, non-sensitive, and eligible for analysis"
        )
    numeric, categorical, temporal, entity_candidates = _candidate_columns(columns, roles, excluded)
    if not transition_event_time and temporal:
        transition_event_time = temporal[0]
    detector = PatternDetector(
        PatternConfig(
            mode=args.mode,
            min_support_rows=args.min_support_rows,
            min_support_rate=args.min_support_rate,
            sample_fraction=args.sample_fraction,
            sample_seed=args.sample_seed,
            max_rows_scanned=args.max_rows_scanned,
            stability_slice_columns=stability_slice_columns,
            stability_min_support_rows=args.stability_min_support_rows,
            stability_min_slices=args.stability_min_slices,
            stability_max_slices=args.stability_max_slices,
            stability_max_spread=args.stability_max_spread,
        )
    )
    analysis_started_at = datetime.now(UTC).isoformat()
    analysis_id = _analysis_id(
        args.analysis_id,
        input_artifact_ids=input_ids,
        source_table=source_table,
        snapshot_kind=snapshot_kind,
        source_version=source_version,
        snapshot_timestamp=snapshot_timestamp,
        configuration_hash=detector.config.configuration_hash,
        rule_set_hash=fingerprint(business_rules),
        run_id=args.run_id,
        started_at=analysis_started_at,
    )
    from sda.patterns.models import Pattern, PatternFamily
    from sda.patterns.platform_constraints import evaluate_platform_constraints
    from sda.patterns.spark_metrics import (
        spark_conditional_distribution,
        spark_conditional_missingness,
        spark_correlation_outlier_diagnostic,
        spark_fanout_by_segment,
        spark_numeric_by_group,
        spark_pearson,
        spark_spearman,
        spark_state_transitions,
        spark_temporal_lag,
        spark_temporal_order,
    )

    patterns: list[Pattern] = []
    # Only aggregate rows are collected; source values never leave Spark.
    try:
        patterns.extend(
            evaluate_platform_constraints(
                source,
                source_constraints(source_table, upstream_refs),
                analysis_id=analysis_id,
                table=source_table,
            )
        )
    except (ValueError, TypeError, KeyError) as exc:
        raise SystemExit(f"unable to evaluate source platform constraints: {exc}") from exc
    correlation_numeric = [
        name
        for name in numeric
        if name in roles.get("driver", ()) and name in roles.get("outcome", ())
    ]
    for index, left in enumerate(correlation_numeric[:50]):
        for right in correlation_numeric[index + 1 : 50]:
            if len(patterns) >= detector.config.max_candidates:
                break
            summary = (
                spark_pearson(source, left, right)
                .where(F.col("valid_pair_count") >= args.min_support_rows)
                .limit(1)
                .collect()
            )
            if (
                summary
                and summary[0]["value"] is not None
                and int(summary[0]["valid_pair_count"] or 0) / max(source_count, 1)
                >= args.min_support_rate
            ):
                row = summary[0].asDict()
                row["stability"] = (
                    _correlation_stability(
                        source,
                        left=left,
                        right=right,
                        method="pearson",
                        global_value=row["value"],
                        slice_columns=stability_slice_columns,
                        min_support_rows=args.stability_min_support_rows,
                        min_slices=args.stability_min_slices,
                        max_slices=args.stability_max_slices,
                        max_spread=args.stability_max_spread,
                        time_grain=args.stability_time_grain,
                    )
                    or "unavailable"
                )
                diagnostic = (
                    spark_correlation_outlier_diagnostic(source, left, right).limit(1).collect()
                )
                if diagnostic:
                    row["correlation_outlier_diagnostic"] = diagnostic[0].asDict()
                patterns.append(
                    detector._pattern(
                        analysis_id,
                        source_table,
                        PatternFamily.CORRELATION,
                        (left, right),
                        {},
                        {"outcome": right},
                        int(row["valid_pair_count"]),
                        row,
                    )
                )
            if args.include_spearman and len(patterns) < detector.config.max_candidates:
                spearman_rows = (
                    spark_spearman(source, left, right)
                    .where(F.col("valid_pair_count") >= args.min_support_rows)
                    .limit(1)
                    .collect()
                )
                if spearman_rows:
                    row = spearman_rows[0].asDict()
                    if (
                        row["value"] is not None
                        and int(row["valid_pair_count"]) / max(source_count, 1)
                        >= args.min_support_rate
                    ):
                        row["association_name"] = "spearman"
                        row["stability"] = (
                            _correlation_stability(
                                source,
                                left=left,
                                right=right,
                                method="spearman",
                                global_value=row["value"],
                                slice_columns=stability_slice_columns,
                                min_support_rows=args.stability_min_support_rows,
                                min_slices=args.stability_min_slices,
                                max_slices=args.stability_max_slices,
                                max_spread=args.stability_max_spread,
                                time_grain=args.stability_time_grain,
                            )
                            or "unavailable"
                        )
                        patterns.append(
                            detector._pattern(
                                analysis_id,
                                source_table,
                                PatternFamily.CORRELATION,
                                (left, right),
                                {},
                                {"outcome": right},
                                int(row["valid_pair_count"]),
                                row,
                            )
                        )
    # Execute the remaining table-local families on bounded aggregate results.
    categorical_drivers = [name for name in categorical if name in roles.get("driver", ())]
    categorical_outcomes = [name for name in categorical if name in roles.get("outcome", ())]
    numeric_outcomes = [name for name in numeric if name in roles.get("outcome", ())]
    driver_sets = _bounded_driver_sets(
        categorical_drivers, max_depth=detector.config.max_condition_depth
    )
    for drivers in driver_sets:
        for outcome in categorical_outcomes[:10]:
            if outcome in drivers or len(patterns) >= detector.config.max_candidates:
                continue
            rows = (
                spark_conditional_distribution(source, drivers, outcome)
                .where(F.col("support_rows") >= args.min_support_rows)
                .where(F.col("support_rows") / F.lit(max(source_count, 1)) >= args.min_support_rate)
                .where(F.col("outcome_count") >= args.min_support_rows)
                .orderBy(
                    F.col("support_rows").desc(),
                    *(F.col(driver).asc_nulls_first() for driver in drivers),
                    F.col(outcome).asc_nulls_first(),
                )
                .limit(max(0, detector.config.max_candidates - len(patterns)))
                .collect()
            )
            for row in rows:
                data = row.asDict()
                condition_rows = int(data.pop("support_rows"))
                support = int(data.pop("outcome_count"))
                outcome_value = data.pop(outcome)
                condition = {driver: data.pop(driver) for driver in drivers}
                global_rate = int(data["global_count"] or 0) / max(source_count, 1)
                condition_rate = support / max(condition_rows, 1)
                slice_stability = _conditional_stability(
                    source,
                    condition=condition,
                    outcome=outcome,
                    outcome_value=outcome_value,
                    slice_columns=stability_slice_columns,
                    min_support_rows=args.stability_min_support_rows,
                    min_slices=args.stability_min_slices,
                    max_slices=args.stability_max_slices,
                    max_spread=args.stability_max_spread,
                    time_grain=args.stability_time_grain,
                    global_value=condition_rate,
                )
                patterns.append(
                    detector._pattern(
                        analysis_id,
                        source_table,
                        PatternFamily.CONDITIONAL_DISTRIBUTION,
                        (*drivers, outcome),
                        condition,
                        {"outcome": outcome, "value": outcome_value},
                        support,
                        {
                            **data,
                            "outcome_count": support,
                            "condition_rows": condition_rows,
                            "global_rate": global_rate,
                            "stability": slice_stability or "unavailable",
                        },
                        support_rate=support / condition_rows,
                    )
                )
        for outcome in numeric_outcomes[:10]:
            if outcome in drivers or len(patterns) >= detector.config.max_candidates:
                continue
            rows = (
                spark_numeric_by_group(source, drivers, outcome)
                .where(F.col("count") >= args.min_support_rows)
                .where(F.col("count") / F.lit(max(source_count, 1)) >= args.min_support_rate)
                .orderBy(
                    F.col("count").desc(),
                    *(F.col(driver).asc_nulls_first() for driver in drivers),
                )
                .limit(max(0, detector.config.max_candidates - len(patterns)))
                .collect()
            )
            for row in rows:
                data = row.asDict()
                support = int(data.pop("count"))
                condition = {driver: data.pop(driver) for driver in drivers}
                patterns.append(
                    detector._pattern(
                        analysis_id,
                        source_table,
                        PatternFamily.CONDITIONAL_DISTRIBUTION,
                        (*drivers, outcome),
                        condition,
                        {"outcome": outcome},
                        support,
                        {
                            **data,
                            "count": support,
                            "method": "exact",
                            "stability": "unavailable",
                        },
                    )
                )
    for outcome in (
        name for name in columns if name not in excluded and name in roles.get("outcome", ())
    ):
        global_null_rate = source.where(
            F.col(f"`{outcome.replace('`', '``')}`").isNull()
        ).count() / max(source_count, 1)
        for drivers in driver_sets:
            if outcome in drivers or len(patterns) >= detector.config.max_candidates:
                continue
            rows = (
                spark_conditional_missingness(source, drivers, outcome)
                .where(F.col("support_rows") >= args.min_support_rows)
                .where(F.col("support_rows") / F.lit(max(source_count, 1)) >= args.min_support_rate)
                .where(F.col("null_rate") > 0)
                .limit(max(0, detector.config.max_candidates - len(patterns)))
                .collect()
            )
            for row in rows:
                data = row.asDict()
                support = int(data.pop("support_rows"))
                condition = {driver: data.pop(driver) for driver in drivers}
                condition_null_rate = float(data["null_rate"])
                slice_stability = _conditional_stability(
                    source,
                    condition=condition,
                    outcome=outcome,
                    null_outcome=True,
                    global_value=condition_null_rate,
                    slice_columns=stability_slice_columns,
                    min_support_rows=args.stability_min_support_rows,
                    min_slices=args.stability_min_slices,
                    max_slices=args.stability_max_slices,
                    max_spread=args.stability_max_spread,
                    time_grain=args.stability_time_grain,
                )
                patterns.append(
                    detector._pattern(
                        analysis_id,
                        source_table,
                        PatternFamily.CONDITIONAL_MISSINGNESS,
                        (*drivers, outcome),
                        condition,
                        {"outcome": outcome},
                        support,
                        {
                            **data,
                            "global_null_rate": global_null_rate,
                            "stability": slice_stability or "unavailable",
                        },
                    )
                )
    for earlier, later in zip(temporal, temporal[1:], strict=False):
        if len(patterns) >= detector.config.max_candidates:
            break
        summary = spark_temporal_order(source, earlier, later).limit(1).collect()
        if (
            summary
            and int(summary[0]["eligible_rows"] or 0) >= args.min_support_rows
            and int(summary[0]["eligible_rows"] or 0) / max(source_count, 1)
            >= args.min_support_rate
        ):
            data = summary[0].asDict()
            support = int(data["eligible_rows"])
            patterns.append(
                detector._pattern(
                    analysis_id,
                    source_table,
                    PatternFamily.TEMPORAL_ORDER,
                    (earlier, later),
                    {},
                    {"later": later},
                    support,
                    data,
                )
            )
            if len(patterns) < detector.config.max_candidates:
                lag = spark_temporal_lag(source, earlier, later).limit(1).collect()
                if lag and int(lag[0]["count"] or 0) >= args.min_support_rows:
                    patterns.append(
                        detector._pattern(
                            analysis_id,
                            source_table,
                            PatternFamily.TEMPORAL_LAG,
                            (earlier, later),
                            {},
                            {"later": later},
                            int(lag[0]["count"]),
                            lag[0].asDict(),
                        )
                    )
    state_candidates = [
        name
        for name in categorical
        if name.lower() in {"status", "state", "stage", "lifecycle_state"}
    ]
    if (
        state_candidates
        and entity_candidates
        and transition_event_time
        and len(patterns) < detector.config.max_candidates
    ):
        tie_breakers = transition_tie_breakers or tuple(
            name
            for name in ("ingested_at", "ingestion_at", "ingestion_time", "event_id")
            if name in columns and name not in {entity_candidates[0], transition_event_time}
        )
        ordering_columns = {
            *tie_breakers,
            transition_event_time,
            *((args.ingestion_time_column,) if args.ingestion_time_column else ()),
        }
        if ordering_columns & unprofiled:
            raise SystemExit("transition ordering columns must have profile evidence")
        if ordering_columns & sensitive_columns:
            raise SystemExit("sensitive columns cannot be used for transition ordering")
        try:
            transition_rows = (
                spark_state_transitions(
                    source,
                    entity_keys=(entity_candidates[0],),
                    state_column=state_candidates[0],
                    event_time=transition_event_time,
                    tie_breakers=tie_breakers,
                    ingestion_time=args.ingestion_time_column or None,
                )
                .limit(max(0, detector.config.max_candidates - len(patterns)))
                .collect()
            )
        except ValueError as exc:
            warnings.warn(f"state transition evidence unavailable: {exc}", stacklevel=1)
            transition_rows = []
        for row in transition_rows:
            data = row.asDict()
            support = int(data.get("count", 0))
            if (
                support >= args.min_support_rows
                and support / max(source_count, 1) >= args.min_support_rate
            ):
                patterns.append(
                    detector._pattern(
                        analysis_id,
                        source_table,
                        PatternFamily.STATE_TRANSITION,
                        (state_candidates[0],),
                        {"from_state": data.pop("from_state")},
                        {"to_state": data.pop("to_state")},
                        support,
                        data,
                    )
                )
    if len(patterns) < detector.config.max_candidates and all(
        (
            args.fanout_parent_table,
            args.fanout_child_table,
            args.fanout_parent_key,
            args.fanout_child_key,
            args.fanout_segment,
        )
    ):
        parent = spark.table(QualifiedName.parse(args.fanout_parent_table).quoted)
        child = spark.table(QualifiedName.parse(args.fanout_child_table).quoted)
        parent_count = parent.count()
        if parent_count > args.max_rows_scanned or child.count() > args.max_rows_scanned:
            raise SystemExit("fanout source rows exceed max_rows_scanned budget")
        segments = tuple(value.strip() for value in args.fanout_segment.split(","))
        if set(segments) & excluded and args.fanout_parent_table == source_table:
            raise SystemExit("fanout segments include excluded columns")
        fanout_rows = (
            spark_fanout_by_segment(
                parent,
                child,
                parent_keys=tuple(args.fanout_parent_key.split(",")),
                child_keys=tuple(args.fanout_child_key.split(",")),
                segments=segments,
            )
            .where(F.col("parent_count") >= args.min_support_rows)
            .limit(max(0, detector.config.max_candidates - len(patterns)))
            .collect()
        )
        for row in fanout_rows:
            data = row.asDict()
            support = int(data.pop("parent_count"))
            if support / max(parent_count, 1) < args.min_support_rate:
                continue
            condition = {segment: data.pop(segment) for segment in segments}
            patterns.append(
                detector._pattern(
                    analysis_id,
                    source_table,
                    PatternFamily.FANOUT_BY_SEGMENT,
                    segments,
                    condition,
                    {"child_count": tuple(args.fanout_child_key.split(","))},
                    support,
                    data,
                    support_rate=support / max(parent_count, 1),
                )
            )

    # Explicit business rules are evaluated as Spark aggregates on the scoped source.
    from datetime import date

    from sda.patterns.spark_rules import detect_spark_rules

    try:
        patterns.extend(
            detect_spark_rules(
                source,
                business_rules,
                table=source_table,
                as_of=date.today(),
                detector=detector,
                analysis_id=analysis_id,
            )
        )
    except (ValueError, KeyError, TypeError) as exc:
        raise SystemExit(f"invalid business rules: {exc}") from exc

    # Multiple families can describe the same finding; persist one content-id row.
    patterns = [
        replace(pattern, support_rate=pattern.support_rows / source_count)
        if pattern.support_rate is None and source_count
        else pattern
        for pattern in {pattern.pattern_id: pattern for pattern in patterns}.values()
    ]
    if args.output_table:
        from sda.artifacts.delta import persist_artifact_lifecycle, persist_distributed_evidence
        from sda.artifacts.fingerprint import fingerprint
        from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType, SourceReference
        from sda.patterns.persistence import (
            ensure_pattern_registry_columns,
            evidence_rows,
            registry_rows,
        )

        artifact_id = analysis_id
        artifact = ArtifactRef(
            artifact_id=artifact_id,
            artifact_type=ArtifactType.PATTERN_REGISTRY,
            artifact_schema_version="1.0",
            status=ArtifactStatus.WRITING,
            tool_name="pattern_detector",
            tool_version=detector.config.detector_version,
            strategy_version=detector.config.scoring_version,
            run_id=args.run_id or artifact_id,
            environment=args.environment,
            created_at=analysis_started_at,
            configuration_hash=detector.config.configuration_hash,
            primary_location=args.output_table,
            related_locations={
                "evidence": args.pattern_evidence_table,
                "registry": args.artifact_registry_table,
            },
            source_references=(
                SourceReference(
                    source_table, "TABLE", snapshot_kind, source_version, snapshot_timestamp, None
                ),
            ),
            checksum=fingerprint([pattern.to_dict() for pattern in patterns]),
            summary="Spark-native pattern registry",
            input_artifact_ids=input_ids,
        )

        rows = registry_rows(
            tuple(patterns),
            configuration_hash=detector.config.configuration_hash,
            input_artifact_ids=input_ids,
            source_references=artifact.source_references,
            detector_version=detector.config.detector_version,
            scoring_policy_version=detector.config.scoring_version,
        )
        ensure_pattern_registry_columns(spark, args.output_table, rows)
        evidence = evidence_rows(tuple(patterns))
        persist_artifact_lifecycle(
            spark,
            artifact,
            rows,
            evidence_location=args.output_table,
            registry_location=args.artifact_registry_table,
            before_complete=(
                lambda: persist_distributed_evidence(
                    spark,
                    spark.createDataFrame(evidence),
                    args.pattern_evidence_table,
                    analysis_id=artifact_id,
                )
            )
            if evidence
            else None,
        )
    print(
        json.dumps(
            {
                "pattern_count": len(patterns),
                "pattern_ids": [p.pattern_id for p in patterns],
                "family_counts": {
                    family: sum(pattern.family.value == family for pattern in patterns)
                    for family in sorted({pattern.family.value for pattern in patterns})
                },
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
