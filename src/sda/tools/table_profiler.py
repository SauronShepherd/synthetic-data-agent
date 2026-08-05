"""Deterministic SDA 05 profiler.

The pure row-oriented path is used by local tests and controlled fixtures. The Spark
adapter supplies bounded rows/aggregates from a single approved relation; it never
collects an unrestricted source sample into the profile artifact.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from sda.artifacts.fingerprint import fingerprint
from sda.artifacts.lineage import source_reference_from_profile
from sda.artifacts.models import ArtifactRef as DurableArtifactRef
from sda.artifacts.models import ArtifactStatus, ArtifactType
from sda.metadata_models import TableMetadata
from sda.models import AgentState, ArtifactRef, RunStage, ToolName, ToolResult
from sda.profile_models import (
    ColumnProfile,
    ColumnProfileKind,
    CalculationMethod,
    MetricEvidence,
    MetricMethod,
    PopulationScope,
    ProfileMode,
    TableProfile,
    TableProfileRequest,
    ValueRetentionPolicy,
    sha256_json,
)
from sda.profiling.categorical import categorical_metrics
from sda.profiling.classify import classify_column
from sda.profiling.common import evidence
from sda.profiling.complex_types import complex_metrics
from sda.profiling.conditional_nulls import conditional_null_hint
from sda.profiling.freshness import business_freshness
from sda.profiling.missingness import missing_metrics
from sda.profiling.numeric import numeric_metrics
from sda.profiling.outliers import numeric_outliers
from sda.profiling.strings import string_metrics
from sda.profiling.temporal import temporal_metrics
from sda.version import __version__


def _is_numeric_dtype(dtype: str) -> bool:
    """Recognize numeric Spark types without substring false positives."""
    normalized = dtype.strip().lower()
    return normalized in {
        "byte",
        "short",
        "int",
        "integer",
        "long",
        "bigint",
        "float",
        "double",
        "decimal",
        "numeric",
    } or normalized.startswith(("decimal(", "numeric("))


class TableProfiler:
    name = ToolName.TABLE_PROFILER

    def __init__(
        self,
        request: TableProfileRequest,
        metadata: TableMetadata,
        *,
        session_timezone: str = "UTC",
    ) -> None:
        if metadata.full_name != request.source_table:
            raise ValueError("metadata identity does not match profiling request")
        self.request = request
        self.metadata = metadata
        self.session_timezone = session_timezone

    def profile_rows(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        source_version: str | None = None,
        source_object_type: str | None = None,
    ) -> TableProfile:
        started = datetime.now(UTC)
        materialized = list(rows)
        selected = [
            column
            for column in self.metadata.columns
            if (not self.request.column_allowlist or column.name in self.request.column_allowlist)
            and column.name not in self.request.column_denylist
        ]
        profiles: list[ColumnProfile] = []
        skipped: list[dict[str, Any]] = []
        for column in selected:
            values = [row.get(column.name) for row in materialized]
            kind, classification = classify_column(
                column.data_type,
                values,
                cardinality_threshold=self.request.category_cardinality_threshold,
                uniqueness_threshold=self.request.identifier_uniqueness_threshold,
            )
            missing = missing_metrics(values, self.request.sentinel_candidates.get(column.name, ()))
            total = len(values)
            null_count = int(missing["null_count"])
            policy = (
                self.request.sensitive_value_retention_policy
                if column.tags
                else self.request.value_retention_policy
            )
            metrics: dict[str, Any]
            outliers: tuple[dict[str, Any], ...] = ()
            warnings: list[str] = []
            if kind is ColumnProfileKind.NUMERIC or kind is ColumnProfileKind.IDENTIFIER_LIKE:
                metrics = numeric_metrics(values)
                outliers = numeric_outliers(values)
            elif kind is ColumnProfileKind.CATEGORICAL:
                metrics = categorical_metrics(
                    values,
                    max_values=self.request.max_category_values,
                    redact=policy is not ValueRetentionPolicy.ALLOW_SAFE_VALUES,
                )
            elif kind is ColumnProfileKind.STRING:
                metrics = string_metrics(
                    values, self.request.expected_string_patterns.get(column.name, ())
                )
            elif kind is ColumnProfileKind.TEMPORAL:
                metrics = temporal_metrics(values, started)
            elif kind in {ColumnProfileKind.ARRAY, ColumnProfileKind.MAP, ColumnProfileKind.STRUCT}:
                metrics, complex_warnings = complex_metrics(values, kind.value)
                warnings.extend(complex_warnings)
            else:
                metrics = {"available": False, "reason": "unsupported_or_redacted_type"}
                warnings.append("unsupported_type_profile")
            if self.request.conditional_null_segments and materialized:
                hints = {
                    segment: conditional_null_hint(
                        materialized,
                        column.name,
                        segment,
                    )
                    for segment in self.request.conditional_null_segments
                    if segment != column.name
                }
                if hints:
                    metrics["conditional_nulls"] = hints
            if (
                policy is not ValueRetentionPolicy.ALLOW_SAFE_VALUES
                and kind is ColumnProfileKind.CATEGORICAL
            ):
                warnings.append("category_values_redacted")
            profiles.append(
                ColumnProfile(
                    column_name=column.name,
                    ordinal_position=column.ordinal_position,
                    declared_data_type=column.data_type,
                    declared_nullable=column.nullable,
                    profile_kind=kind,
                    classification_evidence=classification,
                    sensitivity_signals=column.tags,
                    value_retention_policy=policy,
                    row_count_basis=evidence(total),
                    non_null_count=evidence(total - null_count),
                    null_count=evidence(null_count),
                    null_rate=evidence(null_count / total if total else 0.0),
                    metrics={key: evidence(value) for key, value in metrics.items()},
                    outlier_findings=outliers,
                    warnings=tuple(warnings),
                )
            )
        completed = datetime.now(UTC)
        config = self.request.configuration_hash
        fingerprint = sha256_json(
            {
                "source_table": self.request.source_table,
                "source_version": source_version,
                "metadata": self.metadata.to_dict(),
                "configuration_hash": config,
                "tool_version": __version__,
            }
        )
        profile = TableProfile(
            profile_id=fingerprint,
            profile_schema_version="1.0",
            source_table=self.request.source_table,
            source_object_type=source_object_type or self.metadata.object_type.value,
            source_version=source_version,
            snapshot_kind="provided" if source_version else "best_effort",
            snapshot_reproducible=source_version is not None,
            metadata_fingerprint=sha256_json(self.metadata.to_dict()),
            profile_started_at=started.isoformat(),
            profile_completed_at=completed.isoformat(),
            profile_reference_time=started.isoformat(),
            profile_mode=self.request.mode,
            tool_name=self.name.value,
            tool_version=__version__,
            configuration={
                "source_table": self.request.source_table,
                "mode": self.request.mode.value,
                "sample_fraction": self.request.sample_fraction,
                "sample_seed": self.request.sample_seed,
                "column_allowlist": self.request.column_allowlist,
                "column_denylist": self.request.column_denylist,
                "max_category_values": self.request.max_category_values,
                "outlier_methods": self.request.outlier_methods,
                "business_event_column": self.request.business_event_column,
                "conditional_null_segments": self.request.conditional_null_segments,
                "value_retention_policy": self.request.value_retention_policy.value,
            },
            configuration_hash=config,
            session_timezone=self.session_timezone,
            source_row_count=evidence(len(materialized)),
            sample_row_count=evidence(len(materialized)),
            sample_fraction=self.request.sample_fraction,
            sample_seed=self.request.sample_seed,
            sampling_method="deterministic_input_rows",
            column_count=len(self.metadata.columns),
            profiled_column_count=len(profiles),
            skipped_columns=tuple(skipped),
            column_profiles=tuple(profiles),
            warnings=("source_version_unavailable",) if source_version is None else (),
            agent_summary=self._summary(len(materialized), profiles),
        )
        if self.request.business_event_column:
            freshness = business_freshness(
                (row.get(self.request.business_event_column) for row in materialized),
                started,
                self.request.recent_windows_days,
            )
            profile = replace(profile, business_freshness=freshness)
        return profile

    def profile_spark(self, dataframe: Any, *, source_version: str | None = None) -> TableProfile:
        """Profile one Spark DataFrame with one grouped aggregate action.

        Only aggregate results are collected to the driver; source rows and raw
        category values never enter the profile artifact.
        """
        from pyspark.sql import functions as F

        scan = (
            dataframe
            if self.request.mode is ProfileMode.FULL
            else dataframe.sample(
                withReplacement=False,
                fraction=self.request.sample_fraction,
                seed=self.request.sample_seed,
            )
        )
        live_types = {
            field.name: field.dataType.simpleString().lower() for field in dataframe.schema.fields
        }

        selected = [
            column
            for column in self.metadata.columns
            if (not self.request.column_allowlist or column.name in self.request.column_allowlist)
            and column.name not in self.request.column_denylist
        ]
        expressions: list[Any] = [F.count(F.lit(1)).alias("__row_count")]
        aliases: dict[str, dict[str, str]] = {}
        for index, column in enumerate(selected):
            safe = f"c{index}"
            col = F.col(column.name)
            aliases[column.name] = {"null": f"{safe}_null", "distinct": f"{safe}_distinct"}
            expressions.extend(
                (
                    F.sum(F.when(col.isNull(), 1).otherwise(0)).alias(f"{safe}_null"),
                    F.approx_count_distinct(col).alias(f"{safe}_distinct"),
                )
            )
            dtype = live_types.get(column.name, column.data_type.lower())
            if "string" in dtype or "char" in dtype or "varchar" in dtype:
                aliases[column.name].update(
                    {"blank": f"{safe}_blank", "whitespace": f"{safe}_whitespace"}
                )
                expressions.extend(
                    (
                        F.sum(F.when(col == "", 1).otherwise(0)).alias(f"{safe}_blank"),
                        F.sum(
                            F.when(
                                (F.trim(col) == "") & col.isNotNull() & (col != ""), 1
                            ).otherwise(0)
                        ).alias(f"{safe}_whitespace"),
                    )
                )
                patterns = self.request.expected_string_patterns.get(column.name, ())
                aliases[column.name]["patterns"] = f"{safe}_patterns"
                expressions.append(
                    F.array(
                        *[
                            F.sum(F.when(col.rlike(pattern), 1).otherwise(0)).alias(
                                f"{safe}_pattern_{pattern_index}"
                            )
                            for pattern_index, pattern in enumerate(patterns)
                        ]
                    ).alias(f"{safe}_patterns")
                    if patterns
                    else F.lit([]).alias(f"{safe}_patterns")
                )
            if _is_numeric_dtype(dtype):
                aliases[column.name].update(
                    {
                        "min": f"{safe}_min",
                        "max": f"{safe}_max",
                        "mean": f"{safe}_mean",
                        "stddev": f"{safe}_stddev",
                        "skewness": f"{safe}_skewness",
                        "zero": f"{safe}_zero",
                        "positive": f"{safe}_positive",
                        "negative": f"{safe}_negative",
                        "percentiles": f"{safe}_percentiles",
                    }
                )
                expressions.extend(
                    (
                        F.min(col).alias(f"{safe}_min"),
                        F.max(col).alias(f"{safe}_max"),
                        F.avg(col).alias(f"{safe}_mean"),
                        F.stddev_pop(col).alias(f"{safe}_stddev"),
                        F.skewness(col).alias(f"{safe}_skewness"),
                        F.sum(F.when(col == 0, 1).otherwise(0)).alias(f"{safe}_zero"),
                        F.sum(F.when(col > 0, 1).otherwise(0)).alias(f"{safe}_positive"),
                        F.sum(F.when(col < 0, 1).otherwise(0)).alias(f"{safe}_negative"),
                        F.percentile_approx(
                            col,
                            [0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99],
                            self.request.percentile_accuracy,
                        ).alias(f"{safe}_percentiles"),
                    )
                )
                aliases[column.name].update(
                    {
                        "nan": f"{safe}_nan",
                        "positive_infinity": f"{safe}_positive_infinity",
                        "negative_infinity": f"{safe}_negative_infinity",
                    }
                )
                expressions.extend(
                    (
                        F.sum(F.when(F.isnan(col), 1).otherwise(0)).alias(f"{safe}_nan"),
                        F.sum(F.when(col == float("inf"), 1).otherwise(0)).alias(
                            f"{safe}_positive_infinity"
                        ),
                        F.sum(F.when(col == float("-inf"), 1).otherwise(0)).alias(
                            f"{safe}_negative_infinity"
                        ),
                    )
                )
            elif "string" in dtype or "char" in dtype or "varchar" in dtype:
                aliases[column.name].update(
                    {
                        "length_mean": f"{safe}_length_mean",
                        "length_min": f"{safe}_length_min",
                        "length_max": f"{safe}_length_max",
                    }
                )
                expressions.extend(
                    (
                        F.avg(F.length(col)).alias(f"{safe}_length_mean"),
                        F.min(F.length(col)).alias(f"{safe}_length_min"),
                        F.max(F.length(col)).alias(f"{safe}_length_max"),
                    )
                )
            elif "date" in dtype or "timestamp" in dtype:
                aliases[column.name].update(
                    {
                        "min": f"{safe}_min",
                        "max": f"{safe}_max",
                        "future": f"{safe}_future",
                        "midnight": f"{safe}_midnight",
                    }
                )
                expressions.extend(
                    (
                        F.min(col).alias(f"{safe}_min"),
                        F.max(col).alias(f"{safe}_max"),
                        F.sum(F.when(col > F.current_timestamp(), 1).otherwise(0)).alias(
                            f"{safe}_future"
                        ),
                        F.sum(
                            F.when(
                                (F.hour(col) == 0) & (F.minute(col) == 0) & (F.second(col) == 0),
                                1,
                            ).otherwise(0)
                        ).alias(f"{safe}_midnight"),
                    )
                )
        result = scan.agg(*expressions).first().asDict(recursive=True)
        now = datetime.now(UTC)
        profiles: list[ColumnProfile] = []
        row_count = int(result.get("__row_count") or 0)
        for column in selected:
            names = aliases[column.name]
            null_count = int(result.get(names["null"]) or 0)
            dtype = live_types.get(column.name, column.data_type.lower())
            col = F.col(column.name)
            outliers: tuple[dict[str, Any], ...] = ()
            metrics: dict[str, Any]
            if _is_numeric_dtype(dtype):
                kind = ColumnProfileKind.NUMERIC
                metrics = {
                    "approx_distinct_count": evidence(
                        result.get(names["distinct"]),
                        method=MetricMethod.APPROXIMATE,
                        approximation={"algorithm": "approx_count_distinct"},
                    ),
                    **{
                        key: evidence(
                            result.get(alias),
                            method=(
                                MetricMethod.EXACT
                                if self.request.mode is ProfileMode.FULL
                                else MetricMethod.SAMPLED
                            ),
                            sample_fraction=(
                                None
                                if self.request.mode is ProfileMode.FULL
                                else self.request.sample_fraction
                            ),
                        )
                        for key, alias in names.items()
                        if key in {"min", "max", "mean", "stddev", "skewness"}
                    },
                    "nan_count": evidence(
                        result.get(names.get("nan")),
                        method=(
                            MetricMethod.EXACT
                            if self.request.mode is ProfileMode.FULL
                            else MetricMethod.SAMPLED
                        ),
                    ),
                    "positive_infinity_count": evidence(result.get(names.get("positive_infinity"))),
                    "negative_infinity_count": evidence(result.get(names.get("negative_infinity"))),
                    **{
                        f"{key}_rate": evidence(
                            (result.get(names[key]) or 0) / row_count if row_count else 0.0,
                            method=(
                                MetricMethod.EXACT
                                if self.request.mode is ProfileMode.FULL
                                else MetricMethod.SAMPLED
                            ),
                        )
                        for key in ("zero", "positive", "negative")
                    },
                }
                percentile_values = result.get(names["percentiles"])
                if percentile_values:
                    for key, value in zip(
                        ("p01", "p05", "p25", "p50", "p75", "p95", "p99"),
                        percentile_values,
                        strict=True,
                    ):
                        metrics[key] = evidence(
                            value,
                            method=MetricMethod.APPROXIMATE,
                            approximation={
                                "algorithm": "percentile_approx",
                                "accuracy": self.request.percentile_accuracy,
                            },
                        )
                    if "iqr" in self.request.outlier_methods and percentile_values[2] is not None:
                        q1, q3 = percentile_values[2], percentile_values[4]
                        low, high = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
                        outlier_count = scan.where((col < low) | (col > high)).count()
                        outliers = (
                            {
                                "method": "iqr",
                                "thresholds": {"lower": low, "upper": high},
                                "outlier_count": outlier_count,
                                "population_count": row_count,
                                "count_method": "spark_filter_count",
                            },
                        )
            elif "string" in dtype or "char" in dtype or "varchar" in dtype:
                distinct_value = int(result.get(names["distinct"]) or 0)
                kind = (
                    ColumnProfileKind.CATEGORICAL
                    if distinct_value <= self.request.category_cardinality_threshold
                    else ColumnProfileKind.STRING
                )
                metrics = {
                    "cardinality": evidence(
                        distinct_value,
                        method=MetricMethod.APPROXIMATE,
                        approximation={"algorithm": "approx_count_distinct"},
                    ),
                    "mean_length": evidence(result.get(names["length_mean"])),
                    "min_length": evidence(result.get(names["length_min"])),
                    "max_length": evidence(result.get(names["length_max"])),
                    "blank_count": evidence(result.get(names["blank"])),
                    "whitespace_count": evidence(result.get(names["whitespace"])),
                }
                pattern_counts = result.get(names["patterns"]) or []
                patterns = self.request.expected_string_patterns.get(column.name, ())
                if patterns:
                    metrics["pattern_match_rates"] = evidence(
                        {
                            pattern: (int(count or 0) / row_count if row_count else 0.0)
                            for pattern, count in zip(patterns, pattern_counts, strict=True)
                        }
                    )
                if kind is ColumnProfileKind.CATEGORICAL:
                    policy = (
                        self.request.sensitive_value_retention_policy
                        if column.tags
                        else self.request.value_retention_policy
                    )
                    top_values = (
                        scan.where(col.isNotNull())
                        .groupBy(col)
                        .count()
                        .orderBy(F.desc("count"))
                        .limit(self.request.max_category_values)
                        .collect()
                    )
                    retained_non_null = sum(int(row["count"]) for row in top_values)
                    full_non_null = row_count - null_count
                    metrics.update(
                        {
                            "top_values": [
                                {
                                    "rank": rank,
                                    "value": str(row[0])
                                    if policy is ValueRetentionPolicy.ALLOW_SAFE_VALUES
                                    else None,
                                    "count": int(row["count"]),
                                    "share": (
                                        int(row["count"]) / full_non_null if full_non_null else 0.0
                                    ),
                                }
                                for rank, row in enumerate(top_values, start=1)
                            ],
                            "retained_count": len(top_values),
                            "retained_weight": (
                                retained_non_null / full_non_null if full_non_null else 0.0
                            ),
                            "dominant_share": (
                                int(top_values[0]["count"]) / full_non_null
                                if top_values and full_non_null
                                else 0.0
                            ),
                        }
                    )
            elif "date" in dtype or "timestamp" in dtype:
                kind = ColumnProfileKind.TEMPORAL
                metrics = {
                    key: evidence(result.get(alias))
                    for key, alias in names.items()
                    if key in {"min", "max"}
                }
                metrics.update(
                    {
                        "future_count": evidence(result.get(names["future"])),
                        "midnight_count": evidence(result.get(names["midnight"])),
                    }
                )
                for bucket_name, bucket_expression in (
                    ("month_counts", F.month(col)),
                    ("year_counts", F.year(col)),
                    ("weekday_counts", F.dayofweek(col)),
                    ("hour_counts", F.hour(col)),
                ):
                    bucket_rows = (
                        scan.where(col.isNotNull())
                        .groupBy(bucket_expression.alias("bucket"))
                        .count()
                        .collect()
                    )
                    metrics[bucket_name] = evidence(
                        {str(row["bucket"]): int(row["count"]) for row in bucket_rows}
                    )
            elif dtype in {"boolean", "bool"}:
                kind = ColumnProfileKind.CATEGORICAL
                metrics = {
                    "cardinality": evidence(
                        result.get(names["distinct"]),
                        method=MetricMethod.APPROXIMATE,
                        approximation={"algorithm": "approx_count_distinct"},
                    )
                }
            elif dtype.startswith("array"):
                kind = ColumnProfileKind.ARRAY
                metrics = {
                    "complex_type_evidence": evidence(None, warning="array_values_not_retained")
                }
            elif dtype.startswith("map"):
                kind = ColumnProfileKind.MAP
                metrics = {
                    "complex_type_evidence": evidence(None, warning="map_values_not_retained")
                }
            elif dtype.startswith("struct"):
                kind = ColumnProfileKind.STRUCT
                metrics = {
                    "complex_type_evidence": evidence(None, warning="struct_values_not_retained")
                }
            elif dtype in {"binary", "varbinary"}:
                kind = ColumnProfileKind.BINARY
                metrics = {
                    "complex_type_evidence": evidence(None, warning="binary_values_not_retained")
                }
            elif dtype in {"variant", "json"}:
                kind = ColumnProfileKind.VARIANT
                metrics = {
                    "complex_type_evidence": evidence(None, warning="variant_values_not_retained")
                }
            else:
                kind = ColumnProfileKind.UNSUPPORTED
                metrics = {}
            if self.request.conditional_null_segments:
                conditional: dict[str, Any] = {}
                for segment in self.request.conditional_null_segments:
                    if segment == column.name:
                        continue
                    segment_col = F.col(segment)
                    segment_cardinality = int(
                        scan.select(
                            F.approx_count_distinct(segment_col).alias("cardinality")
                        ).first()["cardinality"]
                        or 0
                    )
                    if segment_cardinality > 50:
                        conditional[segment] = {
                            "available": False,
                            "warning": "conditional_null_segment_too_high_cardinality",
                        }
                        continue
                    groups = (
                        scan.groupBy(segment_col)
                        .agg(
                            F.count(F.lit(1)).alias("row_count"),
                            F.sum(F.when(col.isNull(), 1).otherwise(0)).alias("null_count"),
                        )
                        .orderBy(F.asc_nulls_first(segment))
                        .collect()
                    )
                    segment_metadata = next((c for c in selected if c.name == segment), None)
                    if segment_metadata is None:
                        conditional[segment] = {
                            "available": False,
                            "warning": "conditional_null_segment_not_profiled",
                        }
                        continue
                    segment_policy = (
                        self.request.sensitive_value_retention_policy
                        if segment_metadata.tags
                        else self.request.value_retention_policy
                    )
                    conditional[segment] = {
                        "available": True,
                        "segment_column": segment,
                        "target_column": column.name,
                        "groups": [
                            {
                                "segment": str(group[0])
                                if segment_policy is ValueRetentionPolicy.ALLOW_SAFE_VALUES
                                else None,
                                "row_count": int(group["row_count"]),
                                "null_rate": (
                                    int(group["null_count"]) / int(group["row_count"])
                                    if group["row_count"]
                                    else 0.0
                                ),
                            }
                            for group in groups
                        ],
                    }
                if conditional:
                    metrics["conditional_nulls"] = evidence(conditional)
            metrics = {
                key: value if isinstance(value, MetricEvidence) else evidence(value)
                for key, value in metrics.items()
            }
            if self.request.mode is ProfileMode.QUICK:
                metrics = {
                    key: replace(
                        value,
                        population_scope=PopulationScope.SAMPLE,
                        calculation_method=(
                            CalculationMethod.APPROXIMATE
                            if value.method is MetricMethod.APPROXIMATE
                            else CalculationMethod.EXACT
                        ),
                        sample_fraction=self.request.sample_fraction,
                        sample_seed=self.request.sample_seed,
                    )
                    for key, value in metrics.items()
                }
            profiles.append(
                ColumnProfile(
                    column_name=column.name,
                    ordinal_position=column.ordinal_position,
                    declared_data_type=column.data_type,
                    declared_nullable=column.nullable,
                    profile_kind=kind,
                    classification_evidence=("declared_type",),
                    sensitivity_signals=column.tags,
                    value_retention_policy=self.request.sensitive_value_retention_policy
                    if column.tags
                    else self.request.value_retention_policy,
                    row_count_basis=evidence(row_count),
                    non_null_count=evidence(row_count - null_count),
                    null_count=evidence(null_count),
                    null_rate=evidence(null_count / row_count if row_count else 0.0),
                    metrics=metrics,
                    outlier_findings=outliers,
                    warnings=("unsupported_type_profile",)
                    if kind is ColumnProfileKind.UNSUPPORTED
                    else ("category_values_redacted",)
                    if kind is ColumnProfileKind.CATEGORICAL
                    and (
                        self.request.sensitive_value_retention_policy
                        if column.tags
                        else self.request.value_retention_policy
                    )
                    is not ValueRetentionPolicy.ALLOW_SAFE_VALUES
                    else (),
                )
            )
        fingerprint = sha256_json(
            {
                "source_table": self.request.source_table,
                "source_version": source_version,
                "metadata": self.metadata.to_dict(),
                "configuration_hash": self.request.configuration_hash,
                "tool_version": __version__,
            }
        )
        profile = TableProfile(
            profile_id=fingerprint,
            profile_schema_version="1.0",
            source_table=self.request.source_table,
            source_object_type=self.metadata.object_type.value,
            source_version=source_version,
            snapshot_kind="provided" if source_version else "best_effort",
            snapshot_reproducible=source_version is not None,
            metadata_fingerprint=sha256_json(self.metadata.to_dict()),
            profile_started_at=now.isoformat(),
            profile_completed_at=datetime.now(UTC).isoformat(),
            profile_reference_time=now.isoformat(),
            profile_mode=self.request.mode,
            tool_name=self.name.value,
            tool_version=__version__,
            configuration={
                "source_table": self.request.source_table,
                "mode": self.request.mode.value,
                "sample_fraction": self.request.sample_fraction,
                "sample_seed": self.request.sample_seed,
                "column_allowlist": self.request.column_allowlist,
                "column_denylist": self.request.column_denylist,
                "max_category_values": self.request.max_category_values,
                "percentile_accuracy": self.request.percentile_accuracy,
                "outlier_methods": self.request.outlier_methods,
                "value_retention_policy": self.request.value_retention_policy.value,
                "sensitive_value_retention_policy": (
                    self.request.sensitive_value_retention_policy.value
                ),
            },
            configuration_hash=self.request.configuration_hash,
            session_timezone=self.session_timezone,
            source_row_count=(
                evidence(row_count, method=MetricMethod.EXACT)
                if self.request.mode is ProfileMode.FULL
                else evidence(
                    None,
                    method=MetricMethod.UNAVAILABLE,
                    warning="source_row_count_unavailable_for_sampled_profile",
                )
            ),
            sample_row_count=evidence(
                row_count,
                method=MetricMethod.EXACT
                if self.request.mode is ProfileMode.FULL
                else MetricMethod.SAMPLED,
                sample_fraction=self.request.sample_fraction,
            ),
            sample_fraction=self.request.sample_fraction,
            sample_seed=self.request.sample_seed,
            sampling_method="full_scan"
            if self.request.mode is ProfileMode.FULL
            else "spark_sample",
            column_count=len(self.metadata.columns),
            profiled_column_count=len(profiles),
            skipped_columns=tuple(
                {
                    "column_name": column.name,
                    "reason": (
                        "denylisted"
                        if column.name in self.request.column_denylist
                        else "not_in_allowlist"
                    ),
                }
                for column in self.metadata.columns
                if column not in selected
            ),
            column_profiles=tuple(profiles),
            warnings=("source_version_unavailable",) if source_version is None else (),
            agent_summary=self._summary(row_count, profiles),
        )
        if self.request.business_event_column:
            event_col = F.col(self.request.business_event_column)
            freshness_aliases = {
                str(days): f"recent_{days}" for days in self.request.recent_windows_days
            }
            freshness_row = scan.agg(
                F.max(event_col).alias("max_event_time"),
                *[
                    F.sum(
                        F.when(
                            event_col >= F.current_timestamp() - F.expr(f"INTERVAL {days} DAYS"),
                            1,
                        ).otherwise(0)
                    ).alias(alias)
                    for days, alias in freshness_aliases.items()
                ],
            ).first()
            max_event_time = freshness_row["max_event_time"]
            if max_event_time is not None:
                lag_seconds = max(
                    0.0,
                    (datetime.now(UTC).replace(tzinfo=None) - max_event_time).total_seconds(),
                )
                profile = replace(
                    profile,
                    business_freshness={
                        "available": True,
                        "max_event_time": max_event_time.isoformat(),
                        "lag_seconds": lag_seconds,
                        "recent_window_counts": {
                            days: int(freshness_row[alias] or 0)
                            for days, alias in freshness_aliases.items()
                        },
                        "basis": (
                            "spark_sample"
                            if self.request.mode is ProfileMode.QUICK
                            else "spark_full_scan"
                        ),
                    },
                )
            else:
                profile = replace(
                    profile,
                    business_freshness={
                        "available": False,
                        "warning": "business_event_column_missing_or_unreadable",
                    },
                )
        return profile

    def run(self, state: AgentState, rows: Iterable[Mapping[str, Any]]) -> ToolResult:
        profile = self.profile_rows(rows)
        artifact = ArtifactRef(
            artifact_id=f"{state.request.request_id}:table_profile:{profile.profile_id}",
            artifact_type="table_profile",
            produced_by=self.name,
            summary=profile.agent_summary,
            metadata={
                "profile_id": profile.profile_id,
                "source_table": profile.source_table,
                "column_count": profile.profiled_column_count,
                "warning_count": len(profile.warnings),
            },
        )
        durable_artifact = DurableArtifactRef(
            artifact_id=artifact.artifact_id,
            artifact_type=ArtifactType.TABLE_PROFILE,
            artifact_schema_version=profile.profile_schema_version,
            status=ArtifactStatus.COMPLETE,
            tool_name=self.name.value,
            tool_version=profile.tool_version,
            strategy_version="table-profile-v1",
            run_id=state.request.request_id,
            environment="local",
            created_at=profile.profile_started_at,
            completed_at=profile.profile_completed_at,
            configuration_hash=profile.configuration_hash,
            primary_location=f"table_profile/{artifact.artifact_id}",
            related_locations=profile.artifact_locations,
            input_artifact_ids=(),
            source_references=(source_reference_from_profile(profile),),
            checksum=fingerprint(profile.to_dict()),
            content_checksum=fingerprint(profile.to_dict()),
            summary=profile.agent_summary,
            warnings=profile.warnings,
        )
        return ToolResult(
            tool=self.name,
            stage=RunStage.PROFILED,
            artifacts=(artifact,),
            warnings=profile.warnings,
            metrics={"profiled_columns": profile.profiled_column_count},
            durable_artifacts=(durable_artifact,),
        )

    @staticmethod
    def _summary(row_count: int, profiles: list[ColumnProfile]) -> str:
        warnings = sum(len(profile.warnings) for profile in profiles)
        return (
            f"Profiled {row_count} rows across {len(profiles)} columns; "
            f"{warnings} column warnings. Measurements are evidence, not business rules."
        )
