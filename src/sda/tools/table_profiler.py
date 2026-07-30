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

from sda.metadata_models import TableMetadata
from sda.models import AgentState, ArtifactRef, RunStage, ToolName, ToolResult
from sda.profile_models import (
    ColumnProfile,
    ColumnProfileKind,
    MetricMethod,
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
            dtype = column.data_type.lower()
            if any(
                token in dtype
                for token in ("int", "long", "short", "float", "double", "decimal", "numeric")
            ):
                aliases[column.name].update(
                    {
                        "min": f"{safe}_min",
                        "max": f"{safe}_max",
                        "mean": f"{safe}_mean",
                        "stddev": f"{safe}_stddev",
                    }
                )
                expressions.extend(
                    (
                        F.min(col).alias(f"{safe}_min"),
                        F.max(col).alias(f"{safe}_max"),
                        F.avg(col).alias(f"{safe}_mean"),
                        F.stddev_pop(col).alias(f"{safe}_stddev"),
                    )
                )
            elif "string" in dtype or "char" in dtype or "varchar" in dtype:
                aliases[column.name]["length_mean"] = f"{safe}_length_mean"
                expressions.append(F.avg(F.length(col)).alias(f"{safe}_length_mean"))
            elif "date" in dtype or "timestamp" in dtype:
                aliases[column.name].update({"min": f"{safe}_min", "max": f"{safe}_max"})
                expressions.extend(
                    (F.min(col).alias(f"{safe}_min"), F.max(col).alias(f"{safe}_max"))
                )
        result = scan.agg(*expressions).first().asDict(recursive=True)
        now = datetime.now(UTC)
        profiles: list[ColumnProfile] = []
        row_count = int(result.get("__row_count") or 0)
        for column in selected:
            names = aliases[column.name]
            null_count = int(result.get(names["null"]) or 0)
            dtype = column.data_type.lower()
            if any(
                token in dtype
                for token in ("int", "long", "short", "float", "double", "decimal", "numeric")
            ):
                kind = ColumnProfileKind.NUMERIC
                metrics = {
                    key: evidence(result.get(alias), method=MetricMethod.EXACT)
                    for key, alias in names.items()
                    if key in {"min", "max", "mean", "stddev"}
                }
            elif "string" in dtype or "char" in dtype or "varchar" in dtype:
                kind = ColumnProfileKind.STRING
                metrics = {"mean_length": evidence(result.get(names["length_mean"]))}
            elif "date" in dtype or "timestamp" in dtype:
                kind = ColumnProfileKind.TEMPORAL
                metrics = {
                    key: evidence(result.get(alias))
                    for key, alias in names.items()
                    if key in {"min", "max"}
                }
            else:
                kind = ColumnProfileKind.UNSUPPORTED
                metrics = {}
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
                    warnings=("unsupported_type_profile",)
                    if kind is ColumnProfileKind.UNSUPPORTED
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
        return TableProfile(
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
            configuration={"source_table": self.request.source_table},
            configuration_hash=self.request.configuration_hash,
            session_timezone=self.session_timezone,
            source_row_count=evidence(
                row_count,
                method=MetricMethod.EXACT
                if self.request.mode is ProfileMode.FULL
                else MetricMethod.APPROXIMATE,
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
            skipped_columns=(),
            column_profiles=tuple(profiles),
            warnings=("source_version_unavailable",) if source_version is None else (),
            agent_summary=self._summary(row_count, profiles),
        )

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
        return ToolResult(
            tool=self.name,
            stage=RunStage.PROFILED,
            artifacts=(artifact,),
            warnings=profile.warnings,
            metrics={"profiled_columns": profile.profiled_column_count},
        )

    @staticmethod
    def _summary(row_count: int, profiles: list[ColumnProfile]) -> str:
        warnings = sum(len(profile.warnings) for profile in profiles)
        return (
            f"Profiled {row_count} rows across {len(profiles)} columns; "
            f"{warnings} column warnings. Measurements are evidence, not business rules."
        )
