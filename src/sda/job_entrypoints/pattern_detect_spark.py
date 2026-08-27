"""Thin Databricks entrypoint for SDA 07 pattern detection."""

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import UTC, datetime

from sda.patterns.detector import PatternDetector
from sda.patterns.models import PatternConfig
from sda.patterns.roles import assign_roles
from sda.runtime.identifiers import QualifiedName


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
    p.add_argument("--selected-tables", default="")
    p.add_argument("--min-support-rows", type=int, default=100)
    p.add_argument("--min-support-rate", type=float, default=0.001)
    p.add_argument("--sample-fraction", type=float, default=0.1)
    p.add_argument("--sample-seed", type=int, default=1729)
    p.add_argument("--max-rows-scanned", type=int, default=100_000)
    p.add_argument("--include-spearman", nargs="?", const=True, default=False, type=_bool_arg)
    p.add_argument("--allow-best-effort-snapshot", nargs="?", const=True, default=False, type=_bool_arg)
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
    from sda.artifacts.models import ArtifactRef, ArtifactType
    from sda.artifacts.registry import SparkArtifactRegistry
    from sda.patterns.loaders import load_pattern_inputs
    from sda.patterns.models import PatternInputRefs

    upstream_refs: tuple[ArtifactRef, ...] = ()

    if input_ids and not args.artifact_registry_table:
        raise SystemExit("pattern runs require an artifact registry table")
    if args.artifact_registry_table and input_ids:
        registry = SparkArtifactRegistry(spark, args.artifact_registry_table)
        # Use the same fail-closed input contract as the local coordinator.
        # Legacy relationship/graph registries remain an explicit compatibility
        # fallback for deployments that have not migrated those artifacts.
        if all((args.metadata_artifact_id, args.relationship_artifact_id,
                args.dependency_graph_artifact_id)) and profile_ids:
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
                )
            except Exception as exc:
                if not (args.relationship_registry_table or args.dependency_graph_registry_table):
                    raise SystemExit(f"unable to validate pattern inputs: {exc}") from exc
        expected_types = (
            (ArtifactType.METADATA_INVENTORY,) if args.metadata_artifact_id else ()
        ) + tuple(ArtifactType.TABLE_PROFILE for _ in profile_ids) + (
            (ArtifactType.RELATIONSHIP_ANALYSIS,) if args.relationship_artifact_id else ()
        ) + (
            (ArtifactType.DEPENDENCY_GRAPH,) if args.dependency_graph_artifact_id else ()
        )
        for artifact_id, expected_type in zip(input_ids, expected_types, strict=True):
            try:
                ref = registry.require_complete(artifact_id)
            except Exception as exc:
                legacy_table = (
                    args.relationship_registry_table
                    if expected_type is ArtifactType.RELATIONSHIP_ANALYSIS
                    else args.dependency_graph_registry_table
                    if expected_type is ArtifactType.DEPENDENCY_GRAPH
                    else ""
                )
                if legacy_table:
                    ref = SparkArtifactRegistry(spark, legacy_table).require_latest_complete(
                        artifact_id
                    )
                else:
                    raise SystemExit(
                        f"unable to resolve upstream artifact {artifact_id!r}: {exc}"
                    ) from exc
            if ref.artifact_type is not expected_type:
                raise SystemExit(
                    f"upstream artifact {artifact_id} has type {ref.artifact_type}, "
                    f"expected {expected_type}"
                )
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
    role_columns = [
        {
            "name": field.name,
            "data_type": field.dataType.simpleString(),
            "sensitivity": (),
        }
        for field in source.schema.fields
    ]
    roles = assign_roles(role_columns)
    upstream_sensitivity: set[str] = set()
    for ref in upstream_refs:
        content: dict[str, object] = ref.content if isinstance(ref.content, dict) else {}
        signals: object = content.get("sensitivity_signals", {})
        if isinstance(signals, dict):
            upstream_sensitivity.update(name for name, value in signals.items() if value)
        profiles = content.get("column_profiles", ())
        for profile in profiles if isinstance(profiles, (list, tuple)) else ():
            if isinstance(profile, dict) and profile.get("sensitivity_signals"):
                upstream_sensitivity.add(str(profile.get("column_name", "")))
    if upstream_sensitivity:
        roles["excluded"] = tuple(sorted(set(roles.get("excluded", ())) | upstream_sensitivity))
    excluded = set(roles.get("excluded", ()))
    numeric = [
        name
        for name, dtype in columns.items()
        if dtype in {"double", "float", "int", "bigint", "decimal"} and name not in excluded
    ]
    detector = PatternDetector(
        PatternConfig(
            mode=args.mode,
            min_support_rows=args.min_support_rows,
            min_support_rate=args.min_support_rate,
            sample_fraction=args.sample_fraction,
            sample_seed=args.sample_seed,
            max_rows_scanned=args.max_rows_scanned,
        )
    )
    from sda.patterns.models import PatternFamily
    from sda.patterns.spark_metrics import (
        spark_conditional_distribution,
        spark_conditional_missingness,
        spark_fanout_by_segment,
        spark_pearson,
        spark_spearman,
        spark_state_transitions,
        spark_temporal_order,
    )

    analysis_id = args.analysis_id or None
    patterns = []
    # Only aggregate rows are collected; source values never leave Spark.
    for index, left in enumerate(numeric[:50]):
        for right in numeric[index + 1 : 50]:
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
                patterns.append(
                    detector._pattern(
                        analysis_id or "pattern-run",
                        source_table,
                        PatternFamily.CORRELATION,
                        (left, right),
                        {},
                        {"outcome": right},
                        int(row["valid_pair_count"]),
                        row,
                    )
                )
            if args.include_spearman:
                spearman_rows = (
                    spark_spearman(source, left, right)
                    .where(F.col("valid_pair_count") >= args.min_support_rows)
                    .limit(1)
                    .collect()
                )
                if spearman_rows:
                    row = spearman_rows[0].asDict()
                    if row["value"] is not None and int(row["valid_pair_count"]) / max(source_count, 1) >= args.min_support_rate:
                        row["association_name"] = "spearman"
                        patterns.append(detector._pattern(
                            analysis_id or "pattern-run", source_table,
                            PatternFamily.CORRELATION, (left, right), {}, {"outcome": right},
                            int(row["valid_pair_count"]), row,
                        ))
    # Execute the remaining table-local families on bounded aggregate results.
    categorical = [
        name
        for name, dtype in columns.items()
        if dtype in {"string", "boolean"} and name not in numeric
    ]
    for driver in categorical[:10]:
        for outcome in categorical[:10]:
            if driver == outcome:
                continue
            rows = (
                spark_conditional_distribution(source, (driver,), outcome)
                .where(F.col("support_rows") >= args.min_support_rows)
                .where(F.col("support_rows") / F.lit(max(source_count, 1)) >= args.min_support_rate)
                .limit(detector.config.max_candidates)
                .collect()
            )
            for row in rows:
                data = row.asDict()
                support = int(data.pop("support_rows"))
                patterns.append(
                    detector._pattern(
                        analysis_id or "pattern-run",
                        source_table,
                        PatternFamily.CONDITIONAL_DISTRIBUTION,
                        (driver, outcome),
                        {driver: data.pop(driver)},
                        {"outcome": outcome},
                        support,
                        data,
                    )
                )
    for outcome in columns:
        for driver in categorical[:10]:
            rows = (
                spark_conditional_missingness(source, (driver,), outcome)
                .where(F.col("support_rows") >= args.min_support_rows)
                .where(F.col("support_rows") / F.lit(max(source_count, 1)) >= args.min_support_rate)
                .where(F.col("null_rate") > 0)
                .limit(detector.config.max_candidates)
                .collect()
            )
            for row in rows:
                data = row.asDict()
                support = int(data.pop("support_rows"))
                condition = {driver: data.pop(driver)}
                patterns.append(
                    detector._pattern(
                        analysis_id or "pattern-run",
                        source_table,
                        PatternFamily.CONDITIONAL_MISSINGNESS,
                        (driver, outcome),
                        condition,
                        {"outcome": outcome},
                        support,
                        data,
                    )
                )
    temporal = [
        name
        for name, dtype in columns.items()
        if "timestamp" in dtype or dtype == "date" or name.endswith(("_at", "_date"))
    ]
    for earlier, later in zip(temporal, temporal[1:], strict=False):
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
                    analysis_id or "pattern-run",
                    source_table,
                    PatternFamily.TEMPORAL_ORDER,
                    (earlier, later),
                    {},
                    {"later": later},
                    support,
                    data,
                )
            )
    state_candidates = [
        name
        for name in categorical
        if name.lower() in {"status", "state", "stage", "lifecycle_state"}
    ]
    entity_candidates = [name for name in columns if name.lower().endswith(("_id", "id"))]
    if state_candidates and entity_candidates and temporal:
        try:
            transition_rows = (
                spark_state_transitions(
                    source,
                    entity_keys=(entity_candidates[0],),
                    state_column=state_candidates[0],
                    event_time=temporal[0],
                )
                .limit(detector.config.max_candidates)
                .collect()
            )
        except ValueError:
            transition_rows = []
        for row in transition_rows:
            data = row.asDict()
            support = int(data.get("count", 0))
            if support >= args.min_support_rows:
                patterns.append(
                    detector._pattern(
                        analysis_id or "pattern-run",
                        source_table,
                        PatternFamily.STATE_TRANSITION,
                        (state_candidates[0],),
                        {"from_state": data.pop("from_state")},
                        {"to_state": data.pop("to_state")},
                        support,
                        data,
                    )
                )
    if all((args.fanout_parent_table, args.fanout_child_table, args.fanout_parent_key,
            args.fanout_child_key, args.fanout_segment)):
        parent = spark.table(QualifiedName.parse(args.fanout_parent_table).quoted)
        child = spark.table(QualifiedName.parse(args.fanout_child_table).quoted)
        fanout_rows = spark_fanout_by_segment(
            parent, child,
            parent_keys=tuple(args.fanout_parent_key.split(",")),
            child_keys=tuple(args.fanout_child_key.split(",")),
            segments=tuple(args.fanout_segment.split(",")),
        ).where(F.col("parent_count") >= args.min_support_rows).collect()
        parent_count = parent.count()
        for row in fanout_rows:
            data = row.asDict()
            support = int(data.pop("parent_count"))
            if support / max(parent_count, 1) < args.min_support_rate:
                continue
            segments = tuple(args.fanout_segment.split(","))
            condition = {segment: data.pop(segment) for segment in segments}
            patterns.append(detector._pattern(
                analysis_id or "pattern-run", source_table, PatternFamily.FANOUT_BY_SEGMENT,
                segments, condition,
                {"child_count": tuple(args.fanout_child_key.split(","))}, support, data,
            ))

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
        from sda.patterns.persistence import evidence_rows, registry_rows

        artifact_id = analysis_id or (
            f"pattern_registry_{fingerprint(input_ids + (source_table,))}"
        )
        artifact = ArtifactRef(
            artifact_id=artifact_id,
            artifact_type=ArtifactType.PATTERN_REGISTRY,
            artifact_schema_version="1.0",
            status=ArtifactStatus.WRITING,
            tool_name="pattern_detector",
            tool_version="sda07-v1",
            strategy_version="spark-pattern-v1",
            run_id=args.run_id or artifact_id,
            environment=args.environment,
            created_at=datetime.now(UTC).isoformat(),
            configuration_hash=detector.config.configuration_hash,
            primary_location=args.output_table,
            related_locations={
                "evidence": args.pattern_evidence_table,
                "registry": args.artifact_registry_table,
            },
            source_references=(SourceReference(
                source_table, "TABLE", snapshot_kind, source_version, snapshot_timestamp, None
            ),),
            checksum=fingerprint([pattern.to_dict() for pattern in patterns]),
            summary="Spark-native pattern registry",
            input_artifact_ids=input_ids,
        )

        persist_artifact_lifecycle(
            spark,
            artifact,
            registry_rows(
                tuple(patterns),
                configuration_hash=detector.config.configuration_hash,
                input_artifact_ids=input_ids,
                source_references=artifact.source_references,
                detector_version=detector.config.detector_version,
                scoring_policy_version=detector.config.scoring_version,
            ),
            evidence_location=args.output_table,
            registry_location=args.artifact_registry_table,
        )
        evidence = evidence_rows(tuple(patterns))
        if evidence:
            persist_distributed_evidence(
                spark,
                spark.createDataFrame(evidence),
                args.pattern_evidence_table,
                analysis_id=artifact_id,
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
