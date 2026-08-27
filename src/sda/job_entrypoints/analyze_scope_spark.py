"""Governed scope-analysis entrypoint for the Article 01-06 workflow."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from sda.artifacts.fingerprint import fingerprint
from sda.artifacts.manifest import RunManifest
from sda.runtime.identifiers import QualifiedName
from sda.version import __version__


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--tables", required=True)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--environment", default="dev")
    parser.add_argument("--dry-run", type=lambda value: value.lower() == "true", default=False)
    parser.add_argument("--profile", type=lambda value: value.lower() == "true", default=True)
    parser.add_argument("--parent-table", default="")
    parser.add_argument("--child-table", default="")
    parser.add_argument("--parent-columns", default="")
    parser.add_argument("--child-columns", default="")
    parser.add_argument("--profile-catalog", default="sda_dev")
    parser.add_argument("--profile-schema", default="profiles")
    parser.add_argument("--manifest-table", default="")
    parser.add_argument("--metadata-inventory-id", default="")
    parser.add_argument("--metadata-inventory-table", default="")
    parser.add_argument("--relationship-output-table", default="")
    parser.add_argument("--graph-output-table", default="")
    return parser.parse_args()


def run(spark: Any, args: argparse.Namespace) -> dict[str, Any]:
    tables = tuple(item.strip() for item in re.split(r"[,;|]", args.tables) if item.strip())
    if not tables:
        raise ValueError("--tables must contain at least one table")
    names = tuple(QualifiedName.parse(f"{args.catalog}.{args.schema}.{table}") for table in tables)
    run_id = args.run_id or f"scope-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
    manifest = RunManifest(
        run_id=run_id,
        tool_name="analyze_scope",
        tool_version=__version__,
        artifact_schema_version="1.0",
        environment=getattr(args, "environment", "dev"),
        configuration_hash=fingerprint(
            {"catalog": args.catalog, "schema": args.schema, "tables": tables},
        ),
        status="complete" if args.dry_run else "running",
        started_at=datetime.now(UTC).isoformat(),
        completed_at=datetime.now(UTC).isoformat() if args.dry_run else None,
    )
    metadata_summary: dict[str, Any] = {}
    if not args.dry_run:
        from sda.metadata_models import MetadataReadConfig
        from sda.tools.uc_metadata_reader import (
            InformationSchemaMetadataAdapter,
            SparkSqlExecutor,
        )

        if args.metadata_inventory_id:
            from sda.artifacts.loaders import (
                load_metadata_inventory,
                metadata_inventory_from_payload,
            )

            if not args.metadata_inventory_table:
                raise ValueError("metadata inventory table is required with metadata inventory ID")
            inventory = metadata_inventory_from_payload(
                load_metadata_inventory(
                    spark, args.metadata_inventory_table, args.metadata_inventory_id
                )
            )
        else:
            inventory = InformationSchemaMetadataAdapter(SparkSqlExecutor(spark)).read_inventory(
                MetadataReadConfig(
                    catalog_allowlist=(args.catalog,),
                    schema_allowlist=(args.schema,),
                    table_patterns=tables,
                    max_objects=len(tables),
                )
            )
            if args.metadata_inventory_table:
                inventory_payload = inventory.to_dict()
                args.metadata_inventory_id = f"metadata_inventory_{fingerprint(inventory_payload)}"
                inventory_row = {
                    "inventory_id": args.metadata_inventory_id,
                    "artifact_schema_version": "1.0",
                    "created_at": datetime.now(UTC).isoformat(),
                    "payload": json.dumps(inventory_payload, sort_keys=True),
                }
                spark.createDataFrame([inventory_row]).write.format("delta").mode(
                    "append"
                ).saveAsTable(args.metadata_inventory_table)
        discovered = {table.full_name for table in inventory.tables}
        requested = {name.full_name for name in names}
        missing = sorted(requested - discovered)
        if missing:
            raise RuntimeError(f"requested tables not found in governed metadata: {missing}")
        scoped_tables = tuple(table for table in inventory.tables if table.full_name in requested)
        source_versions: dict[str, str] = {}
        scoped_frames: dict[str, Any] = {}
        for table in scoped_tables:
            qualified = QualifiedName.parse(table.full_name).quoted
            try:
                history = (
                    spark.sql(f"DESCRIBE HISTORY {qualified}")
                    .orderBy("version", ascending=False)
                    .limit(1)
                    .collect()
                )
                if not history:
                    raise RuntimeError("no Delta history available")
                version = str(history[0]["version"])
                source_versions[table.full_name] = version
                scoped_frames[table.full_name] = spark.sql(
                    f"SELECT * FROM {qualified} VERSION AS OF {int(version)}"
                )
            except Exception as exc:
                raise RuntimeError(
                    f"unable to pin governed source snapshot for {table.full_name}"
                ) from exc
        metadata_summary = {
            "metadata_tables": len(inventory.tables),
            "metadata_artifact_status": "complete",
            "metadata_warnings": list(inventory.warnings),
            "metadata_inventory_id": args.metadata_inventory_id,
            "source_versions": source_versions,
        }
        if args.profile:
            from sda.profile_models import ProfileMode, TableProfileRequest
            from sda.profiling.persistence import find_reusable_profile, persist_profile
            from sda.tools.table_profiler import TableProfiler

            profiled: list[dict[str, Any]] = []
            for table in scoped_tables:
                request = TableProfileRequest(
                    source_table=table.full_name,
                    mode=ProfileMode.QUICK,
                    allow_best_effort_snapshot=False,
                )
                profile_target = f"`{args.profile_catalog}`.`{args.profile_schema}`.profile"
                reusable = find_reusable_profile(
                    spark,
                    profile_target,
                    source_table=table.full_name,
                    source_version=source_versions[table.full_name],
                    configuration_hash=request.configuration_hash,
                    metadata_inventory_id=args.metadata_inventory_id or None,
                )
                if reusable:
                    profiled.append(
                        {
                            "source_table": table.full_name,
                            "profile_id": str(reusable.get("profile_id")),
                            "locations": {"table_profiles": f"{profile_target}_table_profiles"},
                            "reused": True,
                        }
                    )
                    continue
                profile = TableProfiler(request, table).profile_spark(
                    scoped_frames[table.full_name],
                    source_version=source_versions[table.full_name],
                )
                if args.metadata_inventory_id:
                    from sda.profile_models import sha256_json

                    profile = replace(
                        profile,
                        profile_id=sha256_json(
                            {
                                "profile_id": profile.profile_id,
                                "metadata_inventory_id": args.metadata_inventory_id,
                            }
                        ),
                        metadata_inventory_id=args.metadata_inventory_id,
                    )
                locations = persist_profile(
                    spark,
                    profile,
                    profile_target,
                    reuse_existing=True,
                )
                profiled.append(
                    {
                        "source_table": table.full_name,
                        "profile_id": profile.profile_id,
                        "locations": locations,
                    }
                )
            metadata_summary["profiled_tables"] = profiled
        if any((args.parent_table, args.child_table, args.parent_columns, args.child_columns)):
            if not all(
                (args.parent_table, args.child_table, args.parent_columns, args.child_columns)
            ):
                raise ValueError("relationship parameters must be supplied together")
            from sda.relationships.spark_metrics import (
                discover_spark_key_candidates,
                measure_spark_join,
            )

            parent = scoped_frames.get(
                args.parent_table, spark.table(QualifiedName.parse(args.parent_table).quoted)
            ).alias("parent")
            child = scoped_frames.get(
                args.child_table, spark.table(QualifiedName.parse(args.child_table).quoted)
            ).alias("child")
            metadata_summary["relationship"] = measure_spark_join(
                parent,
                child,
                tuple(item.strip() for item in args.parent_columns.split(",") if item.strip()),
                tuple(item.strip() for item in args.child_columns.split(",") if item.strip()),
            )
            metadata_summary["candidate_relationships"] = [
                {
                    "parent_table": args.parent_table,
                    "child_table": args.child_table,
                    "parent_columns": [
                        item.strip() for item in args.parent_columns.split(",") if item.strip()
                    ],
                    "child_columns": [
                        item.strip() for item in args.child_columns.split(",") if item.strip()
                    ],
                    "columns": [
                        item.strip() for item in args.child_columns.split(",") if item.strip()
                    ],
                    "origin": "declared",
                    "evidence": metadata_summary["relationship"],
                    "system_decision": "accepted",
                    "review_status": "not_required",
                    "review_decision": None,
                    "reviewer_identity": None,
                    "review_decided_at": None,
                    "review_reason": None,
                    "reason_codes": [],
                }
            ]
        else:
            from itertools import permutations

            from sda.relationships.spark_metrics import (
                discover_spark_key_candidates,
                measure_spark_join,
            )

            candidates: list[dict[str, Any]] = []
            for parent_table, child_table in permutations(scoped_tables, 2):
                declared = [
                    constraint
                    for constraint in child_table.constraints
                    if str(getattr(constraint.kind, "value", constraint.kind)) == "FOREIGN KEY"
                    and constraint.referenced_table == parent_table.full_name
                    and len(constraint.columns) == len(constraint.referenced_columns)
                ]
                candidate_specs: list[tuple[tuple[str, ...], tuple[str, ...], str]] = [
                    (tuple(item.referenced_columns), tuple(item.columns), "declared")
                    for item in declared
                ]
                if not candidate_specs:
                    inferred = discover_spark_key_candidates(
                        parent_table.columns,
                        child_table.columns,
                        max_width=2,
                        max_candidates=20,
                    )
                    if not inferred:
                        continue
                    candidate_specs.extend(
                        (parent_columns, child_columns, "inferred_spark")
                        for parent_columns, child_columns in inferred
                    )
                for parent_columns, child_columns, origin in candidate_specs:
                    evidence = measure_spark_join(
                        scoped_frames[parent_table.full_name],
                        scoped_frames[child_table.full_name],
                        parent_columns,
                        child_columns,
                    )
                    hard_gates_pass = (
                        float(evidence.get("parent_uniqueness_ratio", 0.0)) >= 1.0
                        and float(evidence.get("orphan_rate", 1.0)) <= 0.05
                        and evidence.get("cardinality") != "parent_key_invalid"
                    )
                    candidates.append(
                        {
                            "parent_table": parent_table.full_name,
                            "child_table": child_table.full_name,
                            "columns": list(child_columns),
                            "parent_columns": list(parent_columns),
                            "child_columns": list(child_columns),
                            "origin": origin,
                            "evidence": evidence,
                            "system_decision": "accepted"
                            if origin == "declared" and hard_gates_pass
                            else "awaiting_review",
                            "review_status": "not_required"
                            if origin == "declared" and hard_gates_pass
                            else "required",
                            "review_decision": None,
                            "reviewer_identity": None,
                            "review_decided_at": None,
                            "review_reason": None,
                            "reason_codes": (
                                [] if hard_gates_pass else ["relationship_hard_gate_failed"]
                            ),
                        }
                    )
            metadata_summary["candidate_relationships"] = candidates
        if args.relationship_output_table:
            from sda.artifacts.delta import persist_artifact_lifecycle
            from sda.artifacts.models import (
                ArtifactRef,
                ArtifactStatus,
                ArtifactType,
                SourceReference,
            )

            relationship_rows = []
            if "relationship" in metadata_summary:
                relationship_rows.append(
                    {"kind": "relationship", **metadata_summary["relationship"]}
                )
            relationship_rows.extend(
                {"kind": "candidate", **candidate}
                for candidate in metadata_summary.get("candidate_relationships", [])
            )
            relationship_identity = {
                "run_id": run_id,
                "scope": [n.full_name for n in names],
                "inventory": args.metadata_inventory_id,
                "source_versions": source_versions,
                "profile_ids": [
                    item.get("profile_id") for item in metadata_summary.get("profiled_tables", [])
                ],
                "thresholds": {"orphan_rate_max": 0.05, "parent_uniqueness_min": 1.0},
                "candidate_policy": {
                    "max_inferred_width": 2,
                    "max_inferred_candidates": 20,
                },
            }
            relationship_id = f"relationship_analysis_{fingerprint(relationship_identity)}"
            relationship_ref = ArtifactRef(
                artifact_id=relationship_id,
                artifact_type=ArtifactType.RELATIONSHIP_ANALYSIS,
                artifact_schema_version="1.0",
                status=ArtifactStatus.WRITING,
                tool_name="relationship_detector",
                tool_version=__version__,
                run_id=run_id,
                environment=getattr(args, "environment", "dev"),
                created_at=datetime.now(UTC).isoformat(),
                configuration_hash=fingerprint(relationship_identity),
                primary_location=args.relationship_output_table,
                related_locations={},
                source_references=tuple(
                    SourceReference(
                        name.full_name,
                        "TABLE",
                        "delta_version",
                        source_versions[name.full_name],
                        None,
                        None,
                        metadata_inventory_id=args.metadata_inventory_id,
                    )
                    for name in names
                ),
                checksum=fingerprint(relationship_rows),
                summary="Scope relationship evidence",
                content={"relationships": relationship_rows},
                input_artifact_ids=tuple(
                    item["profile_id"] for item in metadata_summary.get("profiled_tables", [])
                ),
            )
            completed_relationship = persist_artifact_lifecycle(
                spark,
                relationship_ref,
                relationship_rows or [{"kind": "scope", "tables": tables}],
                evidence_location=args.relationship_output_table,
                registry_location=f"{args.relationship_output_table}_registry",
            )
            metadata_summary["relationship_analysis_id"] = completed_relationship.artifact_id
            if args.graph_output_table:
                graph_identity = {"relationship_analysis_id": relationship_id, "scope": tables}
                graph_id = f"dependency_graph_{fingerprint(graph_identity)}"
                graph_rows = [
                    {"kind": "node", "node": table, "relationship_analysis_id": relationship_id}
                    for table in tables
                ]
                graph_rows.extend(
                    {
                        "kind": "edge",
                        "parent_table": candidate["parent_table"],
                        "child_table": candidate["child_table"],
                        "columns": candidate["columns"],
                        "accepted_for_graph": (
                            candidate.get("origin") == "declared"
                            and candidate.get("evidence", {}).get("cardinality")
                            != "parent_key_invalid"
                            and float(
                                candidate.get("evidence", {}).get("parent_uniqueness_ratio", 0.0)
                            )
                            >= 1.0
                            and float(candidate.get("evidence", {}).get("orphan_rate", 1.0)) <= 0.05
                        ),
                        "relationship_analysis_id": relationship_id,
                    }
                    for candidate in metadata_summary.get("candidate_relationships", [])
                )
                accepted_edges = [
                    candidate
                    for candidate in metadata_summary.get("candidate_relationships", [])
                    if candidate.get("origin") == "declared"
                    and candidate.get("evidence", {}).get("cardinality") != "parent_key_invalid"
                    and float(candidate.get("evidence", {}).get("parent_uniqueness_ratio", 0.0))
                    >= 1.0
                    and float(candidate.get("evidence", {}).get("orphan_rate", 1.0)) <= 0.05
                ]
                review_only_count = sum(
                    1
                    for candidate in metadata_summary.get("candidate_relationships", [])
                    if candidate.get("review_status") == "required"
                )
                parents_by_child: dict[str, set[str]] = {}
                for edge in accepted_edges:
                    parents_by_child.setdefault(edge["child_table"], set()).add(
                        edge["parent_table"]
                    )
                bridge_tables = sorted(
                    table for table, parents in parents_by_child.items() if len(parents) >= 2
                )
                bridge_validation: dict[str, dict[str, Any]] = {}
                for bridge in bridge_tables:
                    bridge_edges = [
                        edge for edge in accepted_edges if edge["child_table"] == bridge
                    ]
                    link_columns = sorted(
                        {column for edge in bridge_edges for column in edge["child_columns"]}
                    )
                    frame = scoped_frames[bridge]
                    total_rows = frame.count()
                    distinct_links = frame.select(*link_columns).dropDuplicates().count()
                    bridge_validation[bridge] = {
                        "link_columns": link_columns,
                        "total_rows": total_rows,
                        "distinct_link_tuples": distinct_links,
                        "duplicate_rate": (
                            (total_rows - distinct_links) / total_rows if total_rows else 0.0
                        ),
                        "pair_unique": total_rows == distinct_links,
                    }
                bridge_tables = [
                    table for table in bridge_tables if bridge_validation[table]["pair_unique"]
                ]
                from sda.relationships.graph import DependencyGraph

                dependency_graph = DependencyGraph()
                for table in tables:
                    dependency_graph.add_node(table)
                for edge in accepted_edges:
                    dependency_graph.add_edge(edge["parent_table"], edge["child_table"])
                cycles = dependency_graph.cycles()
                blocked_by_cycle = list(dependency_graph.blocked_by_cycles())
                graph_rows.append(
                    {
                        "kind": "graph_summary",
                        "isolates": list(dependency_graph.isolates()),
                        "components": [
                            list(component) for component in dependency_graph.components()
                        ],
                        "dependency_levels": dependency_graph.dependency_levels(),
                        "generation_order": list(dependency_graph.topological_order()),
                        "cycles": [list(cycle) for cycle in cycles],
                        "blocked_by_cycle": blocked_by_cycle,
                        "unresolved_cycle": bool(cycles),
                        "self_references": list(dependency_graph.self_references()),
                        "bridge_tables": bridge_tables,
                        "bridge_validation": bridge_validation,
                        "review_only_edge_count": review_only_count,
                        "accepted_edge_count": len(accepted_edges),
                        "relationship_analysis_id": relationship_id,
                    }
                )
                graph_ref = replace(
                    relationship_ref,
                    artifact_id=graph_id,
                    artifact_type=ArtifactType.DEPENDENCY_GRAPH,
                    primary_location=args.graph_output_table,
                    related_locations={"relationship_analysis_id": relationship_id},
                    checksum=fingerprint(graph_rows),
                    summary="Scope dependency graph",
                    content={"graph": graph_rows},
                    input_artifact_ids=(relationship_id,),
                )
                completed_graph = persist_artifact_lifecycle(
                    spark,
                    graph_ref,
                    graph_rows,
                    evidence_location=args.graph_output_table,
                    registry_location=f"{args.graph_output_table}_registry",
                )
                metadata_summary["dependency_graph_id"] = completed_graph.artifact_id
        manifest = replace(
            manifest,
            status="complete",
            completed_at=datetime.now(UTC).isoformat(),
            warning_count=len(inventory.warnings),
            input_artifact_ids=(
                (args.metadata_inventory_id,) if args.metadata_inventory_id else ()
            ),
            output_artifact_ids=tuple(
                item["profile_id"] for item in metadata_summary.get("profiled_tables", [])
            )
            + tuple(
                item
                for item in (
                    metadata_summary.get("relationship_analysis_id"),
                    metadata_summary.get("dependency_graph_id"),
                )
                if item
            ),
        )
    result = {
        "run_id": run_id,
        "scope": [name.full_name for name in names],
        "status": (
            "DRY_RUN"
            if args.dry_run
            else (
                "RELATIONSHIPS_MAPPED"
                if metadata_summary.get("relationship_analysis_id")
                else ("PROFILED" if args.profile else "METADATA_VALIDATED")
            )
        ),
        **metadata_summary,
        "manifest": manifest.to_dict(),
    }
    if getattr(args, "manifest_table", ""):
        from sda.artifacts.delta import persist_run_manifest

        persist_run_manifest(spark, manifest, args.manifest_table)
    elif not args.dry_run:
        raise ValueError("manifest_table is required for non-dry-run scope analysis")
    print(json.dumps(result, sort_keys=True))
    return result


def main() -> None:
    from pyspark.sql import SparkSession

    run(SparkSession.getActiveSession() or SparkSession.builder.getOrCreate(), parse_args())


if __name__ == "__main__":
    main()
