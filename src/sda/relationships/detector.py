"""SDA 06 relationship detector orchestration and versioned artifact."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from sda.artifacts.fingerprint import fingerprint
from sda.relationships.candidates import discover_candidates
from sda.relationships.graph import DependencyGraph
from sda.relationships.metrics import measure_join
from sda.relationships.scoring import RelationshipScoringPolicy, score_relationship


@dataclass(frozen=True, slots=True)
class RelationshipDiscoveryConfig:
    max_composite_key_width: int = 3
    max_candidates_per_table: int = 100
    validation_mode: str = "exact"
    detector_version: str = "sda06-v1"
    max_relationship_candidates: int = 1_000
    max_verified_candidates: int = 1_000
    scoring_policy: RelationshipScoringPolicy = RelationshipScoringPolicy()

    def __post_init__(self) -> None:
        if self.max_composite_key_width < 1:
            raise ValueError("max_composite_key_width must be at least one")
        if self.max_candidates_per_table < 1:
            raise ValueError("max_candidates_per_table must be at least one")
        if self.validation_mode not in {"exact", "approximate", "sampled"}:
            raise ValueError("validation_mode must be exact, approximate, or sampled")
        if not self.detector_version.strip():
            raise ValueError("detector_version must not be empty")
        if self.max_relationship_candidates < 1 or self.max_verified_candidates < 1:
            raise ValueError("relationship budgets must be at least one")


class RelationshipDetector:
    def __init__(self, config: RelationshipDiscoveryConfig | None = None) -> None:
        self.config = config or RelationshipDiscoveryConfig()

    def detect(
        self,
        tables: dict[str, Any],
        rows: dict[str, list[dict[str, Any]]],
        *,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        discovered_candidates = discover_candidates(
            tables,
            rows=rows,
            max_width=self.config.max_composite_key_width,
            max_per_table=self.config.max_candidates_per_table,
        )
        budget_warning = False
        candidates = discovered_candidates
        if len(discovered_candidates) > self.config.max_relationship_candidates:
            candidates = discovered_candidates[: self.config.max_relationship_candidates]
            budget_warning = True
        relationships: list[dict[str, Any]] = []
        graph = DependencyGraph()
        accepted_nodes: set[str] = set()
        for table in tables:
            graph.add_node(table)
        verified = 0
        for c in candidates:
            if verified >= self.config.max_verified_candidates:
                budget_warning = True
                relationships.append(
                    {
                        "child_table": c.child_table,
                        "parent_table": c.parent_table,
                        "decision": "untested",
                        "system_decision": "untested",
                        "validation_status": "not_run",
                        "review_status": "not_reviewed",
                        "origin": c.origin,
                        "warnings": ["verification_budget_reached"],
                    }
                )
                continue
            if c.parent_table not in rows or c.child_table not in rows:
                relationships.append(
                    {
                        "child_table": c.child_table,
                        "parent_table": c.parent_table,
                        "decision": "untestable",
                        "system_decision": "untestable",
                        "validation_status": "unavailable",
                        "review_status": "not_reviewed",
                        "origin": c.origin,
                        "warnings": ["table_data_unavailable"],
                    }
                )
                continue
            metrics = measure_join(
                rows[c.parent_table], rows[c.child_table], c.parent_columns, c.child_columns
            )
            verified += 1
            scored = score_relationship(
                metrics,
                declared=c.origin == "declared",
                hints=c.hints,
                policy=self.config.scoring_policy,
            )
            if metrics.parent_uniqueness_ratio < 1.0:
                scored = {**scored, "decision": "rejected", "confidence_band": "low"}
            if scored["decision"] == "accepted":
                graph.add_edge(c.parent_table, c.child_table)
                accepted_nodes.update((c.parent_table, c.child_table))
            relationships.append(
                {
                    "child_table": c.child_table,
                    "child_columns": list(c.child_columns),
                    "parent_table": c.parent_table,
                    "parent_columns": list(c.parent_columns),
                    "origin": c.origin,
                    "declared_constraint": c.declared_constraint,
                    "validation_status": "complete",
                    "system_decision": scored["decision"],
                    "review_status": (
                        "required" if scored["decision"] == "awaiting_review" else "not_required"
                    ),
                    "review_decision": None,
                    "reviewer_identity": None,
                    "review_decided_at": None,
                    "review_reason": None,
                    "accepted_for_graph": scored["decision"] == "accepted",
                    **metrics.to_dict(),
                    **scored,
                    "generation_direction": f"{c.parent_table} -> {c.child_table}",
                }
            )
        accepted = [r for r in relationships if r.get("decision") == "accepted"]
        order = (
            tuple(node for node in graph.topological_order() if node in accepted_nodes)
            if accepted
            else ()
        )
        parents_by_child: dict[str, set[str]] = {}
        for relationship in accepted:
            parents_by_child.setdefault(relationship["child_table"], set()).add(
                relationship["parent_table"]
            )
        bridge_tables = sorted(
            table for table, parents in parents_by_child.items() if len(parents) >= 2
        )
        cycles = graph.cycles()
        cycle_nodes = list(graph.blocked_by_cycles())
        payload = {
            "tables": sorted(tables),
            "relationships": relationships,
            "configuration": asdict(self.config),
            "candidate_counts": {
                "discovered": len(discovered_candidates),
                "retained": len(candidates),
                "verified": verified,
                "untested": sum(1 for item in relationships if item.get("decision") == "untested"),
            },
            "scoring_policy_version": self.config.scoring_policy.version,
        }
        return {
            "artifact_version": "sda06-relationship-v1",
            "analysis_id": f"relationship_analysis_{fingerprint(payload)}",
            "detector_version": self.config.detector_version,
            "scoring_policy_version": self.config.scoring_policy.version,
            "run_id": run_id,
            "configuration": asdict(self.config),
            "relationships": relationships,
            "candidate_counts": payload["candidate_counts"],
            "generation_order": list(order),
            "dependency_levels": graph.dependency_levels(),
            "isolates": list(graph.isolates()),
            "self_references": list(graph.self_references()),
            "components": [list(component) for component in graph.components()],
            "cycles": [list(c) for c in cycles],
            "blocked_by_cycle": cycle_nodes,
            "unresolved_cycle": bool(cycles),
            "bridge_tables": bridge_tables,
            "warnings": [
                warning
                for warning in (
                    "cyclic_dependencies_detected" if cycles else None,
                    "relationship_budget_reached" if budget_warning else None,
                )
                if warning
            ],
        }
