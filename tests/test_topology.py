from __future__ import annotations

import pytest

from sda.topology import (
    GraphKind,
    TopologyError,
    TopologyPlan,
    TopologyResult,
    generate_topology,
    manifest_for_topology,
    validate_topology,
)


def test_topology_preserves_nodes_and_isolates() -> None:
    plan = TopologyPlan("g", "fp", node_count=4, edge_count=2, kind=GraphKind.UNDIRECTED)
    result = generate_topology(plan)
    assert len(result.nodes) == 4
    assert len(result.edges) == 2
    assert result.metrics["isolate_count"] >= 0
    assert len({edge["edge_id"] for edge in result.edges}) == 2


def test_topology_is_reproducible() -> None:
    plan = TopologyPlan("g", "fp", node_count=5, edge_count=4)
    result = generate_topology(plan)
    assert result == generate_topology(plan)
    with pytest.raises(TypeError, match="immutable"):
        result.nodes[0]["node_id"] = "changed"  # type: ignore[index]
    assert result.output_fingerprint == generate_topology(plan).output_fingerprint


def test_topology_manifest_binds_plan_and_result() -> None:
    plan = TopologyPlan("g", "fp", node_count=3, edge_count=2)
    result = generate_topology(plan)
    manifest = manifest_for_topology(plan, result)
    assert manifest.output_fingerprint == result.output_fingerprint
    assert manifest.to_dict()["schema_version"] == "topology-manifest-v1"


def test_topology_rejects_impossible_simple_graph() -> None:
    with pytest.raises(ValueError, match="capacity"):
        TopologyPlan("g", "fp", node_count=2, edge_count=3, kind=GraphKind.UNDIRECTED)
    with pytest.raises(ValueError, match="max_degree capacity"):
        generate_topology(
            TopologyPlan(
                "g", "fp", node_count=3, edge_count=2, kind=GraphKind.UNDIRECTED, max_degree=1
            )
        )


def test_empty_topology_is_valid() -> None:
    plan = TopologyPlan("empty", "fp", node_count=0, edge_count=0)
    result = generate_topology(plan)
    assert result.nodes == ()
    assert result.edges == ()
    validate_topology(plan, result)


def test_undirected_self_loop_capacity_uses_unique_pairs() -> None:
    plan = TopologyPlan(
        "loops", "fp", node_count=2, edge_count=3, kind=GraphKind.UNDIRECTED, allow_self_loops=True
    )
    result = generate_topology(plan)
    assert len(result.edges) == 3


def test_directed_degree_limits_and_dag_constraint_are_enforced() -> None:
    dag = TopologyPlan("dag", "fp", node_count=5, edge_count=4, acyclic=True, max_in_degree=1)
    result = generate_topology(dag)
    incoming = {node["node_id"]: 0 for node in result.nodes}
    for edge in result.edges:
        incoming[edge["target"]] += 1
    assert max(incoming.values()) <= 1
    for edge in result.edges:
        assert edge["source"] != edge["target"]


def test_topology_rejects_acyclic_self_loops() -> None:
    with pytest.raises(ValueError, match="self-loops"):
        TopologyPlan("g", "fp", node_count=2, edge_count=1, acyclic=True, allow_self_loops=True)


def test_topology_result_validator_rejects_unknown_endpoints() -> None:
    plan = TopologyPlan("g", "fp", node_count=2, edge_count=1)
    result = generate_topology(plan)
    corrupted = type(result)(
        result.nodes, ({**result.edges[0], "target": "unknown"},), result.metrics
    )
    with pytest.raises(TopologyError, match="unknown endpoint"):
        validate_topology(plan, corrupted)


def test_topology_validator_rejects_inconsistent_metrics() -> None:
    plan = TopologyPlan("g", "fp", node_count=2, edge_count=1)
    result = generate_topology(plan)
    corrupted = type(result)(result.nodes, result.edges, {**result.metrics, "edge_count": 0})
    with pytest.raises(TopologyError, match="metrics"):
        validate_topology(plan, corrupted)


def test_topology_validator_rejects_duplicate_edge_ids() -> None:
    plan = TopologyPlan("g", "fp", node_count=3, edge_count=2)
    result = generate_topology(plan)
    corrupted = type(result)(
        result.nodes,
        (result.edges[0], {**result.edges[1], "edge_id": result.edges[0]["edge_id"]}),
        result.metrics,
    )
    with pytest.raises(TopologyError, match="edge IDs"):
        validate_topology(plan, corrupted)


def test_bipartite_topology_uses_opposite_node_partitions() -> None:
    plan = TopologyPlan("b", "fp", node_count=4, edge_count=3, bipartite=True)
    result = generate_topology(plan)
    indexes = {node["node_id"]: node["node_index"] for node in result.nodes}
    assert all(indexes[edge["source"]] % 2 != indexes[edge["target"]] % 2 for edge in result.edges)


def test_topology_validator_accepts_acyclic_undirected_tree() -> None:
    plan = TopologyPlan(
        "tree", "fp", node_count=3, edge_count=2, kind=GraphKind.UNDIRECTED, acyclic=True
    )
    result = generate_topology(plan)
    validate_topology(plan, result)
    assert result.to_dict()["output_fingerprint"] == result.output_fingerprint


def test_topology_validator_checks_directed_in_and_out_degree_limits() -> None:
    plan = TopologyPlan("g", "fp", node_count=3, edge_count=2, max_in_degree=1)
    result = generate_topology(plan)
    corrupted = type(result)(
        result.nodes,
        ({**result.edges[0], "target": result.edges[1]["target"]}, result.edges[1]),
        result.metrics,
    )
    with pytest.raises(TopologyError, match="max_in_degree"):
        validate_topology(plan, corrupted)


def test_directed_max_degree_counts_both_endpoints() -> None:
    with pytest.raises(ValueError, match="max_degree capacity"):
        generate_topology(TopologyPlan("g", "fp", node_count=3, edge_count=2, max_degree=1))


def test_topology_plan_rejects_degree_capacity_before_generation() -> None:
    with pytest.raises(ValueError, match="max_in_degree capacity"):
        TopologyPlan("g", "fp", node_count=3, edge_count=4, max_in_degree=1)
    with pytest.raises(ValueError, match="max_degree capacity"):
        TopologyPlan("g", "fp", node_count=3, edge_count=2, max_degree=1)


def test_topology_plan_normalizes_graph_kind_strings() -> None:
    plan = TopologyPlan("g", "fp", node_count=2, edge_count=1, kind="directed")
    assert plan.kind is GraphKind.DIRECTED
    with pytest.raises(ValueError, match="unsupported graph kind"):
        TopologyPlan("g", "fp", node_count=2, edge_count=1, kind="unknown")


def test_topology_result_recursively_freezes_nested_attributes() -> None:
    result = TopologyResult(
        ({"node_id": "n", "labels": ["a", {"sensitive": False}]},),
        (),
        {},
    )
    with pytest.raises(TypeError, match="immutable"):
        result.nodes[0]["labels"][1]["sensitive"] = True  # type: ignore[index]
