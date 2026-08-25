from __future__ import annotations

import pytest

from sda.topology import GraphKind, TopologyError, TopologyPlan, generate_topology


def test_topology_preserves_nodes_and_isolates() -> None:
    plan = TopologyPlan("g", "fp", node_count=4, edge_count=2, kind=GraphKind.UNDIRECTED)
    result = generate_topology(plan)
    assert len(result.nodes) == 4
    assert len(result.edges) == 2
    assert result.metrics["isolate_count"] >= 0
    assert len({edge["edge_id"] for edge in result.edges}) == 2


def test_topology_is_reproducible() -> None:
    plan = TopologyPlan("g", "fp", node_count=5, edge_count=4)
    assert generate_topology(plan) == generate_topology(plan)


def test_topology_rejects_impossible_simple_graph() -> None:
    with pytest.raises(ValueError, match="capacity"):
        TopologyPlan("g", "fp", node_count=2, edge_count=3, kind=GraphKind.UNDIRECTED)
    with pytest.raises(TopologyError, match="realize"):
        generate_topology(
            TopologyPlan(
                "g", "fp", node_count=3, edge_count=2, kind=GraphKind.UNDIRECTED, max_degree=1
            )
        )
