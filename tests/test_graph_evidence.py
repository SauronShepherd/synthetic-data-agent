from sda.relationships.graph import DependencyGraph


def test_graph_reports_isolates_components_and_levels() -> None:
    graph = DependencyGraph()
    graph.add_node("isolated")
    graph.add_edge("parent", "child")
    graph.add_edge("self", "self")

    assert graph.isolates() == ("isolated",)
    assert graph.self_references() == ("self",)
    assert graph.dependency_levels()["child"] == 1
    assert ("child", "parent") in graph.components()


def test_graph_blocks_cycle_members_and_downstream_nodes() -> None:
    graph = DependencyGraph()
    graph.add_edge("a", "b")
    graph.add_edge("b", "a")
    graph.add_edge("b", "c")

    assert graph.cycles() == (("a", "b"),)
    assert graph.blocked_by_cycles() == ("a", "b", "c")
