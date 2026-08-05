"""Dependency graph, deterministic ordering, and cycle detection."""

from __future__ import annotations

from collections import defaultdict, deque


class DependencyGraph:
    def __init__(self) -> None:
        self.edges: dict[str, set[str]] = defaultdict(set)

    def add_edge(self, parent: str, child: str) -> None:
        self.edges[parent].add(child)
        self.edges.setdefault(child, set())

    def add_node(self, node: str) -> None:
        self.edges.setdefault(node, set())

    def isolates(self) -> tuple[str, ...]:
        incoming = {child for children in self.edges.values() for child in children}
        return tuple(
            sorted(node for node in self.edges if not self.edges[node] and node not in incoming)
        )

    def self_references(self) -> tuple[str, ...]:
        return tuple(sorted(node for node, children in self.edges.items() if node in children))

    def components(self) -> tuple[tuple[str, ...], ...]:
        undirected: dict[str, set[str]] = {node: set() for node in self.edges}
        for parent, children in self.edges.items():
            for child in children:
                undirected.setdefault(child, set()).add(parent)
                undirected[parent].add(child)
        seen: set[str] = set()
        result: list[tuple[str, ...]] = []
        for start in sorted(undirected):
            if start in seen:
                continue
            pending = [start]
            component: set[str] = set()
            while pending:
                node = pending.pop()
                if node in component:
                    continue
                component.add(node)
                pending.extend(undirected[node] - component)
            seen.update(component)
            result.append(tuple(sorted(component)))
        return tuple(result)

    def dependency_levels(self) -> dict[str, int]:
        levels = {node: 0 for node in self.edges}
        for node in self.topological_order():
            for child in self.edges[node]:
                levels[child] = max(levels[child], levels[node] + 1)
        return dict(sorted(levels.items()))

    def topological_order(self) -> tuple[str, ...]:
        indegree = {n: 0 for n in self.edges}
        for children in self.edges.values():
            for child in children:
                indegree[child] += 1
        q = deque(sorted(n for n, d in indegree.items() if d == 0))
        result = []
        while q:
            n = q.popleft()
            result.append(n)
            for child in sorted(self.edges[n]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    q.append(child)
        return tuple(result)

    def cycles(self) -> tuple[tuple[str, ...], ...]:
        index = 0
        indices: dict[str, int] = {}
        lowlink: dict[str, int] = {}
        stack: list[str] = []
        on_stack: set[str] = set()
        components: list[tuple[str, ...]] = []

        def visit(node: str) -> None:
            nonlocal index
            indices[node] = lowlink[node] = index
            index += 1
            stack.append(node)
            on_stack.add(node)
            for child in sorted(self.edges[node]):
                if child not in indices:
                    visit(child)
                    lowlink[node] = min(lowlink[node], lowlink[child])
                elif child in on_stack:
                    lowlink[node] = min(lowlink[node], indices[child])
            if lowlink[node] == indices[node]:
                component: list[str] = []
                while True:
                    child = stack.pop()
                    on_stack.remove(child)
                    component.append(child)
                    if child == node:
                        break
                if len(component) > 1 or node in self.edges[node]:
                    components.append(tuple(sorted(component)))

        for node in sorted(self.edges):
            if node not in indices:
                visit(node)
        return tuple(sorted(components))
