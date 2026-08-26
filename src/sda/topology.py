"""Small deterministic topology generator and structural metrics."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from sda.artifacts.fingerprint import fingerprint
from sda.operations import ResourceBudget, enforce_budget


class GraphKind(StrEnum):
    DIRECTED = "directed"
    UNDIRECTED = "undirected"


class TopologyError(ValueError):
    """Raised when topology targets cannot be realized safely."""


class _FrozenDict(dict[str, Any]):
    def _immutable(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("topology result is immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _immutable  # type: ignore[assignment]


@dataclass(frozen=True, slots=True)
class TopologyPlan:
    topology_id: str
    plan_fingerprint: str
    node_count: int
    edge_count: int
    kind: GraphKind = GraphKind.DIRECTED
    allow_self_loops: bool = False
    allow_parallel_edges: bool = False
    max_degree: int | None = None
    max_in_degree: int | None = None
    max_out_degree: int | None = None
    acyclic: bool = False

    def __post_init__(self) -> None:
        if not self.topology_id.strip() or not self.plan_fingerprint.strip():
            raise ValueError("topology identity and plan fingerprint are required")
        if self.node_count < 0 or self.edge_count < 0:
            raise ValueError("node_count and edge_count must not be negative")
        capacity = (
            self.node_count * self.node_count
            if self.allow_self_loops
            else self.node_count * max(self.node_count - 1, 0)
        )
        if not self.allow_parallel_edges and self.edge_count > (
            capacity if self.kind is GraphKind.DIRECTED else capacity // 2
        ):
            raise ValueError("edge_count exceeds simple graph capacity")
        if self.max_degree is not None and self.max_degree < 0:
            raise ValueError("max_degree must not be negative")
        if self.max_in_degree is not None and self.max_in_degree < 0:
            raise ValueError("max_in_degree must not be negative")
        if self.max_out_degree is not None and self.max_out_degree < 0:
            raise ValueError("max_out_degree must not be negative")
        if self.acyclic and self.allow_self_loops:
            raise ValueError("acyclic graphs cannot allow self-loops")


@dataclass(frozen=True, slots=True)
class TopologyResult:
    nodes: tuple[dict[str, Any], ...]
    edges: tuple[dict[str, Any], ...]
    metrics: dict[str, int | float]

    def __post_init__(self) -> None:
        object.__setattr__(self, "nodes", tuple(_FrozenDict(node) for node in self.nodes))
        object.__setattr__(self, "edges", tuple(_FrozenDict(edge) for edge in self.edges))
        object.__setattr__(self, "metrics", _FrozenDict(self.metrics))

    @property
    def output_fingerprint(self) -> str:
        """Stable identity for the generated structure, excluding mutable wrappers."""
        return fingerprint({"nodes": self.nodes, "edges": self.edges, "metrics": self.metrics})


def generate_topology(plan: TopologyPlan) -> TopologyResult:
    enforce_budget(ResourceBudget(max_edges=plan.edge_count), edges=plan.edge_count)
    nodes = tuple(
        {"node_id": _node_id(plan, index), "node_index": index} for index in range(plan.node_count)
    )
    ids = [node["node_id"] for node in nodes]
    edges: list[dict[str, Any]] = []
    used: set[tuple[int, int]] = set()
    degree = [0] * plan.node_count
    in_degree = [0] * plan.node_count
    out_degree = [0] * plan.node_count
    adjacency: list[set[int]] = [set() for _ in range(plan.node_count)]
    cursor = 0
    attempts = 0
    max_attempts = max(plan.edge_count * max(plan.node_count, 1) * 2, 1)
    while len(edges) < plan.edge_count and attempts < max_attempts:
        source = cursor % max(plan.node_count, 1)
        target = (cursor + 1 + (cursor // max(plan.node_count, 1))) % max(plan.node_count, 1)
        cursor += 1
        attempts += 1
        if plan.node_count == 0 or (not plan.allow_self_loops and source == target):
            continue
        pair = (source, target)
        canonical: tuple[int, int] = (
            pair if plan.kind is GraphKind.DIRECTED else (min(pair), max(pair))
        )
        if not plan.allow_parallel_edges and canonical in used:
            continue
        if plan.max_degree is not None and (
            degree[source] >= plan.max_degree or degree[target] >= plan.max_degree
        ):
            continue
        if plan.kind is GraphKind.DIRECTED and (
            (plan.max_out_degree is not None and out_degree[source] >= plan.max_out_degree)
            or (plan.max_in_degree is not None and in_degree[target] >= plan.max_in_degree)
        ):
            continue
        if plan.acyclic and _reaches(adjacency, target, source):
            continue
        used.add(canonical)
        degree[source] += 1
        if plan.kind is GraphKind.UNDIRECTED:
            degree[target] += 1
        else:
            in_degree[target] += 1
            out_degree[source] += 1
        adjacency[source].add(target)
        if plan.kind is GraphKind.UNDIRECTED:
            adjacency[target].add(source)
        edges.append(
            {"edge_id": _edge_id(plan, len(edges)), "source": ids[source], "target": ids[target]}
        )
    if len(edges) != plan.edge_count:
        raise TopologyError(f"could only realize {len(edges)} of {plan.edge_count} requested edges")
    components = _component_count(plan, edges)
    return TopologyResult(
        nodes,
        tuple(edges),
        {
            "node_count": len(nodes),
            "edge_count": len(edges),
            "isolate_count": degree.count(0),
            "component_count": components,
        },
    )


def _reaches(adjacency: list[set[int]], start: int, target: int) -> bool:
    pending = [start]
    visited: set[int] = set()
    while pending:
        current = pending.pop()
        if current == target:
            return True
        if current in visited:
            continue
        visited.add(current)
        pending.extend(adjacency[current] - visited)
    return False


def _node_id(plan: TopologyPlan, index: int) -> str:
    return hashlib.sha256(f"{plan.plan_fingerprint}|node|{index}".encode()).hexdigest()[:24]


def _edge_id(plan: TopologyPlan, index: int) -> str:
    return hashlib.sha256(f"{plan.plan_fingerprint}|edge|{index}".encode()).hexdigest()[:24]


def _component_count(plan: TopologyPlan, edges: list[dict[str, Any]]) -> int:
    parent = list(range(plan.node_count))

    def find(value: int) -> int:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    ids = [_node_id(plan, index) for index in range(plan.node_count)]
    lookup = {value: index for index, value in enumerate(ids)}
    for edge in edges:
        left, right = find(lookup[edge["source"]]), find(lookup[edge["target"]])
        parent[left] = right
    return len({find(index) for index in range(plan.node_count)})
