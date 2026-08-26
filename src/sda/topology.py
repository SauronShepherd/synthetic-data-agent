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

    def to_dict(self) -> dict[str, Any]:
        """Return a canonical persistence payload with its content fingerprint."""
        return {
            "nodes": tuple(dict(node) for node in self.nodes),
            "edges": tuple(dict(edge) for edge in self.edges),
            "metrics": dict(self.metrics),
            "output_fingerprint": self.output_fingerprint,
        }


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
        degree[target] += 1
        if plan.kind is GraphKind.DIRECTED:
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
    result = TopologyResult(
        nodes,
        tuple(edges),
        {
            "node_count": len(nodes),
            "edge_count": len(edges),
            "isolate_count": degree.count(0),
            "component_count": components,
        },
    )
    validate_topology(plan, result)
    return result


def validate_topology(plan: TopologyPlan, result: TopologyResult) -> None:
    """Validate a topology result before it crosses a persistence boundary."""
    if len(result.nodes) != plan.node_count or len(result.edges) != plan.edge_count:
        raise TopologyError("topology result counts do not match the plan")
    if result.metrics.get("node_count") != len(result.nodes) or result.metrics.get(
        "edge_count"
    ) != len(result.edges):
        raise TopologyError("topology result metrics do not match the payload")
    node_ids = [str(node.get("node_id", "")) for node in result.nodes]
    if (
        not node_ids
        or any(not node_id for node_id in node_ids)
        or len(node_ids) != len(set(node_ids))
    ):
        raise TopologyError("topology result contains invalid or duplicate node IDs")
    node_set = set(node_ids)
    edge_ids = [str(edge.get("edge_id", "")) for edge in result.edges]
    if any(not edge_id for edge_id in edge_ids) or len(edge_ids) != len(set(edge_ids)):
        raise TopologyError("topology result contains invalid or duplicate edge IDs")
    pairs: set[tuple[str, str]] = set()
    adjacency: dict[str, set[str]] = {node_id: set() for node_id in node_ids}
    degree = {node_id: 0 for node_id in node_ids}
    in_degree = {node_id: 0 for node_id in node_ids}
    out_degree = {node_id: 0 for node_id in node_ids}
    for edge in result.edges:
        source, target = str(edge.get("source", "")), str(edge.get("target", ""))
        if source not in node_set or target not in node_set:
            raise TopologyError("topology result contains an unknown endpoint")
        if not plan.allow_self_loops and source == target:
            raise TopologyError("topology result contains a forbidden self-loop")
        if plan.kind is GraphKind.DIRECTED:
            pair = (source, target)
        else:
            first, second = sorted((source, target))
            pair = (first, second)
        if not plan.allow_parallel_edges and pair in pairs:
            raise TopologyError("topology result contains a duplicate edge")
        pairs.add(pair)
        degree[source] += 1
        degree[target] += 1
        out_degree[source] += 1
        in_degree[target] += 1
        adjacency[source].add(target)
        if plan.kind is GraphKind.UNDIRECTED:
            adjacency[target].add(source)
    if plan.max_degree is not None and any(value > plan.max_degree for value in degree.values()):
        raise TopologyError("topology result exceeds max_degree")
    if plan.max_in_degree is not None and any(
        value > plan.max_in_degree for value in in_degree.values()
    ):
        raise TopologyError("topology result exceeds max_in_degree")
    if plan.max_out_degree is not None and any(
        value > plan.max_out_degree for value in out_degree.values()
    ):
        raise TopologyError("topology result exceeds max_out_degree")
    if plan.acyclic:
        if plan.kind is GraphKind.DIRECTED:
            if any(_reaches_named(adjacency, target, source) for source, target in pairs):
                raise TopologyError("topology result contains a cycle")
        else:
            parent = {node_id: node_id for node_id in node_ids}
            for source, target in pairs:
                left, right = source, target
                while parent[left] != left:
                    parent[left] = parent[parent[left]]
                    left = parent[left]
                while parent[right] != right:
                    parent[right] = parent[parent[right]]
                    right = parent[right]
                if left == right:
                    raise TopologyError("topology result contains a cycle")
                parent[left] = right


def _reaches_named(adjacency: dict[str, set[str]], start: str, target: str) -> bool:
    pending = [start]
    visited: set[str] = set()
    while pending:
        current = pending.pop()
        if current == target:
            return True
        if current in visited:
            continue
        visited.add(current)
        pending.extend(adjacency[current] - visited)
    return False


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
