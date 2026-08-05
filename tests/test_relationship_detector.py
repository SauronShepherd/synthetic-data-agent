from dataclasses import dataclass

from sda.relationships.detector import RelationshipDetector
from sda.relationships.metrics import measure_join


@dataclass
class Column:
    name: str


@dataclass
class Constraint:
    kind: str
    columns: tuple[str, ...]
    name: str = "pk"
    referenced_table: str | None = None
    referenced_columns: tuple[str, ...] = ()


@dataclass
class Table:
    columns: tuple[Column, ...]
    constraints: tuple[Constraint, ...]


def test_join_metrics_exclude_nulls_and_report_orphans() -> None:
    result = measure_join(
        [{"id": 1}, {"id": 2}],
        [{"customer_id": 1}, {"customer_id": 1}, {"customer_id": 9}, {"customer_id": None}],
        ("id",),
        ("customer_id",),
    )
    assert result.child_row_coverage == 2 / 3
    import pytest

    assert result.orphan_rate == pytest.approx(1 / 3)
    assert result.child_null_rate == 1 / 4
    assert result.cardinality == "many_to_one"


def test_detector_preserves_declared_composite_column_order() -> None:
    parent = Table(
        (Column("country"), Column("number")), (Constraint("PRIMARY KEY", ("country", "number")),)
    )
    child = Table(
        (Column("country"), Column("number")),
        (
            Constraint(
                "FOREIGN KEY", ("country", "number"), "fk", "crm.parent", ("country", "number")
            ),
        ),
    )
    artifact = RelationshipDetector().detect(
        {"crm.parent": parent, "sales.child": child},
        {
            "crm.parent": [{"country": "ES", "number": 1}],
            "sales.child": [{"country": "ES", "number": 1}],
        },
    )
    relationship = artifact["relationships"][0]
    assert relationship["child_columns"] == ["country", "number"]
    assert relationship["decision"] == "accepted"
    assert artifact["generation_order"] == ["crm.parent", "sales.child"]


def test_detector_does_not_order_rejected_relationship() -> None:
    parent = Table((Column("id"),), (Constraint("PRIMARY KEY", ("id",)),))
    child = Table((Column("id"),), (Constraint("FOREIGN KEY", ("id",), "fk", "parent", ("id",)),))
    artifact = RelationshipDetector().detect(
        {"parent": parent, "child": child},
        {"parent": [{"id": 1}, {"id": 1}], "child": [{"id": 1}]},
    )
    assert artifact["relationships"][0]["decision"] == "rejected"
    assert artifact["generation_order"] == []


def test_graph_reports_strongly_connected_cycle_component() -> None:
    from sda.relationships.graph import DependencyGraph

    graph = DependencyGraph()
    graph.add_edge("a", "b")
    graph.add_edge("b", "a")
    graph.add_edge("c", "d")
    assert graph.cycles() == (("a", "b"),)
