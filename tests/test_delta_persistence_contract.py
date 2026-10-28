import sys
from types import ModuleType
from typing import Any

import pytest

from sda.artifacts.delta import persist_rows
from sda.runtime.errors import PersistenceError


class Writer:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    class Frame:
        def __init__(self, owner: "Writer") -> None:
            self.owner = owner

        class Write:
            def format(self, value: str) -> "Writer.Frame.Write":
                return self

            def mode(self, value: str) -> "Writer.Frame.Write":
                return self

            def saveAsTable(self, value: str) -> None:
                return None

        @property
        def write(self) -> "Writer.Frame.Write":
            return self.Write()

    def createDataFrame(self, rows: list[dict[str, Any]]) -> "Writer.Frame":
        self.rows = rows
        return self.Frame(self)


def test_controlled_artifact_fields_cannot_be_overwritten() -> None:
    writer = Writer()
    persist_rows(
        writer,
        [{"artifact_id": "evil", "status": "failed", "value": 1}],
        "main.evidence.artifacts",
        artifact_id="safe",
        status="complete",
    )
    assert writer.rows[0]["artifact_id"] == "safe"
    assert writer.rows[0]["status"] == "complete"


def test_persist_rows_keeps_fields_from_every_evidence_row() -> None:
    writer = Writer()
    persist_rows(
        writer,
        [{"kind": "relationship"}, {"kind": "candidate", "parent_table": "main.p"}],
        "main.evidence.artifacts",
        artifact_id="safe",
    )

    assert writer.rows[0]["parent_table"] is None
    assert writer.rows[1]["parent_table"] == "main.p"
    assert set(writer.rows[0]) == set(writer.rows[1])


def test_persist_rows_allows_empty_evidence_batches() -> None:
    writer = Writer()

    persist_rows(
        writer,
        [],
        "main.evidence.artifacts",
        artifact_id="safe",
    )

    assert writer.rows == []


def test_delta_merge_rejects_missing_columns_instead_of_dropping_them(monkeypatch: Any) -> None:
    class FakeTarget:
        def toDF(self) -> Any:
            return type("TargetFrame", (), {"columns": ["artifact_id", "status", "evidence_id"]})()

    class FakeDeltaTable:
        @staticmethod
        def forName(spark: Any, location: str) -> FakeTarget:
            return FakeTarget()

    delta = ModuleType("delta")
    tables = ModuleType("delta.tables")
    tables.DeltaTable = FakeDeltaTable  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "delta", delta)
    monkeypatch.setitem(sys.modules, "delta.tables", tables)

    class FakeFrame(Writer.Frame):
        columns = ["artifact_id", "status", "evidence_id", "new_evidence"]

        def createOrReplaceTempView(self, name: str) -> None:
            return None

    class FakeSpark(Writer):
        def createDataFrame(self, rows: list[dict[str, Any]], schema: Any = None) -> FakeFrame:
            return FakeFrame(self)

    with pytest.raises(PersistenceError, match="failed to persist Delta artifact") as error:
        persist_rows(
            FakeSpark(),
            [{"new_evidence": "must not disappear"}],
            "main.evidence.artifacts",
            artifact_id="a",
        )
    assert isinstance(error.value.__cause__, PersistenceError)
    assert "new_evidence" in error.value.__cause__.details["columns"]
