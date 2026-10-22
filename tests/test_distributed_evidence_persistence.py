from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

import pytest

from sda.artifacts.delta import persist_distributed_evidence
from sda.runtime.errors import PersistenceError


class _Frame:
    columns = ["analysis_id", "evidence_id", "metric_value"]

    def alias(self, name: str) -> _Frame:
        assert name == "source"
        return self


class _Target:
    def __init__(self, columns: list[str]) -> None:
        self.columns = columns
        self.executed = False
        self.condition = ""

    def toDF(self) -> Any:
        return type("DataFrame", (), {"columns": self.columns})()

    def alias(self, name: str) -> _Target:
        assert name == "target"
        return self

    def merge(self, frame: _Frame, condition: str) -> _Target:
        self.condition = condition
        return self

    def whenNotMatchedInsertAll(self) -> _Target:
        return self

    def execute(self) -> None:
        self.executed = True


def test_distributed_evidence_merges_by_stable_row_identity(monkeypatch: Any) -> None:
    target = _Target(_Frame.columns)

    class _DeltaTable:
        @staticmethod
        def forName(spark: Any, location: str) -> _Target:
            assert location == "main.evidence.patterns"
            return target

    delta = ModuleType("delta")
    tables = ModuleType("delta.tables")
    tables.DeltaTable = _DeltaTable  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "delta", delta)
    monkeypatch.setitem(sys.modules, "delta.tables", tables)

    persist_distributed_evidence(
        object(), _Frame(), "main.evidence.patterns", analysis_id="analysis-1"
    )
    assert target.executed
    assert "target.analysis_id = source.analysis_id" in target.condition
    assert "target.evidence_id = source.evidence_id" in target.condition


def test_distributed_evidence_rejects_missing_stable_identity() -> None:
    frame = _Frame()
    frame.columns = ["analysis_id", "metric_value"]
    with pytest.raises(PersistenceError, match="failed to persist distributed pattern evidence"):
        persist_distributed_evidence(
            object(), frame, "main.evidence.patterns", analysis_id="analysis-1"
        )
