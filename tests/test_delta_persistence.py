import pytest

from sda.artifacts.delta import persist_rows
from sda.runtime.errors import PersistenceError


class BrokenSpark:
    def createDataFrame(self, rows: list[dict[str, int]]) -> None:
        raise RuntimeError("boom")


def test_delta_persistence_classifies_write_failure() -> None:
    with pytest.raises(PersistenceError):
        persist_rows(BrokenSpark(), [{"value": 1}], "main.evidence.artifacts", artifact_id="a")
