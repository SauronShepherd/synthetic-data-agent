from typing import Any

from sda.artifacts.delta import persist_rows


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
