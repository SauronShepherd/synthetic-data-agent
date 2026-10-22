from __future__ import annotations

from typing import Any

from sda.artifacts.delta import ensure_artifact_evidence_columns


class FakeSpark:
    def __init__(self) -> None:
        self.columns = ["kind", "tables", "artifact_id", "status", "evidence_id"]
        self.statements: list[str] = []

        class Catalog:
            @staticmethod
            def tableExists(name: str) -> bool:
                return name == "workspace.profiles.scope_relationship_evidence"

        self.catalog = Catalog()

    def table(self, name: str) -> Any:
        assert name == "`workspace`.`profiles`.`scope_relationship_evidence`"
        return type("Table", (), {"columns": self.columns})()

    def sql(self, statement: str) -> None:
        self.statements.append(statement)
        added = statement.split("ADD COLUMNS (", 1)[1].rstrip(")")
        self.columns.extend(item.strip().split()[0].strip("`") for item in added.split(","))


def test_existing_evidence_table_gets_only_missing_additive_columns() -> None:
    spark = FakeSpark()

    ensure_artifact_evidence_columns(
        spark,
        "workspace.profiles.scope_relationship_evidence",
        [{"kind": "candidate", "parent_table": "main.parent", "columns_json": "[]"}],
    )

    assert len(spark.statements) == 1
    assert "`parent_table` STRING" in spark.statements[0]
    assert "`columns_json` STRING" in spark.statements[0]
    assert "`kind` STRING" not in spark.statements[0]
