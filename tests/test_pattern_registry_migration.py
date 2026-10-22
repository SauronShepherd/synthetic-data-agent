from __future__ import annotations

from typing import Any

import pytest

from sda.patterns.persistence import ensure_pattern_registry_columns


class _Catalog:
    def __init__(self, exists: bool) -> None:
        self.exists = exists

    def tableExists(self, name: str) -> bool:
        assert name == "workspace.profiles.pattern_registry"
        return self.exists


class _Table:
    def __init__(self, columns: list[str]) -> None:
        self.columns = columns


class _Spark:
    def __init__(self, columns: list[str], *, exists: bool = True, apply_ddl: bool = True) -> None:
        self.columns = columns
        self.catalog = _Catalog(exists)
        self.apply_ddl = apply_ddl
        self.statements: list[str] = []

    def table(self, name: str) -> _Table:
        assert name == "`workspace`.`profiles`.`pattern_registry`"
        return _Table(self.columns)

    def sql(self, statement: str) -> None:
        self.statements.append(statement)
        if self.apply_ddl:
            self.columns.extend(
                [
                    "evidence_quality_json",
                    "lifecycle",
                    "rule_provenance_json",
                    "temporal_clock_json",
                ]
            )


def test_existing_pattern_registry_gets_missing_v4_columns_before_write() -> None:
    spark = _Spark(["schema_version", "artifact_id", "status", "evidence_id"])
    rows: list[dict[str, Any]] = [
        {
            "schema_version": "sda07-pattern-registry-v4",
            "evidence_quality_json": "{}",
            "lifecycle": "observed_pattern",
            "rule_provenance_json": "{}",
            "temporal_clock_json": "{}",
        }
    ]
    ensure_pattern_registry_columns(spark, "workspace.profiles.pattern_registry", rows)
    assert spark.statements == [
        "ALTER TABLE `workspace`.`profiles`.`pattern_registry` ADD COLUMNS "
        "(`evidence_quality_json` STRING, `lifecycle` STRING, "
        "`rule_provenance_json` STRING, `temporal_clock_json` STRING)"
    ]


def test_pattern_registry_migration_fails_if_columns_remain_missing() -> None:
    spark = _Spark(["schema_version"], apply_ddl=False)
    with pytest.raises(ValueError, match="missing required fields"):
        ensure_pattern_registry_columns(
            spark, "workspace.profiles.pattern_registry", [{"evidence_quality_json": "{}"}]
        )


def test_new_pattern_registry_needs_no_migration() -> None:
    spark = _Spark([], exists=False)
    ensure_pattern_registry_columns(
        spark, "workspace.profiles.pattern_registry", [{"lifecycle": "x"}]
    )
    assert spark.statements == []
