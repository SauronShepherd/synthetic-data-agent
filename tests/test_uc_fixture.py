from __future__ import annotations

import pytest

from sda.job_entrypoints.uc_fixture_spark import _identifier, cleanup_fixture


class _FakeSpark:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def sql(self, statement: str) -> None:
        self.statements.append(statement)


def test_cleanup_fixture_is_scoped_to_owned_tables() -> None:
    spark = _FakeSpark()

    cleanup_fixture(spark, "workspace", "sda_integration", "run_123")

    assert spark.statements == [
        "DROP TABLE IF EXISTS workspace.sda_integration.sda07_run_123_orders",
        "DROP TABLE IF EXISTS workspace.sda_integration.sda07_run_123_customers",
    ]
    assert all("DROP SCHEMA" not in statement for statement in spark.statements)


@pytest.mark.parametrize("value", ["workspace;drop schema", "`workspace`", "a.b"])
def test_fixture_identifiers_reject_sql_shaped_values(value: str) -> None:
    with pytest.raises(SystemExit):
        _identifier(value)
