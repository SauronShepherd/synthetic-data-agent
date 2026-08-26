"""PostgreSQL/Lakebase implementation of the workflow-state repository."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from typing import Any

from sda.state import StateRepository
from sda.state_sqlite import SQLiteStateRepository


class _PostgresConnection:
    """DB-API shim for the SQL shared with the local adapter."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def execute(self, statement: str, parameters: tuple[Any, ...] = ()) -> Any:
        statement = statement.replace("?", "%s")
        for source, target in (
            ("runs", "sda_runs"),
            ("attempts", "sda_execution_attempts"),
            ("approvals", "sda_approvals"),
            ("feedback", "sda_feedback"),
        ):
            statement = statement.replace(f" {source} ", f" {target} ")
            statement = statement.replace(f" {source}(", f" {target}(")
        try:
            cursor = self._connection.cursor()
            cursor.execute(statement, parameters)
            return cursor
        except Exception as exc:
            if exc.__class__.__name__ in {
                "IntegrityError",
                "UniqueViolation",
                "ForeignKeyViolation",
            }:
                raise sqlite3.IntegrityError(str(exc)) from exc
            raise

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


class PostgreSQLStateRepository(SQLiteStateRepository, StateRepository):
    """Durable repository backed by PostgreSQL or Databricks Lakebase."""

    def __init__(self, connection: Any) -> None:
        self._connection: Any = _PostgresConnection(connection)

    @classmethod
    def connect(
        cls, dsn: str, *, connect: Callable[..., Any] | None = None
    ) -> PostgreSQLStateRepository:
        """Connect using an injected factory or the optional psycopg dependency."""
        if connect is None:
            try:
                import psycopg
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError(
                    "PostgreSQL support requires the optional 'psycopg' dependency"
                ) from exc
            connect = psycopg.connect
        return cls(connect(dsn))
