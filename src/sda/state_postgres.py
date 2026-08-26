"""PostgreSQL/Lakebase implementation of the workflow-state repository."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import date, datetime
from typing import Any

from sda.state import StateRepository
from sda.state_sqlite import SQLiteStateRepository


def _normalise(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


class _PostgresCursor:
    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    @property
    def rowcount(self) -> int:
        return int(self._cursor.rowcount)

    @property
    def statement(self) -> str:
        return str(self._cursor.statement)

    @property
    def parameters(self) -> tuple[Any, ...]:
        return tuple(self._cursor.parameters)

    def execute(self, statement: str, parameters: tuple[Any, ...]) -> None:
        self._cursor.execute(statement, parameters)

    def fetchone(self) -> Any:
        row = self._cursor.fetchone()
        return tuple(_normalise(value) for value in row) if row is not None else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return [tuple(_normalise(value) for value in row) for row in self._cursor.fetchall()]


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
            cursor = _PostgresCursor(self._connection.cursor())
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
