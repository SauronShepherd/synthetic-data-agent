"""PostgreSQL/Lakebase implementation of the workflow-state repository."""

from __future__ import annotations

import json
import os
import sqlite3
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sda.state import StateRepository
from sda.state_sqlite import SQLiteStateRepository

TokenProvider = Callable[[], str]
ConnectFactory = Callable[..., Any]


@dataclass(frozen=True, slots=True)
class LakebaseConnectionConfig:
    """Non-secret settings used to open a short-lived Lakebase connection."""

    host: str
    database: str
    user: str
    token_provider: TokenProvider
    sslmode: str = "require"
    connect_timeout: int = 15

    def __post_init__(self) -> None:
        for value in (self.host, self.database, self.user, self.sslmode):
            if not value.strip():
                raise ValueError("Lakebase connection settings must not be empty")
        if self.connect_timeout < 1:
            raise ValueError("connect_timeout must be positive")

    @classmethod
    def from_environment(
        cls,
        *,
        environ: Mapping[str, str] | None = None,
        token_variable: str = "DATABRICKS_TOKEN",
    ) -> LakebaseConnectionConfig:
        """Build non-secret connection settings from Databricks app/job variables.

        Databricks Apps expose ``PGHOST``, ``PGDATABASE``, ``PGUSER``, and
        ``PGSSLMODE`` for a Lakebase resource. The token remains lazy and is
        supplied by ``token_variable`` so a reconnect can obtain a fresh token.
        """

        values = os.environ if environ is None else environ

        def required(name: str) -> str:
            value = values.get(name, "")
            if not value.strip():
                raise ValueError(f"missing Lakebase environment variable: {name}")
            return value

        timeout_text = values.get("PGCONNECT_TIMEOUT", "15")
        try:
            connect_timeout = int(timeout_text)
        except ValueError as exc:
            raise ValueError("PGCONNECT_TIMEOUT must be an integer") from exc
        return cls(
            host=required("PGHOST"),
            database=required("PGDATABASE"),
            user=required("PGUSER"),
            token_provider=environment_token_provider(token_variable, environ=values),
            sslmode=values.get("PGSSLMODE", "require"),
            connect_timeout=connect_timeout,
        )


def environment_token_provider(
    variable: str = "DATABRICKS_TOKEN", *, environ: Mapping[str, str] | None = None
) -> TokenProvider:
    """Return a lazy token provider so credentials are refreshed per connection."""

    if not variable.strip():
        raise ValueError("token environment variable must not be empty")
    values = os.environ if environ is None else environ

    def provide() -> str:
        token = values.get(variable, "")
        if not token:
            raise RuntimeError(f"missing Lakebase OAuth token in {variable}")
        return token

    return provide


def connect_lakebase(
    config: LakebaseConnectionConfig,
    *,
    connect: ConnectFactory | None = None,
    max_attempts: int = 3,
    backoff_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> Any:
    """Open a Lakebase connection, refreshing the OAuth token on each retry.

    The retry is limited to establishing the connection. Repository operations are
    not retried here, so a reconnect cannot accidentally duplicate a business
    effect. Tokens are passed only to the DB-API driver and never logged.
    """

    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")
    if backoff_seconds < 0:
        raise ValueError("backoff_seconds must not be negative")
    if connect is None:
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "PostgreSQL support requires the optional 'psycopg' dependency"
            ) from exc
        connect = psycopg.connect

    last_error: Exception | None = None
    for attempt in range(max_attempts):
        try:
            token = config.token_provider()
            return connect(
                host=config.host,
                dbname=config.database,
                user=config.user,
                password=token,
                sslmode=config.sslmode,
                connect_timeout=config.connect_timeout,
            )
        except Exception as exc:  # pragma: no cover - exact driver errors vary
            last_error = exc
            if attempt + 1 < max_attempts:
                sleep(backoff_seconds * (2**attempt))
    raise ConnectionError(
        f"Lakebase connection failed after {max_attempts} attempts"
    ) from last_error


def _normalise(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, list | dict):
        return json.dumps(value, separators=(",", ":"))
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
            ("requests", "sda_generation_requests"),
            ("plans", "sda_generation_plans"),
            ("attempts", "sda_execution_attempts"),
            ("approvals", "sda_approvals"),
            ("feedback", "sda_feedback"),
            ("audit_events", "sda_audit_events"),
            ("transition_events", "sda_transition_events"),
            ("run_steps", "sda_run_steps"),
            ("artifact_uses", "sda_artifact_uses"),
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

    @classmethod
    def connect_lakebase(
        cls,
        config: LakebaseConnectionConfig,
        *,
        connect: ConnectFactory | None = None,
        max_attempts: int = 3,
        backoff_seconds: float = 1.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> PostgreSQLStateRepository:
        """Construct a repository using Databricks OAuth credentials."""

        return cls(
            connect_lakebase(
                config,
                connect=connect,
                max_attempts=max_attempts,
                backoff_seconds=backoff_seconds,
                sleep=sleep,
            )
        )
