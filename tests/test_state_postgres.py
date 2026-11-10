from datetime import UTC, datetime

import pytest

from sda.state_postgres import (
    LakebaseConnectionConfig,
    _PostgresConnection,
    connect_lakebase,
    environment_token_provider,
)


class Cursor:
    def __init__(self) -> None:
        self.statement = ""
        self.parameters = ()

    def execute(self, statement: str, parameters: tuple[object, ...]) -> None:
        self.statement = statement
        self.parameters = parameters

    def fetchone(self) -> tuple[object, ...]:
        return (datetime(2026, 1, 2, tzinfo=UTC),)

    def fetchall(self) -> list[tuple[object, ...]]:
        return [(datetime(2026, 1, 2, tzinfo=UTC),)]


class Connection:
    def __init__(self) -> None:
        self.cursor_instance = Cursor()

    def cursor(self) -> Cursor:
        return self.cursor_instance


def test_postgres_connection_translates_placeholders_and_tables() -> None:
    connection = Connection()
    cursor = _PostgresConnection(connection).execute(
        "INSERT INTO runs (run_id) VALUES (?)", ("run-1",)
    )
    assert cursor.statement == "INSERT INTO sda_runs (run_id) VALUES (%s)"
    assert cursor.parameters == ("run-1",)

    audit_cursor = _PostgresConnection(connection).execute(
        "INSERT INTO audit_events (run_id) VALUES (?)", ("run-1",)
    )
    assert audit_cursor.statement == "INSERT INTO sda_audit_events (run_id) VALUES (%s)"


def test_postgres_connection_normalises_temporal_values() -> None:
    connection = Connection()
    adapted = _PostgresConnection(connection)
    cursor = adapted.execute("SELECT created_at FROM runs", ())
    assert cursor.fetchone() == ("2026-01-02T00:00:00+00:00",)


def test_lakebase_connection_refreshes_oauth_token_between_retries() -> None:
    calls: list[dict[str, object]] = []
    tokens = iter(("expired", "fresh"))

    def connect(**kwargs: object) -> object:
        calls.append(kwargs)
        if kwargs["password"] == "expired":
            raise OSError("expired token")
        return object()

    connection = connect_lakebase(
        LakebaseConnectionConfig("db.example", "sda_state", "sda_app", lambda: next(tokens)),
        connect=connect,
        max_attempts=2,
        backoff_seconds=0,
        sleep=lambda _: None,
    )
    assert connection is not None
    assert [call["password"] for call in calls] == ["expired", "fresh"]
    assert calls[1]["sslmode"] == "require"


def test_lakebase_connection_does_not_expose_token_in_failure() -> None:
    secret = "do-not-log-this"

    def connect(**_: object) -> object:
        raise OSError(secret)

    with pytest.raises(ConnectionError, match="after 1 attempts") as error:
        connect_lakebase(
            LakebaseConnectionConfig("db.example", "sda_state", "sda_app", lambda: secret),
            connect=connect,
            max_attempts=1,
            sleep=lambda _: None,
        )
    assert secret not in str(error.value)


def test_environment_token_provider_reads_lazily() -> None:
    values = {"TOKEN": "one"}
    provider = environment_token_provider("TOKEN", environ=values)
    assert provider() == "one"
    values["TOKEN"] = "two"
    assert provider() == "two"
