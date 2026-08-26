from sda.state_postgres import _PostgresConnection


class Cursor:
    def __init__(self) -> None:
        self.statement = ""
        self.parameters = ()

    def execute(self, statement: str, parameters: tuple[object, ...]) -> None:
        self.statement = statement
        self.parameters = parameters


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
    assert cursor is connection.cursor_instance
    assert cursor.statement == "INSERT INTO sda_runs (run_id) VALUES (%s)"
    assert cursor.parameters == ("run-1",)
