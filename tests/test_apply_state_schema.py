from pathlib import Path

import pytest
from scripts.apply_state_schema import apply_schema_files, schema_files, schema_version


def test_schema_version_is_deterministic_and_versioned() -> None:
    assert schema_version("schema") == schema_version("schema")
    assert schema_version("schema").startswith("state-schema-")
    assert schema_version("schema") != schema_version("schema-v2")


def test_schema_files_are_ordered_and_non_empty(tmp_path: Path) -> None:
    (tmp_path / "002_second.sql").write_text("select 2", encoding="utf-8")
    (tmp_path / "001_first.sql").write_text("select 1", encoding="utf-8")
    assert [path.name for path in schema_files(tmp_path)] == [
        "001_first.sql",
        "002_second.sql",
    ]


def test_schema_files_reject_empty_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no SQL schema files"):
        schema_files(tmp_path)


class _Cursor:
    def __init__(self, applied: set[str]) -> None:
        self.applied = applied
        self.executed: list[tuple[str, tuple[str, ...] | None]] = []
        self._fetchone: tuple[int] | None = None

    def execute(self, sql: str, params: tuple[str, ...] | None = None) -> None:
        self.executed.append((sql, params))
        if sql.startswith("SELECT 1 FROM sda_schema_versions"):
            self._fetchone = (1,) if params and params[0] in self.applied else None
        elif sql.startswith("INSERT INTO sda_schema_versions") and params:
            self.applied.add(params[0])

    def fetchone(self) -> tuple[int] | None:
        return self._fetchone

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self.cursor_value = cursor

    def cursor(self) -> _Cursor:
        return self.cursor_value

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *_: object) -> None:
        return None


def test_schema_files_are_not_reexecuted_after_their_fingerprint_is_recorded(
    tmp_path: Path,
) -> None:
    schema_path = tmp_path / "001_state.sql"
    schema_path.write_text("CREATE TABLE example (id INTEGER)", encoding="utf-8")
    applied: set[str] = set()
    cursor = _Cursor(applied)

    def connect(_: str) -> _Connection:
        return _Connection(cursor)

    first = apply_schema_files("dsn", tmp_path, connect=connect)
    first_sql_count = sum(sql.startswith("CREATE TABLE example") for sql, _ in cursor.executed)
    cursor.executed.clear()
    second = apply_schema_files("dsn", tmp_path, connect=connect)
    second_sql_count = sum(sql.startswith("CREATE TABLE example") for sql, _ in cursor.executed)

    assert first == (schema_version("001_state.sql\nCREATE TABLE example (id INTEGER)"),)
    assert second == ()
    assert first_sql_count == 1
    assert second_sql_count == 0
