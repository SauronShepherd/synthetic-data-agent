from __future__ import annotations

import sys
from types import SimpleNamespace

from scripts.check_sda08_live_state import DISPOSABLE_REQUEST_PATTERNS, inspect_state


class _Cursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self._row: tuple[int] = (0,)

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: object = None) -> None:
        self.calls.append((query, params))
        if "information_schema.tables" in query:
            self._row = (11,)
        elif "sda_schema_versions" in query:
            self._row = (1,)
        else:
            self._row = (0,)

    def fetchone(self) -> tuple[int]:
        return self._row


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> _Connection:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def cursor(self) -> _Cursor:
        return self._cursor


def test_live_state_checker_counts_sda09_disposable_requests(monkeypatch) -> None:
    cursor = _Cursor()
    connection = _Connection(cursor)
    fake_psycopg = SimpleNamespace(connect=lambda _dsn: connection)
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)

    assert inspect_state("dsn") == (11, 1, 0)

    query, params = cursor.calls[-1]
    assert "LIKE ANY(%s)" in query
    assert params == (list(DISPOSABLE_REQUEST_PATTERNS),)
    assert "sda09-live-%" in DISPOSABLE_REQUEST_PATTERNS
