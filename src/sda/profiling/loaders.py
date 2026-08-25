"""Typed profile evidence index for downstream relationship/pattern stages."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ProfileIndex:
    """Bounded lookup of profile summaries; never stores source values."""

    by_table: Mapping[str, Mapping[str, Any]]

    @classmethod
    def from_rows(cls, rows: Iterable[Mapping[str, Any]]) -> ProfileIndex:
        index: dict[str, Mapping[str, Any]] = {}
        for row in rows:
            table = str(row.get("source_table", row.get("table_fqn", "")))
            if table:
                index[table] = dict(row)
        return cls(index)

    def get(self, table_fqn: str) -> Mapping[str, Any] | None:
        return self.by_table.get(table_fqn)

    def has_column(self, table_fqn: str, column: str) -> bool:
        row = self.get(table_fqn) or {}
        columns = row.get("columns", row.get("profiled_columns", ()))
        return column in columns or any(
            str(item.get("column_name", "")) == column
            for item in columns
            if isinstance(item, Mapping)
        )
