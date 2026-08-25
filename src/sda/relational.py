"""Deterministic relational generation over synthetic parent key domains."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sda.generation import GenerationError, generate_rows
from sda.planning import GenerationPlan


@dataclass(frozen=True, slots=True)
class ForeignKeySpec:
    child_table: str
    child_column: str
    parent_table: str
    parent_column: str
    nullable: bool = False

    def __post_init__(self) -> None:
        if any(not value.strip() for value in (self.child_table, self.child_column, self.parent_table, self.parent_column)):
            raise ValueError("foreign-key fields must not be empty")


@dataclass(frozen=True, slots=True)
class CompositeForeignKeySpec:
    child_table: str
    child_columns: tuple[str, ...]
    parent_table: str
    parent_columns: tuple[str, ...]
    nullable: bool = False

    def __post_init__(self) -> None:
        if not self.child_columns or len(self.child_columns) != len(self.parent_columns):
            raise ValueError("composite foreign-key columns must be non-empty and have equal length")
        if any(not value.strip() for value in (self.child_table, self.parent_table, *self.child_columns, *self.parent_columns)):
            raise ValueError("composite foreign-key fields must not be empty")


class RelationalGenerationError(GenerationError):
    """Raised when relational constraints cannot be satisfied."""


def generate_relational(
    plan: GenerationPlan,
    *,
    row_counts: dict[str, int],
    foreign_keys: tuple[ForeignKeySpec, ...] = (),
    composite_foreign_keys: tuple[CompositeForeignKeySpec, ...] = (),
    vocabularies: dict[str, tuple[str, ...]] | None = None,
) -> dict[str, tuple[dict[str, Any], ...]]:
    """Generate tables in dependency order and wire child rows to parent keys.

    The first implementation supports one-column parent references and preserves
    zero-child parents. Composite and cyclic relationships are rejected explicitly
    until their dedicated strategies are implemented.
    """
    if set(row_counts) - set(plan.tables):
        raise RelationalGenerationError("row_counts contains tables outside the plan")
    _validate_graph(plan.tables, foreign_keys, composite_foreign_keys)
    by_table = {table: [] for table in plan.tables}
    all_relationships = (*foreign_keys, *composite_foreign_keys)
    for table in _order_tables(plan.tables, all_relationships):
        count = row_counts.get(table, 0)
        table_rows = list(generate_rows(plan, row_count=count, vocabularies=vocabularies))
        for fk in [item for item in foreign_keys if item.child_table == table]:
            parent_rows = by_table[fk.parent_table]
            parent_keys = [row.get(fk.parent_column) for row in parent_rows]
            if not parent_keys and table_rows and not fk.nullable:
                raise RelationalGenerationError(f"no parent keys available for {table}.{fk.child_column}")
            for index, row in enumerate(table_rows):
                row[fk.child_column] = None if fk.nullable and not parent_keys else parent_keys[index % len(parent_keys)]
        for fk in [item for item in composite_foreign_keys if item.child_table == table]:
            parent_keys = [tuple(row.get(column) for column in fk.parent_columns) for row in by_table[fk.parent_table]]
            if not parent_keys and table_rows and not fk.nullable:
                raise RelationalGenerationError(f"no parent keys available for {table}.{fk.child_columns}")
            for index, row in enumerate(table_rows):
                values = (None,) * len(fk.child_columns) if fk.nullable and not parent_keys else parent_keys[index % len(parent_keys)]
                for column, value in zip(fk.child_columns, values, strict=True):
                    row[column] = value
        by_table[table] = table_rows
    _assert_integrity(by_table, foreign_keys, composite_foreign_keys)
    return {table: tuple(rows) for table, rows in by_table.items()}


def _validate_graph(tables: tuple[str, ...], foreign_keys: tuple[ForeignKeySpec, ...], composite: tuple[CompositeForeignKeySpec, ...] = ()) -> None:
    all_fks = (*foreign_keys, *composite)
    if any(fk.child_table == fk.parent_table for fk in all_fks):
        raise RelationalGenerationError("self-referential foreign keys require an explicit cycle strategy")
    for fk in all_fks:
        if fk.child_table not in tables or fk.parent_table not in tables:
            raise RelationalGenerationError("foreign key references a table outside the plan")
    _order_tables(tables, all_fks)


def _order_tables(tables: tuple[str, ...], foreign_keys: tuple[ForeignKeySpec, ...]) -> tuple[str, ...]:
    remaining = set(tables)
    ordered: list[str] = []
    while remaining:
        ready = sorted(table for table in remaining if all(
            fk.parent_table not in remaining for fk in foreign_keys if fk.child_table == table
        ))
        if not ready:
            raise RelationalGenerationError("relationship graph contains an unresolved cycle")
        ordered.extend(ready)
        remaining.difference_update(ready)
    return tuple(ordered)


def _assert_integrity(tables: dict[str, list[dict[str, Any]]], foreign_keys: tuple[ForeignKeySpec, ...], composite: tuple[CompositeForeignKeySpec, ...] = ()) -> None:
    for fk in foreign_keys:
        parent_keys = {row.get(fk.parent_column) for row in tables[fk.parent_table]}
        for row in tables[fk.child_table]:
            value = row.get(fk.child_column)
            if value is None and fk.nullable:
                continue
            if value not in parent_keys:
                raise RelationalGenerationError(f"orphan foreign key in {fk.child_table}.{fk.child_column}")
    for fk in composite:
        parent_keys = {tuple(row.get(column) for column in fk.parent_columns) for row in tables[fk.parent_table]}
        for row in tables[fk.child_table]:
            value = tuple(row.get(column) for column in fk.child_columns)
            if fk.nullable and all(item is None for item in value):
                continue
            if value not in parent_keys:
                raise RelationalGenerationError(f"orphan composite foreign key in {fk.child_table}.{fk.child_columns}")
