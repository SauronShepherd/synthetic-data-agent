"""Deterministic relational generation over synthetic parent key domains."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, replace
from typing import Any, cast

from sda.artifacts.fingerprint import fingerprint
from sda.generation import GenerationError, generate_rows
from sda.planning import GenerationPlan


@dataclass(frozen=True, slots=True)
class ForeignKeySpec:
    child_table: str
    child_column: str
    parent_table: str
    parent_column: str
    nullable: bool = False
    optional_rate: float = 0.0

    def __post_init__(self) -> None:
        if any(
            not value.strip()
            for value in (
                self.child_table,
                self.child_column,
                self.parent_table,
                self.parent_column,
            )
        ):
            raise ValueError("foreign-key fields must not be empty")
        if not self.nullable and self.optional_rate:
            raise ValueError("optional_rate requires a nullable foreign key")
        if not 0 <= self.optional_rate <= 1:
            raise ValueError("optional_rate must be between zero and one")


@dataclass(frozen=True, slots=True)
class CompositeForeignKeySpec:
    child_table: str
    child_columns: tuple[str, ...]
    parent_table: str
    parent_columns: tuple[str, ...]
    nullable: bool = False
    optional_rate: float = 0.0

    def __post_init__(self) -> None:
        if not self.child_columns or len(self.child_columns) != len(self.parent_columns):
            raise ValueError(
                "composite foreign-key columns must be non-empty and have equal length"
            )
        if any(
            not value.strip()
            for value in (
                self.child_table,
                self.parent_table,
                *self.child_columns,
                *self.parent_columns,
            )
        ):
            raise ValueError("composite foreign-key fields must not be empty")
        if not self.nullable and self.optional_rate:
            raise ValueError("optional_rate requires a nullable foreign key")
        if not 0 <= self.optional_rate <= 1:
            raise ValueError("optional_rate must be between zero and one")


class RelationalGenerationError(GenerationError):
    """Raised when relational constraints cannot be satisfied."""


@dataclass(frozen=True, slots=True)
class RelationalGenerationReceipt:
    """Raw-value-free receipt for a deterministic relational output."""

    plan_id: str
    plan_fingerprint: str
    table_counts: tuple[tuple[str, int], ...]
    table_fingerprints: tuple[tuple[str, str], ...]
    output_fingerprint: str
    schema_version: str = "relational-generation-receipt-v1"

    def __post_init__(self) -> None:
        if not self.plan_id.strip() or not self.plan_fingerprint.strip():
            raise ValueError("receipt plan identity must not be empty")
        if any(count < 0 for _, count in self.table_counts):
            raise ValueError("receipt table counts must not be negative")
        if not self.output_fingerprint.strip() or not self.schema_version.strip():
            raise ValueError("receipt fingerprints and schema version must not be empty")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RelationalGenerationManifest:
    """Lineage manifest binding a relational receipt to its approved plan."""

    run_id: str
    output_namespace: str
    plan_id: str
    plan_fingerprint: str
    source_snapshot_ids: tuple[str, ...]
    input_artifact_ids: tuple[str, ...]
    receipt: RelationalGenerationReceipt
    schema_version: str = "relational-generation-manifest-v1"

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (self.run_id, self.output_namespace, self.plan_id, self.plan_fingerprint)
        ):
            raise ValueError("manifest identity fields must not be empty")
        if not self.source_snapshot_ids:
            raise ValueError("source_snapshot_ids must not be empty")
        if (
            self.receipt.plan_id != self.plan_id
            or self.receipt.plan_fingerprint != self.plan_fingerprint
        ):
            raise ValueError("receipt does not belong to manifest plan")

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["receipt"] = self.receipt.to_dict()
        return value


def receipt_for_relational(
    plan: GenerationPlan, tables: dict[str, tuple[dict[str, Any], ...]]
) -> RelationalGenerationReceipt:
    """Create a deterministic receipt without serializing generated values."""
    if plan.status.value != "approved":
        raise RelationalGenerationError("only approved plans may be receipted")
    if set(tables) != set(plan.tables):
        raise RelationalGenerationError("receipt tables must match the approved plan")
    counts = tuple((table, len(tables[table])) for table in sorted(tables))
    fingerprints = tuple((table, fingerprint(tables[table])) for table in sorted(tables))
    return RelationalGenerationReceipt(
        plan.plan_id, plan.plan_fingerprint, counts, fingerprints, fingerprint(fingerprints)
    )


def manifest_for_relational(
    plan: GenerationPlan,
    receipt: RelationalGenerationReceipt,
    *,
    run_id: str,
    output_namespace: str,
) -> RelationalGenerationManifest:
    if receipt.plan_id != plan.plan_id or receipt.plan_fingerprint != plan.plan_fingerprint:
        raise RelationalGenerationError("generation receipt does not belong to the plan")
    return RelationalGenerationManifest(
        run_id,
        output_namespace,
        plan.plan_id,
        plan.plan_fingerprint,
        plan.source_snapshot_ids,
        plan.input_artifact_ids,
        receipt,
    )


def generate_relational(
    plan: GenerationPlan,
    *,
    row_counts: dict[str, int],
    foreign_keys: tuple[ForeignKeySpec, ...] = (),
    composite_foreign_keys: tuple[CompositeForeignKeySpec, ...] = (),
    vocabularies: dict[str, tuple[str, ...]] | None = None,
    fanout_distributions: dict[tuple[str, str], tuple[int, ...]] | None = None,
) -> dict[str, tuple[dict[str, Any], ...]]:
    """Generate tables in dependency order and wire child rows to parent keys.

    The first implementation supports one-column parent references and preserves
    zero-child parents. Composite and cyclic relationships are rejected explicitly
    until their dedicated strategies are implemented.
    """
    if set(row_counts) - set(plan.tables):
        raise RelationalGenerationError("row_counts contains tables outside the plan")
    if set(row_counts) != set(plan.tables):
        missing = sorted(set(plan.tables) - set(row_counts))
        raise RelationalGenerationError(
            f"row_counts must specify every planned table; missing: {missing}"
        )
    if any(count < 0 for count in row_counts.values()):
        raise RelationalGenerationError("row_counts must not contain negative values")
    _validate_graph(plan.tables, foreign_keys, composite_foreign_keys)
    fanouts = fanout_distributions or {}
    _validate_fanouts(fanouts, row_counts, foreign_keys, composite_foreign_keys)
    by_table: dict[str, list[dict[str, Any]]] = {table: [] for table in plan.tables}
    all_relationships: tuple[Any, ...] = (*foreign_keys, *composite_foreign_keys)
    for table in _order_tables(plan.tables, all_relationships):
        count = row_counts.get(table, 0)
        table_plan = replace(
            plan,
            tables=(table,),
            columns=tuple(spec for spec in plan.columns if spec.table == table),
            plan_fingerprint="",
        )
        table_rows = list(generate_rows(table_plan, row_count=count, vocabularies=vocabularies))
        for simple_fk in [item for item in foreign_keys if item.child_table == table]:
            parent_rows = by_table[simple_fk.parent_table]
            parent_keys = [row.get(simple_fk.parent_column) for row in parent_rows]
            if not parent_keys and table_rows and not simple_fk.nullable:
                raise RelationalGenerationError(
                    f"no parent keys available for {table}.{simple_fk.child_column}"
                )
            assignments = _parent_assignments(simple_fk, parent_keys, len(table_rows), fanouts)
            for index, row in enumerate(table_rows):
                row[simple_fk.child_column] = (
                    None
                    if simple_fk.nullable
                    and (not parent_keys or _optional_slot(plan, simple_fk, index))
                    else assignments[index]
                )
        for composite_fk in [item for item in composite_foreign_keys if item.child_table == table]:
            parent_keys = [
                tuple(row.get(column) for column in composite_fk.parent_columns)
                for row in by_table[composite_fk.parent_table]
            ]
            if not parent_keys and table_rows and not composite_fk.nullable:
                raise RelationalGenerationError(
                    f"no parent keys available for {table}.{composite_fk.child_columns}"
                )
            assignments = _parent_assignments(composite_fk, parent_keys, len(table_rows), fanouts)
            for index, row in enumerate(table_rows):
                values: tuple[Any, ...] = (
                    (None,) * len(composite_fk.child_columns)
                    if composite_fk.nullable
                    and (not parent_keys or _optional_slot(plan, composite_fk, index))
                    else cast(tuple[Any, ...], assignments[index])
                )
                for column, value in zip(composite_fk.child_columns, values, strict=True):
                    row[column] = value
        by_table[table] = table_rows
    _assert_integrity(by_table, foreign_keys, composite_foreign_keys)
    return {table: tuple(rows) for table, rows in by_table.items()}


def _parent_assignments(
    fk: Any,
    parent_keys: list[Any],
    child_count: int,
    fanouts: dict[tuple[str, str], tuple[int, ...]],
) -> list[Any]:
    target = fanouts.get((fk.child_table, fk.parent_table))
    if target is None:
        return [parent_keys[index % len(parent_keys)] for index in range(child_count)]
    assignments: list[Any] = []
    for parent_key, count in zip(parent_keys, target, strict=True):
        assignments.extend([parent_key] * count)
    return assignments


def _validate_fanouts(
    fanouts: dict[tuple[str, str], tuple[int, ...]],
    row_counts: dict[str, int],
    foreign_keys: tuple[ForeignKeySpec, ...],
    composite: tuple[CompositeForeignKeySpec, ...],
) -> None:
    relationships: dict[tuple[str, str], Any] = {}
    for relationship in (*foreign_keys, *composite):
        fk: Any = relationship
        relationships[(fk.child_table, fk.parent_table)] = fk
    for key, distribution in fanouts.items():
        fk = relationships.get(key)
        if fk is None:
            raise RelationalGenerationError(f"fan-out target is not a declared relationship: {key}")
        if any(count < 0 for count in distribution):
            raise RelationalGenerationError("fan-out counts must not be negative")
        if getattr(fk, "optional_rate", 0.0):
            raise RelationalGenerationError(
                "explicit fan-out cannot be combined with optional_rate"
            )
        expected = row_counts[fk.parent_table]
        if len(distribution) != expected:
            raise RelationalGenerationError(
                f"fan-out for {key} must contain one count per parent row ({expected})"
            )
        if sum(distribution) != row_counts[fk.child_table]:
            raise RelationalGenerationError(
                f"fan-out for {key} must sum to the child row count ({row_counts[fk.child_table]})"
            )


def _optional_slot(plan: GenerationPlan, fk: Any, index: int) -> bool:
    """Stable Bernoulli allocation independent of table iteration order."""
    if fk.optional_rate <= 0:
        return False
    digest = hashlib.sha256(
        f"{plan.plan_fingerprint}|{fk.child_table}|{fk.parent_table}|{index}".encode()
    ).digest()
    rate = float(fk.optional_rate)
    return bool(int.from_bytes(digest[:8], "big") / 2**64 < rate)


def _validate_graph(
    tables: tuple[str, ...],
    foreign_keys: tuple[ForeignKeySpec, ...],
    composite: tuple[CompositeForeignKeySpec, ...] = (),
) -> None:
    all_fks: tuple[Any, ...] = (*foreign_keys, *composite)
    if any(fk.child_table == fk.parent_table for fk in all_fks):
        raise RelationalGenerationError(
            "self-referential foreign keys require an explicit cycle strategy"
        )
    for fk in all_fks:
        if fk.child_table not in tables or fk.parent_table not in tables:
            raise RelationalGenerationError("foreign key references a table outside the plan")
    _order_tables(tables, all_fks)


def _order_tables(tables: tuple[str, ...], foreign_keys: tuple[Any, ...]) -> tuple[str, ...]:
    remaining = set(tables)
    ordered: list[str] = []
    while remaining:
        ready = sorted(
            table
            for table in remaining
            if all(
                fk.parent_table not in remaining for fk in foreign_keys if fk.child_table == table
            )
        )
        if not ready:
            raise RelationalGenerationError("relationship graph contains an unresolved cycle")
        ordered.extend(ready)
        remaining.difference_update(ready)
    return tuple(ordered)


def _assert_integrity(
    tables: dict[str, list[dict[str, Any]]],
    foreign_keys: tuple[ForeignKeySpec, ...],
    composite: tuple[CompositeForeignKeySpec, ...] = (),
) -> None:
    for simple_fk in foreign_keys:
        parent_keys = {row.get(simple_fk.parent_column) for row in tables[simple_fk.parent_table]}
        for row in tables[simple_fk.child_table]:
            value = row.get(simple_fk.child_column)
            if value is None and simple_fk.nullable:
                continue
            if value not in parent_keys:
                raise RelationalGenerationError(
                    f"orphan foreign key in {simple_fk.child_table}.{simple_fk.child_column}"
                )
    for composite_fk in composite:
        parent_keys = {
            tuple(row.get(column) for column in composite_fk.parent_columns)
            for row in tables[composite_fk.parent_table]
        }
        for row in tables[composite_fk.child_table]:
            value = tuple(row.get(column) for column in composite_fk.child_columns)
            if composite_fk.nullable and all(item is None for item in value):
                continue
            if value not in parent_keys:
                raise RelationalGenerationError(
                    f"orphan composite foreign key in {composite_fk.child_table}.{composite_fk.child_columns}"
                )
