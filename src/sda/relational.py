"""Deterministic relational generation over synthetic key domains.

The relational layer is intentionally small and explicit. It consumes an
approved :class:`~sda.planning.GenerationPlan`, a frozen relationship artifact,
and relationship strategies. It never reads source identifiers while making
keys: parent domains are generated first and every populated child reference is
assigned from those generated domains.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Any

from sda.artifacts.fingerprint import fingerprint
from sda.generation import GenerationError, generate_rows, write_staging_rows
from sda.planning import GenerationPlan, PlanStatus
from sda.relationships.graph import DependencyGraph
from sda.validation import CheckStatus, ValidationCheck, ValidationReport, validate_tables


class _FrozenDict(dict[str, Any]):
    """JSON-compatible immutable mapping used by frozen relational contracts."""

    def _immutable(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("relational mappings are immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _immutable  # type: ignore[assignment]


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return _FrozenDict({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    return value


class RelationalGenerationError(GenerationError):
    """Raised when relational constraints cannot be satisfied safely."""


class RelationalDatasetStatus(StrEnum):
    GENERATED_AWAITING_VALIDATION = "GENERATED_AWAITING_VALIDATION"
    VALIDATED = "VALIDATED"
    APPROVED = "APPROVED"


class CycleStrategy(StrEnum):
    STAGING = "staging"
    DEFERRED = "deferred"
    BOOTSTRAP = "bootstrap"


@dataclass(frozen=True, slots=True)
class KeyDomainSpec:
    """The ordered synthetic key columns that define a table identity."""

    table: str
    columns: tuple[str, ...]
    strategy: str = "synthetic_identifier"

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", tuple(self.columns))
        if not self.table.strip() or not self.columns:
            raise ValueError("key-domain table and columns are required")
        if any(not column.strip() for column in self.columns):
            raise ValueError("key-domain columns must not be empty")
        if len(set(self.columns)) != len(self.columns):
            raise ValueError("key-domain columns must be unique and ordered")
        if self.strategy not in {"synthetic_identifier", "sequential", "structured"}:
            raise ValueError(f"unsupported synthetic key strategy: {self.strategy}")


@dataclass(frozen=True, slots=True)
class KeyDomainRecord:
    """Raw-value-free registry entry for one generated key domain."""

    table: str
    columns: tuple[str, ...]
    row_count: int
    domain_fingerprint: str
    strategy: str
    staging_location: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", tuple(self.columns))
        if not self.table.strip() or not self.columns or self.row_count < 0:
            raise ValueError("invalid key-domain record")
        if not self.domain_fingerprint.strip() or not self.strategy.strip():
            raise ValueError("key-domain record requires fingerprint and strategy")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ForeignKeySpec:
    child_table: str
    child_column: str
    parent_table: str
    parent_column: str
    nullable: bool = False
    optional_rate: float = 0.0
    cardinality: str = "many_to_one"
    relationship_id: str = ""

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
        if self.cardinality not in {"many_to_one", "one_to_one"}:
            raise ValueError("cardinality must be many_to_one or one_to_one")


@dataclass(frozen=True, slots=True)
class CompositeForeignKeySpec:
    child_table: str
    child_columns: tuple[str, ...]
    parent_table: str
    parent_columns: tuple[str, ...]
    nullable: bool = False
    optional_rate: float = 0.0
    cardinality: str = "many_to_one"
    relationship_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "child_columns", tuple(self.child_columns))
        object.__setattr__(self, "parent_columns", tuple(self.parent_columns))
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
        if len(set(self.child_columns)) != len(self.child_columns) or len(
            set(self.parent_columns)
        ) != len(self.parent_columns):
            raise ValueError("composite foreign-key columns must be unique and ordered")
        if not self.nullable and self.optional_rate:
            raise ValueError("optional_rate requires a nullable foreign key")
        if not 0 <= self.optional_rate <= 1:
            raise ValueError("optional_rate must be between zero and one")
        if self.cardinality not in {"many_to_one", "one_to_one"}:
            raise ValueError("cardinality must be many_to_one or one_to_one")


@dataclass(frozen=True, slots=True)
class BridgeTableSpec:
    """Two parent references whose pair must remain unique."""

    table: str
    left: ForeignKeySpec | CompositeForeignKeySpec
    right: ForeignKeySpec | CompositeForeignKeySpec
    pair_unique: bool = True

    def __post_init__(self) -> None:
        if (
            not self.table.strip()
            or self.left.child_table != self.table
            or self.right.child_table != self.table
        ):
            raise ValueError("bridge relationships must belong to the bridge table")
        if self.left.parent_table == self.right.parent_table:
            raise ValueError("bridge relationships must reference two parent tables")
        if self.left.nullable or self.right.nullable:
            raise ValueError("bridge foreign keys must be populated in clean mode")

    @property
    def relationships(self) -> tuple[ForeignKeySpec | CompositeForeignKeySpec, ...]:
        return (self.left, self.right)


@dataclass(frozen=True, slots=True)
class SelfReferenceSpec:
    """A self-referencing hierarchy generated from earlier levels only."""

    table: str
    child_column: str
    parent_column: str
    nullable: bool = True
    root_count: int = 1
    max_depth: int | None = None
    relationship_id: str = ""

    def __post_init__(self) -> None:
        if not self.table.strip() or not self.child_column.strip() or not self.parent_column.strip():
            raise ValueError("self-reference fields must not be empty")
        if self.root_count < 0:
            raise ValueError("root_count must not be negative")
        if self.max_depth is not None and self.max_depth < 1:
            raise ValueError("max_depth must be positive when provided")


@dataclass(frozen=True, slots=True)
class CycleStrategySpec:
    """Explicit approval for deferred assignment inside one cyclic component."""

    tables: tuple[str, ...]
    strategy: CycleStrategy | str = CycleStrategy.STAGING
    relationship_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "tables", tuple(self.tables))
        object.__setattr__(self, "relationship_ids", tuple(self.relationship_ids))
        try:
            object.__setattr__(self, "strategy", CycleStrategy(self.strategy))
        except ValueError as exc:
            raise ValueError("unsupported cycle strategy") from exc
        if not self.tables or any(not table.strip() for table in self.tables):
            raise ValueError("cycle strategy requires tables")
        if len(set(self.tables)) != len(self.tables):
            raise ValueError("cycle strategy tables must be unique")


@dataclass(frozen=True, slots=True)
class RelationalPlan:
    """An approved table plan plus a frozen relationship execution contract."""

    plan: GenerationPlan
    foreign_keys: tuple[ForeignKeySpec, ...] = ()
    composite_foreign_keys: tuple[CompositeForeignKeySpec, ...] = ()
    key_domains: tuple[KeyDomainSpec, ...] = ()
    bridge_tables: tuple[BridgeTableSpec, ...] = ()
    self_references: tuple[SelfReferenceSpec, ...] = ()
    cycle_strategies: tuple[CycleStrategySpec, ...] = ()
    relationship_artifact: Mapping[str, Any] | None = None
    graph_artifact_id: str = ""
    graph_artifact_fingerprint: str = ""
    relational_fingerprint: str = ""

    def __post_init__(self) -> None:
        if self.plan.status is not PlanStatus.APPROVED:
            raise RelationalGenerationError("relational plans require an approved generation plan")
        for name in (
            "foreign_keys",
            "composite_foreign_keys",
            "key_domains",
            "bridge_tables",
            "self_references",
            "cycle_strategies",
        ):
            object.__setattr__(self, name, tuple(getattr(self, name)))
        if self.relationship_artifact is not None:
            object.__setattr__(self, "relationship_artifact", _freeze(dict(self.relationship_artifact)))
        expected = self.compute_fingerprint()
        if self.relational_fingerprint and self.relational_fingerprint != expected:
            raise RelationalGenerationError("relational plan fingerprint does not match contents")
        if not self.relational_fingerprint:
            object.__setattr__(self, "relational_fingerprint", expected)

    def compute_fingerprint(self) -> str:
        payload = {
            "plan_fingerprint": self.plan.plan_fingerprint,
            "foreign_keys": [asdict(item) for item in self.foreign_keys],
            "composite_foreign_keys": [asdict(item) for item in self.composite_foreign_keys],
            "key_domains": [asdict(item) for item in self.key_domains],
            "bridge_tables": [asdict(item) for item in self.bridge_tables],
            "self_references": [asdict(item) for item in self.self_references],
            "cycle_strategies": [asdict(item) for item in self.cycle_strategies],
            "relationship_artifact": self.relationship_artifact,
            "graph_artifact_id": self.graph_artifact_id,
            "graph_artifact_fingerprint": self.graph_artifact_fingerprint,
        }
        return fingerprint(payload)

    @property
    def tables(self) -> tuple[str, ...]:
        return self.plan.tables


def build_relational_plan(
    plan: GenerationPlan,
    *,
    foreign_keys: tuple[ForeignKeySpec, ...] = (),
    composite_foreign_keys: tuple[CompositeForeignKeySpec, ...] = (),
    key_domains: tuple[KeyDomainSpec, ...] = (),
    bridge_tables: tuple[BridgeTableSpec, ...] = (),
    self_references: tuple[SelfReferenceSpec, ...] = (),
    cycle_strategies: tuple[CycleStrategySpec, ...] = (),
    relationship_artifact: Mapping[str, Any] | None = None,
    graph_artifact_id: str = "",
    graph_artifact_fingerprint: str = "",
) -> RelationalPlan:
    """Freeze the approved graph and relationship strategies before generation."""

    relational = RelationalPlan(
        plan,
        foreign_keys,
        composite_foreign_keys,
        key_domains,
        bridge_tables,
        self_references,
        cycle_strategies,
        relationship_artifact,
        graph_artifact_id,
        graph_artifact_fingerprint,
    )
    _validate_relationship_artifact(
        relational.relationship_artifact,
        expected_fingerprint=relational.graph_artifact_fingerprint or None,
        plan_fingerprint=plan.plan_fingerprint,
        relationships=(*foreign_keys, *composite_foreign_keys),
    )
    return relational


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
        object.__setattr__(self, "table_counts", tuple(tuple(item) for item in self.table_counts))
        object.__setattr__(
            self, "table_fingerprints", tuple(tuple(item) for item in self.table_fingerprints)
        )
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
    """Versioned dataset manifest, separate from validation and approval."""

    run_id: str
    output_namespace: str
    plan_id: str
    plan_fingerprint: str
    source_snapshot_ids: tuple[str, ...]
    input_artifact_ids: tuple[str, ...]
    receipt: RelationalGenerationReceipt
    schema_version: str = "relational-generation-manifest-v1"
    status: RelationalDatasetStatus | str = RelationalDatasetStatus.GENERATED_AWAITING_VALIDATION
    dataset_fingerprint: str = ""
    staging_namespace: str = ""
    key_domains: tuple[KeyDomainRecord, ...] = ()
    relationship_metrics: tuple[tuple[str, Mapping[str, Any]], ...] = ()
    validation_report_fingerprint: str | None = None
    approved_by: str | None = None
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_snapshot_ids", tuple(self.source_snapshot_ids))
        object.__setattr__(self, "input_artifact_ids", tuple(self.input_artifact_ids))
        object.__setattr__(self, "key_domains", tuple(self.key_domains))
        object.__setattr__(self, "relationship_metrics", tuple(self.relationship_metrics))
        object.__setattr__(self, "warnings", tuple(self.warnings))
        try:
            object.__setattr__(self, "status", RelationalDatasetStatus(self.status))
        except ValueError as exc:
            raise ValueError("unsupported relational dataset status") from exc
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
        if not self.schema_version.strip():
            raise ValueError("manifest schema_version must not be empty")
        if not self.dataset_fingerprint:
            object.__setattr__(self, "dataset_fingerprint", self.receipt.output_fingerprint)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["status"] = self.status.value
        value["receipt"] = self.receipt.to_dict()
        value["key_domains"] = [domain.to_dict() for domain in self.key_domains]
        value["relationship_metrics"] = {
            name: dict(metrics) for name, metrics in self.relationship_metrics
        }
        return value


def _relationship_token(
    relationship: ForeignKeySpec | CompositeForeignKeySpec,
) -> tuple[str, tuple[str, ...], str, tuple[str, ...]]:
    child_columns = (
        (relationship.child_column,)
        if isinstance(relationship, ForeignKeySpec)
        else relationship.child_columns
    )
    parent_columns = (
        (relationship.parent_column,)
        if isinstance(relationship, ForeignKeySpec)
        else relationship.parent_columns
    )
    return (
        relationship.child_table,
        tuple(child_columns),
        relationship.parent_table,
        tuple(parent_columns),
    )


def _relationship_key(relationship: ForeignKeySpec | CompositeForeignKeySpec) -> tuple[str, str]:
    return relationship.child_table, relationship.parent_table


def _all_relationships(relational: RelationalPlan) -> tuple[ForeignKeySpec | CompositeForeignKeySpec, ...]:
    bridge_relationships = tuple(
        relationship for bridge in relational.bridge_tables for relationship in bridge.relationships
    )
    result: list[ForeignKeySpec | CompositeForeignKeySpec] = [
        *relational.foreign_keys,
        *relational.composite_foreign_keys,
        *bridge_relationships,
    ]
    unique: list[ForeignKeySpec | CompositeForeignKeySpec] = []
    seen: set[tuple[str, tuple[str, ...], str, tuple[str, ...]]] = set()
    for relationship in result:
        token = _relationship_token(relationship)
        if token not in seen:
            unique.append(relationship)
            seen.add(token)
    return tuple(unique)


def _validate_relationship_artifact(
    artifact: Mapping[str, Any] | None,
    *,
    expected_fingerprint: str | None,
    plan_fingerprint: str,
    relationships: Sequence[ForeignKeySpec | CompositeForeignKeySpec],
) -> None:
    if artifact is None:
        return
    if bool(artifact.get("stale")) or str(artifact.get("status", "")).lower() in {
        "stale",
        "superseded",
        "failed",
        "unresolved",
    }:
        raise RelationalGenerationError("relationship or graph artifact is stale or unresolved")
    artifact_plan = artifact.get("plan_fingerprint")
    if artifact_plan and artifact_plan != plan_fingerprint:
        raise RelationalGenerationError("relationship artifact does not match the approved plan")
    actual_fingerprint = artifact.get("graph_fingerprint") or artifact.get("fingerprint")
    if expected_fingerprint and actual_fingerprint and actual_fingerprint != expected_fingerprint:
        raise RelationalGenerationError("graph artifact fingerprint mismatch; artifact is stale")
    if expected_fingerprint and not actual_fingerprint and fingerprint(artifact) != expected_fingerprint:
        raise RelationalGenerationError("graph artifact fingerprint mismatch; artifact is stale")
    requested = {_relationship_token(item): item for item in relationships}
    for candidate in artifact.get("relationships", ()):
        child = str(candidate.get("child_table", ""))
        parent = str(candidate.get("parent_table", ""))
        child_columns = tuple(candidate.get("child_columns", ()))
        parent_columns = tuple(candidate.get("parent_columns", ()))
        token = (child, child_columns, parent, parent_columns)
        if token not in requested:
            continue
        decision = candidate.get("decision", candidate.get("system_decision"))
        review_status = str(candidate.get("review_status", "")).lower()
        validation_status = str(candidate.get("validation_status", "")).lower()
        accepted_for_graph = candidate.get("accepted_for_graph", decision == "accepted")
        if (
            decision != "accepted"
            or not accepted_for_graph
            or review_status in {"required", "pending", "rejected", "stale"}
            or validation_status in {"failed", "unresolved", "stale", "untested", "unavailable"}
        ):
            raise RelationalGenerationError(
                f"relationship {child}->{parent} is rejected, unresolved, or not approved"
            )


def _validate_column_compatibility(
    plan: GenerationPlan,
    relationships: Sequence[ForeignKeySpec | CompositeForeignKeySpec],
) -> None:
    columns = {(spec.table, spec.column): spec for spec in plan.columns}

    def normalise(data_type: str) -> str:
        value = data_type.lower().replace(" ", "")
        if value in {"int", "integer", "bigint", "smallint", "tinyint", "long"}:
            return "integer"
        if value in {"float", "double", "decimal", "number"}:
            return "numeric"
        if value in {"string", "varchar", "char", "text"}:
            return "string"
        return value

    for relationship in relationships:
        child_columns = (
            (relationship.child_column,)
            if isinstance(relationship, ForeignKeySpec)
            else relationship.child_columns
        )
        parent_columns = (
            (relationship.parent_column,)
            if isinstance(relationship, ForeignKeySpec)
            else relationship.parent_columns
        )
        if len(child_columns) != len(parent_columns):
            raise RelationalGenerationError("foreign-key width is incompatible with parent key")
        for child_column, parent_column in zip(child_columns, parent_columns, strict=True):
            child_spec = columns.get((relationship.child_table, child_column))
            parent_spec = columns.get((relationship.parent_table, parent_column))
            # Older SDA plans did not declare generated FK columns. Assignment
            # creates those columns using the parent domain type. When both
            # sides are declared, incompatible types are a hard planning error.
            if child_spec and parent_spec and normalise(child_spec.data_type) != normalise(
                parent_spec.data_type
            ):
                raise RelationalGenerationError(
                    f"incompatible key types for {relationship.child_table}.{child_column} "
                    f"and {relationship.parent_table}.{parent_column}"
                )


def _normalise_key_domains(
    plan: GenerationPlan,
    relationships: Sequence[ForeignKeySpec | CompositeForeignKeySpec],
    explicit: Sequence[KeyDomainSpec],
) -> tuple[KeyDomainSpec, ...]:
    by_table = {domain.table: domain for domain in explicit}
    for domain in explicit:
        if domain.table not in plan.tables:
            raise RelationalGenerationError(f"key domain table is outside the plan: {domain.table}")
        if any(
            not any(spec.table == domain.table and spec.column == column for spec in plan.columns)
            for column in domain.columns
        ):
            raise RelationalGenerationError(f"key domain columns are not declared: {domain.table}")
    result: list[KeyDomainSpec] = []
    for table in plan.tables:
        if table in by_table:
            result.append(by_table[table])
            continue
        referenced = [
            tuple(
                (item.parent_column,)
                if isinstance(item, ForeignKeySpec)
                else item.parent_columns
            )
            for item in relationships
            if item.parent_table == table
        ]
        candidates = [candidate for candidate in referenced if candidate]
        selected: tuple[str, ...] | None = candidates[0] if candidates else None
        if selected and any(candidate != selected for candidate in candidates):
            raise RelationalGenerationError(
                f"multiple parent key domains require an explicit key domain for {table}"
            )
        specs = [spec for spec in plan.columns if spec.table == table]
        if selected is None:
            for candidate in ("id", f"{table}_id"):
                if any(spec.column == candidate for spec in specs):
                    selected = (candidate,)
                    break
        if selected is None:
            identifiers = [
                spec.column for spec in specs if spec.model.lower() in {"id", "identifier"}
            ]
            if identifiers:
                selected = (identifiers[0],)
        if selected is None:
            raise RelationalGenerationError(
                f"no synthetic key domain is declared or inferable for table {table}"
            )
        result.append(KeyDomainSpec(table, selected))
    return tuple(result)


def _validate_key_domain_specs(plan: GenerationPlan, domains: Sequence[KeyDomainSpec]) -> None:
    if {domain.table for domain in domains} != set(plan.tables):
        raise RelationalGenerationError("every planned table requires exactly one key domain")
    for domain in domains:
        if any(
            not any(spec.table == domain.table and spec.column == column for spec in plan.columns)
            for column in domain.columns
        ):
            raise RelationalGenerationError(f"key domain columns are not declared: {domain.table}")


def _cycle_deferred_relationships(
    tables: Sequence[str],
    relationships: Sequence[ForeignKeySpec | CompositeForeignKeySpec],
    strategies: Sequence[CycleStrategySpec],
) -> tuple[set[tuple[str, tuple[str, ...], str, tuple[str, ...]]], tuple[dict[str, Any], ...]]:
    graph = DependencyGraph()
    for table in tables:
        graph.add_node(table)
    for relationship in relationships:
        if relationship.child_table != relationship.parent_table:
            graph.add_edge(relationship.parent_table, relationship.child_table)
    cycles = graph.cycles()
    if not cycles:
        if strategies:
            raise RelationalGenerationError("cycle strategies were supplied but the graph is acyclic")
        return set(), ()
    deferred: set[tuple[str, tuple[str, ...], str, tuple[str, ...]]] = set()
    metrics: list[dict[str, Any]] = []
    for component in cycles:
        matches = [
            strategy
            for strategy in strategies
            if set(component).issubset(set(strategy.tables))
        ]
        if not matches:
            raise RelationalGenerationError(
                "relationship graph contains an unresolved cycle; "
                f"approve staging, deferred, or bootstrap for {list(component)}"
            )
        strategy = sorted(matches, key=lambda item: (len(item.tables), item.strategy.value))[0]
        selected = [
            relationship
            for relationship in relationships
            if relationship.child_table in strategy.tables
            and relationship.parent_table in strategy.tables
            and relationship.child_table != relationship.parent_table
            and (
                not strategy.relationship_ids
                or relationship.relationship_id in strategy.relationship_ids
            )
        ]
        if not selected:
            raise RelationalGenerationError(
                f"cycle strategy {strategy.strategy.value} does not name a relationship edge"
            )
        deferred.update(_relationship_token(item) for item in selected)
        metrics.append(
            {
                "tables": tuple(sorted(component)),
                "strategy": strategy.strategy.value,
                "deferred_relationships": tuple(_relationship_token(item) for item in selected),
            }
        )
    try:
        _order_tables(tables, relationships, deferred)
    except RelationalGenerationError as exc:
        raise RelationalGenerationError(
            "approved cycle strategies did not produce an executable dependency order"
        ) from exc
    return deferred, tuple(metrics)


def _order_tables(
    tables: Sequence[str],
    relationships: Sequence[ForeignKeySpec | CompositeForeignKeySpec],
    deferred: set[tuple[str, tuple[str, ...], str, tuple[str, ...]]] | None = None,
) -> tuple[str, ...]:
    deferred = deferred or set()
    remaining = set(tables)
    ordered: list[str] = []
    while remaining:
        ready = sorted(
            table
            for table in remaining
            if all(
                relationship.parent_table not in remaining
                or _relationship_token(relationship) in deferred
                for relationship in relationships
                if relationship.child_table == table
                and relationship.child_table != relationship.parent_table
            )
        )
        if not ready:
            raise RelationalGenerationError("relationship graph contains an unresolved cycle")
        ordered.extend(ready)
        remaining.difference_update(ready)
    return tuple(ordered)


def _resolve_row_counts(
    plan: GenerationPlan,
    row_counts: Mapping[str, int],
    fanout_distributions: Mapping[tuple[str, str], tuple[int, ...]],
    fanout_mode: str,
) -> dict[str, int]:
    if fanout_mode not in {"distribution-first", "exact-total", "strict"}:
        raise RelationalGenerationError(
            "fanout_mode must be distribution-first, exact-total, or strict"
        )
    counts = dict(row_counts)
    for (child, _), distribution in fanout_distributions.items():
        if child not in counts and fanout_mode == "distribution-first":
            counts[child] = sum(distribution)
    unknown = set(counts) - set(plan.tables)
    if unknown:
        raise RelationalGenerationError(
            f"row_counts contains tables outside the plan: {sorted(unknown)}"
        )
    missing = sorted(set(plan.tables) - set(counts))
    if missing:
        raise RelationalGenerationError(
            f"row_counts must specify every planned table; missing: {missing}"
        )
    if any(count < 0 for count in counts.values()):
        raise RelationalGenerationError("row_counts must not contain negative values")
    return counts


def _stable_order(plan: GenerationPlan, token: str, indexes: Sequence[int]) -> list[int]:
    return sorted(
        indexes,
        key=lambda index: hashlib.sha256(
            f"{plan.plan_fingerprint}|{token}|{index}".encode()
        ).digest(),
    )


def _reconcile_fanout(
    plan: GenerationPlan,
    token: str,
    initial: Sequence[int],
    target: int,
    *,
    minimum: int = 0,
    maximum: int | None = None,
    preserve_zero_child_parents: bool = True,
) -> tuple[tuple[int, ...], int]:
    if minimum < 0 or (maximum is not None and maximum < minimum):
        raise RelationalGenerationError("fanout bounds are invalid")
    values = [int(value) for value in initial]
    if any(value < 0 for value in values):
        raise RelationalGenerationError("fan-out counts must not be negative")
    if maximum is not None and any(value > maximum for value in values):
        raise RelationalGenerationError("fan-out distribution exceeds configured maximum")
    if target < 0:
        raise RelationalGenerationError("fan-out target must not be negative")
    order = _stable_order(plan, token, range(len(values)))
    adjustments = 0
    while sum(values) < target:
        candidates = [
            index
            for index in order
            if (maximum is None or values[index] < maximum)
            and (not preserve_zero_child_parents or values[index] > 0)
        ]
        if not candidates:
            raise RelationalGenerationError("exact-total fan-out target exceeds configured caps")
        for index in candidates:
            if sum(values) >= target:
                break
            values[index] += 1
            adjustments += 1
    while sum(values) > target:
        candidates = [index for index in reversed(order) if values[index] > minimum]
        if not candidates:
            raise RelationalGenerationError("exact-total fan-out target violates configured minimums")
        for index in candidates:
            if sum(values) <= target:
                break
            values[index] -= 1
            adjustments += 1
    return tuple(values), adjustments


def _prepare_fanouts(
    plan: GenerationPlan,
    row_counts: Mapping[str, int],
    relationships: Sequence[ForeignKeySpec | CompositeForeignKeySpec],
    fanout_distributions: Mapping[tuple[str, str], tuple[int, ...]],
    fanout_mode: str,
    fanout_caps: Mapping[tuple[str, str], tuple[int, int]] | None,
    preserve_zero_child_parents: Mapping[tuple[str, str], bool] | None = None,
) -> tuple[dict[tuple[str, str], tuple[int, ...]], dict[str, dict[str, Any]]]:
    relationship_map: dict[tuple[str, str], ForeignKeySpec | CompositeForeignKeySpec] = {}
    for relationship in relationships:
        key = _relationship_key(relationship)
        if key in relationship_map:
            raise RelationalGenerationError(
                f"multiple relationships share the fan-out key {key}; use one relationship per table pair"
            )
        relationship_map[key] = relationship
    prepared: dict[tuple[str, str], tuple[int, ...]] = {}
    metrics: dict[str, dict[str, Any]] = {}
    for key, distribution in fanout_distributions.items():
        relationship = relationship_map.get(key)
        if relationship is None:
            raise RelationalGenerationError(f"fan-out target is not a declared relationship: {key}")
        values = tuple(int(value) for value in distribution)
        expected = row_counts[relationship.parent_table]
        if len(values) != expected:
            raise RelationalGenerationError(
                f"fan-out for {key} must contain one count per parent row ({expected})"
            )
        if getattr(relationship, "optional_rate", 0.0):
            raise RelationalGenerationError("explicit fan-out cannot be combined with optional_rate")
        cap = (fanout_caps or {}).get(key)
        minimum, maximum = cap if cap else (0, None)
        initial_total = sum(values)
        adjustments = 0
        if fanout_mode == "exact-total":
            values, adjustments = _reconcile_fanout(
                plan,
                f"fanout:{key[0]}:{key[1]}",
                values,
                row_counts[relationship.child_table],
                minimum=minimum,
                maximum=maximum,
                preserve_zero_child_parents=(preserve_zero_child_parents or {}).get(key, True),
            )
        elif row_counts[relationship.child_table] != initial_total:
            raise RelationalGenerationError(
                f"{fanout_mode} fan-out for {key} must sum to the child row count; "
                f"it realised {initial_total} rows but "
                f"row_counts requests {row_counts[relationship.child_table]}"
            )
        if relationship.cardinality == "one_to_one" and any(value > 1 for value in values):
            raise RelationalGenerationError("one-to-one fan-out cannot assign more than one child")
        prepared[key] = values
        metrics[f"{key[0]}->{key[1]}"] = {
            "mode": fanout_mode,
            "initial_total": initial_total,
            "final_total": sum(values),
            "adjustments": adjustments,
            "zero_child_parents": sum(value == 0 for value in values),
            "max_children": max(values, default=0),
            "parent_count": len(values),
        }
    return prepared, metrics


def _optional_slot(plan: GenerationPlan, relationship: Any, index: int) -> bool:
    if relationship.optional_rate <= 0:
        return False
    digest = hashlib.sha256(
        f"{plan.plan_fingerprint}|{relationship.child_table}|{relationship.parent_table}|{index}".encode()
    ).digest()
    return bool(int.from_bytes(digest[:8], "big") / 2**64 < float(relationship.optional_rate))


def _parent_keys(
    rows: Sequence[Mapping[str, Any]],
    columns: Sequence[str],
    relationship: ForeignKeySpec | CompositeForeignKeySpec,
) -> list[Any]:
    values: list[Any] = []
    for row in rows:
        raw = tuple(row.get(column) for column in columns)
        if any(value is None for value in raw):
            raise RelationalGenerationError(
                f"parent key domain contains NULL for {relationship.parent_table}.{columns}"
            )
        values.append(raw[0] if len(raw) == 1 else raw)
    return values


def _assign_relationship(
    plan: GenerationPlan,
    by_table: dict[str, list[dict[str, Any]]],
    relationship: ForeignKeySpec | CompositeForeignKeySpec,
    fanouts: Mapping[tuple[str, str], tuple[int, ...]],
    *,
    primary_assignment: bool = False,
) -> dict[str, Any]:
    child_rows = by_table[relationship.child_table]
    child_columns = (
        (relationship.child_column,)
        if isinstance(relationship, ForeignKeySpec)
        else relationship.child_columns
    )
    parent_columns = (
        (relationship.parent_column,)
        if isinstance(relationship, ForeignKeySpec)
        else relationship.parent_columns
    )
    parent_keys = _parent_keys(by_table[relationship.parent_table], parent_columns, relationship)
    if not parent_keys and child_rows and not relationship.nullable:
        raise RelationalGenerationError(
            f"no parent keys available for {relationship.child_table}.{child_columns}"
        )
    if relationship.cardinality == "one_to_one":
        populated = sum(
            not _optional_slot(plan, relationship, index) for index in range(len(child_rows))
        )
        if populated > len(parent_keys):
            raise RelationalGenerationError(
                f"one-to-one relationship {relationship.child_table}->{relationship.parent_table} "
                "has more populated children than parent keys"
            )
    distribution = fanouts.get(_relationship_key(relationship))
    if distribution is not None:
        assignments: list[Any] = []
        for parent_key, count in zip(parent_keys, distribution, strict=True):
            assignments.extend([parent_key] * count)
        if len(assignments) != len(child_rows):
            raise RelationalGenerationError("fan-out allocation does not match child row count")
    elif relationship.cardinality == "one_to_one":
        assignments = parent_keys[: len(child_rows)]
    elif parent_keys:
        assignments = [parent_keys[index % len(parent_keys)] for index in range(len(child_rows))]
    else:
        assignments = []
    populated_count = 0
    null_count = 0
    assignment_index = 0
    for index, row in enumerate(child_rows):
        optional = relationship.nullable and _optional_slot(plan, relationship, index)
        if optional or (not parent_keys and relationship.nullable):
            values: tuple[Any, ...] = (None,) * len(child_columns)
            null_count += 1
        else:
            if assignment_index >= len(assignments):
                raise RelationalGenerationError(
                    f"relationship allocation exhausted for {relationship.child_table}->{relationship.parent_table}"
                )
            assigned = assignments[assignment_index]
            assignment_index += 1
            values = assigned if isinstance(assigned, tuple) else (assigned,)
            populated_count += 1
        for column, value in zip(child_columns, values, strict=True):
            row[column] = value
    return {
        "child_table": relationship.child_table,
        "parent_table": relationship.parent_table,
        "child_columns": tuple(child_columns),
        "parent_columns": tuple(parent_columns),
        "populated_child_rows": populated_count,
        "nullable_child_rows": null_count,
        "cardinality": relationship.cardinality,
        "deferred": not primary_assignment,
    }


def _assign_bridge(
    by_table: dict[str, list[dict[str, Any]]],
    bridge: BridgeTableSpec,
) -> dict[str, Any]:
    left_keys = _parent_keys(
        by_table[bridge.left.parent_table],
        (bridge.left.parent_column,)
        if isinstance(bridge.left, ForeignKeySpec)
        else bridge.left.parent_columns,
        bridge.left,
    )
    right_keys = _parent_keys(
        by_table[bridge.right.parent_table],
        (bridge.right.parent_column,)
        if isinstance(bridge.right, ForeignKeySpec)
        else bridge.right.parent_columns,
        bridge.right,
    )
    rows = by_table[bridge.table]
    capacity = len(left_keys) * len(right_keys)
    if bridge.pair_unique and len(rows) > capacity:
        raise RelationalGenerationError(
            f"bridge {bridge.table} requests {len(rows)} unique pairs but capacity is {capacity}"
        )
    left_columns = (
        (bridge.left.child_column,)
        if isinstance(bridge.left, ForeignKeySpec)
        else bridge.left.child_columns
    )
    right_columns = (
        (bridge.right.child_column,)
        if isinstance(bridge.right, ForeignKeySpec)
        else bridge.right.child_columns
    )
    pairs: list[tuple[Any, Any]] = []
    for index in range(len(rows)):
        if not right_keys:
            break
        left_index, right_index = divmod(index, len(right_keys))
        if left_index >= len(left_keys):
            if bridge.pair_unique:
                raise RelationalGenerationError("bridge pair allocation exhausted")
            left_index %= len(left_keys)
        pairs.append((left_keys[left_index], right_keys[right_index]))
    for row, (left_value, right_value) in zip(rows, pairs, strict=True):
        left_values = left_value if isinstance(left_value, tuple) else (left_value,)
        right_values = right_value if isinstance(right_value, tuple) else (right_value,)
        for column, value in zip(left_columns, left_values, strict=True):
            row[column] = value
        for column, value in zip(right_columns, right_values, strict=True):
            row[column] = value
    return {
        "table": bridge.table,
        "left_parent": bridge.left.parent_table,
        "right_parent": bridge.right.parent_table,
        "pair_count": len(pairs),
        "pair_unique": bridge.pair_unique,
        "capacity": capacity,
    }


def _assign_self_reference(
    plan: GenerationPlan,
    by_table: dict[str, list[dict[str, Any]]],
    spec: SelfReferenceSpec,
    primary_keys: Mapping[str, tuple[str, ...]],
) -> dict[str, Any]:
    rows = by_table[spec.table]
    key_columns = primary_keys[spec.table]
    if len(key_columns) != 1:
        raise RelationalGenerationError("self-reference currently requires a single-column synthetic key")
    key_column = key_columns[0]
    keys = _parent_keys(
        rows,
        key_columns,
        ForeignKeySpec(spec.table, spec.child_column, spec.table, key_column, nullable=True),
    )
    roots = min(spec.root_count, len(rows))
    levels = [0] * len(rows)
    for index, row in enumerate(rows):
        if index < roots:
            if not spec.nullable:
                raise RelationalGenerationError(
                    f"self-reference {spec.table}.{spec.child_column} needs a nullable root convention"
                )
            row[spec.child_column] = None
            continue
        eligible = [
            previous
            for previous in range(index)
            if spec.max_depth is None or levels[previous] < spec.max_depth - 1
        ]
        if not eligible:
            if spec.nullable:
                row[spec.child_column] = None
                roots += 1
                continue
            raise RelationalGenerationError(
                f"self-reference {spec.table} cannot satisfy its maximum depth"
            )
        digest = hashlib.sha256(
            f"{plan.plan_fingerprint}|self:{spec.table}|{index}".encode()
        ).digest()
        parent_index = int.from_bytes(digest[:8], "big") % len(eligible)
        parent_index = eligible[parent_index]
        row[spec.child_column] = keys[parent_index]
        levels[index] = levels[parent_index] + 1
    return {
        "table": spec.table,
        "relationship": f"{spec.table}.{spec.child_column}->{spec.table}.{spec.parent_column}",
        "root_count": roots,
        "max_depth": max(levels, default=0),
        "rows": len(rows),
    }


def _assert_unique_rows(
    table: str, rows: Sequence[Mapping[str, Any]], columns: Sequence[str], *, label: str
) -> None:
    values = [tuple(row.get(column) for column in columns) for row in rows]
    if any(any(value is None for value in item) for item in values):
        raise RelationalGenerationError(f"{label} {table}{tuple(columns)} contains NULL")
    hashes = [fingerprint(item) for item in values]
    if len(hashes) != len(set(hashes)):
        raise RelationalGenerationError(f"{label} {table}{tuple(columns)} is not unique")


def _assert_integrity(
    tables: dict[str, list[dict[str, Any]]],
    relationships: Sequence[ForeignKeySpec | CompositeForeignKeySpec],
    primary_keys: Mapping[str, tuple[str, ...]],
    bridges: Sequence[BridgeTableSpec],
    self_references: Sequence[SelfReferenceSpec],
) -> None:
    for table, columns in primary_keys.items():
        _assert_unique_rows(table, tables[table], columns, label="synthetic primary key")
    for relationship in relationships:
        parent_columns = (
            (relationship.parent_column,)
            if isinstance(relationship, ForeignKeySpec)
            else relationship.parent_columns
        )
        child_columns = (
            (relationship.child_column,)
            if isinstance(relationship, ForeignKeySpec)
            else relationship.child_columns
        )
        parent_values = {
            fingerprint(tuple(row.get(column) for column in parent_columns))
            for row in tables[relationship.parent_table]
        }
        populated: list[str] = []
        for row in tables[relationship.child_table]:
            values = tuple(row.get(column) for column in child_columns)
            all_null = all(value is None for value in values)
            partial = any(value is None for value in values) and not all_null
            if partial:
                raise RelationalGenerationError(
                    f"partially populated composite foreign key in {relationship.child_table}.{child_columns}"
                )
            if all_null and relationship.nullable:
                continue
            if fingerprint(values) not in parent_values:
                populated.append(repr(values))
        if populated:
            raise RelationalGenerationError(
                f"orphan foreign key in {relationship.child_table}.{child_columns}"
            )
        if relationship.cardinality == "one_to_one":
            values = [
                tuple(row.get(column) for column in child_columns)
                for row in tables[relationship.child_table]
                if not all(row.get(column) is None for column in child_columns)
            ]
            if len(values) != len({fingerprint(value) for value in values}):
                raise RelationalGenerationError(
                    f"one-to-one foreign key is not unique in {relationship.child_table}.{child_columns}"
                )
    for bridge in bridges:
        left_columns = (
            (bridge.left.child_column,)
            if isinstance(bridge.left, ForeignKeySpec)
            else bridge.left.child_columns
        )
        right_columns = (
            (bridge.right.child_column,)
            if isinstance(bridge.right, ForeignKeySpec)
            else bridge.right.child_columns
        )
        if bridge.pair_unique:
            pairs = [
                tuple(row.get(column) for column in (*left_columns, *right_columns))
                for row in tables[bridge.table]
            ]
            if len(pairs) != len({fingerprint(pair) for pair in pairs}):
                raise RelationalGenerationError(f"bridge pair duplication detected in {bridge.table}")
    for spec in self_references:
        keys = {
            fingerprint(row.get(spec.parent_column))
            for row in tables[spec.table]
        }
        for row in tables[spec.table]:
            value = row.get(spec.child_column)
            if value is not None and fingerprint(value) not in keys:
                raise RelationalGenerationError(f"orphan self-reference in {spec.table}.{spec.child_column}")


def _key_domain_records(
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    domains: Sequence[KeyDomainSpec],
    *,
    staging_namespace: str = "",
) -> tuple[KeyDomainRecord, ...]:
    records: list[KeyDomainRecord] = []
    for domain in domains:
        rows = tables[domain.table]
        values = tuple(tuple(row.get(column) for column in domain.columns) for row in rows)
        records.append(
            KeyDomainRecord(
                domain.table,
                domain.columns,
                len(rows),
                fingerprint(values),
                domain.strategy,
                f"{staging_namespace}/{domain.table}/key-domain" if staging_namespace else "",
            )
        )
    return tuple(records)


def key_domain_registry(
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    key_domains: Sequence[KeyDomainSpec],
    *,
    staging_namespace: str = "",
) -> tuple[KeyDomainRecord, ...]:
    """Build the raw-value-free registry persisted in a relational manifest."""
    return _key_domain_records(tables, tuple(key_domains), staging_namespace=staging_namespace)


def generate_relational(
    plan: GenerationPlan | RelationalPlan,
    *,
    row_counts: dict[str, int],
    foreign_keys: tuple[ForeignKeySpec, ...] = (),
    composite_foreign_keys: tuple[CompositeForeignKeySpec, ...] = (),
    vocabularies: dict[str, tuple[str, ...]] | None = None,
    fanout_distributions: dict[tuple[str, str], tuple[int, ...]] | None = None,
    fanout_mode: str = "strict",
    fanout_caps: dict[tuple[str, str], tuple[int, int]] | None = None,
    preserve_zero_child_parents: dict[tuple[str, str], bool] | None = None,
    key_domains: tuple[KeyDomainSpec, ...] = (),
    bridge_tables: tuple[BridgeTableSpec, ...] = (),
    self_references: tuple[SelfReferenceSpec, ...] = (),
    cycle_strategies: tuple[CycleStrategySpec, ...] = (),
    relationship_artifact: Mapping[str, Any] | None = None,
    expected_graph_fingerprint: str | None = None,
) -> dict[str, tuple[dict[str, Any], ...]]:
    """Generate a relational dataset from parent domains and approved edges."""
    if isinstance(plan, RelationalPlan):
        relational = plan
        if any(
            (
                foreign_keys,
                composite_foreign_keys,
                key_domains,
                bridge_tables,
                self_references,
                cycle_strategies,
                relationship_artifact,
            )
        ):
            raise RelationalGenerationError("pass relationship strategies through RelationalPlan, not twice")
    else:
        relational = build_relational_plan(
            plan,
            foreign_keys=foreign_keys,
            composite_foreign_keys=composite_foreign_keys,
            key_domains=key_domains,
            bridge_tables=bridge_tables,
            self_references=self_references,
            cycle_strategies=cycle_strategies,
            relationship_artifact=relationship_artifact,
            graph_artifact_fingerprint=expected_graph_fingerprint or "",
        )
    if relational.relationship_artifact is not None:
        _validate_relationship_artifact(
            relational.relationship_artifact,
            expected_fingerprint=expected_graph_fingerprint or relational.graph_artifact_fingerprint or None,
            plan_fingerprint=relational.plan.plan_fingerprint,
            relationships=_all_relationships(relational),
        )
    generation_plan = relational.plan
    relationships = _all_relationships(relational)
    counts = _resolve_row_counts(
        generation_plan,
        row_counts,
        fanout_distributions or {},
        fanout_mode,
    )
    domains = _normalise_key_domains(generation_plan, relationships, relational.key_domains)
    _validate_key_domain_specs(generation_plan, domains)
    _validate_column_compatibility(generation_plan, relationships)
    deferred, _cycle_metrics = _cycle_deferred_relationships(
        generation_plan.tables, relationships, relational.cycle_strategies
    )
    fanouts, _fanout_metrics = _prepare_fanouts(
        generation_plan,
        counts,
        relationships,
        fanout_distributions or {},
        fanout_mode,
        fanout_caps,
        preserve_zero_child_parents,
    )
    bridge_by_table = {bridge.table: bridge for bridge in relational.bridge_tables}
    bridge_tokens = {
        _relationship_token(relationship)
        for bridge in relational.bridge_tables
        for relationship in bridge.relationships
    }
    by_table: dict[str, list[dict[str, Any]]] = {table: [] for table in generation_plan.tables}
    order = _order_tables(generation_plan.tables, relationships, deferred)
    for table in order:
        count = counts[table]
        table_plan = replace(
            generation_plan,
            tables=(table,),
            columns=tuple(spec for spec in generation_plan.columns if spec.table == table),
            plan_fingerprint="",
            requested_row_count=count,
            budgets={
                **generation_plan.budgets,
                "max_rows": max(count, int(generation_plan.budgets.get("max_rows", count))),
            },
        )
        by_table[table] = list(generate_rows(table_plan, row_count=count, vocabularies=vocabularies))
        if table in bridge_by_table:
            _assign_bridge(by_table, bridge_by_table[table])
        else:
            for relationship in relationships:
                token = _relationship_token(relationship)
                if (
                    relationship.child_table == table
                    and token not in deferred
                    and token not in bridge_tokens
                ):
                    _assign_relationship(
                        generation_plan, by_table, relationship, fanouts, primary_assignment=True
                    )
        for self_reference in relational.self_references:
            if self_reference.table == table:
                _assign_self_reference(
                    generation_plan,
                    by_table,
                    self_reference,
                    {domain.table: domain.columns for domain in domains},
                )
    for relationship in relationships:
        token = _relationship_token(relationship)
        if token in deferred and token not in bridge_tokens:
            _assign_relationship(
                generation_plan, by_table, relationship, fanouts, primary_assignment=False
            )
    _assert_integrity(
        by_table,
        relationships,
        {domain.table: domain.columns for domain in domains},
        relational.bridge_tables,
        relational.self_references,
    )
    return {table: tuple(rows) for table, rows in by_table.items()}


def receipt_for_relational(
    plan: GenerationPlan | RelationalPlan, tables: dict[str, tuple[dict[str, Any], ...]]
) -> RelationalGenerationReceipt:
    """Create a deterministic receipt without serializing generated values."""
    generation_plan = plan.plan if isinstance(plan, RelationalPlan) else plan
    if generation_plan.status.value != "approved":
        raise RelationalGenerationError("only approved plans may be receipted")
    if set(tables) != set(generation_plan.tables):
        raise RelationalGenerationError("receipt tables must match the approved plan")
    counts = tuple((table, len(tables[table])) for table in sorted(tables))
    fingerprints = tuple((table, fingerprint(tables[table])) for table in sorted(tables))
    return RelationalGenerationReceipt(
        generation_plan.plan_id,
        generation_plan.plan_fingerprint,
        counts,
        fingerprints,
        fingerprint(fingerprints),
    )


def manifest_for_relational(
    plan: GenerationPlan | RelationalPlan,
    receipt: RelationalGenerationReceipt,
    *,
    run_id: str,
    output_namespace: str,
    key_domains: tuple[KeyDomainRecord, ...] = (),
    relationship_metrics: Mapping[str, Mapping[str, Any]] | None = None,
    staging_namespace: str = "",
    warnings: tuple[str, ...] = (),
) -> RelationalGenerationManifest:
    generation_plan = plan.plan if isinstance(plan, RelationalPlan) else plan
    if (
        receipt.plan_id != generation_plan.plan_id
        or receipt.plan_fingerprint != generation_plan.plan_fingerprint
    ):
        raise RelationalGenerationError("generation receipt does not belong to the plan")
    return RelationalGenerationManifest(
        run_id,
        output_namespace,
        generation_plan.plan_id,
        generation_plan.plan_fingerprint,
        generation_plan.source_snapshot_ids,
        generation_plan.input_artifact_ids,
        receipt,
        key_domains=key_domains,
        relationship_metrics=tuple(
            (name, _FrozenDict(metrics))
            for name, metrics in sorted((relationship_metrics or {}).items())
        ),
        staging_namespace=staging_namespace,
        warnings=warnings,
    )


def generate_relational_dataset(
    plan: GenerationPlan | RelationalPlan,
    *,
    row_counts: dict[str, int],
    run_id: str,
    output_namespace: str,
    staging_base_path: str = "",
    fanout_distributions: dict[tuple[str, str], tuple[int, ...]] | None = None,
    fanout_mode: str = "strict",
    fanout_caps: dict[tuple[str, str], tuple[int, int]] | None = None,
    preserve_zero_child_parents: dict[tuple[str, str], bool] | None = None,
) -> tuple[dict[str, tuple[dict[str, Any], ...]], RelationalGenerationManifest]:
    """Generate, receipt, and manifest a relational dataset in one local flow."""
    relational = plan if isinstance(plan, RelationalPlan) else build_relational_plan(plan)
    tables = generate_relational(
        relational,
        row_counts=row_counts,
        fanout_distributions=fanout_distributions,
        fanout_mode=fanout_mode,
        fanout_caps=fanout_caps,
        preserve_zero_child_parents=preserve_zero_child_parents,
    )
    domains = _normalise_key_domains(
        relational.plan, _all_relationships(relational), relational.key_domains
    )
    receipt = receipt_for_relational(relational, tables)
    staging_namespace = (
        run_scoped_staging_namespace(staging_base_path, run_id, receipt.output_fingerprint)
        if staging_base_path
        else ""
    )
    records = key_domain_registry(tables, domains, staging_namespace=staging_namespace)
    report = validate_relational_tables(
        tables,
        key_domains=domains,
        foreign_keys=relational.foreign_keys,
        composite_foreign_keys=relational.composite_foreign_keys,
        bridge_tables=relational.bridge_tables,
        self_references=relational.self_references,
        expected_counts=row_counts,
    )
    metrics = {
        check.check_id: dict(check.evidence)
        for check in report.checks
        if check.check_id.startswith(("relationship_metrics:", "fanout:", "bridge_pairs:", "self_reference:"))
    }
    manifest = manifest_for_relational(
        relational,
        receipt,
        run_id=run_id,
        output_namespace=output_namespace,
        key_domains=records,
        relationship_metrics=metrics,
        staging_namespace=staging_namespace,
    )
    if staging_base_path:
        stage_relational_dataset(staging_base_path, manifest, tables)
    return tables, manifest


def mark_relational_validated(
    manifest: RelationalGenerationManifest, report: ValidationReport
) -> RelationalGenerationManifest:
    """Move a generated dataset to VALIDATED only after a passing report."""
    if manifest.status is not RelationalDatasetStatus.GENERATED_AWAITING_VALIDATION:
        raise RelationalGenerationError("only generated datasets can become validated")
    if report.technical_disposition is not CheckStatus.PASS:
        raise RelationalGenerationError("failed validation cannot mark a dataset validated")
    return replace(
        manifest,
        status=RelationalDatasetStatus.VALIDATED,
        validation_report_fingerprint=report.fingerprint,
    )


def approve_relational_dataset(
    manifest: RelationalGenerationManifest, approver: str
) -> RelationalGenerationManifest:
    """Record explicit human or control-plane approval; never auto-publish."""
    if manifest.status is not RelationalDatasetStatus.VALIDATED:
        raise RelationalGenerationError("only validated datasets can be approved")
    if not approver.strip():
        raise RelationalGenerationError("dataset approval requires an approver identity")
    return replace(manifest, status=RelationalDatasetStatus.APPROVED, approved_by=approver)


def run_scoped_staging_namespace(base_path: str, run_id: str, dataset_fingerprint: str) -> str:
    """Return a run- and dataset-scoped staging path."""
    if not base_path.strip() or not run_id.strip() or not dataset_fingerprint.strip():
        raise RelationalGenerationError("staging namespace requires base path, run, and fingerprint")
    safe_run = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id)
    safe_fingerprint = re.sub(r"[^A-Za-z0-9]+", "", dataset_fingerprint)[:32]
    return str(Path(base_path).resolve() / safe_run / safe_fingerprint)


def stage_relational_dataset(
    base_path: str,
    manifest: RelationalGenerationManifest,
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
) -> str:
    """Write a dataset to run-scoped JSONL staging, idempotently."""
    destination = Path(
        run_scoped_staging_namespace(base_path, manifest.run_id, manifest.dataset_fingerprint)
    )
    manifest_path = destination / "manifest.json"
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if existing.get("dataset_fingerprint") != manifest.dataset_fingerprint:
            raise RelationalGenerationError("staging fingerprint mismatch during retry")
        return str(destination)
    destination.mkdir(parents=True, exist_ok=True)
    receipt_tables = {table for table, _ in manifest.receipt.table_counts}
    for table in sorted(tables):
        if table not in receipt_tables:
            raise RelationalGenerationError(f"table is not present in the receipt: {table}")
        write_staging_rows(str(destination / f"{table}.jsonl"), tables[table])
    temporary = destination / ".manifest.json.tmp"
    temporary.write_text(
        json.dumps(manifest.to_dict(), sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    temporary.replace(manifest_path)
    return str(destination)


def recover_relational_staging(
    base_path: str, run_id: str, dataset_fingerprint: str
) -> dict[str, Any]:
    """Load a compatible staged manifest or fail closed."""
    destination = Path(run_scoped_staging_namespace(base_path, run_id, dataset_fingerprint))
    manifest_path = destination / "manifest.json"
    if not manifest_path.exists():
        raise RelationalGenerationError("no compatible staged relational dataset exists")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if payload.get("dataset_fingerprint") != dataset_fingerprint:
        raise RelationalGenerationError("staged relational dataset fingerprint mismatch")
    return payload


def _normalise_key_argument(
    plan: GenerationPlan,
    primary_keys: Mapping[str, tuple[str, ...]] | None,
) -> tuple[KeyDomainSpec, ...]:
    return tuple(
        KeyDomainSpec(table, columns)
        for table, columns in (primary_keys or {}).items()
        if table in plan.tables
    )


def validate_relational_tables(
    tables: dict[str, tuple[dict[str, Any], ...]],
    *,
    primary_keys: Mapping[str, tuple[str, ...]] | None = None,
    key_domains: tuple[KeyDomainSpec, ...] = (),
    foreign_keys: tuple[ForeignKeySpec, ...] = (),
    composite_foreign_keys: tuple[CompositeForeignKeySpec, ...] = (),
    bridge_tables: tuple[BridgeTableSpec, ...] = (),
    self_references: tuple[SelfReferenceSpec, ...] = (),
    expected_counts: Mapping[str, int] | None = None,
    fanout_bounds: Mapping[tuple[str, str, str, str], tuple[int, int]] | None = None,
    expected_fanouts: Mapping[tuple[str, str], tuple[int, ...]] | None = None,
    intended_use: str = "qa",
) -> ValidationReport:
    """Validate keys, joins, optionality, cardinality, fan-out, and bridges."""
    bridge_simple = tuple(
        relationship
        for bridge in bridge_tables
        for relationship in bridge.relationships
        if isinstance(relationship, ForeignKeySpec)
    )
    bridge_composite = tuple(
        relationship
        for bridge in bridge_tables
        for relationship in bridge.relationships
        if isinstance(relationship, CompositeForeignKeySpec)
    )
    all_simple = tuple({
        (item.child_table, item.child_column, item.parent_table, item.parent_column): item
        for item in (*foreign_keys, *bridge_simple)
    }.values())
    all_composite = tuple({
        _relationship_token(item): item for item in (*composite_foreign_keys, *bridge_composite)
    }.values())
    domains = tuple(key_domains)
    if not domains and primary_keys:
        domains = tuple(KeyDomainSpec(table, columns) for table, columns in primary_keys.items())
    unique_keys = {
        domain.table: domain.columns[0] for domain in domains if len(domain.columns) == 1
    }
    unique_key_sets = {
        domain.table: domain.columns for domain in domains if len(domain.columns) > 1
    }
    base = validate_tables(
        tables,
        expected_counts=dict(expected_counts or {}),
        unique_keys=unique_keys,
        unique_key_sets=unique_key_sets,
        foreign_keys=tuple(
            (item.child_table, item.child_column, item.parent_table, item.parent_column)
            for item in all_simple
        ),
        fanout_bounds=dict(fanout_bounds or {}),
        intended_use=intended_use,
    )
    checks = list(base.checks)
    existing_ids = {check.check_id for check in checks}
    for relationship in all_composite:
        check_id = (
            f"foreign_key:{relationship.child_table}.({','.join(relationship.child_columns)})"
        )
        parent_rows = tables.get(relationship.parent_table)
        child_rows = tables.get(relationship.child_table)
        if parent_rows is None or child_rows is None:
            checks.append(
                ValidationCheck(
                    check_id,
                    CheckStatus.FAIL,
                    "composite foreign-key input table is unavailable",
                    {"supported": False, "reason": "table_missing"},
                    method="availability_check",
                )
            )
            continue
        parent_values = {
            fingerprint(tuple(row.get(column) for column in relationship.parent_columns))
            for row in parent_rows
        }
        populated = 0
        nulls = 0
        partial = 0
        orphans = 0
        for row in child_rows:
            values = tuple(row.get(column) for column in relationship.child_columns)
            if all(value is None for value in values):
                nulls += 1
                continue
            if any(value is None for value in values):
                partial += 1
                continue
            populated += 1
            orphans += fingerprint(values) not in parent_values
        checks.append(
            ValidationCheck(
                check_id,
                CheckStatus.PASS if partial == 0 and orphans == 0 else CheckStatus.FAIL,
                f"{orphans} composite orphan references and {partial} partial tuples detected",
                {
                    "populated_child_rows": populated,
                    "nullable_child_rows": nulls,
                    "partial_tuples": partial,
                    "orphans": orphans,
                },
                method="semi_join_and_anti_join",
                population="populated_child_rows",
            )
        )
    for relationship in (*all_simple, *all_composite):
        child_columns = (
            (relationship.child_column,)
            if isinstance(relationship, ForeignKeySpec)
            else relationship.child_columns
        )
        parent_columns = (
            (relationship.parent_column,)
            if isinstance(relationship, ForeignKeySpec)
            else relationship.parent_columns
        )
        check_id = (
            f"relationship_metrics:{relationship.child_table}->{relationship.parent_table}:"
            f"{','.join(child_columns)}"
        )
        if check_id in existing_ids:
            continue
        child_rows = tables.get(relationship.child_table, ())
        parent_rows = tables.get(relationship.parent_table, ())
        parent_values = {
            fingerprint(tuple(row.get(column) for column in parent_columns)) for row in parent_rows
        }
        child_values = [tuple(row.get(column) for column in child_columns) for row in child_rows]
        populated_values = [
            value for value in child_values if not all(item is None for item in value)
        ]
        matched = sum(fingerprint(value) in parent_values for value in populated_values)
        referenced_parent_values = {fingerprint(value) for value in populated_values}
        checks.append(
            ValidationCheck(
                check_id,
                CheckStatus.PASS if matched == len(populated_values) else CheckStatus.FAIL,
                "relationship metrics calculated",
                {
                    "populated_child_rows": len(populated_values),
                    "matched_child_rows": matched,
                    "orphan_rows": len(populated_values) - matched,
                    "nullable_child_rows": len(child_values) - len(populated_values),
                    "parent_reference_rate": (
                        len(referenced_parent_values) / len(parent_values)
                        if parent_values
                        else 1.0
                    ),
                },
                method="left_semi_and_left_anti_join_equivalent",
                population="all_child_rows",
            )
        )
        existing_ids.add(check_id)
        if relationship.cardinality == "one_to_one":
            values = [
                fingerprint(value)
                for value in populated_values
            ]
            unique_id = f"cardinality:{relationship.child_table}->{relationship.parent_table}"
            checks.append(
                ValidationCheck(
                    unique_id,
                    CheckStatus.PASS if len(values) == len(set(values)) else CheckStatus.FAIL,
                    "one-to-one child references are unique",
                    {"populated_child_rows": len(values), "distinct_references": len(set(values))},
                    method="non_null_reference_uniqueness",
                )
            )
    for bridge in bridge_tables:
        left_columns = (
            (bridge.left.child_column,)
            if isinstance(bridge.left, ForeignKeySpec)
            else bridge.left.child_columns
        )
        right_columns = (
            (bridge.right.child_column,)
            if isinstance(bridge.right, ForeignKeySpec)
            else bridge.right.child_columns
        )
        pairs = [
            tuple(row.get(column) for column in (*left_columns, *right_columns))
            for row in tables.get(bridge.table, ())
        ]
        unique_id = f"bridge_pairs:{bridge.table}"
        distinct = len({fingerprint(pair) for pair in pairs})
        checks.append(
            ValidationCheck(
                unique_id,
                CheckStatus.PASS
                if not bridge.pair_unique or distinct == len(pairs)
                else CheckStatus.FAIL,
                "bridge parent pairs are unique"
                if distinct == len(pairs)
                else "bridge parent pairs are duplicated",
                {"rows": len(pairs), "distinct_pairs": distinct, "duplicate_pairs": len(pairs) - distinct},
                method="composite_pair_uniqueness",
            )
        )
    for spec in self_references:
        key_values = {
            fingerprint(row.get(spec.parent_column)) for row in tables.get(spec.table, ())
        }
        invalid = sum(
            row.get(spec.child_column) is not None
            and fingerprint(row.get(spec.child_column)) not in key_values
            for row in tables.get(spec.table, ())
        )
        checks.append(
            ValidationCheck(
                f"self_reference:{spec.table}.{spec.child_column}",
                CheckStatus.PASS if invalid == 0 else CheckStatus.FAIL,
                f"{invalid} self-reference orphans detected",
                {"invalid": invalid, "rows": len(tables.get(spec.table, ()))},
                method="hierarchy_parent_key_lookup",
            )
        )
    for (child, parent), expected in (expected_fanouts or {}).items():
        relationship = next(
            (
                item
                for item in (*all_simple, *all_composite)
                if item.child_table == child and item.parent_table == parent
            ),
            None,
        )
        if relationship is None:
            raise RelationalGenerationError(f"expected fan-out has no relationship: {(child, parent)}")
        parent_columns = (
            (relationship.parent_column,)
            if isinstance(relationship, ForeignKeySpec)
            else relationship.parent_columns
        )
        child_columns = (
            (relationship.child_column,)
            if isinstance(relationship, ForeignKeySpec)
            else relationship.child_columns
        )
        counts: dict[str, int] = {
            fingerprint(tuple(row.get(column) for column in parent_columns)): 0
            for row in tables[parent]
        }
        for row in tables[child]:
            value = fingerprint(tuple(row.get(column) for column in child_columns))
            if value in counts:
                counts[value] += 1
        actual = tuple(counts.values())
        checks.append(
            ValidationCheck(
                f"fanout_exact:{child}->{parent}",
                CheckStatus.PASS if actual == tuple(expected) else CheckStatus.FAIL,
                "fan-out distribution matches the approved allocation"
                if actual == tuple(expected)
                else "fan-out distribution differs from the approved allocation",
                {
                    "expected": tuple(expected),
                    "actual": actual,
                    "zero_child_parents": sum(value == 0 for value in actual),
                },
                method="parent_key_grouping",
            )
        )
    disposition = (
        CheckStatus.FAIL
        if any(check.status is CheckStatus.FAIL for check in checks)
        else CheckStatus.PASS
    )
    return ValidationReport(tuple(checks), intended_use, disposition, base.validation_vector)


__all__ = [
    "BridgeTableSpec",
    "CompositeForeignKeySpec",
    "CycleStrategy",
    "CycleStrategySpec",
    "ForeignKeySpec",
    "KeyDomainRecord",
    "KeyDomainSpec",
    "RelationalDatasetStatus",
    "RelationalGenerationError",
    "RelationalGenerationManifest",
    "RelationalGenerationReceipt",
    "RelationalPlan",
    "SelfReferenceSpec",
    "approve_relational_dataset",
    "build_relational_plan",
    "generate_relational",
    "generate_relational_dataset",
    "key_domain_registry",
    "manifest_for_relational",
    "mark_relational_validated",
    "recover_relational_staging",
    "receipt_for_relational",
    "run_scoped_staging_namespace",
    "stage_relational_dataset",
    "validate_relational_tables",
]
