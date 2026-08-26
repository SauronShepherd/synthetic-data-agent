"""Immutable generation-plan contracts.

The plan is the boundary between agent reasoning and deterministic execution.  It
contains references to evidence, never source rows, and is fingerprinted before it
can be approved or executed.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

from sda.artifacts.fingerprint import fingerprint


class _FrozenDict(dict[str, Any]):
    """JSON-compatible immutable mapping used inside frozen plan objects."""

    def _immutable(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("plan mappings are immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _immutable  # type: ignore[assignment]


class PlanStatus(StrEnum):
    DRAFT = "draft"
    AWAITING_APPROVAL = "awaiting_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"


class GenerationMode(StrEnum):
    CLEAN = "clean"
    NOISY = "noisy"
    STREAMING = "streaming"
    TOPOLOGY = "topology"


class RowCountMode(StrEnum):
    EXACT = "exact"
    PROBABILISTIC = "probabilistic"


@dataclass(frozen=True, slots=True)
class ColumnGenerationSpec:
    """Safe, declarative generation instructions for one column."""

    table: str
    column: str
    data_type: str
    nullable: bool = True
    model: str = "profiled"
    parameters: dict[str, str | int | float | bool] = field(default_factory=dict)
    source_evidence_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("table", "column", "data_type", "model"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must not be empty")
        if any(not value.strip() for value in self.source_evidence_ids):
            raise ValueError("source_evidence_ids must not contain empty values")
        object.__setattr__(self, "parameters", _FrozenDict(self.parameters))


@dataclass(frozen=True, slots=True)
class CrossColumnRule:
    """Approved deterministic assignment conditional on another column."""

    if_column: str
    if_value: str | int | float | bool | None
    then_column: str
    then_value: str | int | float | bool | None

    def __post_init__(self) -> None:
        if not self.if_column.strip() or not self.then_column.strip():
            raise ValueError("cross-column rule columns must not be empty")


@dataclass(frozen=True, slots=True)
class GenerationPlan:
    """Versioned immutable execution contract."""

    plan_id: str
    plan_version: int
    request_id: str
    source_snapshot_ids: tuple[str, ...]
    input_artifact_ids: tuple[str, ...]
    target_catalog: str
    target_schema: str
    tables: tuple[str, ...]
    columns: tuple[ColumnGenerationSpec, ...]
    cross_column_rules: tuple[CrossColumnRule, ...] = ()
    mode: GenerationMode = GenerationMode.CLEAN
    scale_factor: float = 1.0
    seed: int = 1729
    intended_use: str = "unspecified"
    privacy_policy_ref: str = "strict-default"
    validation_policy_ref: str = "default"
    row_count_mode: RowCountMode = RowCountMode.EXACT
    requested_row_count: int | None = None
    budgets: dict[str, int | float] = field(default_factory=dict)
    status: PlanStatus = PlanStatus.DRAFT
    plan_fingerprint: str = ""

    def __post_init__(self) -> None:
        for name in ("plan_id", "request_id", "target_catalog", "target_schema", "intended_use"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must not be empty")
        if self.plan_version < 1:
            raise ValueError("plan_version must be positive")
        if not self.source_snapshot_ids or not self.input_artifact_ids:
            raise ValueError("plans require source snapshots and input artifacts")
        if not self.tables or len(set(self.tables)) != len(self.tables):
            raise ValueError("plans require unique target tables")
        column_keys = [(column.table, column.column) for column in self.columns]
        if len(column_keys) != len(set(column_keys)):
            raise ValueError("plans require unique table columns")
        if any(column.table not in self.tables for column in self.columns):
            raise ValueError("plan columns must belong to declared target tables")
        if self.scale_factor <= 0:
            raise ValueError("scale_factor must be greater than zero")
        if self.seed < 0:
            raise ValueError("seed must not be negative")
        if self.requested_row_count is not None and self.requested_row_count < 0:
            raise ValueError("requested_row_count must not be negative")
        if any(value < 0 for value in self.budgets.values()):
            raise ValueError("budgets must not be negative")
        expected = self.compute_fingerprint()
        if self.plan_fingerprint and self.plan_fingerprint != expected:
            raise ValueError("plan_fingerprint does not match plan contents")
        if not self.plan_fingerprint:
            object.__setattr__(self, "plan_fingerprint", expected)
        object.__setattr__(self, "budgets", _FrozenDict(self.budgets))

    def _fingerprint_payload(self) -> dict[str, Any]:
        value = asdict(self)
        value.pop("plan_fingerprint", None)
        value["mode"] = self.mode.value
        value["status"] = self.status.value
        return value

    def compute_fingerprint(self) -> str:
        return fingerprint(self._fingerprint_payload())

    def transition(self, status: PlanStatus) -> GenerationPlan:
        allowed = {
            PlanStatus.DRAFT: {PlanStatus.AWAITING_APPROVAL, PlanStatus.REJECTED},
            PlanStatus.AWAITING_APPROVAL: {
                PlanStatus.APPROVED,
                PlanStatus.REJECTED,
                PlanStatus.EXPIRED,
            },
            PlanStatus.APPROVED: {PlanStatus.EXPIRED},
            PlanStatus.REJECTED: set(),
            PlanStatus.EXPIRED: set(),
        }
        if status not in allowed[self.status]:
            raise ValueError(f"invalid plan transition: {self.status.value} -> {status.value}")
        from dataclasses import replace

        updated = replace(self, status=status, plan_fingerprint="")
        return replace(updated, plan_fingerprint=updated.compute_fingerprint())

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["mode"] = self.mode.value
        value["status"] = self.status.value
        return value
