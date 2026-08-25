"""Budget enforcement and PII-safe operational audit contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum


class BudgetExceeded(RuntimeError):
    """Raised before a stage exceeds an approved execution budget."""


@dataclass(frozen=True, slots=True)
class ResourceBudget:
    max_rows: int | None = None
    max_events: int | None = None
    max_edges: int | None = None
    max_cost: float | None = None
    max_seconds: int | None = None

    def __post_init__(self) -> None:
        for name in ("max_rows", "max_events", "max_edges", "max_seconds"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must not be negative")
        if self.max_cost is not None and self.max_cost < 0:
            raise ValueError("max_cost must not be negative")


def enforce_budget(
    budget: ResourceBudget,
    *,
    rows: int = 0,
    events: int = 0,
    edges: int = 0,
    cost: float = 0.0,
    seconds: int = 0,
) -> None:
    checks = (
        ("rows", rows, budget.max_rows),
        ("events", events, budget.max_events),
        ("edges", edges, budget.max_edges),
        ("cost", cost, budget.max_cost),
        ("seconds", seconds, budget.max_seconds),
    )
    for name, actual, limit in checks:
        if limit is not None and actual > limit:
            raise BudgetExceeded(f"{name} budget exceeded: {actual} > {limit}")


class AuditLevel(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass(frozen=True, slots=True)
class AuditEvent:
    run_id: str
    event_type: str
    level: AuditLevel
    message: str
    stage: str = ""
    metadata: dict[str, str | int | float | bool] | None = None
    occurred_at: str = ""

    def __post_init__(self) -> None:
        if not self.run_id.strip() or not self.event_type.strip() or not self.message.strip():
            raise ValueError("audit identity and message must not be empty")
        if not self.occurred_at:
            object.__setattr__(self, "occurred_at", datetime.now(UTC).isoformat())
        if self.metadata and any(
            "token" in key.lower() or "secret" in key.lower() for key in self.metadata
        ):
            raise ValueError("audit metadata must not contain secret-like keys")


class AuditLog:
    """Small append-only sink used by local runs and adapter contract tests."""

    def __init__(self) -> None:
        self._events: list[AuditEvent] = []

    def append(self, event: AuditEvent) -> AuditEvent:
        self._events.append(event)
        return event

    def for_run(self, run_id: str) -> tuple[AuditEvent, ...]:
        return tuple(event for event in self._events if event.run_id == run_id)

    def snapshot(self) -> tuple[AuditEvent, ...]:
        return tuple(self._events)
