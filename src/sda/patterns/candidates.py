from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import combinations

from sda.patterns.models import PatternConfig, PatternFamily


@dataclass(frozen=True, slots=True)
class PatternCandidate:
    family: PatternFamily
    table: str
    driver_columns: tuple[str, ...] = ()
    outcome_columns: tuple[str, ...] = ()
    condition: Mapping[str, object] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        object.__setattr__(self, "driver_columns", tuple(self.driver_columns))
        object.__setattr__(self, "outcome_columns", tuple(self.outcome_columns))
        if self.condition is not None:
            object.__setattr__(self, "condition", _freeze(self.condition))


class _FrozenMapping(dict[str, object]):
    def _immutable(self, *args: object, **kwargs: object) -> None:
        raise TypeError("pattern candidates are immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _immutable  # type: ignore[assignment]


def _freeze(value: object) -> object:
    if isinstance(value, dict):
        return _FrozenMapping({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class NumericCorrelationCandidate:
    table_fqn: str
    left: str
    right: str
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ConditionalCandidate:
    table_fqn: str
    drivers: tuple[str, ...]
    outcome: str
    outcome_kind: str
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class FanoutSegmentCandidate:
    parent_table: str
    child_table: str
    relationship_id: str
    parent_key: tuple[str, ...]
    child_key: tuple[str, ...]
    segment_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TemporalOrderCandidate:
    table_fqn: str
    earlier_column: str
    later_column: str
    condition: Mapping[str, object] | None = None
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class StateTransitionCandidate:
    table_fqn: str
    entity_key: tuple[str, ...]
    state_column: str
    event_time_column: str
    ingestion_time_column: str | None = None
    tie_breaker_columns: tuple[str, ...] = ()


def generate_candidates(
    table: str,
    columns: Mapping[str, str],
    *,
    roles: Mapping[str, Sequence[str]] | None = None,
    config: PatternConfig | None = None,
) -> tuple[PatternCandidate, ...]:
    cfg = config or PatternConfig()
    roles = roles or {"driver": tuple(columns), "outcome": tuple(columns)}
    drivers = tuple(sorted(set(roles.get("driver", ()))))
    outcomes = tuple(sorted(set(roles.get("outcome", ()))))
    result: list[PatternCandidate] = []
    for left, right in combinations(outcomes, 2):
        if left in drivers and right in drivers:
            result.append(PatternCandidate(PatternFamily.CORRELATION, table, (left,), (right,)))
    for driver in drivers:
        for outcome in outcomes:
            if driver != outcome:
                result.append(
                    PatternCandidate(
                        PatternFamily.CONDITIONAL_DISTRIBUTION, table, (driver,), (outcome,)
                    )
                )
    # These candidates are deliberately structural.  Metrics are evaluated only
    # after the bounded input adapter has established that the required columns
    # and evidence are available.
    temporal = tuple(name for name in outcomes if name.endswith(("_at", "_date", "_time", "_ts")))
    for earlier, later in combinations(temporal, 2):
        result.append(PatternCandidate(PatternFamily.TEMPORAL_ORDER, table, (earlier,), (later,)))
        result.append(PatternCandidate(PatternFamily.TEMPORAL_LAG, table, (earlier,), (later,)))
    entity_columns = tuple(
        name for name in outcomes if name.lower().endswith("_id") or name.lower() == "id"
    )
    state_columns = tuple(name for name in outcomes if name.lower() in {"status", "state", "stage"})
    for entity, state, event_time in (
        (entity, state, event_time)
        for entity in entity_columns
        for state in state_columns
        for event_time in temporal
    ):
        result.append(
            PatternCandidate(
                PatternFamily.STATE_TRANSITION,
                table,
                (entity, state, event_time),
            )
        )
    for driver in drivers:
        for outcome in outcomes:
            if driver != outcome:
                result.append(
                    PatternCandidate(
                        PatternFamily.CONDITIONAL_MISSINGNESS,
                        table,
                        (driver,),
                        (outcome,),
                    )
                )
    return tuple(result[: cfg.max_candidates])
