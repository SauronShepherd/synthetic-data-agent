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
    return tuple(result[: cfg.max_candidates])
