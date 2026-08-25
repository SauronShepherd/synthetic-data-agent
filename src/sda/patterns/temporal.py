from __future__ import annotations

from typing import Any


def temporal_order(rows: list[dict[str, Any]], earlier: str, later: str) -> dict[str, Any]:
    eligible = [
        (r[earlier], r[later])
        for r in rows
        if r.get(earlier) is not None and r.get(later) is not None
    ]
    valid = sum(a <= b for a, b in eligible)
    return {
        "population_rows": len(rows),
        "eligible_rows": len(eligible),
        "valid_rows": valid,
        "violation_rows": len(eligible) - valid,
        "violation_rate": (len(eligible) - valid) / len(eligible) if eligible else None,
    }


def ordered_events(
    rows: list[dict[str, Any]],
    *,
    entity_key: str,
    event_time: str,
    state_column: str,
    tie_breakers: tuple[str, ...] = (),
    ingestion_time: str | None = None,
) -> dict[str, Any]:
    pairs = [(row.get(entity_key), row.get(event_time)) for row in rows]
    if not tie_breakers and len(pairs) != len(set(pairs)):
        return {"transitions": (), "warnings": ("temporal_tie_breaker_missing",)}
    groups: dict[Any, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(row.get(entity_key), []).append(row)
    transitions: list[dict[str, Any]] = []
    late_arrivals = 0
    for entity, values in sorted(groups.items(), key=lambda item: str(item[0])):
        event_order = sorted(
            values,
            key=lambda row: (
                row.get(event_time) is None,
                row.get(event_time),
                *(row.get(column) for column in tie_breakers),
            ),
        )
        if ingestion_time:
            ingestion_order = sorted(
                values,
                key=lambda row: (
                    row.get(ingestion_time) is None,
                    row.get(ingestion_time),
                    *(row.get(column) for column in tie_breakers),
                ),
            )
            if [row.get(state_column) for row in event_order] != [
                row.get(state_column) for row in ingestion_order
            ]:
                late_arrivals += 1
        transitions.extend(
            {
                "entity": entity,
                "from_state": current.get(state_column),
                "to_state": nxt.get(state_column),
            }
            for current, nxt in zip(event_order, event_order[1:], strict=False)
        )
    warnings = ("ingestion_order_differs_from_event_order",) if late_arrivals else ()
    return {
        "transitions": tuple(transitions),
        "late_arrival_entity_count": late_arrivals,
        "warnings": warnings,
    }
