from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any


def state_transitions(
    rows: list[dict[str, Any]],
    *,
    entity_key: str,
    state_column: str,
    order_column: str,
    max_states: int = 50,
) -> dict[str, Any]:
    states = {r.get(state_column) for r in rows if r.get(state_column) is not None}
    if len(states) > max_states:
        return {"transitions": (), "warnings": ("state_cardinality_exceeds_policy",)}
    grouped: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row.get(entity_key)].append(row)
    counts: Counter[tuple[Any, Any]] = Counter()
    outgoing: Counter[Any] = Counter()
    censored: Counter[Any] = Counter()
    for values in grouped.values():
        ordered = sorted(values, key=lambda r: (r.get(order_column) is None, r.get(order_column)))
        for current, nxt in zip(ordered, ordered[1:], strict=False):
            counts[(current.get(state_column), nxt.get(state_column))] += 1
            outgoing[current.get(state_column)] += 1
        if ordered:
            censored[ordered[-1].get(state_column)] += 1
    return {
        "transitions": tuple(
            {
                "from_state": a,
                "to_state": b,
                "transition_count": n,
                "transition_probability": n / outgoing[a],
                "self_transition": a == b,
            }
            for (a, b), n in sorted(counts.items(), key=lambda x: str(x[0]))
        ),
        "right_censored": dict(censored),
        "warnings": ("unseen_transition_not_assumed_invalid",),
    }
