from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from sda.artifacts.fingerprint import fingerprint


def state_transitions(
    rows: list[dict[str, Any]],
    *,
    entity_key: str,
    state_column: str,
    order_column: str,
    max_states: int = 50,
) -> dict[str, Any]:
    states = {fingerprint(r.get(state_column)) for r in rows if r.get(state_column) is not None}
    if len(states) > max_states:
        return {"transitions": (), "warnings": ("state_cardinality_exceeds_policy",)}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[fingerprint(row.get(entity_key))].append(row)
    counts: Counter[tuple[str, str]] = Counter()
    outgoing: Counter[str] = Counter()
    censored: Counter[str] = Counter()
    state_values: dict[str, Any] = {}
    for values in grouped.values():
        ordered = sorted(
            values, key=lambda r: (r.get(order_column) is None, str(r.get(order_column)))
        )
        for current, nxt in zip(ordered, ordered[1:], strict=False):
            current_key = fingerprint(current.get(state_column))
            next_key = fingerprint(nxt.get(state_column))
            state_values.setdefault(current_key, current.get(state_column))
            state_values.setdefault(next_key, nxt.get(state_column))
            counts[(current_key, next_key)] += 1
            outgoing[current_key] += 1
        if ordered:
            final_key = fingerprint(ordered[-1].get(state_column))
            state_values.setdefault(final_key, ordered[-1].get(state_column))
            censored[final_key] += 1
    return {
        "transitions": tuple(
            {
                "from_state": state_values[a],
                "to_state": state_values[b],
                "transition_count": n,
                "transition_probability": n / outgoing[a],
                "self_transition": a == b,
            }
            for (a, b), n in sorted(counts.items(), key=lambda x: str(x[0]))
        ),
        "right_censored": {state_values[key]: count for key, count in censored.items()},
        "warnings": ("unseen_transition_not_assumed_invalid",),
    }
