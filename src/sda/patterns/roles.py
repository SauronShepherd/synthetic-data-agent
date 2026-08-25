from __future__ import annotations

from collections.abc import Mapping, Sequence


def assign_roles(columns: Sequence[Mapping[str, object]]) -> dict[str, tuple[str, ...]]:
    roles: dict[str, list[str]] = {
        "driver": [],
        "outcome": [],
        "entity": [],
        "lifecycle": [],
        "context": [],
        "excluded": [],
    }
    for col in columns:
        name = str(col.get("name", col.get("column_name", "")))
        kind = str(col.get("profile_kind", col.get("data_type", ""))).lower()
        sensitivity = str(col.get("sensitivity", ""))
        if sensitivity or any(
            token in name.lower() for token in ("email", "ssn", "password", "token")
        ):
            roles["excluded"].append(name)
        elif "timestamp" in kind or "date" in kind or name.lower().endswith(("_at", "_date")):
            roles["lifecycle"].append(name)
        elif name.lower().endswith(("_id", "_key")):
            roles["entity"].append(name)
        elif any(token in kind for token in ("numeric", "categor", "string", "boolean")):
            roles["driver"].append(name)
            roles["outcome"].append(name)
        else:
            roles["context"].append(name)
    return {key: tuple(sorted(set(value))) for key, value in roles.items()}
