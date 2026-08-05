"""Cheap, deterministic candidate pruning."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class KeyCandidate:
    child_table: str
    child_columns: tuple[str, ...]
    parent_table: str
    parent_columns: tuple[str, ...]
    origin: str = "inferred"
    declared_constraint: str | None = None
    hints: tuple[str, ...] = ()

    @property
    def width(self) -> int:
        return len(self.child_columns)


@dataclass(frozen=True, slots=True)
class KeyProfile:
    table: str
    columns: tuple[str, ...]
    non_null_rows: int
    distinct_non_null_values: int
    null_rate: float
    uniqueness_ratio: float
    minimal: bool
    score: float
    warnings: tuple[str, ...] = ()


def discover_key_candidates(
    table: str,
    rows: list[dict[str, Any]],
    columns: list[str],
    *,
    max_width: int = 2,
) -> list[KeyProfile]:
    """Rank exact key candidates without collecting values beyond the supplied rows."""
    from itertools import combinations

    profiles: list[KeyProfile] = []
    unique_sets: dict[tuple[str, ...], bool] = {}
    for width in range(1, min(max_width, len(columns)) + 1):
        for combination in combinations(columns, width):
            raw = [tuple(row.get(column) for column in combination) for row in rows]
            populated = [value for value in raw if all(part is not None for part in value)]
            distinct = len(set(populated))
            ratio = distinct / len(populated) if populated else 0.0
            unique = bool(populated) and ratio == 1.0 and len(populated) == len(rows)
            unique_sets[combination] = unique
            minimal = (
                unique
                and all(
                    not unique_sets.get(subset, False)
                    for subset in combinations(combination, width - 1)
                )
                if width > 1
                else unique
            )
            names = " ".join(combination).lower()
            name_bonus = 0.1 if any(token in names for token in ("_id", "_key", "_sk")) else 0.0
            score = min(1.0, ratio + name_bonus) if unique else ratio * 0.8
            warnings = () if unique else ("not_unique_in_snapshot",)
            profiles.append(
                KeyProfile(
                    table,
                    combination,
                    len(populated),
                    distinct,
                    1 - len(populated) / len(rows) if rows else 0.0,
                    ratio,
                    minimal,
                    round(score, 6),
                    warnings,
                )
            )
    return sorted(profiles, key=lambda item: (-item.score, len(item.columns), item.columns))


def discover_candidates(
    tables: dict[str, Any],
    *,
    rows: dict[str, list[dict[str, Any]]] | None = None,
    max_width: int = 3,
    max_per_table: int = 100,
) -> list[KeyCandidate]:
    """Return declared FKs plus plausible inferred pairs from metadata/profile hints."""
    out: list[KeyCandidate] = []
    declared_pairs: set[tuple[str, str]] = set()
    keys: dict[str, list[tuple[str, ...]]] = {}
    for name, table in tables.items():
        keys[name] = []
        for c in getattr(table, "constraints", ()):
            if (
                getattr(c, "kind", None)
                and str(getattr(c.kind, "value", c.kind))
                in {
                    "PRIMARY KEY",
                    "UNIQUE",
                }
                and len(c.columns) <= max_width
            ):
                keys[name].append(tuple(c.columns))
        if rows and name in rows:
            inferred = discover_key_candidates(
                name,
                rows[name],
                [column.name for column in getattr(table, "columns", ())],
                max_width=max_width,
            )
            keys[name].extend(
                profile.columns for profile in inferred if profile.minimal and profile.uniqueness_ratio == 1.0
            )
            keys[name] = list(dict.fromkeys(keys[name]))
    for child_name, child in tables.items():
        count = 0
        for constraint in getattr(child, "constraints", ()):
            parent = getattr(constraint, "referenced_table", None)
            cols = tuple(getattr(constraint, "columns", ()))
            pcols = tuple(getattr(constraint, "referenced_columns", ()))
            if parent and cols and len(cols) == len(pcols) <= max_width:
                out.append(
                    KeyCandidate(child_name, cols, parent, pcols, "declared", constraint.name)
                )
                declared_pairs.add((child_name, parent))
                count += 1
        if count >= max_per_table:
            continue
        for parent_name, parent_keys in keys.items():
            if parent_name == child_name:
                continue
            # A declared relationship is directional evidence.  If it is
            # rejected during validation, do not manufacture a reverse edge
            # from name overlap or inferred uniqueness.
            if (child_name, parent_name) in declared_pairs or (parent_name, child_name) in declared_pairs:
                continue
            child_cols = {c.name.lower(): c for c in getattr(child, "columns", ())}
            for pkey in parent_keys:
                if count >= max_per_table:
                    break
                child_match: list[str] = []
                for parent_column in pkey:
                    exact = child_cols.get(parent_column.lower())
                    parent_token = parent_name.rsplit(".", 1)[-1].lower()
                    prefixes = (
                        parent_token,
                        parent_token[:-1] if parent_token.endswith("s") else parent_token,
                    )
                    suffix = next(
                        (
                            child_cols.get(f"{prefix}_{parent_column.lower()}")
                            for prefix in prefixes
                            if child_cols.get(f"{prefix}_{parent_column.lower()}")
                        ),
                        None,
                    )
                    selected = exact or suffix
                    if selected is None:
                        break
                    child_match.append(selected.name)
                if len(child_match) == len(pkey):
                    hints = (
                        ("matching_column_names",)
                        if tuple(child_match) == pkey
                        else ("table_prefix_id_convention",)
                    )
                    out.append(
                        KeyCandidate(child_name, tuple(child_match), parent_name, pkey, hints=hints)
                    )
                    count += 1
    unique = list(dict.fromkeys(out))
    return sorted(unique, key=lambda candidate: (candidate.origin != "declared", candidate.width))
