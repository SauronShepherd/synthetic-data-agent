"""Translate validated SDA 03-06 evidence into a table-local analytical scope."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sda.artifacts.models import ArtifactRef, ArtifactType
from sda.metadata_models import ConstraintKind, ConstraintMetadata


def scoped_role_columns(
    source_table: str,
    schema_columns: Mapping[str, str],
    refs: tuple[ArtifactRef, ...],
) -> tuple[list[dict[str, object]], set[str]]:
    """Fail closed when supplied upstream evidence cannot describe the source."""
    if not refs:
        return (
            [{"name": name, "data_type": dtype} for name, dtype in schema_columns.items()],
            set(),
        )
    metadata_refs = [ref for ref in refs if ref.artifact_type is ArtifactType.METADATA_INVENTORY]
    profile_refs = [
        ref
        for ref in refs
        if ref.artifact_type is ArtifactType.TABLE_PROFILE
        and ref.content.get("source_table") == source_table
    ]
    if len(metadata_refs) != 1 or len(profile_refs) != 1:
        raise ValueError("source requires exactly one metadata inventory and table profile")
    inventory = metadata_refs[0].content
    tables = inventory.get("tables", ())
    source_tables = (
        [
            table
            for table in tables
            if isinstance(table, dict) and table.get("full_name") == source_table
        ]
        if isinstance(tables, (list, tuple))
        else []
    )
    if len(source_tables) != 1:
        raise ValueError("source table is absent or ambiguous in metadata inventory")
    source_metadata = source_tables[0]
    metadata_columns = _by_name(source_metadata.get("columns", ()), "name")
    profile = profile_refs[0]
    profile_columns = _by_name(profile.content.get("column_profiles", ()), "column_name")
    excluded: set[str] = set()
    selected: set[str] | None = None
    for source_ref in profile.source_references:
        if source_ref.full_name == source_table and source_ref.selected_columns:
            selected = set(source_ref.selected_columns)
    role_columns: list[dict[str, object]] = []
    for name, dtype in schema_columns.items():
        metadata = metadata_columns.get(name)
        if metadata is None:
            raise ValueError(f"source column {name!r} is missing from metadata inventory")
        profiled = profile_columns.get(name)
        if profiled is None or (selected is not None and name not in selected):
            excluded.add(name)
        signals = (
            *_signals(metadata.get("sensitivity_signals")),
            *(_signals(profiled.get("sensitivity_signals")) if profiled else ()),
        )
        role_columns.append(
            {
                "name": name,
                "data_type": dtype,
                "profile_kind": profiled.get("profile_kind", "") if profiled else "",
                "sensitivity": signals,
                "comment": metadata.get("comment"),
                "tags": metadata.get("tags", ()),
            }
        )
    return role_columns, excluded


def source_constraints(
    source_table: str, refs: tuple[ArtifactRef, ...]
) -> tuple[ConstraintMetadata, ...]:
    """Return declared source constraints from the compatible metadata artifact.

    Non-nullable column metadata is normalized into a NOT NULL constraint when
    the information-schema constraint inventory does not already contain one.
    """
    if not refs:
        return ()
    metadata_refs = [ref for ref in refs if ref.artifact_type is ArtifactType.METADATA_INVENTORY]
    if len(metadata_refs) != 1:
        raise ValueError("source constraints require exactly one metadata inventory")
    tables = metadata_refs[0].content.get("tables", ())
    matches = (
        [
            table
            for table in tables
            if isinstance(table, dict) and table.get("full_name") == source_table
        ]
        if isinstance(tables, (list, tuple))
        else []
    )
    if len(matches) != 1:
        raise ValueError("source table is absent or ambiguous in metadata inventory")
    raw = matches[0]
    constraints = [
        ConstraintMetadata(
            name=str(item["name"]),
            kind=ConstraintKind(str(item.get("kind", ConstraintKind.UNKNOWN.value))),
            columns=tuple(str(column) for column in item.get("columns", ())),
            check_clause=item.get("check_clause"),
            referenced_table=item.get("referenced_table"),
            referenced_columns=tuple(str(column) for column in item.get("referenced_columns", ())),
            enforced=bool(item.get("enforced", False)),
            validated=bool(item.get("validated", False)),
            match_option=item.get("match_option"),
            update_rule=item.get("update_rule"),
            delete_rule=item.get("delete_rule"),
            rely=item.get("rely"),
        )
        for item in raw.get("constraints", ())
        if isinstance(item, dict)
    ]
    non_null_columns = {
        column
        for constraint in constraints
        if constraint.kind is ConstraintKind.NOT_NULL
        for column in constraint.columns
    }
    # Information schema can omit a NOT NULL column association; normalized
    # nullable metadata below is the column-level source of truth for that case.
    constraints = [
        constraint
        for constraint in constraints
        if constraint.kind is not ConstraintKind.NOT_NULL or constraint.columns
    ]
    constraints.extend(
        ConstraintMetadata(
            name=f"metadata_not_null_{column['name']}",
            kind=ConstraintKind.NOT_NULL,
            columns=(str(column["name"]),),
            enforced=True,
        )
        for column in raw.get("columns", ())
        if isinstance(column, dict)
        and not bool(column.get("nullable", True))
        and str(column.get("name", "")) not in non_null_columns
    )
    return tuple(constraints)


def _by_name(value: object, key: str) -> dict[str, dict[str, Any]]:
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"upstream {key} collection is invalid")
    result: dict[str, dict[str, Any]] = {}
    for item in value:
        if not isinstance(item, dict) or not isinstance(item.get(key), str):
            raise ValueError(f"upstream {key} record is invalid")
        name = item[key]
        if name in result:
            raise ValueError(f"duplicate upstream {key}: {name}")
        result[name] = item
    return result


def _signals(value: object) -> tuple[str, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(str(signal) for signal in value if signal)
    if isinstance(value, str) and value:
        return (value,)
    return ()
