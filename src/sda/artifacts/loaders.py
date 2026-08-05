"""Compatibility-aware loaders for durable artifact references."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from sda.artifacts.compatibility import require_supported_schema
from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType, SourceReference
from sda.runtime.errors import ArtifactCompatibilityError, ArtifactNotFoundError
from sda.metadata_models import ColumnMetadata, ConstraintKind, ConstraintMetadata, ObjectType, TableMetadata


class ArtifactStore(Protocol):
    def get_ref(self, artifact_id: str) -> Mapping[str, Any] | None: ...
    def get_rows(self, location: str, artifact_id: str) -> Sequence[Mapping[str, Any]]: ...


def load_metadata_inventory(
    spark: Any, table: str, inventory_id: str
) -> Mapping[str, Any]:
    """Load one complete persisted metadata inventory by deterministic ID."""
    if not table or "." not in table:
        raise ValueError("metadata inventory table must be qualified")
    rows = (
        spark.table(table)
        .where(f"inventory_id = '{inventory_id}'")
        .collect()
    )
    if not rows:
        raise ArtifactNotFoundError(
            "metadata inventory was not found", details={"inventory_id": inventory_id}
        )
    complete = [
        row for row in rows
        if (row.get("status", "complete") if isinstance(row, Mapping) else getattr(row, "status", "complete")) == "complete"
    ]
    if len(complete) != 1:
        raise ArtifactCompatibilityError(
            "metadata inventory has no unique COMPLETE version",
            details={"inventory_id": inventory_id, "versions": len(complete)},
        )
    row = complete[0]
    payload = row["payload"] if isinstance(row, Mapping) else row.payload
    return json.loads(str(payload))


def metadata_inventory_from_payload(payload: Mapping[str, Any]):
    """Rehydrate the exact persisted inventory, without re-querying Unity Catalog."""
    tables = []
    for raw in payload.get("tables", []):
        columns = tuple(
            ColumnMetadata(
                name=str(column["name"]),
                data_type=str(column["data_type"]),
                nullable=bool(column.get("nullable", True)),
                ordinal_position=int(column["ordinal_position"]),
                comment=column.get("comment"),
                tags=tuple(column.get("tags", ())),
            )
            for column in raw.get("columns", [])
        )
        constraints = tuple(
            ConstraintMetadata(
                name=str(constraint["name"]),
                kind=ConstraintKind(str(constraint.get("kind", "UNKNOWN"))),
                columns=tuple(constraint.get("columns", ())),
                check_clause=constraint.get("check_clause"),
                referenced_table=constraint.get("referenced_table"),
                referenced_columns=tuple(constraint.get("referenced_columns", ())),
                enforced=bool(constraint.get("enforced", False)),
                validated=bool(constraint.get("validated", False)),
            )
            for constraint in raw.get("constraints", [])
        )
        tables.append(TableMetadata(
            catalog=str(raw["catalog"]), schema=str(raw["schema"]),
            object_name=str(raw["object_name"]),
            object_type=ObjectType(str(raw.get("object_type", "UNKNOWN"))),
            raw_table_type=raw.get("raw_table_type"), owner=raw.get("owner"),
            comment=raw.get("comment"), table_tags=tuple(raw.get("table_tags", ())),
            columns=columns, constraints=constraints,
            relationship_hints=tuple(raw.get("relationship_hints", ())),
            sensitivity_signals=tuple(raw.get("sensitivity_signals", ())),
            metadata_warnings=tuple(raw.get("metadata_warnings", ())),
        ))
    from sda.metadata_models import MetadataInventory
    return MetadataInventory(
        tables=tuple(tables),
        visible_catalogs=tuple(payload.get("visible_catalogs", ())),
        selected_catalogs=tuple(payload.get("selected_catalogs", ())),
        visible_schemas=tuple((item["catalog"], item["schema"]) for item in payload.get("visible_schemas", ())),
        selected_schemas=tuple((item["catalog"], item["schema"]) for item in payload.get("selected_schemas", ())),
        provenance=dict(payload.get("provenance", {})),
        skipped_objects=tuple(payload.get("skipped_objects", ())),
        warnings=tuple(payload.get("warnings", ())),
    )


def load_artifact_ref(
    store: ArtifactStore, artifact_id: str, *, supported_schema: set[str]
) -> ArtifactRef:
    raw = store.get_ref(artifact_id)
    if raw is None:
        raise ArtifactNotFoundError(
            "artifact reference was not found", details={"artifact_id": artifact_id}
        )
    try:
        ref = _ref_from_mapping(raw)
        require_supported_schema(ref, supported_schema)
    except (ValueError, KeyError, TypeError) as exc:
        raise ArtifactCompatibilityError(
            "artifact reference is incompatible", details={"artifact_id": artifact_id}
        ) from exc
    return ref


def load_rows(store: ArtifactStore, ref: ArtifactRef) -> tuple[Mapping[str, Any], ...]:
    if ref.status is not ArtifactStatus.COMPLETE:
        raise ArtifactCompatibilityError(
            "partial artifact cannot be loaded", details={"artifact_id": ref.artifact_id}
        )
    rows = tuple(store.get_rows(ref.primary_location, ref.artifact_id))
    if not rows:
        raise ArtifactNotFoundError(
            "artifact evidence is empty or unavailable", details={"artifact_id": ref.artifact_id}
        )
    return rows


def find_complete_artifact_by_fingerprint(
    store: ArtifactStore,
    refs: Sequence[ArtifactRef],
    *,
    artifact_type: ArtifactType,
    configuration_hash: str,
) -> ArtifactRef | None:
    del store
    return next(
        (
            ref
            for ref in refs
            if ref.artifact_type is artifact_type
            and ref.status is ArtifactStatus.COMPLETE
            and ref.configuration_hash == configuration_hash
        ),
        None,
    )


def _ref_from_mapping(raw: Mapping[str, Any]) -> ArtifactRef:
    sources = tuple(SourceReference(**source) for source in raw.get("source_references", ()))
    return ArtifactRef(
        artifact_id=str(raw["artifact_id"]),
        artifact_type=ArtifactType(str(raw["artifact_type"])),
        artifact_schema_version=str(raw["artifact_schema_version"]),
        status=ArtifactStatus(str(raw["status"])),
        tool_name=str(raw["tool_name"]),
        tool_version=str(raw["tool_version"]),
        run_id=str(raw["run_id"]),
        environment=str(raw["environment"]),
        created_at=str(raw["created_at"]),
        configuration_hash=str(raw["configuration_hash"]),
        primary_location=str(raw["primary_location"]),
        related_locations=dict(raw.get("related_locations", {})),
        source_references=sources,
        checksum=str(raw["checksum"]),
        summary=str(raw["summary"]),
        warnings=tuple(raw.get("warnings", ())),
        strategy_version=str(raw.get("strategy_version", "v1")),
        completed_at=raw.get("completed_at"),
        reuse_fingerprint=str(raw.get("reuse_fingerprint", "")),
        content_checksum=raw.get("content_checksum"),
        input_artifact_ids=tuple(raw.get("input_artifact_ids", ())),
        error_code=raw.get("error_code"),
    )
