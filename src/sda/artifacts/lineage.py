"""Helpers for translating metadata/profile objects into source lineage."""

from __future__ import annotations

from typing import Any

from sda.artifacts.models import SourceReference
from sda.metadata_models import TableMetadata
from sda.profile_models import TableProfile


def source_reference_from_table(
    table: TableMetadata, *, inventory_id: str | None = None
) -> SourceReference:
    """Create metadata-only lineage for a discovered Unity Catalog table."""
    return SourceReference(
        full_name=table.full_name,
        object_type=table.object_type.value,
        snapshot_kind="metadata_only",
        source_version=None,
        snapshot_timestamp=None,
        metadata_fingerprint=None,
        selected_columns=tuple(column.name for column in table.columns),
        metadata_inventory_id=inventory_id,
    )


def source_reference_from_profile(
    profile: TableProfile,
    *,
    inventory_id: str | None = None,
) -> SourceReference:
    """Create source lineage from a completed table profile."""
    snapshot_kind = profile.snapshot_kind
    if snapshot_kind == "provided":
        snapshot_kind = "delta_version"
    elif snapshot_kind not in {"delta_version", "timestamp", "best_effort", "failed"}:
        snapshot_kind = "best_effort" if profile.snapshot_reproducible is False else "timestamp"
    return SourceReference(
        full_name=profile.source_table,
        object_type=profile.source_object_type,
        snapshot_kind=snapshot_kind,
        source_version=profile.source_version,
        snapshot_timestamp=profile.profile_reference_time,
        metadata_fingerprint=profile.metadata_fingerprint,
        selected_columns=tuple(column.column_name for column in profile.column_profiles),
        metadata_inventory_id=inventory_id,
    )


def lineage_summary(reference: SourceReference) -> dict[str, Any]:
    """Return a compact JSON-safe lineage summary for logs and headers."""
    return {
        "full_name": reference.full_name,
        "snapshot_kind": reference.snapshot_kind,
        "source_version": reference.source_version,
        "selected_column_count": len(reference.selected_columns),
        "metadata_inventory_id": reference.metadata_inventory_id,
    }
