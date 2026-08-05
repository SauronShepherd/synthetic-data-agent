"""Thin entrypoint for running SDA 06 from a job or notebook."""

from __future__ import annotations

from typing import Any

from sda.relationships.detector import RelationshipDetector, RelationshipDiscoveryConfig


def detect_relationships(
    tables: dict[str, Any],
    rows: dict[str, list[dict[str, Any]]],
    *,
    run_id: str | None = None,
    config: RelationshipDiscoveryConfig | None = None,
) -> dict[str, Any]:
    """Discover, validate, score, and order relationships for a configured scope."""
    return RelationshipDetector(config).detect(tables, rows, run_id=run_id)
