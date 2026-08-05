"""Scope and output safety checks for governed evidence writes."""

from __future__ import annotations

from sda.runtime.errors import AuthorizationScopeError
from sda.runtime.identifiers import QualifiedName


def require_output_scope(
    source_names: set[str],
    output_name: str,
    *,
    evidence_schema: str,
    allow_development_overlap: bool = False,
) -> None:
    """Ensure evidence output is qualified and separated from source inputs."""
    output = QualifiedName.parse(output_name)
    evidence_parts = evidence_schema.split(".")
    if len(evidence_parts) == 2:
        expected_catalog, expected_schema = evidence_parts
    elif len(evidence_parts) == 3:
        expected_catalog, expected_schema = evidence_parts[:2]
    else:
        raise AuthorizationScopeError("evidence schema must be catalog.schema")
    if output.catalog != expected_catalog or output.schema != expected_schema:
        raise AuthorizationScopeError("output is outside the configured evidence schema")
    if output.full_name in source_names and not allow_development_overlap:
        raise AuthorizationScopeError("evidence output overlaps a source table")
