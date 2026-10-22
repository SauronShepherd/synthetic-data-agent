from __future__ import annotations

from dataclasses import replace

import pytest

from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType, SourceReference
from sda.patterns.roles import assign_roles
from sda.patterns.upstream import scoped_role_columns


def _ref(artifact_id: str, kind: ArtifactType, content: dict) -> ArtifactRef:
    return ArtifactRef(
        artifact_id=artifact_id,
        artifact_type=kind,
        artifact_schema_version="1.0",
        status=ArtifactStatus.COMPLETE,
        tool_name="test",
        tool_version="1",
        run_id="run",
        environment="dev",
        created_at="now",
        configuration_hash="cfg",
        primary_location="memory",
        related_locations={},
        source_references=(),
        checksum="checksum",
        summary="test",
        content=content,
    )


def _upstream() -> tuple[ArtifactRef, ...]:
    metadata = _ref(
        "metadata",
        ArtifactType.METADATA_INVENTORY,
        {
            "tables": [
                {
                    "full_name": "main.sales.orders",
                    "columns": [
                        {"name": "status", "sensitivity_signals": []},
                        {"name": "amount", "sensitivity_signals": []},
                        {"name": "email", "sensitivity_signals": ["personal_data"]},
                        {"name": "free_text", "sensitivity_signals": []},
                        {"name": "unprofiled", "sensitivity_signals": []},
                    ],
                }
            ]
        },
    )
    profile = _ref(
        "profile",
        ArtifactType.TABLE_PROFILE,
        {
            "source_table": "main.sales.orders",
            "column_profiles": [
                {"column_name": "status", "profile_kind": "categorical"},
                {"column_name": "amount", "profile_kind": "numeric"},
                {"column_name": "email", "profile_kind": "categorical"},
                {"column_name": "free_text", "profile_kind": "string"},
            ],
        },
    )
    return metadata, profile


def test_upstream_inventory_profile_and_sensitivity_bound_candidate_roles() -> None:
    refs = _upstream()
    role_columns, unprofiled = scoped_role_columns(
        "main.sales.orders",
        {
            "status": "string",
            "amount": "double",
            "email": "string",
            "free_text": "string",
            "unprofiled": "string",
        },
        refs,
    )
    roles = assign_roles(role_columns)
    excluded = set(roles["excluded"]) | unprofiled
    assert excluded == {"email", "free_text", "unprofiled"}
    assert {"status", "amount"} <= set(roles["outcome"]) - excluded


def test_upstream_scope_fails_closed_on_missing_metadata_or_profile() -> None:
    refs = _upstream()
    with pytest.raises(ValueError, match="missing from metadata"):
        scoped_role_columns("main.sales.orders", {"new_column": "string"}, refs)
    with pytest.raises(ValueError, match="exactly one"):
        scoped_role_columns("main.sales.orders", {"status": "string"}, refs[:1])
    wrong_inventory = replace(
        refs[0],
        content={"tables": [{"full_name": "main.other.orders", "columns": []}]},
    )
    with pytest.raises(ValueError, match="absent or ambiguous"):
        scoped_role_columns("main.sales.orders", {"status": "string"}, (wrong_inventory, refs[1]))


def test_profile_selected_columns_narrow_scope() -> None:
    metadata, profile = _upstream()
    profile = replace(
        profile,
        source_references=(
            SourceReference(
                "main.sales.orders",
                "TABLE",
                "delta_version",
                "1",
                None,
                None,
                selected_columns=("status",),
            ),
        ),
    )
    _, excluded = scoped_role_columns(
        "main.sales.orders", {"status": "string", "amount": "double"}, (metadata, profile)
    )
    assert excluded == {"amount"}
