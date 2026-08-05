from collections.abc import Sequence
from typing import Any

import pytest

from sda.artifacts.loaders import load_artifact_ref, load_rows
from sda.artifacts.models import ArtifactStatus
from sda.runtime.errors import ArtifactCompatibilityError


class Store:
    def __init__(self, raw: dict[str, Any], rows: Sequence[dict[str, Any]] = ()) -> None:
        self.raw = raw
        self.rows = tuple(rows)

    def get_ref(self, artifact_id: str) -> dict[str, Any]:
        return self.raw

    def get_rows(self, location: str, artifact_id: str) -> tuple[dict[str, Any], ...]:
        return self.rows


def raw_ref(status: str = ArtifactStatus.COMPLETE.value) -> dict[str, Any]:
    return {
        "artifact_id": "a",
        "artifact_type": "table_profile",
        "artifact_schema_version": "1.0",
        "status": status,
        "tool_name": "tool",
        "tool_version": "0.6.0",
        "run_id": "r",
        "environment": "dev",
        "created_at": "now",
        "configuration_hash": "c",
        "primary_location": "main.evidence.profiles",
        "related_locations": {},
        "source_references": [],
        "checksum": "x",
        "summary": "ok",
        "warnings": [],
    }


def test_loader_rejects_non_complete_artifact() -> None:
    with pytest.raises(ArtifactCompatibilityError):
        load_artifact_ref(Store(raw_ref("writing")), "a", supported_schema={"1.0"})


def test_loader_round_trips_complete_rows() -> None:
    ref = load_artifact_ref(Store(raw_ref()), "a", supported_schema={"1.0"})
    assert ref.status is ArtifactStatus.COMPLETE
    assert load_rows(Store(raw_ref(), [{"x": 1}]), ref) == ({"x": 1},)
