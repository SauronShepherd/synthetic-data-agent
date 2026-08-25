from __future__ import annotations

from sda.artifacts.models import ArtifactType


def test_generation_lifecycle_artifact_types_are_first_class() -> None:
    assert ArtifactType.GENERATION_PLAN.value == "generation_plan"
    assert ArtifactType.VALIDATION_REPORT.value == "validation_report"
    assert ArtifactType.PUBLICATION.value == "publication"
