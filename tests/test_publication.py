from __future__ import annotations

import pytest

from sda.privacy import PrivacyDecision, PrivacyReport
from sda.publication import Publication, PublicationError, PublicationRegistry, PublicationStatus
from sda.validation import CheckStatus, ValidationCheck, ValidationReport


def reports(*, valid: bool = True, private: bool = True) -> tuple[ValidationReport, PrivacyReport]:
    validation = ValidationReport(
        (ValidationCheck("schema", CheckStatus.PASS if valid else CheckStatus.FAIL, "ok", {}),),
        "qa", CheckStatus.PASS if valid else CheckStatus.FAIL,
    )
    privacy = PrivacyReport(PrivacyDecision.APPROVED if private else PrivacyDecision.REVIEW_REQUIRED, (), "strict")
    return validation, privacy


def staged() -> tuple[PublicationRegistry, Publication]:
    registry = PublicationRegistry()
    item = registry.stage(Publication("dataset", "v1", "uc.schema.table", "vf", "strict"))
    return registry, item


def test_publication_requires_both_gates_and_supports_alias() -> None:
    registry, _ = staged()
    validation, privacy = reports()
    result = registry.publish("dataset", "v1", validation=validation, privacy=privacy, actor="reviewer", alias="latest")
    assert result.status is PublicationStatus.PUBLISHED
    assert result.published_by == "reviewer"


def test_publication_rejects_failed_validation_or_privacy() -> None:
    registry, _ = staged()
    validation, privacy = reports(valid=False)
    with pytest.raises(PublicationError, match="technical"):
        registry.publish("dataset", "v1", validation=validation, privacy=privacy, actor="reviewer")

    registry, _ = staged()
    validation, privacy = reports(private=False)
    with pytest.raises(PublicationError, match="privacy"):
        registry.publish("dataset", "v1", validation=validation, privacy=privacy, actor="reviewer")


def test_revoke_removes_alias() -> None:
    registry, _ = staged()
    validation, privacy = reports()
    registry.publish("dataset", "v1", validation=validation, privacy=privacy, actor="reviewer", alias="latest")
    revoked = registry.revoke("dataset", "v1", reason="policy change")
    assert revoked.status is PublicationStatus.REVOKED
