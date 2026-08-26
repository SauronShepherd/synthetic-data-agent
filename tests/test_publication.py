from __future__ import annotations

import pytest

from sda.privacy import PrivacyDecision, PrivacyReport
from sda.publication import Publication, PublicationError, PublicationRegistry, PublicationStatus
from sda.validation import CheckStatus, ValidationCheck, ValidationReport


def reports(*, valid: bool = True, private: bool = True) -> tuple[ValidationReport, PrivacyReport]:
    validation = ValidationReport(
        (ValidationCheck("schema", CheckStatus.PASS if valid else CheckStatus.FAIL, "ok", {}),),
        "qa",
        CheckStatus.PASS if valid else CheckStatus.FAIL,
    )
    privacy = PrivacyReport(
        PrivacyDecision.APPROVED if private else PrivacyDecision.REVIEW_REQUIRED, (), "strict"
    )
    return validation, privacy


def staged() -> tuple[PublicationRegistry, Publication]:
    registry = PublicationRegistry()
    validation, _ = reports()
    item = registry.stage(
        Publication("dataset", "v1", "uc.schema.table", validation.fingerprint, "strict")
    )
    return registry, item


def test_publication_requires_both_gates_and_supports_alias() -> None:
    registry, _ = staged()
    validation, privacy = reports()
    result = registry.publish(
        "dataset", "v1", validation=validation, privacy=privacy, actor="reviewer", alias="latest"
    )
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
    registry.publish(
        "dataset", "v1", validation=validation, privacy=privacy, actor="reviewer", alias="latest"
    )
    revoked = registry.revoke("dataset", "v1", reason="policy change")
    assert revoked.status is PublicationStatus.REVOKED


def test_publication_exposes_validated_and_approved_lifecycle_states() -> None:
    registry, _ = staged()
    validation, privacy = reports()
    assert (
        registry.validate("dataset", "v1", validation=validation).status
        is PublicationStatus.VALIDATED
    )
    assert (
        registry.approve("dataset", "v1", privacy=privacy, actor="reviewer").status
        is PublicationStatus.APPROVED
    )
    assert (
        registry.publish(
            "dataset",
            "v1",
            validation=validation,
            privacy=privacy,
            actor="reviewer",
            alias="latest",
        ).status
        is PublicationStatus.PUBLISHED
    )


def test_publication_rejects_mismatched_validation_evidence() -> None:
    registry, _ = staged()
    validation, privacy = reports()
    changed = ValidationReport((), "qa", CheckStatus.PASS)
    with pytest.raises(PublicationError, match="fingerprint"):
        registry.publish("dataset", "v1", validation=changed, privacy=privacy, actor="reviewer")
    assert (
        registry.publish(
            "dataset", "v1", validation=validation, privacy=privacy, actor="reviewer"
        ).status
        is PublicationStatus.PUBLISHED
    )
