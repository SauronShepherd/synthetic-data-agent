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
    registry.validate("dataset", "v1", validation=validation)
    registry.approve("dataset", "v1", privacy=privacy, actor="reviewer")
    result = registry.publish(
        "dataset", "v1", validation=validation, privacy=privacy, actor="reviewer", alias="latest"
    )
    assert result.status is PublicationStatus.PUBLISHED
    assert result.published_by == "reviewer"


def test_staged_publication_requires_explicit_human_approval() -> None:
    registry, _ = staged()
    validation, privacy = reports()
    with pytest.raises(PublicationError, match="human approval"):
        registry.publish("dataset", "v1", validation=validation, privacy=privacy, actor="reviewer")


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
    registry.validate("dataset", "v1", validation=validation)
    registry.approve("dataset", "v1", privacy=privacy, actor="reviewer")
    registry.publish(
        "dataset", "v1", validation=validation, privacy=privacy, actor="reviewer", alias="latest"
    )
    revoked = registry.revoke("dataset", "v1", reason="policy change")
    assert revoked.status is PublicationStatus.REVOKED
    assert registry.revoke("dataset", "v1", reason="policy change") is revoked
    with pytest.raises(PublicationError, match="already revoked"):
        registry.revoke("dataset", "v1", reason="different reason")


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


def test_alias_conflict_does_not_partially_publish() -> None:
    first_registry, _ = staged()
    validation, privacy = reports()
    first_registry.validate("dataset", "v1", validation=validation)
    first_registry.approve("dataset", "v1", privacy=privacy, actor="reviewer")
    first_registry.publish(
        "dataset", "v1", validation=validation, privacy=privacy, actor="reviewer", alias="latest"
    )
    second = first_registry.stage(
        Publication("other", "v1", "uc.other", validation.fingerprint, "strict")
    )
    first_registry.validate("other", "v1", validation=validation)
    first_registry.approve("other", "v1", privacy=privacy, actor="reviewer")
    assert second.status is PublicationStatus.STAGED
    with pytest.raises(PublicationError, match="alias already"):
        first_registry.publish(
            "other", "v1", validation=validation, privacy=privacy, actor="reviewer", alias="latest"
        )


def test_staging_is_idempotent_but_rejects_conflicting_evidence() -> None:
    registry, item = staged()
    assert registry.stage(item) is item
    with pytest.raises(PublicationError, match="different evidence"):
        registry.stage(
            Publication("dataset", "v1", "uc.other.table", item.validation_fingerprint, "strict")
        )


def test_publication_rejects_mismatched_validation_evidence() -> None:
    registry, _ = staged()
    validation, privacy = reports()
    changed = ValidationReport((), "qa", CheckStatus.PASS)
    with pytest.raises(PublicationError, match="fingerprint"):
        registry.publish("dataset", "v1", validation=changed, privacy=privacy, actor="reviewer")
    assert (
        registry.validate("dataset", "v1", validation=validation)
        and registry.approve("dataset", "v1", privacy=privacy, actor="reviewer")
        and registry.publish(
            "dataset", "v1", validation=validation, privacy=privacy, actor="reviewer"
        ).status
        is PublicationStatus.PUBLISHED
    )


def test_approved_publication_binds_privacy_evidence() -> None:
    registry, _ = staged()
    validation, privacy = reports()
    registry.validate("dataset", "v1", validation=validation)
    approved = registry.approve("dataset", "v1", privacy=privacy, actor="reviewer")
    assert approved.privacy_fingerprint == privacy.fingerprint
    changed = PrivacyReport(PrivacyDecision.APPROVED, (), "different-policy")
    with pytest.raises(PublicationError, match="privacy evidence fingerprint"):
        registry.publish("dataset", "v1", validation=validation, privacy=changed, actor="reviewer")


def test_approved_publication_binds_validation_evidence() -> None:
    registry, _ = staged()
    validation, privacy = reports()
    registry.validate("dataset", "v1", validation=validation)
    registry.approve("dataset", "v1", privacy=privacy, actor="reviewer")
    changed = ValidationReport((), "qa", CheckStatus.PASS)
    with pytest.raises(PublicationError, match="validation evidence fingerprint"):
        registry.publish("dataset", "v1", validation=changed, privacy=privacy, actor="reviewer")


def test_publication_serializes_lifecycle_and_evidence_bindings() -> None:
    registry, item = staged()
    validation, privacy = reports()
    registry.validate("dataset", "v1", validation=validation)
    approved = registry.approve("dataset", "v1", privacy=privacy, actor="reviewer")
    payload = approved.to_dict()
    assert payload["status"] == "approved"
    assert payload["validation_fingerprint"] == item.validation_fingerprint
    assert payload["privacy_fingerprint"] == privacy.fingerprint
