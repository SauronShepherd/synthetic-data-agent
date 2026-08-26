"""Fail-closed publication contract for validated synthetic datasets."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum

from sda.privacy import PrivacyDecision, PrivacyReport
from sda.validation import CheckStatus, ValidationReport


class PublicationStatus(StrEnum):
    STAGED = "staged"
    VALIDATED = "validated"
    APPROVED = "approved"
    PUBLISHED = "published"
    REVOKED = "revoked"


class PublicationError(RuntimeError):
    """Raised when a publication safety gate is not satisfied."""


@dataclass(frozen=True, slots=True)
class Publication:
    dataset_id: str
    dataset_version: str
    location: str
    validation_fingerprint: str
    privacy_policy_ref: str
    status: PublicationStatus = PublicationStatus.STAGED
    published_by: str | None = None
    revocation_reason: str | None = None

    def __post_init__(self) -> None:
        if any(
            not value.strip()
            for value in (
                self.dataset_id,
                self.dataset_version,
                self.location,
                self.validation_fingerprint,
                self.privacy_policy_ref,
            )
        ):
            raise ValueError("publication identity and evidence fields must not be empty")


class PublicationRegistry:
    """In-memory reference registry; a UC/Delta implementation can persist the same contract."""

    def __init__(self) -> None:
        self._items: dict[tuple[str, str], Publication] = {}
        self._aliases: dict[str, tuple[str, str]] = {}

    def stage(self, publication: Publication) -> Publication:
        key = (publication.dataset_id, publication.dataset_version)
        existing = self._items.get(key)
        if existing is not None:
            return existing
        self._items[key] = publication
        return publication

    def publish(
        self,
        dataset_id: str,
        dataset_version: str,
        *,
        validation: ValidationReport,
        privacy: PrivacyReport,
        actor: str,
        alias: str | None = None,
    ) -> Publication:
        if not actor.strip():
            raise PublicationError("publication actor is required")
        key = (dataset_id, dataset_version)
        try:
            current = self._items[key]
        except KeyError as exc:
            raise PublicationError("dataset version is not staged") from exc
        if current.status is not PublicationStatus.STAGED:
            if current.status is PublicationStatus.VALIDATED:
                raise PublicationError("human approval is required")
            if current.status is PublicationStatus.APPROVED:
                self._check_alias_available(alias, key)
                published = replace(current, status=PublicationStatus.PUBLISHED, published_by=actor)
                self._items[key] = published
                if alias:
                    self._aliases[alias] = key
                return published
            return current
        if validation.technical_disposition is not CheckStatus.PASS:
            raise PublicationError("technical validation did not pass")
        if current.validation_fingerprint != validation.fingerprint:
            raise PublicationError("validation evidence fingerprint does not match staged artifact")
        if validation.intended_use.strip() == "":
            raise PublicationError("validation intended use is required")
        if privacy.decision is not PrivacyDecision.APPROVED:
            raise PublicationError("privacy approval is required")
        self._check_alias_available(alias, key)
        published = replace(current, status=PublicationStatus.PUBLISHED, published_by=actor)
        self._items[key] = published
        if alias:
            self._aliases[alias] = key
        return published

    def _check_alias_available(self, alias: str | None, key: tuple[str, str]) -> None:
        if alias and (previous := self._aliases.get(alias)) is not None and previous != key:
            raise PublicationError(f"alias already points to another dataset: {alias}")

    def validate(
        self, dataset_id: str, dataset_version: str, *, validation: ValidationReport
    ) -> Publication:
        """Advance a staged artifact only after its technical validation passes."""
        key = (dataset_id, dataset_version)
        try:
            current = self._items[key]
        except KeyError as exc:
            raise PublicationError("dataset version is not staged") from exc
        if current.status is not PublicationStatus.STAGED:
            return current
        if validation.technical_disposition is not CheckStatus.PASS:
            raise PublicationError("technical validation did not pass")
        if current.validation_fingerprint != validation.fingerprint:
            raise PublicationError("validation evidence fingerprint does not match staged artifact")
        validated = replace(current, status=PublicationStatus.VALIDATED)
        self._items[key] = validated
        return validated

    def approve(
        self,
        dataset_id: str,
        dataset_version: str,
        *,
        privacy: PrivacyReport,
        actor: str,
    ) -> Publication:
        """Advance a technically validated artifact after independent approvals."""
        if not actor.strip():
            raise PublicationError("approval actor is required")
        key = (dataset_id, dataset_version)
        try:
            current = self._items[key]
        except KeyError as exc:
            raise PublicationError("dataset version is not staged") from exc
        if current.status is not PublicationStatus.VALIDATED:
            if current.status is PublicationStatus.APPROVED:
                return current
            raise PublicationError("technical validation is required before approval")
        if privacy.decision is not PrivacyDecision.APPROVED:
            raise PublicationError("privacy approval is required")
        approved = replace(current, status=PublicationStatus.APPROVED, published_by=actor)
        self._items[key] = approved
        return approved

    def revoke(self, dataset_id: str, dataset_version: str, *, reason: str) -> Publication:
        if not reason.strip():
            raise PublicationError("revocation reason is required")
        key = (dataset_id, dataset_version)
        try:
            current = self._items[key]
        except KeyError as exc:
            raise PublicationError("unknown dataset version") from exc
        revoked = replace(current, status=PublicationStatus.REVOKED, revocation_reason=reason)
        self._items[key] = revoked
        for alias, target in tuple(self._aliases.items()):
            if target == key:
                del self._aliases[alias]
        return revoked
