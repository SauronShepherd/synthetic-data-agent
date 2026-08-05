"""Structured, user-safe SDA errors."""

from __future__ import annotations


class SdaError(Exception):
    code = "SDA_ERROR"

    def __init__(self, message: str, *, details: dict[str, str] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, object]:
        return {"error_code": self.code, "message": self.message, "details": self.details}


class InvalidRequestError(SdaError):
    code = "INVALID_REQUEST"


class AuthorizationScopeError(SdaError):
    code = "AUTHORIZATION_SCOPE"


class MetadataUnavailableError(SdaError):
    code = "METADATA_UNAVAILABLE"


class UnsupportedMetadataFeatureError(SdaError):
    code = "UNSUPPORTED_METADATA_FEATURE"


class ArtifactNotFoundError(SdaError):
    code = "ARTIFACT_NOT_FOUND"


class ArtifactCompatibilityError(SdaError, ValueError):
    code = "ARTIFACT_COMPATIBILITY"


class SourceSnapshotError(SdaError):
    code = "SOURCE_SNAPSHOT"


class CostBudgetExceededError(SdaError):
    code = "COST_BUDGET_EXCEEDED"


class PersistenceError(SdaError):
    code = "PERSISTENCE"


class PartialArtifactError(SdaError):
    code = "PARTIAL_ARTIFACT"


class RelationshipValidationError(SdaError):
    code = "RELATIONSHIP_VALIDATION"


class ReviewRequiredError(SdaError):
    code = "REVIEW_REQUIRED"
