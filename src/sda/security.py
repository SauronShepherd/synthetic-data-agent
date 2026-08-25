"""Explicit authorization policies for governed SDA operations."""

from __future__ import annotations

from dataclasses import dataclass


class AuthorizationError(PermissionError):
    """Raised when an operation is outside the approved policy."""


@dataclass(frozen=True, slots=True)
class SecurityPolicy:
    allowed_sources: tuple[str, ...]
    allowed_outputs: tuple[str, ...]
    allowed_operations: tuple[str, ...] = (
        "read_metadata",
        "profile",
        "generate",
        "validate",
        "publish",
    )
    require_approval_for: tuple[str, ...] = ("generate", "publish")

    def __post_init__(self) -> None:
        if not self.allowed_sources or not self.allowed_outputs:
            raise ValueError("security policy requires source and output allowlists")

    def authorize(
        self,
        *,
        operation: str,
        source: str | None = None,
        output: str | None = None,
        approved: bool = False,
    ) -> None:
        if operation not in self.allowed_operations:
            raise AuthorizationError(f"operation is not allowed: {operation}")
        if source is not None and not _allowed_name(source, self.allowed_sources):
            raise AuthorizationError(f"source is not allowed: {source}")
        if output is not None and not _allowed_name(output, self.allowed_outputs):
            raise AuthorizationError(f"output is not allowed: {output}")
        if operation in self.require_approval_for and not approved:
            raise AuthorizationError(f"operation requires approval: {operation}")


def _allowed_name(value: str, allowlist: tuple[str, ...]) -> bool:
    return any(value == prefix or value.startswith(prefix + ".") for prefix in allowlist)
