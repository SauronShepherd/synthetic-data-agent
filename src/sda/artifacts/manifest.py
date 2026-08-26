"""Run-level receipt contract for linked SDA workflows."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from sda.artifacts.fingerprint import fingerprint


class _FrozenDict(dict[str, str]):
    def _immutable(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("manifest mappings are immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _immutable  # type: ignore[assignment]


@dataclass(frozen=True, slots=True)
class RunManifest:
    run_id: str
    tool_name: str
    tool_version: str
    artifact_schema_version: str
    environment: str
    configuration_hash: str
    input_artifact_ids: tuple[str, ...] = ()
    output_artifact_ids: tuple[str, ...] = ()
    output_fingerprint: str | None = None
    status: str = "running"
    started_at: str = ""
    completed_at: str | None = None
    warning_count: int = 0
    error_code: str | None = None
    error_message: str | None = None
    locations: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "input_artifact_ids", tuple(self.input_artifact_ids))
        object.__setattr__(self, "output_artifact_ids", tuple(self.output_artifact_ids))
        for field_name in (
            "run_id",
            "tool_name",
            "tool_version",
            "artifact_schema_version",
            "environment",
        ):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.status not in {"running", "complete", "failed"}:
            raise ValueError("status must be running, complete, or failed")
        if self.warning_count < 0:
            raise ValueError("warning_count must not be negative")
        if self.output_fingerprint is not None and not self.output_fingerprint.strip():
            raise ValueError("output_fingerprint must not be empty when provided")
        object.__setattr__(self, "locations", _FrozenDict(self.locations))

    @property
    def manifest_id(self) -> str:
        value = asdict(self)
        for volatile in ("completed_at", "status", "warning_count", "error_code", "error_message"):
            value.pop(volatile, None)
        return f"run_manifest_{fingerprint(value)}"

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["manifest_id"] = self.manifest_id
        return value
