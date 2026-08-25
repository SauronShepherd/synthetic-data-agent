from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class GenerationAction:
    kind: str
    evidence_pattern_id: str
    condition: tuple[str, ...] = ()
    fallback_levels: tuple[tuple[str, ...], ...] = ()

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ValidationAction:
    kind: str
    evidence_pattern_id: str
    metric: str | None = None
    tolerance: float | None = None

    def to_dict(self):
        return asdict(self)
