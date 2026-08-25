from __future__ import annotations

from dataclasses import dataclass

from sda.patterns.models import PatternOrigin


@dataclass(frozen=True, slots=True)
class RulePrecedencePolicy:
    version: str = "sda07-precedence-v1"
    origin_rank: dict[PatternOrigin, int] | None = None

    def __post_init__(self) -> None:
        if self.origin_rank is None:
            object.__setattr__(
                self,
                "origin_rank",
                {
                    PatternOrigin.PLATFORM: 7,
                    PatternOrigin.DESTINATION_CONSTRAINT: 6,
                    PatternOrigin.DOMAIN_APPROVED: 5,
                    PatternOrigin.USER_PROVIDED: 4,
                    PatternOrigin.DECLARED: 3,
                    PatternOrigin.OBSERVED: 1,
                },
            )

    def rank(self, origin: PatternOrigin) -> int:
        return (self.origin_rank or {}).get(origin, 0)
