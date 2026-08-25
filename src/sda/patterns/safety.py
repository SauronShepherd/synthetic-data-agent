from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class SafeValueKind(StrEnum):
    LITERAL = "literal"
    REDACTED = "redacted"
    OMITTED = "omitted"


@dataclass(frozen=True, slots=True)
class SafeValue:
    kind: SafeValueKind
    value: Any = None


def safe_pattern_value(*, value: Any, column_policy: str) -> SafeValue:
    if column_policy == "allow_safe_values":
        return SafeValue(SafeValueKind.LITERAL, value)
    if column_policy == "redact_values":
        return SafeValue(SafeValueKind.REDACTED, "<redacted>")
    return SafeValue(SafeValueKind.OMITTED)
