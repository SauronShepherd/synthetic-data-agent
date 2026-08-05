"""Stable hashes for artifact identity and configuration."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, is_dataclass
from enum import StrEnum
from typing import Any


def canonical_json(value: Any) -> str:
    if is_dataclass(value) and not isinstance(value, type):
        value = asdict(value)
    if isinstance(value, StrEnum):
        value = value.value
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
