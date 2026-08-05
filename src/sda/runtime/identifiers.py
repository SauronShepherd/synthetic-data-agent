"""Validation and quoting for Unity Catalog three-level names."""

from __future__ import annotations

import re
from dataclasses import dataclass

from sda.runtime.errors import InvalidRequestError

_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class QualifiedName:
    catalog: str
    schema: str
    object_name: str

    @classmethod
    def parse(cls, value: str) -> QualifiedName:
        parts = value.split(".")
        if len(parts) != 3 or any(not _PART.fullmatch(part) for part in parts):
            raise InvalidRequestError("expected a safe catalog.schema.object name")
        return cls(*parts)

    @property
    def full_name(self) -> str:
        return ".".join((self.catalog, self.schema, self.object_name))

    @property
    def quoted(self) -> str:
        return ".".join(
            quote_identifier(part) for part in (self.catalog, self.schema, self.object_name)
        )


def quote_identifier(value: str) -> str:
    if not _PART.fullmatch(value):
        raise InvalidRequestError("unsafe Unity Catalog identifier")
    return f"`{value}`"
