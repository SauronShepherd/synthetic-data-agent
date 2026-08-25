"""Check-level quality validation for generated data."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class CheckStatus(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    NOT_APPLICABLE = "NOT_APPLICABLE"


@dataclass(frozen=True, slots=True)
class ValidationCheck:
    check_id: str
    status: CheckStatus
    message: str
    evidence: dict[str, Any]
    threshold: float | int | str | None = None
    method: str = "deterministic"


@dataclass(frozen=True, slots=True)
class ValidationReport:
    checks: tuple[ValidationCheck, ...]
    intended_use: str
    technical_disposition: CheckStatus


def validate_tables(
    tables: dict[str, tuple[dict[str, Any], ...]],
    *,
    expected_counts: dict[str, int] | None = None,
    unique_keys: dict[str, str] | None = None,
    foreign_keys: tuple[tuple[str, str, str, str], ...] = (),
    intended_use: str = "unspecified",
) -> ValidationReport:
    checks: list[ValidationCheck] = []
    expected_counts = expected_counts or {}
    unique_keys = unique_keys or {}
    for table, expected in expected_counts.items():
        actual = len(tables.get(table, ()))
        checks.append(ValidationCheck(
            f"row_count:{table}", CheckStatus.PASS if actual == expected else CheckStatus.FAIL,
            f"{table} contains {actual} rows; expected {expected}", {"actual": actual, "expected": expected}, expected,
        ))
    for table, column in unique_keys.items():
        values = [row.get(column) for row in tables.get(table, ())]
        unique = len(values) == len(set(values)) and None not in values
        checks.append(ValidationCheck(
            f"unique:{table}.{column}", CheckStatus.PASS if unique else CheckStatus.FAIL,
            f"{table}.{column} is {'unique and non-null' if unique else 'not unique or contains nulls'}",
            {"rows": len(values), "distinct": len(set(values))},
        ))
    for child, child_column, parent, parent_column in foreign_keys:
        parent_values = {row.get(parent_column) for row in tables.get(parent, ())}
        child_values = [row.get(child_column) for row in tables.get(child, ())]
        orphans = sum(value is not None and value not in parent_values for value in child_values)
        checks.append(ValidationCheck(
            f"foreign_key:{child}.{child_column}", CheckStatus.PASS if orphans == 0 else CheckStatus.FAIL,
            f"{orphans} orphan references detected", {"orphans": orphans, "child_rows": len(child_values)},
        ))
    disposition = CheckStatus.FAIL if any(c.status is CheckStatus.FAIL for c in checks) else CheckStatus.PASS
    return ValidationReport(tuple(checks), intended_use, disposition)
