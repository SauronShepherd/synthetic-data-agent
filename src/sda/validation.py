"""Check-level quality validation for generated data."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from sda.artifacts.fingerprint import fingerprint


class _FrozenDict(dict[str, Any]):
    def _immutable(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("validation evidence is immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _immutable  # type: ignore[assignment]


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

    def __post_init__(self) -> None:
        if not self.check_id.strip() or not self.message.strip() or not self.method.strip():
            raise ValueError("validation check identity, message, and method are required")
        object.__setattr__(self, "evidence", _FrozenDict(self.evidence))


@dataclass(frozen=True, slots=True)
class ValidationReport:
    checks: tuple[ValidationCheck, ...]
    intended_use: str
    technical_disposition: CheckStatus

    def __post_init__(self) -> None:
        if not self.intended_use.strip():
            raise ValueError("validation intended use is required")
        ids = [check.check_id for check in self.checks]
        if len(ids) != len(set(ids)):
            raise ValueError("validation check IDs must be unique")

    @property
    def fingerprint(self) -> str:
        return fingerprint(self)


def not_applicable_check(check_id: str, reason: str) -> ValidationCheck:
    """Represent an intentionally unsupported check without dropping evidence."""
    return ValidationCheck(
        check_id,
        CheckStatus.NOT_APPLICABLE,
        reason,
        {"supported": False, "reason": reason},
        method="unsupported",
    )


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
        if table not in tables:
            checks.append(
                ValidationCheck(
                    f"row_count:{table}",
                    CheckStatus.FAIL,
                    f"{table} is unavailable; cannot validate row count",
                    {"supported": False, "reason": "table_missing"},
                    expected,
                    method="availability_check",
                )
            )
            continue
        actual = len(tables.get(table, ()))
        checks.append(
            ValidationCheck(
                f"row_count:{table}",
                CheckStatus.PASS if actual == expected else CheckStatus.FAIL,
                f"{table} contains {actual} rows; expected {expected}",
                {"actual": actual, "expected": expected},
                expected,
            )
        )
    for table, column in unique_keys.items():
        if table not in tables:
            checks.append(
                ValidationCheck(
                    f"unique:{table}.{column}",
                    CheckStatus.FAIL,
                    f"{table} is unavailable; cannot validate uniqueness",
                    {"supported": False, "reason": "table_missing"},
                    method="availability_check",
                )
            )
            continue
        values = [row.get(column) for row in tables.get(table, ())]
        unique = len(values) == len(set(values)) and None not in values
        checks.append(
            ValidationCheck(
                f"unique:{table}.{column}",
                CheckStatus.PASS if unique else CheckStatus.FAIL,
                f"{table}.{column} is {'unique and non-null' if unique else 'not unique or contains nulls'}",
                {"rows": len(values), "distinct": len(set(values))},
            )
        )
    for child, child_column, parent, parent_column in foreign_keys:
        if child not in tables or parent not in tables:
            missing = child if child not in tables else parent
            checks.append(
                ValidationCheck(
                    f"foreign_key:{child}.{child_column}",
                    CheckStatus.FAIL,
                    f"{missing} is unavailable; cannot validate foreign key",
                    {"supported": False, "reason": "table_missing", "missing_table": missing},
                    method="availability_check",
                )
            )
            continue
        parent_values = {row.get(parent_column) for row in tables.get(parent, ())}
        child_values = [row.get(child_column) for row in tables.get(child, ())]
        orphans = sum(value is not None and value not in parent_values for value in child_values)
        checks.append(
            ValidationCheck(
                f"foreign_key:{child}.{child_column}",
                CheckStatus.PASS if orphans == 0 else CheckStatus.FAIL,
                f"{orphans} orphan references detected",
                {"orphans": orphans, "child_rows": len(child_values)},
            )
        )
    disposition = (
        CheckStatus.FAIL if any(c.status is CheckStatus.FAIL for c in checks) else CheckStatus.PASS
    )
    return ValidationReport(tuple(checks), intended_use, disposition)
