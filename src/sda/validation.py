"""Check-level quality validation for generated data."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from sda.artifacts.fingerprint import fingerprint


class _FrozenDict(dict[str, Any]):
    def _immutable(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("validation evidence is immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _immutable  # type: ignore[assignment]


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return _FrozenDict({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    return value


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
    freshness: str | None = None
    population: str | None = None
    severity: str = "error"
    unsupported_reason: str | None = None

    def __post_init__(self) -> None:
        if not self.check_id.strip() or not self.message.strip() or not self.method.strip():
            raise ValueError("validation check identity, message, and method are required")
        if self.severity not in {"info", "warning", "error"}:
            raise ValueError("validation severity must be info, warning, or error")
        if self.status is CheckStatus.NOT_APPLICABLE and not self.unsupported_reason:
            raise ValueError("NOT_APPLICABLE checks require unsupported_reason")
        object.__setattr__(self, "evidence", _freeze(self.evidence))


@dataclass(frozen=True, slots=True)
class ValidationReport:
    checks: tuple[ValidationCheck, ...]
    intended_use: str
    technical_disposition: CheckStatus
    validation_vector: dict[str, CheckStatus] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.intended_use.strip():
            raise ValueError("validation intended use is required")
        ids = [check.check_id for check in self.checks]
        if len(ids) != len(set(ids)):
            raise ValueError("validation check IDs must be unique")
        expected = (
            CheckStatus.FAIL
            if any(check.status is CheckStatus.FAIL for check in self.checks)
            else CheckStatus.PASS
        )
        if self.technical_disposition is not expected:
            raise ValueError(
                f"technical_disposition must match the check results (expected {expected.value})"
            )
        if any(not key.strip() for key in self.validation_vector):
            raise ValueError("validation vector dimensions must not be empty")
        if any(not isinstance(value, CheckStatus) for value in self.validation_vector.values()):
            raise ValueError("validation vector values must be CheckStatus values")
        object.__setattr__(self, "validation_vector", _FrozenDict(self.validation_vector))

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
        severity="info",
        unsupported_reason=reason,
    )


def validate_tables(
    tables: dict[str, tuple[dict[str, Any], ...]],
    *,
    expected_counts: dict[str, int] | None = None,
    required_columns: dict[str, tuple[str, ...]] | None = None,
    unique_keys: dict[str, str] | None = None,
    foreign_keys: tuple[tuple[str, str, str, str], ...] = (),
    expected_distributions: dict[str, dict[str, dict[Any, float]]] | None = None,
    distribution_tolerance: float = 0.0,
    intended_use: str = "unspecified",
) -> ValidationReport:
    checks: list[ValidationCheck] = []
    expected_counts = expected_counts or {}
    required_columns = required_columns or {}
    unique_keys = unique_keys or {}
    expected_distributions = expected_distributions or {}
    if distribution_tolerance < 0:
        raise ValueError("distribution_tolerance must not be negative")
    for table, columns in required_columns.items():
        if table not in tables:
            checks.append(
                ValidationCheck(
                    f"schema:{table}",
                    CheckStatus.FAIL,
                    f"{table} is unavailable; cannot validate schema",
                    {"supported": False, "reason": "table_missing"},
                    method="availability_check",
                )
            )
            continue
        actual_columns = set().union(*(row.keys() for row in tables[table]))
        schema_missing = sorted(set(columns) - actual_columns)
        checks.append(
            ValidationCheck(
                f"schema:{table}",
                CheckStatus.PASS if not schema_missing else CheckStatus.FAIL,
                f"{table} schema contains all required columns"
                if not schema_missing
                else f"{table} schema is missing columns: {schema_missing}",
                {"required": list(columns), "missing": schema_missing},
                method="schema_contract",
            )
        )
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
    for table, distribution_columns in expected_distributions.items():
        if table not in tables:
            checks.append(
                ValidationCheck(
                    f"distribution:{table}",
                    CheckStatus.FAIL,
                    f"{table} is unavailable; cannot validate distributions",
                    {"supported": False, "reason": "table_missing"},
                    method="availability_check",
                )
            )
            continue
        rows = tables[table]
        for column, expected_distribution in distribution_columns.items():
            check_id = f"distribution:{table}.{column}"
            if not expected_distribution or any(
                probability < 0 for probability in expected_distribution.values()
            ):
                raise ValueError(
                    f"expected distribution must contain non-negative probabilities: {check_id}"
                )
            if abs(sum(expected_distribution.values()) - 1.0) > 1e-9:
                raise ValueError(f"expected distribution must sum to one: {check_id}")
            if any(column not in row for row in rows):
                checks.append(
                    ValidationCheck(
                        check_id,
                        CheckStatus.FAIL,
                        f"{table}.{column} is unavailable; cannot validate distribution",
                        {"supported": False, "reason": "column_missing"},
                        method="availability_check",
                    )
                )
                continue
            counts: dict[Any, int] = {}
            for row in rows:
                value = row[column]
                counts[value] = counts.get(value, 0) + 1
            total = len(rows)
            observed = {value: count / total for value, count in counts.items()} if total else {}
            errors = {
                str(value): abs(observed.get(value, 0.0) - probability)
                for value, probability in expected_distribution.items()
            }
            errors.update(
                {
                    str(value): probability
                    for value, probability in observed.items()
                    if value not in expected_distribution
                }
            )
            maximum_error = max(errors.values(), default=0.0)
            checks.append(
                ValidationCheck(
                    check_id,
                    CheckStatus.PASS
                    if maximum_error <= distribution_tolerance
                    else CheckStatus.FAIL,
                    f"{table}.{column} distribution maximum error is {maximum_error:.6f}",
                    {
                        "expected": expected_distribution,
                        "observed": observed,
                        "maximum_error": maximum_error,
                    },
                    distribution_tolerance,
                    method="categorical_distribution",
                    population="full_table",
                )
            )
    disposition = (
        CheckStatus.FAIL if any(c.status is CheckStatus.FAIL for c in checks) else CheckStatus.PASS
    )
    return ValidationReport(tuple(checks), intended_use, disposition)
