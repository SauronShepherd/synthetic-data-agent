"""Check-level quality validation for generated data."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Any, cast

from sda.artifacts.fingerprint import fingerprint
from sda.patterns.rules import evaluate_rule


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


def _safe_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _safe_payload(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return tuple(_safe_payload(item) for item in value)
    if isinstance(value, int | float | bool) or value is None:
        return value
    return {"fingerprint": fingerprint(value)}


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
    schema_version: str = "validation-check-v1"

    def __post_init__(self) -> None:
        if not self.check_id.strip() or not self.message.strip() or not self.method.strip():
            raise ValueError("validation check identity, message, and method are required")
        if self.severity not in {"info", "warning", "error"}:
            raise ValueError("validation severity must be info, warning, or error")
        if self.status is CheckStatus.NOT_APPLICABLE and not self.unsupported_reason:
            raise ValueError("NOT_APPLICABLE checks require unsupported_reason")
        if not self.schema_version.strip():
            raise ValueError("validation check schema_version must not be empty")
        object.__setattr__(self, "evidence", _freeze(self.evidence))

    def to_dict(self) -> dict[str, Any]:
        """Serialize the check contract with immutable evidence preserved."""
        return {
            "check_id": self.check_id,
            "status": self.status.value,
            "message": self.message,
            "evidence": _safe_payload(self.evidence),
            "threshold": self.threshold,
            "method": self.method,
            "freshness": self.freshness,
            "population": self.population,
            "severity": self.severity,
            "unsupported_reason": self.unsupported_reason,
            "schema_version": self.schema_version,
        }


@dataclass(frozen=True, slots=True)
class ValidationReport:
    checks: tuple[ValidationCheck, ...]
    intended_use: str
    technical_disposition: CheckStatus
    validation_vector: dict[str, CheckStatus] = field(default_factory=dict)
    schema_version: str = "validation-report-v1"

    def __post_init__(self) -> None:
        if not self.intended_use.strip():
            raise ValueError("validation intended use is required")
        if not self.schema_version.strip():
            raise ValueError("validation report schema_version must not be empty")
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

    def to_dict(self) -> dict[str, Any]:
        """Serialize the complete validation disposition for artifact storage."""
        return {
            "checks": tuple(check.to_dict() for check in self.checks),
            "intended_use": self.intended_use,
            "technical_disposition": self.technical_disposition.value,
            "validation_vector": {
                dimension: status.value for dimension, status in self.validation_vector.items()
            },
            "fingerprint": self.fingerprint,
            "schema_version": self.schema_version,
        }


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
    unique_key_sets: dict[str, tuple[str, ...]] | None = None,
    foreign_keys: tuple[tuple[str, str, str, str], ...] = (),
    fanout_bounds: dict[tuple[str, str, str, str], tuple[int, int]] | None = None,
    expected_distributions: dict[str, dict[str, dict[Any, float]]] | None = None,
    distribution_tolerance: float = 0.0,
    conditional_null_rates: dict[str, dict[tuple[str, str], dict[Any, float]]] | None = None,
    format_patterns: dict[str, dict[str, str]] | None = None,
    time_orderings: tuple[tuple[str, str, str], ...] = (),
    numeric_bounds: dict[str, dict[str, tuple[float, float]]] | None = None,
    rules: tuple[Any, ...] = (),
    validation_vector: dict[str, CheckStatus] | None = None,
    intended_use: str = "unspecified",
) -> ValidationReport:
    checks: list[ValidationCheck] = []
    expected_counts = expected_counts or {}
    required_columns = required_columns or {}
    unique_keys = unique_keys or {}
    unique_key_sets = unique_key_sets or {}
    expected_distributions = expected_distributions or {}
    conditional_null_rates = conditional_null_rates or {}
    fanout_bounds = fanout_bounds or {}
    format_patterns = format_patterns or {}
    numeric_bounds = numeric_bounds or {}
    validation_vector = validation_vector or {}
    if distribution_tolerance < 0:
        raise ValueError("distribution_tolerance must not be negative")
    for rule in rules:
        table = str(getattr(rule, "table", ""))
        rows = tables.get(table)
        rule_id = str(getattr(rule, "rule_id", ""))
        check_id = f"rule:{rule_id or fingerprint(rule)}"
        if rows is None:
            checks.append(
                ValidationCheck(
                    check_id,
                    CheckStatus.FAIL,
                    "rule table is unavailable; cannot validate rule",
                    {"supported": False, "reason": "table_missing"},
                    method="rule_evaluation",
                )
            )
            continue
        evaluation = evaluate_rule(list(rows), rule)
        if evaluation.population_rows == 0:
            checks.append(
                ValidationCheck(
                    check_id,
                    CheckStatus.NOT_APPLICABLE,
                    "rule has no evaluable population",
                    {"population_rows": 0},
                    method="rule_evaluation",
                    unsupported_reason="no_evaluable_population",
                    severity="warning",
                )
            )
            continue
        checks.append(
            ValidationCheck(
                check_id,
                CheckStatus.PASS if evaluation.violation_rows == 0 else CheckStatus.FAIL,
                "rule satisfied" if evaluation.violation_rows == 0 else "rule violations detected",
                {
                    "population_rows": evaluation.population_rows,
                    "satisfying_rows": evaluation.satisfying_rows,
                    "violation_rows": evaluation.violation_rows,
                    "satisfaction_rate": evaluation.satisfaction_rate,
                    "violation_rate": evaluation.violation_rate,
                },
                method=evaluation.validation_mode,
                population=str(evaluation.population_rows),
            )
        )
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
    for table, columns in unique_key_sets.items():
        check_id = f"unique:{table}.({','.join(columns)})"
        if not columns:
            raise ValueError(f"composite uniqueness requires at least one column: {check_id}")
        if table not in tables:
            checks.append(
                ValidationCheck(
                    check_id,
                    CheckStatus.FAIL,
                    f"{table} is unavailable; cannot validate uniqueness",
                    {"supported": False, "reason": "table_missing"},
                    method="availability_check",
                )
            )
            continue
        rows = tables[table]
        if any(column not in row for row in rows for column in columns):
            checks.append(
                ValidationCheck(
                    check_id,
                    CheckStatus.FAIL,
                    f"composite key columns are unavailable for {check_id}",
                    {"supported": False, "reason": "column_missing"},
                    method="availability_check",
                )
            )
            continue
        keys = [tuple(row[column] for column in columns) for row in rows]
        unique = None not in keys and len(keys) == len(set(keys))
        checks.append(
            ValidationCheck(
                check_id,
                CheckStatus.PASS if unique else CheckStatus.FAIL,
                f"{check_id} is {'unique and non-null' if unique else 'not unique or contains nulls'}",
                {"rows": len(keys), "distinct": len(set(keys))},
                method="composite_key_uniqueness",
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
    for (child, child_column, parent, parent_column), bounds in fanout_bounds.items():
        minimum, maximum = bounds
        if minimum < 0 or maximum < minimum:
            raise ValueError("fanout bounds must be non-negative and ordered")
        check_id = f"fanout:{parent}.{parent_column}->{child}.{child_column}"
        if child not in tables or parent not in tables:
            missing = child if child not in tables else parent
            checks.append(
                ValidationCheck(
                    check_id,
                    CheckStatus.FAIL,
                    f"{missing} is unavailable; cannot validate fan-out",
                    {"supported": False, "reason": "table_missing", "missing_table": missing},
                    method="availability_check",
                )
            )
            continue
        if any(parent_column not in row for row in tables[parent]) or any(
            child_column not in row for row in tables[child]
        ):
            checks.append(
                ValidationCheck(
                    check_id,
                    CheckStatus.FAIL,
                    f"fan-out columns are unavailable for {check_id}",
                    {"supported": False, "reason": "column_missing"},
                    method="availability_check",
                )
            )
            continue
        fanout_counts = {row[parent_column]: 0 for row in tables[parent]}
        for row in tables[child]:
            if row[child_column] in fanout_counts:
                fanout_counts[row[child_column]] += 1
        violations = sum(not minimum <= count <= maximum for count in fanout_counts.values())
        checks.append(
            ValidationCheck(
                check_id,
                CheckStatus.PASS if violations == 0 else CheckStatus.FAIL,
                f"{violations} parent fan-out counts outside [{minimum}, {maximum}]",
                {
                    "parents": len(fanout_counts),
                    "violations": violations,
                    "minimum": minimum,
                    "maximum": maximum,
                    "zero_child_parents": sum(count == 0 for count in fanout_counts.values()),
                },
                method="parent_fanout_bounds",
                population="all_parent_rows",
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
            observed_distribution = (
                {value: count / total for value, count in counts.items()} if total else {}
            )
            errors = {
                str(value): abs(observed_distribution.get(value, 0.0) - probability)
                for value, probability in expected_distribution.items()
            }
            errors.update(
                {
                    str(value): probability
                    for value, probability in observed_distribution.items()
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
                        "observed": observed_distribution,
                        "maximum_error": maximum_error,
                    },
                    distribution_tolerance,
                    method="categorical_distribution",
                    population="full_table",
                )
            )
    for table, conditions in conditional_null_rates.items():
        if table not in tables:
            checks.append(
                ValidationCheck(
                    f"conditional_null:{table}",
                    CheckStatus.FAIL,
                    f"{table} is unavailable; cannot validate conditional null rates",
                    {"supported": False, "reason": "table_missing"},
                    method="availability_check",
                )
            )
            continue
        rows = tables[table]
        for (driver, target), expected_rates in conditions.items():
            check_id = f"conditional_null:{table}.{driver}->{target}"
            if not expected_rates or any(rate < 0 or rate > 1 for rate in expected_rates.values()):
                raise ValueError(f"conditional null rates must be between zero and one: {check_id}")
            if any(driver not in row or target not in row for row in rows):
                checks.append(
                    ValidationCheck(
                        check_id,
                        CheckStatus.FAIL,
                        f"{table}.{driver} or {target} is unavailable; cannot validate conditional null rates",
                        {"supported": False, "reason": "column_missing"},
                        method="availability_check",
                    )
                )
                continue
            observed: dict[Any, float] = {}
            for value in expected_rates:
                subset = [row for row in rows if row[driver] == value]
                observed[value] = (
                    sum(row[target] is None for row in subset) / len(subset) if subset else 0.0
                )
            errors = {
                str(value): abs(observed[value] - rate) for value, rate in expected_rates.items()
            }
            maximum_error = max(errors.values(), default=0.0)
            checks.append(
                ValidationCheck(
                    check_id,
                    CheckStatus.PASS
                    if maximum_error <= distribution_tolerance
                    else CheckStatus.FAIL,
                    f"{check_id} maximum error is {maximum_error:.6f}",
                    {
                        "expected": expected_rates,
                        "observed": observed,
                        "maximum_error": maximum_error,
                    },
                    distribution_tolerance,
                    method="conditional_null_rate",
                    population="full_table_by_driver",
                )
            )
    for table, format_columns in format_patterns.items():
        if table not in tables:
            checks.append(
                ValidationCheck(
                    f"format:{table}",
                    CheckStatus.FAIL,
                    f"{table} is unavailable; cannot validate formats",
                    {"supported": False, "reason": "table_missing"},
                    method="availability_check",
                )
            )
        rows = tables[table]
        for column, pattern in format_columns.items():
            check_id = f"format:{table}.{column}"
            try:
                matcher = re.compile(pattern)
            except re.error as exc:
                raise ValueError(f"invalid format pattern for {check_id}") from exc
            if any(column not in row for row in rows):
                checks.append(
                    ValidationCheck(
                        check_id,
                        CheckStatus.FAIL,
                        f"{table}.{column} is unavailable; cannot validate format",
                        {"supported": False, "reason": "column_missing"},
                        method="availability_check",
                    )
                )
                continue
            invalid = sum(
                not isinstance(row[column], str) or matcher.fullmatch(row[column]) is None
                for row in rows
            )
            checks.append(
                ValidationCheck(
                    check_id,
                    CheckStatus.PASS if invalid == 0 else CheckStatus.FAIL,
                    f"{table}.{column} has {invalid} values violating its format",
                    {"rows": len(rows), "invalid": invalid},
                    method="regex_fullmatch",
                    population="full_table",
                )
            )
    for table, earlier_column, later_column in time_orderings:
        check_id = f"time_order:{table}.{earlier_column}<={later_column}"
        if table not in tables:
            checks.append(
                ValidationCheck(
                    check_id,
                    CheckStatus.FAIL,
                    f"{table} is unavailable; cannot validate time ordering",
                    {"supported": False, "reason": "table_missing"},
                    method="availability_check",
                )
            )
            continue
        rows = tables[table]
        if any(earlier_column not in row or later_column not in row for row in rows):
            checks.append(
                ValidationCheck(
                    check_id,
                    CheckStatus.FAIL,
                    f"time ordering columns are unavailable for {check_id}",
                    {"supported": False, "reason": "column_missing"},
                    method="availability_check",
                )
            )
            continue
        invalid = 0
        for row in rows:
            earlier, later = row[earlier_column], row[later_column]
            if (
                not isinstance(earlier, date | datetime | int | float | str)
                or not isinstance(later, date | datetime | int | float | str)
                or type(earlier) is not type(later)
            ):
                invalid += 1
            else:
                try:
                    invalid += bool(cast(Any, earlier) > cast(Any, later))
                except TypeError:
                    invalid += 1
        checks.append(
            ValidationCheck(
                check_id,
                CheckStatus.PASS if invalid == 0 else CheckStatus.FAIL,
                f"{invalid} rows violate time ordering",
                {"rows": len(rows), "invalid": invalid},
                method="non_decreasing_order",
                population="full_table",
            )
        )
    for table, numeric_columns in numeric_bounds.items():
        if table not in tables:
            checks.append(
                ValidationCheck(
                    f"numeric_bounds:{table}",
                    CheckStatus.FAIL,
                    f"{table} is unavailable; cannot validate numeric bounds",
                    {"supported": False, "reason": "table_missing"},
                    method="availability_check",
                )
            )
            continue
        rows = tables[table]
        for column, numeric_bounds_value in numeric_columns.items():
            numeric_minimum, numeric_maximum = numeric_bounds_value
            check_id = f"numeric_bounds:{table}.{column}"
            if numeric_maximum < numeric_minimum:
                raise ValueError(f"numeric bounds must be ordered for {check_id}")
            if any(column not in row for row in rows):
                checks.append(
                    ValidationCheck(
                        check_id,
                        CheckStatus.FAIL,
                        f"{table}.{column} is unavailable; cannot validate numeric bounds",
                        {"supported": False, "reason": "column_missing"},
                        method="availability_check",
                    )
                )
                continue
            invalid = sum(
                value is not None
                and (
                    isinstance(value, bool)
                    or not isinstance(value, int | float)
                    or not numeric_minimum <= value <= numeric_maximum
                )
                for value in (row[column] for row in rows)
            )
            checks.append(
                ValidationCheck(
                    check_id,
                    CheckStatus.PASS if invalid == 0 else CheckStatus.FAIL,
                    f"{invalid} values violate numeric bounds [{numeric_minimum}, {numeric_maximum}]",
                    {
                        "rows": len(rows),
                        "invalid": invalid,
                        "minimum": numeric_minimum,
                        "maximum": numeric_maximum,
                    },
                    method="numeric_bounds",
                    population="non_null_values",
                )
            )
    if not checks:
        checks.append(
            ValidationCheck(
                "validation_scope",
                CheckStatus.FAIL,
                "no validation checks were requested",
                {"supported": False, "reason": "no_checks_requested"},
                method="scope_check",
            )
        )
    disposition = (
        CheckStatus.FAIL if any(c.status is CheckStatus.FAIL for c in checks) else CheckStatus.PASS
    )
    return ValidationReport(tuple(checks), intended_use, disposition, validation_vector)
