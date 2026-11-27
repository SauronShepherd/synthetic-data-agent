"""Fit-for-purpose quality evidence without turning validation into approval."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sda.artifacts.fingerprint import fingerprint


class QualityError(ValueError):
    """Raised when a quality expectation is internally inconsistent."""


@dataclass(frozen=True, slots=True)
class QualityCheck:
    check_id: str
    status: str
    message: str
    evidence: dict[str, Any]
    hard_gate: bool = True

    def __post_init__(self) -> None:
        if not self.check_id.strip() or self.status not in {"PASS", "WARN", "FAIL"}:
            raise QualityError("quality check identity and status are required")
        if not self.message.strip():
            raise QualityError("quality check message is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "check_id": self.check_id,
            "status": self.status,
            "message": self.message,
            "evidence": dict(self.evidence),
            "hard_gate": self.hard_gate,
        }


@dataclass(frozen=True, slots=True)
class QualityReport:
    checks: tuple[QualityCheck, ...]
    intended_use: str
    technical_disposition: str
    soft_score: float | None = None
    schema_version: str = "quality-report"

    def __post_init__(self) -> None:
        if not self.intended_use.strip():
            raise QualityError("quality intended use is required")
        if self.technical_disposition not in {"PASS", "FAIL"}:
            raise QualityError("quality technical disposition must be PASS or FAIL")
        if len({check.check_id for check in self.checks}) != len(self.checks):
            raise QualityError("quality check IDs must be unique")
        expected = "FAIL" if any(check.status == "FAIL" and check.hard_gate for check in self.checks) else "PASS"
        if self.technical_disposition != expected:
            raise QualityError("quality disposition does not match hard-gate checks")
        if self.soft_score is not None and not 0.0 <= self.soft_score <= 1.0:
            raise QualityError("quality soft_score must be between zero and one")

    @property
    def fingerprint(self) -> str:
        return fingerprint(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "checks": tuple(check.to_dict() for check in self.checks),
            "intended_use": self.intended_use,
            "technical_disposition": self.technical_disposition,
            "soft_score": self.soft_score,
            "fingerprint": self.fingerprint,
            "schema_version": self.schema_version,
        }


def evaluate_quality(
    rows: tuple[dict[str, Any], ...],
    *,
    expected_count: int,
    required_columns: tuple[str, ...],
    unique_key: str,
    intended_use: str,
) -> QualityReport:
    """Evaluate a small fixture without rescanning source or inventing rules."""
    if expected_count < 0:
        raise QualityError("expected_count must not be negative")
    columns = set().union(*(row.keys() for row in rows)) if rows else set()
    checks = [
        QualityCheck(
            "row_count",
            "PASS" if len(rows) == expected_count else "FAIL",
            "row count matches the frozen expectation",
            {"actual": len(rows), "expected": expected_count},
        ),
        QualityCheck(
            "schema",
            "PASS" if set(required_columns).issubset(columns) else "FAIL",
            "required columns are present",
            {"required": required_columns, "missing": sorted(set(required_columns) - columns)},
        ),
    ]
    values = [row.get(unique_key) for row in rows]
    unique = len(values) == len(set(values)) and None not in values
    checks.append(
        QualityCheck(
            f"unique:{unique_key}",
            "PASS" if unique else "FAIL",
            "key is unique and non-null",
            {"rows": len(values), "distinct": len(set(values)), "nulls": values.count(None)},
        )
    )
    score = sum(check.status == "PASS" for check in checks) / len(checks)
    return QualityReport(
        tuple(checks),
        intended_use,
        "PASS" if all(c.status == "PASS" for c in checks) else "FAIL",
        score,
    )
