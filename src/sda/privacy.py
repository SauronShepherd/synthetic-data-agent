"""Fail-closed privacy review contracts for generated output."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class PrivacyDecision(StrEnum):
    APPROVED = "approved"
    REJECTED = "rejected"
    REVIEW_REQUIRED = "review_required"


@dataclass(frozen=True, slots=True)
class PrivacyFinding:
    code: str
    severity: str
    message: str
    evidence: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PrivacyReport:
    decision: PrivacyDecision
    findings: tuple[PrivacyFinding, ...]
    policy_ref: str


def assess_privacy(
    tables: dict[str, tuple[dict[str, Any], ...]],
    *,
    sensitive_columns: tuple[tuple[str, str], ...] = (),
    approved_columns: tuple[tuple[str, str], ...] = (),
    max_duplicate_rows: int = 0,
    policy_ref: str = "strict-default",
) -> PrivacyReport:
    """Run deterministic, conservative checks before human/publication approval.

    A sensitive column is rejected unless explicitly approved. Duplicate complete
    rows are treated as a memorization-risk signal and require review when over the
    configured budget.
    """
    findings: list[PrivacyFinding] = []
    approved = set(approved_columns)
    for table, column in sensitive_columns:
        if (table, column) not in approved:
            findings.append(
                PrivacyFinding(
                    "sensitive_column_not_approved",
                    "high",
                    f"{table}.{column} is sensitive and has no explicit approval",
                    {"table": table, "column": column},
                )
            )
    for table, rows in tables.items():
        counts: dict[tuple[tuple[str, Any], ...], int] = {}
        for row in rows:
            key = tuple(sorted(row.items()))
            counts[key] = counts.get(key, 0) + 1
        duplicates = sum(count - 1 for count in counts.values() if count > 1)
        if duplicates > max_duplicate_rows:
            findings.append(
                PrivacyFinding(
                    "duplicate_row_risk",
                    "medium",
                    f"{table} contains {duplicates} duplicate rows beyond the approved budget",
                    {"table": table, "duplicates": duplicates, "budget": max_duplicate_rows},
                )
            )
    decision = (
        PrivacyDecision.REJECTED
        if any(f.severity == "high" for f in findings)
        else (PrivacyDecision.REVIEW_REQUIRED if findings else PrivacyDecision.APPROVED)
    )
    return PrivacyReport(decision, tuple(findings), policy_ref)
