"""Fail-closed privacy review contracts for generated output."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from sda.artifacts.fingerprint import fingerprint


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
    direct_identifier_columns: tuple[tuple[str, str], ...] = (),
    quasi_identifier_columns: tuple[tuple[str, str], ...] = (),
    approved_columns: tuple[tuple[str, str], ...] = (),
    max_duplicate_rows: int = 0,
    min_quasi_group_size: int = 2,
    policy_ref: str = "strict-default",
) -> PrivacyReport:
    """Run deterministic, conservative checks before human/publication approval.

    A sensitive column is rejected unless explicitly approved. Duplicate complete
    rows are treated as a memorization-risk signal and require review when over the
    configured budget.
    """
    findings: list[PrivacyFinding] = []
    if min_quasi_group_size < 1:
        raise ValueError("min_quasi_group_size must be positive")
    approved = set(approved_columns)
    for table, column in (*sensitive_columns, *direct_identifier_columns):
        if (table, column) not in approved:
            findings.append(
                PrivacyFinding(
                    "direct_identifier_not_approved"
                    if (table, column) in direct_identifier_columns
                    else "sensitive_column_not_approved",
                    "high",
                    f"{table}.{column} is sensitive and has no explicit approval",
                    {"table": table, "column": column},
                )
            )
    for table, column in quasi_identifier_columns:
        rows = tables.get(table)
        if rows is None:
            findings.append(
                PrivacyFinding(
                    "quasi_identifier_table_missing",
                    "high",
                    f"{table} is unavailable for quasi-identifier review",
                    {"table": table, "column": column},
                )
            )
            continue
        quasi_counts: dict[str, int] = {}
        for row in rows:
            value_fingerprint = fingerprint(row.get(column))
            quasi_counts[value_fingerprint] = quasi_counts.get(value_fingerprint, 0) + 1
        rare = sum(count < min_quasi_group_size for count in quasi_counts.values())
        if rare:
            findings.append(
                PrivacyFinding(
                    "rare_quasi_identifier_values",
                    "medium",
                    f"{table}.{column} has {rare} rare value groups",
                    {
                        "table": table,
                        "column": column,
                        "rare_groups": rare,
                        "minimum_group_size": min_quasi_group_size,
                    },
                )
            )
    for table, rows in tables.items():
        duplicate_counts: dict[str, int] = {}
        for row in rows:
            # Canonical fingerprints support nested values without retaining
            # raw row material in the privacy finding.
            key = fingerprint(row)
            duplicate_counts[key] = duplicate_counts.get(key, 0) + 1
        duplicates = sum(count - 1 for count in duplicate_counts.values() if count > 1)
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
