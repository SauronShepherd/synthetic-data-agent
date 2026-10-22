"""Evaluate Unity Catalog constraints without promoting informational keys."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from functools import reduce
from operator import or_
from typing import Any

from sda.artifacts.fingerprint import fingerprint
from sda.metadata_models import ConstraintKind, ConstraintMetadata
from sda.patterns.models import (
    Pattern,
    PatternDecision,
    PatternFamily,
    PatternLifecycle,
    PatternOrigin,
)

_TOKEN = re.compile(
    r"(?P<space>\s+)"
    r"|(?P<identifier>`(?:``|[^`])+`|[A-Za-z_][A-Za-z0-9_$]*)"
    r"|(?P<string>'(?:''|[^'])*')"
    r"|(?P<number>(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?)"
    r"|(?P<operator><=|>=|<>|!=|=|<|>|\+|-|\*|/|%|\(|\)|,)"
)
_KEYWORDS = {"AND", "OR", "NOT", "IS", "NULL", "IN", "BETWEEN", "TRUE", "FALSE"}


@dataclass(frozen=True, slots=True)
class ParsedCheck:
    expression: str
    columns: tuple[str, ...]


def parse_check_clause(clause: str, available_columns: Iterable[str]) -> ParsedCheck:
    """Allow a deterministic SQL-expression subset and reject functions/SQL control.

    UC check clauses are metadata, but they still cross a SQL parser boundary.
    Restricting tokens prevents arbitrary statement fragments or function calls
    from being interpolated into the Spark expression evaluator.
    """
    raw = clause.strip()
    if raw[:5].upper() == "CHECK" and (len(raw) == 5 or raw[5].isspace() or raw[5] == "("):
        raw = raw[5:].strip()
    if not raw:
        raise ValueError("check clause is empty")
    column_names: dict[str, str] = {}
    for column in available_columns:
        folded = column.casefold()
        if folded in column_names and column_names[folded] != column:
            raise ValueError("source schema contains case-ambiguous column names")
        column_names[folded] = column

    tokens: list[tuple[str, str]] = []
    position = 0
    while position < len(raw):
        match = _TOKEN.match(raw, position)
        if match is None:
            raise ValueError("check clause contains an unsupported SQL token")
        kind = match.lastgroup
        value = match.group()
        position = match.end()
        if kind != "space":
            tokens.append((str(kind), value))

    rendered: list[str] = []
    referenced: set[str] = set()
    meaningful = [value for _, value in tokens]
    for index, (kind, value) in enumerate(tokens):
        if kind == "identifier":
            if value.startswith("`"):
                name = value[1:-1].replace("``", "`")
                matched_column = next((item for item in available_columns if item == name), None)
                if matched_column is None:
                    raise ValueError("check clause references an unknown column")
                if index + 1 < len(tokens) and meaningful[index + 1] == "(":
                    raise ValueError("check clause function calls are not supported")
                referenced.add(matched_column)
                rendered.append(f"`{matched_column.replace('`', '``')}`")
            elif value.upper() in _KEYWORDS:
                rendered.append(value.upper())
            else:
                matched_column = column_names.get(value.casefold())
                if matched_column is None:
                    raise ValueError("check clause references an unknown column or function")
                if index + 1 < len(tokens) and meaningful[index + 1] == "(":
                    raise ValueError("check clause function calls are not supported")
                referenced.add(matched_column)
                rendered.append(f"`{matched_column.replace('`', '``')}`")
        else:
            rendered.append(value)
    if not referenced:
        raise ValueError("check clause must reference at least one source column")
    return ParsedCheck(" ".join(rendered), tuple(sorted(referenced)))


def evaluate_platform_constraints(
    frame: Any,
    constraints: Iterable[ConstraintMetadata],
    *,
    analysis_id: str,
    table: str,
    max_constraints: int = 100,
) -> tuple[Pattern, ...]:
    """Evaluate enforced NOT NULL/CHECK constraints and retain informational keys.

    Primary/foreign/unique keys are recorded as declarations only. Their data
    validity must come from relationship/key analysis, not Databricks enforcement.
    """
    if max_constraints < 1:
        raise ValueError("max_constraints must be positive")
    available = tuple(field.name for field in frame.schema.fields)
    population = frame.count()
    results: list[Pattern] = []
    for constraint in tuple(constraints)[:max_constraints]:
        kind = constraint.kind
        key_kind = kind in {
            ConstraintKind.PRIMARY_KEY,
            ConstraintKind.FOREIGN_KEY,
            ConstraintKind.UNIQUE,
        }
        enforceable_kind = kind in {ConstraintKind.NOT_NULL, ConstraintKind.CHECK}
        enforced = constraint.enforced and enforceable_kind
        warning: list[str] = []
        if key_kind:
            warning.append("informational_key_requires_sda06_validation")
        elif not enforceable_kind:
            warning.append("unsupported_constraint_kind_review_required")

        referenced = tuple(column for column in constraint.columns if column in available)
        violation_rows: int | None = None
        indeterminate_rows: int | None = None
        evaluation_mode = "metadata_only"
        evaluation_error: str | None = None
        if kind is ConstraintKind.NOT_NULL and referenced:
            from pyspark.sql import functions as F

            missing = reduce(or_, (F.col(column).isNull() for column in referenced))
            row = frame.agg(F.sum(F.when(missing, 1).otherwise(0)).alias("violation_rows")).first()
            violation_rows = int(row["violation_rows"] or 0)
            indeterminate_rows = 0
            evaluation_mode = "exact"
        elif kind is ConstraintKind.NOT_NULL:
            evaluation_error = "not_null_column_unavailable"
        elif kind is ConstraintKind.CHECK and constraint.check_clause:
            try:
                parsed = parse_check_clause(constraint.check_clause, available)
                from pyspark.sql import functions as F

                check = F.expr(parsed.expression)
                # Databricks CHECK requires TRUE; SQL NULL/UNKNOWN is not success.
                invalid_check = check.isNull() | check.eqNullSafe(F.lit(False))
                row = frame.agg(
                    F.sum(F.when(invalid_check, 1).otherwise(0)).alias("violation_rows"),
                    F.sum(F.when(check.isNull(), 1).otherwise(0)).alias("indeterminate_rows"),
                ).first()
                violation_rows = int(row["violation_rows"] or 0)
                indeterminate_rows = int(row["indeterminate_rows"] or 0)
                referenced = parsed.columns
                evaluation_mode = "exact"
            except ValueError as exc:
                evaluation_error = str(exc)
        elif kind is ConstraintKind.CHECK:
            evaluation_error = "check_clause_unavailable"

        if evaluation_error:
            warning.append("platform_constraint_evaluation_unavailable")
        if enforced and violation_rows:
            warning.append("enforced_constraint_violations_detected")
        platform_verified = enforced and violation_rows == 0 and evaluation_mode == "exact"
        if enforced:
            origin = PatternOrigin.PLATFORM
            strength = "hard_constraint"
            if platform_verified:
                lifecycle = PatternLifecycle.APPROVED_RULE
                decision = PatternDecision.ACCEPTED_FOR_PLANNING
                review_status = "not_required"
                generation = "enforce_platform_constraint"
            else:
                lifecycle = PatternLifecycle.REVIEW_REQUIRED
                decision = PatternDecision.REVIEW_REQUIRED
                review_status = "required"
                generation = "review_only"
        else:
            origin = PatternOrigin.DECLARED
            lifecycle = PatternLifecycle.DECLARED_RULE
            decision = PatternDecision.REVIEW_REQUIRED
            strength = "declared_constraint" if key_kind or not enforced else "hard_constraint"
            review_status = "required"
            generation = "review_only"

        metric: dict[str, Any] = {
            "constraint_name": constraint.name,
            "constraint_kind": kind.value,
            "declared_enforced": constraint.enforced,
            "effective_enforcement": enforced,
            "validated_metadata": constraint.validated,
            "rely": constraint.rely,
            "referenced_table": constraint.referenced_table,
            "referenced_columns": constraint.referenced_columns,
            "check_clause": constraint.check_clause,
            "population_rows": population,
            "violation_rows": violation_rows,
            "violation_rate": (
                violation_rows / population
                if violation_rows is not None and population
                else 0.0
                if violation_rows == 0
                else None
            ),
            "indeterminate_rows": indeterminate_rows,
            "method": evaluation_mode,
            "evaluation_error": evaluation_error,
        }
        identity = {
            "table": table,
            "name": constraint.name,
            "kind": kind.value,
            "columns": referenced or constraint.columns,
            "check_clause": constraint.check_clause,
        }
        pattern_id = "pat_" + fingerprint(identity)
        results.append(
            Pattern(
                pattern_id=pattern_id,
                analysis_id=analysis_id,
                family=PatternFamily.BUSINESS_RULE,
                origin=origin,
                primary_table=table,
                columns=referenced or constraint.columns,
                condition={"constraint_name": constraint.name},
                outcome={"kind": kind.value, "check_clause": constraint.check_clause},
                support_rows=(population - violation_rows) if violation_rows is not None else 0,
                support_rate=(
                    (population - violation_rows) / population
                    if violation_rows is not None and population
                    else None
                ),
                metric=metric,
                evidence_quality={
                    "validation_mode": evaluation_mode,
                    "platform_enforced": enforced,
                    "support_quality": "measured"
                    if evaluation_mode == "exact"
                    else "metadata_only",
                },
                decision=decision.value,
                rule_strength=strength,
                warnings=tuple(warning),
                generation_action={
                    "kind": generation,
                    "evidence_pattern_id": pattern_id,
                    "requires_approval": not platform_verified,
                },
                validation_action={
                    "kind": "validate_platform_constraint",
                    "evidence_pattern_id": pattern_id,
                    "violation_metric": "violation_rate",
                },
                review_status=review_status,
                lifecycle=lifecycle,
            )
        )
    return tuple(results)
