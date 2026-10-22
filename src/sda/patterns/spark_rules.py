"""Spark evaluation of explicitly supplied business rules."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from typing import Any

from sda.patterns.conflicts import resolve_rule_conflicts
from sda.patterns.models import PatternFamily, PatternLifecycle, PatternOrigin
from sda.patterns.precedence import RulePrecedencePolicy
from sda.patterns.rules import BusinessRule, RuleEvaluationResult, RuleStrength


def parse_rules_json(raw: str) -> tuple[BusinessRule, ...]:
    import json

    payload = json.loads(raw)
    if not isinstance(payload, list):
        raise ValueError("rules JSON must be a list")
    rules = []
    for item in payload:
        if not isinstance(item, dict) or not isinstance(item.get("predicates"), list):
            raise ValueError("each rule requires a predicates list")
        rules.append(
            BusinessRule(
                rule_id=item["rule_id"],
                table=item["table"],
                predicates=tuple(item["predicates"]),
                origin=PatternOrigin(item.get("origin", PatternOrigin.USER_PROVIDED)),
                strength=RuleStrength[item.get("strength", "PROBABILISTIC").upper()],
                approved=item.get("approved", False),
                effective_from=item.get("effective_from"),
                effective_to=item.get("effective_to"),
                review_status=item.get("review_status", "required"),
                rule_version=item.get("rule_version", "1"),
                owner_id=item.get("owner_id"),
                reviewer_id=item.get("reviewer_id"),
                approval_reference=item.get("approval_reference"),
                approved_at=item.get("approved_at"),
                supersedes_rule_id=item.get("supersedes_rule_id"),
            )
        )
    return tuple(rules)


def active_rules(
    rules: tuple[BusinessRule, ...], table: str, *, as_of: date
) -> tuple[BusinessRule, ...]:
    active = []
    for rule in rules:
        if rule.table != table:
            continue
        try:
            start = date.fromisoformat(rule.effective_from) if rule.effective_from else None
            end = date.fromisoformat(rule.effective_to) if rule.effective_to else None
        except ValueError as exc:
            raise ValueError(f"invalid effective date for rule {rule.rule_id}") from exc
        if start and end and start > end:
            raise ValueError(f"inverted effective dates for rule {rule.rule_id}")
        if (start is None or start <= as_of) and (end is None or as_of <= end):
            active.append(rule)
    return tuple(active)


def compile_predicate(frame: Any, predicate: dict[str, Any]) -> Any:
    """Compile a small, closed predicate vocabulary without SQL string interpolation."""
    from pyspark.sql import functions as F

    if not isinstance(predicate, dict) or not isinstance(predicate.get("column"), str):
        raise ValueError("rule predicate requires a column")
    name = predicate["column"]
    if name not in frame.columns:
        raise ValueError(f"rule predicate references unavailable column: {name}")
    if predicate.get("role", "condition") not in {"condition", "requirement"}:
        raise ValueError("unsupported rule predicate role")
    op = predicate.get("operator", "eq")
    column = F.col("`" + name.replace("`", "``") + "`")
    if op == "is_null":
        return column.isNull()
    if op == "is_not_null":
        return column.isNotNull()
    if op in {"in", "not_in"}:
        values = predicate.get("values")
        if not isinstance(values, (list, tuple)) or any(
            v is None or not isinstance(v, (str, int, float, bool)) for v in values
        ):
            raise ValueError("rule membership values must be non-null scalar list")
        match = column.isin(*values) if values else F.lit(False)
        return F.coalesce(match if op == "in" else ~match, F.lit(op == "not_in"))
    if op not in {"eq", "neq", "gte", "lte", "gt", "lt"}:
        raise ValueError(f"unsupported rule operator: {op}")
    if "value" not in predicate or (
        predicate["value"] is not None
        and not isinstance(predicate["value"], (str, int, float, bool))
    ):
        raise ValueError("rule comparison requires a scalar value")
    value = predicate["value"]
    if op == "eq":
        return column.eqNullSafe(F.lit(value))
    if op == "neq":
        return ~column.eqNullSafe(F.lit(value))
    if value is None:
        raise ValueError("ordered comparison requires a non-null value")
    return F.coalesce(
        {
            "gte": column >= F.lit(value),
            "lte": column <= F.lit(value),
            "gt": column > F.lit(value),
            "lt": column < F.lit(value),
        }[op],
        F.lit(False),
    )


def evaluate_spark_rule(frame: Any, rule: BusinessRule) -> RuleEvaluationResult:
    from pyspark.sql import functions as F

    condition = F.lit(True)
    requirement = F.lit(True)
    for predicate in rule.predicates:
        compiled = compile_predicate(frame, predicate)
        if predicate.get("role", "condition") == "condition":
            condition = condition & compiled
        else:
            requirement = requirement & compiled
    row = frame.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(condition, 1).otherwise(0)).alias("population"),
        F.sum(F.when(condition & requirement, 1).otherwise(0)).alias("satisfying"),
    ).first()
    total, population, satisfying = (
        int(row[key] or 0) for key in ("total", "population", "satisfying")
    )
    violations = population - satisfying
    return RuleEvaluationResult(
        population,
        satisfying,
        violations,
        satisfying / population if population else None,
        violations / population if population else None,
        population,
        population / total if total else None,
    )


def detect_spark_rules(
    frame: Any,
    rules: tuple[BusinessRule, ...],
    *,
    table: str,
    as_of: date,
    detector: Any,
    analysis_id: str,
) -> tuple[Any, ...]:
    selected = active_rules(rules, table, as_of=as_of)
    conflicts = resolve_rule_conflicts(list(selected), RulePrecedencePolicy())
    conflict_by_id = {
        rule_id: conflict
        for conflict in conflicts
        for rule_id in (conflict.left_rule_id, conflict.right_rule_id)
    }
    patterns = []
    for rule in selected:
        result = evaluate_spark_rule(frame, rule)
        if result.population_rows < detector.config.min_support_rows:
            continue
        conflict = conflict_by_id.get(rule.rule_id)
        pattern = detector._pattern(
            analysis_id,
            table,
            PatternFamily.BUSINESS_RULE,
            tuple(sorted({predicate["column"] for predicate in rule.predicates})),
            {"rule_id": rule.rule_id},
            {"strength": rule.strength.name},
            result.population_rows,
            {
                "population_rows": result.population_rows,
                "satisfying_rows": result.satisfying_rows,
                "violation_rows": result.violation_rows,
                "satisfaction_rate": result.satisfaction_rate,
                "violation_rate": result.violation_rate,
                "method": result.validation_mode,
                "rule": rule,
                "conflicts": tuple(
                    c.to_dict()
                    for c in conflicts
                    if rule.rule_id in (c.left_rule_id, c.right_rule_id)
                ),
            },
            support_rate=result.condition_support_rate,
            origin=rule.origin,
        )
        # Precedence is a review aid; it never authorizes an automatic promotion.
        pattern = replace(
            pattern,
            decision="review_required",
            lifecycle=PatternLifecycle.REVIEW_REQUIRED,
            review_status="required",
            warnings=("rule_conflict_requires_review",)
            if conflict
            else ("business_rule_requires_review",),
        )
        patterns.append(pattern)
    return tuple(patterns)
