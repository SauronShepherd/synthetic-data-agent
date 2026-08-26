from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, overload

from sda.artifacts.fingerprint import fingerprint
from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType
from sda.patterns.candidates import generate_candidates
from sda.patterns.conditionals import conditional_counts
from sda.patterns.conflicts import resolve_rule_conflicts
from sda.patterns.correlations import pearson
from sda.patterns.missingness import conditional_missingness
from sda.patterns.models import (
    ColumnRoleAssignment,
    Pattern,
    PatternConfig,
    PatternDetectionResult,
    PatternExecutionReceipt,
    PatternFamily,
    PatternInputRefs,
    PatternOrigin,
)
from sda.patterns.precedence import RulePrecedencePolicy
from sda.patterns.rules import evaluate_rule
from sda.patterns.scoring import EvidenceQuality, PatternScoringPolicy
from sda.patterns.temporal_lags import lag_distribution
from sda.patterns.transitions import state_transitions


class PatternDetector:
    """Bounded in-memory reference detector; Spark entrypoints may use equivalent aggregations."""

    def __init__(self, config: PatternConfig | None = None, **kwargs: Any) -> None:
        self.config = config or PatternConfig()
        self.spark = kwargs.get("spark")
        self.artifact_registry = kwargs.get("artifact_registry")
        self.persistence = kwargs.get("persistence")
        self.scoring_policy = kwargs.get(
            "scoring_policy", PatternScoringPolicy(min_support_rows=self.config.min_support_rows)
        )
        self.rule_precedence_policy = kwargs.get("rule_precedence_policy", RulePrecedencePolicy())

    @overload
    def detect(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        table: str | None = None,
        columns: dict[str, str] | None = None,
        analysis_id: str | None = None,
        run_id: str | None = None,
        environment: str | None = None,
        input_refs: None = None,
        selected_tables: tuple[str, ...] = (),
        approved_columns: dict[str, tuple[str, ...]] | None = None,
        role_overrides: tuple[ColumnRoleAssignment, ...] = (),
        rules: tuple[Any, ...] = (),
    ) -> tuple[Pattern, ...]: ...

    @overload
    def detect(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        table: str | None = None,
        columns: dict[str, str] | None = None,
        analysis_id: str | None = None,
        run_id: str | None = None,
        environment: str | None = None,
        input_refs: PatternInputRefs,
        selected_tables: tuple[str, ...] = (),
        approved_columns: dict[str, tuple[str, ...]] | None = None,
        role_overrides: tuple[ColumnRoleAssignment, ...] = (),
        rules: tuple[Any, ...] = (),
    ) -> PatternDetectionResult: ...

    def detect(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        table: str | None = None,
        columns: dict[str, str] | None = None,
        analysis_id: str | None = None,
        run_id: str | None = None,
        environment: str | None = None,
        input_refs: PatternInputRefs | None = None,
        selected_tables: tuple[str, ...] = (),
        approved_columns: dict[str, tuple[str, ...]] | None = None,
        role_overrides: tuple[ColumnRoleAssignment, ...] = (),
        rules: tuple[Any, ...] = (),
    ) -> tuple[Pattern, ...] | PatternDetectionResult:
        if input_refs is not None:
            return self.detect_coordinated(
                rows or [],
                run_id=run_id or analysis_id or "pattern-run",
                environment=environment or "dev",
                input_refs=input_refs,
                selected_tables=selected_tables or ((table,) if table else ()),
                approved_columns=approved_columns,
                role_overrides=role_overrides,
                rules=rules,
            )
        if table is None:
            raise ValueError("table is required")
        rows = rows or []
        if len(rows) > self.config.max_rows_scanned:
            raise ValueError(
                f"input rows exceed max_rows_scanned budget ({self.config.max_rows_scanned})"
            )
        analysis_id = analysis_id or fingerprint(
            {"table": table, "config": self.config.configuration_hash}
        )
        names = sorted(columns or ({k: "numeric" for k in rows[0]} if rows else {}))
        numeric = []
        for name in names:
            declared_type = str((columns or {}).get(name, ""))
            inferred_numeric = bool(rows) and all(
                value is None or (isinstance(value, int | float) and not isinstance(value, bool))
                for value in (row.get(name) for row in rows)
            )
            if (
                "int" in declared_type
                or "double" in declared_type
                or "float" in declared_type
                or inferred_numeric
            ):
                numeric.append(name)
        result: list[Pattern] = []
        for index, left in enumerate(numeric):
            for right in numeric[index + 1 :]:
                pairs = [
                    (float(row[left]), float(row[right]))
                    for row in rows
                    if row.get(left) is not None and row.get(right) is not None
                ]
                metric = pearson(
                    [pair[0] for pair in pairs],
                    [pair[1] for pair in pairs],
                )
                if (
                    metric["valid_pair_count"] < self.config.min_support_rows
                    or metric["value"] is None
                ):
                    continue
                result.append(
                    self._pattern(
                        analysis_id,
                        table,
                        PatternFamily.CORRELATION,
                        (left, right),
                        {},
                        {"outcome": right},
                        metric["valid_pair_count"],
                        metric,
                        support_rate=metric["valid_pair_count"] / len(rows) if rows else 0.0,
                    )
                )
                if len(result) >= self.config.max_candidates:
                    return tuple(result)
        temporal_columns = tuple(name for name in names if name.endswith(("_at", "_date")))
        if len(temporal_columns) >= 2:
            earlier, later = temporal_columns[:2]
            metric = lag_distribution(rows, earlier=earlier, later=later)
            if metric.get("count", 0) >= self.config.min_support_rows:
                result.append(
                    self._pattern(
                        analysis_id,
                        table,
                        PatternFamily.TEMPORAL_ORDER,
                        (earlier, later),
                        {},
                        {"later": later},
                        int(metric["count"]),
                        metric,
                        support_rate=metric["count"] / len(rows) if rows else 0.0,
                    )
                )
        return tuple(result)

    def detect_coordinated(
        self,
        rows: list[dict[str, Any]],
        *,
        run_id: str,
        environment: str,
        input_refs: PatternInputRefs,
        selected_tables: tuple[str, ...],
        approved_columns: dict[str, tuple[str, ...]] | None = None,
        role_overrides: tuple[ColumnRoleAssignment, ...] = (),
        rules: tuple[Any, ...] = (),
    ) -> PatternDetectionResult:
        """Execute a bounded local coordinator; Spark callers provide pre-aggregated rows."""
        table = selected_tables[0] if selected_tables else "unknown.unknown.unknown"
        input_ids = (
            input_refs.metadata_artifact_id,
            *input_refs.profile_artifact_ids,
            input_refs.relationship_artifact_id,
            input_refs.dependency_graph_artifact_id,
        )
        reuse_fingerprint = fingerprint(
            {
                "artifact_type": ArtifactType.PATTERN_REGISTRY.value,
                "environment": environment,
                "input_artifact_ids": sorted(input_ids),
                "tables": tuple(sorted(selected_tables)),
                "config": self.config.configuration_hash,
                "rules": tuple(sorted(getattr(rule, "rule_id", str(rule)) for rule in rules)),
            }
        )
        if self.artifact_registry is not None:
            reusable = self.artifact_registry.find_reusable(
                artifact_type=ArtifactType.PATTERN_REGISTRY,
                reuse_fingerprint=reuse_fingerprint,
                environment=environment,
            )
            if reusable is not None:
                return PatternDetectionResult(
                    patterns=(),
                    artifact_ref=reusable,
                    receipt=PatternExecutionReceipt(),
                    warnings=("pattern_artifact_reused",),
                )
        names = (
            approved_columns.get(table, tuple(rows[0]))
            if approved_columns
            else tuple(rows[0])
            if rows
            else ()
        )
        types = {
            name: "double"
            if rows
            and all((r.get(name) is None or isinstance(r.get(name), int | float)) for r in rows)
            else "string"
            for name in names
        }
        candidates = generate_candidates(table, types, config=self.config)
        patterns = self.detect(rows, table=table, columns=types, analysis_id=run_id)
        # Add bounded conditional and lifecycle evidence to the same aggregate result.
        categorical = [name for name in names if types.get(name) == "string"]
        numeric = [name for name in names if types.get(name) == "double"]
        for driver in categorical[: self.config.max_segment_cardinality]:
            for outcome in numeric[: self.config.max_candidates]:
                cells = conditional_counts(
                    rows, (driver,), outcome, max_cells=self.config.max_candidates
                )
                for cell in cells:
                    if cell["count"] >= self.config.min_support_rows:
                        patterns += (
                            self._pattern(
                                run_id,
                                table,
                                PatternFamily.CONDITIONAL_DISTRIBUTION,
                                (driver, outcome),
                                cell["condition"],
                                {"outcome": outcome},
                                cell["count"],
                                {"conditional_rate": cell["rate"], "method": "exact"},
                            ),
                        )
        # Emit conditional-missingness findings for every bounded driver/outcome pair.
        for driver in categorical[: self.config.max_candidates]:
            driver_values = sorted({row.get(driver) for row in rows}, key=lambda value: str(value))[
                : self.config.max_segment_cardinality
            ]
            for driver_value in driver_values:
                for outcome in names:
                    metric = conditional_missingness(rows, {driver: driver_value}, outcome)
                    if metric.get("support_rows", 0) >= self.config.min_support_rows:
                        patterns += (
                            self._pattern(
                                run_id,
                                table,
                                PatternFamily.CONDITIONAL_MISSINGNESS,
                                (driver, outcome),
                                {driver: driver_value},
                                {"outcome": outcome},
                                int(metric["support_rows"]),
                                metric,
                            ),
                        )
        # State transitions are entity/lifecycle evidence, not hard business rules.
        entity = next((name for name in names if name.lower().endswith(("_id", "id"))), None)
        state = next(
            (name for name in categorical if name.lower() in {"status", "state", "stage"}),
            None,
        )
        order = next((name for name in names if name.endswith(("_at", "_date"))), None)
        if entity and state and order:
            transition_metric = state_transitions(
                rows, entity_key=entity, state_column=state, order_column=order
            )
            for transition in transition_metric.get("transitions", ()):
                if transition["transition_count"] >= self.config.min_support_rows:
                    patterns += (
                        self._pattern(
                            run_id,
                            table,
                            PatternFamily.STATE_TRANSITION,
                            (state,),
                            {"from_state": transition["from_state"]},
                            {"to_state": transition["to_state"]},
                            transition["transition_count"],
                            transition,
                        ),
                    )
        # Evaluate approved/user rules and preserve their review semantics.
        for rule in rules:
            evaluation = evaluate_rule(rows, rule)
            if evaluation.population_rows >= self.config.min_support_rows:
                patterns += (
                    self._pattern(
                        run_id,
                        table,
                        PatternFamily.BUSINESS_RULE,
                        tuple(sorted({p["column"] for p in rule.predicates})),
                        {"rule_id": rule.rule_id},
                        {"strength": rule.strength.name},
                        evaluation.population_rows,
                        {
                            "population_rows": evaluation.population_rows,
                            "satisfying_rows": evaluation.satisfying_rows,
                            "violation_rows": evaluation.violation_rows,
                            "satisfaction_rate": evaluation.satisfaction_rate,
                            "violation_rate": evaluation.violation_rate,
                            "method": evaluation.validation_mode,
                        },
                    ),
                )
        for earlier, later in zip(
            tuple(name for name in names if name.endswith(("_at", "_date")))[:2],
            tuple(name for name in names if name.endswith(("_at", "_date")))[1:2],
            strict=False,
        ):
            metric = lag_distribution(rows, earlier=earlier, later=later)
            if metric.get("count", 0) >= self.config.min_support_rows:
                patterns += (
                    self._pattern(
                        run_id,
                        table,
                        PatternFamily.TEMPORAL_ORDER,
                        (earlier, later),
                        {},
                        {"later": later},
                        int(metric["count"]),
                        metric,
                    ),
                )
        if rows:
            patterns = tuple(
                replace(pattern, support_rate=pattern.support_rows / len(rows))
                if pattern.support_rate is None
                else pattern
                for pattern in patterns
            )
        accepted = sum(p.decision == "accepted_for_planning" for p in patterns)
        rule_conflicts = resolve_rule_conflicts(list(rules), self.rule_precedence_policy)
        receipt = PatternExecutionReceipt(
            candidate_count_total=len(candidates),
            patterns_emitted=len(patterns),
            patterns_accepted_for_planning=accepted,
            patterns_review_required=len(patterns) - accepted,
            rules_evaluated=sum(
                1
                for rule in rules
                if evaluate_rule(rows, rule).population_rows >= self.config.min_support_rows
            ),
            source_tables_scanned=len(selected_tables),
            sample_fraction=self.config.sample_fraction,
            sample_seed=self.config.sample_seed,
        )
        artifact_ref = None
        if self.persistence is not None:
            artifact_ref = ArtifactRef(
                artifact_id="patreg_" + reuse_fingerprint,
                artifact_type=ArtifactType.PATTERN_REGISTRY,
                artifact_schema_version="2.0",
                status=ArtifactStatus.WRITING,
                tool_name="pattern_detector",
                tool_version=self.config.detector_version,
                strategy_version=self.config.detector_version,
                run_id=run_id,
                environment=environment,
                created_at=datetime.now(UTC).isoformat(),
                configuration_hash=self.config.configuration_hash,
                primary_location=getattr(self.persistence, "registry_table", ""),
                related_locations={"evidence": getattr(self.persistence, "evidence_table", "")},
                source_references=(),
                checksum=reuse_fingerprint,
                summary="SDA 07 pattern registry",
                input_artifact_ids=tuple(sorted(input_ids)),
                reuse_fingerprint=reuse_fingerprint,
            )
            artifact_ref = self.persistence.persist(artifact_ref, patterns)
            if self.artifact_registry is not None:
                self.artifact_registry.put(artifact_ref)
        return PatternDetectionResult(
            patterns=patterns,
            artifact_ref=artifact_ref,
            receipt=receipt,
            warnings=tuple(sorted({w for p in patterns for w in p.warnings}))
            + tuple(
                "rule_conflict_requires_review"
                for conflict in rule_conflicts
                if conflict.requires_review
            ),
            review_questions=tuple(
                {
                    "pattern_id": p.pattern_id,
                    "reason_code": "observed_candidate_rule_requires_domain_approval",
                }
                for p in patterns
            )
            + tuple(
                {
                    "rule_conflict": (conflict.left_rule_id, conflict.right_rule_id),
                    "reason_code": "rule_conflict_requires_review",
                }
                for conflict in rule_conflicts
                if conflict.requires_review
            ),
        )

    def _pattern(
        self,
        analysis_id: str,
        table: str,
        family: PatternFamily,
        columns: tuple[str, ...],
        condition: dict[str, Any],
        outcome: dict[str, Any],
        support: int,
        metric: dict[str, Any],
        support_rate: float | None = None,
    ) -> Pattern:
        pid = "pat_" + fingerprint(
            {
                "analysis": analysis_id,
                "family": family.value,
                "columns": columns,
                "condition": condition,
            }
        )
        decision = self.scoring_policy.decide(
            support_rows=support,
            metric=metric.get("value"),
            quality=EvidenceQuality(
                support_quality="sufficient",
                validation_mode=metric.get("method", "exact"),
                stability_quality="unknown",
                source_quality="compatible",
            ),
            origin=PatternOrigin.OBSERVED,
        ).value
        population_rows = int(metric.get("population_rows", support))
        violation_rows = int(metric.get("violation_rows", 0))
        violation_rate = metric.get("violation_rate")
        if violation_rate is None and population_rows:
            violation_rate = violation_rows / population_rows
        evidence_quality = {
            "validation_mode": metric.get("method", "exact"),
            "support_quality": "sufficient",
            "confidence": metric.get("confidence"),
            "stability": metric.get("stability", "unknown"),
            "population_rows": population_rows,
            "sampling": {
                "fraction": self.config.sample_fraction,
                "seed": self.config.sample_seed,
            },
            "violation_count": violation_rows,
            "violation_rate": violation_rate,
            "limitations": tuple(metric.get("limitations", ())),
        }
        return Pattern(
            pid,
            analysis_id,
            family,
            PatternOrigin.OBSERVED,
            table,
            columns,
            condition,
            outcome,
            support,
            support_rate,
            metric,
            evidence_quality,
            decision=decision,
            warnings=("observed_pattern_requires_review",)
            if family is PatternFamily.BUSINESS_RULE
            else (),
            generation_action={
                "kind": {
                    PatternFamily.CORRELATION: "preserve_numeric_dependency",
                    PatternFamily.CONDITIONAL_DISTRIBUTION: "sample_conditional_distribution",
                    PatternFamily.CONDITIONAL_MISSINGNESS: "apply_conditional_missingness",
                    PatternFamily.FANOUT_BY_SEGMENT: "sample_child_count_by_parent_segment",
                    PatternFamily.TEMPORAL_ORDER: "sample_temporal_lag",
                    PatternFamily.STATE_TRANSITION: "sample_next_state",
                }.get(family, "review_pattern"),
                "evidence_pattern_id": pid,
            },
            validation_action={
                "kind": {
                    PatternFamily.CORRELATION: "compare_correlation",
                    PatternFamily.CONDITIONAL_DISTRIBUTION: "compare_conditional_distribution",
                    PatternFamily.CONDITIONAL_MISSINGNESS: "compare_null_probability",
                    PatternFamily.TEMPORAL_ORDER: "compare_lag_distribution",
                    PatternFamily.STATE_TRANSITION: "compare_transition_probabilities",
                }.get(family, "review_pattern")
            },
            review_status="required" if family is PatternFamily.BUSINESS_RULE else "not_required",
        )


def detect_conditionals(
    rows: list[dict[str, Any]],
    *,
    table: str,
    drivers: tuple[str, ...],
    outcome: str,
    config: PatternConfig | None = None,
) -> tuple[dict[str, Any], ...]:
    cfg = config or PatternConfig()
    return tuple(
        cell
        for cell in conditional_counts(rows, drivers, outcome, max_cells=cfg.max_candidates)
        if cell["count"] >= cfg.min_support_rows
    )


def detect_missingness(
    rows: list[dict[str, Any]], *, condition: dict[str, Any], outcome: str
) -> dict[str, Any]:
    return conditional_missingness(rows, condition, outcome)
