from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, overload

from sda.artifacts.fingerprint import fingerprint
from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType, SourceReference
from sda.patterns.actions import GenerationAction, ValidationAction
from sda.patterns.candidates import generate_candidates
from sda.patterns.conditionals import conditional_counts, deterministic_fallback_plan
from sda.patterns.conflicts import resolve_rule_conflicts
from sda.patterns.correlations import correlation_outlier_diagnostic, pearson, spearman
from sda.patterns.fanout import fanout_by_segment
from sda.patterns.loaders import load_pattern_inputs
from sda.patterns.missingness import conditional_missingness
from sda.patterns.models import (
    ColumnRoleAssignment,
    Pattern,
    PatternConfig,
    PatternDetectionResult,
    PatternExecutionReceipt,
    PatternFamily,
    PatternInputRefs,
    PatternLifecycle,
    PatternOrigin,
)
from sda.patterns.numeric import numeric_by_group
from sda.patterns.precedence import RulePrecedencePolicy
from sda.patterns.roles import assign_roles
from sda.patterns.rules import evaluate_rule
from sda.patterns.safety import safe_pattern_value
from sda.patterns.scoring import EvidenceQuality, PatternScoringPolicy
from sda.patterns.stability import stability
from sda.patterns.temporal import ordered_events, temporal_order
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
        fanout_inputs: tuple[dict[str, Any], ...] = (),
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
        fanout_inputs: tuple[dict[str, Any], ...] = (),
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
        fanout_inputs: tuple[dict[str, Any], ...] = (),
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
                fanout_inputs=fanout_inputs,
            )
        if table is None:
            raise ValueError("table is required")
        rows = rows or []
        self._sensitive_columns: set[str] = set()
        if len(rows) > self.config.max_rows_scanned:
            raise ValueError(
                f"input rows exceed max_rows_scanned budget ({self.config.max_rows_scanned})"
            )
        # Input partition/order is not evidence.  Canonical row fingerprints make
        # the in-memory reference implementation match the partition-independent
        # contract expected from distributed aggregations without retaining raw
        # values in any artifact.
        rows = sorted(rows, key=fingerprint)
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
                if self.config.include_spearman:
                    metric["spearman"] = spearman(
                        [pair[0] for pair in pairs], [pair[1] for pair in pairs]
                    )
                metric["correlation_outlier_diagnostic"] = correlation_outlier_diagnostic(
                    [pair[0] for pair in pairs], [pair[1] for pair in pairs]
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
        categorical = [
            name
            for name in names
            if name not in numeric
            and len({fingerprint(row.get(name)) for row in rows})
            <= self.config.max_segment_cardinality
        ]
        for driver in categorical:
            for outcome in names:
                if driver == outcome:
                    continue
                cells = conditional_counts(
                    rows,
                    (driver,),
                    outcome,
                    max_cells=self.config.max_candidates - len(result),
                )
                for cell in cells:
                    if cell["count"] < self.config.min_support_rows:
                        continue
                    result.append(
                        self._pattern(
                            analysis_id,
                            table,
                            PatternFamily.CONDITIONAL_DISTRIBUTION,
                            (driver, outcome),
                            cell["condition"],
                            {"outcome": outcome},
                            cell["count"],
                            {"conditional_rate": cell["rate"], "method": "exact"},
                        )
                    )
                    if len(result) >= self.config.max_candidates:
                        return tuple(result)
        # Evaluate conditional null rates in the standalone path as well as the
        # coordinated path, using stable segment ordering and the same support gate.
        for driver in categorical:
            values_by_fingerprint = {fingerprint(row.get(driver)): row.get(driver) for row in rows}
            values = [values_by_fingerprint[key] for key in sorted(values_by_fingerprint)][
                : self.config.max_segment_cardinality
            ]
            for value in values:
                for outcome in names:
                    if driver == outcome:
                        continue
                    metric = conditional_missingness(rows, {driver: value}, outcome)
                    support = int(metric["support_rows"])
                    if support < self.config.min_support_rows:
                        continue
                    result.append(
                        self._pattern(
                            analysis_id,
                            table,
                            PatternFamily.CONDITIONAL_MISSINGNESS,
                            (driver, outcome),
                            {driver: value},
                            {"outcome": outcome},
                            support,
                            metric,
                            support_rate=support / len(rows) if rows else 0.0,
                        )
                    )
                    if len(result) >= self.config.max_candidates:
                        return tuple(result)
        entity_columns = tuple(
            name for name in names if name.lower() == "id" or name.lower().endswith("_id")
        )
        state_columns = tuple(
            name for name in names if name.lower() in {"state", "status", "stage"}
        )
        temporal_columns = tuple(
            name for name in names if name.endswith(("_at", "_date", "_time", "_ts"))
        )
        if entity_columns and state_columns and temporal_columns:
            for entity in entity_columns[:1]:
                for state in state_columns[:1]:
                    for order in temporal_columns[:1]:
                        metric = state_transitions(
                            rows,
                            entity_key=entity,
                            state_column=state,
                            order_column=order,
                            max_states=self.config.max_segment_cardinality,
                        )
                        ingestion = next(
                            (
                                name
                                for name in names
                                if name.lower() in {"ingested_at", "ingestion_at", "ingestion_time"}
                            ),
                            None,
                        )
                        metric["event_order"] = ordered_events(
                            rows,
                            entity_key=entity,
                            event_time=order,
                            state_column=state,
                            ingestion_time=ingestion,
                        )
                        support = sum(
                            int(item["transition_count"]) for item in metric["transitions"]
                        )
                        if support >= self.config.min_support_rows:
                            result.append(
                                self._pattern(
                                    analysis_id,
                                    table,
                                    PatternFamily.STATE_TRANSITION,
                                    (entity, state, order),
                                    {},
                                    {"state_column": state},
                                    support,
                                    metric,
                                    support_rate=support / len(rows) if rows else 0.0,
                                )
                            )
                            if len(result) >= self.config.max_candidates:
                                return tuple(result)
        if len(temporal_columns) >= 2:
            earlier, later = temporal_columns[:2]
            order_metric = temporal_order(rows, earlier, later)
            lag_metric = lag_distribution(rows, earlier=earlier, later=later)
            if order_metric.get("eligible_rows", 0) >= self.config.min_support_rows:
                result.append(
                    self._pattern(
                        analysis_id,
                        table,
                        PatternFamily.TEMPORAL_ORDER,
                        (earlier, later),
                        {},
                        {"later": later},
                        int(order_metric["eligible_rows"]),
                        {**order_metric, **lag_metric},
                        support_rate=order_metric["eligible_rows"] / len(rows) if rows else 0.0,
                    )
                )
            metric = lag_metric
            if metric.get("count", 0) >= self.config.min_support_rows:
                result.append(
                    self._pattern(
                        analysis_id,
                        table,
                        PatternFamily.TEMPORAL_LAG,
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
        fanout_inputs: tuple[dict[str, Any], ...] = (),
    ) -> PatternDetectionResult:
        """Execute a bounded local coordinator; Spark callers provide pre-aggregated rows."""
        if not rows:
            raise ValueError(
                "coordinated pattern detection requires available input rows; received none"
            )
        if self.config.mode == "quick" and self.config.sample_fraction < 1.0:
            threshold = int(self.config.sample_fraction * 1_000_000)
            rows = [
                row
                for row in rows
                if int(fingerprint({"row": row, "seed": self.config.sample_seed})[:12], 16)
                % 1_000_000
                < threshold
            ]
            if not rows:
                raise ValueError("quick-mode sample contains no rows")
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
        upstream_artifacts: tuple[ArtifactRef, ...] = ()
        self._source_compatible = True
        if self.artifact_registry is not None:
            # Validate every upstream artifact through the shared contract before
            # reuse or detection; callers may additionally provide the bounded
            # row evidence used by this local coordinator.
            upstream_artifacts = load_pattern_inputs(
                self.artifact_registry, input_refs, environment=environment
            )
            snapshots: list[str] = [
                reference.source_version
                for artifact in upstream_artifacts
                for reference in artifact.source_references
                if reference.source_version is not None
            ]
            self._source_compatible = not snapshots or len(set(snapshots)) == 1
            reusable = self.artifact_registry.find_reusable(
                artifact_type=ArtifactType.PATTERN_REGISTRY,
                reuse_fingerprint=reuse_fingerprint,
                environment=environment,
            )
            if reusable is not None:
                return PatternDetectionResult(
                    patterns=(),
                    artifact_ref=reusable,
                    receipt=PatternExecutionReceipt(
                        source_tables_scanned=len(selected_tables),
                        source_tables_reused=len(selected_tables),
                        sample_fraction=self.config.sample_fraction,
                        sample_seed=self.config.sample_seed,
                    ),
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
        metadata_content: dict[str, Any] = (
            upstream_artifacts[0].content if upstream_artifacts else {}
        )
        profile_content: dict[str, Any] = (
            upstream_artifacts[1].content if len(upstream_artifacts) > 1 else {}
        )
        metadata_table = (
            next(
                (
                    item
                    for item in metadata_content.get("tables", ())
                    if item.get("full_name") == table
                ),
            )
            if isinstance(metadata_content.get("tables", ()), list)
            else None
        )
        raw_sensitivity = (
            metadata_table.get("sensitivity_signals", {})
            if metadata_table
            else metadata_content.get("sensitivity_signals", {})
        )
        sensitivity = raw_sensitivity if isinstance(raw_sensitivity, dict) else {}
        self._sensitive_columns = {name for name, signals in sensitivity.items() if signals}
        role_columns = [
            {"name": name, "data_type": types[name], "sensitivity": sensitivity.get(name, "")}
            for name in names
        ]
        roles = assign_roles(role_columns)
        profiled_columns = tuple(
            {
                "name": item.get("column_name", item.get("name")),
                "data_type": item.get("declared_data_type", item.get("data_type", "")),
                "sensitivity": item.get("sensitivity_signals", ()),
            }
            for item in profile_content.get("column_profiles", ())
            if item.get("column_name", item.get("name"))
        )
        if profiled_columns:
            roles = assign_roles(tuple(profiled_columns))
        # Numeric columns are outcomes for correlation/distribution evidence;
        # drivers may be categorical or numeric, while excluded/sensitive fields
        # never enter either role.
        excluded = set(roles.get("excluded", ())) | set(roles.get("entity", ()))
        roles["outcome"] = tuple(
            name
            for name in roles.get("outcome", ())
            if types.get(name) == "double" and name not in excluded
        )
        roles["driver"] = tuple(
            name for name in roles.get("driver", ()) if name in names and name not in excluded
        )
        for override in role_overrides:
            if override.table == table:
                roles[override.role] = tuple(
                    sorted(set(roles.get(override.role, ())) | {override.column})
                )
        candidates = generate_candidates(table, types, roles=roles, config=self.config)
        candidate_count_by_family: dict[str, int] = {}
        for candidate in candidates:
            family = candidate.family.value
            candidate_count_by_family[family] = candidate_count_by_family.get(family, 0) + 1
        patterns = tuple(
            pattern
            for pattern in self.detect(rows, table=table, columns=types, analysis_id=run_id)
            if pattern.family in {PatternFamily.CORRELATION, PatternFamily.TEMPORAL_ORDER}
        )
        # Add bounded conditional and lifecycle evidence to the same aggregate result.
        categorical = [name for name in roles.get("driver", ()) if types.get(name) == "string"]
        numeric = [name for name in roles.get("outcome", ()) if types.get(name) == "double"]

        def supported(support: int, population: int | None = None) -> bool:
            denominator = population if population is not None else len(rows)
            return support >= self.config.min_support_rows and (
                denominator == 0 or support / denominator >= self.config.min_support_rate
            )

        for driver in categorical[: self.config.max_segment_cardinality]:
            for outcome in numeric[: self.config.max_candidates]:
                cells = conditional_counts(
                    rows, (driver,), outcome, max_cells=self.config.max_candidates
                )
                for cell in cells:
                    condition_rows = sum(
                        1
                        for row in rows
                        if all(row.get(key) == value for key, value in cell["condition"].items())
                    )
                    if supported(cell["count"], condition_rows):
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
                for group in numeric_by_group(rows, group=driver, outcome=outcome):
                    if supported(group["count"]):
                        patterns += (
                            self._pattern(
                                run_id,
                                table,
                                PatternFamily.CONDITIONAL_DISTRIBUTION,
                                (driver, outcome),
                                {driver: group["group"]},
                                {"outcome": outcome},
                                group["count"],
                                {**group, "global_count": len(rows), "method": "exact"},
                            ),
                        )
        # Emit conditional-missingness findings for every bounded driver/outcome pair.
        for driver in categorical[: self.config.max_candidates]:
            driver_values = sorted({row.get(driver) for row in rows}, key=lambda value: str(value))[
                : self.config.max_segment_cardinality
            ]
            for driver_value in driver_values:
                for outcome in names:
                    if outcome in excluded:
                        continue
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
            ingestion = next(
                (
                    name
                    for name in names
                    if name.lower() in {"ingested_at", "ingestion_at", "ingestion_time"}
                ),
                None,
            )
            event_order_metric = ordered_events(
                rows,
                entity_key=entity,
                event_time=order,
                state_column=state,
                ingestion_time=ingestion,
            )
            transition_metric = {
                **transition_metric,
                "event_order": event_order_metric,
                "warnings": tuple(
                    dict.fromkeys(
                        (
                            *transition_metric.get("warnings", ()),
                            *event_order_metric.get("warnings", ()),
                        )
                    )
                ),
            }
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
                            "rule": rule,
                        },
                        origin=rule.origin,
                    ),
                )
        # Relationship evidence is supplied as already-authorized bounded rows;
        # this stage never reads raw child rows implicitly.
        for relation in fanout_inputs:
            required = {"parents", "children", "parent_key", "segment", "child_key"}
            if set(relation) != required:
                raise ValueError(
                    "fanout input must contain exactly the required relationship fields"
                )
            metrics = fanout_by_segment(
                relation["parents"],
                relation["children"],
                parent_key=relation["parent_key"],
                segment=relation["segment"],
                child_key=relation["child_key"],
            )
            for metric in metrics:
                if metric["parent_count"] < self.config.min_support_rows:
                    continue
                patterns += (
                    self._pattern(
                        run_id,
                        table,
                        PatternFamily.FANOUT_BY_SEGMENT,
                        (relation["segment"],),
                        {"segment": metric["segment"]},
                        {"child_count": relation["child_key"]},
                        metric["parent_count"],
                        {**metric, "method": "exact"},
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
                        PatternFamily.TEMPORAL_LAG,
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
        rejected = sum(p.decision == "rejected" for p in patterns)
        insufficient = sum(p.decision == "insufficient_evidence" for p in patterns)
        rule_conflicts = resolve_rule_conflicts(list(rules), self.rule_precedence_policy)
        emitted_by_family: dict[str, int] = {}
        for pattern in patterns:
            emitted_by_family[pattern.family.value] = (
                emitted_by_family.get(pattern.family.value, 0) + 1
            )
        skipped_by_reason = {
            f"{family}_insufficient_support": count - emitted_by_family.get(family, 0)
            for family, count in candidate_count_by_family.items()
            if count > emitted_by_family.get(family, 0)
        }
        receipt = PatternExecutionReceipt(
            candidate_count_total=len(candidates),
            candidate_count_by_family=candidate_count_by_family,
            candidate_skipped_by_reason=skipped_by_reason,
            patterns_emitted=len(patterns),
            patterns_accepted_for_planning=accepted,
            patterns_review_required=len(patterns) - accepted - rejected - insufficient,
            patterns_rejected=rejected,
            patterns_insufficient=insufficient,
            conflicts_found=len(rule_conflicts),
            rules_evaluated=len(rules),
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
                source_references=(
                    tuple(
                        reference
                        for artifact in upstream_artifacts
                        for reference in artifact.source_references
                    )
                    or (SourceReference(table, "TABLE", "best_effort", None, None, None),)
                ),
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
                if p.review_status == "required" or p.decision == "review_required"
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
        origin: PatternOrigin = PatternOrigin.OBSERVED,
    ) -> Pattern:
        sensitive_columns: set[str] = getattr(self, "_sensitive_columns", set())

        def protect(payload: dict[str, Any]) -> dict[str, Any]:
            protected = dict(payload)
            for key in tuple(protected):
                if key in sensitive_columns:
                    safe = safe_pattern_value(
                        value=protected[key],
                        column_policy=(
                            "redact_values"
                            if self.config.sensitive_value_policy == "fingerprints_only"
                            else "no_values"
                        ),
                    )
                    protected[key] = {"kind": safe.kind.value, "value": safe.value}
            return protected

        condition = protect(condition)
        outcome = protect(outcome)
        pid = "pat_" + fingerprint(
            {
                "analysis": self.config.configuration_hash,
                "family": family.value,
                "columns": columns,
                "condition": condition,
            }
        )
        stability_values = metric.get("stability_values")
        stability_result = stability(list(stability_values)) if stability_values else None
        decision = self.scoring_policy.decide(
            support_rows=support,
            metric=metric.get("value"),
            quality=EvidenceQuality(
                support_quality="sufficient",
                validation_mode=metric.get("method", "exact"),
                stability_quality=(
                    "stable"
                    if stability_result is not None and stability_result.get("stable") is True
                    else "unstable"
                    if stability_result is not None
                    else str(metric.get("stability", "unavailable"))
                ),
                source_quality=(
                    "compatible"
                    if metric.get("source_compatible", getattr(self, "_source_compatible", True))
                    else "mismatch"
                ),
            ),
            origin=origin,
        ).value
        population_rows = int(metric.get("population_rows", support))
        if support_rate is None and population_rows > 0:
            support_rate = support / population_rows
        violation_rows = int(metric.get("violation_rows", 0))
        violation_rate = metric.get("violation_rate")
        if violation_rate is None and population_rows:
            violation_rate = violation_rows / population_rows
        evidence_quality = {
            "validation_mode": metric.get("method", "exact"),
            "source_quality": "compatible"
            if metric.get("source_compatible", getattr(self, "_source_compatible", True))
            else "mismatch",
            "support_quality": "sufficient",
            "confidence": metric.get("confidence"),
            "stability": stability_result or metric.get("stability", "unavailable"),
            "population_rows": population_rows,
            "sampling": {
                "fraction": self.config.sample_fraction,
                "seed": self.config.sample_seed,
            },
            "violation_count": violation_rows,
            "violation_rate": violation_rate,
            "limitations": tuple(metric.get("limitations", ())),
        }
        generation_kind = {
            PatternFamily.CORRELATION: "preserve_numeric_dependency",
            PatternFamily.CONDITIONAL_DISTRIBUTION: "sample_conditional_distribution",
            PatternFamily.CONDITIONAL_MISSINGNESS: "apply_conditional_missingness",
            PatternFamily.FANOUT_BY_SEGMENT: "sample_child_count_by_parent_segment",
            PatternFamily.TEMPORAL_ORDER: "sample_temporal_lag",
            PatternFamily.TEMPORAL_LAG: "sample_temporal_lag",
            PatternFamily.STATE_TRANSITION: "sample_next_state",
        }.get(family, "review_pattern")
        validation_kind = {
            PatternFamily.CORRELATION: "compare_correlation",
            PatternFamily.CONDITIONAL_DISTRIBUTION: "compare_conditional_distribution",
            PatternFamily.CONDITIONAL_MISSINGNESS: "compare_null_probability",
            PatternFamily.TEMPORAL_ORDER: "compare_lag_distribution",
            PatternFamily.TEMPORAL_LAG: "compare_lag_distribution",
            PatternFamily.STATE_TRANSITION: "compare_transition_probabilities",
        }.get(family, "review_pattern")
        fallback = deterministic_fallback_plan(
            tuple(condition),
            min_support_rows=self.config.min_support_rows,
            min_support_rate=self.config.min_support_rate,
        )
        return Pattern(
            pid,
            analysis_id,
            family,
            origin,
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
            generation_action=GenerationAction(
                kind=generation_kind,
                evidence_pattern_id=pid,
                condition=tuple(condition),
                fallback_levels=tuple(level.condition_columns for level in fallback.levels),
            ).to_dict(),
            validation_action=ValidationAction(
                kind=validation_kind,
                evidence_pattern_id=pid,
                metric=family.value,
                tolerance=metric.get("tolerance"),
            ).to_dict(),
            review_status="required" if family is PatternFamily.BUSINESS_RULE else "not_required",
            rule_strength=(
                metric["rule"].strength.name.lower()
                if metric.get("rule") is not None
                else "probabilistic_pattern"
            ),
            lifecycle=(
                PatternLifecycle.REJECTED
                if decision == "rejected"
                else PatternLifecycle.REVIEW_REQUIRED
                if decision == "review_required"
                else PatternLifecycle.INSUFFICIENT_EVIDENCE
                if decision == "insufficient_evidence"
                else PatternLifecycle.DECLARED_RULE
                if origin in {PatternOrigin.DECLARED, PatternOrigin.USER_PROVIDED}
                else PatternLifecycle.APPROVED_RULE
                if origin in {PatternOrigin.DOMAIN_APPROVED, PatternOrigin.DESTINATION_CONSTRAINT}
                else PatternLifecycle.OBSERVED_PATTERN
            ),
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
