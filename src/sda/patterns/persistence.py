from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any, cast

from sda.artifacts.delta import persist_artifact_lifecycle, persist_distributed_evidence
from sda.artifacts.fingerprint import fingerprint
from sda.patterns.models import Pattern
from sda.runtime.identifiers import QualifiedName, quote_identifier

PATTERN_REGISTRY_SCHEMA_VERSION = "sda07-pattern-registry-v2"
PATTERN_EVIDENCE_SCHEMA_VERSION = "sda07-pattern-evidence-v1"

_ASSOCIATION_NAMES = {
    "correlation": "pearson",
    "conditional_distribution": "conditional_probability",
    "conditional_missingness": "null_probability",
    "fanout_by_segment": "child_count_distribution",
    "temporal_order": "temporal_order",
    "temporal_lag": "temporal_lag_distribution",
    "state_transition": "transition_probability",
    "business_rule": "rule_satisfaction",
}


def require_pattern_schema_version(row: dict[str, Any], *, expected: str) -> None:
    """Reject pattern rows from an incompatible persistence schema."""
    actual = row.get("schema_version")
    if actual != expected:
        raise ValueError(f"incompatible pattern schema: expected {expected}, got {actual!r}")


def _safe_payload(value: Any) -> Any:
    """Keep structure while replacing non-numeric values with fingerprints."""
    if isinstance(value, dict):
        return {str(key): _safe_payload(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_safe_payload(item) for item in value]
    if isinstance(value, int | float | bool) or value is None:
        return value
    return {"fingerprint": fingerprint(value)}


def _safe_stability(value: Any) -> Any:
    if value == "unavailable":
        return {"status": "unavailable"}
    safe = _safe_payload(value)
    if (
        isinstance(value, dict)
        and isinstance(value.get("quality"), str)
        and value["quality"] in {"stable", "unstable"}
    ):
        safe["quality"] = value["quality"]
    return safe


class PatternPersistence:
    """Adapter that keeps compact registry rows and large evidence separate."""

    def __init__(
        self, spark: Any, *, registry_table: str, evidence_table: str, artifact_registry_table: str
    ) -> None:
        self.spark = spark
        self.registry_table = registry_table
        self.evidence_table = evidence_table
        self.artifact_registry_table = artifact_registry_table

    def persist(
        self, ref: Any, patterns: tuple[Pattern, ...], evidence_frame: Any | None = None
    ) -> Any:
        rows = registry_rows(
            patterns,
            configuration_hash=ref.configuration_hash,
            input_artifact_ids=ref.input_artifact_ids,
            source_references=ref.source_references,
            detector_version=ref.tool_version,
            scoring_policy_version=getattr(ref, "strategy_version", "sda07-policy-v1"),
        )
        ensure_pattern_registry_columns(self.spark, self.registry_table, rows)
        completed = persist_artifact_lifecycle(
            self.spark,
            ref,
            rows,
            evidence_location=self.registry_table,
            registry_location=self.artifact_registry_table,
            before_complete=(
                lambda: persist_distributed_evidence(
                    self.spark, evidence_frame, self.evidence_table, analysis_id=ref.artifact_id
                )
            )
            if evidence_frame is not None
            else None,
        )
        return completed


def ensure_pattern_registry_columns(spark: Any, table: str, rows: list[dict[str, Any]]) -> None:
    """Add v2 evidence fields to an existing v1 registry before publishing rows."""
    name = QualifiedName.parse(table)
    if not rows or not spark.catalog.tableExists(name.full_name):
        return
    existing = {column.lower() for column in spark.table(name.quoted).columns}
    missing = [column for column in rows[0] if column.lower() not in existing]
    if missing:
        definitions = ", ".join(f"{quote_identifier(column)} STRING" for column in missing)
        spark.sql(f"ALTER TABLE {name.quoted} ADD COLUMNS ({definitions})")
    remaining = {column.lower() for column in rows[0]} - {
        column.lower() for column in spark.table(name.quoted).columns
    }
    if remaining:
        raise ValueError(f"pattern registry schema is missing required fields: {sorted(remaining)}")


def registry_rows(
    patterns: tuple[Pattern, ...],
    *,
    configuration_hash: str = "",
    input_artifact_ids: tuple[str, ...] = (),
    source_references: tuple[Any, ...] = (),
    detector_version: str = "sda07-v1",
    scoring_policy_version: str = "sda07-policy-v1",
    precedence_policy_version: str = "sda07-precedence-v1",
) -> list[dict[str, Any]]:
    return [
        {
            "schema_version": PATTERN_REGISTRY_SCHEMA_VERSION,
            "analysis_id": p.analysis_id,
            "pattern_id": p.pattern_id,
            "pattern_version": 1,
            "family": p.family.value,
            "rule_strength": p.rule_strength,
            "origin": p.origin.value,
            "primary_table": p.primary_table,
            "table_fqns_json": json.dumps([p.primary_table]),
            "columns_json": json.dumps(p.columns),
            "condition_json": json.dumps(_safe_payload(p.condition), sort_keys=True),
            "outcome_json": json.dumps(_safe_payload(p.outcome), sort_keys=True),
            "population_json": json.dumps(
                {"support_rows": p.support_rows, "support_rate": p.support_rate},
                sort_keys=True,
            ),
            "support_rows": p.support_rows,
            "support_rate": p.support_rate,
            "association_name": _ASSOCIATION_NAMES.get(p.family.value, "pattern_metric"),
            "association_value": p.metric.get("value"),
            "baseline_json": json.dumps(
                _safe_payload(p.evidence_quality.get("baseline", {})), sort_keys=True
            ),
            "validation_mode": p.evidence_quality.get("validation_mode", "unknown"),
            "stability_json": json.dumps(
                _safe_stability(p.evidence_quality.get("stability", {})), sort_keys=True
            ),
            "evidence_quality_json": json.dumps(_safe_payload(p.evidence_quality), sort_keys=True),
            "confidence": p.evidence_quality.get("confidence"),
            "violation_count": p.evidence_quality.get("violation_count", 0),
            "violation_rate": p.evidence_quality.get("violation_rate"),
            "sampling_json": json.dumps(
                _safe_payload(p.evidence_quality.get("sampling", {})), sort_keys=True
            ),
            "limitations_json": json.dumps(
                _safe_payload(p.evidence_quality.get("limitations", ())), sort_keys=True
            ),
            "generation_action_json": json.dumps(
                _safe_payload(p.generation_action), sort_keys=True
            ),
            "validation_action_json": json.dumps(
                _safe_payload(p.validation_action), sort_keys=True
            ),
            "decision": p.decision,
            "review_status": p.review_status,
            "lifecycle": p.lifecycle.value,
            "detector_version": detector_version,
            "scoring_policy_version": scoring_policy_version,
            "rule_precedence_policy_version": precedence_policy_version,
            "configuration_hash": configuration_hash,
            "source_references_json": json.dumps(
                [
                    asdict(source) if is_dataclass(source) else cast(Any, source)  # type: ignore[arg-type]
                    for source in source_references
                ],
                sort_keys=True,
                default=str,
            ),
            "input_artifact_ids_json": json.dumps(sorted(input_artifact_ids)),
            "warnings_json": json.dumps(p.warnings),
        }
        for p in patterns
    ]


def evidence_rows(patterns: tuple[Pattern, ...]) -> list[dict[str, Any]]:
    rows = []
    for pattern in patterns:
        for key, value in sorted(pattern.metric.items()):
            rows.append(
                {
                    "schema_version": PATTERN_EVIDENCE_SCHEMA_VERSION,
                    "analysis_id": pattern.analysis_id,
                    "pattern_id": pattern.pattern_id,
                    "evidence_id": f"{pattern.pattern_id}:{key}",
                    "evidence_kind": "metric",
                    "metric_name": key,
                    "metric_value": float(value) if isinstance(value, int | float) else None,
                    "payload_json": json.dumps(_safe_payload({key: value}), sort_keys=True),
                    "validation_mode": pattern.evidence_quality.get("validation_mode", "unknown"),
                }
            )
    return rows
