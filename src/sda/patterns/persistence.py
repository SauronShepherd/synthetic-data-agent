from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any, cast

from sda.artifacts.delta import persist_artifact_lifecycle, persist_distributed_evidence
from sda.patterns.models import Pattern

PATTERN_REGISTRY_SCHEMA_VERSION = "sda07-pattern-registry-v1"
PATTERN_EVIDENCE_SCHEMA_VERSION = "sda07-pattern-evidence-v1"


def require_pattern_schema_version(row: dict[str, Any], *, expected: str) -> None:
    """Reject pattern rows from an incompatible persistence schema."""
    actual = row.get("schema_version")
    if actual != expected:
        raise ValueError(f"incompatible pattern schema: expected {expected}, got {actual!r}")


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
        completed = persist_artifact_lifecycle(
            self.spark,
            ref,
            registry_rows(
                patterns,
                configuration_hash=ref.configuration_hash,
                input_artifact_ids=ref.input_artifact_ids,
                source_references=ref.source_references,
                detector_version=ref.tool_version,
                scoring_policy_version=getattr(ref, "strategy_version", "sda07-policy-v1"),
            ),
            evidence_location=self.registry_table,
            registry_location=self.artifact_registry_table,
        )
        if evidence_frame is not None:
            persist_distributed_evidence(
                self.spark, evidence_frame, self.evidence_table, analysis_id=ref.artifact_id
            )
        return completed


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
            "condition_json": json.dumps(p.condition, sort_keys=True),
            "outcome_json": json.dumps(p.outcome, sort_keys=True),
            "population_json": json.dumps(
                {"support_rows": p.support_rows, "support_rate": p.support_rate},
                sort_keys=True,
            ),
            "support_rows": p.support_rows,
            "support_rate": p.support_rate,
            "association_name": next(iter(p.metric), None),
            "association_value": p.metric.get("value"),
            "baseline_json": json.dumps(p.evidence_quality.get("baseline", {}), sort_keys=True),
            "validation_mode": p.evidence_quality.get("validation_mode", "unknown"),
            "stability_json": json.dumps(p.evidence_quality.get("stability", {}), sort_keys=True),
            "generation_action_json": json.dumps(p.generation_action, sort_keys=True),
            "validation_action_json": json.dumps(p.validation_action, sort_keys=True),
            "decision": p.decision,
            "review_status": p.review_status,
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
                    "payload_json": json.dumps({key: value}, sort_keys=True),
                    "validation_mode": pattern.evidence_quality.get("validation_mode", "unknown"),
                }
            )
    return rows
