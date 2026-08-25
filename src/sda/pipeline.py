"""Reference end-to-end local pipeline for approved standalone generation."""

from __future__ import annotations

from dataclasses import dataclass

from sda.generation import generate_rows
from sda.operations import AuditEvent, AuditLevel, AuditLog, ResourceBudget, enforce_budget
from sda.planning import GenerationPlan, PlanStatus
from sda.privacy import PrivacyReport, assess_privacy
from sda.publication import Publication, PublicationRegistry
from sda.validation import ValidationReport, validate_tables


@dataclass(frozen=True, slots=True)
class PipelineResult:
    rows: tuple[dict[str, object], ...]
    validation: ValidationReport
    privacy: PrivacyReport
    publication: Publication | None


def run_standalone(
    plan: GenerationPlan,
    *,
    row_count: int,
    dataset_id: str,
    dataset_version: str,
    location: str,
    actor: str | None = None,
    unique_key: str | None = None,
    sensitive_columns: tuple[tuple[str, str], ...] = (),
    audit_log: AuditLog | None = None,
) -> PipelineResult:
    """Execute generation, validation, privacy assessment, and optional publication."""
    if plan.status is not PlanStatus.APPROVED:
        raise ValueError("pipeline requires an approved generation plan")
    enforce_budget(
        ResourceBudget(max_rows=int(plan.budgets.get("max_rows", row_count))), rows=row_count
    )
    audit = audit_log or AuditLog()
    audit.append(
        AuditEvent(
            plan.request_id,
            "generation_started",
            AuditLevel.INFO,
            "standalone generation started",
            stage="generate",
        )
    )
    rows = generate_rows(plan, row_count=row_count)
    table_name = plan.tables[0]
    validation = validate_tables(
        {table_name: rows},
        expected_counts={table_name: row_count},
        unique_keys={table_name: unique_key} if unique_key else {},
        intended_use=plan.intended_use,
    )
    privacy = assess_privacy(
        {table_name: rows}, sensitive_columns=sensitive_columns, policy_ref=plan.privacy_policy_ref
    )
    publication = None
    if actor is not None:
        registry = PublicationRegistry()
        publication = registry.stage(
            Publication(
                dataset_id,
                dataset_version,
                location,
                plan.plan_fingerprint,
                plan.privacy_policy_ref,
            )
        )
        publication = registry.publish(
            dataset_id, dataset_version, validation=validation, privacy=privacy, actor=actor
        )
    audit.append(
        AuditEvent(
            plan.request_id,
            "generation_finished",
            AuditLevel.INFO,
            "standalone generation finished",
            stage="publish",
            metadata={"rows": row_count, "published": publication is not None},
        )
    )
    return PipelineResult(rows, validation, privacy, publication)
