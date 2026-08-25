"""Read-only operational snapshot for dashboards and natural-language tools."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sda.operations import AuditEvent, AuditLog
from sda.privacy import PrivacyReport
from sda.publication import Publication
from sda.state import RunRecord
from sda.validation import ValidationReport


@dataclass(frozen=True, slots=True)
class ControlCenterSnapshot:
    run: RunRecord
    validation: ValidationReport | None
    privacy: PrivacyReport | None
    publication: Publication | None
    artifact_ids: tuple[str, ...]
    audit_events: tuple[AuditEvent, ...]

    def to_safe_dict(self) -> dict[str, Any]:
        """Return dashboard data without source rows or secret values."""
        return {
            "run_id": self.run.run_id,
            "request_id": self.run.request_id,
            "status": self.run.status.value,
            "plan_id": self.run.plan_id,
            "plan_fingerprint": self.run.plan_fingerprint,
            "artifact_ids": self.artifact_ids,
            "validation_disposition": self.validation.technical_disposition.value
            if self.validation
            else None,
            "privacy_decision": self.privacy.decision.value if self.privacy else None,
            "publication_status": self.publication.status.value if self.publication else None,
            "audit_event_count": len(self.audit_events),
        }


def build_snapshot(
    run: RunRecord,
    *,
    artifact_ids: tuple[str, ...] = (),
    validation: ValidationReport | None = None,
    privacy: PrivacyReport | None = None,
    publication: Publication | None = None,
    audit_log: AuditLog | None = None,
) -> ControlCenterSnapshot:
    return ControlCenterSnapshot(
        run,
        validation,
        privacy,
        publication,
        artifact_ids,
        audit_log.for_run(run.run_id) if audit_log else (),
    )
