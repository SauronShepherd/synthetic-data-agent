from __future__ import annotations

import pytest

from sda.privacy import PrivacyDecision, PrivacyFinding, PrivacyReport, assess_privacy


def test_sensitive_output_requires_explicit_approval() -> None:
    report = assess_privacy(
        {"t": (({"email": "x@example.test"}),)}, sensitive_columns=(("t", "email"),)
    )
    assert report.decision is PrivacyDecision.REJECTED
    assert report.findings[0].code == "sensitive_column_not_approved"


def test_duplicate_rows_require_review() -> None:
    report = assess_privacy({"t": (({"x": 1}), ({"x": 1}))})
    assert report.decision is PrivacyDecision.REVIEW_REQUIRED


def test_clean_approved_output_passes() -> None:
    report = assess_privacy(
        {"t": (({"email": "synthetic@example.test"}),)},
        sensitive_columns=(("t", "email"),),
        approved_columns=(("t", "email"),),
    )
    assert report.decision is PrivacyDecision.APPROVED


def test_approved_sensitive_column_missing_fails_closed() -> None:
    report = assess_privacy(
        {"users": (({"id": 1}),)},
        sensitive_columns=(("users", "email"),),
        approved_columns=(("users", "email"),),
    )
    assert report.decision is PrivacyDecision.REJECTED
    assert report.findings[0].code == "approved_sensitive_column_missing"


def test_duplicate_detection_handles_nested_values_without_raw_material() -> None:
    report = assess_privacy({"t": (({"tags": ["a", "b"]}), ({"tags": ["a", "b"]}))})
    assert report.decision is PrivacyDecision.REVIEW_REQUIRED
    assert "tags" not in str(report.findings[0].evidence)


def test_direct_identifiers_and_rare_quasi_identifiers_are_checked() -> None:
    report = assess_privacy(
        {"users": (({"email": "a@example.test", "zip": "10001"}),)},
        direct_identifier_columns=(("users", "email"),),
        quasi_identifier_columns=(("users", "zip"),),
    )
    assert report.decision is PrivacyDecision.REJECTED
    assert {finding.code for finding in report.findings} == {
        "direct_identifier_not_approved",
        "rare_quasi_identifier_values",
    }
    assert "a@example.test" not in str(report.findings)


def test_privacy_report_serializes_decision_and_schema_version() -> None:
    report = assess_privacy({"t": (({"x": 1}),)})
    assert report.to_dict() == {
        "decision": "approved",
        "findings": (),
        "policy_ref": "strict-default",
        "schema_version": "privacy-report-v1",
    }


def test_approved_privacy_report_cannot_contain_findings() -> None:
    with pytest.raises(ValueError, match="approved privacy reports"):
        PrivacyReport(
            PrivacyDecision.APPROVED,
            (PrivacyFinding("risk", "high", "risk found", {}),),
            "strict",
        )


def test_privacy_report_findings_are_immutable() -> None:
    findings = [PrivacyFinding("risk", "medium", "review", {})]
    report = PrivacyReport(PrivacyDecision.REVIEW_REQUIRED, findings, "strict")
    findings.append(PrivacyFinding("other", "medium", "review", {}))
    assert len(report.findings) == 1


def test_privacy_findings_require_auditable_fields() -> None:
    with pytest.raises(ValueError, match="code, severity, and message"):
        PrivacyFinding("", "high", "risk", {})
    with pytest.raises(ValueError, match="severity is unsupported"):
        PrivacyFinding("risk", "unknown", "risk", {})


def test_privacy_finding_evidence_is_deeply_immutable() -> None:
    report = assess_privacy(
        {"users": (({"email": "a@example.test"}),)},
        direct_identifier_columns=(("users", "email"),),
    )
    with pytest.raises(TypeError, match="immutable"):
        report.findings[0].evidence["table"] = "changed"


def test_privacy_finding_serialization_redacts_raw_evidence() -> None:
    report = assess_privacy(
        {"users": (({"email": "secret@example.test"}),)},
        direct_identifier_columns=(("users", "email"),),
    )
    assert "secret@example.test" not in str(report.findings[0].to_dict())


def test_reference_matches_are_rejected_without_raw_values() -> None:
    report = assess_privacy(
        {"t": (({"id": 1}),)},
        reference_tables={"t": (({"id": 1}),)},
    )
    assert report.decision is PrivacyDecision.REJECTED
    finding = report.findings[0]
    assert finding.code == "memorization_match_risk"
    assert "1" in str(finding.to_dict())


def test_approved_vocabulary_rejects_unknown_values_without_raw_values() -> None:
    report = assess_privacy(
        {"t": (({"status": "secret"}),)},
        approved_vocabularies={("t", "status"): ("approved", "rejected")},
    )
    assert report.decision is PrivacyDecision.REJECTED
    assert report.findings[0].code == "unapproved_vocabulary_value"
    assert "secret" not in str(report.findings[0].to_dict())
