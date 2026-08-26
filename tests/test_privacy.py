from __future__ import annotations

from sda.privacy import PrivacyDecision, assess_privacy


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
