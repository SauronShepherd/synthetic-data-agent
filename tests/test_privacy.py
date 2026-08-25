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
