from __future__ import annotations

import pytest

from sda.operations import (
    AuditEvent,
    AuditLevel,
    AuditLog,
    BudgetExceeded,
    ResourceBudget,
    enforce_budget,
)


def test_budget_fails_before_overrun() -> None:
    enforce_budget(ResourceBudget(max_rows=10), rows=10)
    with pytest.raises(BudgetExceeded, match="rows budget"):
        enforce_budget(ResourceBudget(max_rows=10), rows=11)


def test_audit_log_is_append_only_and_rejects_secret_keys() -> None:
    log = AuditLog()
    event = log.append(
        AuditEvent("run-1", "stage_started", AuditLevel.INFO, "started", metadata={"rows": 1})
    )
    assert log.for_run("run-1") == (event,)
    with pytest.raises(ValueError, match="secret"):
        AuditEvent("run-1", "bad", AuditLevel.ERROR, "bad", metadata={"api_token": "redacted"})
