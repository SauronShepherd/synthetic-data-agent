from __future__ import annotations

import pytest

from sda.security import AuthorizationError, SecurityPolicy


def policy() -> SecurityPolicy:
    return SecurityPolicy(("main.sales",), ("synthetic.qa",))


def test_security_policy_allows_scoped_approved_operation() -> None:
    policy().authorize(
        operation="generate",
        source="main.sales.orders",
        output="synthetic.qa.orders",
        approved=True,
    )


def test_security_policy_denies_scope_and_missing_approval() -> None:
    with pytest.raises(AuthorizationError, match="source"):
        policy().authorize(operation="profile", source="main.finance.secrets")
    with pytest.raises(AuthorizationError, match="approval"):
        policy().authorize(operation="publish", output="synthetic.qa.orders")


def test_security_policy_denies_unknown_operations() -> None:
    with pytest.raises(AuthorizationError, match="operation"):
        policy().authorize(operation="delete_everything")
