from __future__ import annotations

import pytest

from sda.gateway import GatewayBudget, GatewayDenied, GatewayPolicy


def test_gateway_allows_scoped_calls_until_budget() -> None:
    budget = GatewayBudget(GatewayPolicy(("model-a",), ("metadata",), max_requests_per_run=1, max_estimated_cost=1.0))
    budget.authorize(model="model-a", tool="metadata", estimated_cost=0.5)
    with pytest.raises(GatewayDenied, match="request budget"):
        budget.authorize(model="model-a", tool="metadata", estimated_cost=0.1)


def test_gateway_denies_model_tool_and_cost() -> None:
    budget = GatewayBudget(GatewayPolicy(("model-a",), ("metadata",), max_estimated_cost=1.0))
    with pytest.raises(GatewayDenied, match="model"):
        budget.authorize(model="model-b")
    with pytest.raises(GatewayDenied, match="tool"):
        budget.authorize(model="model-a", tool="publish")
    with pytest.raises(GatewayDenied, match="cost"):
        budget.authorize(model="model-a", estimated_cost=1.1)
