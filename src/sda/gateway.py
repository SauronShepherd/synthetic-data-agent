"""Policy-first model and tool gateway controls."""

from __future__ import annotations

from dataclasses import dataclass


class GatewayDenied(PermissionError):
    """Raised when a model/tool interaction is outside policy."""


@dataclass(frozen=True, slots=True)
class GatewayPolicy:
    allowed_models: tuple[str, ...]
    allowed_tools: tuple[str, ...]
    max_requests_per_run: int = 100
    max_estimated_cost: float = 0.0

    def __post_init__(self) -> None:
        if not self.allowed_models:
            raise ValueError("gateway requires at least one allowed model")
        if self.max_requests_per_run < 1 or self.max_estimated_cost < 0:
            raise ValueError("gateway limits are invalid")


class GatewayBudget:
    def __init__(self, policy: GatewayPolicy) -> None:
        self.policy = policy
        self.requests = 0
        self.estimated_cost = 0.0

    def authorize(
        self, *, model: str, tool: str | None = None, estimated_cost: float = 0.0
    ) -> None:
        if model not in self.policy.allowed_models:
            raise GatewayDenied(f"model is not allowed: {model}")
        if tool is not None and tool not in self.policy.allowed_tools:
            raise GatewayDenied(f"tool is not allowed: {tool}")
        if estimated_cost < 0:
            raise ValueError("estimated_cost must not be negative")
        if self.requests + 1 > self.policy.max_requests_per_run:
            raise GatewayDenied("gateway request budget exceeded")
        if self.estimated_cost + estimated_cost > self.policy.max_estimated_cost:
            raise GatewayDenied("gateway cost budget exceeded")
        self.requests += 1
        self.estimated_cost += estimated_cost
