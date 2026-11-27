"""Small, explicit contracts for the governed interfaces in SDA 15–20."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from sda.artifacts.fingerprint import fingerprint
from sda.gateway import GatewayBudget, GatewayPolicy
from sda.operations import ResourceBudget, enforce_budget


class ContractDenied(PermissionError):
    """Raised when an interface request crosses its declared boundary."""


@dataclass(frozen=True, slots=True)
class GenieSemanticContract:
    approved_views: tuple[str, ...]
    benchmark_questions: tuple[str, ...]
    prohibited_fragments: tuple[str, ...] = ("source", "truth_ledger", "secret")

    def authorize(
        self,
        *,
        view: str,
        question_id: str,
        evidence_status: str,
        stale: bool = False,
    ) -> None:
        if view not in self.approved_views:
            raise ContractDenied("Genie may query approved semantic views only")
        if any(fragment in view.lower() for fragment in self.prohibited_fragments):
            raise ContractDenied("Genie view is prohibited by the semantic contract")
        if question_id not in self.benchmark_questions:
            raise ContractDenied("question is outside the reviewed benchmark set")
        if evidence_status != "APPROVED" or stale:
            raise ContractDenied("Genie evidence is not current approved evidence")


@dataclass(frozen=True, slots=True)
class ActionPolicy:
    allowed_actions: tuple[str, ...] = ("retry", "quarantine", "revoke")
    approval_required: tuple[str, ...] = ("quarantine", "revoke")

    def authorize(self, *, action: str, approved: bool, target: str) -> None:
        if action not in self.allowed_actions:
            raise ContractDenied(f"action is not allowed: {action}")
        if not target.strip():
            raise ContractDenied("action target is required")
        if action in self.approval_required and not approved:
            raise ContractDenied(f"action requires explicit approval: {action}")


@dataclass(frozen=True, slots=True)
class GatewayRoute:
    service: str
    model: str
    weight: int = 1
    fallback: bool = False

    def __post_init__(self) -> None:
        if not self.service.strip() or not self.model.strip() or self.weight < 1:
            raise ValueError("gateway route identity and positive weight are required")


@dataclass(frozen=True, slots=True)
class GatewayReceipt:
    request_id: str
    service: str
    selected_model: str
    fallback: bool
    policy_fingerprint: str


class DeterministicGateway:
    def __init__(self, policy: GatewayPolicy, routes: tuple[GatewayRoute, ...]) -> None:
        if not routes:
            raise ValueError("gateway requires at least one route")
        if any(route.model not in policy.allowed_models for route in routes):
            raise ValueError("gateway route uses a model outside policy")
        self._policy = policy
        self._routes = routes
        self._budget = GatewayBudget(policy)

    def route(
        self,
        *,
        request_id: str,
        service: str,
        estimated_cost: float = 0.0,
        allow_fallback: bool = False,
    ) -> GatewayReceipt:
        candidates = tuple(route for route in self._routes if route.service == service)
        if not candidates:
            raise ContractDenied("model service has no approved route")
        primary = tuple(route for route in candidates if not route.fallback)
        fallback = tuple(route for route in candidates if route.fallback)
        selected = primary or (fallback if allow_fallback else ())
        if not selected:
            raise ContractDenied("unapproved gateway fallback")
        token = int(hashlib.sha256(request_id.encode()).hexdigest(), 16)
        total = sum(route.weight for route in selected)
        cursor = token % total
        for route in selected:
            if cursor < route.weight:
                self._budget.authorize(
                    model=route.model,
                    tool=service,
                    estimated_cost=estimated_cost,
                )
                return GatewayReceipt(
                    request_id,
                    service,
                    route.model,
                    route.fallback,
                    fingerprint(self._policy),
                )
            cursor -= route.weight
        raise ContractDenied("gateway route selection failed closed")


@dataclass(frozen=True, slots=True)
class OperationReceipt:
    run_id: str
    operation: str
    target: str
    status: str
    policy_fingerprint: str


class OperationGuard:
    def __init__(self, *, allowed_operations: tuple[str, ...], budget: ResourceBudget) -> None:
        if not allowed_operations:
            raise ValueError("operation guard requires an allowlist")
        self._allowed = allowed_operations
        self._budget = budget

    def authorize(
        self,
        *,
        run_id: str,
        operation: str,
        target: str,
        approved: bool = False,
        rows: int = 0,
        cost: float = 0.0,
    ) -> OperationReceipt:
        if operation not in self._allowed:
            raise ContractDenied(f"operation is not allowed: {operation}")
        if operation in {"delete", "publish", "revoke"} and not approved:
            raise ContractDenied(f"operation requires approval: {operation}")
        enforce_budget(self._budget, rows=rows, cost=cost)
        return OperationReceipt(
            run_id,
            operation,
            target,
            "AUTHORIZED",
            fingerprint({"allowed_operations": self._allowed, "budget": self._budget}),
        )


@dataclass(frozen=True, slots=True)
class ReleaseReceipt:
    candidate_id: str
    status: str
    failed_gates: tuple[str, ...]
    receipt_fingerprint: str


def evaluate_release(
    candidate_id: str,
    *,
    gates: dict[str, bool],
    required_gates: tuple[str, ...],
) -> ReleaseReceipt:
    if not candidate_id.strip() or not required_gates:
        raise ValueError("release candidate and required gates are required")
    failed = tuple(gate for gate in required_gates if not gates.get(gate, False))
    status = "PASS" if not failed else "FAIL"
    payload = {"candidate_id": candidate_id, "status": status, "failed_gates": failed}
    return ReleaseReceipt(candidate_id, status, failed, fingerprint(payload))


def safe_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    """Keep control-center facts compact and reject obvious raw-row leakage."""
    if any("raw" in key.lower() or "secret" in key.lower() for key in payload):
        raise ContractDenied("control-center snapshot cannot expose raw or secret fields")
    return dict(payload)
