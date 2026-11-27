from __future__ import annotations

import pytest

from sda.gateway import GatewayPolicy
from sda.operations import ResourceBudget
from sda.orchestration import (
    ActionPolicy,
    ContractDenied,
    DeterministicGateway,
    GatewayRoute,
    GenieSemanticContract,
    OperationGuard,
    evaluate_release,
    safe_snapshot,
)


def test_genie_contract_allows_approved_view_and_denies_source() -> None:
    contract = GenieSemanticContract(("approved.view",), ("known_question",))
    contract.authorize(
        view="approved.view", question_id="known_question", evidence_status="APPROVED"
    )
    with pytest.raises(ContractDenied, match="approved semantic"):
        contract.authorize(
            view="source.customers", question_id="known_question", evidence_status="APPROVED"
        )


def test_action_and_safety_contracts_fail_closed() -> None:
    with pytest.raises(ContractDenied, match="approval"):
        ActionPolicy().authorize(action="revoke", approved=False, target="run")
    guard = OperationGuard(allowed_operations=("retry",), budget=ResourceBudget(max_rows=1))
    with pytest.raises(ContractDenied, match="not allowed"):
        guard.authorize(run_id="run", operation="publish", target="run")


def test_gateway_selection_and_release_receipt_are_deterministic() -> None:
    gateway = DeterministicGateway(
        GatewayPolicy(("model-a",), ("genie",), max_estimated_cost=1.0),
        (GatewayRoute("genie", "model-a"),),
    )
    assert gateway.route(request_id="r", service="genie").selected_model == "model-a"
    receipt = evaluate_release(
        "candidate", gates={"tests": True}, required_gates=("tests", "approval")
    )
    assert receipt.status == "FAIL"
    assert receipt.failed_gates == ("approval",)


def test_safe_snapshot_rejects_raw_fields() -> None:
    with pytest.raises(ContractDenied, match="raw"):
        safe_snapshot({"raw_rows": [{"id": 1}]})
