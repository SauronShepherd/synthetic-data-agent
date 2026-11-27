"""Databricks smoke entrypoint for the governed SDA interface contracts."""

from __future__ import annotations

import argparse
import json

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
from sda.runtime.identifiers import QualifiedName


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--contract",
        required=True,
        choices=("genie", "control_center", "actions", "gateway", "safety", "release"),
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-table", required=True)
    parser.add_argument("--human-approved", default="true")
    return parser.parse_args()


def run_contract(args: argparse.Namespace) -> dict[str, object]:
    if args.contract == "genie":
        contract = GenieSemanticContract(
            ("sda_dev.profiles.approved_summary",),
            ("customers_by_region", "failed_quality_checks"),
        )
        contract.authorize(
            view="sda_dev.profiles.approved_summary",
            question_id="customers_by_region",
            evidence_status="APPROVED",
        )
        denied = False
        try:
            contract.authorize(
                view="sda_dev.source.customers",
                question_id="customers_by_region",
                evidence_status="APPROVED",
            )
        except ContractDenied:
            denied = True
        if not denied:
            raise ContractDenied("Genie smoke did not reject a raw source view")
        return {"contract": "genie", "allowed_query": True, "raw_view_denied": True}
    if args.contract == "control_center":
        snapshot = safe_snapshot(
            {
                "run_id": args.run_id,
                "dataset_state": "VALIDATED",
                "validation_state": "PASS",
                "artifact_count": 3,
                "source_rows_exposed": False,
            }
        )
        return {"contract": "control_center", "safe_fields": sorted(snapshot), "read_only": True}
    if args.contract == "actions":
        policy = ActionPolicy()
        policy.authorize(action="retry", approved=False, target=args.run_id)
        denied = False
        try:
            policy.authorize(action="publish", approved=True, target=args.run_id)
        except ContractDenied:
            denied = True
        if not denied:
            raise ContractDenied("action smoke did not reject an unlisted publish action")
        return {"contract": "actions", "retry_allowed": True, "publish_denied": True}
    if args.contract == "gateway":
        gateway = DeterministicGateway(
            GatewayPolicy(
                ("model-a",),
                ("genie",),
                max_requests_per_run=2,
                max_estimated_cost=1.0,
            ),
            (GatewayRoute("genie", "model-a"),),
        )
        receipt = gateway.route(request_id=args.run_id, service="genie", estimated_cost=0.2)
        return {
            "contract": "gateway",
            "selected_model": receipt.selected_model,
            "fallback": receipt.fallback,
        }
    if args.contract == "safety":
        guard = OperationGuard(
            allowed_operations=("retry", "quarantine", "revoke"),
            budget=ResourceBudget(max_rows=100),
        )
        receipt = guard.authorize(
            run_id=args.run_id,
            operation="retry",
            target=args.run_id,
            rows=40,
        )
        denied = False
        try:
            guard.authorize(run_id=args.run_id, operation="revoke", target=args.run_id)
        except ContractDenied:
            denied = True
        return {
            "contract": "safety",
            "authorized": receipt.status,
            "unapproved_revoke_denied": denied,
        }
    approved = args.human_approved.lower() == "true"
    receipt = evaluate_release(
        args.run_id,
        gates={
            "release_hygiene": True,
            "static_tests": True,
            "bundle_validation": True,
            "staging_acceptance": True,
            "human_approval": approved,
        },
        required_gates=(
            "release_hygiene",
            "static_tests",
            "bundle_validation",
            "staging_acceptance",
            "human_approval",
        ),
    )
    return {
        "contract": "release",
        "status": receipt.status,
        "failed_gates": receipt.failed_gates,
        "candidate_id": receipt.candidate_id,
    }


def main() -> None:
    from pyspark.sql import SparkSession

    args = parse_args()
    output = QualifiedName.parse(args.output_table)
    spark = SparkSession.builder.getOrCreate()
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS `{output.catalog}`.`{output.schema}`"
    )
    payload = run_contract(args)
    row = {
        "run_id": args.run_id,
        "contract": args.contract,
        "status": str(payload.get("status", "PASS")),
        "payload_json": json.dumps(payload, sort_keys=True),
    }
    spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable(output.quoted)
    print(json.dumps(payload, sort_keys=True, default=str))
    if args.contract == "release" and payload["status"] != "PASS":
        raise SystemExit("release gates failed")


if __name__ == "__main__":
    main()
