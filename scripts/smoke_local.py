"""Run the bounded local authorized/denied release smoke scenarios."""

from __future__ import annotations

from sda.pipeline import run_standalone
from sda.planning import ColumnGenerationSpec, GenerationPlan, PlanStatus


def _plan() -> GenerationPlan:
    return (
        GenerationPlan(
            plan_id="release-smoke",
            plan_version=1,
            request_id="release-smoke-run",
            source_snapshot_ids=("controlled-fixture",),
            input_artifact_ids=("controlled-profile",),
            target_catalog="sda_smoke",
            target_schema="synthetic",
            tables=("items",),
            columns=(
                ColumnGenerationSpec("items", "id", "string", nullable=False, model="identifier"),
            ),
            intended_use="qa",
            budgets={"max_rows": 2},
        )
        .transition(PlanStatus.AWAITING_APPROVAL)
        .transition(PlanStatus.APPROVED)
    )


def main() -> int:
    plan = _plan()
    authorized = run_standalone(
        plan,
        row_count=2,
        dataset_id="release-smoke",
        dataset_version="v1",
        location="sda_smoke.synthetic.items",
        actor="release-reviewer",
        unique_key="id",
    )
    if authorized.publication is None or authorized.publication.status.value != "published":
        raise RuntimeError("authorized smoke scenario did not publish")
    denied = run_standalone(
        plan,
        row_count=2,
        dataset_id="release-smoke-denied",
        dataset_version="v1",
        location="sda_smoke.synthetic.items_denied",
        direct_identifier_columns=(("items", "id"),),
    )
    if denied.privacy.decision.value != "rejected":
        raise RuntimeError("denied smoke scenario was not rejected")
    print("Local release smoke: authorized publish and denied identifier scenarios passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
