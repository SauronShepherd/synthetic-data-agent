from __future__ import annotations

from sda.pipeline import run_standalone
from sda.planning import ColumnGenerationSpec, GenerationPlan, PlanStatus


def approved_plan() -> GenerationPlan:
    return (
        GenerationPlan(
            plan_id="p",
            plan_version=1,
            request_id="r",
            source_snapshot_ids=("s",),
            input_artifact_ids=("a",),
            target_catalog="c",
            target_schema="s",
            tables=("t",),
            columns=(
                ColumnGenerationSpec("t", "id", "string", nullable=False, model="identifier"),
            ),
            intended_use="qa",
            budgets={"max_rows": 2},
        )
        .transition(PlanStatus.AWAITING_APPROVAL)
        .transition(PlanStatus.APPROVED)
    )


def test_pipeline_runs_all_gates_and_publishes() -> None:
    result = run_standalone(
        approved_plan(),
        row_count=2,
        dataset_id="d",
        dataset_version="v1",
        location="uc.t",
        actor="reviewer",
        unique_key="id",
    )
    assert len(result.rows) == 2
    assert result.validation.technical_disposition.value == "PASS"
    assert result.privacy.decision.value == "approved"
    assert result.publication is not None
    assert result.publication.status.value == "published"
