from __future__ import annotations

import json

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
    assert result.receipt.row_count == 2
    assert result.receipt.plan_fingerprint == approved_plan().plan_fingerprint
    assert result.generation_manifest.receipt == result.receipt
    assert result.generation_manifest.output_table == "uc.t"
    assert result.validation.technical_disposition.value == "PASS"
    assert result.privacy.decision.value == "approved"
    assert result.publication is not None
    assert result.publication.status.value == "published"
    assert result.manifest.status == "complete"
    assert result.manifest.input_artifact_ids == ("a",)
    assert result.manifest.output_fingerprint == result.receipt.output_fingerprint
    assert result.manifest.locations == {"output": "uc.t"}
    assert result.publication.validation_fingerprint != approved_plan().plan_fingerprint


def test_pipeline_can_write_atomic_staging_output(tmp_path) -> None:
    destination = tmp_path / "staging" / "rows.jsonl"
    result = run_standalone(
        approved_plan(),
        row_count=2,
        dataset_id="d",
        dataset_version="v1",
        location="uc.t",
        staging_path=str(destination),
    )
    assert [json.loads(line) for line in destination.read_text().splitlines()] == list(result.rows)


def test_pipeline_blocks_publication_for_unapproved_direct_identifier() -> None:
    result = run_standalone(
        approved_plan(),
        row_count=2,
        dataset_id="dataset",
        dataset_version="v1",
        location="catalog.schema.table",
        actor=None,
        direct_identifier_columns=(("t", "id"),),
    )
    assert result.privacy.decision.value == "rejected"
    assert result.privacy.findings[0].code == "direct_identifier_not_approved"
