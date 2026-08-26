"""Databricks Spark entrypoint for bounded standalone generation."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from sda.generation import generate_rows, manifest_for, receipt_for
from sda.planning import ColumnGenerationSpec, GenerationMode, GenerationPlan, PlanStatus
from sda.runtime.identifiers import QualifiedName


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-table", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--plan-id", required=True)
    parser.add_argument("--plan-fingerprint", required=True)
    parser.add_argument("--source-snapshot-ids", required=True)
    parser.add_argument("--input-artifact-ids", required=True)
    parser.add_argument("--target-catalog", required=True)
    parser.add_argument("--target-schema", required=True)
    parser.add_argument("--tables", required=True)
    parser.add_argument("--columns-json", required=True)
    parser.add_argument("--row-count", type=int, required=True)
    parser.add_argument("--max-rows", type=int, required=True)
    parser.add_argument("--intended-use", required=True)
    parser.add_argument("--privacy-policy-ref", default="strict-default")
    parser.add_argument("--seed", type=int, default=1729)
    parser.add_argument("--approved", default="false")
    parser.add_argument("--manifest-path")
    return parser.parse_args()


def write_manifest(path: str, payload: dict[str, Any]) -> None:
    """Atomically publish a JSON manifest without exposing a partial file."""
    destination = Path(path)
    if not destination.name:
        raise ValueError("manifest path must name a file")
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)
        raise


def main() -> None:
    from pyspark.sql import SparkSession

    args = parse_args()
    if args.approved.lower() != "true":
        raise SystemExit("generation requires approved=true")
    if args.row_count < 0 or args.max_rows < 1 or args.row_count > args.max_rows:
        raise SystemExit("row count exceeds generation bounds")
    try:
        raw_columns = json.loads(args.columns_json)
        columns = tuple(ColumnGenerationSpec(**item) for item in raw_columns)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit("columns-json must be a JSON list of valid column specifications") from exc
    plan = GenerationPlan(
        plan_id=args.plan_id,
        plan_version=1,
        request_id=args.request_id,
        source_snapshot_ids=tuple(filter(None, args.source_snapshot_ids.split(","))),
        input_artifact_ids=tuple(filter(None, args.input_artifact_ids.split(","))),
        target_catalog=args.target_catalog,
        target_schema=args.target_schema,
        tables=tuple(filter(None, args.tables.split(","))),
        columns=columns,
        mode=GenerationMode.CLEAN,
        seed=args.seed,
        intended_use=args.intended_use,
        privacy_policy_ref=args.privacy_policy_ref,
        budgets={"max_rows": args.max_rows},
        status=PlanStatus.APPROVED,
        plan_fingerprint=args.plan_fingerprint,
    )
    rows = generate_rows(plan, row_count=args.row_count)
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    output = QualifiedName.parse(args.output_table)
    frame = spark.createDataFrame([{"run_id": args.run_id, **row} for row in rows])
    frame.write.mode("overwrite").option(
        "userMetadata", f"sda-run-id={args.run_id};plan={plan.plan_fingerprint}"
    ).saveAsTable(output.quoted)
    receipt = receipt_for(plan, rows)
    generation_manifest = manifest_for(
        plan,
        receipt,
        run_id=args.run_id,
        output_table=args.output_table,
    )
    result = {
        "status": "complete",
        "receipt": receipt.to_dict(),
        "manifest": generation_manifest.to_dict(),
    }
    if args.manifest_path:
        write_manifest(args.manifest_path, result)
    print(json.dumps(result, sort_keys=True))
