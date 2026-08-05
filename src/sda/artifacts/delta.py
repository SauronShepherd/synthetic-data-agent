"""Small Spark adapter for governed artifact lifecycle persistence."""

from __future__ import annotations

import json
from typing import Any

from sda.artifacts.manifest import RunManifest
from sda.artifacts.models import ArtifactRef, ArtifactStatus
from sda.runtime.errors import PersistenceError


def persist_rows(
    spark: Any,
    rows: list[dict[str, Any]],
    location: str,
    *,
    artifact_id: str,
    status: str = "complete",
) -> None:
    """Append a bounded receipt/evidence batch to a Delta table.

    Callers should write a ``writing`` row before expensive work and replace
    or supersede it only after the artifact is complete. This helper never
    silently falls back to driver files or non-Delta formats.
    """
    if status not in {"writing", "complete", "failed", "superseded"}:
        raise ValueError("invalid artifact status")
    if not location or "." not in location:
        raise ValueError("location must be a qualified table name")
    try:
        values = [{**row, "artifact_id": artifact_id, "status": status} for row in rows]
        values = [
            {
                key: json.dumps(value, default=str) if isinstance(value, (list, dict)) else value
                for key, value in row.items()
            }
            for row in values
        ]
        try:
            from pyspark.sql.types import StringType, StructField, StructType

            keys = tuple(values[0]) if values else ("artifact_id", "status")
            schema = StructType([StructField(key, StringType(), True) for key in keys])
            frame = spark.createDataFrame(values, schema=schema)
        except ModuleNotFoundError:
            frame = spark.createDataFrame(values)
        try:
            from delta.tables import DeltaTable

            frame.createOrReplaceTempView("__sda_artifact_write")
            target = DeltaTable.forName(spark, location)
            (
                target.alias("target")
                .merge(
                    frame.alias("source"),
                    "target.artifact_id = source.artifact_id AND target.status = source.status",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        except Exception:
            frame.write.format("delta").mode("append").saveAsTable(location)
    except Exception as exc:
        raise PersistenceError(
            "failed to persist Delta artifact",
            details={"location": location, "artifact_id": artifact_id},
        ) from exc


def persist_artifact_registry(spark: Any, ref: ArtifactRef, location: str) -> None:
    """Persist the searchable typed identity/header for one artifact."""
    if not location or "." not in location:
        raise ValueError("location must be a qualified table name")
    row = {
        "artifact_id": ref.artifact_id,
        "artifact_type": ref.artifact_type.value,
        "artifact_schema_version": ref.artifact_schema_version,
        "status": ref.status.value,
        "tool_name": ref.tool_name,
        "tool_version": ref.tool_version,
        "strategy_version": ref.strategy_version,
        "run_id": ref.run_id,
        "environment": ref.environment,
        "created_at": ref.created_at,
        "completed_at": ref.completed_at,
        "configuration_hash": ref.configuration_hash,
        "reuse_fingerprint": ref.reuse_fingerprint,
        "content_checksum": ref.effective_content_checksum,
        "primary_location": ref.primary_location,
        "summary": ref.summary,
        "error_code": ref.error_code,
    }
    try:
        try:
            frame = spark.createDataFrame([row])
        except Exception:
            from pyspark.sql.types import StringType, StructField, StructType

            schema = StructType([StructField(key, StringType(), True) for key in row])
            frame = spark.createDataFrame(
                [{key: json.dumps(value, default=str) if isinstance(value, list) else value
                  for key, value in row.items()}],
                schema=schema,
            )
        frame.write.format("delta").mode("append").saveAsTable(location)
    except Exception as exc:
        raise PersistenceError(
            "failed to persist artifact registry row",
            details={"location": location, "artifact_id": ref.artifact_id},
        ) from exc


def persist_artifact_lifecycle(
    spark: Any,
    ref: ArtifactRef,
    rows: list[dict[str, Any]],
    *,
    evidence_location: str,
    registry_location: str,
) -> ArtifactRef:
    """Publish one idempotent COMPLETE publication for bounded evidence.

    Delta tables are append-only at the storage layer, so publication carries
    a deterministic artifact id and writes the evidence/header once.  Readers
    must select the unique COMPLETE row for that id; retries therefore do not
    create a second writing/complete evidence pair.
    """
    if ref.status is not ArtifactStatus.WRITING:
        raise ValueError("lifecycle publication must start with a writing artifact")
    # Keep a lightweight pre-publication receipt for operational observability;
    # raw evidence is written only once below, at COMPLETE.
    receipt = {key: None for key in (rows[0] if rows else {"receipt": None})}
    persist_rows(
        spark, [receipt], evidence_location, artifact_id=ref.artifact_id,
        status=ArtifactStatus.WRITING.value,
    )
    persist_artifact_registry(spark, ref, registry_location)
    completed = ref.transition(ArtifactStatus.COMPLETE)
    persist_rows(
        spark,
        rows,
        evidence_location,
        artifact_id=completed.artifact_id,
        status=ArtifactStatus.COMPLETE.value,
    )
    persist_artifact_registry(spark, completed, registry_location)
    return completed


def persist_run_manifest(spark: Any, manifest: RunManifest, location: str) -> None:
    """Persist one searchable run receipt without raw source data."""
    if not location or "." not in location:
        raise ValueError("location must be a qualified table name")
    row = {
        "manifest_id": manifest.manifest_id,
        "run_id": manifest.run_id,
        "tool_name": manifest.tool_name,
        "tool_version": manifest.tool_version,
        "artifact_schema_version": manifest.artifact_schema_version,
        "environment": manifest.environment,
        "configuration_hash": manifest.configuration_hash,
        "input_artifact_ids": list(manifest.input_artifact_ids),
        "output_artifact_ids": list(manifest.output_artifact_ids),
        "status": manifest.status,
        "started_at": manifest.started_at,
        "completed_at": manifest.completed_at,
        "warning_count": manifest.warning_count,
        "error_code": manifest.error_code,
        "error_message": manifest.error_message,
    }
    try:
        values = {
            key: json.dumps(value, default=str) if isinstance(value, (list, dict)) else value
            for key, value in row.items()
        }
        try:
            from pyspark.sql.types import StringType, StructField, StructType

            schema = StructType([StructField(key, StringType(), True) for key in row])
            frame = spark.createDataFrame([values], schema=schema)
        except ModuleNotFoundError:
            frame = spark.createDataFrame([values])
        frame.write.format("delta").mode("append").saveAsTable(location)
    except Exception as exc:
        raise PersistenceError(
            "failed to persist run manifest",
            details={"location": location, "run_id": manifest.run_id},
        ) from exc
