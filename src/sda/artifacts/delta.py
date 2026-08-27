"""Small Spark adapter for governed artifact lifecycle persistence."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from sda.artifacts.fingerprint import fingerprint
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
        values = [
            {
                **row,
                "artifact_id": artifact_id,
                "status": status,
                "evidence_id": fingerprint({"index": index, "row": row}),
            }
            for index, row in enumerate(rows)
        ]
        values = [
            {
                key: json.dumps(value, default=str) if isinstance(value, list | dict) else value
                for key, value in row.items()
            }
            for row in values
        ]
        try:
            from pyspark.sql.types import StringType, StructField, StructType

            keys = tuple(values[0]) if values else ("artifact_id", "status")
            schema = StructType([StructField(key, StringType(), True) for key in keys])
            frame = spark.createDataFrame(values, schema=schema)
        except (ModuleNotFoundError, TypeError):
            frame = spark.createDataFrame(values)
        try:
            from delta.tables import DeltaTable

            frame.createOrReplaceTempView("__sda_artifact_write")
            target = DeltaTable.forName(spark, location)
            target_columns = {column.lower() for column in target.toDF().columns}
            merge_condition = (
                "target.artifact_id = source.artifact_id AND target.status = source.status"
            )
            if "evidence_id" in target_columns:
                merge_condition += " AND target.evidence_id = source.evidence_id"
            insert_values = {
                column: f"source.{column}"
                for column in frame.columns
                if column.lower() in target_columns
            }
            (
                target.alias("target")
                .merge(
                    frame.alias("source"),
                    merge_condition,
                )
                .whenNotMatchedInsert(values=insert_values)
                .execute()
            )
        except (ImportError, ModuleNotFoundError):
            frame.write.format("delta").mode("append").saveAsTable(location)
        except Exception as exc:
            if not _is_missing_table_error(exc):
                raise
            frame.write.format("delta").mode("append").saveAsTable(location)
    except (ImportError, ModuleNotFoundError):
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
        "error_message_safe": ref.error_message_safe,
        "related_locations_json": json.dumps(ref.related_locations, sort_keys=True),
        "source_references_json": json.dumps(
            [asdict(source) for source in ref.source_references], sort_keys=True, default=str
        ),
        "warnings_json": json.dumps(ref.warnings),
        "input_artifact_ids_json": json.dumps(sorted(ref.input_artifact_ids)),
        "content_json": json.dumps(ref.content, sort_keys=True, default=str),
    }
    try:
        try:
            frame = spark.createDataFrame([row])
        except Exception:
            from pyspark.sql.types import StringType, StructField, StructType

            schema = StructType([StructField(key, StringType(), True) for key in row])
            frame = spark.createDataFrame(
                [
                    {
                        key: json.dumps(value, default=str) if isinstance(value, list) else value
                        for key, value in row.items()
                    }
                ],
                schema=schema,
            )
        _merge_or_append(
            frame,
            spark,
            location,
            "target.artifact_id = source.artifact_id AND target.status = source.status",
        )
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

    The registry is the lifecycle header; the evidence table contains only
    COMPLETE detail rows. Readers must select the unique COMPLETE registry
    entry before loading details.
    """
    if ref.status is not ArtifactStatus.WRITING:
        raise ValueError("lifecycle publication must start with a writing artifact")
    persist_artifact_registry(spark, ref, registry_location)
    try:
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
    except Exception as exc:
        failed = ref.transition(ArtifactStatus.FAILED)
        persist_artifact_registry(spark, failed, registry_location)
        raise PersistenceError(
            "failed to persist artifact lifecycle",
            details={"artifact_id": ref.artifact_id, "error_code": "artifact_write_failed"},
        ) from exc


def persist_distributed_evidence(
    spark: Any, frame: Any, location: str, *, analysis_id: str
) -> None:
    """Persist a Spark DataFrame without collecting distributed evidence to the driver."""
    if not location or "." not in location:
        raise ValueError("location must be a qualified table name")
    try:
        frame.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            location
        )
    except Exception as exc:
        raise PersistenceError(
            "failed to persist distributed pattern evidence",
            details={"location": location, "analysis_id": analysis_id},
        ) from exc


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
            key: json.dumps(value, default=str) if isinstance(value, list | dict) else value
            for key, value in row.items()
        }
        try:
            from pyspark.sql.types import StringType, StructField, StructType

            schema = StructType([StructField(key, StringType(), True) for key in row])
            frame = spark.createDataFrame([values], schema=schema)
        except (ModuleNotFoundError, TypeError):
            frame = spark.createDataFrame([values])
        _merge_or_append(frame, spark, location, "target.manifest_id = source.manifest_id")
    except Exception as exc:
        raise PersistenceError(
            "failed to persist run manifest",
            details={"location": location, "run_id": manifest.run_id},
        ) from exc


def _merge_or_append(frame: Any, spark: Any, location: str, condition: str) -> None:
    """Merge deterministic headers when Delta APIs exist; create/append otherwise."""
    try:
        from delta.tables import DeltaTable

        DeltaTable.forName(spark, location).alias("target").merge(
            frame.alias("source"), condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except (ImportError, ModuleNotFoundError):
        frame.write.format("delta").mode("append").saveAsTable(location)
    except Exception as exc:
        if not _is_missing_table_error(exc):
            raise
        frame.write.format("delta").mode("append").saveAsTable(location)


def _is_missing_table_error(exc: Exception) -> bool:
    message = str(exc).upper()
    return any(
        marker in message
        for marker in ("TABLE_OR_VIEW_NOT_FOUND", "TABLE_NOT_FOUND", "NOT A DELTA TABLE")
    )
