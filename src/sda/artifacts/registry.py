"""Central artifact registry abstractions used by SDA 07 and later stages."""

from __future__ import annotations

from typing import Protocol

from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType
from sda.runtime.errors import ArtifactCompatibilityError, ArtifactNotFoundError


class ArtifactRegistryStore(Protocol):
    def put(self, ref: ArtifactRef) -> None: ...
    def get(self, artifact_id: str) -> ArtifactRef | None: ...
    def require_complete(self, artifact_id: str) -> ArtifactRef: ...
    def find_reusable(
        self, *, artifact_type: ArtifactType, reuse_fingerprint: str, environment: str
    ) -> ArtifactRef | None: ...


class InMemoryArtifactRegistry:
    """Deterministic local registry for unit tests and offline orchestration."""

    def __init__(self) -> None:
        self._refs: dict[str, ArtifactRef] = {}

    def put(self, ref: ArtifactRef) -> None:
        self._refs[ref.artifact_id] = ref

    def get(self, artifact_id: str) -> ArtifactRef | None:
        return self._refs.get(artifact_id)

    def require_complete(self, artifact_id: str) -> ArtifactRef:
        ref = self.get(artifact_id)
        if ref is None:
            raise ArtifactNotFoundError(
                "artifact was not found", details={"artifact_id": artifact_id}
            )
        if ref.status is not ArtifactStatus.COMPLETE:
            raise ArtifactCompatibilityError(
                "artifact is not complete", details={"artifact_id": artifact_id}
            )
        return ref

    def require_latest_complete(self, artifact_id: str) -> ArtifactRef:
        """Read the newest complete legacy row when historical duplicates exist."""
        from pyspark.sql import functions as F

        rows = (
            self.spark.table(self.table)
            .where(
                (F.col("artifact_id") == F.lit(artifact_id))
                & (F.col("status") == F.lit(ArtifactStatus.COMPLETE.value))
            )
            .orderBy(F.col("completed_at").desc_nulls_last(), F.col("created_at").desc())
            .limit(1)
            .collect()
        )
        if not rows:
            raise ArtifactNotFoundError(
                "artifact was not found", details={"artifact_id": artifact_id}
            )
        return _artifact_ref_from_row(rows[0])

    def find_reusable(
        self, *, artifact_type: ArtifactType, reuse_fingerprint: str, environment: str
    ) -> ArtifactRef | None:
        matches = [
            ref
            for ref in self._refs.values()
            if ref.artifact_type is artifact_type
            and ref.status is ArtifactStatus.COMPLETE
            and ref.reuse_fingerprint == reuse_fingerprint
            and ref.environment == environment
        ]
        if len(matches) > 1:
            raise ArtifactCompatibilityError(
                "duplicate reusable artifacts",
                details={"reason_code": "artifact_registry_duplicate_id"},
            )
        return matches[0] if matches else None


class SparkArtifactRegistry:
    """Spark-backed registry with exact filters and bounded uniqueness checks."""

    def __init__(self, spark: object, table: str) -> None:
        if not table or table.count(".") != 2:
            raise ValueError("registry table must be a catalog.schema.table FQN")
        self.spark = spark
        self.table = table

    def put(self, ref: ArtifactRef) -> None:
        from sda.artifacts.delta import persist_artifact_registry

        persist_artifact_registry(self.spark, ref, self.table)

    def _rows(self, condition: object, limit: int = 2) -> list[object]:
        frame = self.spark.table(self.table).where(condition).limit(limit)
        return list(frame.collect())

    def get(self, artifact_id: str) -> ArtifactRef | None:
        from pyspark.sql import functions as F

        rows = self._rows(F.col("artifact_id") == F.lit(artifact_id))
        if len(rows) > 1:
            raise ArtifactCompatibilityError(
                "duplicate artifact registry ID",
                details={"reason_code": "artifact_registry_duplicate_id"},
            )
        if not rows:
            return None
        return _artifact_ref_from_row(rows[0])

    def require_complete(self, artifact_id: str) -> ArtifactRef:
        ref = self.get(artifact_id)
        if ref is None:
            raise ArtifactNotFoundError(
                "artifact was not found", details={"artifact_id": artifact_id}
            )
        if ref.status is not ArtifactStatus.COMPLETE:
            raise ArtifactCompatibilityError(
                "artifact is not complete", details={"artifact_id": artifact_id}
            )
        return ref

    def require_latest_complete(self, artifact_id: str) -> ArtifactRef:
        from pyspark.sql import functions as F

        rows = (
            self.spark.table(self.table)
            .where(
                (F.col("artifact_id") == F.lit(artifact_id))
                & (F.col("status") == F.lit(ArtifactStatus.COMPLETE.value))
            )
            .orderBy(F.col("completed_at").desc_nulls_last(), F.col("created_at").desc())
            .limit(1)
            .collect()
        )
        if not rows:
            raise ArtifactNotFoundError(
                "artifact was not found", details={"artifact_id": artifact_id}
            )
        return _artifact_ref_from_row(rows[0])

    def find_reusable(
        self, *, artifact_type: ArtifactType, reuse_fingerprint: str, environment: str
    ) -> ArtifactRef | None:
        from pyspark.sql import functions as F

        rows = self._rows(
            (F.col("artifact_type") == F.lit(artifact_type.value))
            & (F.col("reuse_fingerprint") == F.lit(reuse_fingerprint))
            & (F.col("environment") == F.lit(environment))
            & (F.col("status") == F.lit(ArtifactStatus.COMPLETE.value))
        )
        if len(rows) > 1:
            raise ArtifactCompatibilityError(
                "duplicate reusable registry artifacts",
                details={"reason_code": "artifact_registry_duplicate_id"},
            )
        return _artifact_ref_from_row(rows[0]) if rows else None


def _artifact_ref_from_row(row: object) -> ArtifactRef:
    from sda.artifacts.loaders import _ref_from_mapping

    mapping = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
    # Registry v2 has no legacy checksum column; content checksum is the effective checksum.
    mapping.setdefault("checksum", mapping.get("content_checksum") or "unverified")
    return _ref_from_mapping(mapping)


def artifact_ref_from_registry_row(row: object) -> ArtifactRef:
    """Public v2/legacy registry reader with an explicit compatibility warning."""
    mapping = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
    legacy = (
        not mapping.get("registry_schema_version")
        or str(mapping.get("registry_schema_version")) == "1"
    )
    ref = _artifact_ref_from_row(row)
    if legacy and "legacy_artifact_registry_v1_missing_lineage_fields" not in ref.warnings:
        from dataclasses import replace

        ref = replace(
            ref, warnings=(*ref.warnings, "legacy_artifact_registry_v1_missing_lineage_fields")
        )
    return ref


def artifact_ref_to_registry_row(ref: ArtifactRef) -> dict[str, object]:
    """Serialize every rehydration-critical field deterministically."""
    import json
    from dataclasses import asdict

    return {
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
        "related_locations_json": json.dumps(ref.related_locations, sort_keys=True),
        "source_references_json": json.dumps(
            [asdict(item) for item in ref.source_references], sort_keys=True
        ),
        "input_artifact_ids_json": json.dumps(sorted(set(ref.input_artifact_ids))),
        "warnings_json": json.dumps(sorted(set(ref.warnings))),
        "summary": ref.summary,
        "error_code": ref.error_code,
        "error_message_safe": ref.error_message_safe,
        "registry_schema_version": "2",
    }
