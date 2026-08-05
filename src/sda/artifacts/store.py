"""Store protocols and a deterministic in-memory implementation for local tests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from sda.artifacts.models import ArtifactRef, ArtifactStatus


class ArtifactRepository:
    """Minimal repository boundary used by workflow reuse decisions."""

    def get_complete_by_fingerprint(self, reuse_fingerprint: str) -> ArtifactRef | None:
        raise NotImplementedError

    def supersede(self, artifact_id: str) -> ArtifactRef:
        raise NotImplementedError


class InMemoryArtifactStore(ArtifactRepository):
    """Local store matching the durable loader protocol without source scans."""

    def __init__(self) -> None:
        self._refs: dict[str, ArtifactRef] = {}
        self._rows: dict[tuple[str, str], tuple[Mapping[str, Any], ...]] = {}

    def put(self, ref: ArtifactRef, rows: Sequence[Mapping[str, Any]] = ()) -> None:
        self._refs[ref.artifact_id] = ref
        self._rows[(ref.primary_location, ref.artifact_id)] = tuple(rows)

    def get_ref(self, artifact_id: str) -> Mapping[str, Any] | None:
        ref = self._refs.get(artifact_id)
        return ref.to_dict() if ref else None

    def get_rows(self, location: str, artifact_id: str) -> tuple[Mapping[str, Any], ...]:
        return self._rows.get((location, artifact_id), ())

    def get_complete_by_fingerprint(self, reuse_fingerprint: str) -> ArtifactRef | None:
        return next(
            (
                ref
                for ref in self._refs.values()
                if ref.status is ArtifactStatus.COMPLETE
                and ref.reuse_fingerprint == reuse_fingerprint
            ),
            None,
        )

    def supersede(self, artifact_id: str) -> ArtifactRef:
        ref = self._refs[artifact_id]
        updated = ref.transition(ArtifactStatus.SUPERSEDED)
        self._refs[artifact_id] = updated
        return updated
