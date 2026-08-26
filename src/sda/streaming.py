"""Deterministic bounded streaming workload contracts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any


class StreamMode(StrEnum):
    BOUNDED = "bounded"
    ACCELERATED = "accelerated"
    CONTINUOUS = "continuous"


class StreamError(ValueError):
    """Raised for incompatible stream plans or unsafe bounds."""


class _FrozenDict(dict[str, Any]):
    def _immutable(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("stream events are immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _immutable  # type: ignore[assignment]


@dataclass(frozen=True, slots=True)
class StreamingPlan:
    stream_id: str
    plan_fingerprint: str
    mode: StreamMode = StreamMode.BOUNDED
    event_count: int = 0
    events_per_second: float = 1.0
    start_time: str = "2020-01-01T00:00:00+00:00"
    schema_version: str = "1"
    checkpoint_id: str = ""
    max_events: int = 100_000
    inter_arrival_seconds: float = 1.0
    seed: int = 1729
    query_id: str = ""
    watermark_delay_seconds: float = 0.0

    def __post_init__(self) -> None:
        if not self.stream_id.strip() or not self.plan_fingerprint.strip():
            raise ValueError("stream_id and plan_fingerprint must not be empty")
        if self.event_count < 0 or self.max_events < 1:
            raise ValueError("event_count must not be negative and max_events must be positive")
        if self.events_per_second <= 0:
            raise ValueError("events_per_second must be positive")
        if self.inter_arrival_seconds <= 0:
            raise ValueError("inter_arrival_seconds must be positive")
        if self.seed < 0:
            raise ValueError("seed must not be negative")
        if self.query_id and not self.query_id.strip():
            raise ValueError("query_id must not be whitespace")
        if self.watermark_delay_seconds < 0:
            raise ValueError("watermark_delay_seconds must not be negative")
        if self.event_count > self.max_events:
            raise ValueError("event_count exceeds max_events")
        if self.mode is StreamMode.CONTINUOUS and not self.checkpoint_id.strip():
            raise ValueError("continuous streams require checkpoint_id")


@dataclass(frozen=True, slots=True)
class StreamManifest:
    stream_id: str
    plan_fingerprint: str
    event_count: int
    first_event_id: str | None
    last_event_id: str | None
    checkpoint_id: str | None
    replay_fingerprint: str
    events_per_second: float
    inter_arrival_seconds: float
    seed: int
    query_id: str | None
    watermark_delay_seconds: float
    manifest_schema_version: str = "stream-manifest-v1"

    def __post_init__(self) -> None:
        if not self.stream_id.strip() or not self.plan_fingerprint.strip():
            raise ValueError("stream manifest identity must not be empty")
        if self.event_count < 0:
            raise ValueError("stream manifest event_count must not be negative")
        if not self.replay_fingerprint.strip():
            raise ValueError("stream manifest replay_fingerprint must not be empty")
        if self.events_per_second <= 0 or self.inter_arrival_seconds <= 0:
            raise ValueError("stream manifest rate and inter-arrival must be positive")
        if self.seed < 0:
            raise ValueError("stream manifest seed must not be negative")
        if (self.event_count == 0) != (self.first_event_id is None and self.last_event_id is None):
            raise ValueError("empty stream manifests must not contain event IDs")
        if self.watermark_delay_seconds < 0:
            raise ValueError("watermark_delay_seconds must not be negative")
        if not self.manifest_schema_version.strip():
            raise ValueError("manifest_schema_version must not be empty")

    def to_dict(self) -> dict[str, Any]:
        return {
            "stream_id": self.stream_id,
            "plan_fingerprint": self.plan_fingerprint,
            "event_count": self.event_count,
            "first_event_id": self.first_event_id,
            "last_event_id": self.last_event_id,
            "checkpoint_id": self.checkpoint_id,
            "replay_fingerprint": self.replay_fingerprint,
            "events_per_second": self.events_per_second,
            "inter_arrival_seconds": self.inter_arrival_seconds,
            "seed": self.seed,
            "query_id": self.query_id,
            "watermark_delay_seconds": self.watermark_delay_seconds,
            "manifest_schema_version": self.manifest_schema_version,
        }


@dataclass(frozen=True, slots=True)
class StreamCheckpoint:
    """Portable checkpoint for deterministic bounded replay."""

    stream_id: str
    plan_fingerprint: str
    schema_version: str
    query_id: str
    next_offset: int
    replay_fingerprint: str

    def __post_init__(self) -> None:
        if any(
            not value.strip()
            for value in (
                self.stream_id,
                self.plan_fingerprint,
                self.schema_version,
                self.replay_fingerprint,
            )
        ):
            raise ValueError("checkpoint identity and replay fingerprint are required")
        if self.next_offset < 0:
            raise ValueError("checkpoint next_offset must not be negative")

    def to_dict(self) -> dict[str, Any]:
        return {
            "stream_id": self.stream_id,
            "plan_fingerprint": self.plan_fingerprint,
            "schema_version": self.schema_version,
            "query_id": self.query_id,
            "next_offset": self.next_offset,
            "replay_fingerprint": self.replay_fingerprint,
        }


def checkpoint(plan: StreamingPlan, events: tuple[dict[str, Any], ...]) -> StreamCheckpoint:
    """Create a checkpoint that can only be resumed by the same stream plan."""
    if any(event.get("stream_id") != plan.stream_id for event in events):
        raise StreamError("checkpoint events belong to a different stream")
    try:
        offsets = [int(event["offset"]) for event in events]
    except (KeyError, TypeError, ValueError) as exc:
        raise StreamError("checkpoint events require integer offsets") from exc
    if any(offset < 0 or offset >= plan.event_count for offset in offsets):
        raise StreamError("checkpoint event offset is outside the stream range")
    if offsets and offsets[0] != 0:
        raise StreamError("checkpoint events must start at offset zero")
    if offsets and offsets != list(range(offsets[0], offsets[-1] + 1)):
        raise StreamError("checkpoint events must contain contiguous offsets")
    next_offset = offsets[-1] + 1 if offsets else 0
    return StreamCheckpoint(
        plan.stream_id,
        plan.plan_fingerprint,
        plan.schema_version,
        plan.query_id,
        next_offset,
        manifest(plan, events).replay_fingerprint,
    )


def resume_from_checkpoint(
    plan: StreamingPlan, saved: StreamCheckpoint
) -> tuple[dict[str, Any], ...]:
    """Resume a bounded stream after validating checkpoint ownership."""
    if (saved.stream_id, saved.plan_fingerprint, saved.schema_version, saved.query_id) != (
        plan.stream_id,
        plan.plan_fingerprint,
        plan.schema_version,
        plan.query_id,
    ):
        raise StreamError("checkpoint is incompatible with the stream plan")
    if saved.next_offset < 0 or saved.next_offset > plan.event_count:
        raise StreamError("checkpoint offset is outside the stream range")
    prefix = generate_bounded_events(plan, start_offset=0)[: saved.next_offset]
    expected_replay = manifest(plan, prefix).replay_fingerprint
    if saved.replay_fingerprint != expected_replay:
        raise StreamError("checkpoint replay fingerprint does not match the stream")
    return generate_bounded_events(plan, start_offset=saved.next_offset)


def deduplicate_events(events: tuple[dict[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    """Keep first-seen event IDs in stable order for retry-safe sinks."""
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for event in events:
        event_id = str(event.get("event_id", ""))
        if not event_id:
            raise StreamError("events require a non-empty event_id")
        if event_id not in seen:
            seen.add(event_id)
            result.append(event)
    return tuple(result)


def generate_bounded_events(
    plan: StreamingPlan, *, start_offset: int = 0
) -> tuple[dict[str, Any], ...]:
    """Generate a deterministic bounded event slice, suitable for replay tests."""
    if start_offset < 0 or start_offset > plan.event_count:
        raise StreamError("start_offset must be within the event range")
    if plan.mode is StreamMode.CONTINUOUS:
        raise StreamError("continuous mode requires a Structured Streaming adapter")
    events = tuple(_event(plan, offset) for offset in range(start_offset, plan.event_count))
    return tuple(_FrozenDict(event) for event in events)


def manifest(plan: StreamingPlan, events: tuple[dict[str, Any], ...]) -> StreamManifest:
    if any(
        event.get("stream_id") != plan.stream_id
        or event.get("schema_version") != plan.schema_version
        for event in events
    ):
        raise StreamError("manifest events do not belong to the stream plan")
    try:
        offsets = [int(event["offset"]) for event in events]
    except (KeyError, TypeError, ValueError) as exc:
        raise StreamError("manifest events require integer offsets") from exc
    if any(offset < 0 or offset >= plan.event_count for offset in offsets):
        raise StreamError("manifest event offset is outside the stream range")
    if offsets and offsets != list(range(offsets[0], offsets[-1] + 1)):
        raise StreamError("manifest events must contain contiguous offsets")
    try:
        ids = [str(event["event_id"]) for event in events]
    except (KeyError, TypeError, ValueError) as exc:
        raise StreamError("manifest events require event IDs") from exc
    if any(not event_id for event_id in ids) or len(ids) != len(set(ids)):
        raise StreamError("manifest events must have unique non-empty event IDs")
    replay = hashlib.sha256("|".join(ids).encode()).hexdigest()
    return StreamManifest(
        plan.stream_id,
        plan.plan_fingerprint,
        len(events),
        ids[0] if ids else None,
        ids[-1] if ids else None,
        plan.checkpoint_id or None,
        replay,
        plan.events_per_second,
        plan.inter_arrival_seconds,
        plan.seed,
        plan.query_id or None,
        plan.watermark_delay_seconds,
    )


def _event(plan: StreamingPlan, offset: int) -> dict[str, Any]:
    event_id = hashlib.sha256(
        f"{plan.plan_fingerprint}|{plan.stream_id}|{plan.seed}|{offset}".encode()
    ).hexdigest()[:32]
    start = datetime.fromisoformat(plan.start_time.replace("Z", "+00:00"))
    event_time = (start + timedelta(seconds=offset * plan.inter_arrival_seconds)).isoformat()
    arrival_time = event_time
    processing_time = (
        start
        + timedelta(seconds=offset * plan.inter_arrival_seconds + plan.watermark_delay_seconds)
    ).isoformat()
    return {
        "event_id": event_id,
        "stream_id": plan.stream_id,
        "offset": offset,
        "event_time": event_time,
        "arrival_time": arrival_time,
        "processing_time": processing_time,
        "watermark_time": processing_time,
        "schema_version": plan.schema_version,
    }
