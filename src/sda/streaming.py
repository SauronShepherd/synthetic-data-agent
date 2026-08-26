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

    def __post_init__(self) -> None:
        if not self.stream_id.strip() or not self.plan_fingerprint.strip():
            raise ValueError("stream_id and plan_fingerprint must not be empty")
        if self.event_count < 0 or self.max_events < 1:
            raise ValueError("event_count must not be negative and max_events must be positive")
        if self.events_per_second <= 0:
            raise ValueError("events_per_second must be positive")
        if self.inter_arrival_seconds <= 0:
            raise ValueError("inter_arrival_seconds must be positive")
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


@dataclass(frozen=True, slots=True)
class StreamCheckpoint:
    """Portable checkpoint for deterministic bounded replay."""

    stream_id: str
    plan_fingerprint: str
    schema_version: str
    next_offset: int
    replay_fingerprint: str


def checkpoint(plan: StreamingPlan, events: tuple[dict[str, Any], ...]) -> StreamCheckpoint:
    """Create a checkpoint that can only be resumed by the same stream plan."""
    if any(event.get("stream_id") != plan.stream_id for event in events):
        raise StreamError("checkpoint events belong to a different stream")
    offsets = [int(event["offset"]) for event in events]
    if any(offset < 0 or offset >= plan.event_count for offset in offsets):
        raise StreamError("manifest event offset is outside the stream range")
    if offsets and offsets != list(range(offsets[0], offsets[-1] + 1)):
        raise StreamError("checkpoint events must contain contiguous offsets")
    next_offset = offsets[-1] + 1 if offsets else 0
    return StreamCheckpoint(
        plan.stream_id,
        plan.plan_fingerprint,
        plan.schema_version,
        next_offset,
        manifest(plan, events).replay_fingerprint,
    )


def resume_from_checkpoint(
    plan: StreamingPlan, saved: StreamCheckpoint
) -> tuple[dict[str, Any], ...]:
    """Resume a bounded stream after validating checkpoint ownership."""
    if (saved.stream_id, saved.plan_fingerprint, saved.schema_version) != (
        plan.stream_id,
        plan.plan_fingerprint,
        plan.schema_version,
    ):
        raise StreamError("checkpoint is incompatible with the stream plan")
    if saved.next_offset < 0 or saved.next_offset > plan.event_count:
        raise StreamError("checkpoint offset is outside the stream range")
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
    return events


def manifest(plan: StreamingPlan, events: tuple[dict[str, Any], ...]) -> StreamManifest:
    if any(
        event.get("stream_id") != plan.stream_id
        or event.get("schema_version") != plan.schema_version
        for event in events
    ):
        raise StreamError("manifest events do not belong to the stream plan")
    offsets = [int(event["offset"]) for event in events]
    if any(offset < 0 or offset >= plan.event_count for offset in offsets):
        raise StreamError("manifest event offset is outside the stream range")
    if offsets and offsets != list(range(offsets[0], offsets[-1] + 1)):
        raise StreamError("manifest events must contain contiguous offsets")
    ids = [str(event["event_id"]) for event in events]
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
    )


def _event(plan: StreamingPlan, offset: int) -> dict[str, Any]:
    event_id = hashlib.sha256(
        f"{plan.plan_fingerprint}|{plan.stream_id}|{offset}".encode()
    ).hexdigest()[:32]
    start = datetime.fromisoformat(plan.start_time.replace("Z", "+00:00"))
    event_time = (start + timedelta(seconds=offset * plan.inter_arrival_seconds)).isoformat()
    return {
        "event_id": event_id,
        "stream_id": plan.stream_id,
        "offset": offset,
        "event_time": event_time,
        "schema_version": plan.schema_version,
    }
