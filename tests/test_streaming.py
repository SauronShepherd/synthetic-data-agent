from __future__ import annotations

import pytest

from sda.streaming import (
    StreamError,
    StreamingPlan,
    StreamMode,
    checkpoint,
    deduplicate_events,
    generate_bounded_events,
    manifest,
    resume_from_checkpoint,
)


def plan(**kwargs: object) -> StreamingPlan:
    return StreamingPlan("stream-1", "fp-1", event_count=4, **kwargs)


def test_bounded_stream_is_replayable_and_offset_stable() -> None:
    first = generate_bounded_events(plan())
    replay = generate_bounded_events(plan())
    suffix = generate_bounded_events(plan(), start_offset=2)
    assert first == replay
    assert suffix == first[2:]
    assert len({event["event_id"] for event in first}) == 4
    assert manifest(plan(), first).replay_fingerprint == manifest(plan(), replay).replay_fingerprint


def test_continuous_requires_checkpoint_and_local_generator_rejects_it() -> None:
    with pytest.raises(ValueError, match="checkpoint"):
        plan(mode=StreamMode.CONTINUOUS)
    continuous = plan(mode=StreamMode.CONTINUOUS, checkpoint_id="cp-1")
    with pytest.raises(StreamError, match="Structured Streaming"):
        generate_bounded_events(continuous)


def test_offsets_and_bounds_are_fail_closed() -> None:
    with pytest.raises(StreamError, match="within"):
        generate_bounded_events(plan(), start_offset=5)
    with pytest.raises(ValueError, match="max_events"):
        StreamingPlan("s", "f", event_count=2, max_events=1)


def test_checkpoint_resume_is_plan_bound_and_duplicate_safe() -> None:
    current = plan()
    prefix = generate_bounded_events(current, start_offset=0)[:2]
    saved = checkpoint(current, prefix)
    assert resume_from_checkpoint(current, saved) == generate_bounded_events(current)[2:]
    assert len(deduplicate_events(prefix + prefix)) == len(prefix)
    with pytest.raises(StreamError, match="incompatible"):
        resume_from_checkpoint(StreamingPlan("stream-1", "other", event_count=4), saved)


def test_checkpoint_rejects_gaps_and_cross_stream_events() -> None:
    current = plan()
    events = generate_bounded_events(current)
    with pytest.raises(StreamError, match="contiguous"):
        checkpoint(current, (events[0], events[2]))
    with pytest.raises(StreamError, match="different stream"):
        checkpoint(current, ({**events[0], "stream_id": "other"},))


def test_manifest_rejects_schema_mismatch_and_offset_gaps() -> None:
    current = plan()
    events = generate_bounded_events(current)
    with pytest.raises(StreamError, match="stream plan"):
        manifest(current, ({**events[0], "schema_version": "2"},))
    with pytest.raises(StreamError, match="contiguous"):
        manifest(current, (events[0], events[2]))


def test_bounded_events_have_deterministic_inter_arrival_times() -> None:
    current = plan(inter_arrival_seconds=2.5)
    events = generate_bounded_events(current)
    assert events[0]["event_time"] == "2020-01-01T00:00:00+00:00"
    assert events[1]["event_time"] == "2020-01-01T00:00:02.500000+00:00"
    with pytest.raises(ValueError, match="inter_arrival"):
        plan(inter_arrival_seconds=0)
