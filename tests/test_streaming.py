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
    with pytest.raises(TypeError, match="immutable"):
        first[0]["offset"] = 99  # type: ignore[index]


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
    assert saved.to_dict()["query_id"] == current.query_id
    assert resume_from_checkpoint(current, saved) == generate_bounded_events(current)[2:]
    assert len(deduplicate_events(prefix + prefix)) == len(prefix)
    with pytest.raises(StreamError, match="incompatible"):
        resume_from_checkpoint(StreamingPlan("stream-1", "other", event_count=4), saved)
    with pytest.raises(StreamError, match="incompatible"):
        resume_from_checkpoint(
            StreamingPlan("stream-1", "fp-1", event_count=4, query_id="other"), saved
        )


def test_checkpoint_rejects_gaps_and_cross_stream_events() -> None:
    current = plan()
    events = generate_bounded_events(current)
    with pytest.raises(StreamError, match="contiguous"):
        checkpoint(current, (events[0], events[2]))
    with pytest.raises(StreamError, match="different stream"):
        checkpoint(current, ({**events[0], "stream_id": "other"},))
    with pytest.raises(StreamError, match="offset zero"):
        checkpoint(current, (events[1],))


def test_manifest_rejects_schema_mismatch_and_offset_gaps() -> None:
    current = plan()
    events = generate_bounded_events(current)
    with pytest.raises(StreamError, match="stream plan"):
        manifest(current, ({**events[0], "schema_version": "2"},))
    with pytest.raises(StreamError, match="contiguous"):
        manifest(current, (events[0], events[2]))
    with pytest.raises(StreamError, match="outside"):
        manifest(current, ({**events[0], "offset": current.event_count},))
    with pytest.raises(StreamError, match="unique"):
        manifest(current, (events[0], {**events[1], "event_id": events[0]["event_id"]}))


def test_bounded_events_have_deterministic_inter_arrival_times() -> None:
    current = plan(inter_arrival_seconds=2.5)
    events = generate_bounded_events(current)
    assert events[0]["event_time"] == "2020-01-01T00:00:00+00:00"
    assert events[1]["event_time"] == "2020-01-01T00:00:02.500000+00:00"
    with pytest.raises(ValueError, match="inter_arrival"):
        plan(inter_arrival_seconds=0)


def test_manifest_records_the_declared_event_rate_contract() -> None:
    current = plan(events_per_second=4.0, inter_arrival_seconds=0.25, seed=42, query_id="query-1")
    result = manifest(current, generate_bounded_events(current))
    assert result.events_per_second == 4.0
    assert result.inter_arrival_seconds == 0.25
    assert result.seed == 42
    assert result.query_id == "query-1"
    assert result.watermark_delay_seconds == 0.0
    assert result.to_dict()["seed"] == 42


def test_stream_seed_is_part_of_deterministic_event_identity() -> None:
    first = generate_bounded_events(plan(seed=1))
    second = generate_bounded_events(plan(seed=2))
    assert first[0]["event_id"] != second[0]["event_id"]


def test_stream_events_record_processing_and_watermark_times() -> None:
    current = plan(watermark_delay_seconds=5.0)
    event = generate_bounded_events(current)[1]
    assert event["arrival_time"] == event["event_time"]
    assert event["processing_time"] == "2020-01-01T00:00:06+00:00"
    assert event["watermark_time"] == event["processing_time"]
