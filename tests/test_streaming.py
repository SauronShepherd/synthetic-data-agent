from __future__ import annotations

import pytest

from sda.streaming import StreamError, StreamMode, StreamingPlan, generate_bounded_events, manifest


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
