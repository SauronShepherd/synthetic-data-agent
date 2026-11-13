import pytest

from sda.benchmark import median_ms, summarise_latencies


def test_latency_summary_uses_nearest_rank_percentiles() -> None:
    summary = summarise_latencies("probe", (1.0, 2.0, 3.0, 4.0, 100.0))
    assert summary.samples == 5
    assert summary.p50_ms == 3.0
    assert summary.p95_ms == 100.0
    assert summary.max_ms == 100.0
    assert median_ms((1.0, 3.0, 5.0)) == 3.0


@pytest.mark.parametrize(
    "samples",
    [(), (-1.0, 1.0)],
)
def test_latency_summary_rejects_invalid_samples(samples: tuple[float, ...]) -> None:
    with pytest.raises(ValueError):
        summarise_latencies("probe", samples)
