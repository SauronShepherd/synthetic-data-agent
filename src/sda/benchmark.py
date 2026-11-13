"""Small, dependency-free latency summaries for the SDA-09 workload probe."""

from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from statistics import median


@dataclass(frozen=True, slots=True)
class LatencySummary:
    operation: str
    samples: int
    p50_ms: float
    p95_ms: float
    max_ms: float

    def as_dict(self) -> dict[str, float | int | str]:
        return {
            "operation": self.operation,
            "samples": self.samples,
            "p50_ms": self.p50_ms,
            "p95_ms": self.p95_ms,
            "max_ms": self.max_ms,
        }


def _percentile(values: tuple[float, ...], percentile: float) -> float:
    if not values:
        raise ValueError("at least one latency sample is required")
    if not 0 < percentile <= 100:
        raise ValueError("percentile must be greater than zero and at most 100")
    ordered = sorted(values)
    rank = max(1, ceil(len(ordered) * percentile / 100))
    return ordered[rank - 1]


def summarise_latencies(operation: str, samples_ms: tuple[float, ...]) -> LatencySummary:
    """Return conservative nearest-rank p50/p95 values in milliseconds."""

    if not operation.strip():
        raise ValueError("operation must not be empty")
    if any(sample < 0 for sample in samples_ms):
        raise ValueError("latencies cannot be negative")
    return LatencySummary(
        operation=operation,
        samples=len(samples_ms),
        p50_ms=_percentile(samples_ms, 50),
        p95_ms=_percentile(samples_ms, 95),
        max_ms=max(samples_ms),
    )


def median_ms(samples_ms: tuple[float, ...]) -> float:
    """Expose the median for callers that want a single summary value."""

    if not samples_ms:
        raise ValueError("at least one latency sample is required")
    return float(median(samples_ms))
