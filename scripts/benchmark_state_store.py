"""Probe Lakebase with short, disposable control-plane operations.

The probe is intentionally not a synthetic-data benchmark. It measures the two
operations that justify an operational state store: opening a fresh connection
and completing a small request transaction. Every inserted request is deleted
in a finally block, and the command never prints the DSN or credential.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from collections.abc import Callable
from typing import Any

from sda.benchmark import LatencySummary, summarise_latencies


def _connect(dsn: str) -> Any:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("install the 'postgres' extra to benchmark Lakebase") from exc
    return psycopg.connect(dsn)


def _measure(
    operation: str,
    iterations: int,
    warmup: int,
    action: Callable[[], None],
    *,
    clock: Callable[[], float] = time.perf_counter,
) -> LatencySummary:
    for _ in range(warmup):
        action()
    samples: list[float] = []
    for _ in range(iterations):
        started = clock()
        action()
        samples.append((clock() - started) * 1000)
    return summarise_latencies(operation, tuple(samples))


def benchmark(
    dsn: str,
    *,
    iterations: int = 20,
    warmup: int = 3,
    connect: Callable[[str], Any] = _connect,
) -> tuple[LatencySummary, LatencySummary]:
    """Measure reconnect and short request transaction latency."""

    if not dsn.strip():
        raise ValueError("dsn must not be empty")
    if iterations < 1 or warmup < 0:
        raise ValueError("iterations must be positive and warmup cannot be negative")

    def connect_and_ping() -> None:
        with connect(dsn) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()

    reconnect = _measure("connect_and_ping", iterations, warmup, connect_and_ping)

    marker = f"sda09-benchmark-{uuid.uuid4()}"
    request_id = str(uuid.uuid4())
    idempotency_key = marker

    def request_transaction() -> None:
        with connect(dsn) as connection, connection.cursor() as cursor:
            try:
                cursor.execute(
                    "INSERT INTO sda_generation_requests "
                    "(request_id, requester_id, environment, idempotency_key, source_scope, generation_options) "
                    "VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb)",
                    (request_id, "sda09-benchmark", "dev", idempotency_key, "{}", "{}"),
                )
                cursor.execute(
                    "SELECT request_id FROM sda_generation_requests WHERE request_id = %s",
                    (request_id,),
                )
                if cursor.fetchone() is None:
                    raise RuntimeError("benchmark request was not readable in its transaction")
                cursor.execute(
                    "DELETE FROM sda_generation_requests WHERE request_id = %s", (request_id,)
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    try:
        transaction = _measure(
            "request_insert_select_delete", iterations, warmup, request_transaction
        )
    finally:
        with connect(dsn) as connection, connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM sda_generation_requests WHERE request_id = %s", (request_id,)
            )
            connection.commit()
    return reconnect, transaction


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-env", default="SDA_POSTGRES_DSN")
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--warmup", type=int, default=3)
    args = parser.parse_args()
    dsn = os.environ.get(args.dsn_env, "")
    if not dsn:
        raise SystemExit(f"environment variable {args.dsn_env} is empty")
    summaries = benchmark(dsn, iterations=args.iterations, warmup=args.warmup)
    print(json.dumps({summary.operation: summary.as_dict() for summary in summaries}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
