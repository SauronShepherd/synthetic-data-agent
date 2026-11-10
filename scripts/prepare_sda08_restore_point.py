"""Create and remove a disposable Lakebase row while recording its restore time."""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from uuid import uuid4


def prepare_restore_point(dsn: str) -> tuple[str, str]:
    """Return a restore timestamp for a committed, then removed request marker."""

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise RuntimeError("install the 'postgres' extra to prepare a restore point") from exc

    request_id = "sda08-restore-" + uuid4().hex
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO sda_generation_requests "
                "(request_id, requester_id, environment, idempotency_key, source_scope, "
                "generation_options, intended_use) "
                "VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)",
                (
                    request_id,
                    "sda08-recovery-check",
                    "dev",
                    request_id,
                    '{"catalog":"samples","schema":"wanderbricks","tables":["users"]}',
                    "{}",
                    "disposable point-in-time recovery verification",
                ),
            )
        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute("SELECT clock_timestamp()")
            restore_time = cursor.fetchone()[0]
        if not isinstance(restore_time, datetime):  # pragma: no cover - driver contract
            raise RuntimeError("Lakebase did not return a timestamp for the restore point")

        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM sda_generation_requests WHERE request_id = %s", (request_id,)
            )
        connection.commit()

    return request_id, restore_time.isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn-env",
        default="SDA_POSTGRES_DSN",
        help="environment variable containing the PostgreSQL/Lakebase DSN",
    )
    args = parser.parse_args()
    dsn = os.environ.get(args.dsn_env, "")
    if not dsn:
        raise SystemExit(f"environment variable {args.dsn_env} is empty")
    request_id, restore_time = prepare_restore_point(dsn)
    print(f"request_id={request_id}")
    print(f"source_branch_time={restore_time}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
