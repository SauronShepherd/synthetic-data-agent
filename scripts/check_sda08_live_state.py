"""Inspect the disposable-row and schema-version invariants of a live SDA database."""

from __future__ import annotations

import argparse
import os


def inspect_state(dsn: str) -> tuple[int, int, int]:
    """Return `(sda_tables, schema_versions, disposable_rows)` for a live database."""

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise RuntimeError("install the 'postgres' extra to inspect live state") from exc

    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name LIKE 'sda_%'"
        )
        sda_tables = int(cursor.fetchone()[0])
        cursor.execute("SELECT count(*) FROM sda_schema_versions")
        schema_versions = int(cursor.fetchone()[0])
        cursor.execute(
            "SELECT count(*) FROM sda_generation_requests "
            "WHERE request_id LIKE 'sda08-restore-%' OR request_id LIKE 'live-%'"
        )
        disposable_rows = int(cursor.fetchone()[0])
    return sda_tables, schema_versions, disposable_rows


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
    tables, schema_versions, disposable_rows = inspect_state(dsn)
    print(f"sda_tables={tables}")
    print(f"schema_versions={schema_versions}")
    print(f"disposable_rows={disposable_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
