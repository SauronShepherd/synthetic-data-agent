"""Apply the checked-in PostgreSQL/Lakebase state schema."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path


def migration_id(sql: str) -> str:
    return "state-schema-" + hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16]


def apply_schema(dsn: str, schema_path: Path) -> str:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("install the 'postgres' extra to apply the state schema") from exc
    sql = schema_path.read_text(encoding="utf-8")
    identifier = migration_id(sql)
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.execute(
            "INSERT INTO sda_schema_migrations (migration_id) VALUES (%s) "
            "ON CONFLICT (migration_id) DO NOTHING",
            (identifier,),
        )
    return identifier


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dsn", help="PostgreSQL/Lakebase connection string")
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path(__file__).parents[1] / "sql" / "lakebase_state_schema.sql",
    )
    args = parser.parse_args()
    print(f"Applied state schema: {apply_schema(args.dsn, args.schema)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
