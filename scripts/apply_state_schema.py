"""Apply the checked-in PostgreSQL/Lakebase state schema."""

from __future__ import annotations

import argparse
import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Any


def migration_id(sql: str) -> str:
    return "state-schema-" + hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16]


def migration_files(migrations_dir: Path) -> tuple[Path, ...]:
    """Return ordered SQL migrations and reject an empty migration directory."""

    files = tuple(sorted(migrations_dir.glob("*.sql")))
    if not files:
        raise ValueError(f"no SQL migrations found in {migrations_dir}")
    return files


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


def apply_migrations(
    dsn: str,
    migrations_dir: Path,
    *,
    connect: Callable[..., Any] | None = None,
) -> tuple[str, ...]:
    """Apply ordered, content-addressed migrations exactly once each."""

    if connect is None:
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("install the 'postgres' extra to apply state migrations") from exc
        connect = psycopg.connect

    applied: list[str] = []
    with connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS sda_schema_migrations ("
            "migration_id TEXT PRIMARY KEY, "
            "applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        for path in migration_files(migrations_dir):
            sql = path.read_text(encoding="utf-8")
            identifier = migration_id(f"{path.name}\n{sql}")
            cursor.execute(sql)
            cursor.execute(
                "INSERT INTO sda_schema_migrations (migration_id) VALUES (%s) "
                "ON CONFLICT (migration_id) DO NOTHING",
                (identifier,),
            )
            applied.append(identifier)
    return tuple(applied)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dsn", help="PostgreSQL/Lakebase connection string")
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path(__file__).parents[1] / "sql" / "lakebase_state_schema.sql",
    )
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        help="Apply ordered SQL files instead of the legacy single-schema option.",
    )
    args = parser.parse_args()
    if args.migrations_dir:
        applied = apply_migrations(args.dsn, args.migrations_dir)
        print(f"Applied state migrations: {', '.join(applied)}")
    else:
        print(f"Applied state schema: {apply_schema(args.dsn, args.schema)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
