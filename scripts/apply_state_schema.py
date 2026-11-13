"""Apply the checked-in PostgreSQL/Lakebase state schema."""

from __future__ import annotations

import argparse
import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Any


def schema_version(sql: str) -> str:
    return "state-schema-" + hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16]


def schema_files(schema_dir: Path) -> tuple[Path, ...]:
    """Return ordered SQL schema files and reject an empty schema directory."""

    files = tuple(sorted(schema_dir.glob("*.sql")))
    if not files:
        raise ValueError(f"no SQL schema files found in {schema_dir}")
    return files


def apply_schema_files(
    dsn: str,
    schema_dir: Path,
    *,
    connect: Callable[..., Any] | None = None,
) -> tuple[str, ...]:
    """Apply ordered, content-addressed schema files exactly once each."""

    if connect is None:
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("install the 'postgres' extra to apply the state schema") from exc
        connect = psycopg.connect

    applied: list[str] = []
    with connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS sda_schema_versions ("
            "schema_version TEXT PRIMARY KEY, "
            "applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        for path in schema_files(schema_dir):
            sql = path.read_text(encoding="utf-8")
            identifier = schema_version(f"{path.name}\n{sql}")
            cursor.execute(
                "SELECT 1 FROM sda_schema_versions WHERE schema_version = %s",
                (identifier,),
            )
            if cursor.fetchone() is not None:
                continue
            cursor.execute(sql)
            cursor.execute(
                "INSERT INTO sda_schema_versions (schema_version) VALUES (%s) "
                "ON CONFLICT (schema_version) DO NOTHING",
                (identifier,),
            )
            applied.append(identifier)
    return tuple(applied)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dsn", help="PostgreSQL/Lakebase connection string")
    parser.add_argument(
        "--schema-dir",
        type=Path,
        required=True,
        help="Directory containing the ordered SQL schema files.",
    )
    args = parser.parse_args()
    applied = apply_schema_files(args.dsn, args.schema_dir)
    print(f"Applied state schema files: {', '.join(applied)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
