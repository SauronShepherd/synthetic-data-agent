"""Validate the credential-free PostgreSQL/Lakebase state schema contract."""

from __future__ import annotations

import sys
from pathlib import Path

REQUIRED_TABLES = {
    "sda_schema_migrations",
    "sda_runs",
    "sda_execution_attempts",
    "sda_approvals",
    "sda_feedback",
    "sda_audit_events",
}
REQUIRED_FRAGMENTS = {
    "sda_schema_migrations": ("migration_id TEXT PRIMARY KEY", "applied_at TIMESTAMPTZ"),
    "sda_runs": ("idempotency_key TEXT NOT NULL UNIQUE", "sda_runs_status"),
    "sda_execution_attempts": ("REFERENCES sda_runs(run_id)", "sda_one_running_attempt_per_stage"),
    "sda_approvals": ("PRIMARY KEY (run_id, approval_type)", "sda_approval_decision"),
    "sda_feedback": ("feedback_id TEXT PRIMARY KEY", "sda_feedback_identity_nonempty"),
    "sda_audit_events": ("metadata JSONB", "sda_audit_events_run_time"),
}


def validate(sql: str) -> list[str]:
    errors: list[str] = []
    for table in sorted(REQUIRED_TABLES):
        if f"CREATE TABLE IF NOT EXISTS {table}" not in sql:
            errors.append(f"missing required table: {table}")
        for fragment in REQUIRED_FRAGMENTS[table]:
            if fragment not in sql:
                errors.append(f"missing {table} contract fragment: {fragment}")
    if "sda_runs" in sql and "sda_execution_attempts" in sql and "sda_runs(run_id)" not in sql:
        errors.append("execution state must reference runs")
    return errors


def main(root: Path = Path(".")) -> int:
    path = root / "sql" / "lakebase_state_schema.sql"
    if not path.is_file():
        raise SystemExit(f"state schema is missing: {path}")
    errors = validate(path.read_text(encoding="utf-8"))
    if errors:
        raise SystemExit("State schema contract failed:\n- " + "\n- ".join(errors))
    print(f"State schema contract: OK ({len(REQUIRED_TABLES)} tables)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")))
