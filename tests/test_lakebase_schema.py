from __future__ import annotations

from pathlib import Path


def test_lakebase_schema_contains_operational_constraints() -> None:
    sql = Path("sql/lakebase_state_schema.sql").read_text(encoding="utf-8")
    assert "idempotency_key TEXT NOT NULL UNIQUE" in sql
    assert "sda_one_running_attempt_per_stage" in sql
    assert "PRIMARY KEY (run_id, approval_type)" in sql
    assert "metadata JSONB" in sql
