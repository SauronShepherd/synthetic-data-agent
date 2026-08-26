from pathlib import Path

from scripts.check_state_schema import validate


def test_lakebase_state_schema_has_required_contracts() -> None:
    sql = (Path(__file__).parents[1] / "sql" / "lakebase_state_schema.sql").read_text(
        encoding="utf-8"
    )
    assert validate(sql) == []


def test_state_schema_validator_rejects_missing_table() -> None:
    errors = validate("CREATE TABLE IF NOT EXISTS sda_runs (run_id TEXT PRIMARY KEY);")
    assert any("sda_execution_attempts" in error for error in errors)
