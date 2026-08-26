from pathlib import Path


def test_lakebase_schema_contains_immutable_feedback_contract() -> None:
    sql = (Path(__file__).parents[1] / "sql" / "lakebase_state_schema.sql").read_text(
        encoding="utf-8"
    )
    assert "CREATE TABLE IF NOT EXISTS sda_feedback" in sql
    assert "REFERENCES sda_runs(run_id)" in sql
    assert "sda_feedback_run_time" in sql
    assert "sda_feedback_identity_nonempty" in sql


def test_lakebase_schema_tracks_migration_versions() -> None:
    sql = (Path(__file__).parents[1] / "sql" / "lakebase_state_schema.sql").read_text(
        encoding="utf-8"
    )
    assert "CREATE TABLE IF NOT EXISTS sda_schema_migrations" in sql
    assert "migration_id TEXT PRIMARY KEY" in sql
