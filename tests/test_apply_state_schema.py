from pathlib import Path

import pytest
from scripts.apply_state_schema import migration_files, migration_id


def test_migration_id_is_deterministic_and_versioned() -> None:
    assert migration_id("schema") == migration_id("schema")
    assert migration_id("schema").startswith("state-schema-")
    assert migration_id("schema") != migration_id("schema-v2")


def test_migration_files_are_ordered_and_non_empty(tmp_path: Path) -> None:
    (tmp_path / "002_second.sql").write_text("select 2", encoding="utf-8")
    (tmp_path / "001_first.sql").write_text("select 1", encoding="utf-8")
    assert [path.name for path in migration_files(tmp_path)] == [
        "001_first.sql",
        "002_second.sql",
    ]


def test_migration_files_reject_empty_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no SQL migrations"):
        migration_files(tmp_path)
