from scripts.apply_state_schema import migration_id


def test_migration_id_is_deterministic_and_versioned() -> None:
    assert migration_id("schema") == migration_id("schema")
    assert migration_id("schema").startswith("state-schema-")
    assert migration_id("schema") != migration_id("schema-v2")
