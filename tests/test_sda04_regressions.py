from sda.metadata_models import MetadataReadConfig, ObjectType
from sda.tools.uc_metadata_reader import RawTableMetadata, UcMetadataReader, build_agent_summary


def test_current_unity_catalog_table_types_are_normalized() -> None:
    for raw in ("MANAGED", "EXTERNAL", "FOREIGN", "MANAGED_SHALLOW_CLONE", "EXTERNAL_SHALLOW_CLONE"):
        assert ObjectType.from_platform(raw) is ObjectType.TABLE
    assert ObjectType.from_platform("STREAMING_TABLE") is ObjectType.STREAMING_TABLE
    assert ObjectType.from_platform("MATERIALIZED_VIEW") is ObjectType.MATERIALIZED_VIEW
    assert ObjectType.from_platform("future_type") is ObjectType.UNKNOWN


def test_table_serialization_contains_deterministic_agent_summary() -> None:
    table = UcMetadataReader(
        MetadataReadConfig(catalog_allowlist=("main",)),
        (RawTableMetadata("main", "sales", "customers", "MANAGED"),),
    ).read_inventory().tables[0]
    payload = table.to_dict()
    assert payload["raw_table_type"] == "MANAGED"
    assert payload["agent_summary"]
    assert table.full_name in payload["agent_summary"]
    assert table.full_name in build_agent_summary(table)
