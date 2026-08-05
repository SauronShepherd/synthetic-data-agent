from sda.artifacts.models import ArtifactRef, ArtifactStatus, ArtifactType, SourceReference


def test_artifact_retains_strategy_completion_and_inventory_lineage() -> None:
    source = SourceReference(
        "main.sales.orders",
        "TABLE",
        "delta_version",
        "3",
        None,
        "fp",
        metadata_inventory_id="inventory-1",
    )
    ref = ArtifactRef(
        "a",
        ArtifactType.TABLE_PROFILE,
        "1.0",
        ArtifactStatus.COMPLETE,
        "tool",
        "0.6.0",
        "run",
        "dev",
        "now",
        "cfg",
        "main.evidence.profile",
        {},
        (source,),
        "checksum",
        "summary",
        strategy_version="spark-v1",
        completed_at="later",
    )
    payload = ref.to_dict()
    assert payload["strategy_version"] == "spark-v1"
    assert payload["completed_at"] == "later"
    assert payload["source_references"][0]["metadata_inventory_id"] == "inventory-1"
