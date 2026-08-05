from sda.relationships.detector import RelationshipDetector


def test_relationship_artifact_does_not_return_source_rows() -> None:
    result = RelationshipDetector().detect(
        {"main.parent": object(), "main.child": object()},
        {"main.parent": [{"id": "CANARY_SECRET"}], "main.child": [{"parent_id": "CANARY_SECRET"}]},
    )

    assert "evidence" not in result
    assert "CANARY_SECRET" not in str(result)
