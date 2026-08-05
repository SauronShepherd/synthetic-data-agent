from sda.relationships.detector import RelationshipDetector


def test_relationship_analysis_id_is_deterministic() -> None:
    first = RelationshipDetector().detect({}, {}, run_id="run")
    second = RelationshipDetector().detect({}, {}, run_id="run")
    assert first["analysis_id"] == second["analysis_id"]
