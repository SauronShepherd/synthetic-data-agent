from sda.relationships.detector import RelationshipDetector, RelationshipDiscoveryConfig
from sda.relationships.scoring import RelationshipScoringPolicy


def test_relationship_detector_uses_configured_scoring_policy():
    policy = RelationshipScoringPolicy(
        version="custom-v2", accepted_threshold=0.99, review_threshold=0.5
    )
    detector = RelationshipDetector(RelationshipDiscoveryConfig(scoring_policy=policy))
    result = detector.detect(
        {
            "parent": {"columns": {"id": "int"}},
            "child": {"columns": {"parent_id": "int"}},
        },
        {"parent": [{"id": 1}], "child": [{"parent_id": 1}]},
    )
    assert result["configuration"]["scoring_policy"]["version"] == "custom-v2"
