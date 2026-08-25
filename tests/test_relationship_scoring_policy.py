from sda.relationships.metrics import measure_join
from sda.relationships.scoring import RelationshipScoringPolicy, score_relationship


def test_scoring_policy_is_versioned_and_exposes_components():
    metrics = measure_join([{"id": 1}], [{"parent_id": 1}], ("id",), ("parent_id",))
    policy = RelationshipScoringPolicy(
        version="test-v2", child_row_coverage_weight=0.5, child_value_coverage_weight=0.2,
        parent_uniqueness_weight=0.2, origin_evidence_weight=0.1,
    )
    result = score_relationship(metrics, policy=policy)
    assert result["scoring_policy_version"] == "test-v2"
    assert sum(result["score_contributions"].values()) == result["confidence_score"]
    assert result["hard_gates"]["parent_key_unique"] is True
