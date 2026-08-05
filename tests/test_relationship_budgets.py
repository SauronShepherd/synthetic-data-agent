from sda.relationships.detector import RelationshipDetector, RelationshipDiscoveryConfig


def test_relationship_budget_marks_unverified_candidates() -> None:
    detector = RelationshipDetector(
        RelationshipDiscoveryConfig(max_relationship_candidates=1, max_verified_candidates=1)
    )
    result = detector.detect({}, {})

    assert "relationship_budget_reached" not in result["warnings"]


def test_relationship_config_rejects_zero_budget() -> None:
    try:
        RelationshipDiscoveryConfig(max_verified_candidates=0)
    except ValueError as error:
        assert "budgets" in str(error)
    else:
        raise AssertionError("zero verification budget must be rejected")
