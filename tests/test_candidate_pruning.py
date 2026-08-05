from sda.relationships.candidates import discover_key_candidates
from sda.relationships.detector import RelationshipDetector, RelationshipDiscoveryConfig


def test_composite_candidate_marks_minimality() -> None:
    profiles = discover_key_candidates(
        "orders",
        [
            {"region": "n", "order_id": 1},
            {"region": "n", "order_id": 2},
            {"region": "s", "order_id": 1},
            {"region": "s", "order_id": 2},
        ],
        ["region", "order_id"],
        max_width=2,
    )

    composite = next(profile for profile in profiles if profile.columns == ("region", "order_id"))
    assert composite.minimal is True


def test_relationship_analysis_reports_candidate_counts() -> None:
    result = RelationshipDetector(
        RelationshipDiscoveryConfig(max_relationship_candidates=1)
    ).detect({}, {})

    assert result["candidate_counts"] == {
        "discovered": 0,
        "retained": 0,
        "verified": 0,
        "untested": 0,
    }
