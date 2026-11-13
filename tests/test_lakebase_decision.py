from sda.lakebase_decision import SDA09_DECISION, StoragePlane


def test_sda09_decision_keeps_operational_and_analytical_planes_separate() -> None:
    SDA09_DECISION.validate()
    assert SDA09_DECISION.chosen_operational_store == "Lakebase Postgres Autoscaling"
    assert SDA09_DECISION.analytical_store == "Unity Catalog and Delta"
    assert SDA09_DECISION.boundary.owner_for("leases") is StoragePlane.OPERATIONAL_STATE
    assert SDA09_DECISION.boundary.owner_for("profiles") is StoragePlane.ANALYTICAL_EVIDENCE
    assert "raw source rows" in SDA09_DECISION.boundary.forbidden_operational_payloads


def test_sda09_decision_records_all_candidates_and_revisit_conditions() -> None:
    names = {candidate.name for candidate in SDA09_DECISION.candidates}
    assert names == {"Lakebase Postgres Autoscaling", "Delta tables", "External managed PostgreSQL"}
    assert len(SDA09_DECISION.criteria) >= 6
    assert SDA09_DECISION.revisit_triggers
    assert any("cost" in trigger for trigger in SDA09_DECISION.revisit_triggers)
    assert "replacing the lakehouse" in SDA09_DECISION.non_goals
