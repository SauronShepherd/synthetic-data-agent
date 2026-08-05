from sda.job_entrypoints.relationship_detect import detect_relationships


def test_relationship_entrypoint_returns_versioned_artifact() -> None:
    artifact = detect_relationships({}, {}, run_id="run-1")
    assert artifact["artifact_version"] == "sda06-relationship-v1"
    assert artifact["run_id"] == "run-1"
