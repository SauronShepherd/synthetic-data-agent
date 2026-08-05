from sda.artifacts.manifest import RunManifest


def test_manifest_id_is_stable_when_completion_time_changes() -> None:
    first = RunManifest("run-1", "tool", "0.6.0", "1.0", "dev", "config", started_at="now")
    second = RunManifest(
        "run-1",
        "tool",
        "0.6.0",
        "1.0",
        "dev",
        "config",
        started_at="now",
        completed_at="later",
        status="complete",
    )
    assert first.manifest_id == second.manifest_id
