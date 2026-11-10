"""Validate static SDA bundle wiring without workspace credentials."""

from __future__ import annotations

import sys
from pathlib import Path

REQUIRED_VARIABLES = {
    "sda_pattern_registry_table",
    "sda_pattern_evidence_table",
    "sda_pattern_mode",
    "sda_pattern_min_support_rows",
    "sda_pattern_min_support_rate",
    "sda_pattern_sample_fraction",
    "sda_pattern_sample_seed",
    "sda_pattern_max_rows_scanned",
}
REQUIRED_PARAMETERS = {
    "metadata_artifact_id",
    "profile_artifact_ids_json",
    "relationship_artifact_id",
    "dependency_graph_artifact_id",
    "pattern_evidence_table",
}


def main(root: Path = Path(".")) -> int:
    resources = (root / "bundle" / "resources.yml").read_text(encoding="utf-8")
    databricks = (root / "databricks.yml").read_text(encoding="utf-8")
    if "pattern_detector:" not in resources:
        raise SystemExit("pattern_detector resource is missing")
    resource_block = resources.split("pattern_detector:", 1)[1].split("standalone_generator:", 1)[0]
    missing_parameters = sorted(
        name for name in REQUIRED_PARAMETERS if f"name: {name}" not in resource_block
    )
    if missing_parameters:
        raise SystemExit(
            f"pattern detector parameters are missing: {', '.join(missing_parameters)}"
        )
    missing_defaults = sorted(
        name
        for name in REQUIRED_VARIABLES
        if f"var.{name}" not in resource_block and name not in databricks
    )
    if missing_defaults:
        raise SystemExit(f"pattern variables are missing: {', '.join(missing_defaults)}")
    targets = sorted((root / "bundle" / "targets").glob("*.yml"))
    if not targets:
        raise SystemExit("bundle targets are missing")
    for target in targets:
        text = target.read_text(encoding="utf-8")
        missing = sorted(name for name in REQUIRED_VARIABLES if f"{name}:" not in text)
        if missing:
            raise SystemExit(f"{target} is missing pattern variables: {', '.join(missing)}")
    lakebase_path = root / "bundle" / "lakebase.yml"
    if lakebase_path.exists():
        lakebase = lakebase_path.read_text(encoding="utf-8")
        required_lakebase_resources = (
            "postgres_projects:",
            "postgres_branches:",
            "postgres_endpoints:",
            "postgres_roles:",
            "postgres_databases:",
            "postgres_snapshot_schedules:",
            "sda_state_backup_schedule:",
            "daily_schedule:",
            "retention: 604800s",
        )
        missing_lakebase = [
            marker for marker in required_lakebase_resources if marker not in lakebase
        ]
        if missing_lakebase:
            raise SystemExit(
                "Lakebase resources or backup schedule are missing: "
                + ", ".join(missing_lakebase)
            )
    print(f"Bundle contract: OK ({len(targets)} targets)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")))
