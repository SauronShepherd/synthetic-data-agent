from pathlib import Path

import pytest
from scripts.check_bundle_contract import main

ROOT = Path(__file__).parents[1]


def test_bundle_contract_accepts_all_targets() -> None:
    assert main(ROOT) == 0


def test_bundle_contract_rejects_target_missing_pattern_variable(tmp_path: Path) -> None:
    (tmp_path / "bundle" / "targets").mkdir(parents=True)
    (tmp_path / "bundle" / "targets" / "dev.yml").write_text(
        "sda_pattern_registry_table: x\n", encoding="utf-8"
    )
    (tmp_path / "bundle" / "resources.yml").parent.mkdir(exist_ok=True)
    (tmp_path / "bundle" / "resources.yml").write_text(
        "pattern_detector:\n"
        + "\n".join(
            f"name: {name}"
            for name in (
                "metadata_artifact_id",
                "profile_artifact_ids_json",
                "relationship_artifact_id",
                "dependency_graph_artifact_id",
                "pattern_evidence_table",
            )
        ),
        encoding="utf-8",
    )
    (tmp_path / "databricks.yml").write_text(
        "\n".join(
            f"{name}: value"
            for name in (
                "sda_pattern_registry_table",
                "sda_pattern_evidence_table",
                "sda_pattern_mode",
                "sda_pattern_min_support_rows",
                "sda_pattern_min_support_rate",
                "sda_pattern_sample_fraction",
                "sda_pattern_sample_seed",
            )
        ),
        encoding="utf-8",
    )
    with pytest.raises(SystemExit, match="missing pattern variables"):
        main(tmp_path)
