from pathlib import Path


def test_sda_capability_matrix_covers_all_articles_and_release_gates() -> None:
    matrix = (Path(__file__).parents[1] / "docs" / "sda-00-08-capability-matrix.md").read_text(
        encoding="utf-8"
    )
    for article in range(9):
        assert f"SDA-{article:02d}" in matrix
    for gate in (
        "pytest",
        "ruff check",
        "mypy",
        "check_state_schema.py",
        "check_bundle_contract.py",
    ):
        assert gate in matrix
