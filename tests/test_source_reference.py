import pytest

from sda.artifacts.models import SourceReference


def test_source_reference_requires_version_for_delta_snapshot() -> None:
    with pytest.raises(ValueError):
        SourceReference("main.sales.orders", "TABLE", "delta_version", None, None, None)


def test_source_reference_rejects_unknown_snapshot_kind() -> None:
    with pytest.raises(ValueError):
        SourceReference("main.sales.orders", "TABLE", "unknown", None, None, None)
