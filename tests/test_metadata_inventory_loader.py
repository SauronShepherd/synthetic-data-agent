from __future__ import annotations

import pytest

from sda.artifacts.loaders import load_metadata_inventory
from sda.runtime.errors import ArtifactNotFoundError


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def where(self, expression):
        del expression
        return self

    def limit(self, amount):
        return _Rows(self.rows[:amount])

    def collect(self):
        return self.rows


class _Spark:
    def __init__(self, rows):
        self.rows = rows

    def table(self, name):
        assert name == "sda_dev.profiles.metadata_inventory"
        return _Rows(self.rows)


def test_load_metadata_inventory_by_id() -> None:
    inventory = load_metadata_inventory(
        _Spark([{"payload": '{"tables": [{"full_name": "sda_dev.sample_source.t"}]}' }]),
        "sda_dev.profiles.metadata_inventory",
        "inventory-1",
    )
    assert inventory["tables"][0]["full_name"] == "sda_dev.sample_source.t"


def test_load_metadata_inventory_missing_id() -> None:
    with pytest.raises(ArtifactNotFoundError):
        load_metadata_inventory(
            _Spark([]), "sda_dev.profiles.metadata_inventory", "missing"
        )
