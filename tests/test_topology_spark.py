from __future__ import annotations

import pytest

from sda.topology_spark import TopologySparkError, topology_table_names


def test_topology_table_names_are_run_scoped() -> None:
    names = topology_table_names("sda_dev.sda_integration", "retry/one")
    assert names[0].endswith("sda12_nodes__retry_one")
    assert names[1].endswith("sda12_edges__retry_one")
    assert names[2].endswith("sda12_manifest__retry_one")


def test_topology_table_names_reject_unsafe_schema() -> None:
    with pytest.raises(TopologySparkError, match="catalog.schema"):
        topology_table_names("sda_dev", "run")
