import ast
from pathlib import Path

import pytest


@pytest.mark.spark  # type: ignore[untyped-decorator]
def test_spark_metrics_does_not_collect_key_domains() -> None:
    source = Path("src/sda/relationships/spark_metrics.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    assert not any(
        isinstance(node, ast.Attribute) and node.attr == "collect" for node in ast.walk(tree)
    )
