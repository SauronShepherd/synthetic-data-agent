from sda.artifacts.loaders import load_metadata_inventory


class _Frame:
    def __init__(self) -> None:
        self.expression = ""

    def where(self, expression: str):
        self.expression = expression
        return self

    def limit(self, count: int):
        assert count == 2
        return self

    def collect(self):
        return []


class _Spark:
    def __init__(self) -> None:
        self.frame = _Frame()

    def table(self, _name: str):
        return self.frame


def test_metadata_loader_escapes_artifact_id_filter():
    spark = _Spark()
    try:
        load_metadata_inventory(spark, "main.evidence.inventory", "id' OR '1'='1")
    except Exception as exc:
        assert "not found" in str(exc)
    assert "id'' OR ''1''=''1" in spark.frame.expression
