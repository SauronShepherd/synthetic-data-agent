from sda.profiling.persistence import find_reusable_profile


class Row:
    def asDict(self, recursive: bool = True) -> dict[str, str]:
        del recursive
        return {"profile_id": "p1", "status": "COMPLETE"}


class Frame:
    def where(self, condition: str) -> "Frame":
        assert condition
        return self

    def limit(self, count: int) -> "Frame":
        assert count == 1
        return self

    def collect(self) -> list[Row]:
        return [Row()]


class Spark:
    def table(self, name: str) -> Frame:
        assert name == "sda_profiles_table_profiles"
        return Frame()


def test_find_reusable_profile_returns_completed_header() -> None:
    result = find_reusable_profile(
        Spark(),
        "sda_profiles",
        source_table="main.sales.orders",
        source_version="4",
        configuration_hash="cfg",
    )

    assert result == {"profile_id": "p1", "status": "COMPLETE"}
