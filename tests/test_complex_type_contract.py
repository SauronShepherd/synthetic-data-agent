from __future__ import annotations

from sda.profiling.complex_types import complex_metrics


def test_struct_profile_records_nested_shape_and_element_types() -> None:
    metrics, warnings = complex_metrics(
        [{"customer": {"id": 1, "name": "a"}, "tags": ["x"]}], "struct"
    )

    assert warnings == ()
    assert metrics["field_names"] == ["customer", "tags"]
    assert metrics["schema_depth"] >= 2
    assert "dict" in metrics["element_types"]


def test_unavailable_complex_types_are_explicitly_warned() -> None:
    for kind in ("binary", "large_text"):
        metrics, warnings = complex_metrics([b"secret"], kind)
        assert metrics["available"] is False
        assert warnings == (f"{kind}_profile_unavailable",)
