from sda.metadata_models import ColumnMetadata, ObjectType, TableMetadata
from sda.profile_models import TableProfileRequest
from sda.tools.table_profiler import TableProfiler


def test_conditional_nulls_are_recorded_for_explicit_segments() -> None:
    metadata = TableMetadata(
        catalog="main",
        schema="source",
        object_name="events",
        object_type=ObjectType.TABLE,
        columns=(
            ColumnMetadata("segment", "string", True, 1),
            ColumnMetadata("value", "string", True, 2),
        ),
    )
    profile = TableProfiler(
        TableProfileRequest(
            "main.source.events",
            conditional_null_segments=("segment",),
        ),
        metadata,
    ).profile_rows(
        [
            {"segment": "A", "value": None},
            {"segment": "A", "value": "x"},
            {"segment": "B", "value": "y"},
        ]
    )
    metrics = profile.column_profiles[1].metrics["conditional_nulls"].value
    assert metrics["segment"]["groups"][0]["null_rate"] == 0.5
