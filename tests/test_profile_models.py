from sda.metadata_models import ColumnMetadata, ObjectType, TableMetadata
from sda.profile_models import ProfileMode, TableProfileRequest, ValueRetentionPolicy
from sda.tools.table_profiler import TableProfiler


def metadata() -> TableMetadata:
    return TableMetadata(
        catalog="main",
        schema="sales",
        object_name="orders",
        object_type=ObjectType.TABLE,
        columns=(
            ColumnMetadata("amount", "DOUBLE", False, 1),
            ColumnMetadata("status", "STRING", True, 2, tags=("class.order_status",)),
        ),
    )


def test_request_validates_full_mode_and_hashes_configuration() -> None:
    request = TableProfileRequest("main.sales.orders", mode=ProfileMode.FULL, sample_fraction=1.0)
    assert request.configuration_hash
    assert (
        request.configuration_hash
        != TableProfileRequest("main.sales.orders", sample_seed=7).configuration_hash
    )


def test_table_profiler_is_deterministic_and_redacts_sensitive_categories() -> None:
    request = TableProfileRequest(
        "main.sales.orders",
        sample_fraction=1.0,
        value_retention_policy=ValueRetentionPolicy.ALLOW_SAFE_VALUES,
    )
    profile = TableProfiler(request, metadata()).profile_rows(
        [
            {"amount": 1.0, "status": "new"},
            {"amount": 100.0, "status": "new"},
            {"amount": None, "status": "closed"},
        ],
        source_version="7",
    )
    assert profile.source_version == "7"
    assert profile.snapshot_reproducible is True
    amount = profile.column_profiles[0]
    assert amount.metrics["mean"].value == 50.5
    status = profile.column_profiles[1]
    assert status.value_retention_policy is ValueRetentionPolicy.NO_VALUES
    assert "category_values_redacted" in status.warnings
    assert (
        profile.profile_id
        == TableProfiler(request, metadata())
        .profile_rows(
            [
                {"amount": 1.0, "status": "new"},
                {"amount": 100.0, "status": "new"},
                {"amount": None, "status": "closed"},
            ],
            source_version="7",
        )
        .profile_id
    )
