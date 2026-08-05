from sda.metadata_models import ColumnMetadata, ObjectType, TableMetadata
from sda.profile_models import TableProfileRequest, ValueRetentionPolicy
from sda.tools.table_profiler import TableProfiler


def test_sensitive_canary_is_not_serialized_in_profile_artifact() -> None:
    canary = "CANARY_SENSITIVE_VALUE_9f3a"
    metadata = TableMetadata(
        catalog="main",
        schema="source",
        object_name="customers",
        object_type=ObjectType.TABLE,
        columns=(
            ColumnMetadata("email", "string", True, 1, tags=("pii=email",)),
        ),
    )
    profile = TableProfiler(
        TableProfileRequest(
            "main.source.customers",
            value_retention_policy=ValueRetentionPolicy.ALLOW_SAFE_VALUES,
            sensitive_value_retention_policy=ValueRetentionPolicy.NO_VALUES,
        ),
        metadata,
    ).profile_rows([{"email": canary}])

    assert canary not in str(profile.to_dict())
    assert profile.column_profiles[0].value_retention_policy is ValueRetentionPolicy.NO_VALUES
