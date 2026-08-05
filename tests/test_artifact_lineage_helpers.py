from sda.artifacts.lineage import lineage_summary, source_reference_from_table
from sda.metadata_models import ColumnMetadata, ObjectType, TableMetadata


def test_source_reference_from_table_preserves_metadata_scope() -> None:
    table = TableMetadata(
        catalog="main",
        schema="sales",
        object_name="orders",
        object_type=ObjectType.TABLE,
        columns=(
            ColumnMetadata(
                name="order_id",
                data_type="BIGINT",
                nullable=False,
                ordinal_position=1,
            ),
        ),
    )

    reference = source_reference_from_table(table, inventory_id="inventory-1")

    assert reference.full_name == "main.sales.orders"
    assert reference.snapshot_kind == "metadata_only"
    assert reference.selected_columns == ("order_id",)
    assert lineage_summary(reference)["selected_column_count"] == 1
