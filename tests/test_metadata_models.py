from __future__ import annotations

import pytest

from sda.metadata_models import (
    ColumnMetadata,
    ConstraintKind,
    ConstraintMetadata,
    MetadataInventory,
    MetadataReadConfig,
    ObjectType,
    TableMetadata,
)


def test_table_metadata_full_name_and_dict() -> None:
    table = TableMetadata(
        catalog="main",
        schema="sales",
        object_name="customers",
        object_type=ObjectType.BASE_TABLE,
        columns=(
            ColumnMetadata(
                name="customer_id",
                data_type="BIGINT",
                nullable=False,
                ordinal_position=1,
            ),
        ),
        constraints=(
            ConstraintMetadata(
                name="pk_customers",
                kind=ConstraintKind.PRIMARY_KEY,
                columns=("customer_id",),
            ),
        ),
    )

    payload = table.to_dict()

    assert table.full_name == "main.sales.customers"
    assert payload["full_name"] == "main.sales.customers"
    assert payload["object_type"] == "BASE TABLE"
    assert payload["constraints"][0]["kind"] == "PRIMARY KEY"


def test_check_constraint_can_be_preserved_without_key_columns() -> None:
    constraint = ConstraintMetadata(
        name="ck_positive",
        kind=ConstraintKind.CHECK,
        columns=(),
        check_clause="amount > 0",
    )
    assert constraint.columns == ()
    assert constraint.check_clause == "amount > 0"


def test_inventory_rejects_duplicate_full_names() -> None:
    table = TableMetadata(
        catalog="main",
        schema="sales",
        object_name="customers",
        object_type=ObjectType.BASE_TABLE,
    )

    with pytest.raises(ValueError, match="unique"):
        MetadataInventory(tables=(table, table))


def test_metadata_read_config_requires_positive_limit() -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        MetadataReadConfig(catalog_allowlist=("main",), max_objects=0)
