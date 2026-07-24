"""Article 02 architecture demonstration."""

from __future__ import annotations

from sda.models import AgentState, GenerationRequest, SourceScope
from sda.orchestrator import SyntheticDataAgent
from sda.tools import article_02_toolchain


def run_design_demo() -> AgentState:
    """Run the design-only customers -> accounts -> transactions vertical slice."""
    request = GenerationRequest(
        request_id="article-02-demo",
        source=SourceScope(
            catalog="main",
            schema="sales",
            tables=("customers", "accounts", "transactions"),
        ),
        scale_factor=10.0,
        preserve_relationships=True,
        privacy_mode="strict",
        target_catalog="main",
        target_schema="synthetic_sales",
    )
    state = AgentState(request=request)
    return SyntheticDataAgent().run(state, article_02_toolchain())


def run_metadata_demo() -> dict[str, object]:
    """Run a deterministic Article 04 metadata-reader demo."""
    from sda.metadata_models import (
        ColumnMetadata,
        ConstraintKind,
        ConstraintMetadata,
        MetadataReadConfig,
    )
    from sda.tools.uc_metadata_reader import RawTableMetadata, UcMetadataReader

    reader = UcMetadataReader(
        MetadataReadConfig(catalog_allowlist=("main",), max_objects=10),
        raw_tables=(
            RawTableMetadata(
                catalog="main",
                schema="sales",
                name="customers",
                object_type="BASE TABLE",
                owner="data_engineering",
                comment="Customer master table",
                columns=(
                    ColumnMetadata(
                        name="customer_id",
                        data_type="BIGINT",
                        nullable=False,
                        ordinal_position=1,
                        comment="Unique customer identifier",
                    ),
                    ColumnMetadata(
                        name="email",
                        data_type="STRING",
                        nullable=True,
                        ordinal_position=2,
                        comment="Customer email address",
                        tags=("pii",),
                    ),
                ),
                constraints=(
                    ConstraintMetadata(
                        name="pk_customers",
                        kind=ConstraintKind.PRIMARY_KEY,
                        columns=("customer_id",),
                    ),
                ),
            ),
            RawTableMetadata(
                catalog="sandbox",
                schema="demo",
                name="test_customers_real_final",
                object_type="BASE TABLE",
                columns=(),
            ),
        ),
    )
    inventory = reader.read_inventory()
    return inventory.to_dict()
