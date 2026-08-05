from __future__ import annotations

from sda.metadata_models import (
    ColumnMetadata,
    ConstraintKind,
    ConstraintMetadata,
    MetadataReadConfig,
)
from sda.models import AgentState, GenerationRequest, RunStage, SourceScope, ToolName
from sda.tools.uc_metadata_reader import (
    RawTableMetadata,
    UcMetadataReader,
    build_agent_summary,
    information_schema_queries,
)


def test_check_constraint_expression_is_normalized() -> None:
    from sda.tools.uc_metadata_reader import _build_constraints_by_table

    result = _build_constraints_by_table(
        [{
            "constraint_catalog": "main", "constraint_schema": "sales",
            "constraint_name": "amount_positive", "catalog": "main",
            "schema": "sales", "name": "orders", "constraint_type": "CHECK",
            "enforced": "YES",
        }],
        [], [], [], [],
        [{
            "constraint_catalog": "main", "constraint_schema": "sales",
            "constraint_name": "amount_positive", "check_clause": "amount > 0",
        }],
    )
    assert result[("main", "sales", "orders")][0].check_clause == "amount > 0"


def make_reader() -> UcMetadataReader:
    return UcMetadataReader(
        MetadataReadConfig(catalog_allowlist=("main",), table_patterns=("customers",)),
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
                name="customers",
                object_type="BASE TABLE",
            ),
        ),
    )


def test_reader_filters_and_normalizes_inventory() -> None:
    inventory = make_reader().read_inventory()

    assert [table.full_name for table in inventory.tables] == ["main.sales.customers"]
    assert inventory.skipped_objects == ("sandbox.demo.customers",)
    table = inventory.tables[0]
    assert table.sensitivity_signals == ("email:sensitive_name_or_comment", "email:sensitive_tag")
    assert "declared_constraints_unvalidated" in table.metadata_warnings
    assert table.relationship_hints == ("PRIMARY KEY:customer_id",)


def test_reader_runs_as_orchestration_tool() -> None:
    state = AgentState(
        request=GenerationRequest(
            request_id="req-1",
            source=SourceScope(catalog="main", schema="sales", tables=("customers",)),
        )
    )

    result = make_reader().run(state)

    assert result.tool is ToolName.UC_METADATA_READER
    assert result.stage is RunStage.METADATA_DISCOVERED
    assert result.artifacts[0].artifact_type == "metadata_inventory"
    assert result.metrics["metadata_tables"] == 1


def test_agent_summary_keeps_reasons() -> None:
    table = make_reader().read_inventory().tables[0]

    summary = build_agent_summary(table)

    assert "main.sales.customers" in summary
    assert "email:sensitive_tag" in summary
    assert "declared_constraints_unvalidated" in summary


def test_information_schema_query_plan_includes_key_views() -> None:
    queries = information_schema_queries("main", schema="sales")
    joined = "\n".join(queries).lower()

    assert "information_schema.tables" in joined
    assert "information_schema.columns" in joined
    assert "information_schema.table_constraints" in joined
    assert "information_schema.referential_constraints" in joined


class FakeExecutor:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, sql: str) -> tuple[dict[str, object], ...]:
        self.queries.append(sql)
        sql_lower = sql.lower()
        if "information_schema.tables" in sql_lower:
            return (
                {
                    "catalog": "main",
                    "schema": "sales",
                    "name": "customers",
                    "object_type": "BASE TABLE",
                    "owner": "data_engineering",
                    "comment": "Customer master table",
                },
            )
        if "information_schema.columns" in sql_lower:
            return (
                {
                    "catalog": "main",
                    "schema": "sales",
                    "name": "customers",
                    "column_name": "customer_id",
                    "ordinal_position": 1,
                    "is_nullable": "NO",
                    "full_data_type": "BIGINT",
                    "comment": "Unique customer identifier",
                },
                {
                    "catalog": "main",
                    "schema": "sales",
                    "name": "customers",
                    "column_name": "email",
                    "ordinal_position": 2,
                    "is_nullable": "YES",
                    "full_data_type": "STRING",
                    "comment": "Customer email address",
                },
            )
        if "information_schema.column_tags" in sql_lower:
            return (
                {
                    "catalog": "main",
                    "schema": "sales",
                    "name": "customers",
                    "column_name": "email",
                    "tag_name": "pii",
                    "tag_value": "email",
                },
            )
        if "information_schema.table_tags" in sql_lower:
            return (
                {
                    "catalog": "main",
                    "schema": "sales",
                    "name": "customers",
                    "tag_name": "domain",
                    "tag_value": "customer",
                },
            )
        if "information_schema.table_constraints" in sql_lower:
            return (
                {
                    "constraint_catalog": "main",
                    "constraint_schema": "sales",
                    "constraint_name": "pk_customers",
                    "catalog": "main",
                    "schema": "sales",
                    "name": "customers",
                    "constraint_type": "PRIMARY KEY",
                    "enforced": "NO",
                },
            )
        if "information_schema.key_column_usage" in sql_lower:
            return (
                {
                    "constraint_catalog": "main",
                    "constraint_schema": "sales",
                    "constraint_name": "pk_customers",
                    "catalog": "main",
                    "schema": "sales",
                    "name": "customers",
                    "column_name": "customer_id",
                    "ordinal_position": 1,
                },
            )
        return ()


def test_information_schema_adapter_reads_real_query_results() -> None:
    from sda.tools.uc_metadata_reader import InformationSchemaMetadataAdapter

    executor = FakeExecutor()
    adapter = InformationSchemaMetadataAdapter(executor)

    inventory = adapter.read_inventory(
        MetadataReadConfig(catalog_allowlist=("main",), schema_allowlist=("sales",))
    )

    table = inventory.tables[0]
    assert table.full_name == "main.sales.customers"
    assert table.table_tags == ("domain=customer",)
    assert table.columns[1].tags == ("pii=email",)
    assert table.constraints[0].name == "pk_customers"
    assert table.relationship_hints == ("PRIMARY KEY:customer_id",)
    assert any("information_schema.tables" in query.lower() for query in executor.queries)
