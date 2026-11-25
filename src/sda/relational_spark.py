"""Distributed Spark adapter for the relational generator.

The local generator is useful for small fixtures and receipts. This adapter
keeps row identities, key domains, assignments, joins, and validation in Spark
DataFrames. Only compact counts and metrics cross the driver boundary.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

from sda.artifacts.fingerprint import fingerprint
from sda.planning import ColumnGenerationSpec, GenerationPlan, PlanStatus
from sda.relational import (
    BridgeTableSpec,
    CompositeForeignKeySpec,
    CycleStrategySpec,
    ForeignKeySpec,
    KeyDomainSpec,
    RelationalGenerationError,
    RelationalPlan,
    SelfReferenceSpec,
    _all_relationships,
    _cycle_deferred_relationships,
    _normalise_key_domains,
    _order_tables,
    _prepare_fanouts,
    _resolve_row_counts,
    _validate_column_compatibility,
    _validate_key_domain_specs,
    build_relational_plan,
)
from sda.runtime.identifiers import QualifiedName


def _spark_functions() -> tuple[Any, Any, Any]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    return F, Window, None


def _qualified_schema(schema: str) -> tuple[str, str]:
    parts = schema.split(".")
    if len(parts) != 2 or any(not part.strip() for part in parts):
        raise RelationalGenerationError("staging schema must be catalog.schema")
    return parts[0], parts[1]


def _staged_table_name(schema: str, table: str, run_id: str) -> QualifiedName:
    catalog, schema_name = _qualified_schema(schema)
    safe_run = re.sub(r"[^A-Za-z0-9_]+", "_", run_id).strip("_") or "run"
    return QualifiedName.parse(f"{catalog}.{schema_name}.{table}__sda11_{safe_run}")


def _manifest_name(value: str, run_id: str) -> QualifiedName:
    parsed = QualifiedName.parse(value)
    safe_run = re.sub(r"[^A-Za-z0-9_]+", "_", run_id).strip("_") or "run"
    return QualifiedName.parse(f"{parsed.catalog}.{parsed.schema}.{parsed.object_name}__{safe_run}")


def _column_spec_map(plan: GenerationPlan) -> dict[tuple[str, str], ColumnGenerationSpec]:
    return {(spec.table, spec.column): spec for spec in plan.columns}


def _typed_null(F: Any, data_type: Any) -> Any:
    return F.lit(None).cast(data_type)


def _column_expression(F: Any, plan: GenerationPlan, spec: ColumnGenerationSpec) -> Any:
    coordinate = F.col("__sda_row_id")
    token = F.concat_ws(
        "|",
        F.lit(plan.plan_fingerprint),
        F.lit(spec.table),
        F.lit(spec.column),
        coordinate.cast("string"),
    )
    digest = F.sha2(token, 256)
    model = spec.model.lower()
    if model in {"identifier", "id"}:
        return F.substring(digest, 1, 24).cast(spec.data_type)
    if model in {"string", "format"}:
        signature = spec.parameters.get("format_signature")
        if signature:
            # Spark-side smoke generation keeps the stable coordinate visible
            # while respecting the prefix and width contract.
            prefix = str(signature).replace("A", "X").replace("#", "0")
            return F.concat(F.lit(prefix + "-"), F.lpad(coordinate.cast("string"), 8, "0")).cast(
                spec.data_type
            )
        prefix = str(spec.parameters.get("prefix", spec.column))
        return F.concat(F.lit(prefix + "-"), F.lpad(coordinate.cast("string"), 8, "0")).cast(
            spec.data_type
        )
    if model in {"integer", "numeric", "uniform"}:
        low = float(spec.parameters.get("min", 0))
        high = float(spec.parameters.get("max", 1))
        if high < low:
            raise RelationalGenerationError(f"invalid numeric range for {spec.column}")
        if low == high:
            return F.lit(low).cast(spec.data_type)
        span = max(1, int(high - low + 1))
        return (F.pmod(F.xxhash64(token), F.lit(span)) + F.lit(low)).cast(spec.data_type)
    if model in {"boolean", "bool"}:
        return (F.pmod(coordinate, F.lit(2)) == F.lit(1)).cast(spec.data_type)
    if model == "date":
        return F.date_add(F.lit("2020-01-01"), F.pmod(coordinate, F.lit(28)).cast("int")).cast(
            spec.data_type
        )
    if model == "timestamp":
        return F.to_timestamp(
            F.concat(
                F.date_format(
                    F.date_add(F.lit("2020-01-01"), F.pmod(coordinate, F.lit(28)).cast("int")),
                    "yyyy-MM-dd",
                ),
                F.lit("T00:00:00Z"),
            )
        ).cast(spec.data_type)
    raise RelationalGenerationError(f"Spark adapter does not support generation model {spec.model}")


def _base_frame(spark: Any, plan: GenerationPlan, table: str, count: int) -> Any:
    F, _, _ = _spark_functions()
    frame = spark.range(count).withColumnRenamed("id", "__sda_row_id")
    for spec in plan.columns:
        if spec.table == table:
            frame = frame.withColumn(spec.column, _column_expression(F, plan, spec))
    return frame


def _relationship_columns(
    relationship: ForeignKeySpec | CompositeForeignKeySpec,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if isinstance(relationship, ForeignKeySpec):
        return (relationship.child_column,), (relationship.parent_column,)
    return relationship.child_columns, relationship.parent_columns


def _allocation_frame(
    spark: Any,
    child_count: int,
    parent_count: int,
    relationship: ForeignKeySpec | CompositeForeignKeySpec,
    plan: GenerationPlan,
    fanouts: Mapping[tuple[str, str], tuple[int, ...]],
) -> Any:
    F, Window, _ = _spark_functions()
    child = spark.range(child_count).withColumnRenamed("id", "__sda_row_id")
    if parent_count == 0:
        return child.withColumn("__sda_parent_idx", F.lit(None).cast("long"))
    if relationship.cardinality == "one_to_one":
        return child.withColumn(
            "__sda_parent_idx",
            F.when(F.col("__sda_row_id") < F.lit(parent_count), F.col("__sda_row_id")),
        )
    distribution = fanouts.get((relationship.child_table, relationship.parent_table))
    if distribution is None:
        return child.withColumn(
            "__sda_parent_idx", F.pmod(F.col("__sda_row_id"), F.lit(parent_count))
        )
    allocation = spark.createDataFrame(
        [(index, int(count)) for index, count in enumerate(distribution)],
        "__sda_parent_idx long, __sda_child_count long",
    )
    previous = F.sum("__sda_child_count").over(
        Window.orderBy("__sda_parent_idx").rowsBetween(Window.unboundedPreceding, -1)
    )
    allocation = allocation.withColumn("__sda_start", F.coalesce(previous, F.lit(0)))
    allocation = allocation.where(F.col("__sda_child_count") > 0).withColumn(
        "__sda_child_idx",
        F.explode(
            F.sequence(
                F.col("__sda_start"),
                F.col("__sda_start") + F.col("__sda_child_count") - F.lit(1),
            )
        ),
    )
    allocation = allocation.where(F.col("__sda_child_idx").isNotNull()).select(
        F.col("__sda_child_idx").alias("__sda_row_id"), F.col("__sda_parent_idx")
    )
    return child.join(allocation, "__sda_row_id", "left")


def _optional_condition(F: Any, plan: GenerationPlan, relationship: Any) -> Any:
    if relationship.optional_rate <= 0:
        return F.lit(False)
    token = F.concat_ws(
        "|",
        F.lit(plan.plan_fingerprint),
        F.lit(relationship.child_table),
        F.lit(relationship.parent_table),
        F.col("__sda_row_id").cast("string"),
    )
    threshold = int(float(relationship.optional_rate) * 1_000_000)
    return F.pmod(F.xxhash64(token), F.lit(1_000_000)) < F.lit(threshold)


def _assign_relationship_spark(
    frame: Any,
    frames: Mapping[str, Any],
    relationship: ForeignKeySpec | CompositeForeignKeySpec,
    plan: GenerationPlan,
    fanouts: Mapping[tuple[str, str], tuple[int, ...]],
) -> Any:
    F, _, _ = _spark_functions()
    child_columns, parent_columns = _relationship_columns(relationship)
    parent = frames[relationship.parent_table]
    parent_count = parent.count()
    allocation = _allocation_frame(
        frame.sparkSession,
        frame.count(),
        parent_count,
        relationship,
        plan,
        fanouts,
    )
    parent_select = parent.select(
        F.col("__sda_row_id").alias("__sda_parent_idx"),
        *[
            F.col(column).alias(f"__sda_parent_value_{index}")
            for index, column in enumerate(parent_columns)
        ],
    )
    assignment = allocation.select("__sda_row_id", "__sda_parent_idx")
    joined = frame.join(assignment, "__sda_row_id", "left").join(
        parent_select, "__sda_parent_idx", "left"
    )
    optional = _optional_condition(F, plan, relationship)
    for index, child_column in enumerate(child_columns):
        parent_value = F.col(f"__sda_parent_value_{index}")
        child_spec = next(
            (item for item in plan.columns if item.table == relationship.child_table and item.column == child_column),
            None,
        )
        data_type = child_spec.data_type if child_spec else parent.schema[parent_columns[index]].dataType
        assigned = F.when(optional, _typed_null(F, data_type)).otherwise(parent_value)
        joined = joined.withColumn(child_column, assigned.cast(data_type))
    helper_columns = [
        "__sda_parent_idx",
        *[f"__sda_parent_value_{index}" for index in range(len(parent_columns))],
    ]
    return joined.drop(*helper_columns)


def _assign_bridge_spark(
    frame: Any,
    frames: Mapping[str, Any],
    bridge: BridgeTableSpec,
) -> Any:
    F, _, _ = _spark_functions()
    left_columns, left_parent_columns = _relationship_columns(bridge.left)
    right_columns, right_parent_columns = _relationship_columns(bridge.right)
    left = frames[bridge.left.parent_table]
    right = frames[bridge.right.parent_table]
    left_count = left.count()
    right_count = right.count()
    if bridge.pair_unique and frame.count() > left_count * right_count:
        raise RelationalGenerationError(f"bridge {bridge.table} exceeds unique pair capacity")
    assignments = frame.withColumn(
        "__sda_left_idx",
        F.floor(F.col("__sda_row_id") / F.lit(max(1, right_count))).cast("long"),
    ).withColumn(
        "__sda_right_idx",
        F.pmod(F.col("__sda_row_id"), F.lit(max(1, right_count))).cast("long"),
    )
    left_select = left.select(
        F.col("__sda_row_id").alias("__sda_left_idx"),
        *[
            F.col(column).alias(f"__sda_left_value_{index}")
            for index, column in enumerate(left_parent_columns)
        ],
    )
    right_select = right.select(
        F.col("__sda_row_id").alias("__sda_right_idx"),
        *[
            F.col(column).alias(f"__sda_right_value_{index}")
            for index, column in enumerate(right_parent_columns)
        ],
    )
    joined = assignments.join(left_select, "__sda_left_idx", "left").join(
        right_select, "__sda_right_idx", "left"
    )
    for index, column in enumerate(left_columns):
        joined = joined.withColumn(column, F.col(f"__sda_left_value_{index}"))
    for index, column in enumerate(right_columns):
        joined = joined.withColumn(column, F.col(f"__sda_right_value_{index}"))
    return joined.drop(
        "__sda_left_idx",
        "__sda_right_idx",
        *[f"__sda_left_value_{index}" for index in range(len(left_columns))],
        *[f"__sda_right_value_{index}" for index in range(len(right_columns))],
    )


def _assign_self_reference_spark(
    frame: Any,
    spec: SelfReferenceSpec,
    plan: GenerationPlan,
    key_column: str,
) -> Any:
    F, _, _ = _spark_functions()
    frame_count = frame.count()
    if frame_count == 0:
        return frame.withColumn(spec.child_column, _typed_null(F, "string"))
    token = F.concat_ws(
        "|", F.lit(plan.plan_fingerprint), F.lit(spec.table), F.col("__sda_row_id").cast("string")
    )
    parent_idx = F.when(
        F.col("__sda_row_id") < F.lit(min(spec.root_count, frame_count)),
        F.lit(None).cast("long"),
    ).otherwise(F.pmod(F.xxhash64(token), F.greatest(F.col("__sda_row_id"), F.lit(1))))
    parent = frame.select(
        F.col("__sda_row_id").alias("__sda_self_parent_idx"),
        F.col(key_column).alias("__sda_self_parent_value"),
    )
    joined = frame.withColumn("__sda_self_parent_idx", parent_idx).join(
        parent, "__sda_self_parent_idx", "left"
    )
    data_type = next(
        (
            item.data_type
            for item in plan.columns
            if item.table == spec.table and item.column == spec.child_column
        ),
        "string",
    )
    joined = joined.withColumn(spec.child_column, F.col("__sda_self_parent_value").cast(data_type))
    return joined.drop("__sda_self_parent_idx", "__sda_self_parent_value")


def _safe_dataset_fingerprint(
    relational: RelationalPlan,
    counts: Mapping[str, int],
    run_id: str,
    fanouts: Mapping[tuple[str, str], tuple[int, ...]],
) -> str:
    return fingerprint(
        {
            "relational_plan": relational.relational_fingerprint,
            "row_counts": dict(sorted(counts.items())),
            "run_id": run_id,
            "fanouts": {str(key): value for key, value in sorted(fanouts.items())},
        }
    )


def generate_relational_spark(
    spark: Any,
    relational: RelationalPlan,
    *,
    row_counts: Mapping[str, int],
    staging_schema: str,
    run_id: str,
    manifest_table: str,
    fanout_distributions: Mapping[tuple[str, str], tuple[int, ...]] | None = None,
    fanout_mode: str = "exact-total",
    fanout_caps: Mapping[tuple[str, str], tuple[int, int]] | None = None,
) -> dict[str, Any]:
    """Generate run-scoped Delta tables using Spark-only row/domain movement."""
    if relational.plan.status is not PlanStatus.APPROVED:
        raise RelationalGenerationError("Spark relational generation requires an approved plan")
    F, _, _ = _spark_functions()
    relationships = _all_relationships(relational)
    distributions = dict(fanout_distributions or {})
    counts = _resolve_row_counts(relational.plan, row_counts, distributions, fanout_mode)
    _domains = _normalise_key_domains(relational.plan, relationships, relational.key_domains)
    _validate_key_domain_specs(relational.plan, _domains)
    _validate_column_compatibility(relational.plan, relationships)
    deferred, _ = _cycle_deferred_relationships(
        relational.plan.tables, relationships, relational.cycle_strategies
    )
    fanouts, fanout_metrics = _prepare_fanouts(
        relational.plan, counts, relationships, distributions, fanout_mode, fanout_caps
    )
    dataset_fp = _safe_dataset_fingerprint(relational, counts, run_id, fanouts)
    catalog, schema_name = _qualified_schema(staging_schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema_name}`")
    manifest_name = _manifest_name(manifest_table, run_id)
    if spark.catalog.tableExists(manifest_name.full_name):
        existing = spark.table(manifest_name.quoted).where(F.col("dataset_fingerprint") == dataset_fp).limit(1).first()
        if existing is not None:
            return {
                "status": "reused",
                "dataset_fingerprint": dataset_fp,
                "manifest_table": manifest_name.full_name,
                "tables": {
                    table: _staged_table_name(staging_schema, table, run_id).full_name
                    for table in relational.plan.tables
                },
            }
        raise RelationalGenerationError("existing staging manifest has a different dataset fingerprint")
    bridge_by_table = {bridge.table: bridge for bridge in relational.bridge_tables}
    bridge_tokens = {
        token
        for bridge in relational.bridge_tables
        for token in (
            (
                bridge.left.child_table,
                (bridge.left.child_column,)
                if isinstance(bridge.left, ForeignKeySpec)
                else bridge.left.child_columns,
                bridge.left.parent_table,
                (bridge.left.parent_column,)
                if isinstance(bridge.left, ForeignKeySpec)
                else bridge.left.parent_columns,
            ),
            (
                bridge.right.child_table,
                (bridge.right.child_column,)
                if isinstance(bridge.right, ForeignKeySpec)
                else bridge.right.child_columns,
                bridge.right.parent_table,
                (bridge.right.parent_column,)
                if isinstance(bridge.right, ForeignKeySpec)
                else bridge.right.parent_columns,
            )
        )
    }
    frames: dict[str, Any] = {}
    order = _order_tables(relational.plan.tables, relationships, deferred)
    for table in order:
        frame = _base_frame(spark, relational.plan, table, counts[table])
        if table in bridge_by_table:
            frame = _assign_bridge_spark(frame, frames, bridge_by_table[table])
        else:
            for relationship in relationships:
                child_columns = _relationship_columns(relationship)[0]
                token = (
                    relationship.child_table,
                    child_columns,
                    relationship.parent_table,
                    _relationship_columns(relationship)[1],
                )
                if relationship.child_table == table and token not in deferred and token not in bridge_tokens:
                    frame = _assign_relationship_spark(frame, frames, relationship, relational.plan, fanouts)
        for self_reference in relational.self_references:
            if self_reference.table == table:
                key_domain = next(domain for domain in _domains if domain.table == table)
                if len(key_domain.columns) != 1:
                    raise RelationalGenerationError("Spark self-reference requires a single-column key")
                frame = _assign_self_reference_spark(
                    frame, self_reference, relational.plan, key_domain.columns[0]
                )
        frames[table] = frame
    for relationship in relationships:
        child_columns, _ = _relationship_columns(relationship)
        token = (
            relationship.child_table,
            child_columns,
            relationship.parent_table,
            _relationship_columns(relationship)[1],
        )
        if token in deferred and token not in bridge_tokens:
            frames[relationship.child_table] = _assign_relationship_spark(
                frames[relationship.child_table], frames, relationship, relational.plan, fanouts
            )
    table_names: dict[str, str] = {}
    table_counts: dict[str, int] = {}
    for table in relational.plan.tables:
        destination = _staged_table_name(staging_schema, table, run_id)
        output = frames[table].drop("__sda_row_id")
        output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            destination.quoted
        )
        table_names[table] = destination.full_name
        table_counts[table] = output.count()
    key_domain_records = [
        {
            "table": domain.table,
            "columns": list(domain.columns),
            "row_count": table_counts[domain.table],
            "domain_fingerprint": fingerprint(
                {
                    "plan_fingerprint": relational.plan.plan_fingerprint,
                    "table": domain.table,
                    "columns": domain.columns,
                    "row_count": table_counts[domain.table],
                    "strategy": domain.strategy,
                }
            ),
            "strategy": domain.strategy,
            "staging_location": table_names[domain.table],
        }
        for domain in _domains
    ]
    manifest_row = {
        "schema_version": "relational-generation-manifest-v1",
        "run_id": run_id,
        "plan_id": relational.plan.plan_id,
        "plan_fingerprint": relational.plan.plan_fingerprint,
        "relational_plan_fingerprint": relational.relational_fingerprint,
        "dataset_fingerprint": dataset_fp,
        "status": "GENERATED_AWAITING_VALIDATION",
        "table_counts_json": json.dumps(dict(sorted(table_counts.items())), sort_keys=True),
        "table_names_json": json.dumps(dict(sorted(table_names.items())), sort_keys=True),
        "key_domains_json": json.dumps(key_domain_records, sort_keys=True),
        "relationship_metrics_json": json.dumps(fanout_metrics, sort_keys=True, default=str),
        "validation_report_fingerprint": "",
        "approved_by": "",
    }
    spark.createDataFrame([manifest_row]).write.format("delta").mode("overwrite").saveAsTable(
        manifest_name.quoted
    )
    return {
        "status": "generated",
        "dataset_fingerprint": dataset_fp,
        "manifest_table": manifest_name.full_name,
        "tables": table_names,
        "table_counts": table_counts,
    }


def _join_condition(F: Any, left: Any, right: Any, left_columns: Sequence[str], right_columns: Sequence[str]) -> Any:
    condition = None
    for left_column, right_column in zip(left_columns, right_columns, strict=True):
        equality = left[left_column] == right[right_column]
        condition = equality if condition is None else condition & equality
    return condition


def validate_relational_spark(
    spark: Any,
    relational: RelationalPlan,
    *,
    table_names: Mapping[str, str],
    expected_counts: Mapping[str, int] | None = None,
    fanout_bounds: Mapping[tuple[str, str, str, str], tuple[int, int]] | None = None,
) -> dict[str, Any]:
    """Validate relational Delta tables with distributed joins and aggregates."""
    F, _, _ = _spark_functions()
    frames = {table: spark.table(QualifiedName.parse(name).quoted) for table, name in table_names.items()}
    checks: list[dict[str, Any]] = []
    for table, expected in (expected_counts or {}).items():
        actual = frames[table].count()
        checks.append(
            {
                "check_id": f"row_count:{table}",
                "status": "PASS" if actual == expected else "FAIL",
                "actual": actual,
                "expected": expected,
            }
        )
    for table, domain in ((domain.table, domain) for domain in _normalise_key_domains(relational.plan, _all_relationships(relational), relational.key_domains)):
        frame = frames[table]
        null_condition = None
        for column in domain.columns:
            part = F.col(column).isNull()
            null_condition = part if null_condition is None else null_condition | part
        nulls = frame.where(null_condition).count()
        duplicates = frame.where(~null_condition).groupBy(*domain.columns).count().where(F.col("count") > 1).count()
        checks.append(
            {
                "check_id": f"unique:{table}.({','.join(domain.columns)})",
                "status": "PASS" if nulls == 0 and duplicates == 0 else "FAIL",
                "nulls": nulls,
                "duplicate_groups": duplicates,
            }
        )
    for relationship in _all_relationships(relational):
        child_columns, parent_columns = _relationship_columns(relationship)
        child = frames[relationship.child_table]
        parent = frames[relationship.parent_table]
        populated_condition = None
        for column in child_columns:
            part = F.col(column).isNotNull()
            populated_condition = part if populated_condition is None else populated_condition & part
        populated = child.where(populated_condition)
        matched = populated.join(parent, _join_condition(F, populated, parent, child_columns, parent_columns), "left_semi").count()
        orphans = populated.join(parent, _join_condition(F, populated, parent, child_columns, parent_columns), "left_anti").count()
        total = child.count()
        nulls = total - populated.count()
        check_id = f"foreign_key:{relationship.child_table}.({','.join(child_columns)})"
        checks.append(
            {
                "check_id": check_id,
                "status": "PASS" if orphans == 0 else "FAIL",
                "populated_child_rows": populated.count(),
                "matched_child_rows": matched,
                "orphans": orphans,
                "nullable_child_rows": nulls,
            }
        )
        if relationship.cardinality == "one_to_one":
            duplicates = populated.groupBy(*child_columns).count().where(F.col("count") > 1).count()
            checks.append(
                {
                    "check_id": f"cardinality:{relationship.child_table}->{relationship.parent_table}",
                    "status": "PASS" if duplicates == 0 else "FAIL",
                    "duplicate_references": duplicates,
                }
            )
    for bridge in relational.bridge_tables:
        left_columns, _ = _relationship_columns(bridge.left)
        right_columns, _ = _relationship_columns(bridge.right)
        pairs = frames[bridge.table].groupBy(*(left_columns + right_columns)).count()
        duplicate_pairs = pairs.where(F.col("count") > 1).count()
        checks.append(
            {
                "check_id": f"bridge_pairs:{bridge.table}",
                "status": "PASS" if not bridge.pair_unique or duplicate_pairs == 0 else "FAIL",
                "duplicate_pairs": duplicate_pairs,
            }
        )
    for child, child_column, parent, parent_column in (fanout_bounds or {}):
        minimum, maximum = fanout_bounds[(child, child_column, parent, parent_column)]
        counts = frames[child].groupBy(child_column).count().withColumnRenamed("count", "__sda_child_count")
        parent_values = frames[parent].select(F.col(parent_column).alias("__sda_parent_value")).distinct()
        child_counts = counts.select(
            F.col(child_column).alias("__sda_parent_value"), F.col("__sda_child_count")
        )
        parent_frame = parent_values.join(child_counts, "__sda_parent_value", "left").fillna(
            {"__sda_child_count": 0}
        )
        violations = parent_frame.where(
            (F.col("__sda_child_count") < minimum) | (F.col("__sda_child_count") > maximum)
        ).count()
        checks.append(
            {
                "check_id": f"fanout:{parent}.{parent_column}->{child}.{child_column}",
                "status": "PASS" if violations == 0 else "FAIL",
                "violations": violations,
                "minimum": minimum,
                "maximum": maximum,
            }
        )
    failed = sum(check["status"] == "FAIL" for check in checks)
    return {
        "status": "FAIL" if failed else "PASS",
        "checks": checks,
        "fingerprint": fingerprint(checks),
    }


def build_sda11_smoke_relational_plan(
    *,
    target_catalog: str = "workspace",
    target_schema: str = "sda_integration",
) -> tuple[RelationalPlan, dict[str, int], dict[tuple[str, str], tuple[int, ...]]]:
    """Build the reproducible cross-feature fixture used by local and UC tests."""
    columns = (
        ColumnGenerationSpec("customers", "customer_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("orders", "order_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("orders", "customer_id", "string", nullable=False, model="string"),
        ColumnGenerationSpec("orders", "amount", "double", nullable=False, model="uniform", parameters={"min": 1, "max": 10}),
        ColumnGenerationSpec("customer_note", "note_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("customer_note", "customer_id", "string", nullable=True, model="string"),
        ColumnGenerationSpec("customer_profile", "profile_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("customer_profile", "customer_id", "string", nullable=False, model="string"),
        ColumnGenerationSpec("products", "product_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("customer_product", "link_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("customer_product", "customer_id", "string", nullable=False, model="string"),
        ColumnGenerationSpec("customer_product", "product_id", "string", nullable=False, model="string"),
        ColumnGenerationSpec("employees", "employee_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("employees", "manager_id", "string", nullable=True, model="string"),
        ColumnGenerationSpec("locations", "tenant_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("locations", "location_number", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("accounts", "account_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("accounts", "tenant_id", "string", nullable=False, model="string"),
        ColumnGenerationSpec("accounts", "location_number", "string", nullable=False, model="string"),
        ColumnGenerationSpec("account_events", "event_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("account_events", "tenant_id", "string", nullable=False, model="string"),
        ColumnGenerationSpec("account_events", "location_number", "string", nullable=False, model="string"),
        ColumnGenerationSpec("cycle_a", "a_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("cycle_a", "b_id", "string", nullable=False, model="string"),
        ColumnGenerationSpec("cycle_b", "b_id", "string", nullable=False, model="identifier"),
        ColumnGenerationSpec("cycle_b", "a_id", "string", nullable=False, model="string"),
    )
    plan = GenerationPlan(
        plan_id="sda11-relational-smoke",
        plan_version=1,
        request_id="sda11-smoke",
        source_snapshot_ids=("sda11-fixture-snapshot",),
        input_artifact_ids=("sda11-approved-graph",),
        target_catalog=target_catalog,
        target_schema=target_schema,
        tables=(
            "customers",
            "orders",
            "customer_note",
            "customer_profile",
            "products",
            "customer_product",
            "employees",
            "locations",
            "accounts",
            "account_events",
            "cycle_a",
            "cycle_b",
        ),
        columns=columns,
        budgets={"max_rows": 1000},
    ).transition(PlanStatus.AWAITING_APPROVAL).transition(PlanStatus.APPROVED)
    foreign_keys = (
        ForeignKeySpec("orders", "customer_id", "customers", "customer_id", relationship_id="orders-customers"),
        ForeignKeySpec(
            "customer_note",
            "customer_id",
            "customers",
            "customer_id",
            nullable=True,
            optional_rate=0.5,
            relationship_id="notes-customers",
        ),
        ForeignKeySpec("customer_profile", "customer_id", "customers", "customer_id", cardinality="one_to_one", relationship_id="profile-customers"),
        ForeignKeySpec("customer_product", "customer_id", "customers", "customer_id", relationship_id="bridge-customers"),
        ForeignKeySpec("customer_product", "product_id", "products", "product_id", relationship_id="bridge-products"),
        ForeignKeySpec("cycle_a", "b_id", "cycle_b", "b_id", relationship_id="cycle-a-b"),
        ForeignKeySpec("cycle_b", "a_id", "cycle_a", "a_id", relationship_id="cycle-b-a"),
    )
    composite = (
        CompositeForeignKeySpec(
            "account_events",
            ("tenant_id", "location_number"),
            "locations",
            ("tenant_id", "location_number"),
            relationship_id="events-locations",
        ),
        CompositeForeignKeySpec(
            "accounts",
            ("tenant_id", "location_number"),
            "locations",
            ("tenant_id", "location_number"),
            relationship_id="accounts-locations",
        ),
    )
    bridge = (
        BridgeTableSpec(
            "customer_product",
            foreign_keys[3],
            foreign_keys[4],
        ),
    )
    self_refs = (SelfReferenceSpec("employees", "manager_id", "employee_id", root_count=2),)
    cycle_strategies = (CycleStrategySpec(("cycle_a", "cycle_b"), "staging"),)
    all_edges = (*foreign_keys, *composite)
    artifact = {
        "status": "complete",
        "plan_fingerprint": plan.plan_fingerprint,
        "relationships": [
            {
                "child_table": relationship.child_table,
                "child_columns": list(_relationship_columns(relationship)[0]),
                "parent_table": relationship.parent_table,
                "parent_columns": list(_relationship_columns(relationship)[1]),
                "decision": "accepted",
                "accepted_for_graph": True,
                "validation_status": "complete",
                "review_status": "not_required",
            }
            for relationship in all_edges
        ],
    }
    domains = tuple(
        KeyDomainSpec(table, columns)
        for table, columns in (
            ("customers", ("customer_id",)),
            ("orders", ("order_id",)),
            ("customer_note", ("note_id",)),
            ("customer_profile", ("profile_id",)),
            ("products", ("product_id",)),
            ("customer_product", ("link_id",)),
            ("employees", ("employee_id",)),
            ("locations", ("tenant_id", "location_number")),
            ("accounts", ("account_id",)),
            ("account_events", ("event_id",)),
            ("cycle_a", ("a_id",)),
            ("cycle_b", ("b_id",)),
        )
    )
    relational = build_relational_plan(
        plan,
        foreign_keys=foreign_keys,
        composite_foreign_keys=composite,
        key_domains=domains,
        bridge_tables=bridge,
        self_references=self_refs,
        cycle_strategies=cycle_strategies,
        relationship_artifact=artifact,
    )
    counts = {
        "customers": 4,
        "orders": 10,
        "customer_note": 4,
        "customer_profile": 4,
        "products": 3,
        "customer_product": 5,
        "employees": 6,
        "locations": 4,
        "accounts": 6,
        "account_events": 8,
        "cycle_a": 2,
        "cycle_b": 3,
    }
    fanouts = {("orders", "customers"): (3, 0, 1, 6)}
    return relational, counts, fanouts


__all__ = [
    "build_sda11_smoke_relational_plan",
    "generate_relational_spark",
    "validate_relational_spark",
]
