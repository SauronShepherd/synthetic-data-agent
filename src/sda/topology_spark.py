"""Spark implementation of the bounded SDA 12 topology contract.

The local :mod:`sda.topology` module is deliberately small and useful for
fixtures.  This adapter keeps the node and edge domains in Spark DataFrames so
that the Databricks path does not collect a graph-sized object on the driver.
It implements the deterministic structural contract used by the article's
smoke run; richer community and path preservation remains outside this scope.
"""

from __future__ import annotations

import re
from typing import Any

from sda.artifacts.fingerprint import fingerprint
from sda.runtime.identifiers import QualifiedName


class TopologySparkError(ValueError):
    """Raised when a Spark topology fixture cannot satisfy its contract."""


def _safe_run_id(run_id: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_]+", "_", run_id).strip("_") or "run"
    return token[:80]


def _schema_parts(schema: str) -> tuple[str, str]:
    parts = schema.split(".")
    if len(parts) != 2 or any(not part.strip() for part in parts):
        raise TopologySparkError("staging schema must be catalog.schema")
    QualifiedName.parse(f"{parts[0]}.{parts[1]}.placeholder")
    return parts[0], parts[1]


def topology_table_names(staging_schema: str, run_id: str) -> tuple[str, str, str]:
    """Return run-scoped node, edge, and manifest table names."""
    catalog, schema = _schema_parts(staging_schema)
    suffix = _safe_run_id(run_id)
    return (
        f"{catalog}.{schema}.sda12_nodes__{suffix}",
        f"{catalog}.{schema}.sda12_edges__{suffix}",
        f"{catalog}.{schema}.sda12_manifest__{suffix}",
    )


def generate_topology_spark(
    spark: Any,
    *,
    plan_fingerprint: str,
    run_id: str,
    staging_schema: str,
    node_count: int,
    edge_count: int,
    acyclic: bool = False,
    bipartite: bool = False,
) -> dict[str, Any]:
    """Generate and validate a deterministic bounded topology in Spark.

    The fixture uses a stable path for acyclic graphs and a stable ring for
    cyclic graphs.  Both are expressed as DataFrame columns.  Counts and
    validation flags are the only compact values brought to the driver.
    """
    if not plan_fingerprint.strip():
        raise TopologySparkError("plan fingerprint is required")
    if node_count < 1:
        raise TopologySparkError("node_count must be positive for a Spark topology")
    if edge_count < 0:
        raise TopologySparkError("edge_count must not be negative")
    capacity = node_count - 1 if acyclic else node_count
    if edge_count > capacity:
        raise TopologySparkError(
            f"edge_count {edge_count} cannot be realized by the selected topology fixture"
        )
    staging_catalog, staging_schema_name = _schema_parts(staging_schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{staging_catalog}`.`{staging_schema_name}`")
    F = __import__("pyspark.sql.functions", fromlist=["functions"])
    nodes_name, edges_name, manifest_name = topology_table_names(staging_schema, run_id)

    node_index = F.col("node_index")
    node_token = F.concat_ws("|", F.lit(plan_fingerprint), F.lit("node"), node_index.cast("string"))
    nodes = (
        spark.range(node_count)
        .withColumnRenamed("id", "node_index")
        .withColumn("node_id", F.substring(F.sha2(node_token, 256), 1, 24))
        .withColumn("run_id", F.lit(run_id))
    )
    edges = spark.range(edge_count).withColumnRenamed("id", "edge_index")
    if acyclic:
        source_index = F.col("edge_index")
        target_index = F.col("edge_index") + F.lit(1)
    elif bipartite:
        left_size = max(node_count // 2, 1)
        right_start = left_size
        right_size = node_count - right_start
        if right_size < 1 or edge_count > min(left_size, right_size):
            raise TopologySparkError("bipartite fixture cannot realize requested edge count")
        source_index = F.col("edge_index")
        target_index = F.lit(right_start) + F.col("edge_index")
    else:
        source_index = F.col("edge_index")
        target_index = F.pmod(F.col("edge_index") + F.lit(1), F.lit(node_count))
    edge_token = F.concat_ws("|", F.lit(plan_fingerprint), F.lit("edge"), F.col("edge_index"))
    source_token = F.concat_ws("|", F.lit(plan_fingerprint), F.lit("node"), source_index)
    target_token = F.concat_ws("|", F.lit(plan_fingerprint), F.lit("node"), target_index)
    edges = (
        edges.withColumn("source_index", source_index)
        .withColumn("target_index", target_index)
        .withColumn("edge_id", F.substring(F.sha2(edge_token, 256), 1, 24))
        .withColumn("source", F.substring(F.sha2(source_token, 256), 1, 24))
        .withColumn("target", F.substring(F.sha2(target_token, 256), 1, 24))
        .withColumn("run_id", F.lit(run_id))
    )
    node_columns = ["run_id", "node_id", "node_index"]
    edge_columns = ["run_id", "edge_id", "source", "target", "source_index", "target_index"]
    nodes.select(*node_columns).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(nodes_name)
    edges.select(*edge_columns).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(edges_name)

    node_rows = nodes.count()
    edge_rows = edges.count()
    duplicate_nodes = nodes.groupBy("node_id").count().where(F.col("count") > 1).limit(1).count()
    duplicate_edges = edges.groupBy("source", "target").count().where(F.col("count") > 1).limit(1).count()
    unknown_endpoints = (
        edges.select("source").union(edges.select("target")).join(
            nodes.select("node_id"), F.col("source") == F.col("node_id"), "left_anti"
        ).limit(1).count()
    )
    self_loops = edges.where(F.col("source") == F.col("target")).limit(1).count()
    status = "PASS" if (
        node_rows == node_count
        and edge_rows == edge_count
        and duplicate_nodes == 0
        and duplicate_edges == 0
        and unknown_endpoints == 0
        and self_loops == 0
    ) else "FAIL"
    output_fingerprint = fingerprint(
        {
            "plan_fingerprint": plan_fingerprint,
            "node_count": node_count,
            "edge_count": edge_count,
            "acyclic": acyclic,
            "bipartite": bipartite,
        }
    )
    manifest = spark.createDataFrame(
        [
            {
                "run_id": run_id,
                "plan_fingerprint": plan_fingerprint,
                "output_fingerprint": output_fingerprint,
                "node_count": node_rows,
                "edge_count": edge_rows,
                "duplicate_nodes": duplicate_nodes,
                "duplicate_edges": duplicate_edges,
                "unknown_endpoints": unknown_endpoints,
                "self_loops": self_loops,
                "status": status,
                "nodes_table": nodes_name,
                "edges_table": edges_name,
            }
        ]
    )
    manifest.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        manifest_name
    )
    result = {
        "run_id": run_id,
        "status": status,
        "node_count": node_rows,
        "edge_count": edge_rows,
        "duplicate_nodes": duplicate_nodes,
        "duplicate_edges": duplicate_edges,
        "unknown_endpoints": unknown_endpoints,
        "self_loops": self_loops,
        "nodes_table": nodes_name,
        "edges_table": edges_name,
        "manifest_table": manifest_name,
        "output_fingerprint": output_fingerprint,
    }
    if status != "PASS":
        raise TopologySparkError("topology output failed structural validation")
    return result
