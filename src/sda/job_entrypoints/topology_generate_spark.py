"""Databricks entrypoint for the SDA 12 topology smoke flow."""

from __future__ import annotations

import argparse
import json

from sda.topology_spark import TopologySparkError, generate_topology_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", default="sda12-smoke")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--staging-schema", required=True)
    parser.add_argument("--plan-fingerprint", default="sda12-plan")
    parser.add_argument("--node-count", default="8", type=int)
    parser.add_argument("--edge-count", default="8", type=int)
    parser.add_argument("--acyclic", default="false")
    parser.add_argument("--bipartite", default="false")
    return parser.parse_args()


def main() -> None:
    from pyspark.sql import SparkSession

    args = parse_args()
    if args.fixture != "sda12-smoke":
        raise SystemExit("only the sda12-smoke fixture is supported by this entrypoint")
    try:
        result = generate_topology_spark(
            SparkSession.builder.getOrCreate(),
            plan_fingerprint=args.plan_fingerprint,
            run_id=args.run_id,
            staging_schema=args.staging_schema,
            node_count=args.node_count,
            edge_count=args.edge_count,
            acyclic=args.acyclic.lower() == "true",
            bipartite=args.bipartite.lower() == "true",
        )
    except TopologySparkError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(result, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
