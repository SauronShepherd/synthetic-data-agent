"""Databricks entrypoint for the SDA 13 controlled-noise smoke flow."""

from __future__ import annotations

import argparse
import json

from sda.noise_spark import NoiseSparkError, generate_noise_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", default="sda13-smoke")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--staging-schema", required=True)
    parser.add_argument("--noise-id", default="sda13-noise")
    parser.add_argument("--profile", default="qa")
    parser.add_argument("--defect-type", default="malformed_value")
    parser.add_argument("--budget", default=0, type=int)
    parser.add_argument("--rate", default="")
    parser.add_argument("--seed", default=1729, type=int)
    parser.add_argument("--row-count", default=40, type=int)
    return parser.parse_args()


def main() -> None:
    from pyspark.sql import SparkSession

    args = parse_args()
    if args.fixture != "sda13-smoke":
        raise SystemExit("only the sda13-smoke fixture is supported by this entrypoint")
    try:
        result = generate_noise_spark(
            SparkSession.builder.getOrCreate(),
            run_id=args.run_id,
            staging_schema=args.staging_schema,
            noise_id=args.noise_id,
            profile=args.profile,
            defect_type=args.defect_type,
            budget=args.budget,
            rate=float(args.rate) if args.rate else None,
            seed=args.seed,
            row_count=args.row_count,
        )
    except NoiseSparkError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(result, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
