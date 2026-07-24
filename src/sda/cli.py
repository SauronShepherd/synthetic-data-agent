"""Command-line interface for the Synthetic Data Agent."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from typing import Any, NoReturn

from sda.config import Settings
from sda.demo import run_design_demo, run_metadata_demo
from sda.logging import configure_logging
from sda.tools.uc_metadata_reader import (
    read_uc_metadata_with_databricks_sql,
    read_uc_metadata_with_spark,
)
from sda.version import __version__


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(
        prog="sda",
        description="Synthetic Data Agent architecture shell.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("hello", help="Print the project greeting.")
    subparsers.add_parser("version", help="Print the package version.")
    subparsers.add_parser("config", help="Print validated runtime configuration.")
    subparsers.add_parser(
        "design-demo",
        help="Run the Article 02 design-only orchestration flow.",
    )
    subparsers.add_parser(
        "metadata-demo",
        help="Run the Article 04 local metadata-reader demo.",
    )
    subparsers.add_parser(
        "metadata-read",
        help=(
            "Query real Unity Catalog INFORMATION_SCHEMA metadata. "
            "Auto-selects SQL Warehouse or Spark."
        ),
    )
    subparsers.add_parser(
        "metadata-read-spark",
        help="Query real Unity Catalog INFORMATION_SCHEMA metadata with Databricks Spark.",
    )
    subparsers.add_parser(
        "metadata-read-sql",
        help="Query real Unity Catalog INFORMATION_SCHEMA metadata through a SQL Warehouse.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return a process exit code."""
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()
    configure_logging(settings.log_level)

    if args.command == "hello":
        print(settings.app_name)
        print("The agent orchestrates; deterministic tools calculate and execute.")
    elif args.command == "version":
        print(__version__)
    elif args.command == "config":
        _print_json(settings.to_dict())
    elif args.command == "design-demo":
        _print_json(run_design_demo().to_dict())
    elif args.command == "metadata-demo":
        _print_json(run_metadata_demo())
    elif args.command == "metadata-read":
        try:
            inventory = _read_metadata_auto(settings)
        except RuntimeError as exc:
            _fail(str(exc))
        _print_json(inventory.to_dict())
    elif args.command == "metadata-read-sql":
        try:
            inventory = _read_metadata_with_databricks_sql(settings)
        except RuntimeError as exc:
            _fail(str(exc))
        _print_json(inventory.to_dict())
    elif args.command == "metadata-read-spark":
        try:
            inventory = _read_metadata_with_spark(settings)
        except RuntimeError as exc:
            _fail(str(exc))
        _print_json(inventory.to_dict())
    else:  # pragma: no cover - argparse prevents this path
        raise AssertionError(f"Unexpected command: {args.command}")

    return 0


def _read_metadata_auto(settings: Settings) -> Any:
    """Read metadata using the configured real UC runtime."""
    if settings.metadata_runtime == "databricks_sql":
        return _read_metadata_with_databricks_sql(settings)
    if settings.metadata_runtime == "spark":
        return _read_metadata_with_spark(settings)
    if settings.has_databricks_sql_credentials():
        return _read_metadata_with_databricks_sql(settings)
    return _read_metadata_with_spark(settings)


def _read_metadata_with_databricks_sql(settings: Settings) -> Any:
    """Read real Unity Catalog metadata through a Databricks SQL Warehouse."""
    if not settings.has_databricks_sql_credentials():
        raise RuntimeError(
            "Databricks SQL metadata reads require DATABRICKS_HOST or "
            "DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN. "
            "Set those variables, then run: sda metadata-read-sql. "
            "Use sda metadata-demo for local contract testing."
        )

    assert settings.databricks_server_hostname is not None
    assert settings.databricks_http_path is not None
    assert settings.databricks_token is not None

    return read_uc_metadata_with_databricks_sql(
        settings.metadata_read_config(),
        server_hostname=settings.databricks_server_hostname,
        http_path=settings.databricks_http_path,
        access_token=settings.databricks_token,
    )


def _read_metadata_with_spark(settings: Settings) -> Any:
    """Read real Unity Catalog metadata with an active Databricks Spark session."""
    spark = _active_spark_session()
    return read_uc_metadata_with_spark(settings.metadata_read_config(), spark)


def _active_spark_session() -> Any:
    """Return an active Spark session, or fail clearly outside Databricks/Spark."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover - depends on local environment
        raise RuntimeError(
            "sda metadata-read requires PySpark and should run on Databricks compute, "
            "or in a correctly configured Spark environment. Use sda metadata-demo "
            "for local execution."
        ) from exc

    try:
        return SparkSession.builder.getOrCreate()
    except Exception as exc:  # pragma: no cover - depends on local Spark/Java setup
        raise RuntimeError(
            "Could not start a Spark session. sda metadata-read is intended to run inside "
            "Databricks compute where Unity Catalog INFORMATION_SCHEMA is available. "
            "Run this command as a Databricks job, notebook task, or bundle task. "
            "For local testing, use sda metadata-demo."
        ) from exc


def _print_json(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _fail(message: str) -> NoReturn:
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)
