"""Command-line interface for the Article 02 architecture milestone."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

from sda.config import Settings
from sda.demo import run_design_demo
from sda.logging import configure_logging
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
        print(json.dumps(settings.to_dict(), indent=2, sort_keys=True))
    elif args.command == "design-demo":
        print(json.dumps(run_design_demo().to_dict(), indent=2, sort_keys=True))
    else:  # pragma: no cover - argparse prevents this path
        raise AssertionError(f"Unexpected command: {args.command}")

    return 0
