"""Validate a Databricks bundle target without requiring a workspace run."""

from __future__ import annotations

import argparse
import shutil
import subprocess


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, choices=("dev", "staging", "prod"))
    parser.add_argument("--profile")
    args = parser.parse_args()
    if shutil.which("databricks") is None:
        parser.error(
            "Databricks CLI is required for bundle validation; "
            "install the official CLI and ensure it is on PATH"
        )
    command = ["databricks", "bundle", "validate", "-t", args.target]
    if args.profile:
        command.extend(("--profile", args.profile))
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
