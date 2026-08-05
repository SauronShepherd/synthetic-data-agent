"""Fail closed before controlled bundle validation or deployment."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys

REQUIRED_ENV = (
    "SDA_RUNTIME_SERVICE_PRINCIPAL",
    "SDA_PLATFORM_ADMIN_GROUP",
    "SDA_OPERATOR_GROUP",
    "SDA_OBSERVER_GROUP",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=("dev", "staging", "prod"), required=True)
    parser.add_argument("--profile")
    args = parser.parse_args()
    if args.target in {"staging", "prod"}:
        values = {name: os.getenv(name, "").strip() for name in REQUIRED_ENV}
        if any(not value for value in values.values()) or any(
            "__REQUIRED_" in value for value in values.values()
        ):
            print("Controlled-target identity variables are missing or placeholders", file=sys.stderr)
            return 2
    command = ["databricks", "bundle", "validate", "-t", args.target]
    if args.profile:
        command.extend(("--profile", args.profile))
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
