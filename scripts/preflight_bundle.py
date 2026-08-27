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
IDENTITY_VARIABLES = {
    "SDA_RUNTIME_SERVICE_PRINCIPAL": "runtime_service_principal_name",
    "SDA_PLATFORM_ADMIN_GROUP": "platform_admin_group_name",
    "SDA_OPERATOR_GROUP": "operator_group_name",
    "SDA_OBSERVER_GROUP": "observer_group_name",
}
CONTROLLED_SCOPE_ENV = {
    "SDA_PROFILE_SOURCE_TABLE": "sda_profile_source_table",
    "SDA_SCOPE_CATALOG": "sda_scope_catalog",
    "SDA_SCOPE_SCHEMA": "sda_scope_schema",
    "SDA_SCOPE_TABLES": "sda_scope_tables",
    "SDA_RELATIONSHIP_PARENT_TABLE": "sda_relationship_parent_table",
    "SDA_RELATIONSHIP_CHILD_TABLE": "sda_relationship_child_table",
}

FQN_OUTPUT_VARIABLES = (
    "sda_manifest_table",
    "sda_metadata_inventory_table",
    "sda_relationship_output_table",
    "sda_graph_output_table",
    "sda_artifact_registry_table",
    "sda_pattern_registry_table",
    "sda_pattern_evidence_table",
)


def validate_output_prefixes(target: str, values: dict[str, str]) -> list[str]:
    expected = {
        "dev": "sda_dev.",
        "staging": "workspace.sda_staging.",
        "prod": "workspace.sda_prod.",
    }[target]
    return [
        name
        for name in FQN_OUTPUT_VARIABLES
        if values.get(name, "").strip() and not values[name].strip().startswith(expected)
    ]


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
            print(
                "Controlled-target identity variables are missing or placeholders", file=sys.stderr
            )
            return 2
        scope_values = {name: os.getenv(name, "").strip() for name in CONTROLLED_SCOPE_ENV}
        if any(not value for value in scope_values.values()) or any(
            "__REQUIRED_" in value for value in scope_values.values()
        ):
            print("Controlled-target source scope variables are required", file=sys.stderr)
            return 2
        if (
            scope_values["SDA_RELATIONSHIP_PARENT_TABLE"]
            == scope_values["SDA_RELATIONSHIP_CHILD_TABLE"]
        ):
            print("Controlled-target relationship parent and child must differ", file=sys.stderr)
            return 2
        output_values = {name: os.getenv(name, "").strip() for name in FQN_OUTPUT_VARIABLES}
        invalid = validate_output_prefixes(args.target, output_values)
        if invalid:
            print(
                f"Controlled-target outputs use the wrong catalog: {', '.join(invalid)}",
                file=sys.stderr,
            )
            return 2
    command = ["databricks", "bundle", "validate", "-t", args.target]
    if args.target in {"staging", "prod"}:
        for env_name, variable_name in IDENTITY_VARIABLES.items():
            command.extend(("--var", f"{variable_name}={os.environ[env_name].strip()}"))
        for env_name, variable_name in CONTROLLED_SCOPE_ENV.items():
            command.extend(("--var", f"{variable_name}={os.environ[env_name].strip()}"))
    if args.profile:
        command.extend(("--profile", args.profile))
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
