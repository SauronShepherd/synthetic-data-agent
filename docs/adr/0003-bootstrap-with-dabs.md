# ADR 0003 — Bootstrap with Declarative Automation Bundles

## Status

Accepted for Article 03.

## Context

The SDA is moving from an architecture-only local project to a deployable Databricks project. Future tools need a repeatable path from source control to a real workspace.

## Decision

Use Declarative Automation Bundles as the delivery contract. The repository will include a root `databricks.yml`, modular resource and target files, a first `bootstrap_check` job, target-specific variables, permissions, and staging/prod run identities.

## Consequences

- Databricks resources are declared as code rather than created manually.
- Development, staging, and production receive explicit target boundaries.
- The first job validates deployment, parameters, run identity, and Unity Catalog discovery.
- Production deployment requires real group names, service principals, and locked-down workspace paths.
- Unity Catalog grants remain outside the bundle and must be provisioned deliberately.

## Non-goals

This decision does not implement profiling, relationship detection, generation, validation, or publishing. It creates the safe execution path those tools will use.
