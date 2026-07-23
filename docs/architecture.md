# Architecture baseline — Article 01

Article 01 establishes the mission and the repository baseline. It does not implement the future Databricks system yet.

## Intended system direction

The completed Synthetic Data Agent will coordinate deterministic tools that:

1. discover governed source assets through Unity Catalog;
2. profile distributions, nulls, formats, and outliers;
3. validate keys and table relationships;
4. capture cross-column patterns and business rules;
5. build an approved generation plan;
6. generate synthetic tables;
7. validate usefulness, integrity, and safety;
8. publish approved outputs into governed destinations.

The agent will orchestrate and explain. Deterministic tools will calculate and execute. Governance, state, validation, and explicit approvals will remain first-class concerns.

## Scope in this branch

This branch contains only an installable Python package, configuration, logging, a CLI, tests, static analysis, and CI. Databricks SDKs, Spark, Unity Catalog access, agents, generation, and validation are intentionally deferred.

## Article reference

[SDA 01: Why Build a Synthetic Data Agent on Databricks?](https://medium.com/towards-data-engineering/sda-01-why-build-a-synthetic-data-agent-on-databricks-1c1b4e0738b7)
