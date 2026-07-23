# Architecture — Article 03

Article 03 gives the Synthetic Data Agent a deployable Databricks home. The project still follows the Article 02 design principle:

> The agent orchestrates. Deterministic tools calculate facts and execute work.

This milestone adds the delivery layer that future deterministic tools will use.

## Runtime path

```text
Git repository
  -> Declarative Automation Bundle
  -> Databricks workspace job
  -> bootstrap notebook
  -> Unity Catalog information_schema
  -> structured discovery summary
```

## Bundle responsibilities

The bundle now owns:

- deployment targets: `dev`, `staging`, and `prod`;
- workflow resources: `bootstrap_check`;
- environment variables for catalogs and schemas;
- resource permissions;
- staging/prod run identity;
- controlled workspace paths.

The bundle does not own Unity Catalog data privileges. Those remain explicit platform grants handled through SQL, Terraform, or another approved provisioning layer.

## First workflow

`bootstrap_check` proves that the platform path works before any synthetic generation starts:

1. Databricks deploys the source-controlled job.
2. The job passes target-specific parameters into the notebook.
3. The notebook validates simple Unity Catalog identifiers.
4. The run identity queries `system.information_schema`.
5. The task returns visible table and column counts.

Discovery is not profiling. It only verifies the governed metadata path.

## Permission boundaries

The project separates three concerns:

- deployment identity: who runs `databricks bundle deploy`;
- run identity: who executes the Databricks job;
- Unity Catalog privileges: what the run identity can see or modify.

Development can run interactively. Staging and production should run with service principals and restricted bundle root paths outside `/Shared`.

## What remains out of scope

This milestone does not implement:

- full Unity Catalog metadata normalization;
- table profiling;
- relationship detection;
- pattern detection;
- Lakebase state;
- synthetic generation;
- validation;
- publishing.

The point is to make sure every future milestone has a controlled, testable, deployable place to live.
