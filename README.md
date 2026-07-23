# Synthetic Data Agent — Article 03

This repository implements the **SDA 03 bootstrap** from *Bootstrapping with Declarative Automation Bundles*.

The project still does **not** generate synthetic rows. That is intentional. This milestone proves that the solution has a deployable Databricks home before the agent starts profiling, detecting relationships, planning generation, or writing outputs.

## What this milestone adds

- A root `databricks.yml` bundle definition.
- Modular bundle files under `bundle/`.
- Separate `dev`, `staging`, and `prod` targets.
- Target-specific Unity Catalog variables.
- A first Databricks workflow: `bootstrap_check`.
- A thin notebook entry point: `notebooks/00_bootstrap_check.py`.
- Validated parameter loading for catalog and schema names.
- Automatic development bootstrap for Unity Catalog objects when the run identity is allowed to create them.
- Optional dev fallback to the first visible catalog/schema when catalog creation is not allowed.
- A tiny sample source table so the first discovery run has something to find.
- Unity Catalog discovery through `system.information_schema`.
- Bundle resource permissions and staging/prod `run_as` boundaries.
- Local unit tests for bootstrap contracts.
- Real Databricks validate/deploy/run instructions.

## Project layout

```text
synthetic-data-agent/
├── databricks.yml
├── bundle/
│   ├── resources.yml
│   └── targets/
│       ├── dev.yml
│       ├── staging.yml
│       └── prod.yml
├── notebooks/
│   └── 00_bootstrap_check.py
├── scripts/
│   └── grants/
│       └── bootstrap_uc_grants.sql      # optional reference only
├── src/
│   └── sda/
│       ├── bootstrap.py
│       ├── uc_discovery.py
│       └── ...
├── tests/
└── docs/
```

The rule from the article is enforced in the structure: notebooks are entry points. Core logic belongs in `src/sda/` where it can be tested, reviewed, packaged, and reused.

## Local setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

## Local tests

Run the deterministic checks that do not require Databricks:

```powershell
ruff check .
mypy src tests
pytest
```

Or run everything through Make:

```powershell
make check
```

The Databricks notebook itself is tested through a real bundle deployment because it depends on the Databricks runtime, `spark`, `dbutils`, and Unity Catalog permissions.

## Authenticate to Databricks

This project expects your dev workspace to be configured as a CLI profile named `sda-dev`:

```powershell
databricks auth login --host https://<your-workspace-url> -p sda-dev
databricks auth describe -p sda-dev
```

The `dev` target intentionally uses the profile instead of hardcoding the workspace host:

```yaml
workspace:
  profile: sda-dev
```

For staging and production, create equivalent profiles named `sda-staging` and `sda-prod`, or change the target files to match your organization’s profile names.

## Self-contained development bootstrap

The dev target is designed to run without manual SQL.

By default it tries to prepare this Unity Catalog scope automatically:

```text
catalog:       sda_dev
source schema: sample_source
output schema: sandbox
sample table:  sda_dev.sample_source.sample_customers
```

The first job does this inside Databricks:

1. Loads bundle parameters.
2. Runs `CREATE CATALOG IF NOT EXISTS sda_dev`.
3. Runs `CREATE SCHEMA IF NOT EXISTS sda_dev.sample_source`.
4. Runs `CREATE SCHEMA IF NOT EXISTS sda_dev.sandbox`.
5. Creates and seeds a tiny `sample_customers` Delta table.
6. Reads `system.information_schema` to discover visible tables and columns.
7. Emits a structured `bootstrap_summary`.

If your user cannot create catalogs, the dev target falls back to the first visible catalog/schema so the bundle can still prove the deployment path. The run output will include a warning explaining which fallback scope was used.

This fallback is enabled only for development. Staging and production set:

```yaml
auto_create_uc_objects: "false"
allow_catalog_fallback: "false"
```

That keeps controlled environments honest: they should validate approved governed assets, not silently create or switch scopes.

## Deploy and run on Databricks

From the project root:

```powershell
databricks bundle validate -t dev
databricks bundle plan -t dev
databricks bundle deploy -t dev
databricks bundle run bootstrap_check -t dev
databricks bundle summary -t dev
```

No `bootstrap_cluster_id` is required. The bootstrap task does not declare `existing_cluster_id`; it is ready for serverless Jobs compute where your workspace supports it. If your workspace requires explicit job compute, add that compute policy to `bundle/resources.yml` or the target configuration rather than hardcoding it in the notebook.

A successful run prints a JSON payload like:

```json
{
  "bootstrap_summary": {
    "catalog_name": "sda_dev",
    "source_schema_name": "sample_source",
    "output_schema_name": "sandbox",
    "visible_tables": 1,
    "visible_columns": 5,
    "target_environment": "dev",
    "auto_create_uc_objects": true,
    "seed_sample_data": true,
    "warnings": [
      "Verified or created configured Unity Catalog bootstrap objects."
    ]
  }
}
```

If the run cannot create `sda_dev` and uses fallback, that is still a valid dev smoke test. For the next articles, use a real governed catalog/schema by setting variables at deployment time or in the target file.

## Optional variable overrides

You can still override the development scope without editing source files:

```powershell
databricks bundle deploy -t dev `
  --var "catalog_name=main" `
  --var "source_schema_name=default" `
  --var "output_schema_name=default" `
  --var "auto_create_uc_objects=false"

databricks bundle run bootstrap_check -t dev
```

Remember: bundle variables are deployment-time configuration. Redeploy after changing them.

## Promote beyond development

Only after the dev loop works, configure staging and production profiles plus real group names and a real service principal:

```powershell
databricks auth login --host https://<staging-workspace-url> -p sda-staging
databricks auth login --host https://<prod-workspace-url> -p sda-prod
```

Then validate staging with controlled values:

```powershell
databricks bundle validate -t staging `
  --var "runtime_service_principal_name=<service-principal-app-id>" `
  --var "platform_admin_group_name=<group>" `
  --var "operator_group_name=<group>" `
  --var "observer_group_name=<group>"
```

Production should use the same lifecycle, but with production-safe values, a locked-down `root_path`, service-principal execution, and reviewed Unity Catalog privileges. Do not hotfix production jobs through the workspace UI and forget to bring the change back into the bundle.

## What success means

Success is not synthetic data.

Success is proving that:

- the repository validates locally;
- the bundle validates for a target;
- Databricks deploys the job from source-controlled YAML;
- the notebook receives bundle-driven parameters;
- the workflow can prepare or resolve a Unity Catalog scope in dev;
- the run identity can read Unity Catalog metadata;
- the workflow returns a structured discovery summary.

Next milestone: replace this bootstrap check with the first real tool, `uc_metadata_reader`.

## Article references

- Article 01: Why Build a Synthetic Data Agent on Databricks?
- Article 02: Designing the Synthetic Data Agent
- Article 03: Bootstrapping with Declarative Automation Bundles
