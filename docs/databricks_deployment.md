# Deploying SDA 03 on Databricks

This guide turns the local repository into a real Databricks deployment. The development path is self-contained: it does not require you to run Unity Catalog setup SQL manually.

## 1. Prerequisites

You need:

- Databricks CLI installed and authenticated.
- Workspace access for the target environment.
- A Unity Catalog metastore attached to the workspace.
- For development: permission to create the configured dev catalog, or at least visibility into one existing catalog/schema so the fallback smoke test can run.
- For staging and production: a service principal application ID and approved groups for bundle permissions.

No all-purpose cluster ID is required by default. The job task does not use `existing_cluster_id`; it is ready for serverless Jobs compute where available. If your workspace requires explicit job compute, define that compute in `bundle/resources.yml` or target configuration.

## 2. Authenticate with a profile

For your current dev workspace:

```powershell
databricks auth login --host https://<your-workspace-url> -p sda-dev
databricks auth describe -p sda-dev
```

The dev target uses:

```yaml
workspace:
  profile: sda-dev
```

This keeps the workspace host in your CLI profile instead of hardcoding it into the bundle.

## 3. Validate locally

```powershell
ruff check .
mypy src tests
pytest
```

## 4. Validate the bundle

```powershell
databricks bundle validate -t dev
```

A warning about the user-owned workspace folder permission can be ignored in dev. The blocking errors to fix are missing variables, broken paths, authentication, or invalid resource definitions.

## 5. Inspect the plan

```powershell
databricks bundle plan -t dev
```

Review what the bundle is about to create, update, or remove. In production, do not rely on selective deployment as a normal release process.

## 6. Deploy

```powershell
databricks bundle deploy -t dev
```

This creates or updates the Databricks job and synchronizes the notebook and source files to the workspace.

## 7. Run the bootstrap job

```powershell
databricks bundle run bootstrap_check -t dev
```

The job will:

1. Read the bundle parameters.
2. Try to create `sda_dev.sample_source` and `sda_dev.sandbox`.
3. Seed `sda_dev.sample_source.sample_customers`.
4. If catalog creation is not permitted in dev, fall back to the first visible catalog/schema.
5. Query `system.information_schema` for visible tables and columns.
6. Print a structured `bootstrap_summary`.

## 8. Inspect managed resources

```powershell
databricks bundle summary -t dev
```

Use the resource links to verify the deployed job and its workspace path.

## 9. Expected output

A successful self-contained dev run prints something like:

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

If fallback was used, the summary will show the effective catalog/schema and include a warning. That means the bundle path works, but your user does not have privileges to create the requested dev catalog.

## 10. Promote carefully

Create staging and production CLI profiles:

```powershell
databricks auth login --host https://<staging-workspace-url> -p sda-staging
databricks auth login --host https://<prod-workspace-url> -p sda-prod
```

Then validate staging with real values:

```powershell
databricks bundle validate -t staging `
  --var "runtime_service_principal_name=<service-principal-app-id>" `
  --var "platform_admin_group_name=<group>" `
  --var "operator_group_name=<group>" `
  --var "observer_group_name=<group>"
```

Repeat `plan`, `deploy`, `run`, and `summary` for staging. Production should follow only after staging proves the same workflow with production-like permissions and paths.

## Troubleshooting

### `Catalog 'sda_dev' is not visible`

In this version, the dev job tries to create `sda_dev` automatically. If your user cannot create catalogs, the job should fall back to a visible catalog/schema when one exists. If no fallback is possible, your workspace identity has neither create privileges nor visible catalog access.

### Serverless Jobs compute is not available

Add an explicit job compute configuration to `bundle/resources.yml` or your target file. Keep compute in bundle configuration, not inside notebook code.

### Job deploys to the wrong workspace

Check the selected target and CLI profile:

```powershell
databricks auth describe -p sda-dev
databricks bundle validate -t dev
```

### Manual UI fix disappears

That is expected. Bundle configuration is the source of truth. Put the fix in YAML, redeploy, and avoid permanent UI drift.

### Tables are visible but data reads fail later

This milestone checks metadata discovery. Later profiling jobs require data-read privileges. Metadata access does not automatically mean full row-level data access.
