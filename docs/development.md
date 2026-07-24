# Development

This project is intentionally platform-neutral. The Python package and CLI do not assume Windows or Linux; only the shell syntax for environment variables changes.

## Linux / macOS setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

For the Databricks SQL Warehouse metadata reader:

```bash
python -m pip install -e ".[dev,databricks]"
```

Equivalent helper scripts are available:

```bash
./scripts/setup-dev.sh
./scripts/setup-dev-databricks.sh
```

## Windows / PowerShell setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

For the Databricks SQL Warehouse metadata reader:

```powershell
python -m pip install -e ".[dev,databricks]"
```

## Environment variables

The code reads standard environment variables. Use the syntax appropriate for your shell.

Linux / macOS / bash:

```bash
export SDA_CATALOG_ALLOWLIST="<catalog>"
export SDA_SCHEMA_ALLOWLIST="<schema>"
export SDA_TABLE_PATTERNS="*"
export SDA_MAX_METADATA_OBJECTS="100"
sda config
```

Windows / PowerShell:

```powershell
$env:SDA_CATALOG_ALLOWLIST = "<catalog>"
$env:SDA_SCHEMA_ALLOWLIST = "<schema>"
$env:SDA_TABLE_PATTERNS = "*"
$env:SDA_MAX_METADATA_OBJECTS = "100"
sda config
```

A reusable template is available at `.env.example`. On Linux/macOS, copy it to `.env`, edit the values, and load it with:

```bash
set -a
source .env
set +a
```

Do not commit a real `.env` file or Databricks token.

## Local quality checks

```bash
ruff check .
mypy src tests
pytest
```

or on Linux/macOS:

```bash
make check
./scripts/check.sh
```

## Databricks CLI profile

Use a valid Databricks CLI profile when running bundle commands:

```bash
databricks auth profiles
databricks current-user me --profile <profile-name>
```

Then pass the profile explicitly:

```bash
databricks bundle validate -t dev --profile <profile-name>
databricks bundle deploy -t dev --profile <profile-name>
databricks bundle run uc_metadata_reader -t dev --profile <profile-name>
```

Do not commit profile names, workspace URLs, run URLs, user emails, or tokens to the repository.

## Databricks Free/serverless workflow

This branch uses a serverless-compatible bundle job. The job task uses an `environment_key` and does not require:

- `existing_cluster_id`;
- `new_cluster`;
- `job_cluster_key`;
- a local all-purpose cluster.

Run:

```bash
databricks bundle validate -t dev --profile <profile-name>
databricks bundle deploy -t dev --profile <profile-name>
databricks bundle run uc_metadata_reader -t dev --profile <profile-name>
```

Override metadata scope as needed:

```bash
databricks bundle run uc_metadata_reader -t dev --profile <profile-name> \
  --var="sda_catalog_allowlist=<catalog>" \
  --var="sda_schema_allowlist=<schema>" \
  --var="sda_table_patterns=*" \
  --var="sda_max_metadata_objects=100"
```

An empty inventory means the job ran successfully but found no visible tables in the configured scope.

## Troubleshooting

### `Refresh token is invalid`

Your Databricks CLI profile token is stale. Re-authenticate the affected profile:

```bash
databricks auth login --host <workspace-host> --profile <profile-name>
```

### `resource with key "uc_metadata_reader" not found`

The bundle does not include `bundle/resources.yml`, or the job resource key is missing. Confirm `databricks.yml` includes:

```yaml
include:
  - bundle/resources.yml
  - bundle/targets/*.yml
```

### `Only serverless compute is supported in the workspace`

Do not use `existing_cluster_id`, `new_cluster`, or `job_cluster_key` in this workspace. The current bundle uses `environment_key` for serverless execution.

### `TABLE_OR_VIEW_NOT_FOUND` for `<catalog>.information_schema.tables`

Databricks Free/serverless may not expose catalog-local information schema. This project uses `system.information_schema` and filters by `table_catalog` to avoid that problem.

### Successful run returns no tables

The run identity has no visible matching tables in the configured scope, or the catalog/schema/table pattern is too narrow. Override the variables with existing visible objects.
