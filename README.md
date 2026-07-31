# Synthetic Data Agent — Article 05

This branch extends the governed Article 04 foundation with the deterministic SDA 05 `table_profiler` tool.

The project still does **not** generate synthetic rows. SDA 05 adds governed value-level profiling while preserving the Article 04 metadata contract.

## Current status

This branch now supports three execution modes:

- **Local demo mode** with deterministic sample metadata.
- **Local Databricks SQL Warehouse mode** through the Databricks SQL Connector.
- **Databricks Bundle serverless mode** for Databricks Free/serverless workspaces.

Bundle validation and deployment are environment-dependent; local checks cover compilation and deterministic reader contracts. A successful run may still return an empty inventory when the configured catalog/schema scope does not contain visible matching tables.

## What this milestone adds

- `uc_metadata_reader` as a deterministic metadata discovery tool.
- Typed Article 04 metadata contracts in `metadata_models.py`.
- Explicit metadata reader configuration: catalog allowlist, schema allowlist, table patterns, and object limit.
- Real Databricks/Spark `INFORMATION_SCHEMA` adapter for Unity Catalog metadata.
- Real local Databricks SQL Warehouse adapter for Unity Catalog metadata.
- Databricks Free/serverless-compatible bundle job using a job `environment_key`.
- Workspace-wide `system.information_schema` queries filtered by catalog and schema.
- Filtering by catalog, schema, table pattern, view inclusion, and object limit.
- Sensitivity signals from names, comments, and tags.
- Relationship hints from declared PK/FK/unique constraints.
- Metadata warnings for missing comments, missing constraints, unvalidated claims, and sensitive fields without tags.
- Compact agent-readable summaries with traceable reasons.
- CLI `metadata-demo` command for local contract testing.
- CLI `metadata-read-sql` command for local SQL Warehouse execution.
- CLI `metadata-read-spark` command for Databricks Spark/serverless execution.
- Tests for metadata contracts, reader behavior, orchestration integration, and configuration.

## What it deliberately does not add

- Source table value reads.
- Statistical profiling.
- Relationship validation.
- Privacy approval logic.
- Synthetic row generation.
- Publishing.

Metadata is evidence, not truth. Declared constraints are captured as claims and marked unvalidated; profiling will validate actual behavior in Article 05.

## Requirements and installation

Python 3.11 or newer. The project is platform-neutral: use the command block that matches your shell.

### Linux / macOS / bash

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Or use the helper script:

```bash
./scripts/setup-dev.sh
```

To execute real Unity Catalog queries from your laptop through a Databricks SQL Warehouse, install the optional Databricks connector extra:

```bash
python -m pip install -e ".[dev,databricks]"
```

Or use:

```bash
./scripts/setup-dev-databricks.sh
```

### Windows / PowerShell

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

To execute real Unity Catalog queries from your laptop through a Databricks SQL Warehouse, install the optional Databricks connector extra:

```powershell
python -m pip install -e ".[dev,databricks]"
```

## Local commands

```bash
sda config
sda design-demo
sda metadata-demo
```

The same commands work in PowerShell.

`design-demo` proves the Article 02 orchestration contract. `metadata-demo` returns a small normalized metadata inventory that shows allowed-object filtering, sensitivity signals, constraint hints, skipped objects, and warnings.

## Read real Unity Catalog metadata from a laptop

Use this path when you want to query Unity Catalog from your local machine through a Databricks SQL Warehouse.

Linux / macOS / bash:

```bash
export SDA_METADATA_RUNTIME="databricks_sql"
export DATABRICKS_HOST="https://<workspace-host>"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
export DATABRICKS_TOKEN="<token>"
export SDA_CATALOG_ALLOWLIST="<catalog>"
export SDA_SCHEMA_ALLOWLIST="<schema>"
export SDA_TABLE_PATTERNS="*"
export SDA_MAX_METADATA_OBJECTS="100"

sda metadata-read-sql
```

Windows / PowerShell:

```powershell
$env:SDA_METADATA_RUNTIME = "databricks_sql"
$env:DATABRICKS_HOST = "https://<workspace-host>"
$env:DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/<warehouse-id>"
$env:DATABRICKS_TOKEN = "<token>"
$env:SDA_CATALOG_ALLOWLIST = "<catalog>"
$env:SDA_SCHEMA_ALLOWLIST = "<schema>"
$env:SDA_TABLE_PATTERNS = "*"
$env:SDA_MAX_METADATA_OBJECTS = "100"

sda metadata-read-sql
```

A reusable template is available in `.env.example`. Do not commit a real `.env` file, Databricks token, workspace host, SQL Warehouse path, or user-specific profile.

## Run as a Databricks Bundle job

This is the recommended path for Databricks Free/serverless workspaces.

The bundle job resource key is:

```text
uc_metadata_reader
```

The job is defined in:

```text
bundle/resources.yml
```

It runs this Spark entrypoint:

```text
src/sda/job_entrypoints/metadata_read_spark.py
```

The task is configured for **Databricks serverless** with an `environment_key`; it does not require an existing cluster, a job cluster, or a cluster ID.

Validate, deploy, and run:

```bash
databricks bundle validate -t dev --profile <profile-name>
databricks bundle deploy -t dev --profile <profile-name>
databricks bundle run uc_metadata_reader -t dev --profile <profile-name>
```

PowerShell uses the same commands:

```powershell
databricks bundle validate -t dev --profile <profile-name>
databricks bundle deploy -t dev --profile <profile-name>
databricks bundle run uc_metadata_reader -t dev --profile <profile-name>
```

Run the SDA 05 profiler against one approved relation:

```bash
databricks bundle run table_profiler -t dev --profile <profile-name> \
  --params="source_table=<catalog>.<schema>.<table>,mode=quick"
```

The dev target permits diagnostic schema creation and best-effort snapshots. Staging
and production disable both by default; their governed profile schema must already
exist and a reproducible Delta source version must be available.

Override the metadata scope when running the bundle:

```bash
databricks bundle run uc_metadata_reader -t dev --profile <profile-name> \
  --var="sda_catalog_allowlist=<catalog>" \
  --var="sda_schema_allowlist=<schema>" \
  --var="sda_table_patterns=*" \
  --var="sda_max_metadata_objects=100"
```

PowerShell one-line version:

```powershell
databricks bundle run uc_metadata_reader -t dev --profile <profile-name> --var="sda_catalog_allowlist=<catalog>" --var="sda_schema_allowlist=<schema>" --var="sda_table_patterns=*" --var="sda_max_metadata_objects=100"
```

## Databricks Free/serverless notes

Databricks Free/serverless workspaces may not expose catalog-local paths such as:

```sql
<catalog>.information_schema.tables
```

For that reason, this project queries workspace-wide metadata views such as:

```sql
system.information_schema.tables
system.information_schema.columns
system.information_schema.table_constraints
```

and filters by `table_catalog`, `table_schema`, and table name patterns. This keeps the reader compatible with the serverless runtime used by Databricks Free.

A successful run can legitimately return:

```json
{
  "skipped_objects": [],
  "tables": [],
  "warnings": []
}
```

That means the job executed successfully, but the configured catalog/schema/table scope did not return visible matching tables for the run identity. Change `sda_catalog_allowlist`, `sda_schema_allowlist`, or `sda_table_patterns` to point at objects that exist and are visible to the profile or job identity.

## Quality checks

Run before committing:

```bash
ruff check .
mypy src tests
pytest
```

On Linux/macOS you can also run:

```bash
make check
./scripts/check.sh
```

## Safety and repository hygiene

Before committing, verify that no personal or workspace-specific values are present:

- no Databricks tokens;
- no workspace host URLs;
- no user email addresses;
- no profile names;
- no cluster IDs;
- no SQL Warehouse IDs;
- no job run URLs;
- no generated `.env` files;
- no local virtual environments or caches.

Use placeholders such as `<profile-name>`, `<workspace-host>`, `<warehouse-id>`, `<catalog>`, and `<schema>` in documentation.
## SDA 05 milestone

Implemented through SDA 05: declarative bundle deployment with environment-specific
guardrails, a deterministic Unity Catalog metadata reader, and a one-relation
`table_profiler` that produces versioned numeric, categorical, string, temporal,
missingness, outlier, complex-type, freshness, provenance, and governed Delta evidence.

Designed but intentionally deferred: relationship validation/inference, broad
cross-column pattern detection, durable operational state, generation, quality
validation, and publishing. A missing metadata signal is not proof that data is safe;
declared constraints remain unvalidated, and profiling remains evidence rather than a
business-rule or privacy verdict.
