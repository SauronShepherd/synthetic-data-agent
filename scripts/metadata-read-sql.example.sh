#!/usr/bin/env bash
set -euo pipefail

export SDA_METADATA_RUNTIME="databricks_sql"
export DATABRICKS_HOST="https://<workspace-host>"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
export DATABRICKS_TOKEN="<token>"
export SDA_CATALOG_ALLOWLIST="main"
export SDA_SCHEMA_ALLOWLIST="sales"
export SDA_TABLE_PATTERNS="customers*,orders"
export SDA_MAX_METADATA_OBJECTS="100"

sda metadata-read-sql
