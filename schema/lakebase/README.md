# SDA-08 Lakebase state schema

This directory contains the versioned PostgreSQL/Lakebase state schema. The SQL
file is applied by the schema runner and recorded by content hash in
`sda_schema_versions`; rerunning a deployment is therefore idempotent while a
changed schema definition produces a new identifier instead of silently changing
history.

Apply the versioned schema from a workstation or a controlled Databricks job
after obtaining a short-lived Lakebase credential:

```powershell
python scripts/apply_state_schema.py "$env:SDA_POSTGRES_DSN" `
  --schema-dir schema/lakebase
```
